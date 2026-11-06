"""VERIFY_TABLE on Python edges (F-03): argument whitelist + generic row-based implementation. Stdlib only, so the dev
edge and the Linux daemon can use it without pydantic.

Contract = alelt_common.models.VerifyTableArgs / VerifyTableResult = the C agent (edge-agent/core/src/agent/command.c,
connectors/sources/db2/edge/src/db2_plugin.c). `validate_args` mirrors the C parser: unknown fields, checks or
identifiers are refused; key values travel as text; defaults are made explicit.

`compute_from_rows` serves edges whose connector has no server-side aggregates (remote engines behind aleltd): it
reads rows through the connector's own read-only snapshot path and computes the same result shape on the edge host.
Db2 edges use server-side SELECTs instead (alelt_db2edge.verify, same statements as the C plugin).
"""
from __future__ import annotations

import time as _time
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

CHECKS = ("row_count", "aggregates", "keys", "rows")
MAX_RANGES = 256
MAX_KEYS = 10_000
MAX_KEY_VALUES = 1_000
MAX_COLUMNS = 1012
MAX_TIMEOUT_S = 600
MAX_KEYCOLS = 16
MAX_KEYVAL = 511
MAX_RESULT_BYTES = 8 * 1024 * 1024
_IDENT = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_$#@")
_ARG_KEYS = {"table", "verify_id", "checks", "columns", "ranges", "key_values", "max_keys", "timeout_s"}


class VerifyArgsError(ValueError):
    """Refused by the whitelist (R5): acked ok=false, never executed."""


def ident_ok(s: Any) -> bool:
    return isinstance(s, str) and 0 < len(s) < 129 and set(s) <= _IDENT


def _uuid_ok(s: Any) -> bool:
    import uuid
    if not isinstance(s, str) or len(s) != 36:
        return False
    try:
        uuid.UUID(s)
    except ValueError:
        return False
    return True


def _key(v: Any, what: str) -> List[str]:
    if not isinstance(v, list) or not 1 <= len(v) <= MAX_KEYCOLS:
        raise VerifyArgsError(f"{what} must be an array of 1..{MAX_KEYCOLS} scalars")
    out = []
    for x in v:
        if isinstance(x, bool) or not isinstance(x, (str, int, float)):
            raise VerifyArgsError(f"{what} values must be short strings or numbers")
        t = x if isinstance(x, str) else repr(x) if isinstance(x, float) else str(x)
        if len(t.encode("utf-8")) > MAX_KEYVAL:
            raise VerifyArgsError(f"{what} values must be short strings or numbers")
        out.append(t)
    return out


def validate_args(a: Any) -> Dict[str, Any]:
    """Whitelisted, canonical VERIFY_TABLE args (raises VerifyArgsError)."""
    if not isinstance(a, dict):
        raise VerifyArgsError("VERIFY_TABLE needs args")
    bad = set(a) - _ARG_KEYS
    if bad:
        raise VerifyArgsError(f"unknown argument '{sorted(bad)[0]}'")
    t = a.get("table")
    if not isinstance(t, dict) or set(t) - {"schema", "name"} or not ident_ok(t.get("schema")) \
            or not ident_ok(t.get("name")):
        raise VerifyArgsError("args.table must be {schema,name} identifiers")
    if not _uuid_ok(a.get("verify_id")):
        raise VerifyArgsError("verify_id must be a UUID")
    checks = a.get("checks")
    if not isinstance(checks, list) or not 1 <= len(checks) <= 4 or len(set(map(str, checks))) != len(checks):
        raise VerifyArgsError("checks must be an array of 1..4 unique names")
    for c in checks:
        if c not in CHECKS:
            raise VerifyArgsError(f"check {str(c)[:32]!r} is not whitelisted")
    out: Dict[str, Any] = {"table": {"schema": t["schema"], "name": t["name"]}, "verify_id": a["verify_id"],
                           "checks": list(checks)}
    cols = a.get("columns")
    if cols is not None:
        if not isinstance(cols, list) or not 1 <= len(cols) <= MAX_COLUMNS or not all(ident_ok(c) for c in cols):
            raise VerifyArgsError("columns must be an array of 1..1012 identifiers")
        out["columns"] = list(cols)
    rng = a.get("ranges")
    if rng is not None:
        if not isinstance(rng, list) or len(rng) > MAX_RANGES:
            raise VerifyArgsError(f"ranges must be an array (max {MAX_RANGES})")
        rs = []
        for r in rng:
            if not isinstance(r, dict) or set(r) - {"after", "to"}:
                raise VerifyArgsError("each range is {after, to}")
            rs.append({"after": None if r.get("after") is None else _key(r["after"], "ranges[].after"),
                       "to": None if r.get("to") is None else _key(r["to"], "ranges[].to")})
        out["ranges"] = rs
    kv = a.get("key_values")
    if ("rows" in checks) != bool(kv):
        raise VerifyArgsError("key_values is required with (and only with) the 'rows' check")
    if kv is not None:
        if not isinstance(kv, list) or len(kv) > MAX_KEY_VALUES:
            raise VerifyArgsError(f"key_values must be an array (max {MAX_KEY_VALUES})")
        out["key_values"] = [_key(k, "key_values[]") for k in kv]
    for name, default, hi in (("max_keys", MAX_KEYS, MAX_KEYS), ("timeout_s", 300, MAX_TIMEOUT_S)):
        v = a.get(name, default)
        if isinstance(v, bool) or not isinstance(v, int) or not 1 <= v <= hi:
            raise VerifyArgsError(f"{name} out of range")
        out[name] = v
    return out


# ------------------------------------------------------------------ text forms + aggregate classes
def text(v: Any) -> Optional[str]:
    """Source value -> the text form edges put on the wire (hub parses it by column type)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex().upper()
    if isinstance(v, datetime):
        if v.tzinfo is not None:
            v = v.astimezone(timezone.utc).replace(tzinfo=None)
        return v.isoformat(sep=" ")
    if isinstance(v, (date, time)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, float):
        return repr(v)
    return str(v)


def parse_text(v: Any, ctype: Any) -> Any:
    """Source text form (Db2 CLI / generic edge) -> typed value of the canonical type; unparseable stays text."""
    if v is None or not isinstance(v, str):
        return v
    t = getattr(ctype, "value", ctype)
    s = v.strip()
    try:
        if t in ("SHORT", "INT", "LONG"):
            return int(Decimal(s))
        if t == "DECIMAL":
            return Decimal(s)
        if t in ("FLOAT", "DOUBLE"):
            return float(s)
        if t == "LOCALDATE":
            return date.fromisoformat(s[:10])
        if t == "LOCALTIME":
            return time.fromisoformat(s.replace(".", ":", 2)[:15])
        if t in ("LOCALDATETIME", "INSTANT"):
            if len(s) > 10 and s[10] == "-":                     # Db2 'yyyy-mm-dd-hh.mm.ss.ffffff'
                s = s[:10] + " " + s[11:].replace(".", ":", 2)
            head, dot, frac = s.partition(".")
            s = head + (dot + frac[:6] if frac else "")
            d = datetime.fromisoformat(s)
            return d
        if t == "BINARY":
            return bytes.fromhex(s)
        if t == "BOOLEAN":
            return s.upper() in ("1", "TRUE", "T", "Y")
    except (ValueError, InvalidOperation, ArithmeticError):
        return v
    return v



def agg_class(canonical_type: str, engine_type: str = "") -> str:
    """Aggregate class from a catalog column (connector canonical type)."""
    t = getattr(canonical_type, "value", canonical_type)
    if t in ("SHORT", "INT", "LONG"):
        return "int"
    if t == "DECIMAL":
        return "dec"
    if t in ("FLOAT", "DOUBLE"):
        return "flt"
    if t in ("LOCALDATE", "LOCALTIME", "LOCALDATETIME", "INSTANT"):
        return "tmp"
    if t == "STRING" and engine_type.upper() not in ("CLOB", "DBCLOB", "LONG VARCHAR", "LONG VARG", "TEXT", "NTEXT",
                                                     "LONGTEXT", "MEDIUMTEXT", "NCLOB"):
        return "str"
    return "cnt"


def _order(v: Any) -> Any:
    if isinstance(v, datetime) and v.tzinfo is not None:
        return v.astimezone(timezone.utc).replace(tzinfo=None)
    return v


def _agg(rows: Sequence[Dict[str, Any]], col: str, cls: str) -> Dict[str, Any]:
    vals = [r.get(col) for r in rows if r.get(col) is not None]
    d: Dict[str, Any] = {"count": len(vals)}
    if cls in ("int", "dec", "flt", "tmp") and vals:
        d["min"], d["max"] = text(min(vals, key=_order)), text(max(vals, key=_order))
    elif cls in ("int", "dec", "flt", "tmp"):
        d["min"] = d["max"] = None
    if cls in ("int", "dec"):
        d["sum"] = text(sum((Decimal(v) if not isinstance(v, Decimal) else v for v in vals), Decimal(0))) \
            if vals else None
    if cls == "str":
        d["len"] = str(sum(len(str(v).rstrip(" ")) for v in vals)) if vals else None
    return d


def compute_from_rows(args: Dict[str, Any], rows: Iterable[Dict[str, Any]], key_columns: Sequence[str],
                      classes: Dict[str, str], key_of: Callable[[Sequence[str]], Tuple[Any, ...]],
                      engine: str, position: Callable[[], Optional[str]] = lambda: None,
                      max_bytes: int = MAX_RESULT_BYTES) -> Dict[str, Any]:
    """Generic VERIFY_TABLE over the rows of one table (live rows read by the connector, read-only).

    `key_of(texts)` converts a key text tuple from the args to a typed tuple comparable with the rows' key values
    (connector-specific parsing). Ranges: `after` exclusive, `to` inclusive (lexicographic on the typed key)."""
    t0 = _time.monotonic()
    checks = set(args["checks"])
    cols = list(args.get("columns") or classes)
    for c in cols:
        if c not in classes:
            raise VerifyArgsError(f"column {c} is not a column of the table")
    if (checks & {"keys", "rows"} or args.get("ranges")) and not key_columns:
        raise VerifyArgsError("table has no primary key; keyed reads impossible")
    before = position()
    data = list(rows)

    def k(r: Dict[str, Any]) -> Tuple[Any, ...]:
        return tuple(_order(r.get(c)) for c in key_columns)

    res: Dict[str, Any] = {"verify_id": args["verify_id"], "table": args["table"], "engine": engine,
                           "isolation": "connector", "key_columns": list(key_columns),
                           "columns": {c: classes[c] for c in cols}, "position_before": before, "ranges": [],
                           "rows": [], "missing": [], "truncated": False}
    size = 0
    if checks & {"row_count", "aggregates", "keys"}:
        ranges = args.get("ranges") or [{"after": None, "to": None}]
        for i, r in enumerate(ranges):
            lo = key_of(r["after"]) if r.get("after") else None
            hi = key_of(r["to"]) if r.get("to") else None
            part = [x for x in data if (lo is None or k(x) > lo) and (hi is None or k(x) <= hi)]
            out: Dict[str, Any] = {"index": i}
            if checks & {"row_count", "aggregates"}:
                out["rows"] = len(part)
            if "aggregates" in checks:
                out["agg"] = {c: _agg(part, c, classes[c]) for c in cols}
            if "keys" in checks:
                ks = sorted(part, key=k)
                out["keys"] = [[text(x.get(c)) for c in key_columns] for x in ks[:args["max_keys"]]]
                out["keys_truncated"] = len(ks) > args["max_keys"]
                size += sum(len(str(x)) for x in out["keys"])
            res["ranges"].append(out)
    if "rows" in checks:
        by = {k(x): x for x in data}
        for kt in args["key_values"]:
            if size >= max_bytes:
                res["truncated"] = True
                break
            row = by.get(key_of(kt))
            if row is None:
                res["missing"].append(list(kt))
            else:
                res["rows"].append({c: text(row.get(c)) for c in cols})
                size += sum(len(v or "") for v in res["rows"][-1].values())
    res["position_after"] = position()
    res["elapsed_ms"] = int((_time.monotonic() - t0) * 1000)
    return res
