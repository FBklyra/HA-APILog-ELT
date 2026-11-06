"""Edge-source reconciliation (F-03): the source side is computed ON the edge by the whitelisted VERIFY_TABLE command
(server-side aggregates, Db2 `WITH CS`); only aggregates, keys of differing ranges and a bounded number of rows cross
the wire. Bounded memory on the source side (F-23): the edge never materialises the table.

Algorithm (cheap first, exact detail only where something differs):
1. key ranges are cut on the candidate (destination) key order, `range_rows` keys each, open at both ends, so the
   whole source key space is covered: (-inf, b0], (b0, b1], ..., (bn, +inf);
2. one command per <= 256 ranges: row_count + aggregates per range (per column class: count, min, max, exact sum,
   RTRIM length sum) + a spread sample of keys re-read in full ("rows");
3. ranges whose rows / aggregates differ: keys of those ranges (`keys`) -> exact KEY_SET diff (missing / extra);
   rows of those ranges re-read by key (<= `max_reread`, 1 000 per command) -> SAMPLED_ROWS pinpoints the
   differing values.

Edge values are the source's text forms (as in snapshot chunks) and are parsed by the catalog's canonical type before
the engine-neutral comparison (`alelt_verify.defaults.canon`).
`call(args) -> result dict` is the transport (hub: issue the command, wait for the ack).
"""
from __future__ import annotations

import json
import math
import uuid
from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import Any

from alelt_common.models import VERIFY_MAX_KEY_VALUES, VERIFY_MAX_RANGES, TableRef
from alelt_common.verify_edge import parse_text

from . import checks
from .defaults import SuiteResult, _sort_key, canon
from .models import CheckName, ReconResult, Status

Call = Callable[[dict[str, Any]], dict[str, Any]]


def factory(table: str, run_id: str | None = None,
            pos_window: tuple[str | None, str | None] = (None, None)) -> checks.Factory:
    """ReconResult factory for one run of one table (evidence rows)."""
    from alelt_common.util import utcnow

    rid = run_id or uuid.uuid4().hex

    def mk(check: CheckName, status: Status, **kw: Any) -> ReconResult:
        return ReconResult(run_id=rid, table=table, check=check, status=status, recorded_at=utcnow(),
                           pos_from=pos_window[0], pos_to=pos_window[1], **kw)
    return mk  # type: ignore[return-value]


# edge text -> typed value: alelt_common.verify_edge.parse_text (shared with the Python edges)


def _num(v: Any) -> Decimal | None:
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, int):
        return Decimal(v)
    if isinstance(v, Decimal):
        return v
    if isinstance(v, float):
        return Decimal(repr(v))
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError):
        return None


def _ord(v: Any) -> Any:
    """Ordering key of a typed value for min/max (numbers numeric, everything else canonical text)."""
    n = _num(v) if isinstance(v, (int, float, Decimal)) and not isinstance(v, bool) else None
    return (0, n, "") if n is not None else (1, Decimal(0), canon(v) or "")


def candidate_agg(rows: Sequence[Mapping[str, Any]], col: str, cls: str) -> dict[str, Any]:
    """Destination-side aggregate with the edge's semantics (canonical text forms)."""
    vals = [r.get(col) for r in rows if r.get(col) is not None]
    out: dict[str, Any] = {"count": len(vals)}
    if cls in ("int", "dec", "flt", "tmp"):
        out["min"] = canon(min(vals, key=_ord)) if vals else None
        out["max"] = canon(max(vals, key=_ord)) if vals else None
    if cls in ("int", "dec"):
        out["sum"] = canon(sum((_num(v) or Decimal(0) for v in vals), Decimal(0))) if vals else None
    if cls == "str":
        out["len"] = str(sum(len(canon(v) or "") for v in vals)) if vals else None
    return out


def edge_agg(a: Mapping[str, Any], cls: str, ctype: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"count": int(a.get("count") or 0)}
    if cls in ("int", "dec", "flt", "tmp"):
        out["min"] = canon(parse_text(a.get("min"), ctype))
        out["max"] = canon(parse_text(a.get("max"), ctype))
    if cls in ("int", "dec"):
        s = a.get("sum")
        out["sum"] = canon(_num(s)) if s is not None else None
    if cls == "str":
        n = a.get("len")
        out["len"] = str(int(Decimal(str(n)))) if n is not None else None
    return out


# ------------------------------------------------------------------ suite
def _key_text(k: tuple[str | None, ...]) -> list[str]:
    return ["" if x is None else x for x in k]


def run_edge_suite(mk: checks.Factory, call: Call, table: TableRef, cand_rows: list[dict[str, Any]],
                   keys: Sequence[str], columns: Sequence[str], types: Mapping[str, Any], range_rows: int = 10_000,
                   sample: int = 50, max_key_diff: int = 100, max_keys: int = 10_000,
                   timeout_s: int = 300, max_reread: int = 10_000) -> SuiteResult:
    """Reference = the source through the edge (`call`), candidate = destination rows (live, keys upper case).
    `columns`/`keys`: source column names; `types`: column -> canonical type (source catalog)."""
    up = {c.upper(): c for c in [*keys, *columns]}
    keys_u = [k.upper() for k in keys]
    cols = list(columns)
    res: list[ReconResult] = []

    def tkey(r: Mapping[str, Any]) -> tuple[str | None, ...]:
        return tuple(canon(r.get(k)) for k in keys_u)

    def ekey(vals: Sequence[Any]) -> tuple[str | None, ...]:
        return tuple(canon(parse_text(v, types.get(up[k]))) for k, v in zip(keys_u, vals))

    cand_by = {tkey(r): r for r in cand_rows} if keys_u else {}
    order = sorted(cand_by, key=_sort_key)
    if keys_u and order:
        n_ranges = max(1, math.ceil(len(order) / max(1, range_rows)))
        bounds = [order[min(len(order) - 1, (i + 1) * range_rows - 1)] for i in range(n_ranges - 1)]
    else:
        bounds = []
    ranges = [{"after": _key_text(bounds[i - 1]) if i > 0 else None,
               "to": _key_text(bounds[i]) if i < len(bounds) else None} for i in range(len(bounds) + 1)]

    def range_of(k: tuple[str | None, ...]) -> int:
        sk = _sort_key(k)
        lo, hi = 0, len(bounds)
        while lo < hi:                                      # first bound >= k
            mid = (lo + hi) // 2
            if _sort_key(bounds[mid]) < sk:
                lo = mid + 1
            else:
                hi = mid
        return lo

    parts: dict[int, list[dict[str, Any]]] = {i: [] for i in range(len(ranges))}
    for k, r in (cand_by.items() if keys_u else ((None, r) for r in cand_rows)):  # type: ignore[misc]
        parts[range_of(k) if bounds else 0].append(r)

    picked: list[tuple[str | None, ...]] = []
    if order and sample > 0:
        step = max(1, len(order) // sample)
        picked = order[::step][:sample]
        if order[-1] not in picked:
            picked.append(order[-1])

    def args(checks_: list[str], rngs: list[dict[str, Any]] | None, kv: list[list[str]] | None = None) -> dict[str, Any]:
        a: dict[str, Any] = {"table": {"schema": table.schema_, "name": table.name}, "verify_id": str(uuid.uuid4()),
                             "checks": checks_, "columns": cols or None, "max_keys": max_keys, "timeout_s": timeout_s}
        if rngs is not None:
            a["ranges"] = rngs
        if kv:
            a["key_values"] = kv
        return {k: v for k, v in a.items() if v is not None}

    # 1. counts + aggregates per range (+ the sample rows with the first command)
    edge_ranges: list[dict[str, Any]] = []
    sample_rows: list[dict[str, Any]] = []
    classes: dict[str, str] = {}
    positions: list[str | None] = []
    for i in range(0, len(ranges), VERIFY_MAX_RANGES):
        first = i == 0
        kv = [_key_text(k) for k in picked] if first and picked else None
        out = call(args(["row_count", "aggregates", *(["rows"] if kv else [])],
                        ranges[i:i + VERIFY_MAX_RANGES] if bounds or keys_u else None, kv))
        classes = dict(out.get("columns") or classes)
        positions += [out.get("position_before"), out.get("position_after")]
        for r in out.get("ranges") or []:
            edge_ranges.append({**r, "index": i + int(r["index"])})
        sample_rows += out.get("rows") or []
    src_total = sum(int(r.get("rows") or 0) for r in edge_ranges)
    res += checks.row_count(mk, {"reference": src_total, "candidate": len(cand_rows)})

    bad: list[int] = []
    for er in edge_ranges:
        i = er["index"]
        part = parts.get(i, [])
        diffs: list[tuple[str | None, Any, Any]] = []
        if int(er.get("rows") or 0) != len(part):
            diffs.append((None, er.get("rows"), len(part)))
        for c, a in (er.get("agg") or {}).items():
            cls = classes.get(c, "cnt")
            e, g = edge_agg(a, cls, types.get(c)), candidate_agg(part, c.upper(), cls)
            if e != g:
                diffs.append((c, e, g))
        if diffs:
            bad.append(i)
            for c, e, g in diffs:
                res.append(mk(CheckName.COLUMN_AGGREGATES, Status.FAIL, column=c, bucket=i,
                              expected=json.dumps(e, default=str), actual=json.dumps(g, default=str),
                              detail={"range": ranges[i] if i < len(ranges) else None}))
    if not bad:
        res.append(mk(CheckName.COLUMN_AGGREGATES, Status.PASS,
                      detail={"ranges": len(ranges), "columns": len(cols), "classes": classes}))

    # sampled rows (full values)
    if picked:
        exp: dict[tuple[str | None, ...], dict[str, str | None]] = {}
        for r in sample_rows:
            exp[ekey([r.get(k) for k in keys])] = {c: canon(parse_text(r.get(c), types.get(c))) for c in cols}
        got = {k: {c: canon(cand_by[k].get(c.upper())) for c in cols} for k in picked if k in exp}
        res += checks.sampled_rows(mk, exp, got, cols)

    # 2. drill-down on the differing ranges: exact key diff + keyed re-read of their rows
    if bad and keys_u:
        missing: list[tuple[str | None, ...]] = []
        extra: list[tuple[str | None, ...]] = []
        common: list[tuple[str | None, ...]] = []
        truncated = False
        for j in range(0, len(bad), VERIFY_MAX_RANGES):
            sel = bad[j:j + VERIFY_MAX_RANGES]
            out = call(args(["keys"], [ranges[i] for i in sel]))
            for r in out.get("ranges") or []:
                i = sel[int(r["index"])]
                ek = {ekey(k) for k in r.get("keys") or []}
                ck = {tkey(x) for x in parts.get(i, [])}
                truncated |= bool(r.get("keys_truncated"))
                missing += sorted(ek - ck, key=_sort_key)
                if not r.get("keys_truncated"):
                    extra += sorted(ck - ek, key=_sort_key)
                common += sorted(ek & ck, key=_sort_key)
        for k in missing[:max_key_diff]:
            res.append(mk(CheckName.KEY_SET, Status.FAIL, key=json.dumps(list(k)), expected="row", actual="missing"))
        for k in extra[:max_key_diff]:
            res.append(mk(CheckName.KEY_SET, Status.FAIL, key=json.dumps(list(k)), expected="missing", actual="row"))
        res.append(mk(CheckName.KEY_SET, Status.FAIL if (missing or extra) else Status.PASS,
                      detail={"scope_ranges": len(bad), "missing": len(missing), "extra": len(extra),
                              "keys_truncated": truncated}))
        # value differences inside the differing ranges (bounded)
        todo = common[:max_reread]
        for j in range(0, len(todo), VERIFY_MAX_KEY_VALUES):
            part = todo[j:j + VERIFY_MAX_KEY_VALUES]
            out = call(args(["rows"], None, [_key_text(k) for k in part]))
            exp = {ekey([r.get(k) for k in keys]): {c: canon(parse_text(r.get(c), types.get(c))) for c in cols}
                   for r in out.get("rows") or []}
            got = {k: {c: canon(cand_by[k].get(c.upper())) for c in cols} for k in part if k in exp and k in cand_by}
            res += [r for r in checks.sampled_rows(mk, exp, got, cols) if r.status is not Status.PASS]
        if len(common) > max_reread:
            res.append(mk(CheckName.SAMPLED_ROWS, Status.FAIL, detail={
                "reread_capped": max_reread, "keys_in_differing_ranges": len(common)}))

    status = Status.FAIL if any(r.status is Status.FAIL for r in res) else (
        Status.ERROR if any(r.status is Status.ERROR for r in res) else Status.PASS)
    fails = [r for r in res if r.status is Status.FAIL]
    pos = [p for p in positions if p]
    return SuiteResult(status, res, {
        "reference_rows": src_total, "candidate_rows": len(cand_rows), "ranges": len(ranges), "bad_ranges": len(bad),
        "failures": len(fails), "checks": sorted({r.check.value for r in fails}), "source": "edge VERIFY_TABLE",
        "position_before": pos[0] if pos else None, "position_after": pos[-1] if pos else None})
