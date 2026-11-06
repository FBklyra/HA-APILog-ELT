"""PostgreSQL type -> canonical type, and text-format value parsing (stdlib only).

Engine type names in the catalog: the pg_type name of the base type (`int4`, `numeric`, `timestamptz`, ...);
arrays `<elem>[]` (e.g. `int4[]`); enums `enum:<typname>`; domains resolve to their base type; composites
`composite:<typname>`, ranges `range:<typname>`.

Values arrive in the text output format of each type; the replication / snapshot sessions run with
DateStyle=ISO, TimeZone=UTC, IntervalStyle=iso_8601, extra_float_digits=3, bytea_output=hex.
"""
from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from alelt_common.canonical_jsonl import Undecodable

# built-in type OIDs (pg_type.dat, stable across releases)
OID_NAME: dict[int, str] = {
    16: "bool", 17: "bytea", 18: "char", 19: "name", 20: "int8", 21: "int2", 23: "int4", 25: "text", 26: "oid",
    114: "json", 142: "xml", 700: "float4", 701: "float8", 790: "money", 829: "macaddr", 869: "inet", 650: "cidr",
    774: "macaddr8", 1042: "bpchar", 1043: "varchar", 1082: "date", 1083: "time", 1114: "timestamp",
    1184: "timestamptz", 1186: "interval", 1266: "timetz", 1560: "bit", 1562: "varbit", 1700: "numeric",
    2950: "uuid", 3802: "jsonb", 3614: "tsvector", 3615: "tsquery", 4072: "jsonpath", 600: "point",
    28: "xid", 5069: "xid8", 3220: "pg_lsn",
    601: "lseg", 602: "path", 603: "box", 604: "polygon", 628: "line", 718: "circle",
}
ARRAY_ELEM: dict[int, int] = {
    1000: 16, 1001: 17, 1002: 18, 1003: 19, 1016: 20, 1005: 21, 1007: 23, 1009: 25, 1028: 26, 199: 114,
    143: 142, 1021: 700, 1022: 701, 791: 790, 1040: 829, 1041: 869, 651: 650, 1014: 1042, 1015: 1043,
    1182: 1082, 1183: 1083, 1115: 1114, 1185: 1184, 1187: 1186, 1270: 1266, 1561: 1560, 1563: 1562,
    1231: 1700, 2951: 2950, 3807: 3802,
}

CANONICAL: dict[str, str] = {
    "bool": "BOOLEAN", "int2": "SHORT", "int4": "INT", "int8": "LONG", "oid": "LONG", "xid": "LONG",
    "xid8": "DECIMAL", "numeric": "DECIMAL", "float4": "FLOAT", "float8": "DOUBLE", "money": "STRING",
    "text": "STRING", "varchar": "STRING", "bpchar": "STRING", "char": "STRING", "name": "STRING",
    "bytea": "BINARY", "date": "LOCALDATE", "time": "LOCALTIME", "timetz": "STRING",
    "timestamp": "LOCALDATETIME", "timestamptz": "INSTANT", "interval": "STRING",
    "json": "JSON", "jsonb": "JSON", "jsonpath": "STRING", "xml": "XML", "uuid": "STRING",
    "inet": "STRING", "cidr": "STRING", "macaddr": "STRING", "macaddr8": "STRING", "bit": "STRING",
    "varbit": "STRING", "tsvector": "STRING", "tsquery": "STRING", "point": "STRING", "pg_lsn": "STRING",
    # geometric types: their documented text output form (manual 8.8), lossless
    "lseg": "STRING", "path": "STRING", "box": "STRING", "polygon": "STRING", "line": "STRING", "circle": "STRING",
}


def canonical_name(engine_type: str) -> str:
    t = engine_type.strip().lower()
    if t.endswith("[]"):
        return "JSON"
    if t.startswith(("enum:", "composite:", "range:", "multirange:", "ext:")):
        return "STRING"
    return CANONICAL.get(t, "UNKNOWN")


def name_of(oid: int) -> str | None:
    if oid in OID_NAME:
        return OID_NAME[oid]
    if oid in ARRAY_ELEM:
        return f"{OID_NAME[ARRAY_ELEM[oid]]}[]"
    return None


def typmod_info(engine_type: str, typmod: int) -> tuple[int | None, int | None]:
    """(length/precision, scale) from atttypmod."""
    t = engine_type.lower()
    if typmod < 0:
        return None, None
    if t == "numeric":
        v = typmod - 4
        return (v >> 16) & 0xFFFF, v & 0xFFFF
    if t in ("varchar", "bpchar"):
        return typmod - 4, None
    if t in ("timestamp", "timestamptz", "time", "timetz"):
        return None, typmod
    if t in ("bit", "varbit"):
        return typmod, None
    return None, None


# ------------------------------------------------------------------ text -> canonical value
def _bool(s: str) -> bool:
    if s in ("t", "true"):
        return True
    if s in ("f", "false"):
        return False
    raise ValueError(s)


def _bytea(s: str) -> bytes:
    if s.startswith("\\x"):
        return bytes.fromhex(s[2:])
    out = bytearray()                       # escape format (bytea_output=escape)
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == "\\":
            if s[i + 1] == "\\":
                out.append(0x5C)
                i += 2
            else:
                out.append(int(s[i + 1:i + 4], 8))
                i += 4
        else:
            out.extend(ch.encode("utf-8"))
            i += 1
    return bytes(out)


def _float(s: str) -> float:
    return float({"Infinity": "inf", "-Infinity": "-inf", "NaN": "nan"}.get(s, s))


def _numeric(s: str) -> Decimal:
    """SRC-pg-08: numeric 'NaN' / 'Infinity' / '-Infinity' have no DECIMAL form in any destination: the row is
    quarantined with this reason (R7, docs/TYPE-MAP.md) instead of failing later at the destination."""
    if s in ("NaN", "Infinity", "-Infinity"):
        raise ValueError(f"numeric {s} has no DECIMAL form")
    try:
        return Decimal(s)
    except InvalidOperation:
        raise ValueError(s) from None


def _tz(s: str) -> str:
    """'+00' / '+05:30' / '-03:00:00' -> ISO offset accepted by fromisoformat."""
    for i in range(len(s) - 1, 9, -1):
        if s[i] in "+-":
            off = s[i + 1:]
            if ":" not in off and len(off) == 2:
                off += ":00"
            return s[:i] + s[i] + off
    return s


def _timestamptz(s: str) -> datetime:
    if s in ("infinity", "-infinity") or s.endswith(" BC"):
        raise ValueError(s)
    d = datetime.fromisoformat(_tz(s.replace(" ", "T", 1)))
    if d.tzinfo is None:
        raise ValueError(f"no offset in {s!r}")
    return d.astimezone(timezone.utc)


def _timestamp(s: str) -> datetime:
    if s in ("infinity", "-infinity") or s.endswith(" BC"):
        raise ValueError(s)
    return datetime.fromisoformat(s.replace(" ", "T", 1))


def _date(s: str) -> date:
    if s in ("infinity", "-infinity") or s.endswith(" BC"):
        raise ValueError(s)
    return date.fromisoformat(s)


def _time(s: str) -> time:
    if s.startswith("24:"):       # SRC-pg-08: valid PG end of day -> the largest LOCALTIME (docs/TYPE-MAP.md)
        return time.max
    return time.fromisoformat(s)


SCALAR: dict[str, Callable[[str], Any]] = {
    "bool": _bool, "int2": int, "int4": int, "int8": int, "oid": int, "xid": int, "xid8": Decimal,
    "numeric": _numeric, "float4": _float, "float8": _float, "bytea": _bytea, "date": _date, "time": _time,
    "timestamp": _timestamp, "timestamptz": _timestamptz,
}


# ------------------------------------------------------------------ arrays -> JSON text
def parse_array(s: str) -> Any:
    """PostgreSQL array literal -> nested Python lists of str | None (dimension decoration ignored)."""
    if s.startswith("["):                    # '[1:3]={...}'
        s = s[s.index("=") + 1:]
    pos = 0

    def parse() -> list[Any]:
        nonlocal pos
        if s[pos] != "{":
            raise ValueError("array must start with '{'")
        pos += 1
        out: list[Any] = []
        if s[pos] == "}":
            pos += 1
            return out
        while True:
            ch = s[pos]
            if ch == "{":
                out.append(parse())
            elif ch == '"':
                pos += 1
                buf = []
                while s[pos] != '"':
                    if s[pos] == "\\":
                        pos += 1
                    buf.append(s[pos])
                    pos += 1
                pos += 1
                out.append("".join(buf))
            else:
                start = pos
                while s[pos] not in ",}":
                    pos += 1
                tok = s[start:pos].strip()
                out.append(None if tok == "NULL" else tok)
            if s[pos] == ",":
                pos += 1
                continue
            if s[pos] == "}":
                pos += 1
                return out
            raise ValueError(f"bad array literal near {pos}")

    v = parse()
    if pos != len(s):
        raise ValueError("trailing characters after array")
    return v


def _json_elem(elem: str, v: str | None) -> Any:
    if v is None:
        return None
    if elem in ("int2", "int4", "int8", "oid"):
        return int(v)
    if elem in ("float4", "float8"):
        f = _float(v)
        return f if f == f and f not in (float("inf"), float("-inf")) else v
    if elem == "numeric":                     # exact: integers as JSON numbers, others as strings
        _numeric(v)
        return int(v) if v.lstrip("-").isdigit() else v
    if elem == "bool":
        return _bool(v)
    if elem in ("json", "jsonb"):
        return json.loads(v)
    return v


def _array(elem: str, s: str) -> str:
    def walk(x: Any) -> Any:
        return [walk(i) for i in x] if isinstance(x, list) else _json_elem(elem, x)
    return json.dumps(walk(parse_array(s)), ensure_ascii=False, separators=(",", ":"))


def parse_text(engine_type: str, s: str) -> Any:
    """Text output -> canonical Python value. Unconvertible -> Undecodable(text) (quarantined at decode, R7)."""
    t = engine_type.lower()
    try:
        if t.endswith("[]"):
            return _array(t[:-2], s)
        fn = SCALAR.get(t)
        if fn is not None:
            return fn(s)
        if canonical_name(t) == "UNKNOWN":
            return Undecodable(f"{engine_type}:{s}")
        return s                              # text-like types, JSON text, uuid, enums, ...
    except (ValueError, IndexError, json.JSONDecodeError) as e:
        return Undecodable(f"{engine_type}:{s} ({e})")
