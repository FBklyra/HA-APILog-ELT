"""Type widening on schema drift (DST-kit-04 / R4): decide, engine-neutrally, whether an existing physical column
type must be widened to hold the type the current catalog maps to.

`widen_to(existing, target)` returns the physical type to ALTER the column to, or None (already wide enough, not
comparable, or a narrowing that is never applied). Families: strings (varchar / character varying / nvarchar /
varchar2 / String / text / clob ...), exact decimals (decimal / numeric / number), binaries (varbinary / raw) and
integers (smallint < int < bigint). A decimal widens to keep BOTH the integer digits and the scale of either side.
"""
from __future__ import annotations

import re

_STR = ("varchar", "character varying", "nvarchar", "varchar2", "nvarchar2", "char", "character", "nchar", "string",
        "text", "clob", "longtext", "mediumtext")
_UNBOUNDED_STR = ("text", "clob", "longtext", "mediumtext", "string", "varchar(max)", "nvarchar(max)", "varchar")
_DEC = ("decimal", "numeric", "number")
_BIN = ("varbinary", "binary", "raw", "bytea", "blob", "varbyte")
_INT_RANK = {"smallint": 1, "int2": 1, "int16": 1, "integer": 2, "int": 2, "int4": 2, "int32": 2,
             "bigint": 3, "int8": 3, "int64": 3, "long": 3}
_NUM = re.compile(r"\(\s*(max|\d+)\s*(?:,\s*(-?\d+))?\s*(?:char|byte)?\s*\)")


def _base(t: str) -> str:
    return re.sub(r"\(.*$", "", t.strip().lower()).replace(" not null", "").strip()


def _args(t: str) -> tuple[str | None, int | None]:
    m = _NUM.search(t.lower())
    if not m:
        return None, None
    return m.group(1), (int(m.group(2)) if m.group(2) is not None else None)


def family(t: str) -> str | None:
    b = _base(t)
    if b.startswith(_DEC):
        return "dec"
    if b in _INT_RANK:
        return "int"
    if b.startswith(_BIN):
        return "bin"
    if b.startswith(_STR):
        return "str"
    return None


def _length(t: str) -> float:
    """Character/byte capacity; unbounded = inf."""
    n, _ = _args(t)
    if n == "max" or (n is None and _base(t) in _UNBOUNDED_STR + ("bytea", "blob", "varbyte")):
        return float("inf")
    return float(n) if n is not None else 1.0


def widen_to(existing: str, target: str, max_precision: int = 38) -> str | None:
    fe, ft = family(existing), family(target)
    if fe is None or fe != ft:
        return None
    if fe in ("str", "bin"):
        return target if _length(target) > _length(existing) else None
    if fe == "int":
        return target if _INT_RANK[_base(target)] > _INT_RANK[_base(existing)] else None
    pe, se = _args(existing)
    pt, st = _args(target)
    if pe is None or pt is None or pe == "max" or pt == "max":
        return None
    pe_i, pt_i, se_i, st_i = int(pe), int(pt), se or 0, st or 0
    scale = max(se_i, st_i)
    digits = max(pe_i - se_i, pt_i - st_i)
    p = min(digits + scale, max_precision)
    if p <= pe_i and scale <= se_i:
        return None
    return _NUM.sub(f"({p},{scale})", target, count=1)
