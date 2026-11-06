"""Db2 LUW type -> canonical type name, and fixed-section widths in a row image (stdlib only).

Names returned are alelt_common.models.CanonicalType values (kept as str so this module has no pydantic dependency).
"""
from __future__ import annotations

# catalog TYPENAME (SYSCAT.COLUMNS, built-in) -> canonical
_MAP: dict[str, str] = {
    "SMALLINT": "SHORT",
    "INTEGER": "INT",
    "BIGINT": "LONG",
    "DECIMAL": "DECIMAL",
    "REAL": "FLOAT",
    "DOUBLE": "DOUBLE",
    "DATE": "LOCALDATE",
    "TIME": "LOCALTIME",
    "TIMESTAMP": "LOCALDATETIME",
    "BOOLEAN": "BOOLEAN",
    "CHARACTER": "STRING",
    "VARCHAR": "STRING",
    "GRAPHIC": "STRING",
    "VARGRAPHIC": "STRING",
    "CLOB": "STRING",
    "DBCLOB": "STRING",
    "BINARY": "BINARY",
    "VARBINARY": "BINARY",
    "BLOB": "BINARY",
    "XML": "XML",
    "LONG VARCHAR": "STRING",
    "LONG VARGRAPHIC": "STRING",
}
ALIASES = {"CHAR": "CHARACTER", "INT": "INTEGER", "FLOAT": "DOUBLE", "NUMERIC": "DECIMAL",
           "DEC": "DECIMAL", "CHARACTER VARYING": "VARCHAR"}

VAR_TYPES = frozenset({"VARCHAR", "VARGRAPHIC", "VARBINARY"})
LOB_TYPES = frozenset({"CLOB", "BLOB", "DBCLOB"})
# Types we never decode from a row image: column -> quarantine of the event (R7).
UNSUPPORTED: frozenset[str] = frozenset()
LONG_TYPES = frozenset({"LONG VARCHAR", "LONG VARGRAPHIC"})   # 4 B descriptor in the row, data by long field manager
# Types whose row-image encoding IBM does not document: the column is skipped (width known), reported as
# `value_unavailable:<COL>` and the row is re-read by key (DESIGN.md §5a).
REREAD_TYPES = frozenset({"DECFLOAT", "XML"}) | LONG_TYPES


def norm(engine_type: str) -> str:
    t = engine_type.strip().upper()
    return ALIASES.get(t, t)


def canonical_name(engine_type: str, codepage: int | None = None) -> str:
    """Canonical type name; character types with codepage 0 (FOR BIT DATA) are BINARY."""
    t = norm(engine_type)
    if t in UNSUPPORTED or t == "DECFLOAT":
        return "UNKNOWN"
    c = _MAP.get(t, "UNKNOWN")
    if c == "STRING" and codepage == 0 and t in ("CHARACTER", "VARCHAR", "CLOB"):
        return "BINARY"
    return c


def fixed_width(engine_type: str, length: int, scale: int) -> int | None:
    """Bytes the column occupies in the fixed section (without the trailing null byte).
    None = width unknown (cannot even skip the column)."""
    t = norm(engine_type)
    if t == "SMALLINT":
        return 2
    if t == "INTEGER":
        return 4
    if t in ("BIGINT", "DOUBLE"):
        return 8
    if t == "REAL":
        return 4
    if t == "BOOLEAN":
        return 1
    if t == "DECIMAL":
        return length // 2 + 1
    if t == "DATE":
        return 4
    if t == "TIME":
        return 3
    if t == "TIMESTAMP":                       # scale = fractional precision 0..12
        return 7 + (scale + 1) // 2
    if t in ("CHARACTER", "BINARY"):
        return length
    if t == "GRAPHIC":
        return 2 * length
    if t == "DECFLOAT":
        return 8 if length == 8 else 16
    if t in VAR_TYPES or t in LOB_TYPES or t in ("XML", "LONG VARCHAR", "LONG VARGRAPHIC"):
        return 4                               # u16 offset + u16 length into the record
    return None
