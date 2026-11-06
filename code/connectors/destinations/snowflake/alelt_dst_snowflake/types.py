"""Canonical -> Snowflake type mapping (fallback when no golden DDL exists, R8)."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from alelt_common.models import CanonicalType, ColumnInfo


class UnmappedTypeError(ValueError):
    """Column has no destination type (UNKNOWN) => quarantine + alert (R7)."""


DEFAULT_MAP: dict[str, Any] = {
    "types": {
        "BOOLEAN": "BOOLEAN", "SHORT": "NUMBER(5,0)", "INT": "NUMBER(10,0)", "LONG": "NUMBER(19,0)",
        "DECIMAL": "NUMBER({p},{s})", "FLOAT": "FLOAT", "DOUBLE": "FLOAT", "LOCALDATE": "DATE",
        "LOCALTIME": "TIME(9)", "LOCALDATETIME": "TIMESTAMP_NTZ(9)", "INSTANT": "TIMESTAMP_TZ(9)",
        "STRING": "VARCHAR({n})", "BINARY": "BINARY", "JSON": "VARIANT", "XML": "VARCHAR", "UNKNOWN": None,
    },
    "defaults": {"STRING_without_length": "VARCHAR(16777216)", "DECIMAL_without_precision": "NUMBER(38,0)"},
}


def load_type_map(path: str | Path | None) -> dict[str, Any]:
    """Load config/type_maps/canonical_to_snowflake.yaml; built-in default when path is None."""
    if path is None:
        return DEFAULT_MAP
    import yaml

    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def snowflake_type(col: ColumnInfo, type_map: dict[str, Any] = DEFAULT_MAP) -> str:
    """Physical Snowflake type for a canonical column. Raises UnmappedTypeError for UNKNOWN."""
    tmpl = type_map["types"].get(col.canonical_type.value)
    if tmpl is None:
        raise UnmappedTypeError(f"{col.name}: canonical type {col.canonical_type.value} has no Snowflake mapping")
    defaults = type_map.get("defaults", {})
    if "{p}" in tmpl:
        if not col.length:
            return str(defaults.get("DECIMAL_without_precision", "NUMBER(38,0)"))
        return tmpl.format(p=min(col.length, 38), s=col.scale or 0)
    if "{n}" in tmpl:
        if col.canonical_type == CanonicalType.STRING and col.length:
            return tmpl.format(n=min(col.length, 16777216))
        return str(defaults.get("STRING_without_length", "VARCHAR(16777216)"))
    return str(tmpl)
