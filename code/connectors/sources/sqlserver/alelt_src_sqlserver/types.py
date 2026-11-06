"""SQL Server type -> canonical type, driver value -> canonical value (stdlib only).

| SQL Server (sys.types name) | Canonical |
|---|---|
| bit | BOOLEAN |
| tinyint, smallint / int / bigint | SHORT / INT / LONG (tinyint 0..255 fits SHORT) |
| decimal(p,s), numeric(p,s) | DECIMAL(p,s) |
| money / smallmoney | DECIMAL(19,4) / DECIMAL(10,4) |
| real, float(1..24) / float(25..53) | FLOAT / DOUBLE |
| char varchar nchar nvarchar (incl. max), text, ntext, uniqueidentifier | STRING |
| binary varbinary (incl. max), image, rowversion/timestamp | BINARY |
| date / time / datetime datetime2 smalldatetime | LOCALDATE / LOCALTIME / LOCALDATETIME |
| datetimeoffset | INSTANT (UTC) |
| xml | XML |
| hierarchyid, geometry, geography, sql_variant | STRING = their documented text form (`CAST(... AS nvarchar(max))`: ToString() / WKT); CDC row values are CLR serializations -> re-read by key (SRC-ms-05) |
| other CLR / alias-less user types | UNKNOWN (quarantine, R7) |
| datetime2(7) / time(7) / datetimeoffset(7) | microseconds kept; the 100 ns digit is truncated by the driver (docs/TYPE-MAP.md) |

Alias (user-defined) types are resolved to their system base type by the catalog query.
"""
from __future__ import annotations

from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

LOB_TYPES = {"text", "ntext", "image", "xml"}       # + (n)varchar(max) / varbinary(max): max_length = -1
STRING_TYPES = {"char", "varchar", "nchar", "nvarchar", "text", "ntext", "uniqueidentifier", "sysname"}
BINARY_TYPES = {"binary", "varbinary", "image", "timestamp", "rowversion"}
TEXT_FORM_TYPES = {"hierarchyid", "geometry", "geography", "sql_variant"}   # read as text by key (SRC-ms-05)


def canonical_name(engine_type: str, precision: int | None = None, scale: int | None = None) -> str:
    t = engine_type.lower()
    if t == "bit":
        return "BOOLEAN"
    if t in ("tinyint", "smallint"):
        return "SHORT"
    if t == "int":
        return "INT"
    if t == "bigint":
        return "LONG"
    if t in ("decimal", "numeric", "money", "smallmoney"):
        return "DECIMAL"
    if t == "real":
        return "FLOAT"
    if t == "float":
        return "FLOAT" if precision is not None and precision <= 24 else "DOUBLE"
    if t in STRING_TYPES:
        return "STRING"
    if t in BINARY_TYPES:
        return "BINARY"
    if t == "date":
        return "LOCALDATE"
    if t == "time":
        return "LOCALTIME"
    if t in ("datetime", "datetime2", "smalldatetime"):
        return "LOCALDATETIME"
    if t == "datetimeoffset":
        return "INSTANT"
    if t == "xml":
        return "XML"
    if t in TEXT_FORM_TYPES:
        return "STRING"
    return "UNKNOWN"


def decimal_shape(engine_type: str, precision: int | None, scale: int | None) -> tuple[int | None, int | None]:
    t = engine_type.lower()
    if t == "money":
        return 19, 4
    if t == "smallmoney":
        return 10, 4
    return precision, scale


def is_lob(engine_type: str, max_length: int | None) -> bool:
    t = engine_type.lower()
    return t in LOB_TYPES or (t in ("varchar", "nvarchar", "varbinary") and max_length == -1)


class ConvertError(ValueError):
    pass


def _quantize(d: Decimal, scale: int | None) -> Decimal:
    if scale is not None and scale > 0 and d.is_finite():
        try:
            return d.quantize(Decimal(1).scaleb(-scale))
        except InvalidOperation:
            return d
    return d


def convert(value: Any, canonical: str, scale: int | None = None) -> Any:
    """pytds / pyodbc value -> canonical Python value (REMOTE.md S7)."""
    if value is None:
        return None
    if canonical == "BOOLEAN":
        return bool(value)
    if canonical in ("SHORT", "INT", "LONG"):
        return int(value)
    if canonical == "DECIMAL":
        d = value if isinstance(value, Decimal) else Decimal(str(value))
        return _quantize(d, scale)
    if canonical in ("FLOAT", "DOUBLE"):
        return float(value)
    if canonical == "STRING":
        if isinstance(value, UUID):
            return str(value).upper()       # SQL Server prints uniqueidentifier upper case
        if isinstance(value, (bytes, bytearray)):
            raise ConvertError("binary value for a character column")
        return str(value)
    if canonical == "BINARY":
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        raise ConvertError(f"{type(value).__name__} for a binary column")
    if canonical == "LOCALDATE":
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        raise ConvertError(f"{type(value).__name__} for a date column")
    if canonical == "LOCALTIME":
        if isinstance(value, time):
            return value.replace(tzinfo=None)
        raise ConvertError(f"{type(value).__name__} for a time column")
    if canonical == "LOCALDATETIME":
        if isinstance(value, datetime):
            return value.replace(tzinfo=None) if value.tzinfo is None else value.astimezone(timezone.utc).replace(tzinfo=None)
        raise ConvertError(f"{type(value).__name__} for a datetime column")
    if canonical == "INSTANT":
        if isinstance(value, datetime):
            if value.tzinfo is None:
                raise ConvertError("datetimeoffset without offset")
            return value.astimezone(timezone.utc)
        raise ConvertError(f"{type(value).__name__} for a datetimeoffset column")
    if canonical in ("XML", "JSON"):
        return value if isinstance(value, str) else str(value)
    raise ConvertError(f"no conversion for canonical type {canonical}")
