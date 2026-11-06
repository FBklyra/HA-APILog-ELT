"""Oracle type -> canonical type, and literal / driver value -> canonical value (stdlib only).

| Oracle (ALL_TAB_COLUMNS.DATA_TYPE) | Canonical |
|---|---|
| NUMBER(p,0) p<=4 / p<=9 / p<=18 | SHORT / INT / LONG |
| NUMBER(p,0) p>18, NUMBER(p,s>0), NUMBER(*,0) / INTEGER (precision NULL, scale 0) | DECIMAL(p,s) (p defaults to 38) |
| NUMBER (no precision, no scale), FLOAT(b) | DECIMAL(38, None) - floating decimal kept exact as `Decimal` |
| BINARY_FLOAT / BINARY_DOUBLE | FLOAT / DOUBLE |
| VARCHAR2 NVARCHAR2 CHAR NCHAR CLOB NCLOB LONG ROWID UROWID | STRING |
| RAW LONG RAW BLOB | BINARY |
| DATE, TIMESTAMP(n) | LOCALDATETIME (Oracle DATE carries a time of day) |
| TIMESTAMP(n) WITH TIME ZONE / WITH LOCAL TIME ZONE | INSTANT (UTC) |
| INTERVAL YEAR TO MONTH / DAY TO SECOND | STRING (Oracle text form, e.g. `+01-02`, `+02 03:04:05.000000`) |
| XMLTYPE | XML ; JSON | JSON ; BOOLEAN (23ai) | BOOLEAN |
| BFILE, object / spatial / user-defined types, anything else | UNKNOWN (quarantine, R7) |

The golden spec overrides this mapping per column (R8).
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from .redosql import Lit

STRING_TYPES = {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "LONG", "ROWID", "UROWID", "VARCHAR"}
BINARY_TYPES = {"RAW", "LONG RAW", "BLOB"}
LOB_TYPES = {"CLOB", "NCLOB", "BLOB"}


def base_type(engine_type: str) -> str:
    """'TIMESTAMP(6) WITH TIME ZONE' -> 'TIMESTAMP WITH TIME ZONE'; 'INTERVAL DAY(2) TO SECOND(6)' -> 'INTERVAL DAY TO SECOND'."""
    return re.sub(r"\(\s*\d+\s*(,\s*\d+\s*)?\)", "", engine_type.upper()).replace("  ", " ").strip()


def canonical_name(engine_type: str, precision: int | None, scale: int | None) -> str:
    t = base_type(engine_type)
    if t == "NUMBER":
        if scale == 0:
            if precision is None:
                return "DECIMAL"
            if precision <= 4:
                return "SHORT"
            if precision <= 9:
                return "INT"
            if precision <= 18:
                return "LONG"
        return "DECIMAL"
    if t == "FLOAT":
        return "DECIMAL"
    if t == "BINARY_FLOAT":
        return "FLOAT"
    if t == "BINARY_DOUBLE":
        return "DOUBLE"
    if t in STRING_TYPES or t.startswith("INTERVAL"):
        return "STRING"
    if t in BINARY_TYPES:
        return "BINARY"
    if t == "DATE" or t == "TIMESTAMP":
        return "LOCALDATETIME"
    if t in ("TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"):
        return "INSTANT"
    if t in ("XMLTYPE", "SYS.XMLTYPE"):
        return "XML"
    if t == "JSON":
        return "JSON"
    if t == "BOOLEAN":
        return "BOOLEAN"
    return "UNKNOWN"


def decimal_shape(engine_type: str, precision: int | None, scale: int | None) -> tuple[int | None, int | None]:
    """(length, scale) stored on ColumnInfo for DECIMAL columns."""
    t = base_type(engine_type)
    if t == "NUMBER":
        if precision is None and scale is None:
            return 38, None
        return precision or 38, scale
    if t == "FLOAT":
        return 38, None
    return precision, scale


class ConvertError(ValueError):
    pass


def _dec(text: str) -> Decimal:
    t = str(text).strip()
    try:
        return Decimal(t)
    except InvalidOperation as e:
        raise ConvertError(f"not a number: {t[:40]!r}") from e


def _float(text: str) -> float:
    t = str(text).strip().upper()
    if t in ("NAN", "BINARY_FLOAT_NAN", "BINARY_DOUBLE_NAN"):
        return float("nan")
    if t in ("INF", "+INF", "INFINITY", "BINARY_FLOAT_INFINITY", "BINARY_DOUBLE_INFINITY"):
        return float("inf")
    if t in ("-INF", "-INFINITY", "-BINARY_FLOAT_INFINITY", "-BINARY_DOUBLE_INFINITY"):
        return float("-inf")
    try:
        return float(t)
    except ValueError as e:
        raise ConvertError(f"not a float: {t[:40]!r}") from e


def _utc(dt: datetime, assume_utc: bool) -> datetime:
    if dt.tzinfo is None:
        if not assume_utc:
            raise ConvertError("timestamp without time zone for an INSTANT column")
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _quantize(d: Decimal, scale: int | None) -> Decimal:
    if scale is not None and scale > 0 and d.is_finite():
        try:
            return d.quantize(Decimal(1).scaleb(-scale))
        except InvalidOperation:
            return d
    return d


def convert_lit(lit: Lit, canonical: str, engine_type: str, scale: int | None) -> Any:
    """Literal parsed from SQL_REDO/UNDO -> canonical Python value."""
    if lit.kind == "null":
        return None
    k, v = lit.kind, lit.value
    if k == "empty_lob":
        return b"" if canonical == "BINARY" else ""
    if canonical in ("SHORT", "INT", "LONG"):
        d = _dec(v)
        if d != d.to_integral_value():
            raise ConvertError(f"fraction for integer column: {v!r}")
        return int(d)
    if canonical == "DECIMAL":
        return _quantize(_dec(v), scale)
    if canonical in ("FLOAT", "DOUBLE"):
        return _float(v)
    if canonical == "BOOLEAN":
        t = str(v).strip().upper()
        if t in ("1", "TRUE", "T", "Y", "YES"):
            return True
        if t in ("0", "FALSE", "F", "N", "NO"):
            return False
        raise ConvertError(f"not a boolean: {v!r}")
    if canonical == "BINARY":
        if k == "raw":
            return bytes(v)
        if k == "str":                 # some LogMiner versions print RAW as a hex string literal
            try:
                return bytes.fromhex(v)
            except ValueError as e:
                raise ConvertError("RAW literal is not hex") from e
        raise ConvertError(f"{k} literal for BINARY column")
    if canonical == "LOCALDATETIME":
        if isinstance(v, datetime):
            return v.replace(tzinfo=None) if v.tzinfo is None else v.astimezone(timezone.utc).replace(tzinfo=None)
        raise ConvertError(f"{k} literal for DATE/TIMESTAMP column")
    if canonical == "INSTANT":
        if isinstance(v, datetime):
            # WITH LOCAL TIME ZONE values are printed in the session time zone, which the miner sets to UTC
            return _utc(v, assume_utc=True)
        raise ConvertError(f"{k} literal for TIMESTAMP WITH TIME ZONE column")
    if canonical in ("STRING", "XML", "JSON"):
        if k == "raw":
            raise ConvertError("RAW literal for a character column")
        return v if isinstance(v, str) else str(v)
    raise ConvertError(f"no conversion for canonical type {canonical}")


def convert_driver(value: Any, canonical: str, scale: int | None) -> Any:
    """python-oracledb fetched value (snapshot) -> canonical Python value. NUMBER is fetched as Decimal."""
    if value is None:
        return None
    if hasattr(value, "read"):                        # LOB locator
        value = value.read()
    if canonical in ("SHORT", "INT", "LONG"):
        return int(value)
    if canonical == "DECIMAL":
        return _quantize(value if isinstance(value, Decimal) else _dec(str(value)), scale)
    if canonical in ("FLOAT", "DOUBLE"):
        return float(value)
    if canonical == "BOOLEAN":
        return bool(value)
    if canonical == "BINARY":
        return bytes(value)
    if canonical == "LOCALDATETIME":
        return value if value.tzinfo is None else value.astimezone(timezone.utc).replace(tzinfo=None)
    if canonical == "INSTANT":
        return _utc(value, assume_utc=True)
    if canonical in ("STRING",):
        return value if isinstance(value, str) else str(value)
    if canonical in ("XML", "JSON"):
        if isinstance(value, (dict, list)):
            import json
            # key order as stored (the CDC path keeps the document text): no re-sorting (SRC-ora-10)
            return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
        return str(value)
    return value
