"""pyarrow helpers shared by the lake plugins (delta, iceberg). Imported only by plugins that need pyarrow."""
from __future__ import annotations

import json
from datetime import date, datetime, time as dtime, timezone
from decimal import Decimal, Inexact, localcontext
from typing import Any

import pyarrow as pa

from alelt_common.models import CanonicalType


def exact_decimal(v: Any, scale: int) -> Decimal:
    """Decimal at the column scale with no silent rounding: 38 digits exceed the default context (28), and a value
    with more fractional digits than the column holds raises instead of being rounded (XC-31)."""
    with localcontext() as ctx:
        ctx.prec = 80
        ctx.traps[Inexact] = True
        return Decimal(v).quantize(Decimal(1).scaleb(-scale))

_NAMES = {
    "boolean": pa.bool_(), "byte": pa.int8(), "short": pa.int16(), "integer": pa.int32(), "int": pa.int32(),
    "long": pa.int64(), "float": pa.float32(), "double": pa.float64(), "date": pa.date32(), "string": pa.string(),
    "binary": pa.binary(), "time": pa.time64("us"), "timestamp_ntz": pa.timestamp("us"),
}


def arrow_type(t: str, timestamp_is_tz: bool = True) -> pa.DataType:
    """Delta ('timestamp' = UTC instant, 'timestamp_ntz') and Iceberg ('timestamp' = local, 'timestamptz') names."""
    t = t.strip().lower()
    if t.startswith("decimal"):
        p, s = (int(x) for x in t[t.index("(") + 1:t.index(")")].split(","))
        return pa.decimal128(p, s)
    if t == "timestamptz":
        return pa.timestamp("us", tz="UTC")
    if t == "timestamp":
        return pa.timestamp("us", tz="UTC") if timestamp_is_tz else pa.timestamp("us")
    return _NAMES[t]


def to_arrow_value(v: Any, ct: CanonicalType | None, at: pa.DataType) -> Any:
    if v is None:
        return None
    if ct is CanonicalType.LOCALTIME and isinstance(v, dtime) and pa.types.is_string(at):
        return v.isoformat()
    if ct is CanonicalType.JSON and not isinstance(v, str):
        return json.dumps(v, separators=(",", ":"), default=str)
    if pa.types.is_decimal(at):
        return exact_decimal(v, at.scale)
    if pa.types.is_timestamp(at) and isinstance(v, str):
        v = parse_timestamp_text(v)
    if pa.types.is_timestamp(at) and isinstance(v, datetime):
        if at.tz and v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        if not at.tz and v.tzinfo is not None:
            return v.astimezone(timezone.utc).replace(tzinfo=None)
    return v


class SubMicrosecondError(ValueError):
    """A timestamp with non-zero digits below the microsecond: the lake formats store microseconds only."""


def parse_timestamp_text(v: str) -> datetime:
    """ISO timestamp text (sources with more than 6 fractional digits deliver text, e.g. Db2 TIMESTAMP(12)) ->
    datetime. Digits below the microsecond must be zero: never truncated silently (DST-iceberg-06)."""
    t = v.strip().replace("T", " ", 1)
    head, dot, frac = t.partition(".")
    tail = ""
    if dot:
        digits = ""
        for i, ch in enumerate(frac):
            if not ch.isdigit():
                tail = frac[i:]
                break
            digits += ch
        if len(digits) > 6:
            if digits[6:].strip("0"):
                raise SubMicrosecondError(f"timestamp {v!r} has sub-microsecond digits; the destination stores "
                                          "microseconds - refused (map the column to text upstream to keep them)")
            digits = digits[:6]
        t = head + ("." + digits if digits else "") + tail
    return datetime.fromisoformat(t)


def from_arrow_value(v: Any, ct: CanonicalType | None) -> Any:
    if v is None:
        return None
    if ct is CanonicalType.LOCALTIME and isinstance(v, str):
        return dtime.fromisoformat(v)
    return v


def coerce_key(v: Any, at: pa.DataType) -> Any:
    """Canonical key (alelt_loader._canon form) -> python value of the key column's arrow type."""
    if v is None:
        return None
    if pa.types.is_integer(at):
        return int(v)
    if pa.types.is_decimal(at):
        return exact_decimal(str(v), at.scale)
    if pa.types.is_date(at) and isinstance(v, str):
        return date.fromisoformat(v)
    if pa.types.is_timestamp(at) and isinstance(v, str):
        d = datetime.fromisoformat(v)
        return d.replace(tzinfo=timezone.utc) if at.tz and d.tzinfo is None else d
    if pa.types.is_binary(at) and isinstance(v, str):
        return bytes.fromhex(v)
    return v
