"""Tagged JSON form of canonical values (corpus files, diagnostics). Stdlib only.

None/bool/int/float/str as JSON; Decimal {"$dec"}, date {"$date"}, time {"$time"},
datetime {"$ts"}, bytes {"$hex"}.
"""
from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any


def to_json(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, Decimal):
        return {"$dec": str(v)}
    if isinstance(v, datetime):
        return {"$ts": v.isoformat()}
    if isinstance(v, date):
        return {"$date": v.isoformat()}
    if isinstance(v, time):
        return {"$time": v.isoformat()}
    if isinstance(v, (bytes, bytearray, memoryview)):
        return {"$hex": bytes(v).hex()}
    if isinstance(v, dict):
        return {k: to_json(x) for k, x in v.items()}
    raise TypeError(f"no JSON form for {type(v).__name__}")


def from_json(v: Any) -> Any:
    if isinstance(v, dict) and len(v) == 1:
        (k, x), = v.items()
        if k == "$dec":
            return Decimal(x)
        if k == "$ts":
            return datetime.fromisoformat(x)
        if k == "$date":
            return date.fromisoformat(x)
        if k == "$time":
            return time.fromisoformat(x)
        if k == "$hex":
            return bytes.fromhex(x)
    if isinstance(v, dict):
        return {k: from_json(x) for k, x in v.items()}
    return v


def row_to_json(row: dict[str, Any] | None) -> dict[str, Any] | None:
    return None if row is None else {k: to_json(v) for k, v in row.items()}
