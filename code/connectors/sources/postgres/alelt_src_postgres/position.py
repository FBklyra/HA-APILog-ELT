"""PostgreSQL LSN positions. Text form: '<hi hex>/<lo hex>' upper case, no padding (as pg_lsn prints it)."""
from __future__ import annotations

import re

_RX = re.compile(r"^\s*([0-9A-Fa-f]{1,8})/([0-9A-Fa-f]{1,8})\s*$")


def parse_lsn(text: str) -> int:
    m = _RX.match(text)
    if not m:
        raise ValueError(f"invalid PostgreSQL LSN {text!r}; expected 'X/Y' (hex)")
    return (int(m.group(1), 16) << 32) | int(m.group(2), 16)


def format_lsn(v: int) -> str:
    if not 0 <= v < 1 << 64:
        raise ValueError("LSN out of range")
    return f"{v >> 32:X}/{v & 0xFFFFFFFF:X}"


def normalize(text: str) -> str:
    return format_lsn(parse_lsn(text))


def compare_lsn(a: str, b: str) -> int:
    x, y = parse_lsn(a), parse_lsn(b)
    return (x > y) - (x < y)
