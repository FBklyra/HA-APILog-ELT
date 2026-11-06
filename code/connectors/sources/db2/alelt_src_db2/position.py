"""Db2 type-1 LRI positions. Text form: '<part1 decimal>:<part2 hex16 upper>' (stdlib only)."""
from __future__ import annotations

import re
import struct
from dataclasses import dataclass

_RX = re.compile(r"^\s*(\d{1,20}):([0-9A-Fa-f]{1,16})\s*$")
_LRI = struct.Struct("<QQQ")   # db2LRI: lriType, part1, part2
_U64 = (1 << 64) - 1


@dataclass(frozen=True, order=True)
class Lri:
    """Type-1 LRI; ordering is (part1, part2)."""

    part1: int
    part2: int

    def __post_init__(self) -> None:
        if not (0 <= self.part1 <= _U64 and 0 <= self.part2 <= _U64):
            raise ValueError("LRI parts must be unsigned 64-bit")

    @property
    def text(self) -> str:
        return f"{self.part1}:{self.part2:016X}"

    def __str__(self) -> str:
        return self.text

    @classmethod
    def parse(cls, text: str) -> Lri:
        m = _RX.match(text)
        if not m:
            raise ValueError(f"invalid Db2 LRI {text!r}; expected '<part1>:<part2 hex16>'")
        return cls(int(m.group(1)), int(m.group(2), 16))

    @classmethod
    def from_struct(cls, buf: bytes | memoryview, offset: int = 0) -> Lri:
        """Read a 24-byte db2LRI (lriType must be 1)."""
        if len(buf) - offset < _LRI.size:
            raise ValueError("buffer too short for db2LRI")
        typ, p1, p2 = _LRI.unpack_from(buf, offset)
        if typ != 1:
            raise ValueError(f"unexpected LRI type {typ}")
        return cls(p1, p2)

    def to_struct(self) -> bytes:
        return _LRI.pack(1, self.part1, self.part2)


def parse_lri(text: str) -> Lri:
    return Lri.parse(text)


def format_lri(lri: Lri) -> str:
    return lri.text


def compare_lri(a: str | Lri, b: str | Lri) -> int:
    """<0, 0, >0 like strcmp."""
    x = a if isinstance(a, Lri) else Lri.parse(a)
    y = b if isinstance(b, Lri) else Lri.parse(b)
    return (x > y) - (x < y)
