"""SQL Server CDC positions (stdlib only).

Text form ``"<start_lsn hex20>:<seqval hex20>"`` (both ``binary(10)``, upper-case hex), compared as the byte pair.

| Position | Meaning |
|---|---|
| ``<lsn>:<seqval>`` | DML event (``__$start_lsn`` = commit LSN of its transaction, ``__$seqval`` = order inside it) |
| ``<lsn>:FFFFFFFFFFFFFFFFFFFF`` | **commit token** of the transaction ``<lsn>`` (resume: deliver ``__$start_lsn > lsn``) |
| ``<lsn>:00000000000000000000`` | "before every change committed at ``lsn``" (resume: deliver ``__$start_lsn >= lsn``) |
"""
from __future__ import annotations

import re
from dataclasses import dataclass

_RX = re.compile(r"^\s*(?:0x)?([0-9A-Fa-f]{20})(?::(?:0x)?([0-9A-Fa-f]{20}))?\s*$")
ZERO = b"\x00" * 10
MAX = b"\xff" * 10


class PositionError(ValueError):
    pass


@dataclass(frozen=True)
class Lsn:
    lsn: bytes
    seq: bytes = MAX

    @classmethod
    def parse(cls, text: str) -> Lsn:
        m = _RX.match(text or "")
        if not m:
            raise PositionError(f"not a SQL Server CDC position: {text!r}")
        return cls(bytes.fromhex(m.group(1)), bytes.fromhex(m.group(2)) if m.group(2) else MAX)

    @property
    def text(self) -> str:
        return f"{self.lsn.hex().upper()}:{self.seq.hex().upper()}"

    @property
    def key(self) -> tuple[bytes, bytes]:
        return (self.lsn, self.seq)

    @property
    def is_commit_token(self) -> bool:
        return self.seq == MAX


def lsn_text(b: bytes) -> str:
    return bytes(b).hex().upper()


def increment(b: bytes) -> bytes:
    """Same result as sys.fn_cdc_increment_lsn for a binary(10) LSN."""
    n = int.from_bytes(b, "big") + 1
    return min(n, (1 << 80) - 1).to_bytes(10, "big")     # SRC-ms-07: the highest LSN saturates, never overflows


def compare_lsn(a: str, b: str) -> int:
    ka, kb = Lsn.parse(a).key, Lsn.parse(b).key
    return (ka > kb) - (ka < kb)
