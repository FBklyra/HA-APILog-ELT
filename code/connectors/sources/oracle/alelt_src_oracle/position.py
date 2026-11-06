"""Oracle positions (stdlib only).

Text forms (all parse to one total order on ``(scn, xid)``):

| Form | Meaning |
|---|---|
| ``"<scn>"`` | a plain SCN (current position, snapshot start without open transactions) |
| ``"<scn>~<restart>"`` | plain SCN + the SCN mining must restart from (oldest transaction open at ``scn``) |
| ``"<scn>.<xid>"`` | a DML event of transaction ``xid`` at ``scn`` (informational) |
| ``"<scn>.<xid>~<restart>"`` | **commit token**: transaction ``xid`` committed at ``scn``; on resume, mining restarts at
  ``restart`` and every transaction whose ``(commit_scn, xid) <= (scn, xid)`` is skipped |

``xid`` = ``RAWTOHEX(XID)`` (16 hex digits, upper case). ``restart`` never takes part in comparisons.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

_RX = re.compile(r"^\s*(\d+)(?:\.([0-9A-Fa-f]{1,32}))?(?:~(\d+))?\s*$")


class PositionError(ValueError):
    pass


@dataclass(frozen=True)
class Scn:
    scn: int
    xid: str = ""
    restart: int | None = None

    @classmethod
    def parse(cls, text: str) -> Scn:
        m = _RX.match(text or "")
        if not m:
            raise PositionError(f"not an Oracle position: {text!r}")
        restart = int(m.group(3)) if m.group(3) is not None else None
        return cls(int(m.group(1)), (m.group(2) or "").upper(), restart)

    @property
    def key(self) -> tuple[int, str]:
        return (self.scn, self.xid)

    @property
    def mine_from(self) -> int:
        """First SCN that must be mined to resume after this position."""
        if self.restart is not None:
            return min(self.restart, self.scn)
        return self.scn

    @property
    def text(self) -> str:
        s = str(self.scn)
        if self.xid:
            s += "." + self.xid
        if self.restart is not None and (self.restart != self.scn or self.xid):
            s += f"~{self.restart}"
        return s

    def __str__(self) -> str:  # pragma: no cover
        return self.text


def compare_scn(a: str, b: str) -> int:
    ka, kb = Scn.parse(a).key, Scn.parse(b).key
    return (ka > kb) - (ka < kb)
