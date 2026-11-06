"""Off-peak window enforcement (local wall-clock window, may wrap midnight)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class OffPeakWindow:
    start: time
    end: time
    tz: str = "UTC"

    @classmethod
    def parse(cls, start: str, end: str, tz: str = "UTC") -> OffPeakWindow:
        return cls(time.fromisoformat(start), time.fromisoformat(end), tz)

    @classmethod
    def always(cls) -> OffPeakWindow:
        return cls(time(0, 0), time(0, 0))

    def contains(self, at: datetime) -> bool:
        if self.start == self.end:
            return True
        t = at.astimezone(ZoneInfo(self.tz)).time()
        if self.start < self.end:
            return self.start <= t < self.end
        return t >= self.start or t < self.end

    def next_open(self, at: datetime) -> datetime:
        """`at` if inside, else the next window start (aware datetime in the window tz)."""
        if self.contains(at):
            return at
        local = at.astimezone(ZoneInfo(self.tz))
        cand = local.replace(hour=self.start.hour, minute=self.start.minute, second=0, microsecond=0)
        return cand if cand > local else cand + timedelta(days=1)
