"""Per-table certification (master spec §10), computed only from recorded evidence - no manual flags."""
from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timedelta

from pydantic import BaseModel, ConfigDict, Field

from .models import CheckName, ReconResult, Status

REQUIRED: tuple[CheckName, ...] = (CheckName.ROW_COUNT, CheckName.KEY_SET, CheckName.COLUMN_CHECKSUM,
                                   CheckName.DELETES, CheckName.LATENCY, CheckName.DBT_SHADOW)


class DayEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")
    day: date
    clean: bool
    missing: list[CheckName] = Field(default_factory=list)
    failed: list[CheckName] = Field(default_factory=list)


class Certification(BaseModel):
    model_config = ConfigDict(extra="forbid")
    table: str
    certified: bool
    evaluated_at: datetime
    days_required: int
    clean_streak: int
    criteria: dict[str, bool]
    days: list[DayEvidence]
    evidence_ids: list[str]
    reasons: list[str]


def evaluate(table: str, results: Iterable[ReconResult], now: datetime, days: int = 7,
             required: tuple[CheckName, ...] = REQUIRED, gates: dict[str, str | None] | None = None) -> Certification:
    """Certified iff each of the last `days` complete UTC days has >=1 PASS for every required check and no
    FAIL/ERROR for any check; any failure taints its day. Today (incomplete) needs no evidence, but a failure
    recorded today blocks certification (NO_OPEN_FAILURES). `gates`: extra criteria {name: reason | None} from the hub
    (None = met), e.g. NO_PENDING_COMPLETION while out-of-row values of the table are still being completed."""
    end = now.date()
    window = [end - timedelta(days=i) for i in range(days, 0, -1)]
    by_day: dict[date, list[ReconResult]] = {d: [] for d in window}
    today_failed: list[ReconResult] = []
    for r in results:
        if r.table != table:
            continue
        if r.recorded_at.date() in by_day:
            by_day[r.recorded_at.date()].append(r)
        elif r.recorded_at.date() == end and r.status != Status.PASS:
            today_failed.append(r)
    day_ev: list[DayEvidence] = []
    for d in window:
        rs = by_day[d]
        passed = {r.check for r in rs if r.status == Status.PASS}
        failed = sorted({r.check for r in rs if r.status != Status.PASS}, key=lambda c: c.value)
        missing = [c for c in required if c not in passed]
        day_ev.append(DayEvidence(day=d, clean=not missing and not failed, missing=missing, failed=failed))
    streak = 0
    for ev in reversed(day_ev):
        if not ev.clean:
            break
        streak += 1
    all_rs = [r for d in window for r in by_day[d]] + today_failed
    criteria = {c.value: all(c not in ev.failed and c not in ev.missing for ev in day_ev) for c in required}
    criteria["CONSECUTIVE_CLEAN_DAYS"] = streak >= days
    criteria["NO_OPEN_FAILURES"] = not today_failed
    reasons = [f"{ev.day}: missing {[c.value for c in ev.missing]} failed {[c.value for c in ev.failed]}"
               for ev in day_ev if not ev.clean]
    reasons += [f"{end}: open {r.check.value} {r.status.value}" for r in today_failed]
    for name, why in (gates or {}).items():       # soft gates of the hub (e.g. NO_PENDING_COMPLETION, ADR-013)
        criteria[name] = why is None
        if why is not None:
            reasons.append(f"{name}: {why}")
    return Certification(table=table, certified=all(criteria.values()), evaluated_at=now, days_required=days,
                         clean_streak=streak, criteria=criteria, days=day_ev,
                         evidence_ids=sorted(r.recon_id for r in all_rs), reasons=reasons)
