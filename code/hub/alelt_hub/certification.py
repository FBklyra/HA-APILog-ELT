"""Per-table certification criteria computed from recorded evidence only (F-19).

Rules:
- A criterion is PASS only when evidence recorded by the platform proves it. Missing evidence = UNKNOWN/PENDING,
  never PASS (a proxy value such as the current capture lag is not evidence).
- LATENCY_P95: commit -> apply latency per flush = `batch_table_stat.created_at - commit_ts_min` (the oldest source
  commit the flush applied for that table, i.e. the worst latency of the flush). p95 by nearest rank over the flushes
  of the window. This is an upper bound of the per-row p95 (conservative), and it is computed from timestamps the
  loader records in the same metadata transaction as the checkpoint mirror.
- CLEAN_DAYS_7: evaluated by the ONE certification evaluator, `alelt_verify.certification.evaluate` (XC-11): each of the
  last 7 complete UTC days needs a PASS of every required check (ROW_COUNT, KEY_SET, COLUMN_CHECKSUM, DELETES,
  LATENCY from recorded flushes, DBT_SHADOW) and no failure; a failure recorded today blocks.
- Keyless tables (no key column): PK_SET_PARITY is proven by the KEY_SET recon run, whose suite compares the table as
  a multiset (F-10, alelt_verify ROW_MULTISET); the criterion value says so.
"""
from __future__ import annotations

import math
import uuid
from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import Connection, func, select

from .db import batch_table_stat, source_column, source_table

LATENCY_WINDOW = timedelta(hours=24)
MIN_FLUSHES = 1


def percentile(values: Iterable[float], p: float = 95.0) -> float | None:
    """Nearest-rank percentile (no interpolation: the value is one that was actually observed)."""
    xs = sorted(values)
    if not xs:
        return None
    rank = max(1, math.ceil(p / 100.0 * len(xs)))
    return xs[rank - 1]


def _aware(ts: datetime) -> datetime:
    return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)


def flush_latencies(c: Connection, connection_id: str, table_fqn: str | None = None,
                    since: datetime | None = None) -> list[float]:
    """Worst commit -> apply latency (seconds) of every flush in the window, for one table or the whole connection."""
    since = since or datetime.now(timezone.utc) - LATENCY_WINDOW
    q = (select(batch_table_stat.c.created_at, batch_table_stat.c.commit_ts_min)
         .where(batch_table_stat.c.batch_id.like(f"{connection_id}:%"),
                batch_table_stat.c.commit_ts_min.is_not(None), batch_table_stat.c.created_at >= since))
    if table_fqn is not None:
        q = q.where(batch_table_stat.c.table_fqn == table_fqn)
    return [max(0.0, (_aware(at) - _aware(ts)).total_seconds()) for at, ts in c.execute(q)]


def latency_p95(c: Connection, connection_id: str, table_fqn: str | None = None,
                since: datetime | None = None) -> float | None:
    xs = flush_latencies(c, connection_id, table_fqn, since)
    return percentile(xs) if len(xs) >= MIN_FLUSHES else None


def latency_criterion(c: Connection, connection_id: str, table_fqn: str, target_s: float) -> tuple[str, str]:
    p = latency_p95(c, connection_id, table_fqn)
    if p is None:
        return "UNKNOWN", "no applied flush with commit timestamps in the last 24 h"
    return ("PASS" if p <= target_s else "FAIL"), f"p95 {p:.0f}s over {len(flush_latencies(c, connection_id, table_fqn))} flushes"


_KIND_TO_CHECK = {"ROW_COUNT": "ROW_COUNT", "KEY_SET": "KEY_SET", "COLUMN_CHECKSUM": "COLUMN_CHECKSUM",
                  "DELETES": "DELETES", "DBT_SHADOW": "DBT_SHADOW"}


def evidence(rows: Iterable[tuple[str, str, datetime]], table: str, latency_days: dict[date, bool]) -> list[Any]:
    """Recon rows (check_kind, outcome, created_at) + per-day latency verdicts -> alelt_verify ReconResults."""
    from alelt_verify.models import CheckName, ReconResult, Status

    out = []
    for kind, outcome, at in rows:
        chk = _KIND_TO_CHECK.get(kind)
        if chk is None:
            continue
        st = Status.PASS if outcome == "MATCH" else Status.FAIL if outcome == "MISMATCH" else Status.ERROR
        out.append(ReconResult(run_id="hub", table=table, check=CheckName(chk), status=st, recorded_at=_aware(at)))
    for d, ok in latency_days.items():
        out.append(ReconResult(run_id="latency", table=table, check=CheckName.LATENCY,
                               status=Status.PASS if ok else Status.FAIL,
                               recorded_at=datetime(d.year, d.month, d.day, 12, tzinfo=timezone.utc)))
    return out


def latency_by_day(c: Connection, connection_id: str, table_fqn: str, target_s: float, days: int = 8) -> dict[date, bool]:
    """Per UTC day: p95 of recorded flush latencies <= target (days without flushes carry no LATENCY evidence)."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    q = (select(batch_table_stat.c.created_at, batch_table_stat.c.commit_ts_min)
         .where(batch_table_stat.c.batch_id.like(f"{connection_id}:%"), batch_table_stat.c.table_fqn == table_fqn,
                batch_table_stat.c.commit_ts_min.is_not(None), batch_table_stat.c.created_at >= since))
    per: dict[date, list[float]] = {}
    for at, ts in c.execute(q):
        per.setdefault(_aware(at).date(), []).append(max(0.0, (_aware(at) - _aware(ts)).total_seconds()))
    return {d: (percentile(xs) or 0.0) <= target_s for d, xs in per.items()}


def clean_days_criterion(c: Connection, connection_id: str, table_fqn: str, target_s: float,
                         rows: Iterable[tuple[str, str, datetime]], days: int = 7) -> tuple[str, str, int]:
    """CLEAN_DAYS_7 via the ONE evaluator (`alelt_verify.certification.evaluate`, XC-11): each of the last `days` complete
    UTC days needs a PASS of every required check (incl. LATENCY from recorded flushes) and no failure; a failure
    recorded today blocks. Returns (status, value, streak)."""
    from alelt_verify.certification import evaluate

    from .db import table_completion

    ev = evidence(rows, table_fqn, latency_by_day(c, connection_id, table_fqn, target_s, days + 1))
    pending = c.execute(select(func.count()).select_from(table_completion).where(
        table_completion.c.connection_id == connection_id, table_completion.c.table_fqn == table_fqn,
        table_completion.c.status != "CANCELLED")).scalar_one()
    res = evaluate(table_fqn, ev, datetime.now(timezone.utc), days=days, gates={
        "NO_PENDING_COMPLETION": f"{pending} out-of-row completion(s) pending" if pending else None})
    ok = res.criteria["CONSECUTIVE_CLEAN_DAYS"] and res.criteria["NO_OPEN_FAILURES"] and         res.criteria["NO_PENDING_COMPLETION"]
    return ("PASS" if ok else "PENDING"), str(res.clean_streak), res.clean_streak


def is_keyless(c: Connection, table_id: uuid.UUID) -> bool:
    keyed = c.execute(select(source_column.c.id).where(source_column.c.table_id == table_id,
                                                       source_column.c.key_seq.is_not(None),
                                                       source_column.c.key_seq > 0).limit(1)).first()
    has_cols = c.execute(select(source_column.c.id).where(source_column.c.table_id == table_id).limit(1)).first()
    return has_cols is not None and keyed is None


def table_fqn(c: Connection, table_id: uuid.UUID) -> str | None:
    r = c.execute(select(source_table.c.src_schema, source_table.c.src_table)
                  .where(source_table.c.id == table_id)).first()
    return f"{r[0]}.{r[1]}" if r else None


def summary(c: Connection, connection_id: str) -> dict[str, Any]:
    """Connection-level latency evidence for dashboards (None = no evidence, shown as unknown)."""
    xs = flush_latencies(c, connection_id)
    return {"latency_p95_seconds": percentile(xs), "latency_flushes": len(xs)}
