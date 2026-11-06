"""Job queue in the metadata database (no extra broker). Claim is atomic on both dialects (dialect.claim_one).

Liveness (F-20): the worker running a job touches `locked_at` every `HEARTBEAT_S` (`heartbeat()` thread), so
`requeue_stale` only returns jobs whose worker stopped heart-beating (dead process / lost host), however long the job
legitimately runs. `finish` / `fail` with `worker_id` are conditional on still owning the job: a worker whose job was
requeued and claimed by another worker can never mark the other worker's run DONE.

Lanes and priority (RC4): `data` kinds (decode, bulk replay, snapshot chunk loads, attach) and `control` kinds (every
other kind) are claimed by separate loops of the worker, so a long decode slice never delays an integrity / snapshot /
lifecycle step. Claim order inside a lane: `priority` (lower first, PRIORITY by kind), then `run_after`, then id.
Marker kinds (MARKER_KINDS) are deduplicated while QUEUED (one "stream dirty" marker per connection): the claim clears
the dedup key, so a marker enqueued while one RUNS queues the next run.
"""
from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from typing import Any

from sqlalchemy import Connection, Engine, case, select, update

from alelt_common.util import utcnow

from .db import job
from .dialect import claim_one, insert

log = logging.getLogger("alelt_hub")

HEARTBEAT_S = 30.0
STALE_S = 120            # > 3 missed heartbeats

DATA_KINDS = ("decode", "replay", "snapshot_load", "attach")
CONTROL_KINDS = ("integrity", "completion", "lifecycle", "snapshot", "schema", "verify", "resync", "reprocess",
                 "maintain")
LANES = {"data": DATA_KINDS, "control": CONTROL_KINDS}
# control lane priorities (lower first); data kinds keep the default (FIFO by readiness)
PRIORITY = {"integrity": 10, "completion": 20, "lifecycle": 30, "snapshot": 40, "schema": 50, "verify": 60,
            "resync": 70, "reprocess": 70, "maintain": 90}
DEFAULT_PRIORITY = 100
MARKER_KINDS = ("decode", "lifecycle", "completion", "schema")


def marker_key(kind: str, cid: str) -> str:
    """Dedup key of the per-connection marker of `kind` (one QUEUED marker per connection)."""
    return f"{kind}:{cid}"


def enqueue(conn: Connection, kind: str, payload: dict[str, Any], dedup_key: str | None = None,
            delay_s: float = 0, priority: int | None = None) -> None:
    """Insert a job in the caller's transaction; duplicate dedup_key is ignored."""
    stmt = insert(job).values(kind=kind, payload=payload, dedup_key=dedup_key,
                              run_after=utcnow() + timedelta(seconds=delay_s),
                              priority=PRIORITY.get(kind, DEFAULT_PRIORITY) if priority is None else priority)
    conn.execute(stmt.on_conflict_do_nothing(index_elements=["dedup_key"]) if dedup_key else stmt)


def mark_dirty(conn: Connection, cid: str, delay_s: float = 0) -> None:
    """The stream of `cid` has work (batches received, a continuation, an attach tail): one decode marker."""
    enqueue(conn, "decode", {"connection_id": cid}, dedup_key=marker_key("decode", cid), delay_s=delay_s)


def claim(engine: Engine, worker_id: str, kinds: list[str] | tuple[str, ...]) -> dict[str, Any] | None:
    """Atomically claim the next ready job of `kinds` by (priority, run_after, id). The dedup key of a marker kind is
    cleared on claim, so the next marker of the same connection can be queued while this one runs."""
    with engine.begin() as c:
        return claim_one(c, job, [job.c.status == "QUEUED", job.c.kind.in_(list(kinds)), job.c.run_after <= utcnow()],
                         (job.c.priority, job.c.run_after, job.c.id),
                         dict(status="RUNNING", locked_by=worker_id, locked_at=utcnow(), attempts=job.c.attempts + 1,
                              dedup_key=case((job.c.kind.in_(MARKER_KINDS), None), else_=job.c.dedup_key)))


def _mine(job_id: int, worker_id: str | None) -> Any:
    cond = job.c.id == job_id
    if worker_id is not None:
        cond = cond & (job.c.locked_by == worker_id) & (job.c.status == "RUNNING")
    return cond


def touch(engine: Engine, job_id: int, worker_id: str) -> bool:
    """Heartbeat one running job; False when the job is no longer ours (requeued / taken over)."""
    with engine.begin() as c:
        return c.execute(update(job).where(_mine(job_id, worker_id)).values(locked_at=utcnow())).rowcount == 1


@contextmanager
def heartbeat(engine: Engine, job_id: int, worker_id: str, every_s: float = HEARTBEAT_S) -> Iterator[None]:
    """Keep `locked_at` fresh while the body runs (daemon thread; errors are logged, never raised)."""
    stop = threading.Event()

    def loop() -> None:
        while not stop.wait(every_s):
            try:
                if not touch(engine, job_id, worker_id):
                    log.warning("job lost while running", extra={"component": "alelt_hub", "correlation_id": str(job_id)})
                    return
            except Exception:  # noqa: BLE001 - a missed heartbeat is retried at the next tick
                log.exception("job heartbeat failed", extra={"component": "alelt_hub", "correlation_id": str(job_id)})

    t = threading.Thread(target=loop, name=f"job-hb-{job_id}", daemon=True)
    t.start()
    try:
        yield
    finally:
        stop.set()
        t.join(timeout=5)


def finish(engine: Engine, job_id: int, worker_id: str | None = None) -> bool:
    with engine.begin() as c:
        return c.execute(update(job).where(_mine(job_id, worker_id))
                         .values(status="DONE", finished_at=utcnow(), error=None)).rowcount == 1


# Data-path jobs are never abandoned (XC-10): after max_attempts they stay QUEUED and are retried once per
# STUCK_RETRY_S (an incident is raised on every failure by alelt_diagnose.hooks.job_failed); other kinds end FAILED.
PERSISTENT_KINDS = frozenset({"decode", "replay", "integrity", "snapshot_load", "snapshot", "resync", "reprocess",
                              "attach", "completion", "lifecycle", "schema"})
STUCK_RETRY_S = 3600


def fail(engine: Engine, job_id: int, error: str, attempts: int, max_attempts: int = 50,
         worker_id: str | None = None) -> bool:
    """Retry with exponential backoff (capped at 5 min); after max_attempts: data-path kinds hourly, others FAILED."""
    delay = min(300, 2 ** min(attempts, 9))
    with engine.begin() as c:
        kind = c.execute(select(job.c.kind).where(job.c.id == job_id)).scalar()
        stuck = attempts >= max_attempts
        keep = not stuck or kind in PERSISTENT_KINDS
        if stuck and keep:
            delay = STUCK_RETRY_S
            error = f"[stuck after {attempts} attempts, retried every {STUCK_RETRY_S}s] {error}"
        return c.execute(update(job).where(_mine(job_id, worker_id)).values(
            status="QUEUED" if keep else "FAILED",
            run_after=utcnow() + timedelta(seconds=delay), error=error[:4000], locked_by=None)).rowcount == 1


def retry(engine: Engine, job_id: int) -> bool:
    """Operator retry: a FAILED or waiting (QUEUED) job runs now with a fresh attempt budget."""
    with engine.begin() as c:
        return c.execute(update(job).where(job.c.id == job_id, job.c.status.in_(("FAILED", "QUEUED"))).values(
            status="QUEUED", attempts=0, run_after=utcnow(), locked_by=None)).rowcount == 1


def requeue_stale(engine: Engine, older_than_s: int = STALE_S) -> int:
    """Return RUNNING jobs whose worker stopped heart-beating to the queue."""
    with engine.begin() as c:
        r = c.execute(update(job).where(job.c.status == "RUNNING",
                                        job.c.locked_at < utcnow() - timedelta(seconds=older_than_s))
                      .values(status="QUEUED", locked_by=None))
        return r.rowcount