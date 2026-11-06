"""F-20 job liveness (heartbeat + owner-conditional finish) and F-29 (SQLite = one worker)."""
from __future__ import annotations

import time
import uuid
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import select, update

from alelt_common.util import utcnow


def _job(engine: Any, kind: str) -> int:
    from alelt_hub import jobs

    with engine.begin() as c:
        jobs.enqueue(c, kind, {"x": 1})
    j = jobs.claim(engine, "w-a", [kind])
    assert j is not None
    return int(j["id"])


def test_long_running_job_with_heartbeat_is_not_requeued(client: Any) -> None:
    from alelt_hub import jobs
    from alelt_hub.db import get_engine, job

    eng = get_engine()
    kind = f"t-{uuid.uuid4().hex[:6]}"
    jid = _job(eng, kind)
    with eng.begin() as c:                        # pretend the job started long ago
        c.execute(update(job).where(job.c.id == jid).values(locked_at=utcnow() - timedelta(seconds=3600)))
    with jobs.heartbeat(eng, jid, "w-a", every_s=0.05):
        time.sleep(0.4)
        jobs.requeue_stale(eng)
        with eng.connect() as c:
            assert c.execute(select(job.c.status, job.c.locked_by).where(job.c.id == jid)).one() == ("RUNNING", "w-a")
    assert jobs.finish(eng, jid, "w-a")


def test_dead_worker_job_requeued_and_old_owner_cannot_finish(client: Any) -> None:
    from alelt_hub import jobs
    from alelt_hub.db import get_engine, job

    eng = get_engine()
    kind = f"t-{uuid.uuid4().hex[:6]}"
    jid = _job(eng, kind)
    with eng.begin() as c:                        # no heartbeat for > STALE_S: worker considered dead
        c.execute(update(job).where(job.c.id == jid).values(locked_at=utcnow() - timedelta(seconds=jobs.STALE_S + 5)))
    assert jobs.requeue_stale(eng) >= 1
    j2 = jobs.claim(eng, "w-b", [kind])
    assert j2 is not None and j2["id"] == jid
    assert not jobs.finish(eng, jid, "w-a")       # the zombie cannot mark w-b's run DONE
    assert not jobs.fail(eng, jid, "late error", 1, worker_id="w-a")
    assert not jobs.touch(eng, jid, "w-a")
    assert jobs.finish(eng, jid, "w-b")
    with eng.connect() as c:
        assert c.execute(select(job.c.status).where(job.c.id == jid)).scalar() == "DONE"


def test_sqlite_single_worker(client: Any, hub_env: dict[str, str]) -> None:
    from alelt_hub.db import get_engine
    from alelt_hub.dialect import unlock
    from alelt_hub.worker import sqlite_single_worker

    eng = get_engine()
    if hub_env["backend"] != "sqlite":
        assert sqlite_single_worker(eng) is None
        return
    lk = sqlite_single_worker(eng)
    try:
        with pytest.raises(SystemExit) as e:
            sqlite_single_worker(eng)
        assert e.value.code == 78
    finally:
        unlock(lk, "worker:sqlite-singleton")
