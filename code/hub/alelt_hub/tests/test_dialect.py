"""Metadata dialect layer on every backend: job claim under 3 concurrent worker processes, advisory locks,
UTC timestamps, upserts, batch sequence, migrate (create + additive columns)."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import Engine, func, select, text

from .conftest import ADMIN, BACKENDS, _ROOT, _admin

HUB = str(_ROOT / "hub")


@pytest.fixture(params=BACKENDS)
def meta_url(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[str]:
    if request.param == "sqlite":
        yield f"sqlite:///{(tmp_path / 'data' / 'alelt.db').as_posix()}"
        return
    name = f"alelt_test_dialect_{uuid.uuid4().hex[:8]}"
    _admin(f"CREATE DATABASE {name}")
    yield f"{ADMIN.replace('postgresql://', 'postgresql+psycopg://')}/{name}"   # type: ignore[union-attr]
    from alelt_hub.db import get_engine

    get_engine(f"{ADMIN.replace('postgresql://', 'postgresql+psycopg://')}/{name}").dispose()  # type: ignore[union-attr]
    _admin(f"DROP DATABASE {name} WITH (FORCE)")


@pytest.fixture
def eng(meta_url: str) -> Iterator[Engine]:
    from alelt_hub.db import get_engine, init_db

    e = get_engine(meta_url)
    init_db(e)
    yield e
    e.dispose()


def _child(code: str, url: str, *args: str) -> subprocess.Popen[str]:
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([HUB, os.environ.get("PYTHONPATH", "")]), "ALELT_DB_URL": url}
    return subprocess.Popen([sys.executable, "-c", code, *args], env=env, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


CLAIMER = r"""
import json, sys, time
from pathlib import Path
from alelt_hub import jobs
from alelt_hub.db import get_engine
url, wid, go = sys.argv[1], sys.argv[2], Path(sys.argv[3])
eng = get_engine(url)
print("ready", flush=True)
end = time.monotonic() + 30
while not go.exists() and time.monotonic() < end:
    time.sleep(0.01)
got, idle = [], 0
while idle < 30 and time.monotonic() < end + 60:
    j = jobs.claim(eng, wid, ["t"])
    if j is None:
        idle += 1
        time.sleep(0.02)
        continue
    idle = 0
    got.append(j["id"])
    time.sleep(0.002)                    # simulated work: interleave the workers
    jobs.finish(eng, j["id"])
print(json.dumps(got), flush=True)
"""


def test_three_workers_never_double_claim(eng: Engine, meta_url: str, tmp_path: Path) -> None:
    from alelt_hub import jobs
    from alelt_hub.db import job

    n = 300
    with eng.begin() as c:
        for i in range(n):
            jobs.enqueue(c, "t", {"i": i}, dedup_key=f"t:{i}")
        jobs.enqueue(c, "t", {"i": 0}, dedup_key="t:0")             # duplicate dedup_key ignored
    go = tmp_path / "go"
    procs = [_child(CLAIMER, meta_url, meta_url, f"w{i}", str(go)) for i in range(3)]
    try:
        for p in procs:
            assert p.stdout is not None and p.stdout.readline().strip() == "ready"
        go.touch()
        outs = [p.communicate(timeout=120) for p in procs]
    finally:
        for p in procs:
            if p.poll() is None:
                p.kill()
    for p, (_, err) in zip(procs, outs):
        assert p.returncode == 0, err[-2000:]
    claimed = [json.loads(out.strip().splitlines()[-1]) for out, _ in outs]
    flat = [j for w in claimed for j in w]
    assert len(flat) == n and len(set(flat)) == n                      # every job exactly once
    assert sum(1 for w in claimed if w) >= 2, [len(w) for w in claimed]  # really concurrent
    with eng.connect() as c:
        assert dict(c.execute(select(job.c.status, func.count()).group_by(job.c.status)).all()) == {"DONE": n}
        assert c.execute(select(func.max(job.c.attempts))).scalar() == 1


HOLDER = r"""
import sys, time
from alelt_hub.db import get_engine
from alelt_hub.dialect import try_lock
h = try_lock(get_engine(sys.argv[1]), "pipeline:x")
print("locked" if h is not None else "busy", flush=True)
sys.stdin.readline()
"""


def test_advisory_lock_cross_process(eng: Engine, meta_url: str) -> None:
    from alelt_hub.dialect import try_lock, unlock

    a = try_lock(eng, "pipeline:y")
    assert a is not None and try_lock(eng, "pipeline:y") is None       # second session/handle is refused
    unlock(a, "pipeline:y")
    b = try_lock(eng, "pipeline:y")
    assert b is not None
    unlock(b, "pipeline:y")

    p = _child(HOLDER, meta_url, meta_url)
    try:
        assert p.stdout is not None and p.stdout.readline().strip() == "locked"
        assert try_lock(eng, "pipeline:x") is None                     # held by another process
        assert p.stdin is not None
        p.stdin.write("\n")
        p.stdin.flush()
        p.wait(timeout=30)
    finally:
        if p.poll() is None:
            p.kill()
    end = time.monotonic() + 10                                        # released when the holder exits
    h = None
    while h is None and time.monotonic() < end:
        h = try_lock(eng, "pipeline:x")
        time.sleep(0.1)
    assert h is not None
    unlock(h, "pipeline:x")


def test_utc_timestamps_upsert_and_seq(eng: Engine) -> None:
    from alelt_hub.db import batch, connection, hub_setting, job
    from alelt_hub.dialect import insert

    plus2 = timezone(timedelta(hours=2))
    t = datetime(2026, 11, 3, 12, 0, 0, 123456, tzinfo=plus2)
    with eng.begin() as c:
        c.execute(insert(connection).values(id="c1", engine="fake"))
        c.execute(insert(connection).values(id="c1", engine="other").on_conflict_do_nothing(index_elements=["id"]))
        for i in range(3):
            c.execute(insert(batch).values(id=uuid.uuid4(), connection_id="c1", agent_id="a", engine="fake",
                                           from_lri=str(i), to_lri=str(i), rec_count=0, raw_bytes=0, sha256="x",
                                           codec="none", spool_path="p", updated_at=t if i == 0 else None))
        for v in (1, 2):
            s = insert(hub_setting).values(key="k", value={"v": v})
            c.execute(s.on_conflict_do_update(index_elements=["key"], set_={"value": s.excluded.value}))
    with eng.connect() as c:
        assert c.execute(select(connection.c.engine)).scalar() == "fake"
        assert c.execute(select(hub_setting.c.value)).scalar() == {"v": 2}
        rows = c.execute(select(batch.c.seq, batch.c.updated_at, batch.c.received_at).order_by(batch.c.seq)).all()
        seqs = [r[0] for r in rows]
        assert seqs == sorted(set(seqs)) and len(seqs) == 3             # unique, increasing
        got = rows[0][1]
        assert got == t and got.tzinfo is not None and got.utcoffset() == timedelta(0)
        now = datetime.now(timezone.utc)
        assert all(r[2].tzinfo is not None and abs((now - r[2]).total_seconds()) < 60 for r in rows)  # server default
        # comparisons between server defaults and bound aware values
        assert c.execute(select(func.count()).select_from(batch)
                         .where(batch.c.received_at > now - timedelta(minutes=1))).scalar() == 3
        assert c.execute(select(func.min(batch.c.received_at))).scalar().tzinfo is not None
    with eng.begin() as c:
        c.execute(insert(job).values(kind="k", payload={}))
        c.execute(insert(job).values(kind="k", payload={}))
    with eng.connect() as c:
        assert [r[0] for r in c.execute(select(job.c.id).order_by(job.c.id))] == [1, 2]


def test_migrate_creates_and_adds_columns(meta_url: str) -> None:
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([HUB, os.environ.get("PYTHONPATH", "")]), "ALELT_DB_URL": meta_url}

    def run(*a: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run([sys.executable, "-m", "alelt_hub.migrate", *a], env=env, capture_output=True, text=True,
                              timeout=120)
    assert run("--check").returncode == 1
    r = run()
    assert r.returncode == 0 and "metadata schema ready" in r.stdout, r.stderr[-2000:]
    assert run("--check").returncode == 0
    from alelt_hub.db import get_engine

    e = get_engine(meta_url)
    with e.begin() as c:
        c.execute(text('ALTER TABLE "agent" DROP COLUMN "limits"'))
        c.execute(text('ALTER TABLE "job" DROP COLUMN "error"'))
    e.dispose()
    assert run("--check").returncode == 1
    r = run()
    assert r.returncode == 0 and "added 2 columns" in r.stdout, r.stdout + r.stderr[-2000:]
    assert run("--check").returncode == 0
