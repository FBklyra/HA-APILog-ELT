"""Job lanes, priority and the per-connection "stream dirty" marker (RC4, WP0), both metadata backends."""
from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import func, select, update

from alelt_common.util import utcnow


def _eng(hub_env: dict[str, str]) -> Any:
    from alelt_hub.db import get_engine, init_db

    eng = get_engine()
    init_db(eng)
    return eng


def _clean(eng: Any, tag: str) -> None:
    from alelt_hub.db import job

    with eng.begin() as c:   # other suites share the queue: only this test's jobs are looked at, the rest parked
        c.execute(update(job).where(job.c.status == "QUEUED").values(status="DONE", error=f"parked by {tag}"))


def test_claim_order_priority_then_readiness_then_id(hub_env: dict[str, str]) -> None:
    from alelt_hub import jobs

    eng = _eng(hub_env)
    _clean(eng, "order")
    tag = uuid.uuid4().hex[:6]
    with eng.begin() as c:
        for kind in ("maintain", "verify", "snapshot", "integrity", "lifecycle", "completion", "resync"):
            jobs.enqueue(c, kind, {"connection_id": tag})
        jobs.enqueue(c, "integrity", {"connection_id": tag, "n": 2})          # same priority: id order
    got = []
    while True:
        j = jobs.claim(eng, f"w-{tag}", jobs.CONTROL_KINDS)
        if j is None:
            break
        got.append((j["kind"], j["payload"].get("n")))
        jobs.finish(eng, j["id"])
    assert got == [("integrity", None), ("integrity", 2), ("completion", None), ("lifecycle", None),
                   ("snapshot", None), ("verify", None), ("resync", None), ("maintain", None)], got


def test_lanes_claim_only_their_kinds(hub_env: dict[str, str]) -> None:
    from alelt_hub import jobs

    eng = _eng(hub_env)
    _clean(eng, "lanes")
    cid = f"lane{uuid.uuid4().hex[:6]}"
    with eng.begin() as c:
        jobs.mark_dirty(c, cid)
        jobs.enqueue(c, "integrity", {"connection_id": cid, "table": "S.T"})
    d = jobs.claim(eng, "w-data", jobs.DATA_KINDS)
    k = jobs.claim(eng, "w-ctl", jobs.CONTROL_KINDS)
    assert d is not None and d["kind"] == "decode"
    assert k is not None and k["kind"] == "integrity"
    assert set(jobs.DATA_KINDS).isdisjoint(jobs.CONTROL_KINDS)
    jobs.finish(eng, d["id"])
    jobs.finish(eng, k["id"])


def test_marker_dedup_and_cleared_on_claim(hub_env: dict[str, str]) -> None:
    from alelt_hub import jobs
    from alelt_hub.db import job

    eng = _eng(hub_env)
    _clean(eng, "marker")
    cid = f"mk{uuid.uuid4().hex[:6]}"
    for _ in range(200):
        with eng.begin() as c:
            jobs.mark_dirty(c, cid)
    with eng.connect() as c:
        n = c.execute(select(func.count()).select_from(job).where(
            job.c.kind == "decode", job.c.status == "QUEUED", job.c.dedup_key == f"decode:{cid}")).scalar_one()
    assert n == 1
    j = jobs.claim(eng, "w1", ["decode"])
    assert j is not None and j["dedup_key"] is None                           # cleared by the claim
    with eng.begin() as c:                                                    # a batch arrives while it RUNS
        jobs.mark_dirty(c, cid)
        jobs.mark_dirty(c, cid)
    with eng.connect() as c:
        q = c.execute(select(func.count()).select_from(job).where(
            job.c.kind == "decode", job.c.status == "QUEUED", job.c.dedup_key == f"decode:{cid}")).scalar_one()
    assert q == 1                                                             # exactly one follow-up marker
    jobs.finish(eng, j["id"], "w1")
    nxt = jobs.claim(eng, "w1", ["decode"])
    assert nxt is not None and nxt["payload"] == {"connection_id": cid}
    jobs.finish(eng, nxt["id"], "w1")


def test_non_marker_dedup_key_kept_on_claim(hub_env: dict[str, str]) -> None:
    from alelt_hub import jobs

    eng = _eng(hub_env)
    _clean(eng, "nonmarker")
    key = f"replay:x:{uuid.uuid4().hex}"
    with eng.begin() as c:
        jobs.enqueue(c, "replay", {"connection_id": "x", "pass_id": "p"}, dedup_key=key)
    j = jobs.claim(eng, "w2", ["replay"])
    assert j is not None and j["dedup_key"] == key                            # passes stay deduplicated for life
    jobs.finish(eng, j["id"], "w2")


def test_requeue_stale_unaffected_by_lanes(hub_env: dict[str, str]) -> None:
    from alelt_hub import jobs
    from alelt_hub.db import job

    eng = _eng(hub_env)
    _clean(eng, "stale")
    cid = f"st{uuid.uuid4().hex[:6]}"
    with eng.begin() as c:
        jobs.mark_dirty(c, cid)
    j = jobs.claim(eng, "dead-worker", jobs.DATA_KINDS)
    assert j is not None
    with eng.begin() as c:
        c.execute(update(job).where(job.c.id == j["id"]).values(locked_at=utcnow() - timedelta(seconds=999)))
    assert jobs.requeue_stale(eng) >= 1
    again = jobs.claim(eng, "live-worker", jobs.DATA_KINDS)
    assert again is not None and again["id"] == j["id"] and again["attempts"] == 2
    jobs.finish(eng, again["id"], "live-worker")


def test_priority_defaults_by_kind(hub_env: dict[str, str]) -> None:
    from alelt_hub import jobs
    from alelt_hub.db import job

    eng = _eng(hub_env)
    tag = uuid.uuid4().hex
    with eng.begin() as c:
        jobs.enqueue(c, "integrity", {"t": tag}, dedup_key=f"p1:{tag}")
        jobs.enqueue(c, "decode", {"t": tag}, dedup_key=f"p2:{tag}")
        jobs.enqueue(c, "verify", {"t": tag}, dedup_key=f"p3:{tag}", priority=5)
        got = dict(c.execute(select(job.c.dedup_key, job.c.priority).where(job.c.dedup_key.like(f"%:{tag}"))).all())
        c.execute(update(job).where(job.c.dedup_key.like(f"%:{tag}")).values(status="DONE"))
    assert got == {f"p1:{tag}": 10, f"p2:{tag}": 100, f"p3:{tag}": 5}
