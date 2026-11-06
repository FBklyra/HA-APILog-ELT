"""Capture lease (X-47/X-48): acquire / renew / release, fencing epoch, stale epoch -> 423, takeover after the TTL,
two agents on one connection -> only the holder's batches are accepted; the reusable Python lease client."""
from __future__ import annotations

import threading
import uuid
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import delete, select

from alelt_common.lease_client import ACTIVE, LOST, RENEWED, TAKEOVER, LeaseClient, LeaseState
from alelt_hub.testing import FakeAgent, ev, lri

from .conftest import TOKEN

CONN = "haconn"


class Clock:
    """Shifts the hub's notion of now (alelt_hub.ha.utcnow) and the agents' monotonic clock together."""

    def __init__(self) -> None:
        self.offset = 0.0
        self.mono = 1000.0

    def advance(self, s: float) -> None:
        self.offset += s
        self.mono += s


@pytest.fixture(scope="module")
def clock() -> Iterator[Clock]:
    from alelt_hub import ha

    real, real_db = ha.utcnow, ha.db_now
    ck = Clock()
    mp = pytest.MonkeyPatch()
    mp.setattr(ha, "utcnow", lambda: real() + timedelta(seconds=ck.offset))
    mp.setattr(ha, "db_now", lambda c: real_db(c) + timedelta(seconds=ck.offset))   # HA-12: PG clock too
    yield ck
    mp.undo()


@pytest.fixture(scope="module")
def pair(client: Any, clock: Clock) -> Iterator[tuple[FakeAgent, FakeAgent]]:
    a = FakeAgent(client, TOKEN, agent_id="ha-a", connection_id=CONN)
    b = FakeAgent(client, TOKEN, agent_id="ha-b", connection_id=CONN)
    a.register()
    b.register()
    yield a, b
    _cleanup(CONN, "ha-race")          # keep the other modules' workers away from these test batches


def _cleanup(*conns: str) -> None:
    from alelt_hub.db import batch, get_engine, job
    from alelt_hub.ha_models import capture_lease

    with get_engine().begin() as c:
        ids = [str(r[0]) for r in c.execute(select(batch.c.id).where(batch.c.connection_id.in_(conns)))]
        if ids:
            c.execute(delete(job).where(job.c.dedup_key.in_([f"decode:{i}" for i in ids])))
            c.execute(delete(batch).where(batch.c.connection_id.in_(conns)))
        c.execute(delete(capture_lease).where(capture_lease.c.connection_id.in_(conns)))


def lease(client: Any, ag: FakeAgent, op: str, **kw: Any) -> Any:
    body = {"agent_id": ag.agent_id, "connection_id": ag.connection_id, **kw}
    return client.post(f"/agent/v1/lease/{op}", headers=ag.h, json=body)


def ship(client: Any, ag: FakeAgent, n: int, epoch: int | None, from_pos: str | None = None) -> Any:
    hdr, body = ag.make_batch([ev(n, f"T{n}", "INSERT", "S.KEYED", after={"ID": n}), ev(n + 1, f"T{n}", "COMMIT")],
                              from_pos=from_pos)
    h = {**ag.h, "X-ALELT-Batch": hdr.model_dump_json()}
    if epoch is not None:
        h["X-ALELT-Lease-Epoch"] = str(epoch)
    r = client.post("/agent/v1/blocks", headers=h, content=body)
    if r.status_code == 200:
        ag.next_from = hdr.next_pos.text  # type: ignore[union-attr]
    return r


def test_lease_lifecycle_and_fencing(client: Any, pair: tuple[FakeAgent, FakeAgent], clock: Clock) -> None:
    a, b = pair
    st = client.get("/agent/v1/lease", params={"connection_id": CONN}, headers=a.h).json()
    assert st["granted"] is False and st["holder"] is None                 # no lease row: standalone
    assert ship(client, a, 1, None).status_code == 200                      # standalone: header ignored

    g = lease(client, a, "acquire", ttl_s=10).json()
    assert g["granted"] and g["holder"] == "ha-a" and g["epoch"] == 1 and not g["takeover"]
    assert g["resume_from"]["text"] == lri(2)                               # hub durable position
    e1, lid = g["epoch"], g["lease_id"]
    again = lease(client, a, "acquire", ttl_s=10).json()                    # own lease: same epoch / lease_id
    assert again["granted"] and again["epoch"] == e1 and again["lease_id"] == lid

    sb = lease(client, b, "acquire", ttl_s=10).json()                       # live lease of someone else
    assert sb["granted"] is False and sb["holder"] == "ha-a" and sb["retry_after_s"] >= 1
    assert lease(client, b, "renew", epoch=e1, lease_id=lid).status_code == 409   # non-holder renew
    r = lease(client, a, "renew", epoch=e1 + 5, lease_id=lid)               # stale / wrong epoch
    assert r.status_code == 409 and r.json()["detail"]["error"] == "lease_lost"
    rn = lease(client, a, "renew", epoch=e1, lease_id=lid)
    assert rn.status_code == 200 and rn.json()["granted"] and rn.json()["epoch"] == e1

    # fencing: only the holder with the current epoch
    assert ship(client, a, 3, e1).status_code == 200
    r = ship(client, a, 5, None)                                            # holder without header
    assert r.status_code == 423 and r.json()["detail"]["error"] == "lease_fenced"
    r = ship(client, a, 5, e1 - 1)                                          # holder with a stale epoch
    assert r.status_code == 423
    b.next_from = a.next_from
    r = ship(client, b, 5, e1)                                              # standby with the right epoch
    assert r.status_code == 423 and r.json()["detail"]["holder"] == "ha-a"
    st = client.get("/agent/v1/lease", params={"connection_id": CONN}, headers=b.h).json()
    assert st["granted"] and st["holder"] == "ha-a" and st["epoch"] == e1

    # takeover after the TTL: epoch + 1, resume at the hub durable position, old holder fenced
    clock.advance(11)
    t = lease(client, b, "acquire", ttl_s=10).json()
    assert t["granted"] and t["takeover"] and t["holder"] == "ha-b" and t["epoch"] == e1 + 1
    assert t["lease_id"] != lid and t["resume_from"]["text"] == lri(4)
    assert lease(client, a, "renew", epoch=e1, lease_id=lid).status_code == 409
    assert ship(client, a, 5, e1).status_code == 423                        # split brain attempt
    assert lease(client, a, "acquire", ttl_s=10).json()["granted"] is False  # old holder is standby now
    b.next_from = t["resume_from"]["text"]
    assert ship(client, b, 5, t["epoch"]).status_code == 200                # new holder continues the chain

    # release -> the standby takes over at once
    rel = lease(client, b, "release", epoch=t["epoch"]).json()
    assert rel["released"] is True
    assert lease(client, a, "release").json()["released"] is False          # non-holder cannot release
    t2 = lease(client, a, "acquire", ttl_s=10).json()
    assert t2["granted"] and t2["takeover"] and t2["epoch"] == e1 + 2 and t2["resume_from"]["text"] == lri(6)

    from alelt_hub.db import audit_event, get_engine

    with get_engine().connect() as c:
        acts = [r[0] for r in c.execute(select(audit_event.c.action).where(audit_event.c.target == CONN)
                                        .order_by(audit_event.c.id))]
    assert acts.count("agent.lease.acquire") == 1 and acts.count("agent.lease.takeover") == 2
    assert "agent.lease.release" in acts


def test_lease_clear_back_to_standalone(client: Any, pair: tuple[FakeAgent, FakeAgent], clock: Clock) -> None:
    from alelt_hub import ha
    from alelt_hub.db import get_engine

    a, _ = pair
    g = lease(client, a, "acquire", ttl_s=10).json()
    assert g["granted"]
    with get_engine().begin() as c, pytest.raises(RuntimeError):
        ha.clear_lease(c, CONN, "test")                                     # refused while live
    assert lease(client, a, "release", epoch=g["epoch"]).json()["released"]
    with get_engine().begin() as c:
        assert ha.clear_lease(c, CONN, "test") is True
    a.next_from = lri(6)
    assert ship(client, a, 7, None).status_code == 200                      # standalone again: no header needed


def test_unknown_agent_and_connection(client: Any, pair: tuple[FakeAgent, FakeAgent]) -> None:
    a, _ = pair
    h = a.h
    r = client.post("/agent/v1/lease/acquire", headers=h, json={"agent_id": "nobody", "connection_id": CONN})
    assert r.status_code == 404
    r = client.post("/agent/v1/lease/acquire", headers=h, json={"agent_id": "ha-a", "connection_id": "fakeconn"})
    assert r.status_code == 403
    assert client.get("/agent/v1/lease", params={"connection_id": "nope"}, headers=h).status_code == 404
    assert client.post("/agent/v1/lease/acquire", json={"agent_id": "ha-a", "connection_id": CONN},
                       headers={"X-ALELT-Protocol": "1", "X-ALELT-Agent-Token": "bad"}).status_code == 401


def test_concurrent_acquire_single_holder(client: Any, clock: Clock) -> None:
    agents = [FakeAgent(client, TOKEN, agent_id=f"race-{i}", connection_id="ha-race") for i in range(4)]
    for ag in agents:
        ag.register()
    out: list[dict[str, Any]] = []
    lock = threading.Lock()

    def go(ag: FakeAgent) -> None:
        for _ in range(3):
            r = lease(client, ag, "acquire", ttl_s=30)
            with lock:
                out.append(r.json())

    ts = [threading.Thread(target=go, args=(ag,)) for ag in agents]
    for t in ts:
        t.start()
    for t in ts:
        t.join(30)
    holders = {g["holder"] for g in out if g["granted"]}
    assert len(holders) == 1 and {g["epoch"] for g in out if g["granted"]} == {1}


# ------------------------------------------------------------------ alelt_common.lease_client against the real hub
def test_lease_client_failover(client: Any, clock: Clock) -> None:
    conn = f"ha-lc-{uuid.uuid4().hex[:6]}"
    ags = [FakeAgent(client, TOKEN, agent_id=f"{conn}-{x}", connection_id=conn) for x in "ab"]
    for ag in ags:
        ag.register()

    def post_for(ag: FakeAgent) -> Any:
        def post(path: str, body: dict[str, Any]) -> tuple[int, Any]:
            r = client.post(path, headers=ag.h, json=body)
            return r.status_code, r.json()
        return post

    la, lb = (LeaseClient(post_for(ag), ag.agent_id, conn, ttl_s=10, renew_s=3, clock=lambda: clock.mono)
              for ag in ags)
    assert la.tick().kind == ACTIVE and la.may_read() and la.headers() == {"X-ALELT-Lease-Epoch": "1"}
    assert lb.tick().kind == "none" and not lb.may_read() and lb.headers() == {}
    clock.advance(3)
    assert la.tick().kind == RENEWED and lb.tick().kind == "none"
    assert ship(client, ags[0], 1, int(la.headers()["X-ALELT-Lease-Epoch"])).status_code == 200
    # active host dies (no more ticks); after ttl - renew it would self-fence, after ttl the standby takes over
    clock.advance(11)
    ev_b = lb.tick()
    assert ev_b.kind == TAKEOVER and ev_b.epoch == 2 and ev_b.resume_from["text"] == lri(2)  # type: ignore[index]
    assert not la.may_read()                                                 # self-fenced by time alone
    assert la.tick().kind == LOST and la.headers() == {}
    ags[1].next_from = ev_b.resume_from["text"]                              # type: ignore[index]
    assert ship(client, ags[1], 3, ev_b.epoch).status_code == 200
    assert ship(client, ags[0], 3, 1).status_code == 423
    assert la.on_lost().kind == "none"                                       # already standby
    assert lb.release() is True and not lb.may_read()
    clock.advance(3)
    assert la.tick().kind == TAKEOVER                                        # planned switchover, no TTL wait
    _cleanup(conn)


def test_lease_state_rules() -> None:
    s = LeaseState(True, ttl_s=30, renew_s=10)
    assert not s.active and s.due(0) and s.header_epoch() == -1
    assert s.on_grant({"granted": True, "epoch": 4, "lease_id": "x", "ttl_s": 30}, 0).kind == ACTIVE
    assert s.header_epoch() == 4 and not s.due(9) and s.due(10)
    assert s.on_grant({"granted": True, "epoch": 4, "lease_id": "x"}, 10).kind == RENEWED
    assert s.on_error(15).kind == "none" and s.active                        # transient error, within margin
    assert s.on_error(30).kind == LOST and not s.active                      # 10 + (30 - 10) reached: self-fence
    assert s.on_grant({"granted": False, "holder": "o", "retry_after_s": 2}, 31).kind == "none"
    assert s.next_at == 33
    s2 = LeaseState(True, ttl_s=10, renew_s=9)                               # renew > ttl/2 -> ttl/3
    assert s2.renew_s == 3
    s2.on_grant({"granted": True, "epoch": 1, "lease_id": "a", "ttl_s": 6}, 0)
    assert s2.ttl_s == 6 and s2.renew_s == 3
    off = LeaseState(False)
    assert off.active and off.header_epoch() == -1 and off.on_lost(0).kind == "none"
