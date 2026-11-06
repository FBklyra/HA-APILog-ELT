"""DESIGN-REVIEW §4.4 scenarios that need no external account (in-process hub, Postgres landing):
#4/#5 volume + long open transaction spanning batches, forced spill, worker "killed" (abandoned) mid-transaction and a
fresh worker rebuilding from the spool -> exact parity; #6 schema drift mid-stream (column added at the source before
the catalog refresh) -> never silently dropped; #7 clock skew -> agent-reported clocks never drive fencing.
Named `zz` so it runs after the shared-connection modules (relative assertions only)."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg
import pytest
from sqlalchemy import select

from alelt_hub.testing import FakeAgent, ev, fake_catalog

from .conftest import TOKEN, drain, need_landing, role


def landing(sql: str) -> list[tuple[Any, ...]]:
    need_landing()
    with psycopg.connect(os.environ["ALELT_DEST_URL"]) as c:
        return c.execute(sql).fetchall()  # type: ignore[arg-type]


@pytest.fixture(scope="module")
def agent(client: Any, worker: Any) -> FakeAgent:
    from alelt_hub.db import batch, get_engine

    a = FakeAgent(client, TOKEN)
    a.register()
    a.catalog(fake_catalog(a.connection_id))
    drain(worker)
    with get_engine().connect() as c:
        a.next_from = c.execute(select(batch.c.next_lri).where(batch.c.connection_id == a.connection_id)
                                .order_by(batch.c.seq.desc()).limit(1)).scalar() or a.next_from
    return a


def test_long_txn_spill_and_worker_loss(client: Any, agent: FakeAgent) -> None:
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    base, rows, per_batch = 0x8000000, 3000, 600
    ids = list(range(100_000, 100_000 + rows))
    first = Worker(get_engine(), get_settings(), "vol-1")
    first.pipeline(agent.connection_id).asm.byte_budget = 64 << 10     # force the spill path
    pos = base
    for i in range(0, rows, per_batch):                                # ONE transaction over 5 batches
        evs = [ev(pos + k, "BIG", "INSERT", "S.KEYED", after={"ID": n, "NAME": f"v{n}", "AMT": "1.00"})
               for k, n in enumerate(ids[i:i + per_batch])]
        pos += per_batch
        assert agent.send(evs).status_code == 200
        if i == per_batch * 2:
            drain(first)
            assert first.pipelines[agent.connection_id].asm.stats.spills >= 1
            del first                                                  # worker lost with the txn open
    assert agent.send([ev(pos, "BIG", "COMMIT")]).status_code == 200
    fresh = Worker(get_engine(), get_settings(), "vol-2")              # must rebuild BIG from restart_seq
    drain(fresh)
    got = landing(f"SELECT count(*), min(id), max(id) FROM land.keyed WHERE id >= {ids[0]} AND id <= {ids[-1]}")
    assert got == [(rows, ids[0], ids[-1])]


# REQ-26 fixed (stream P, replay.drift_check): unknown column -> quarantine + integrity hold, never dropped
def test_schema_drift_mid_stream_never_silent(client: Any, agent: FakeAgent, worker: Any) -> None:
    """A column the catalog does not know yet (ALTER ADD before SNAPSHOT_CATALOG) must land, be quarantined, or raise a
    schema-change event - it must never vanish while the batch shows LOADED."""
    from alelt_hub.db import get_engine, quarantine_event, schema_change_event

    with get_engine().connect() as c:
        q0 = len(c.execute(select(quarantine_event.c.id)).all())
        s0 = len(c.execute(select(schema_change_event.c.id)).all())
    base = 0x9000000
    assert agent.send([ev(base, "DR", "INSERT", "S.KEYED", after={"ID": 200_001, "NAME": "drift", "AMT": "2.00",
                                                                   "NEWCOL": "surprise"}),
                       ev(base + 1, "DR", "COMMIT")]).status_code == 200
    drain(worker)
    cols = {r[0] for r in landing("SELECT column_name FROM information_schema.columns "
                                  "WHERE table_schema='land' AND table_name='keyed'")}
    landed = "newcol" in cols and landing("SELECT newcol FROM land.keyed WHERE id = 200001") == [("surprise",)]
    with get_engine().connect() as c:
        quarantined = len(c.execute(select(quarantine_event.c.id)).all()) > q0
        drift_event = len(c.execute(select(schema_change_event.c.id)).all()) > s0
    assert landed or quarantined or drift_event, "new column value silently dropped"


def test_agent_clock_skew_never_drives_fencing(client: Any) -> None:
    """Lease expiry and fencing use the hub clock only: an agent whose wall clock is years off still gets a lease that
    expires TTL seconds after the hub's now, and its heartbeat timestamps are not trusted for liveness decisions."""
    a = FakeAgent(client, TOKEN, agent_id="skew-a", connection_id="skewconn")
    a.register()
    future = (datetime.now(timezone.utc) + timedelta(days=3650)).isoformat()
    hb = client.post("/agent/v1/heartbeat", headers=a.h, json={"agent_id": a.agent_id, "state": "CAPTURING",
                                                               "spool_bytes": 0, "at": future})
    assert hb.status_code == 200
    g = client.post("/agent/v1/lease/acquire", headers=a.h,
                    json={"agent_id": a.agent_id, "connection_id": "skewconn", "ttl_s": 10}).json()
    assert g["granted"]
    exp = datetime.fromisoformat(str(g["expires_at"]).replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    assert now < exp <= now + timedelta(seconds=15)                     # hub clock + TTL, not the agent's clock
    agents = client.get("/api/v1/agents", headers=role("PlatformAdmin")).json()
    items = agents["items"] if isinstance(agents, dict) else agents
    me = next((x for x in items if x.get("agent_id", x.get("id")) == "skew-a"), None)
    if me is not None and me.get("last_seen"):
        seen = datetime.fromisoformat(str(me["last_seen"]).replace("Z", "+00:00"))
        assert seen <= now + timedelta(seconds=5)                       # last_seen = receipt time on the hub
    client.post("/agent/v1/lease/release", headers=a.h, json={"agent_id": a.agent_id, "connection_id": "skewconn"})
