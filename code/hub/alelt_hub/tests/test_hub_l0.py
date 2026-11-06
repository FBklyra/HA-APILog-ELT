"""L0 harness (shortened): fake agent registers, heartbeats, ships good/bad/duplicate batches, receives commands;
worker decodes -> assembles -> loads into the Postgres destination; replay is idempotent; RBAC per role."""
from __future__ import annotations

import os
from typing import Any

import psycopg
import pytest

from alelt_hub.testing import FakeAgent, ev, fake_catalog, lri

from .conftest import TOKEN, drain, need_landing, role


@pytest.fixture(scope="module")
def agent(client: Any) -> FakeAgent:
    a = FakeAgent(client, TOKEN)
    assert a.register()["accepted"] is True
    assert a.catalog(fake_catalog(a.connection_id))["tables"] == 2
    return a


def landing(sql: str) -> list[tuple[Any, ...]]:
    need_landing()
    with psycopg.connect(os.environ["ALELT_DEST_URL"]) as c:
        return c.execute(sql).fetchall()  # type: ignore[arg-type]


def test_protocol_and_identity(client: Any) -> None:
    body = {"agent_id": "x", "connection_id": "y", "engine": "fake", "version": "1", "hostname": "h", "database": "d"}
    assert client.post("/agent/v1/register", json=body, headers={"X-ALELT-Agent-Token": TOKEN}).status_code == 426
    r = client.post("/agent/v1/register", json=body, headers={"X-ALELT-Protocol": "1", "X-ALELT-Agent-Token": "bad"})
    assert r.status_code == 401 and r.json()["error"]["code"] == "unauthenticated"


def test_heartbeat(client: Any, agent: FakeAgent) -> None:
    r = client.post("/agent/v1/heartbeat", headers=agent.h, json={
        "agent_id": agent.agent_id, "state": "CAPTURING", "spool_bytes": 10, "at": "2026-11-03T00:00:00Z",
        "cpu_pct": 1.5, "mem_mb": 20})
    assert r.status_code == 200


def test_batches_end_to_end(client: Any, agent: FakeAgent, worker: Any) -> None:
    # T1 committed in batch 1; T2 spans batches 1-2; T3 rolled back (with compensation); bad line quarantined
    b1 = [ev(1, "T1", "INSERT", "S.KEYED", after={"ID": 1, "NAME": "one", "AMT": "1.10", "SECRET": "pw"}),
          ev(2, "T2", "INSERT", "S.KEYED", after={"ID": 2, "NAME": "two", "AMT": "2.20", "SECRET": None}),
          ev(3, "T1", "COMMIT"),
          ev(4, "T3", "INSERT", "S.KEYED", after={"ID": 99, "NAME": "ROLLED BACK"})]
    r = agent.send(b1)
    assert r.status_code == 200 and r.json()["durable_pos"]["text"] == lri(4)
    hdr, body = agent.make_batch([ev(5, "T3", "COMPENSATION", "S.KEYED"), ev(6, "T3", "ROLLBACK"),
                                  ev(7, "T2", "UPDATE", "S.KEYED", before={"ID": 2}, after={"ID": 2, "NAME": "TWO"}),
                                  ev(8, "T2", "INSERT", "S.LOG", after={"MSG": "hello", "N": 1}),
                                  ev(9, "T2", "COMMIT")])
    raw_ok = agent.post(hdr, body)
    assert raw_ok.status_code == 200
    dup = agent.post(hdr, body)                                     # idempotent on batch_id
    assert dup.status_code == 200 and dup.json()["duplicate"] is True
    # bad sha256
    h2, b2 = agent.make_batch([ev(10, "T4", "INSERT", "S.KEYED", after={"ID": 4})])
    bad = h2.model_copy(update={"sha256": "0" * 64})
    assert agent.post(bad, b2).status_code == 422
    # gap: from_pos must equal previous next_pos
    h3, b3 = agent.make_batch([ev(20, "T5", "COMMIT")], from_pos=lri(15))
    g = agent.post(h3, b3)
    assert g.status_code == 409 and g.json()["detail"]["expected_from"] == lri(9)

    assert drain(worker) >= 2
    rows = dict((r[0], r[1:]) for r in landing(
        'SELECT id, name, secret, _alelt_deleted FROM land.keyed ORDER BY id'))
    assert set(rows) == {1, 2}                                       # 99 rolled back, never emitted
    assert rows[2][0] == "TWO" and len(rows[1][1]) == 64             # hashed SECRET (placeholder, OQ-03)
    assert landing("SELECT msg, length(_alelt_id) FROM land.log") == [("hello", 32)]
    ck = landing("SELECT position FROM alelt_control.checkpoint WHERE stream='fakeconn'")
    assert ck == [(lri(9),)]

    # bad line -> quarantine (R7), batch still loads
    q = agent.send([{"garbage": True}, ev(11, "T6", "INSERT", "S.KEYED", after={"ID": 6, "NAME": "six"}),
                    ev(12, "T6", "COMMIT")])
    assert q.status_code == 200
    drain(worker)
    qi = client.get("/api/v1/quarantine", headers=role("Viewer")).json()
    assert qi["total"] == 1 and qi["items"][0]["raw_ref"]["offset"] == 0
    assert landing("SELECT count(*) FROM land.keyed")[0][0] == 3


def test_restart_replay_idempotent(client: Any, agent: FakeAgent, worker: Any) -> None:
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    # open txn T7 across a batch boundary, then "crash" the worker before the commit arrives
    agent.send([ev(13, "T7", "INSERT", "S.KEYED", after={"ID": 7, "NAME": "seven"})])
    drain(worker)
    mirror = landing("SELECT 1")  # landing reachable
    assert mirror
    for _ in range(3):                                               # three fresh workers = replay x3
        agent_rows_before = landing("SELECT count(*), max(_alelt_synced) FROM land.keyed")
        fresh = Worker(get_engine(), get_settings(), "fresh")
        fresh.pipeline("fakeconn").rebuild()
        fresh.pipelines["fakeconn"].process_pending()
        assert landing("SELECT count(*), max(_alelt_synced) FROM land.keyed") == agent_rows_before
    agent.send([ev(14, "T7", "COMMIT")])
    fresh = Worker(get_engine(), get_settings(), "fresh2")           # restart: must rebuild T7 from spool
    drain(fresh)
    assert landing("SELECT name FROM land.keyed WHERE id = 7") == [("seven",)]
    assert landing("SELECT count(*) FROM alelt_control.checkpoint WHERE stream LIKE 'fakeconn%'") == [(1,)]
    audits = landing("SELECT count(*) FROM alelt_control.audit_batch")[0][0]
    fresh.pipeline("fakeconn").rebuild()
    fresh.pipelines["fakeconn"].process_pending()                    # full replay -> no new audit row
    assert landing("SELECT count(*) FROM alelt_control.audit_batch")[0][0] == audits


def test_commands_long_poll_and_ws(client: Any, agent: FakeAgent) -> None:
    aid = agent.agent_id
    url = f"/api/v1/agents/{aid}/commands"
    adm = role("PlatformAdmin")
    assert client.post(url, headers=role("Viewer"), json={"name": "RESUME", "reason": "test"}).status_code == 403
    assert client.post(url, headers=role("DataOperator"), json={"name": "RESUME", "reason": "test"}).status_code == 403
    r = client.post(url, headers=adm, json={"name": "PAUSE", "reason": "test"})
    assert r.status_code == 400 and r.json()["error"]["code"] == "CONFIRM_MISMATCH"
    r = client.post(url, headers=adm, json={"name": "RUN_SQL", "reason": "test"})
    assert r.status_code == 400 and r.json()["error"]["code"] == "COMMAND_NOT_WHITELISTED"
    assert client.post(url, headers=adm, json={"name": "START_CAPTURE", "reason": "test"}).status_code == 400
    r = client.post(url, headers=adm, json={"name": "PROBE", "args": {"name": "rm -rf"}, "reason": "test"})
    assert r.status_code == 400 and r.json()["error"]["code"] == "COMMAND_NOT_WHITELISTED"
    r = client.post(url, headers=adm, json={"name": "PAUSE", "reason": "maintenance", "confirm_text": f"PAUSE {aid}"})
    assert r.status_code == 200 and r.json()["status"] == "QUEUED"
    cmds = client.get(f"/agent/v1/commands?agent_id={aid}&wait=0", headers=agent.h).json()
    assert [c["name"] for c in cmds] == ["PAUSE"]
    assert client.post(f"/agent/v1/commands/{cmds[0]['command_id']}/ack?agent_id={aid}", headers=agent.h,
                       json={"ok": True}).status_code == 200
    client.post(url, headers=adm, json={"name": "SET_LIMITS", "args": {"cpu_pct": 15}, "reason": "slow down"})
    with client.websocket_connect(f"/agent/v1/control?agent_id={aid}", headers=agent.h) as ws:
        msg = ws.receive_json()
        assert msg["type"] == "command" and msg["command"]["name"] == "SET_LIMITS"
        assert msg["command"]["args"]["cpu_pct"] == 15
        ws.send_json({"type": "ack", "command_id": msg["command"]["command_id"], "ok": True})
    hist = client.get(url, headers=adm).json()
    assert {h["status"] for h in hist} == {"ACKED"} or len(hist) == 2


def test_agent_events(client: Any, agent: FakeAgent) -> None:
    aid = agent.agent_id
    ev_ = {"agent_id": aid, "type": "PAUSED_BY_GOVERNOR", "command_id": None,
           "detail": {"message": "host_cpu 91.0% >= 85%"}, "at": "2026-11-03T12:00:00.000Z"}
    assert client.post("/agent/v1/events", headers=agent.h, json=ev_).json() == {"ok": True}
    assert client.post("/agent/v1/events", headers=agent.h, json={**ev_, "type": "rm -rf"}).status_code == 422
    assert client.post("/agent/v1/events", headers=agent.h, json={**ev_, "extra": 1}).status_code == 422
    assert client.post("/agent/v1/events", headers=agent.h, json={**ev_, "agent_id": "nobody"}).status_code == 404
    assert client.post("/agent/v1/events", headers={"X-ALELT-Protocol": "1"}, json=ev_).status_code in (401, 403)
    audit = client.get("/api/v1/audit", headers=role("Auditor")).json()
    items = audit["items"] if isinstance(audit, dict) else audit
    assert any(i.get("action") == "agent.event.PAUSED_BY_GOVERNOR" for i in items)


def test_console_reads_and_rbac(client: Any, agent: FakeAgent) -> None:
    need_landing()                                                   # reads the batches loaded above
    for path in ("/overview", "/tables", "/batches", "/recon", "/ladder", "/me"):
        assert client.get(f"/api/v1{path}", headers=role("Viewer")).status_code == 200, path
    assert client.get("/api/v1/agents", headers=role("PlatformAdmin")).status_code == 200
    assert client.get("/api/v1/tables").status_code == 401
    assert client.get("/api/v1/audit", headers=role("Viewer")).status_code == 403
    audit = client.get("/api/v1/audit", headers={**role("Auditor"), "X-ALELT-Correlation-Id": "c-1"}).json()
    assert any(a["action"] == "agent.command.PAUSE" for a in audit["items"])
    tables = client.get("/api/v1/tables", headers=role("Viewer")).json()["items"]
    keyed = next(t for t in tables if t["table"]["name"] == "KEYED")
    body = {"reason": "disable for test", "enabled": False, "columns": [{"name": "NAME", "enabled": True,
                                                                         "hashed": True}]}
    assert client.put(f"/api/v1/tables/{keyed['id']}/config", headers=role("Viewer"), json=body).status_code == 403
    d = client.put(f"/api/v1/tables/{keyed['id']}/config", headers=role("DataOperator"), json=body)
    assert d.status_code == 200 and d.json()["enabled"] is False and d.json()["revision"] >= 1
    assert next(c for c in d.json()["columns"] if c["name"] == "NAME")["hashed"] is True
    revs = client.get("/api/v1/revisions", headers=role("DataOperator")).json()
    assert revs[0]["current"] is True
    ov = client.get("/api/v1/overview", headers=role("Viewer")).json()
    assert ov["output_profile"] == "alelt" and ov["last_batch"] is not None
    assert ov["system_columns"] == {"synced": "_ALELT_SYNCED", "deleted": "_ALELT_DELETED"}
    # restore the shared fakeconn state for later modules (S.KEYED enabled, NAME not hashed): test isolation
    restore = {"reason": "restore after test", "enabled": True,
               "columns": [{"name": "NAME", "enabled": True, "hashed": False}]}
    r = client.put(f"/api/v1/tables/{keyed['id']}/config", headers=role("DataOperator"), json=restore)
    assert r.status_code == 200 and r.json()["enabled"] is True


def test_ops_endpoints(client: Any) -> None:
    need_landing()                                                   # metrics produced by the batch tests
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").status_code == 200
    m = client.get("/metrics").text
    for name in ("alelt_batches_received_total", "alelt_queue_depth", "alelt_agent_last_heartbeat_timestamp_seconds",
                 "alelt_spool_bytes", "alelt_capture_lag_seconds"):
        assert name in m, name
    assert 'status="sha_mismatch"' in m and "alelt_metrics_collector_errors_total" not in m
