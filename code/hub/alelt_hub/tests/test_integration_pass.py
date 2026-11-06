"""Integration pass: REQ-10/11/14/16/20/21/23, snapshot wiring (remote + edge), schedule, destination selection,
verify defaults + verify/resync/reprocess consumers, wizard progress with the stored secret."""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from alelt_hub.testing import FakeAgent, ev, fake_catalog, lri

from . import fakes
from .conftest import TOKEN, drain, need_landing, role

ADM = role("PlatformAdmin")


def landing(sql: str, args: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    need_landing()
    with psycopg.connect(os.environ["ALELT_DEST_URL"]) as c:
        return c.execute(sql, args).fetchall()  # type: ignore[arg-type]


@pytest.fixture()
def remote(client: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    """A fresh remote connection over fakes.FakeRemote (registered in ALELT_SOURCES for this test)."""
    from alelt_hub import store
    from alelt_hub.settings import get_settings

    cid = f"rc{uuid.uuid4().hex[:6]}"
    get_settings().sources["fakeremote"] = "alelt_hub.tests.fakes:FakeRemote"
    store.source_connector.cache_clear()
    fakes.WORLDS[cid] = fakes.World()
    monkeypatch.setenv("FAKE_REMOTE_PW", fakes.WORLDS[cid].password)
    monkeypatch.setenv("ALELT_CAPTURE_CONNECTIONS", json.dumps(
        {cid: {"engine": "fakeremote", "settings": {"world": cid}, "password_env": "FAKE_REMOTE_PW"}}))
    from alelt_capture import receipt
    from alelt_hub.db import get_engine

    receipt.ensure_registered(get_engine(), cid, "fakeremote")
    receipt.ingest_catalog(get_engine(), fakes.catalog(cid))
    return cid


def _runner(cid: str) -> Any:
    from alelt_capture.config import connections
    from alelt_capture.runner import CaptureConfig, CaptureRunner
    from alelt_hub.db import get_engine

    return CaptureRunner(get_engine(), cid, connections(get_engine())[cid].factory(),
                         CaptureConfig(batch_ms=0, retry_s=0.1))


# ------------------------------------------------------------------ REQ-10 / REQ-11
def test_start_capture_resume_and_heartbeat_metrics(client: Any) -> None:
    a = FakeAgent(client, TOKEN, agent_id="req10-agent", connection_id="req10")
    a.register()
    url = f"/api/v1/agents/{a.agent_id}/commands"
    assert client.post(url, headers=ADM, json={"name": "START_CAPTURE", "reason": "t"}).status_code == 400
    assert client.post(url, headers=ADM, json={"name": "START_CAPTURE", "args": {"from_pos": ""},
                                               "reason": "t"}).status_code == 400
    r = client.post(url, headers=ADM, json={"name": "START_CAPTURE", "args": {"resume": True}, "reason": "resume"})
    assert r.status_code == 200
    cmds = client.get(f"/agent/v1/commands?agent_id={a.agent_id}&wait=0", headers=a.h).json()
    assert cmds[0]["name"] == "START_CAPTURE" and cmds[0]["args"] == {}     # agent resumes on its own position
    hb = {"agent_id": a.agent_id, "state": "CAPTURING", "at": "2026-11-04T00:00:00Z", "cpu_pct": 2,
          "metrics": {"alelt_agent_spool_files": 3, "alelt_agent_read_lag_seconds": 1.5, "Bad Name": 1}}
    assert client.post("/agent/v1/heartbeat", headers=a.h, json=hb).status_code == 200
    m = client.get("/metrics").text
    assert 'alelt_agent_spool_files{agent_id="req10-agent"} 3.0' in m and "Bad Name" not in m


# ------------------------------------------------------------------ snapshot (remote) + REQ-21 + REQ-23 + verify
def test_remote_initial_load_catch_up_verify(client: Any, remote: str) -> None:
    need_landing()
    from sqlalchemy import select

    from alelt_hub import holds
    from alelt_hub.db import get_engine, hub_setting
    from alelt_hub.settings import get_settings
    from alelt_hub.snapshots import SnapshotService
    from alelt_hub.worker import Worker

    cid, w_ = remote, fakes.WORLDS[remote]
    for i in range(1, 6):
        w_.txn([("INSERT", fakes.item(i, f"n{i}", f"{i}.125", bytes([i, 0, 255])))])
    w = Worker(get_engine(), get_settings(), "int-worker")
    started = SnapshotService(get_engine(), w.pipeline(cid)).start()
    assert started == ["S.ITEMS"] and holds.held_tables(get_engine(), cid) == {"S.ITEMS"}
    with get_engine().connect() as c:
        st = c.execute(select(hub_setting.c.value).where(hub_setting.c.key == f"capture.start:{cid}")).scalar()
    assert st["position"] == fakes.p(w_.pos)                                  # REQ-21 recorded
    w_.txn([("UPDATE", {"ID": 1, "NAME": "after"}), ("INSERT", fakes.item(6, "six", "6.5", b"\x01"))])
    assert _runner(cid).run(idle_exit_s=0.3) == "idle"
    assert w_.stream_from[0] == st["position"]                                # runner started at the snapshot
    drain(w)
    rows = landing(f"SELECT id, name, amt, data, ts FROM {cid}.items ORDER BY id")
    assert [r[0] for r in rows] == [1, 2, 3, 4, 5, 6] and rows[0][1] == "after"
    assert rows[1][2] == Decimal("2.125") and bytes(rows[1][3]) == bytes([2, 0, 255])       # REQ-23 typed
    assert rows[1][4] == datetime(2026, 1, 2, 3, 4, 5, 123456)
    hold = holds.rows(get_engine(), cid, "snapshot")[0]
    assert hold["state"] == "IN_SYNC" and not hold["held"]                    # catch-up + clean verify

    # verify consumer via the console (default suite), then a located mismatch
    from alelt_hub.db import source_table

    with get_engine().connect() as c:
        tid = c.execute(select(source_table.c.id).where(source_table.c.connection_id == cid)).scalar()
    r = client.post("/api/v1/recon/runs", headers=role("DataOperator"),
                    json={"table_ids": [str(tid)], "check_kind": "COLUMN_CHECKSUM", "reason": "check"})
    assert r.status_code == 200, r.text
    drain(w)
    run = next(x for x in client.get("/api/v1/recon", headers=role("Viewer")).json() if x["id"] == r.json()["id"])
    assert run["finished_at"] and run["results"][0]["outcome"] == "MATCH"
    landing(f"DELETE FROM {cid}.items WHERE id = 4 RETURNING id")
    r = client.post("/api/v1/recon/runs", headers=role("DataOperator"),
                    json={"table_ids": [str(tid)], "check_kind": "ROW_COUNT", "reason": "check"})
    drain(w)
    run = next(x for x in client.get("/api/v1/recon", headers=role("Viewer")).json() if x["id"] == r.json()["id"])
    res = run["results"][0]
    assert res["outcome"] == "MISMATCH" and any(d.get("key") == '["4"]' for d in res["drill"])

    # resync consumer: snapshot again -> row 4 back
    from alelt_hub import jobs

    with get_engine().begin() as c:
        jobs.enqueue(c, "resync", {"table": "S.ITEMS", "connection_id": cid, "table_id": str(tid)})
    drain(w)
    assert landing(f"SELECT count(*) FROM {cid}.items WHERE NOT _alelt_deleted") == [(6,)]


# ------------------------------------------------------------------ REQ-16: source policy + reprocess
def test_source_policy_and_reprocess(client: Any, remote: str) -> None:
    need_landing()
    from sqlalchemy import select

    from alelt_hub import jobs
    from alelt_hub.db import get_engine, hub_setting, quarantine_event
    from alelt_hub.dialect import insert
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    cid, w_ = remote, fakes.WORLDS[remote]
    with get_engine().begin() as c:
        c.execute(insert(hub_setting).values(key="diag.source_policy", value={
            "S.ITEMS.NAME": {"OUT_OF_RANGE": {"option": "QUARANTINE_SKIP"}},
            "S.ITEMS.TS": {"INVALID_DATE": {"option": "COERCE"}}}).on_conflict_do_update(
            index_elements=["key"], set_={"value": {"S.ITEMS.NAME": {"OUT_OF_RANGE": {"option": "QUARANTINE_SKIP"}},
                                                    "S.ITEMS.TS": {"INVALID_DATE": {"option": "COERCE"}}}}))
    try:
        w_.txn([("INSERT", fakes.item(1, "ok")), ("INSERT", fakes.item(2, "way too long name")),
                ("INSERT", fakes.item(3, "old", ts=datetime(1800, 1, 1)))])
        assert _runner(cid).run(idle_exit_s=0.3) == "idle"
        w = Worker(get_engine(), get_settings(), "pol-worker")
        drain(w)
        assert landing(f"SELECT id, ts FROM {cid}.items ORDER BY id") == [
            (1, datetime(2026, 1, 2, 3, 4, 5, 123456)), (3, None)]          # 2 skipped, 3 coerced
        with get_engine().connect() as c:
            q = c.execute(select(quarantine_event.c.reason).where(quarantine_event.c.connection_id == cid)).all()
        assert any("QUARANTINE_SKIP: OUT_OF_RANGE in NAME" in r[0] for r in q)
        # policy changed to COERCE (truncate) -> reprocess from the first position re-applies row 2 (inclusive)
        with get_engine().begin() as c:
            from sqlalchemy import update

            c.execute(update(hub_setting).where(hub_setting.c.key == "diag.source_policy").values(value={
                "S.ITEMS.NAME": {"OUT_OF_RANGE": {"option": "COERCE"}}}))
            jobs.enqueue(c, "reprocess", {"table": "S.ITEMS", "connection_id": cid, "from_pos": fakes.p(1),
                                          "to_pos": fakes.p(w_.pos)})
        w.pipelines[cid].source_policy._at = 0
        drain(w)
        assert landing(f"SELECT id, name FROM {cid}.items ORDER BY id") == [(1, "ok"), (2, "way too lo"), (3, "old")]
    finally:
        with get_engine().begin() as c:
            from sqlalchemy import delete

            c.execute(delete(hub_setting).where(hub_setting.c.key == "diag.source_policy"))


# ------------------------------------------------------------------ REQ-20
def test_source_gap_stops_runner_with_incident(client: Any, remote: str) -> None:
    from sqlalchemy import select

    from alelt_capture.runner import CaptureError
    from alelt_diagnose.tables import incident
    from alelt_hub.db import get_engine

    fakes.WORLDS[remote].gap = True
    r = _runner(remote)
    with pytest.raises(CaptureError):
        r.run(idle_exit_s=0.3)
    assert r.state == "ERROR" and "gap" in (r.last_error or "")
    with get_engine().connect() as c:
        kinds = {k for k, in c.execute(select(incident.c.kind).where(incident.c.connection_id == remote))}
    assert "POSITION_GAP" in kinds


# ------------------------------------------------------------------ REQ-14 + schedule window
def test_worker_reads_pipeline_definition_and_schedule_window(client: Any) -> None:
    need_landing()
    from sqlalchemy import select

    from alelt_hub import holds
    from alelt_hub.db import get_engine, pipeline_definition
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker
    from alelt_loader.policy import TableSchedule

    cid = "schedconn"
    a = FakeAgent(client, TOKEN, agent_id="sched-agent", connection_id=cid)
    a.register()
    a.catalog(fake_catalog(cid))
    closed = {"start": "00:00", "end": "00:01", "tz": "UTC"}
    if datetime.now(timezone.utc).strftime("%H:%M") == "00:00":
        closed = {"start": "12:00", "end": "12:01", "tz": "UTC"}
    doc = {"connection_id": cid, "landing_schema": "SCHEDLAND", "output_profile": "alelt", "schemas": {"S": {
        "enabled": True, "tables": {"KEYED": {"enabled": True, "schedule": {"class": "daily", "window": closed}},
                                    "LOG": {"enabled": True}}}}}
    with get_engine().begin() as c:
        c.execute(pipeline_definition.insert().values(
            connection_id=cid, origin="wizard", environment="TEST", source_engine="fake",
            destination_engine="postgres", source={}, destination={}, tables_doc=doc, status="SHADOW",
            created_by="t"))
    w = Worker(get_engine(), get_settings(), "sched-worker")
    a.send([ev(1, "A", "INSERT", "S.LOG", after={"MSG": "first", "N": 1}), ev(2, "A", "COMMIT")])
    drain(w)                                                             # first tick: KEYED held from here
    assert landing("SELECT msg FROM schedland.log") == [("first",)]      # REQ-14: schema from the definition
    assert "S.KEYED" in holds.held_tables(get_engine(), cid)
    a.send([ev(3, "B", "INSERT", "S.KEYED", after={"ID": 1, "NAME": "night"}), ev(4, "B", "COMMIT")])
    drain(w)
    assert landing("SELECT to_regclass('schedland.keyed') IS NULL") == [(True,)]   # outside the window
    p = w.pipelines[cid]
    p.policy.tables["S.KEYED"].schedule = TableSchedule("daily", None, {"start": "00:00", "end": "00:00"})  # open
    doc["schemas"]["S"]["tables"]["KEYED"]["schedule"]["window"] = {"start": "00:00", "end": "00:00", "tz": "UTC"}
    with get_engine().begin() as c:                  # replays re-read the stored policy (HC-20): open it there too
        from alelt_hub import jobs

        c.execute(pipeline_definition.update().where(pipeline_definition.c.connection_id == cid)
                  .values(tables_doc=doc))

        jobs.enqueue(c, "decode", {"connection_id": cid})
    drain(w)
    assert landing("SELECT name FROM schedland.keyed") == [("night",)]   # released: replayed from the hold
    assert "S.KEYED" not in holds.held_tables(get_engine(), cid)
    a.send([ev(5, "C", "UPDATE", "S.KEYED", before={"ID": 1}, after={"ID": 1, "NAME": "day"}), ev(6, "C", "COMMIT")])
    drain(w)
    assert landing("SELECT name FROM schedland.keyed") == [("day",)]
    with get_engine().connect() as c:
        assert c.execute(select(pipeline_definition.c.status).where(
            pipeline_definition.c.connection_id == cid)).scalar() == "SHADOW"


# ------------------------------------------------------------------ edge snapshot via SNAPSHOT_TABLE
def test_edge_snapshot_command_and_chunks(client: Any) -> None:
    need_landing()
    from alelt_common.models import SnapshotChunkHeader, TableRef
    from alelt_common.util import compress
    from alelt_hub import holds
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.snapshots import SnapshotService
    from alelt_hub.worker import Worker

    cid = "edgesnap"
    a = FakeAgent(client, TOKEN, agent_id="edgesnap-agent", connection_id=cid)
    a.register()
    a.catalog(fake_catalog(cid))
    a.send([ev(1, "A", "INSERT", "S.LOG", after={"MSG": "m", "N": 1}), ev(2, "A", "COMMIT")])
    w = Worker(get_engine(), get_settings(), "edge-worker")
    drain(w)
    assert SnapshotService(get_engine(), w.pipeline(cid)).start(["S.KEYED"]) == ["S.KEYED"]
    drain(w)                                                              # step -> SNAPSHOT_TABLE issued
    cmds = client.get(f"/agent/v1/commands?agent_id={a.agent_id}&wait=0", headers=a.h).json()
    snap = next(c for c in cmds if c["name"] == "SNAPSHOT_TABLE")
    assert set(snap["args"]) <= {"table", "key_from", "key_to", "chunk_rows", "snapshot_id"}   # C agent contract
    a.send([ev(3, "B", "UPDATE", "S.KEYED", before={"ID": 1}, after={"ID": 1, "NAME": "cdc"}), ev(4, "B", "COMMIT")])
    drain(w)
    assert landing("SELECT to_regclass('edgesnap.keyed') IS NULL") == [(True,)]          # held while snapshotting
    for n, (rows, last) in enumerate([([{"ID": 1, "NAME": "snap"}], False), ([{"ID": 2, "NAME": "two"}], True)]):
        body = b"".join(json.dumps(r).encode() + b"\n" for r in rows)
        hdr = SnapshotChunkHeader(snapshot_id=uuid.UUID(snap["args"]["snapshot_id"]), table=TableRef(schema="S",
                                  name="KEYED"), chunk_no=n, rows=len(rows), last=last, codec="zlib")
        r = client.post("/agent/v1/snapshot", headers={**a.h, "X-ALELT-Snapshot": hdr.model_dump_json(by_alias=True),
                                                        "X-ALELT-Agent-Id": a.agent_id}, content=compress(body, "zlib"))
        assert r.status_code == 200
    drain(w)
    assert landing("SELECT id, name FROM edgesnap.keyed ORDER BY id") == [(1, "cdc"), (2, "two")]   # catch-up
    h = holds.rows(get_engine(), cid, "snapshot")[0]
    assert h["state"] == "CATCHING_UP" and not h["held"] and h["body"]["catchup_done"]
    # X-123: the mandatory post-snapshot reconciliation ran; nobody answered VERIFY_TABLE within the bounded wait
    # (ALELT_VERIFY_EDGE_TIMEOUT_S) -> command EXPIRED, reason on the hold, table stays CATCHING_UP, retry queued.
    from sqlalchemy import select as _select

    from alelt_hub.db import agent_command
    from alelt_hub.edge_verify import request_post_snapshot_verify

    from .test_edge_verify import AgentLoop

    assert "waiting for the agent" in str(h["body"].get("verify"))
    with get_engine().connect() as c:
        st = {r[0] for r in c.execute(_select(agent_command.c.status).where(
            agent_command.c.agent_id == a.agent_id, agent_command.c.name == "VERIFY_TABLE"))}
    assert st == {"EXPIRED"}
    # the agent answers (source = what the destination should hold) -> MATCH -> IN_SYNC
    truth = {"S.KEYED": [{"ID": 1, "NAME": "cdc", "AMT": None, "SECRET": None},
                         {"ID": 2, "NAME": "two", "AMT": None, "SECRET": None}]}
    with AgentLoop(client, a, truth):
        request_post_snapshot_verify(get_engine(), cid, "S.KEYED")
        drain(w)
    assert holds.rows(get_engine(), cid, "snapshot")[0]["state"] == "IN_SYNC"


# ------------------------------------------------------------------ wizard progress uses the stored secret
def test_progress_uses_stored_secret(client: Any, remote: str, monkeypatch: pytest.MonkeyPatch) -> None:
    from alelt_hub.db import get_engine, pipeline_definition

    monkeypatch.setenv("FAKE_PROGRESS_PW", fakes.WORLDS[remote].password)
    with get_engine().begin() as c:
        c.execute(pipeline_definition.insert().values(
            connection_id=remote, origin="wizard", environment="TEST", source_engine="fakeremote",
            destination_engine="postgres", source={"engine": "fakeremote", "settings": {"world": remote},
                                                   "secret_refs": {"password": "env:FAKE_PROGRESS_PW"}},
            destination={}, tables_doc={"connection_id": remote}, status="SHADOW", created_by="t"))
    r = client.get(f"/api/v1/wizard/pipelines/{remote}/progress", headers=role("Viewer"))
    assert r.status_code == 200, r.text
    pre = next(s for s in r.json()["steps"] if s["id"] == "prerequisites")
    assert [c["status"] for c in pre["checks"]] == ["PASS"] and "s3cret" not in r.text


# ------------------------------------------------------------------ destination selection (one code path)
class _HubDest:
    seen: dict[str, Any] = {}

    @classmethod
    def from_hub(cls, url: str, schema: str, output_profile: str | None, compare: Any) -> Any:
        cls.seen = {"url": url, "schema": schema}
        return cls()


def test_destination_selection() -> None:
    from alelt_hub.pipeline import destination_class, make_destination
    from alelt_hub.settings import Settings
    from alelt_hub.testing import FakeSource
    from alelt_loader import load_policy

    pg = destination_class("postgres")
    assert pg is destination_class("alelt_dst_postgres.connector:PostgresDestination")
    try:
        import alelt_dst_sql  # noqa: F401

        assert destination_class("sql").__name__ == "SqlDestination"
    except ImportError:
        pass
    pol = load_policy("x", doc={"connection_id": "x", "landing_schema": "L"})
    s = Settings(destination="alelt_hub.tests.test_integration_pass:_HubDest", dest_url="u://default")
    make_destination(s, pol, FakeSource())
    assert _HubDest.seen == {"url": "u://default", "schema": "L"}
    make_destination(Settings(destination="postgres", dest_url="u://x"), pol, FakeSource(),
                     {"engine": "alelt_hub.tests.test_integration_pass:_HubDest", "settings": {"url": "u://pipe"}})
    assert _HubDest.seen["url"] == "u://pipe"                        # per-pipeline destination wins


# ------------------------------------------------------------------ HC-15: remote verify with a consistency point
def test_remote_verify_consistency_point_end_to_end(client: Any, remote: str) -> None:
    """Source read ahead of the destination -> MISMATCH reported as NOT settled (retried, never recorded as a real
    mismatch); once the change is applied the same verify is a settled MATCH."""
    need_landing()
    from alelt_hub import ops
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.snapshots import SnapshotService
    from alelt_hub.worker import Worker

    cid, w_ = remote, fakes.WORLDS[remote]
    for i in range(1, 4):
        w_.txn([("INSERT", fakes.item(i, f"n{i}"))])
    w = Worker(get_engine(), get_settings(), "hc15-worker")
    assert SnapshotService(get_engine(), w.pipeline(cid)).start() == ["S.ITEMS"]
    w_.txn([("INSERT", fakes.item(4, "four"))])                    # a captured change after the snapshot start
    assert _runner(cid).run(idle_exit_s=0.3) == "idle"
    drain(w)
    p = w.pipeline(cid)
    res = ops.verify_table(get_engine(), p, "S.ITEMS")
    assert res.outcome == "MATCH" and res.summary["settled"] is True and res.summary["position_before"]
    w_.txn([("INSERT", fakes.item(5, "late"))])                    # committed at the source, not captured yet
    res = ops.verify_table(get_engine(), p, "S.ITEMS")
    assert res.outcome == "MISMATCH" and res.summary["settled"] is False
    assert _runner(cid).run(idle_exit_s=0.3) == "idle"
    drain(w)
    res = ops.verify_table(get_engine(), p, "S.ITEMS")
    assert res.outcome == "MATCH" and res.summary["settled"] is True
