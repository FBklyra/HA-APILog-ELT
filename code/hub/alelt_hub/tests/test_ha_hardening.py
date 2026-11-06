"""Hub HA hardening (code review HA-01/02/04/05/10/12/14/19/20/21/22/26): operator release is never revived by a renew,
the released holder yields to the standby, epochs stay monotonic across lease-clear, stale holders are fenced on
/catalog and /snapshot, non-holder heartbeats show STANDBY, snapshot target = lease holder, the lease clock is the
database clock on PostgreSQL (replica skew), the topology write freeze, offline agents and unfinished work at drain,
a crash in the middle of a switch step, and the secret allow-list of the generated configuration."""
from __future__ import annotations

import json
import threading
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import insert, select, update

from alelt_common.util import compress, utcnow
from alelt_hub.testing import FakeAgent, ev, fake_catalog

from .conftest import TOKEN
from .test_topology import AGENT, CONN, FakeAgentLoop, _audits, _commands, _seed, _state, hub


def _pair(client: Any, tag: str) -> tuple[FakeAgent, FakeAgent, str]:
    conn = f"hh-{tag}-{uuid.uuid4().hex[:6]}"
    a = FakeAgent(client, TOKEN, agent_id=f"{conn}-a", connection_id=conn)
    b = FakeAgent(client, TOKEN, agent_id=f"{conn}-b", connection_id=conn)
    a.register()
    b.register()
    return a, b, conn


def lease(client: Any, ag: FakeAgent, op: str, **kw: Any) -> Any:
    return client.post(f"/agent/v1/lease/{op}", headers=ag.h,
                       json={"agent_id": ag.agent_id, "connection_id": ag.connection_id, **kw})


def _cleanup(conn: str) -> None:
    from sqlalchemy import delete

    from alelt_hub.db import batch, get_engine, job

    with get_engine().begin() as c:
        ids = [str(r[0]) for r in c.execute(select(batch.c.id).where(batch.c.connection_id == conn))]
        if ids:
            c.execute(delete(job).where(job.c.dedup_key.in_([f"decode:{i}" for i in ids])))
            c.execute(delete(batch).where(batch.c.connection_id == conn))


# ------------------------------------------------------------------ HA-01 / HA-13 / HA-14
def test_operator_release_is_not_revived_and_standby_wins(client: Any) -> None:
    from alelt_hub import ha
    from alelt_hub.db import get_engine

    a, b, conn = _pair(client, "rel")
    g = lease(client, a, "acquire", ttl_s=30).json()
    assert g["granted"] and g["epoch"] == 1
    assert lease(client, b, "acquire", ttl_s=30).json()["granted"] is False        # b polls as a candidate
    with get_engine().begin() as c:                                                 # console "Release lease"
        assert ha.release(c, conn, None, "admin", "forced failover")["released"]
    r = lease(client, a, "renew", epoch=g["epoch"], lease_id=g["lease_id"])          # holder renewing meanwhile
    assert r.status_code == 409 and r.json()["detail"]["error"] == "lease_lost"
    assert lease(client, a, "acquire", ttl_s=30).json()["granted"] is False         # released holder yields
    t = lease(client, b, "acquire", ttl_s=30).json()
    assert t["granted"] and t["takeover"] and t["holder"] == b.agent_id and t["epoch"] == 2
    assert lease(client, a, "renew", epoch=g["epoch"], lease_id=g["lease_id"]).status_code == 409

    # agent-initiated release, nobody else polling: the same agent may come back, with a NEW epoch
    assert lease(client, b, "release").json()["released"]
    r = lease(client, b, "renew", epoch=2, lease_id=t["lease_id"])
    assert r.status_code == 409
    # HA-14: lease-clear keeps the row, the epoch never restarts at 1
    with get_engine().begin() as c:
        assert ha.clear_lease(c, conn, "test") is True
        assert conn not in ha.lease_rows(c)
        ha.check_fence(c, conn, a.agent_id, None)                                   # standalone: no fence
    st = client.get("/agent/v1/lease", params={"connection_id": conn}, headers=a.h).json()
    assert st["granted"] is False and st["holder"] is None
    g3 = lease(client, a, "acquire", ttl_s=30).json()
    assert g3["granted"] and g3["epoch"] == 3                                        # monotonic


def test_renew_after_expiry_is_refused(client: Any) -> None:
    from alelt_hub import ha

    a, _, _ = _pair(client, "exp")
    g = lease(client, a, "acquire", ttl_s=5).json()
    mp = pytest.MonkeyPatch()
    real_db, real = ha.db_now, ha.utcnow
    mp.setattr(ha, "db_now", lambda c: real_db(c) + timedelta(seconds=6))
    mp.setattr(ha, "utcnow", lambda: real() + timedelta(seconds=6))
    try:
        r = lease(client, a, "renew", epoch=g["epoch"], lease_id=g["lease_id"])
        assert r.status_code == 409                                                 # expired: no silent revival
        again = lease(client, a, "acquire", ttl_s=5).json()                          # HA-13: nobody else held it
        assert again["granted"] and again["epoch"] == g["epoch"]
    finally:
        mp.undo()


# ------------------------------------------------------------------ HA-04 / HA-05
def test_stale_holder_fenced_on_catalog_snapshot_and_heartbeat(client: Any) -> None:
    from alelt_hub import ha
    from alelt_hub.db import agent, get_engine
    from alelt_hub.snapshots import SnapshotService

    a, b, conn = _pair(client, "fence")
    g = lease(client, a, "acquire", ttl_s=30).json()
    assert g["granted"]
    snap = fake_catalog(conn).model_dump_json(by_alias=True)
    hdr = {**b.h, "Content-Type": "application/json"}
    r = client.post("/agent/v1/catalog", headers={**hdr, "X-ALELT-Agent-Id": b.agent_id}, content=snap)
    assert r.status_code == 423 and r.json()["detail"]["what"] == "catalog"
    r = client.post("/agent/v1/catalog", headers={**a.h, "Content-Type": "application/json",
                                                  "X-ALELT-Agent-Id": a.agent_id, "X-ALELT-Lease-Epoch": "99"}, content=snap)
    assert r.status_code == 423                                                     # holder, wrong epoch
    r = client.post("/agent/v1/catalog", headers={**a.h, "Content-Type": "application/json",
                                                  "X-ALELT-Agent-Id": a.agent_id, "X-ALELT-Lease-Epoch": str(g["epoch"])},
                    content=snap)
    assert r.status_code == 200
    sh = {"snapshot_id": str(uuid.uuid4()), "table": {"schema": "S", "name": "KEYED"}, "chunk_no": 0, "rows": 1,
          "last": True, "codec": "zlib"}
    r = client.post("/agent/v1/snapshot", headers={**b.h, "X-ALELT-Snapshot": json.dumps(sh), "X-ALELT-Agent-Id": b.agent_id},
                    content=compress(b'{"ID": 1}\n', "zlib"))
    assert r.status_code == 423 and r.json()["detail"]["what"] == "snapshot"
    # a standby that claims CAPTURING is shown as STANDBY; the holder keeps its state
    for ag in (a, b):
        assert client.post("/agent/v1/heartbeat", headers=ag.h,
                           json={"agent_id": ag.agent_id, "state": "CAPTURING",
                                 "at": utcnow().isoformat()}).status_code == 200
    with get_engine().connect() as c:
        st = dict(c.execute(select(agent.c.id, agent.c.status).where(agent.c.connection_id == conn)).all())
        assert st == {a.agent_id: "CAPTURING", b.agent_id: "STANDBY"}
        assert ha.holder(c, conn) == a.agent_id
    # HA-05: SNAPSHOT_TABLE goes to the holder even when the standby heartbeats last
    svc = SnapshotService.__new__(SnapshotService)
    svc.engine, svc.cid = get_engine(), conn
    with get_engine().begin() as c:
        c.execute(update(agent).where(agent.c.id == b.agent_id).values(last_heartbeat=utcnow() + timedelta(seconds=60)))
    assert svc._edge_agent() == a.agent_id


# ------------------------------------------------------------------ HA-12
def test_replica_clock_skew_cannot_take_over(client: Any, hub_env: dict[str, str]) -> None:
    from alelt_hub import ha

    if hub_env["backend"] != "postgres":
        pytest.skip("SQLite metadata is single-host (one clock); the DB clock applies to PostgreSQL")
    a, b, _ = _pair(client, "skew")
    assert lease(client, a, "acquire", ttl_s=30).json()["granted"]
    mp = pytest.MonkeyPatch()
    real = ha.utcnow
    mp.setattr(ha, "utcnow", lambda: real() + timedelta(seconds=45))               # this replica runs 45 s ahead
    try:
        g = lease(client, b, "acquire", ttl_s=30).json()
        assert g["granted"] is False and g["holder"] == a.agent_id                  # PG now() decides, not the replica
    finally:
        mp.undo()


# ------------------------------------------------------------------ HA-02 freeze (gateway)
def test_write_freeze_refuses_agent_writes(client: Any) -> None:
    from alelt_hub import ha
    from alelt_hub.db import get_engine

    a, _, conn = _pair(client, "frz")
    with get_engine().begin() as c:
        ha.set_freeze(c, "topo-test", "test")
    try:
        r = a.send([ev(1, "T1", "INSERT", "S.KEYED", after={"ID": 1}), ev(2, "T1", "COMMIT")])
        assert r.status_code == 503 and r.headers.get("retry-after") == "30"
        assert r.json()["detail"]["error"] == "topology_frozen"
        r = client.post("/agent/v1/catalog", headers={**a.h, "Content-Type": "application/json"},
                        content=fake_catalog(conn).model_dump_json(by_alias=True))
        assert r.status_code == 503
    finally:
        with get_engine().begin() as c:
            assert ha.lift_freeze(c)
    assert a.send([ev(1, "T1", "INSERT", "S.KEYED", after={"ID": 1}), ev(2, "T1", "COMMIT")]).status_code == 200
    _cleanup(conn)


# ------------------------------------------------------------------ topology: offline agents, drain completeness
def _sqlite(tmp: Path, name: str) -> str:
    return f"sqlite:///{(tmp / name).as_posix()}"


def test_offline_agent_blocks_unless_allowed(tmp_path: Path) -> None:
    from alelt_hub import topology
    from alelt_hub.db import agent

    spool = "file://" + (tmp_path / "spool").as_posix()
    with hub(_sqlite(tmp_path, "src.db"), spool, tmp_path / "scratch") as src:
        _seed(src, 2)
        with src.begin() as c:
            c.execute(insert(agent).values(id="gone", connection_id=CONN, version="t", hostname="h2", status="IDLE",
                                           last_heartbeat=utcnow() - timedelta(hours=2)))
            c.execute(insert(agent).values(id="capture:hub", connection_id=CONN, version="t", hostname="h3",
                                           status="IDLE"))              # hub-run capture: not an offline edge
        dst = _sqlite(tmp_path, "dst.db")
        p = topology.plan("standalone", db_url=dst, spool_url=spool)
        assert any("gone" in b and "--allow-offline" in b for b in p["blockers"])
        assert not any("capture:hub" in b for b in p["blockers"])
        p = topology.plan("standalone", db_url=dst, spool_url=spool, allow_offline=True)
        assert not p["blockers"] and any("gone" in w for w in p["warnings"])
        assert topology.apply("standalone", db_url=dst, spool_url=spool, out=str(tmp_path / "o"), drain_timeout_s=2,
                              log=lambda s: None) == 1                  # preflight refuses


def _op(**kw: Any) -> dict[str, Any]:
    return {"id": f"topo-t-{uuid.uuid4().hex[:4]}", "to": "standalone", "paused_agents": {}, "steps": {}, **kw}


def test_drain_waits_for_every_kind_of_unfinished_work(tmp_path: Path) -> None:
    from alelt_hub import ha, topology
    from alelt_hub.db import agent, agent_command, batch, job

    with hub(_sqlite(tmp_path, "src.db"), "file://" + (tmp_path / "spool").as_posix(), tmp_path / "s") as src:
        _seed(src, 3)
        logs: list[str] = []
        with src.begin() as c:                                               # a running verify job (HA-10)
            c.execute(insert(job).values(kind="verify", payload={}, status="RUNNING", dedup_key="t-run"))
        with FakeAgentLoop(src), pytest.raises(TimeoutError, match="running or due jobs: 1"):
            topology._drain(src, _op(), "t", 2, logs.append)
        with src.begin() as c:
            c.execute(update(job).where(job.c.dedup_key == "t-run").values(status="DONE"))
            bid = c.execute(select(batch.c.id).limit(1)).scalar()
            c.execute(update(batch).where(batch.c.id == bid).values(status="DECODED"))
            ha.lift_freeze(c)
        with FakeAgentLoop(src), pytest.raises(TimeoutError, match="batches not loaded: 1"):
            topology._drain(src, _op(), "t", 2, logs.append)
        with src.begin() as c:
            c.execute(update(batch).where(batch.c.id == bid).values(status="FAILED"))
        with FakeAgentLoop(src), pytest.raises(RuntimeError, match="FAILED"):
            topology._drain(src, _op(), "t", 2, logs.append)
        with src.begin() as c:
            c.execute(update(batch).where(batch.c.id == bid).values(status="LOADED"))
        # HA-26: an agent that refused PAUSE (command FAILED) is not drained
        op = _op()
        with src.begin() as c:
            c.execute(update(agent).where(agent.c.id == AGENT).values(last_heartbeat=utcnow(), status="IDLE"))
        stop_ack = FakeAgentLoop(src)

        def refuse() -> None:                                                # every command answered FAILED
            while not stop_ack.stop.is_set():
                with src.begin() as c:
                    c.execute(update(agent_command).where(agent_command.c.status.in_(["PENDING", "SENT"]))
                              .values(status="FAILED"))
                    c.execute(update(agent).where(agent.c.id == AGENT).values(last_heartbeat=utcnow()))
                stop_ack.stop.wait(0.2)

        stop_ack.t = threading.Thread(target=refuse, daemon=True)
        with stop_ack, pytest.raises(TimeoutError, match="cmd=FAILED"):
            topology._drain(src, op, "t", 2, logs.append)
        # all clear: drain freezes writes, then verifies again
        with src.begin() as c:
            ha.lift_freeze(c)
        with FakeAgentLoop(src):
            out = topology._drain(src, _op(), "t", 10, logs.append)
        assert "writes frozen" in out
        with src.connect() as c:
            assert ha.frozen(c) is not None
        assert "topology.freeze" in _audits(src, "topology.")
        with src.begin() as c:
            ha.lift_freeze(c)


# ------------------------------------------------------------------ topology crash mid-step + HA-21 / HA-22 / HA-19
def test_crash_mid_step_resumes_and_abort_reapply(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from alelt_hub import ha, metamigrate, topology

    spool = "file://" + (tmp_path / "spool").as_posix()
    dst = _sqlite(tmp_path, "dst.db")
    logs: list[str] = []
    monkeypatch.setenv("ALELT_LOG_LEVEL", "INFO")
    monkeypatch.setenv("ALELT_CUSTOM_DEST_PASSWORD_HINT", "x")                  # never carried (secret-ish)
    monkeypatch.setenv("ALELT_SNOWFLAKE_PRIVATE_KEY_PATH", "/k.p8")             # unknown key: not in the allow-list
    with hub(_sqlite(tmp_path, "src.db"), spool, tmp_path / "scratch") as src:
        _seed(src, 4)
        real = metamigrate.migrate
        calls = {"n": 0}

        def crash_once(*a: Any, **kw: Any) -> Any:
            calls["n"] += 1
            rep = real(*a, **kw)                      # target half-written ...
            if calls["n"] == 1:
                raise RuntimeError("simulated crash after the copy")   # ... then the process dies
            return rep

        monkeypatch.setattr(metamigrate, "migrate", crash_once)
        with FakeAgentLoop(src):
            rc = topology.apply("standalone", db_url=dst, spool_url=spool, out=str(tmp_path / "o"),
                                drain_timeout_s=30, log=logs.append)
        assert rc == 1
        op = _state(src)["operation"]
        assert op["steps"]["migrate_metadata"]["status"] == "failed" and op["metadata_started"]
        with src.connect() as c:
            assert ha.frozen(c) is not None                                  # still frozen while unfinished
        with FakeAgentLoop(src):                                             # re-run: target reset, continues
            rc = topology.apply("standalone", db_url=dst, spool_url=spool, out=str(tmp_path / "o"),
                                drain_timeout_s=30, log=logs.append)
        assert rc == 3, logs                                                 # waits for the restart
        env_txt = (tmp_path / "o" / "alelt.standalone.env").read_text()
        assert "ALELT_LOG_LEVEL=INFO" in env_txt
        assert "PASSWORD_HINT" not in env_txt and "SNOWFLAKE_PRIVATE_KEY_PATH" not in env_txt
        # abort, then apply again: the populated target is reset, not refused (HA-22)
        assert topology.abort(log=logs.append) == 0
        with src.connect() as c:
            assert ha.frozen(c) is None                                      # abort lifts the freeze
        with FakeAgentLoop(src):
            rc = topology.apply("standalone", db_url=dst, spool_url=spool, out=str(tmp_path / "o"),
                                drain_timeout_s=30, log=logs.append)
        assert rc == 3, logs
        # HA-21: RESUME issued, crash before the state was saved -> the re-run does not issue a second one
        from alelt_hub.dialect import make_engine

        de = make_engine(dst)
        op = _state(src)["operation"]
        try:
            before = _commands(de, "RESUME")
            with de.begin() as c:
                from alelt_hub import store
                from alelt_common.models import CommandName

                store.issue_command(c, AGENT, CommandName.RESUME, {}, "cli")
            topology._resume(de, de, op, "standalone", "cli", False, 0, logs.append)
            topology._resume(de, de, {**op, "paused_agents": {k: {**v, "resumed": False}
                                                              for k, v in op["paused_agents"].items()}},
                             "standalone", "cli", False, 0, logs.append)
            assert _commands(de, "RESUME") == before + 1
            with de.connect() as c:
                assert ha.frozen(c) is None                                  # target accepts writes after resume
        finally:
            de.dispose()


# ------------------------------------------------------------------ HA-20 hub_node GC
def test_hub_node_gc_and_restart(client: Any) -> None:
    from alelt_hub import ha
    from alelt_hub.db import get_engine
    from alelt_hub.ha_models import hub_node

    eng = get_engine()
    with eng.begin() as c:
        c.execute(insert(hub_node).values(node_id="old:1:api", role="api", last_seen=utcnow() - timedelta(days=2),
                                          started_at=utcnow() - timedelta(days=3)))
    ha.node_beat(eng, "api", "fixed-node", first=True)
    first = None
    with eng.connect() as c:
        ids = {r[0]: r[1] for r in c.execute(select(hub_node.c.node_id, hub_node.c.started_at))}
        first = ids["fixed-node"]
    assert "old:1:api" not in ids
    ha.node_beat(eng, "api", "fixed-node")
    ha.node_beat(eng, "api", "fixed-node", first=True)                       # process restart, same ALELT_NODE_ID
    with eng.connect() as c:
        again = c.execute(select(hub_node.c.started_at).where(hub_node.c.node_id == "fixed-node")).scalar()
    assert again >= first


# ------------------------------------------------------------------ HA-08 / HA-11 metadata locks and durability
def test_advisory_lock_loss_detected_and_sqlite_full_sync(client: Any, hub_env: dict[str, str]) -> None:
    from sqlalchemy import text

    from alelt_hub.db import get_engine
    from alelt_hub.dialect import LockLost, lock_alive, require_lock, try_lock, unlock

    eng = get_engine()
    if hub_env["backend"] == "sqlite":
        with eng.connect() as c:
            assert c.exec_driver_sql("PRAGMA synchronous").scalar() == 2          # FULL
        h = try_lock(eng, "hh:lock")
        assert lock_alive(h)
        unlock(h, "hh:lock")
        return
    import time

    key = f"hh:lock:{uuid.uuid4().hex}"           # unique: suites running in parallel on the same server never collide
    h = try_lock(eng, key)
    assert h is not None and lock_alive(h)
    assert try_lock(eng, key) is None                                            # second worker refused
    pid = h.info["alelt_lock"][1]
    with eng.connect() as c:                                                     # failover / killed session
        assert c.execute(text("SELECT pg_terminate_backend(:p)"), {"p": pid}).scalar()
        c.commit()
        # pg_terminate_backend only signals: wait (bounded) until the backend is really gone, so the checks below
        # never race the signal (the cause of the old flake)
        deadline = time.monotonic() + 15
        while c.execute(text("SELECT count(*) FROM pg_stat_activity WHERE pid = :p"), {"p": pid}).scalar():
            c.commit()
            assert time.monotonic() < deadline, "terminated backend still alive after 15 s"
            time.sleep(0.05)
        c.commit()
    assert not lock_alive(h)
    with pytest.raises(LockLost):
        require_lock(h, key)
    try:
        h.close()
    except Exception:  # noqa: BLE001
        pass
    h2 = try_lock(eng, key)                                                      # lock really released
    assert h2 is not None
    unlock(h2, key)


def test_transaction_pooler_is_detected_and_refused() -> None:
    """HA-08: behind transaction pooling the backend changes between transactions: the lock is refused (and released
    on the backend that took it), never silently trusted."""
    from alelt_hub.dialect import PoolerError, try_lock

    class Res:
        def __init__(self, v: Any) -> None:
            self.v = v

        def scalar(self) -> Any:
            return self.v

    class Conn:
        def __init__(self) -> None:
            self.pids = iter([101, 202])          # pooler hands the next transaction to another backend
            self.sql: list[str] = []
            self.closed = False
            self.info: dict[str, Any] = {}

        def execute(self, stmt: Any, binds: Any = None) -> Res:
            q = str(stmt)
            self.sql.append(q)
            if "pg_backend_pid" in q:
                return Res(next(self.pids))
            return Res(True)

        def commit(self) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    class Eng:
        class dialect:  # noqa: N801
            name = "postgresql"

        def __init__(self) -> None:
            self.conn = Conn()

        def connect(self) -> Conn:
            return self.conn

    e = Eng()
    with pytest.raises(PoolerError):
        try_lock(e, "hh:pool")  # type: ignore[arg-type]
    assert any("pg_advisory_unlock" in q for q in e.conn.sql) and e.conn.closed


# ------------------------------------------------------------------ AHS-24 capture state in the grant
def test_grant_carries_operator_pause_to_the_new_holder(client: Any) -> None:
    """A PAUSE sent to the old holder never reaches the standby: the takeover grant says capturing=false, so the
    new holder starts paused; RESUME / START_CAPTURE flip it back; no command yet = null (agent default)."""
    from alelt_common.models import CommandName
    from alelt_hub import ha, store
    from alelt_hub.db import get_engine

    a, b, conn = _pair(client, "cap")
    g = lease(client, a, "acquire", ttl_s=30).json()
    assert g["granted"] and g["capturing"] is None
    with get_engine().begin() as c:
        store.issue_command(c, a.agent_id, CommandName.START_CAPTURE, {"resume": True}, "admin")
    with get_engine().begin() as c:
        assert ha.capture_running(c, conn) is True
        store.issue_command(c, a.agent_id, CommandName.PAUSE, {}, "admin")
    with get_engine().begin() as c:
        assert ha.release(c, conn, None, "admin", "failover")["released"]
    t = lease(client, b, "acquire", ttl_s=30).json()
    assert t["granted"] and t["takeover"] and t["capturing"] is False
    with get_engine().begin() as c:
        store.issue_command(c, b.agent_id, CommandName.RESUME, {}, "admin")
    assert lease(client, b, "acquire", ttl_s=30).json()["capturing"] is True


def test_standby_poll_does_not_rewrite_candidates_every_time(client: Any) -> None:
    """HA-15: a standby polling acquire records itself at most once per TTL/3 (no row write per poll)."""
    from alelt_hub.db import get_engine
    from alelt_hub.ha_models import capture_lease

    a, b, conn = _pair(client, "cand")
    assert lease(client, a, "acquire", ttl_s=30).json()["granted"]

    def cand() -> Any:
        with get_engine().connect() as c:
            return c.execute(select(capture_lease.c.candidates).where(capture_lease.c.connection_id == conn)).scalar()

    assert lease(client, b, "acquire", ttl_s=30).json()["granted"] is False
    first = cand()[b.agent_id]
    for _ in range(5):
        assert lease(client, b, "acquire", ttl_s=30).json()["granted"] is False
    assert cand()[b.agent_id] == first
    with get_engine().begin() as c:                   # older than TTL/3: written again
        c.execute(update(capture_lease).where(capture_lease.c.connection_id == conn).values(
            candidates={b.agent_id: (utcnow() - timedelta(seconds=11)).isoformat()}))
    lease(client, b, "acquire", ttl_s=30)
    assert cand()[b.agent_id] != first


def test_busy_decode_requeue_is_deduplicated(client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """HA-25: a worker that finds the stream lock busy re-enqueues ONE decode job per 2 s bucket, not one per try."""
    from sqlalchemy import func

    from alelt_hub import worker as W
    from alelt_hub.db import get_engine, job

    conn = f"busy-{uuid.uuid4().hex[:6]}"
    monkeypatch.setattr(W, "try_lock", lambda engine, key: None)          # someone else holds pipeline:<cid>
    monkeypatch.setattr(W.time, "time", lambda: 1_000_000.5)
    wk = W.Worker.__new__(W.Worker)
    wk.engine = get_engine()
    for _ in range(10):
        wk._decode({"connection_id": conn})
    with get_engine().connect() as c:
        n = c.execute(select(func.count()).select_from(job).where(job.c.dedup_key.like(f"busy:{conn}:%"))).scalar()
    assert n == 1
