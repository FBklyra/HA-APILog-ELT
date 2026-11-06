"""Topology switch (X-50): standalone (SQLite + local spool) -> HA (PostgreSQL + S3) -> standalone, with a fake agent
that obeys PAUSE / RESUME. Plan / apply / abort: idempotent, resumable after a failure, every step audited, no batch
lost (every stored ref readable on the target with the original sha256)."""
from __future__ import annotations

import os
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, func, insert, select, update

from alelt_common.util import decompress, sha256_hex, utcnow
from alelt_hub.testing import FakeAgent, ev

from .conftest import ADMIN, TOKEN, _admin, _reset_caches
from .s3fake import bucket, s3_endpoint

ENV_KEYS = ("ALELT_DB_URL", "ALELT_SPOOL_URL", "ALELT_SPOOL_DIR", "ALELT_AGENT_TOKENS", "ALELT_DEV_AUTH", "ALELT_ENV", "ALELT_SOURCES",
            "ALELT_DEST_URL", "ALELT_TOPOLOGY_OUT")
AGENT, CONN = "topo-agent", "topoconn"


@contextmanager
def hub(db_url: str, spool_url: str, scratch: Path) -> Iterator[Engine]:
    """Point the hub modules (settings, engine, blob store) at one metadata DB + spool, restore afterwards."""
    from alelt_hub import blobstore
    from alelt_hub.db import get_engine, init_db
    from alelt_hub.metamigrate import hub_tables

    old = {k: os.environ.get(k) for k in ENV_KEYS}
    os.environ.update({"ALELT_DB_URL": db_url, "ALELT_SPOOL_URL": spool_url, "ALELT_SPOOL_DIR": str(scratch),
                       "ALELT_AGENT_TOKENS": TOKEN, "ALELT_ENV": "test", "ALELT_DEST_URL": "",
                       "ALELT_SOURCES": '{"fake": "alelt_hub.testing:FakeSource"}'})
    _reset_caches()
    blobstore.reset_cache()
    hub_tables()
    init_db()
    try:
        yield get_engine()
    finally:
        _reset_caches()
        blobstore.reset_cache()
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class FakeAgentLoop:
    """Acks every command of the agent and heartbeats like a paused / capturing agent with an empty spool."""

    def __init__(self, eng: Engine) -> None:
        self.eng, self.stop = eng, threading.Event()
        self.t = threading.Thread(target=self.run, daemon=True)

    def run(self) -> None:
        from alelt_hub.db import agent, agent_command

        while not self.stop.is_set():
            try:
                with self.eng.begin() as c:
                    cmds = c.execute(select(agent_command.c.id, agent_command.c.name)
                                     .where(agent_command.c.agent_id == AGENT,
                                            agent_command.c.status.in_(["PENDING", "SENT"]))
                                     .order_by(agent_command.c.issued_at)).all()
                    state = None
                    for cid, name in cmds:
                        c.execute(update(agent_command).where(agent_command.c.id == cid)
                                  .values(status="ACKED", acked_at=utcnow(), result={"ok": True}))
                        state = "PAUSED" if name == "PAUSE" else "CAPTURING" if name == "RESUME" else state
                    vals: dict[str, Any] = {"last_heartbeat": utcnow(), "spool_bytes": 0}
                    if state:
                        vals["status"] = state
                    c.execute(update(agent).where(agent.c.id == AGENT).values(**vals))
            except Exception:  # noqa: BLE001 - target DB may be busy / being created
                pass
            self.stop.wait(0.3)

    def __enter__(self) -> "FakeAgentLoop":
        self.t.start()
        return self

    def __exit__(self, *a: Any) -> None:
        self.stop.set()
        self.t.join(5)


def _seed(eng: Engine, n: int = 12) -> None:
    """Agent + n batches received through the real gateway path, then marked loaded (as after a worker run)."""
    from alelt_hub.db import agent, batch, connection, job
    from alelt_hub.gateway import _receive_batch

    with eng.begin() as c:
        c.execute(insert(connection).values(id=CONN, engine="fake", db_database="FAKE"))
        c.execute(insert(agent).values(id=AGENT, connection_id=CONN, version="t", hostname="h", status="CAPTURING",
                                       last_heartbeat=utcnow(), spool_bytes=0))
    fa = FakeAgent(None, TOKEN, agent_id=AGENT, connection_id=CONN)
    for i in range(n):
        hdr, body = fa.make_batch([ev(2 * i + 1, f"T{i}", "INSERT", "S.KEYED", after={"ID": i, "PAD": "x" * i}),
                                   ev(2 * i + 2, f"T{i}", "COMMIT")])
        _receive_batch(hdr, body, None, None)
        fa.next_from = hdr.next_pos.text  # type: ignore[union-attr]
    with eng.begin() as c:
        c.execute(update(batch).values(status="LOADED"))
        c.execute(update(job).values(status="DONE", finished_at=utcnow()))


def _verify_batches(eng: Engine, expect_prefix: str, n: int, local: bool = False) -> None:
    from alelt_hub import blobstore
    from alelt_hub.db import batch

    with eng.connect() as c:
        rows = c.execute(select(batch.c.spool_path, batch.c.sha256, batch.c.codec, batch.c.raw_bytes)).all()
    assert len(rows) == n
    for ref, sha, codec, size in rows:
        assert ref.startswith(expect_prefix) and not (local and ref.startswith("s3://")), ref
        raw = decompress(blobstore.read_ref(ref), codec)
        assert len(raw) == size and sha256_hex(raw) == sha


def _restart_on(eng: Engine, spool_url: str) -> None:
    """What the operator's restart does for `resume`: an api node of the new stack heartbeats on the target spool."""
    from alelt_hub.ha_models import hub_node

    with eng.begin() as c:
        c.execute(update(hub_node).values(last_seen=utcnow().replace(year=2000)))    # old stack's copied rows: stale
        c.execute(insert(hub_node).values(node_id=f"new-{uuid.uuid4().hex[:6]}:api", role="api", host="new",
                                          spool_url=spool_url, db_backend=eng.dialect.name, started_at=utcnow(),
                                          last_seen=utcnow()))


def _state(eng: Engine) -> dict[str, Any]:
    from alelt_hub.db import hub_setting

    with eng.connect() as c:
        return dict(c.execute(select(hub_setting.c.value).where(hub_setting.c.key == "topology")).scalar() or {})


def _audits(eng: Engine, prefix: str) -> list[str]:
    from alelt_hub.db import audit_event

    with eng.connect() as c:
        return [r[0] for r in c.execute(select(audit_event.c.action).where(audit_event.c.action.like(prefix + "%"))
                                        .order_by(audit_event.c.at))]


def _commands(eng: Engine, name: str) -> int:
    from alelt_hub.db import agent_command

    with eng.connect() as c:
        return int(c.execute(select(func.count()).select_from(agent_command).where(
            agent_command.c.agent_id == AGENT, agent_command.c.name == name)).scalar_one())


@pytest.fixture()
def s3() -> Iterator[str]:
    with s3_endpoint():
        url = f"s3://alelt-topo-{uuid.uuid4().hex[:8]}/hub/spool"
        bucket(url)
        yield url


@pytest.fixture()
def pgdb() -> Iterator[str]:
    if not ADMIN:
        pytest.skip("the HA target needs PostgreSQL: set ALELT_TEST_PG_URL")
    name = f"alelt_test_topo_{uuid.uuid4().hex[:8]}"
    _admin(f"CREATE DATABASE {name}")
    yield f"{ADMIN.replace('postgresql://', 'postgresql+psycopg://')}/{name}"
    _admin(f"DROP DATABASE {name} WITH (FORCE)")


def test_plan_blockers(tmp_path: Path) -> None:
    from alelt_hub import topology

    src = f"sqlite:///{(tmp_path / 'm.db').as_posix()}"
    with hub(src, "file://" + (tmp_path / "spool").as_posix(), tmp_path / "scratch"):
        p = topology.plan("ha", check=True)
        assert len(p["blockers"]) == 2                            # SQLite metadata + local spool
        assert all(s["status"] == "blocked" for s in p["steps"])
        assert topology.apply("ha", log=lambda _m: None) == 1     # preflight refuses, nothing changed
        st = topology.status()
        assert st["mode"] == "standalone" and {x["layer"]: x["ha"] for x in st["layers"]}["metadata"] is False
        assert [o["mode"] for o in st["install_options"]] == ["standalone", "ha", "ha"]
        with pytest.raises(ValueError):
            topology.plan("sideways")


def test_switch_standalone_ha_standalone(tmp_path: Path, s3: str, pgdb: str) -> None:
    from alelt_hub import topology

    n = 12
    src_db = f"sqlite:///{(tmp_path / 'src' / 'alelt.db').as_posix()}"
    (tmp_path / "src").mkdir()
    src_spool = "file://" + (tmp_path / "spool").as_posix()
    out = tmp_path / "out"
    logs: list[str] = []
    # ---------------------------------------------------------------- standalone -> HA
    with hub(src_db, src_spool, tmp_path / "scratch") as src:
        _seed(src, n)
        p = topology.plan("ha", db_url=pgdb, spool_url=s3)
        assert not p["blockers"] and [s["status"] for s in p["steps"]] == ["todo"] * 7
        with FakeAgentLoop(src):
            rc = topology.apply("ha", db_url=pgdb, spool_url=s3, out=str(out), drain_timeout_s=30, log=logs.append)
        assert rc == 3, logs                                       # waits for the restart on the new configuration
        op = _state(src)["operation"]
        assert op["status"] == "waiting" and AGENT in op["paused_agents"]
        assert [op["steps"][k]["status"] for k in topology.STEPS[:-1]] == ["done"] * 6
        env_txt = (out / "alelt.ha.env").read_text()
        assert f"ALELT_SPOOL_URL={s3}" in env_txt and "alelttestsecret" not in env_txt and "ALELT_TOPOLOGY=ha" in env_txt
        assert (out / "values.ha.yaml").exists()
        assert topology.apply("ha", db_url=pgdb, spool_url=s3, out=str(out), log=logs.append) == 3   # idempotent
        assert topology.apply("standalone", log=logs.append) == 1  # another switch is refused while unfinished
        assert _audits(src, "topology.") == ["topology.preflight", "topology.freeze", "topology.drain",
                                             "topology.copy_spool", "topology.migrate_metadata", "topology.write_config"]
    with hub(pgdb, s3, tmp_path / "scratch-ha") as dst:
        _verify_batches(dst, s3 + "/", n)                          # every object in S3, refs rewritten, sha256 equal
        assert _commands(dst, "RESUME") == 0                       # nothing resumed before the restart
        _restart_on(dst, s3)
    with hub(src_db, src_spool, tmp_path / "scratch"):
        assert topology.apply("ha", db_url=pgdb, spool_url=s3, out=str(out), log=logs.append) == 0, logs
    with hub(pgdb, s3, tmp_path / "scratch-ha") as dst:
        assert _commands(dst, "RESUME") == 1
        st = _state(dst)
        assert st["mode"] == "ha" and st["operation"]["status"] == "done"
        assert "topology.rewrite_refs" in _audits(dst, "topology.") and "topology.switched" in _audits(dst, "topology.")
        layers = {x["layer"]: x["ha"] for x in topology.status()["layers"]}
        assert layers["metadata"] and layers["spool"]
        # ------------------------------------------------------------ HA -> standalone (new SQLite + local spool)
        back_db = f"sqlite:///{(tmp_path / 'back' / 'alelt.db').as_posix()}"
        (tmp_path / "back").mkdir()
        back_spool = "file://" + (tmp_path / "back-spool").as_posix()
        with FakeAgentLoop(dst):
            rc = topology.apply("standalone", db_url=back_db, spool_url=back_spool, out=str(out), drain_timeout_s=30,
                                log=logs.append)
        assert rc == 3, logs
        assert (out / "alelt.standalone.env").exists()
    with hub(back_db, back_spool, tmp_path / "scratch-back") as back:
        _verify_batches(back, "", n, local=True)
        _restart_on(back, back_spool)
    with hub(pgdb, s3, tmp_path / "scratch-ha") as dst:
        assert topology.apply("standalone", db_url=back_db, spool_url=back_spool, out=str(out), log=logs.append) == 0
        assert _state(dst)["superseded_by"].startswith("sqlite")
    with hub(back_db, back_spool, tmp_path / "scratch-back") as back:
        _verify_batches(back, "", n)
        st = _state(back)
        assert st["mode"] == "standalone" and _commands(back, "RESUME") == 2   # one per switch
        assert topology.status()["mode"] == "standalone"


def test_failed_drain_resume_and_abort(tmp_path: Path, s3: str, pgdb: str) -> None:
    from alelt_hub import topology

    src_db = f"sqlite:///{(tmp_path / 'alelt.db').as_posix()}"
    logs: list[str] = []
    with hub(src_db, "file://" + (tmp_path / "spool").as_posix(), tmp_path / "scratch") as src:
        _seed(src, 3)
        rc = topology.apply("ha", db_url=pgdb, spool_url=s3, out=str(tmp_path / "o"), drain_timeout_s=2,
                            log=logs.append)                       # agent never acks PAUSE
        op = _state(src)["operation"]
        assert rc == 1 and op["status"] == "failed" and op["steps"]["drain"]["status"] == "failed"
        assert "drain timed out" in op["steps"]["drain"]["detail"]
        assert topology.apply("ha", db_url=pgdb, spool_url=s3 + "-other", log=logs.append) == 1   # other target
        p = topology.plan("standalone")
        assert any("unfinished" in b for b in p["blockers"])
        with FakeAgentLoop(src):                                   # cause fixed: re-run continues at drain
            rc = topology.apply("ha", db_url=pgdb, spool_url=s3, out=str(tmp_path / "o"), drain_timeout_s=30,
                                log=logs.append)
        assert rc == 3
        assert _audits(src, "topology.").count("topology.preflight") == 1     # preflight not repeated
        assert _commands(src, "PAUSE") == 1                                    # PAUSE sent once
        assert topology.abort(log=logs.append) == 0
        op = _state(src)["operation"]
        assert op["status"] == "aborted" and _commands(src, "RESUME") == 1
        assert "topology.abort" in _audits(src, "topology.")
        assert topology.abort(log=logs.append) == 0 and _commands(src, "RESUME") == 1   # nothing left to abort
        assert not topology.plan("standalone")["blockers"]
