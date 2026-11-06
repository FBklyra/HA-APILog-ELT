"""Table lifecycle (RC2 / RC3, ADR-012, FIX-DESIGN WP2), both metadata backends, in-process rig (no landing needed).

NEW -> INITIAL_LOAD -> CATCH_UP -> STREAMING for every entry point; initial load / catch-up land whatever the schedule
window; the schedule gate starts at the attach position; integrity takes over during INITIAL_LOAD; restarts resume
from metadata (snapshot `key_from`, bulk passes, attach tails) with exactly one apply per change.
"""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import select, update

from alelt_common.util import utcnow
from alelt_hub.testing import FakeAgent, SimEdge, sim_catalog

from .conftest import TOKEN
from .test_completion import DEST, WmDest


@pytest.fixture()
def env(client: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    from alelt_hub import ops, pipeline

    monkeypatch.setattr(pipeline, "make_destination",
                        lambda s, policy, source, spec=None: DEST.setdefault(policy.connection_id, WmDest()))
    monkeypatch.setattr(ops, "verify_table", lambda engine, p, fqn, kind="COLUMN_CHECKSUM": SimpleNamespace(
        outcome="MATCH", summary={"settled": True}, results=[]))
    monkeypatch.setenv("ALELT_INITIAL_LOAD", "auto")
    monkeypatch.setenv("ALELT_COMPLETION_POLL_S", "0")
    monkeypatch.setenv("ALELT_VERIFY_EDGE_TIMEOUT_S", "20")
    monkeypatch.setenv("ALELT_SNAPSHOT_IGNORE_WINDOW", "1")
    return client


def _doc(cid: str, tables: dict[str, Any] | None = None, **conn: Any) -> dict[str, Any]:
    return {"connection_id": cid, "output_profile": "alelt",
            "schemas": {"SIM": {"enabled": True, "tables": tables or {}}}, **conn}


def _setup(client: Any, n: int = 2, lob_every: int = 0, doc: dict[str, Any] | None = None,
           catalog: bool = True) -> tuple[str, FakeAgent, Any, Any]:
    from alelt_hub import config_store as cs
    from alelt_hub.db import get_engine, pipeline_definition
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    cid = f"lc{uuid.uuid4().hex[:6]}"
    a = FakeAgent(client, TOKEN, agent_id=f"{cid}-agent", connection_id=cid)
    a.register()
    if doc is not None:
        with get_engine().begin() as c:
            c.execute(pipeline_definition.insert().values(
                connection_id=cid, origin="wizard", environment="TEST", source_engine="fake",
                destination_engine="postgres", source={}, destination={}, tables_doc=_doc(cid), status="SHADOW",
                created_by="t"))
            cs.commit(c, cid, {**_doc(cid), **doc}, "test", "lifecycle test", "test")
    cat = sim_catalog(cid, n, lob_every=lob_every)
    if catalog:
        a.catalog(cat)
    return cid, a, cat, Worker(get_engine(), get_settings(), f"{cid}-w")


def _ready() -> None:
    from alelt_hub.db import get_engine, job, table_completion

    with get_engine().begin() as c:
        c.execute(update(job).where(job.c.status == "QUEUED").values(run_after=utcnow() - timedelta(seconds=1)))
        c.execute(update(table_completion).values(next_attempt_at=utcnow() - timedelta(seconds=1)))


def _run(w: Any, until: Any, timeout_s: float = 60, ready: bool = True) -> None:
    t0 = time.monotonic()
    swept = 0.0
    while time.monotonic() - t0 < timeout_s:
        if ready:
            _ready()
        if time.monotonic() - swept > 1.0:                 # the worker's periodic sweep (every 30 s in production)
            w.sweep()
            swept = time.monotonic()
        busy = w.run_once()
        if until():
            return
        if not busy:
            w.sweep()
            time.sleep(0.05)
    raise AssertionError(f"not reached within {timeout_s}s: {_phases(w)} {_jobs()}")


def _jobs() -> list[tuple[Any, ...]]:
    from alelt_hub.db import get_engine, job

    with get_engine().connect() as c:
        return [tuple(r) for r in c.execute(select(job.c.kind, job.c.status, job.c.error).where(
            job.c.status != "DONE").order_by(job.c.id.desc()).limit(8))]


def _phases(w: Any, cid: str | None = None) -> dict[str, str]:
    from alelt_hub import lifecycle
    from alelt_hub.db import get_engine, table_lifecycle

    if cid is None:
        with get_engine().connect() as c:
            return {f"{a}:{b}": str(p) for a, b, p in c.execute(select(
                table_lifecycle.c.connection_id, table_lifecycle.c.table_fqn, table_lifecycle.c.phase)
                .order_by(table_lifecycle.c.updated_at.desc()).limit(10))}
    return {r["table_fqn"]: str(r["phase"]) for r in lifecycle.rows(get_engine(), cid)}


def _streaming(cid: str, n: int) -> bool:
    from alelt_hub import lifecycle
    from alelt_hub.db import get_engine

    rs = lifecycle.rows(get_engine(), cid)
    return len(rs) == n and all(r["phase"] == "STREAMING" and r["verified_at"] is not None for r in rs)


def _live(cid: str, fqn: str) -> dict[Any, dict[str, Any]]:
    return DEST[cid].live(fqn.partition(".")[2])


def _applied(cid: str, fqn: str, rid: int, kind: str | None = None) -> int:
    return sum(1 for s, t, k, key in DEST[cid].applied
               if t == fqn and key == str((str(rid),)) and (kind is None or k == kind))


# ------------------------------------------------------------------ 1. every entry point creates rows
def test_every_entry_point_creates_lifecycle_rows(env: Any) -> None:
    from alelt_hub import config_store as cs
    from alelt_hub import lifecycle
    from alelt_hub.db import get_engine

    # edge catalog (agent POST /catalog), mode auto
    cid, a, cat, w = _setup(env, n=2)
    assert _phases(w, cid) == {"SIM.T000": "NEW", "SIM.T001": "NEW"}
    # initial_load: none -> STREAMING at once; replay_from -> CATCH_UP from the position (driver)
    cid2, a2, cat2, w2 = _setup(env, n=2, doc={"initial_load": "none", "schemas": {"SIM": {"enabled": True, "tables": {
        "T001": {"replay_from": "1:0000000000000000"}}}}})
    ph = _phases(w2, cid2)
    assert ph["SIM.T000"] == "STREAMING" and ph["SIM.T001"] == "NEW"
    lifecycle.drive(get_engine(), w2, cid2)
    assert _phases(w2, cid2)["SIM.T001"] == "CATCH_UP"
    # config import / console revision disabling then re-enabling a table
    with get_engine().begin() as c:
        cs.commit(c, cid, _doc(cid, {"T001": {"enabled": False}}), "t", "disable", "test")
    assert _phases(w, cid)["SIM.T001"] == "DISABLED"
    with get_engine().begin() as c:
        cs.commit(c, cid, _doc(cid, {"T001": {"enabled": True}}), "t", "enable", "test")
    assert _phases(w, cid)["SIM.T001"] == "NEW"
    # a later catalog with a new table (allow_new_tables): only that table starts its initial load
    big = sim_catalog(cid, 3, lob_every=0)
    a.catalog(big.model_copy(update={"taken_at": datetime.now(timezone.utc)}))
    assert _phases(w, cid)["SIM.T002"] == "NEW"
    # remote runner catalog ingest goes through the same store.ingest_catalog
    from alelt_capture.receipt import ingest_catalog
    from alelt_hub.db import connection
    from alelt_hub.dialect import insert

    rcid = f"rm{uuid.uuid4().hex[:6]}"
    with get_engine().begin() as c:
        c.execute(insert(connection).values(id=rcid, engine="fake"))
    ingest_catalog(get_engine(), sim_catalog(rcid, 1, lob_every=0))
    assert _phases(w, rcid) == {"SIM.T000": "NEW"}


def test_file_seed_creates_rows_after_first_catalog(env: Any, tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from alelt_hub import config_store as cs
    from alelt_hub.db import get_engine

    cid = f"fs{uuid.uuid4().hex[:6]}"
    f = tmp_path / "t.yaml"
    f.write_text(f"connection_id: {cid}\noutput_profile: alelt\nschemas:\n  SIM:\n    enabled: true\n",
                 encoding="utf-8")
    assert cs.seed_from_files(get_engine(), str(f))
    a = FakeAgent(env, TOKEN, agent_id=f"{cid}-agent", connection_id=cid)
    a.register()
    a.catalog(sim_catalog(cid, 2, lob_every=0))
    assert _phases(None, cid) == {"SIM.T000": "NEW", "SIM.T001": "NEW"}


# ------------------------------------------------------------------ 2. pre-capture rows land
def test_initial_load_lands_pre_capture_rows_and_streams(env: Any) -> None:
    cid, a, cat, w = _setup(env, n=2, lob_every=2)
    with SimEdge(env, a, cat) as e:
        for i in (2, 3, 4):
            e.seed("SIM.T000", i, f"C000{i}", doc=f"note {i}")
            e.seed("SIM.T001", i, f"D{i}")
        e.tx([("UPDATE", "SIM.T000", 3, "C0003-upd", None)])    # captured before the snapshot read
        _run(w, lambda: _streaming(cid, 2))
        assert {k[0] for k in _live(cid, "SIM.T000")} == {"2", "3", "4"}
        assert _live(cid, "SIM.T000")[("3",)]["NAME"] == "C0003-upd" and _live(cid, "SIM.T000")[("2",)]["DOC"] == "note 2"
        e.tx([("INSERT", "SIM.T001", 9, "cdc", None)])
        _run(w, lambda: ("9",) in _live(cid, "SIM.T001"))
    assert e.seen["SNAPSHOT_TABLE"] == 2


# ------------------------------------------------------------------ 3./4. gate precedence
def _closed_window() -> dict[str, str]:
    now = datetime.now(timezone.utc)
    s = (now + timedelta(hours=3)).strftime("%H:%M")
    e_ = (now + timedelta(hours=4)).strftime("%H:%M")
    return {"start": s, "end": e_, "tz": "UTC"}


def _open_window() -> dict[str, str]:
    now = datetime.now(timezone.utc)
    return {"start": (now - timedelta(hours=1)).strftime("%H:%M"), "end": (now + timedelta(hours=1)).strftime("%H:%M"),
            "tz": "UTC"}


def test_window_table_initial_load_lands_then_gate_at_attach_position(env: Any) -> None:
    from alelt_hub import config_store as cs
    from alelt_hub import holds
    from alelt_hub.db import get_engine

    cid, a, cat, w = _setup(env, n=2, doc={"schemas": {"SIM": {"enabled": True, "tables": {
        "T000": {"schedule": {"window": _closed_window()}}}}}})
    with SimEdge(env, a, cat) as e:
        for i in (1, 2):
            e.seed("SIM.T000", i, f"w{i}")
            e.seed("SIM.T001", i, f"s{i}")
        e.tx([("UPDATE", "SIM.T000", 1, "w1-catchup", None)])   # in the catch-up range
        _run(w, lambda: _streaming(cid, 2))
        assert _live(cid, "SIM.T000")[("1",)]["NAME"] == "w1-catchup"   # landed while the window is closed
        h = holds.get(get_engine(), cid, "SIM.T000", "schedule")
        assert h is not None and h["held"]
        att = w.pipeline(cid).flushed_position()
        assert h["start_pos"] == att                              # gate placed exactly at the attach position
        e.tx([("UPDATE", "SIM.T000", 2, "late", None), ("UPDATE", "SIM.T001", 2, "s2-live", None)])
        _run(w, lambda: _live(cid, "SIM.T001")[("2",)]["NAME"] == "s2-live")
        for _ in range(10):
            _ready()
            w.run_once()
        assert _live(cid, "SIM.T000")[("2",)]["NAME"] == "w2"     # early == 0: nothing before the window opens
        with get_engine().begin() as c:
            cs.commit(c, cid, {**_doc(cid), "schemas": {"SIM": {"enabled": True, "tables": {
                "T000": {"schedule": {"window": _open_window()}}}}}}, "t", "open window", "test")
        w.pipeline(cid).refresh_policy()
        _run(w, lambda: _live(cid, "SIM.T000")[("2",)]["NAME"] == "late")
    assert _applied(cid, "SIM.T000", 2, "UPDATE") == 1           # one pass, no double apply
    assert _applied(cid, "SIM.T000", 1, "UPDATE") <= 1           # snapshot row or catch-up, never twice


def test_deferred_table_initial_load_lands_then_rounds(env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from alelt_hub import holds
    from alelt_hub.db import get_engine

    monkeypatch.setenv("ALELT_SCHEDULE_DEFER_MIN_S", "60")
    cid, a, cat, w = _setup(env, n=1, doc={"schemas": {"SIM": {"enabled": True, "tables": {
        "T000": {"schedule": {"latency_target_s": 3600}}}}}})
    with SimEdge(env, a, cat) as e:
        e.seed("SIM.T000", 1, "d1")
        _run(w, lambda: _streaming(cid, 1))
        assert ("1",) in _live(cid, "SIM.T000")
        h = holds.get(get_engine(), cid, "SIM.T000", "schedule")
        assert h is not None and h["held"]
        e.tx([("UPDATE", "SIM.T000", 1, "d1-upd", None)])
        for _ in range(10):
            _ready()
            w.run_once()
        assert _live(cid, "SIM.T000")[("1",)]["NAME"] == "d1"     # deferred: not before the round
        holds.set_body(get_engine(), cid, "SIM.T000", "schedule", next_due=(utcnow() - timedelta(seconds=1)).isoformat())
        _run(w, lambda: _live(cid, "SIM.T000")[("1",)]["NAME"] == "d1-upd")
    assert _applied(cid, "SIM.T000", 1, "UPDATE") == 1


# ------------------------------------------------------------------ 5. integrity during INITIAL_LOAD
def test_integrity_hold_during_initial_load_takes_over(env: Any) -> None:
    from alelt_hub import holds, integrity
    from alelt_hub.db import get_engine

    cid, a, cat, w = _setup(env, n=1)
    with SimEdge(env, a, cat) as e:
        e.seed("SIM.T000", 1, "x")
        e.paused = True                                          # snapshot not answered yet
        _run(w, lambda: _phases(w, cid)["SIM.T000"] == "INITIAL_LOAD")
        snap = holds.get(get_engine(), cid, "SIM.T000", "snapshot")
        # an unexplained record of the table while it loads: integrity takes the table at the watermark
        p = w.pipeline(cid)
        from alelt_common.models import QuarantineRecord, TableRef

        integrity.on_quarantine(p, {"id": uuid.uuid4(), "seq": 1}, [QuarantineRecord(
            position=None, table=TableRef(schema="SIM", name="T000"), reason="undecodable: test", raw_ref=None)], p.held)
        holds.set_body(get_engine(), cid, "SIM.T000", integrity.KIND, step="manual")   # recovery waits (operator)
        r = integrity.row(get_engine(), cid, "SIM.T000")
        assert r["held"] and r["start_pos"] in (None, snap["start_pos"])  # watermark_for_new_hold (None = from the start)
        e.paused = False
        _run(w, lambda: _phases(w, cid)["SIM.T000"] == "CATCH_UP")
        for _ in range(10):                                      # blocked: no catch-up pass passes the integrity hold
            _ready()
            w.run_once()
        assert _phases(w, cid)["SIM.T000"] == "CATCH_UP"
        integrity.acknowledge(get_engine(), cid, "SIM.T000", "t", "test") or holds.put(
            get_engine(), cid, "SIM.T000", integrity.KIND, integrity.IN_SYNC, False, None, {})
        _run(w, lambda: _streaming(cid, 1))
    assert ("1",) in _live(cid, "SIM.T000")


# ------------------------------------------------------------------ 6. restart matrix
def test_restart_mid_snapshot_resumes_with_key_from(env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from alelt_hub import holds
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    monkeypatch.setenv("ALELT_SNAPSHOT_CHUNK_ROWS", "10")
    monkeypatch.setenv("ALELT_SNAPSHOT_CHUNK_TIMEOUT_S", "1")
    cid, a, cat, w = _setup(env, n=1)
    with SimEdge(env, a, cat) as e:
        for i in range(1, 36):
            e.seed("SIM.T000", i, f"r{i}")
        e.chunk_fail_after = 2                                   # the edge dies after two chunks (no ack)
        _run(w, lambda: cid in DEST and len(DEST[cid].rows.get("T000", {})) >= 20)
        sid = holds.get(get_engine(), cid, "SIM.T000", "snapshot")["body"]["snapshot_id"]
        w = Worker(get_engine(), get_settings(), f"{cid}-w2")   # worker restart: state from metadata only
        time.sleep(1.2)
        _run(w, lambda: _streaming(cid, 1))
    snaps = [x for n, x in e.args if n == "SNAPSHOT_TABLE"]
    assert len(snaps) == 2 and snaps[1]["snapshot_id"] == sid == snaps[0]["snapshot_id"]
    assert snaps[1]["key_from"] == ["20"]                        # resumed after the last loaded chunk
    assert sorted(int(k[0]) for k in _live(cid, "SIM.T000")) == list(range(1, 36))
    assert holds.get(get_engine(), cid, "SIM.T000", "snapshot")["body"]["reissues"] == 1


def test_restart_in_catch_up_bulk_and_before_streaming_exactly_once(env: Any) -> None:
    from alelt_hub import lifecycle, replay
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    cid, a, cat, w = _setup(env, n=1)
    with SimEdge(env, a, cat) as e:
        e.seed("SIM.T000", 1, "a")
        e.paused = True
        _run(w, lambda: _phases(w, cid)["SIM.T000"] == "INITIAL_LOAD")
        e.tx([("UPDATE", "SIM.T000", 1, "b", None)])             # committed after the hold, before the read
        e.paused = False
        _run(w, lambda: _phases(w, cid)["SIM.T000"] == "CATCH_UP")
        real = replay.run_pass
        crashed = {"n": 0}

        def crash(p: Any, pid: str) -> Any:
            if crashed["n"] == 0:
                crashed["n"] += 1
                raise RuntimeError("worker killed in the bulk phase")
            return real(p, pid)
        replay.run_pass = crash                                   # type: ignore[assignment]
        try:
            _run(w, lambda: crashed["n"] == 1)
        finally:
            replay.run_pass = real                               # type: ignore[assignment]
        w = Worker(get_engine(), get_settings(), f"{cid}-w3")
        real_caught = lifecycle.caught_up
        died = {"n": 0}

        def die(p: Any, fqn: str, held: set[str]) -> None:       # between the attach tail and STREAMING
            if died["n"] == 0:
                died["n"] += 1
                raise RuntimeError("worker killed after the attach tail")
            real_caught(p, fqn, held)
        lifecycle.caught_up = die                                 # type: ignore[assignment]
        try:
            _run(w, lambda: died["n"] == 1)
        finally:
            lifecycle.caught_up = real_caught                    # type: ignore[assignment]
        w = Worker(get_engine(), get_settings(), f"{cid}-w4")
        _run(w, lambda: _streaming(cid, 1))
        for _ in range(3):                                       # destination-level replay x3: nothing applied again
            w.pipeline(cid).rebuild()
            _ready()
            w.run_once()
    assert _live(cid, "SIM.T000")[("1",)]["NAME"] == "b"
    assert crashed["n"] == 1 and died["n"] == 1
    assert _applied(cid, "SIM.T000", 1, "UPDATE") <= 1           # snapshot row or catch-up: never twice


# ------------------------------------------------------------------ 7. 222-table shape
def test_catalog_shape_pacing_and_order(env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    monkeypatch.setenv("ALELT_SCHEDULE_DEFER_MIN_S", "60")
    n = int(os.environ.get("ALELT_TEST_SIM_TABLES", "222"))
    tables = {f"T{i:03d}": ({"schedule": {"window": _closed_window()}} if i % 4 == 0 else
                            {"schedule": {"latency_target_s": 3600}} if i % 4 == 1 else {}) for i in range(n)}
    cid, a, cat, w = _setup(env, n=n, lob_every=6, doc={"schemas": {"SIM": {"enabled": True, "tables": tables}}})
    peak = {"n": 0}
    order: list[str] = []
    t0 = time.monotonic()
    with SimEdge(env, a, cat) as e:
        for f in e.infos:
            for i in (1, 2):
                e.seed(f, i, f"{f}-{i}", doc="d" if e.lob(f) else None)
        orig = e._snapshot

        def spy(args: dict[str, Any]) -> Any:
            order.append(f"{args['table']['schema']}.{args['table']['name']}")
            from alelt_hub import holds
            from alelt_hub.db import get_engine

            loading = sum(1 for r in holds.rows(get_engine(), cid, "snapshot") if r["state"] == "SNAPSHOTTING")
            peak["n"] = max(peak["n"], loading)
            return orig(args)
        e._snapshot = spy                                         # type: ignore[method-assign]
        _run(w, lambda: _streaming(cid, n), 900)
    took = time.monotonic() - t0
    assert peak["n"] <= 8, peak
    rank = {f"SIM.T{i:03d}": (2 if i % 4 == 0 else 1 if i % 4 == 1 else 0) for i in range(n)}
    ranks = [rank[f] for f in order]
    assert ranks == sorted(ranks), ranks[:40]                    # stream -> deferred -> window
    assert all(len(_live(cid, f)) == 2 for f in rank)
    print(f"\n{n} tables initial load on the rig: {took:.1f}s")
