"""Out-of-row completion in the data path (RC1 / ADR-013, FIX-DESIGN WP1), both metadata backends, in-process rig.

The destination is an in-memory SPI implementation (checkpoint CAS, partial UPDATE merge, explicit watermarks
recorded) so every case runs on SQLite and Postgres metadata without a landing database. The edge is `SimEdge`
(alelt_hub.testing): it ships out-of-row values as needs_keyed_reread signals and answers VERIFY_TABLE{rows} from its
truth, optionally frozen at an earlier read position, delayed, refusing or truncating.
"""
from __future__ import annotations

import time
import uuid
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import func, select, update

from alelt_common.models import Position
from alelt_common.util import utcnow
from alelt_hub.testing import FakeAgent, SimEdge, lri, sim_catalog

from .conftest import TOKEN
from .test_integrity_replay import MemDest

DEST: dict[str, "WmDest"] = {}


class WmDest(MemDest):
    """MemDest + per-stream watermarks (forward-only merge, like alelt_dst_kit) + delete_checkpoint."""

    def __init__(self) -> None:
        super().__init__()
        self.wm: dict[str, dict[str, str]] = {}
        self.dropped: list[str] = []
        self.flushes: list[tuple[str, int]] = []

    def apply(self, stream: str, changesets: Any, to_pos: Position, batch_id: str, table_map: Any, tables: Any,
              **kw: Any) -> Any:
        before = self.ck.get(stream)
        out = super().apply(stream, changesets, to_pos, batch_id, table_map, tables, **kw)
        if self.ck.get(stream) != before:
            n = sum(len(cs.events) for cs in changesets)
            self.flushes.append((stream, n))
            wm = kw.get("watermarks")
            cur = self.wm.setdefault(stream, {})
            new = ({table_map[k] if k in table_map else str(k): (to_pos.text if v is None else str(v))
                    for k, v in wm.items()} if wm else {d: to_pos.text for d in table_map.values()})
            for k, v in new.items():
                if k not in cur or self.cmp(Position(engine="fake", text=v), Position(engine="fake", text=cur[k])) > 0:
                    cur[k] = v
        return out

    def delete_checkpoint(self, stream: str) -> None:
        self.dropped.append(stream)
        self.ck.pop(stream, None)


@pytest.fixture()
def env(client: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    from alelt_hub import pipeline

    monkeypatch.setattr(pipeline, "make_destination",
                        lambda s, policy, source, spec=None: DEST.setdefault(policy.connection_id, WmDest()))
    monkeypatch.setenv("ALELT_COMPLETION_POLL_S", "0")
    monkeypatch.setenv("ALELT_VERIFY_EDGE_TIMEOUT_S", "20")
    return client


def _setup(client: Any, n: int = 1, lob_every: int = 1) -> tuple[str, FakeAgent, Any, Any]:
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    cid = f"cp{uuid.uuid4().hex[:6]}"
    a = FakeAgent(client, TOKEN, agent_id=f"{cid}-agent", connection_id=cid)
    a.register()
    cat = sim_catalog(cid, n, lob_every=lob_every)
    a.catalog(cat)
    return cid, a, cat, Worker(get_engine(), get_settings(), f"{cid}-w")


def _ready() -> None:
    from alelt_hub.db import get_engine, job, table_completion

    with get_engine().begin() as c:
        c.execute(update(job).where(job.c.status == "QUEUED").values(run_after=utcnow() - timedelta(seconds=1)))
        c.execute(update(table_completion).values(next_attempt_at=utcnow() - timedelta(seconds=1)))


def _run(w: Any, until: Any, timeout_s: float = 30) -> None:
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout_s:
        _ready()
        busy = w.run_once()
        if until():
            return
        if not busy:
            time.sleep(0.05)
    from alelt_hub.db import get_engine, job

    with get_engine().connect() as c:
        jl = [tuple(r) for r in c.execute(select(job.c.kind, job.c.status, job.c.dedup_key, job.c.error)
                                          .where(job.c.kind == "decode").order_by(job.c.id.desc()).limit(8))]
    raise AssertionError(f"not reached within {timeout_s}s: {_entries()} jobs {jl}")


def _entries(cid: str | None = None) -> list[dict[str, Any]]:
    from alelt_hub.db import get_engine, table_completion

    q = select(table_completion)
    if cid:
        q = q.where(table_completion.c.connection_id == cid)
    with get_engine().connect() as c:
        return [dict(r) for r in c.execute(q).mappings()]


def _row(cid: str, fqn: str, rid: int) -> dict[str, Any] | None:
    d = DEST.get(cid)
    return d.rows.get(fqn.partition(".")[2], {}).get((str(rid),)) if d else None


def _counts(cid: str) -> dict[str, int]:
    from alelt_diagnose.tables import incident
    from alelt_hub.db import get_engine, quarantine_event, table_hold

    with get_engine().connect() as c:
        return {
            "quarantine": c.execute(select(func.count()).select_from(quarantine_event).where(
                quarantine_event.c.connection_id == cid)).scalar_one(),
            "holds": c.execute(select(func.count()).select_from(table_hold).where(
                table_hold.c.connection_id == cid, table_hold.c.held.is_(True))).scalar_one(),
            "incidents": c.execute(select(func.count()).select_from(incident).where(
                incident.c.connection_id == cid, incident.c.kind.in_(
                    ("TABLE_OUT_OF_SYNC", "COMPLETION_STALLED", "COMPLETION_OVERSIZE")))).scalar_one()}


def _incidents(cid: str, kind: str) -> list[tuple[str, str]]:
    from alelt_diagnose.tables import incident
    from alelt_hub.db import get_engine

    with get_engine().connect() as c:
        return [(str(a), str(b)) for a, b in c.execute(select(incident.c.status, incident.c.table_fqn).where(
            incident.c.connection_id == cid, incident.c.kind == kind))]


# ------------------------------------------------------------------ 1. one signal -> one fetch -> value written
def test_one_signal_one_fetch_value_written_no_integrity(env: Any) -> None:
    cid, a, cat, w = _setup(env)
    with SimEdge(env, a, cat) as e:
        e.tx([("INSERT", "SIM.T000", 1, "one", "doc one")])
        _run(w, lambda: not _entries(cid) and (_row(cid, "SIM.T000", 1) or {}).get("DOC") == "doc one", 5)
    r = _row(cid, "SIM.T000", 1)
    assert r["NAME"] == "one" and r["DOC"] == "doc one"
    vt = [x for n, x in e.args if n == "VERIFY_TABLE"]
    assert len(vt) == 1 and vt[0]["checks"] == ["rows"] and vt[0]["columns"] == ["ID", "DOC"]
    assert vt[0]["key_values"] == [["1"]]
    assert _counts(cid) == {"quarantine": 0, "holds": 0, "incidents": 0}


# ------------------------------------------------------------------ 2. batching by keys and bytes
def test_2500_keys_three_commands(env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    cid, a, cat, w = _setup(env)
    with SimEdge(env, a, cat) as e:
        e.paused = True
        for x in range(0, 2500, 500):
            e.tx([("INSERT", "SIM.T000", i, f"n{i}", f"d{i}") for i in range(x, x + 500)])
        _run(w, lambda: len({r["command_id"] for r in _entries(cid) if r["status"] == "INFLIGHT"}) == 3)
        cmds: dict[str, int] = {}
        for r in _entries(cid):
            cmds[str(r["command_id"])] = cmds.get(str(r["command_id"]), 0) + 1
        assert sorted(cmds.values()) == [500, 1000, 1000]
        e.paused = False
        _run(w, lambda: not _entries(cid), 60)
    assert all(_row(cid, "SIM.T000", i)["DOC"] == f"d{i}" for i in range(2500))
    assert e.seen["VERIFY_TABLE"] == 3


def test_byte_cap_with_composite_key() -> None:
    from alelt_hub import completion

    ents = [{"key": {"A": f"{'x' * 40}{i}", "B": i, "C": f"c{i}"}} for i in range(3000)]
    import os

    os.environ["ALELT_COMPLETION_CMD_BYTES"] = "4096"
    try:
        chunks = completion._chunks(ents, ["A", "B", "C"], 1000)
    finally:
        os.environ.pop("ALELT_COMPLETION_CMD_BYTES")
    import json

    assert sum(len(c) for c in chunks) == 3000 and len(chunks) > 3
    for c in chunks:
        assert len(json.dumps([[str(e["key"][k]) for k in "ABC"] for e in c])) <= 4096 + 64


# ------------------------------------------------------------------ 3./4. read position semantics
def test_newer_change_wins_over_read_at_r(env: Any) -> None:
    import copy

    cid, a, cat, w = _setup(env)
    with SimEdge(env, a, cat) as e:
        e.paused = True
        e.tx([("INSERT", "SIM.T000", 1, "v1", "doc1")])
        with e.lock:
            e.frozen = (e.pos, copy.deepcopy(e.truth))           # the edge read happens HERE (NAME v1)
        e.tx([("UPDATE", "SIM.T000", 1, "v2", None)])            # committed after R, no signal
        e.paused = False
        _run(w, lambda: not _entries(cid) and (_row(cid, "SIM.T000", 1) or {}).get("DOC") == "doc1")
    assert _row(cid, "SIM.T000", 1)["NAME"] == "v2"


def test_signal_after_read_requeues_and_converges_to_newest(env: Any) -> None:
    import copy

    cid, a, cat, w = _setup(env)
    with SimEdge(env, a, cat) as e:
        e.paused = True
        e.tx([("INSERT", "SIM.T000", 1, "v1", "doc1")])
        _run(w, lambda: any(r["status"] == "INFLIGHT" for r in _entries(cid)))
        with e.lock:
            e.frozen = (e.pos, copy.deepcopy(e.truth))           # read at R: doc1
        e.tx([("UPDATE", "SIM.T000", 1, None, "doc2")])          # second signal after R
        e.paused = False
        _run(w, lambda: e.seen.get("VERIFY_TABLE", 0) >= 1 and (_row(cid, "SIM.T000", 1) or {}).get("DOC") == "doc1")
        e.frozen = None
        _run(w, lambda: not _entries(cid) and (_row(cid, "SIM.T000", 1) or {}).get("DOC") == "doc2")
    assert e.seen["VERIFY_TABLE"] >= 2


# ------------------------------------------------------------------ 5. delete before the answer
def test_delete_before_answer_never_resurrects(env: Any) -> None:
    cid, a, cat, w = _setup(env)
    with SimEdge(env, a, cat) as e:
        e.paused = True
        e.tx([("INSERT", "SIM.T000", 1, "v1", "doc1")])
        _run(w, lambda: any(r["status"] == "INFLIGHT" for r in _entries(cid)))
        import copy

        with e.lock:
            e.frozen = (e.pos, copy.deepcopy(e.truth))           # the read still sees the row ...
        e.tx([("DELETE", "SIM.T000", 1, None, None)])            # ... the DELETE commits after it
        _run(w, lambda: bool(_row(cid, "SIM.T000", 1)) and _row(cid, "SIM.T000", 1)["_deleted"])
        e.paused = False
        _run(w, lambda: not _entries(cid) and e.seen.get("VERIFY_TABLE", 0) >= 1)
        for _ in range(5):
            _ready()
            w.run_once()
        assert _row(cid, "SIM.T000", 1)["_deleted"] is True     # not resurrected by the late answer
        e.frozen = None
        e.tx([("INSERT", "SIM.T000", 1, "again", "doc3")])       # re-insert -> new entry -> converges
        _run(w, lambda: not _entries(cid) and (_row(cid, "SIM.T000", 1) or {}).get("DOC") == "doc3")
    assert _row(cid, "SIM.T000", 1)["_deleted"] is False


# ------------------------------------------------------------------ 6. missing / failure / stall / recovery
def test_missing_key_is_done_without_write(env: Any) -> None:
    cid, a, cat, w = _setup(env)
    with SimEdge(env, a, cat) as e:
        e.paused = True
        e.tx([("INSERT", "SIM.T000", 1, "v1", "doc1")])
        e.tx([("DELETE", "SIM.T000", 1, None, None)])
        _run(w, lambda: bool(_row(cid, "SIM.T000", 1)))
        e.paused = False
        _run(w, lambda: not _entries(cid))
    assert _row(cid, "SIM.T000", 1)["_deleted"] is True


def test_failure_backoff_stall_incident_then_recovery(env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALELT_COMPLETION_STALL_S", "1")
    cid, a, cat, w = _setup(env, n=2, lob_every=2)              # T000 with DOC, T001 without
    with SimEdge(env, a, cat) as e:
        e.refuse = {"VERIFY_TABLE": "NOT_ALLOWED"}
        e.tx([("INSERT", "SIM.T000", 1, "v1", "doc1")])
        _run(w, lambda: any((r["attempts"] or 0) >= 1 and "NOT_ALLOWED" in (r["last_error"] or "")
                            for r in _entries(cid)))
        time.sleep(1.2)
        e.tx([("INSERT", "SIM.T001", 7, "other", None)])         # the stream keeps applying other rows
        _run(w, lambda: bool(_row(cid, "SIM.T001", 7)) and bool(_incidents(cid, "COMPLETION_STALLED")))
        assert _counts(cid)["holds"] == 0
        e.refuse = {}
        _run(w, lambda: not _entries(cid) and (_row(cid, "SIM.T000", 1) or {}).get("DOC") == "doc1")
    assert {s for s, _ in _incidents(cid, "COMPLETION_STALLED")} == {"RESOLVED"}


# ------------------------------------------------------------------ 7. truncation
def test_truncated_result_halves_then_oversize(env: Any) -> None:
    cid, a, cat, w = _setup(env)
    with SimEdge(env, a, cat) as e:
        e.max_bytes = 40                                         # a few rows per answer
        e.tx([("INSERT", "SIM.T000", i, f"n{i}", f"doc{i:04d}") for i in range(8)])
        _run(w, lambda: not _entries(cid), 60)
        sizes = [len(x["key_values"]) for n, x in e.args if n == "VERIFY_TABLE"]
        assert sizes[0] == 8 and min(sizes) < 8                   # halved re-issues
        assert all(_row(cid, "SIM.T000", i)["DOC"] == f"doc{i:04d}" for i in range(8))
        e.max_bytes = None
        e.oversize_ids = {1}                                     # one row alone exceeds the cap
        e.tx([("UPDATE", "SIM.T000", 1, None, "x" * 50)])
        _run(w, lambda: bool(_incidents(cid, "COMPLETION_OVERSIZE")))
    ent = _entries(cid)
    assert len(ent) == 1 and ent[0]["last_error"] == "oversize" and ent[0]["status"] == "PENDING"


# ------------------------------------------------------------------ 8. completion floor + DR rebuild
def test_completion_floor_watermark_and_rebuild(env: Any) -> None:
    from alelt_hub import completion
    from alelt_hub.db import get_engine, table_completion

    cid, a, cat, w = _setup(env, n=2, lob_every=2)
    with SimEdge(env, a, cat) as e:
        e.paused = True
        e.tx([("INSERT", "SIM.T000", 1, "v1", "doc1")])
        first = lri(e.pos)                                       # commit position of the signal's transaction
        e.tx([("INSERT", "SIM.T001", 2, "x", None)])
        _run(w, lambda: bool(_row(cid, "SIM.T001", 2)))
        assert DEST[cid].wm[cid][f"{cid}#t:SIM.T000"] == first   # floor = first signal position
        assert DEST[cid].wm[cid]["T001"] == lri(e.pos)
        with get_engine().begin() as c:                          # metadata lost (DR)
            c.execute(table_completion.delete().where(table_completion.c.connection_id == cid))
        assert completion.rebuild(w.pipeline(cid)) == 1
        assert [r["first_pos"] for r in _entries(cid)] == [first]
        e.paused = False
        _run(w, lambda: not _entries(cid))
        e.tx([("INSERT", "SIM.T001", 3, "y", None)])
        _run(w, lambda: bool(_row(cid, "SIM.T001", 3)))
    assert DEST[cid].wm[cid][f"{cid}#t:SIM.T000"] == lri(e.pos)   # drained: floor moved to the stream position


# ------------------------------------------------------------------ 9. crash between destination and metadata
def test_crash_after_destination_commit_refetch_idempotent(env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from alelt_hub import completion

    cid, a, cat, w = _setup(env)
    real = completion.after_flush
    boom = {"n": 0}

    def crash(p: Any, done: Any, held: Any) -> None:
        if boom["n"] == 0:
            boom["n"] += 1
            raise RuntimeError("crash after the destination committed")
        real(p, done, held)
    monkeypatch.setattr(completion, "after_flush", crash)
    with SimEdge(env, a, cat) as e:
        e.tx([("INSERT", "SIM.T000", 1, "v1", "doc1")])
        _run(w, lambda: boom["n"] == 1)
        assert (_row(cid, "SIM.T000", 1) or {}).get("DOC") == "doc1" and _entries(cid)   # written, entry kept
        e.tx([("INSERT", "SIM.T000", 2, "v2", None)])            # stream moves on: next flush re-carries the result
        _run(w, lambda: not _entries(cid))
    assert _row(cid, "SIM.T000", 1)["DOC"] == "doc1"


# ------------------------------------------------------------------ idle source: transient stream
def test_idle_source_completion_through_transient_stream(env: Any) -> None:
    cid, a, cat, w = _setup(env)
    with SimEdge(env, a, cat) as e:
        e.paused = True
        e.tx([("INSERT", "SIM.T000", 1, "v1", "doc1")])          # last change of the run: the source goes quiet
        _run(w, lambda: bool(_row(cid, "SIM.T000", 1)))
        e.paused = False
        _run(w, lambda: not _entries(cid))
    assert _row(cid, "SIM.T000", 1)["DOC"] == "doc1"
    d = DEST[cid]
    assert d.dropped and all(s.startswith(f"{cid}#completion:") for s in d.dropped)
    assert not any(k.startswith(f"{cid}#completion:") for k in d.ck)        # checkpoint rows dropped


# ------------------------------------------------------------------ 10. remote fetcher
def test_remote_fetcher_converges(env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from alelt_common.completeness import KeyedReread
    from alelt_hub import store
    from alelt_hub.settings import get_settings
    from alelt_hub.testing import FakeSource

    truth: dict[int, dict[str, Any]] = {}

    class RemoteSource(FakeSource):
        engine = "fakeremote"
        capture_mode = "remote"

        def keyed_reread(self, table: Any, keys: Any) -> Any:
            rows = [dict(truth[k["ID"]]) for k in keys if k["ID"] in truth]
            missing = [k for k in keys if k["ID"] not in truth]
            return KeyedReread(rows=rows, missing=missing, read_from=Position(engine="fakeremote", text=lri(99)),
                               isolation="test")

        def close(self) -> None:
            pass
    import alelt_hub.tests.test_completion as me

    me.RemoteSource = RemoteSource                                # type: ignore[attr-defined]
    get_settings().sources["fakeremote"] = "alelt_hub.tests.test_completion:RemoteSource"
    store.source_connector.cache_clear()

    class RC:
        def factory(self) -> Any:
            return RemoteSource
    import alelt_capture.config as cc

    monkeypatch.setattr(cc, "connections", lambda engine: {cid: RC()})
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings as gs
    from alelt_hub.worker import Worker

    cid = f"cr{uuid.uuid4().hex[:6]}"
    ag = FakeAgent(env, TOKEN, agent_id=f"capture:{cid}", connection_id=cid, engine="fakeremote")
    ag.register()
    cat = sim_catalog(cid, 1, lob_every=1, engine="fakeremote")
    ag.catalog(cat)
    w = Worker(get_engine(), gs(), f"{cid}-w")
    from alelt_common import completeness as fc
    from alelt_common.models import TableRef

    truth[1] = {"ID": 1, "NAME": "r", "AMT": None, "DOC": "remote doc"}
    sig = fc.keyed_reread(TableRef(schema="SIM", name="T000"), [{"ID": 1}], "toast", "pg:U", columns=["DOC"])
    ag.send([{"pos": lri(1), "txid": "t", "kind": "INSERT", "table": "SIM.T000", "after": {"ID": 1, "NAME": "r"}},
             {"pos": lri(2), "txid": "t", "kind": "OTHER", "table": "SIM.T000", "note": sig.to_note()},
             {"pos": lri(3), "txid": "t", "kind": "COMMIT"}])
    _run(w, lambda: not _entries(cid) and (_row(cid, "SIM.T000", 1) or {}).get("DOC") == "remote doc")


# ------------------------------------------------------------------ rolled-back signals never queue
def test_rolled_back_signal_never_queued(env: Any) -> None:
    cid, a, cat, w = _setup(env)
    with SimEdge(env, a, cat) as e:
        e.tx([("INSERT", "SIM.T000", 1, "v1", "doc1")], rollback=True)
        e.tx([("INSERT", "SIM.T000", 2, "v2", None)])
        _run(w, lambda: bool(_row(cid, "SIM.T000", 2)))
    assert _entries(cid) == [] and _row(cid, "SIM.T000", 1) is None


# ------------------------------------------------------------------ 11. binary out-of-row values: hex text form
def test_binary_values_hex_text_roundtrip() -> None:
    """Edges put BLOB / VARBINARY values on the wire as hex text (alelt_common.verify_edge.text, Db2 CLI hex form);
    parse_text turns them into the exact bytes for BINARY columns (completion results are typed by the catalog)."""
    from alelt_common.verify_edge import parse_text, text

    for b in (b"", b"\x00\x01\xff", bytes(range(256)) * 4):
        t = text(b)
        assert t == b.hex().upper() and parse_text(t, "BINARY") == b
    assert parse_text("00ff", "BINARY") == b"\x00\xff"                     # either case
