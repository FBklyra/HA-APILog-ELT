"""X-134 hub-core robustness: retention purge (HC-05/XC-08), busy re-enqueue dedup (HC-17), decoded cache byte bound
(HC-22), data-path jobs never abandoned (XC-10)."""
from __future__ import annotations

import uuid
from datetime import timedelta
from types import SimpleNamespace
from typing import Any

from sqlalchemy import delete, func, insert, select

from alelt_common.models import ChangeEvent, ChangeKind, Position, TableRef
from alelt_common.spi import DecodeResult
from alelt_common.util import utcnow

CONN = "robustconn"


def _engine() -> Any:
    from alelt_hub.db import get_engine

    return get_engine()


def _ensure_conn(c: Any, cid: str) -> None:
    from alelt_hub.db import connection

    if c.execute(select(connection.c.id).where(connection.c.id == cid)).first() is None:
        c.execute(insert(connection).values(id=cid, engine="fake"))


def _batch(c: Any, cid: str, status: str, age_days: float) -> uuid.UUID:
    from alelt_hub.db import batch

    bid = uuid.uuid4()
    c.execute(insert(batch).values(id=bid, connection_id=cid, agent_id="a", engine="fake", from_lri="1:0",
                                   to_lri="1:1", rec_count=1, raw_bytes=1, sha256="0" * 64, codec="none",
                                   spool_path=f"missing/{bid}", status=status,
                                   received_at=utcnow() - timedelta(days=age_days)))
    return bid


class _FakeWorker:
    def pipeline(self, cid: str) -> Any:
        if cid != CONN:
            raise KeyError(cid)
        return SimpleNamespace(source=SimpleNamespace(compare=lambda a, b: (a.text > b.text) - (a.text < b.text)),
                               engine_name="fake", s=SimpleNamespace(catchup_lookback=10))


def test_retention_purge_jobs_decoded_cache_and_raw_archive(client: Any, monkeypatch: Any) -> None:
    from alelt_hub import retention
    from alelt_hub.blobstore import get_store
    from alelt_hub.db import batch, checkpoint_mirror, job

    eng = _engine()
    now = utcnow()
    with eng.begin() as c:
        _ensure_conn(c, CONN)
        old_done = c.execute(insert(job).values(kind="decode", payload={}, status="DONE",
                                                finished_at=now - timedelta(days=8)).returning(job.c.id)).scalar_one()
        new_done = c.execute(insert(job).values(kind="decode", payload={}, status="DONE",
                                                finished_at=now - timedelta(days=1)).returning(job.c.id)).scalar_one()
        queued = c.execute(insert(job).values(kind="decode", payload={}, status="QUEUED").returning(job.c.id)).scalar_one()
        old_loaded = _batch(c, CONN, "LOADED", 40)
        old_recv = _batch(c, CONN, "RECEIVED", 40)        # not LOADED: never purged
        young = _batch(c, CONN, "LOADED", 1)
        c.execute(delete(checkpoint_mirror).where(checkpoint_mirror.c.stream == CONN))
        c.execute(insert(checkpoint_mirror).values(stream=CONN, connection_id=CONN, restart_seq=10**15))
    st = get_store()
    keys = {b: f"decoded/{CONN}/{b}.v1.x.cjl.z" for b in (old_loaded, young)}
    for k in keys.values():
        st.put(k, b"x")
    monkeypatch.setenv("ALELT_RAW_ARCHIVE_DAYS", "30")
    out = retention.purge(eng, _FakeWorker())
    with eng.connect() as c:
        left = {r[0] for r in c.execute(select(job.c.id).where(job.c.id.in_([old_done, new_done, queued])))}
        bl = {r[0] for r in c.execute(select(batch.c.id).where(batch.c.connection_id == CONN))}
    assert left == {new_done, queued} and out["jobs"] >= 1
    assert old_loaded not in bl and old_recv in bl and young in bl and out["batches"] == 1
    names = {k for k, _ in st.list(f"decoded/{CONN}/")}
    assert keys[old_loaded] not in names and keys[young] in names


def test_retention_floor_keeps_what_a_held_table_needs(client: Any) -> None:
    """A held table pins its recorded restart_seq: batches at or above it are never purged."""
    from alelt_hub import retention
    from alelt_hub.db import checkpoint_mirror

    eng = _engine()
    with eng.begin() as c:
        c.execute(delete(checkpoint_mirror).where(checkpoint_mirror.c.stream == CONN))
        c.execute(insert(checkpoint_mirror).values(stream=CONN, connection_id=CONN, restart_seq=500))
    import alelt_hub.retention as R

    orig = R.holds.rows
    R.holds.rows = lambda e, cid: [{"kind": "schedule", "state": "HELD", "held": True, "start_pos": "1:0",
                                    "body": {"restart_seq": 42}}]
    try:
        assert retention.replay_floor(eng, CONN, None, "fake", 10) == 42
    finally:
        R.holds.rows = orig


def test_busy_decode_reenqueue_is_one_job_per_bucket(client: Any) -> None:
    """HC-17: a worker finding the stream locked re-enqueues ONE look-again job per 2 s bucket, never a storm."""
    from alelt_hub.db import job
    from alelt_hub.dialect import try_lock, unlock
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    eng = _engine()
    cid = "busyconn-" + uuid.uuid4().hex[:6]
    lock = try_lock(eng, f"pipeline:{cid}")
    assert lock is not None
    try:
        w = Worker(eng, get_settings(), "busy-worker")
        for _ in range(5):
            w._decode({"connection_id": cid})
        with eng.connect() as c:
            n = c.execute(select(func.count()).select_from(job).where(job.c.dedup_key.like(f"busy:{cid}:%"))).scalar()
        assert 1 <= n <= 2                      # 5 attempts within <= 2 buckets
    finally:
        unlock(lock, f"pipeline:{cid}")
        with eng.begin() as c:
            c.execute(delete(job).where(job.c.dedup_key.like(f"busy:{cid}:%")))


def _res(n_events: int, width: int) -> DecodeResult:
    evs = [ChangeEvent(kind=ChangeKind.INSERT, txid="t", position=Position(engine="fake", text=f"1:{i}"),
                       table=TableRef(schema="S", name="T"), after={"ID": i, "V": "x" * width}) for i in range(n_events)]
    return DecodeResult(events=evs, quarantined=[])


def test_decoded_cache_memory_is_bounded_in_bytes_lru() -> None:
    """HC-22: the in-memory decoded cache never exceeds ALELT_DECODED_CACHE_MEM_MB; least recently used goes first;
    a result larger than the whole budget is not kept."""
    from alelt_hub.replay import DecodedCache, _res_bytes

    one = _res(20, 200)
    size = _res_bytes(one)
    dc = DecodedCache("c", SimpleNamespace(), "fake", mem_bytes=int(size * 3.5))
    for k in ("a", "b", "c"):
        dc._remember(k, _res(20, 200))
    dc._mem.move_to_end("a")                       # 'a' recently used
    dc._remember("d", _res(20, 200))
    assert dc._n <= dc.mem_bytes and list(dc._mem) == ["c", "a", "d"]
    dc._remember("huge", _res(200, 200))           # > budget: not cached, nothing evicted
    assert "huge" not in dc._mem and list(dc._mem) == ["c", "a", "d"]
    assert dc._n == sum(n for _, n in dc._mem.values())


def test_data_path_job_is_never_abandoned_and_retry_resets(client: Any) -> None:
    """XC-10: after max_attempts a decode job stays QUEUED (hourly retry), a non-data job ends FAILED; retry() runs it
    now with a fresh attempt budget."""
    from alelt_hub import jobs
    from alelt_hub.db import job

    eng = _engine()
    with eng.begin() as c:
        d = c.execute(insert(job).values(kind="decode", payload={}, status="RUNNING", locked_by="w")
                      .returning(job.c.id)).scalar_one()
        m = c.execute(insert(job).values(kind="maintain", payload={}, status="RUNNING", locked_by="w")
                      .returning(job.c.id)).scalar_one()
    assert jobs.fail(eng, d, "boom", 50, worker_id="w") and jobs.fail(eng, m, "boom", 50, worker_id="w")
    with eng.connect() as c:
        rows = {r[0]: r for r in c.execute(select(job.c.id, job.c.status, job.c.run_after, job.c.error)
                                           .where(job.c.id.in_([d, m])))}
    assert rows[d][1] == "QUEUED" and "stuck" in rows[d][3]
    ra = rows[d][2] if rows[d][2].tzinfo else rows[d][2].replace(tzinfo=utcnow().tzinfo)
    assert ra > utcnow() + timedelta(minutes=50)
    assert rows[m][1] == "FAILED"
    assert jobs.retry(eng, m)
    with eng.connect() as c:
        st, att = c.execute(select(job.c.status, job.c.attempts).where(job.c.id == m)).one()
    assert st == "QUEUED" and att == 0
    with eng.begin() as c:
        c.execute(delete(job).where(job.c.id.in_([d, m])))


def test_source_connector_failure_is_retried_not_cached_forever(monkeypatch: Any) -> None:
    """HC-31: a failed connector load returns None, is retried after SOURCE_RETRY_S; a success is cached."""
    from alelt_hub import store

    calls = {"n": 0}

    class Ok:
        pass

    def fake_load(ref: str) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ImportError("driver not installed yet")
        return Ok

    monkeypatch.setattr(store, "load", fake_load)
    monkeypatch.setattr(store, "get_settings", lambda: SimpleNamespace(sources={"x": "mod:Ok"}))
    store.source_connector.cache_clear()
    try:
        assert store.source_connector("x") is None
        assert store.source_connector("x") is None and calls["n"] == 1      # within the retry window: no reload
        store._SRC_FAIL["x"] -= store.SOURCE_RETRY_S + 1
        got = store.source_connector("x")
        assert isinstance(got, Ok) and store.source_connector("x") is got and calls["n"] == 2
    finally:
        store.source_connector.cache_clear()


def test_unacked_command_is_redelivered_then_expired(client: Any, monkeypatch: Any) -> None:
    """HC-12: a SENT command whose delivery was lost is re-delivered after ALELT_COMMAND_REDELIVER_S, at most
    ALELT_COMMAND_MAX_DELIVERIES times, then EXPIRED (audited); an acknowledged command is never re-sent."""
    from alelt_hub import store
    from alelt_hub.db import agent, agent_command

    eng = _engine()
    aid = "redeliver-" + uuid.uuid4().hex[:6]
    with eng.begin() as c:
        _ensure_conn(c, CONN)
        c.execute(insert(agent).values(id=aid, connection_id=CONN))
    from alelt_common.models import CommandName

    with eng.begin() as c:
        cid = uuid.uuid4()
        c.execute(insert(agent_command).values(id=cid, agent_id=aid, name=CommandName.PAUSE.value, args={},
                                               issued_by="test"))
        ok_id = uuid.uuid4()
        c.execute(insert(agent_command).values(id=ok_id, agent_id=aid, name=CommandName.RESUME.value, args={},
                                               issued_by="test"))
    monkeypatch.setattr(store, "COMMAND_REDELIVER_S", -1.0)
    monkeypatch.setattr(store, "COMMAND_MAX_DELIVERIES", 2)
    with eng.begin() as c:
        first = [x.name.value for x in store.pending_commands(c, aid)]
    assert sorted(first) == ["PAUSE", "RESUME"]
    with eng.begin() as c:
        store.ack_command(c, aid, ok_id, True, None)
    with eng.begin() as c:
        again = [x.command_id for x in store.pending_commands(c, aid)]
    assert again == [cid]                                   # lost delivery re-sent, acked one not
    with eng.begin() as c:
        assert store.pending_commands(c, aid) == []         # 2 deliveries used: expired, not re-sent
    with eng.connect() as c:
        st = dict(c.execute(select(agent_command.c.id, agent_command.c.status).where(agent_command.c.agent_id == aid)).all())
    assert st[cid] == "EXPIRED" and st[ok_id] == "ACKED"
    with eng.begin() as c:
        c.execute(delete(agent_command).where(agent_command.c.agent_id == aid))
        c.execute(delete(agent).where(agent.c.id == aid))


def test_idle_caught_up_source_has_no_lag(client: Any) -> None:
    """HC-16: a caught-up source with no commits for hours is not late (lag 0, no LAG_ABOVE_TARGET); waiting batches
    or a dead source give the real age."""
    from alelt_hub.db import agent, batch
    from alelt_hub.lag import effective_lag_s

    eng = _engine()
    cid = "lagconn-" + uuid.uuid4().hex[:6]
    old = utcnow() - timedelta(hours=5)
    with eng.begin() as c:
        _ensure_conn(c, cid)
        c.execute(insert(agent).values(id=f"{cid}-a", connection_id=cid, status="CAPTURING", last_heartbeat=utcnow()))
        assert effective_lag_s(c, cid, old) == 0.0                       # idle, alive, nothing waiting
        bid = _batch(c, cid, "RECEIVED", 0.01)                            # ~15 min old batch waiting
        lag = effective_lag_s(c, cid, old)
        assert lag is not None and lag >= 5 * 3600 - 5                  # waiting work: the real commit age
        c.execute(delete(batch).where(batch.c.id == bid))
        c.execute(agent.update().where(agent.c.id == f"{cid}-a").values(last_heartbeat=utcnow() - timedelta(hours=1)))
        lag = effective_lag_s(c, cid, old)
        assert lag is not None and lag >= 5 * 3600 - 5                  # source silent: not "idle", report age
        c.execute(delete(agent).where(agent.c.connection_id == cid))


def test_full_loader_buffer_is_flushed_before_feeding() -> None:
    """HC-18: a full loader buffer is flushed BEFORE the next batch is fed to the assembler (no Backpressure after
    asm.feed, which forced a rebuild from restart_seq)."""
    from alelt_hub.pipeline import StreamPipeline

    order: list[str] = []

    class Stop(Exception):
        pass

    def vid(b: Any) -> int:
        order.append("decode")
        raise Stop

    fake = SimpleNamespace(loader=SimpleNamespace(buf=SimpleNamespace(rows=10), max_buffer_rows=10),
                           flush=lambda: order.append("flush"), catalog_vid_for=vid)
    try:
        StreamPipeline._process(fake, {"status": "RECEIVED"})  # type: ignore[arg-type]
    except Stop:
        pass
    assert order == ["flush", "decode"]
    order.clear()
    fake.loader.buf.rows = 3
    try:
        StreamPipeline._process(fake, {"status": "RECEIVED"})  # type: ignore[arg-type]
    except Stop:
        pass
    assert order == ["decode"]


def test_replay_reuses_the_catalog_version_of_the_first_decode() -> None:
    """XC-22: once decoded, a batch keeps its catalog version (a later catalog with an earlier valid_from does not
    change how replays decode it)."""
    from alelt_hub.pipeline import StreamPipeline

    fake = SimpleNamespace(engine=None)      # no DB access when the version is recorded
    assert StreamPipeline.catalog_vid_for(fake, {"decoded_catalog_vid": 7, "from_lri": "1:0"}) == 7  # type: ignore[arg-type]


def test_process_pending_refreshes_the_held_set_for_every_caller(monkeypatch: Any) -> None:
    """XC-23: holds placed by another worker are seen by reprocess / snapshot paths too (the refresh lives in
    process_pending, not only in the decode job); the set object shared with the loader is updated in place."""
    from alelt_hub import holds, integrity
    from alelt_hub.pipeline import StreamPipeline

    class Stop(Exception):
        pass

    monkeypatch.setattr(integrity, "blocks_replay", lambda *a, **k: False)
    from alelt_hub import completion, lifecycle

    monkeypatch.setattr(holds, "held_tables", lambda engine, cid: {"S.NEW"})
    monkeypatch.setattr(lifecycle, "unapplied", lambda engine, cid: {"S.LIFECYCLE_NEW"})   # ADR-012: NEW tables
    monkeypatch.setattr(completion, "frozen", lambda p: {"S.NEW"})
    shared = {"S.OLD"}

    def mirror() -> Any:
        raise Stop

    fake = SimpleNamespace(engine=None, cid="c", held=shared, _mirror=mirror, frozen=set(),
                           loader=SimpleNamespace(_ddl_fail={}))
    fake.refresh_held = lambda: StreamPipeline.refresh_held(fake)  # type: ignore[arg-type]
    try:
        StreamPipeline.process_pending(fake)  # type: ignore[arg-type]
    except Stop:
        pass
    assert shared == {"S.NEW", "S.LIFECYCLE_NEW"} and fake.held is shared and fake.frozen == {"S.NEW"}


def test_reposition_compares_positions_in_engine_order(monkeypatch: Any) -> None:
    """XC-17: an operator's START_CAPTURE text and the agent's normalised X-ALELT-Reposition text match by value (case,
    zero padding), not by string; garbage never matches."""
    from alelt_hub import gateway, store
    from alelt_hub.testing import FakeSource

    monkeypatch.setattr(store, "source_connector", lambda engine: FakeSource())
    assert gateway._same_pos("fake", "1:00000000000a0000", "1:A0000")
    assert not gateway._same_pos("fake", "1:00000000000a0000", "1:A0001")
    assert not gateway._same_pos("fake", "not-a-pos", "1:A0000")


def test_start_capture_position_is_normalised_or_refused(client: Any) -> None:
    """XC-17: the console stores START_CAPTURE.from_pos as the engine parses it; an unparsable one is a 400."""
    from alelt_hub.testing import FakeAgent

    from .conftest import TOKEN, role

    a = FakeAgent(client, TOKEN, agent_id="xc17-agent", connection_id="xc17conn")
    a.register()
    url = f"/api/v1/agents/{a.agent_id}/commands"
    r = client.post(url, headers=role("PlatformAdmin"), json={"name": "START_CAPTURE", "reason": "re-chain",
                                                                "args": {"from_pos": "  1:0a "}})
    assert r.status_code == 200, r.text
    hist = client.get(url, headers=role("PlatformAdmin")).json()
    assert hist[0]["args"]["from_pos"] == "1:0a"


def test_edge_snapshot_start_uses_agent_position_before_the_first_batch(client: Any) -> None:
    """XC-13: an idle edge source (catalog posted, no batch yet) gets its snapshot start position from the capturing
    agent's heartbeat, not 'no start position' forever; a stale heartbeat does not count."""
    from alelt_hub.db import agent
    from alelt_hub.snapshots import SnapshotService

    eng = _engine()
    cid = "xc13-" + uuid.uuid4().hex[:6]
    with eng.begin() as c:
        _ensure_conn(c, cid)
        c.execute(insert(agent).values(id=f"{cid}-a", connection_id=cid, status="CAPTURING",
                                       durable_pos="1:00A0", last_lri="1:00B0", last_heartbeat=utcnow()))
    svc = SnapshotService.__new__(SnapshotService)
    svc.engine, svc.cid, svc.p = eng, cid, SimpleNamespace(engine_name="fake")
    assert svc._hub_position().text == "1:00A0"  # type: ignore[union-attr]
    with eng.begin() as c:
        c.execute(agent.update().where(agent.c.id == f"{cid}-a").values(last_heartbeat=utcnow() - timedelta(hours=2)))
    assert svc._hub_position() is None
    with eng.begin() as c:
        c.execute(delete(agent).where(agent.c.id == f"{cid}-a"))


def test_backlog_gauges_and_bounded_quarantine_label(client: Any) -> None:
    """XC-27: oldest unloaded batch age / failed + stuck jobs / checkpoint progress are exported; quarantine reason
    labels never carry positions or ids."""
    from alelt_hub.app import _collect_backlog
    from alelt_hub.db import batch, job
    from alelt_hub.metrics import Registry
    from alelt_hub.pipeline import reason_label

    eng = _engine()
    cid = "xc27-" + uuid.uuid4().hex[:6]
    with eng.begin() as c:
        _ensure_conn(c, cid)
        bid = _batch(c, cid, "RECEIVED", 1 / 24)                 # 1 h old, not applied
        jid = c.execute(insert(job).values(kind="verify", payload={}, status="FAILED").returning(job.c.id)).scalar_one()
    reg = Registry()
    with eng.connect() as c:
        _collect_backlog(reg, c)
    text = reg.render() if hasattr(reg, "render") else str(reg.__dict__)
    assert "alelt_batch_oldest_unloaded_age_seconds" in text and cid in text and "alelt_jobs_failed" in text
    with eng.begin() as c:
        c.execute(delete(batch).where(batch.c.id == bid))
        c.execute(delete(job).where(job.c.id == jid))
    a = reason_label("row 17 at LRI 0000000000A1B2C3: unknown table id 4711 (tbspace 3)")
    b = reason_label("row 99 at LRI 00000000FFEE0011: unknown table id 12 (tbspace 9)")
    assert a == b and len(a) <= 40 and not any(ch.isdigit() for ch in a)


def test_verify_float_canon_sees_13th_digit_and_remote_verify_has_a_consistency_point() -> None:
    """HC-15: two DOUBLEs differing in the 13th significant digit no longer compare equal; the remote verify takes the
    source position before its read and reports `settled` (an unsettled MISMATCH is retried by consume_verify)."""
    import inspect

    from alelt_hub import ops
    from alelt_verify.defaults import canon

    assert canon(1.0000000000001) != canon(1.0000000000002)
    assert canon(0.1) == canon(float("0.1")) and canon(2.0) == "2"
    src = inspect.getsource(ops.verify_table)
    assert "current_position()" in src and 'summary["settled"]' in src


def test_delete_of_unknown_key_reaches_the_destination_with_every_available_column(tmp_path: Any) -> None:
    """XC-07 (hub side of the one rule in DESIGN.md 5.2): a DELETE whose key the batch never inserted is passed to
    the destination as a DELETE carrying the whole available before image (never dropped, never reduced to the
    key); the destination then writes a tombstone with those columns (or only sets the deleted flag when it holds
    the row). The per-destination conformance cases live with the destination kit."""
    from alelt_common.models import CanonicalType, ChangeSet, ColumnInfo, TableInfo
    from alelt_common.spi import DestinationConnector
    from alelt_loader import Loader, load_policy

    k = TableRef(schema="S", name="KEYED")
    info = TableInfo(table=k, columns=[ColumnInfo(name="ID", ordinal=0, engine_type="int", key_seq=1,
                                                  canonical_type=CanonicalType.INT),
                                       ColumnInfo(name="NAME", ordinal=1, engine_type="text",
                                                  canonical_type=CanonicalType.STRING)])
    got: list[Any] = []

    class Rec(DestinationConnector):
        engine = "rec"

        def bootstrap(self) -> None: ...
        def ensure_table(self, table: Any, dest_name: str, golden_ddl: Any) -> None: ...
        def add_column(self, dest_name: str, col: Any) -> None: ...
        def read_checkpoint(self, stream: str) -> Any: return None
        def load_snapshot(self, dest_name: str, table: Any, rows: Any) -> int: return 0

        def apply(self, stream: str, changesets: Any, to_pos: Any, batch_id: str, table_map: Any, tables: Any,
                  **kw: Any) -> list[Any]:
            got.extend(e for s in changesets for e in s.events)
            return []

    cfg = tmp_path / "t.yaml"
    cfg.write_text("connection_id: c\noutput_profile: alelt\nlanding_schema: L\nschemas:\n  S:\n    enabled: true\n",
                   encoding="utf-8")

    def cmp(a: Position, b: Position) -> int:
        return (int(a.text) > int(b.text)) - (int(a.text) < int(b.text))

    ld = Loader(Rec(), load_policy("c", cfg), "c", cmp, max_latency_s=0)
    ld.add([ChangeSet(txid="t", commit_position=Position(engine="x", text="2"), commit_ts=None, events=[
        ChangeEvent(position=Position(engine="x", text="1"), txid="t", kind=ChangeKind.DELETE, table=k,
                    before={"ID": 7, "NAME": "gone"}),
        ChangeEvent(position=Position(engine="x", text="2"), txid="t", kind=ChangeKind.DELETE, table=k,
                    before={"ID": 8})])], Position(engine="x", text="2"))
    ld.flush({k: info}, "b")
    dels = sorted(((e.before or {}).get("ID"), e.before) for e in got if e.kind is ChangeKind.DELETE)
    assert [d[0] for d in dels] == [7, 8]
    assert dels[0][1]["NAME"] == "gone" and "NAME" not in dels[1][1]    # key-only stays key-only, never invented


def test_keyless_snapshot_row_id_equals_cdc_row_id() -> None:
    """XC-21: the same keyless row read by a snapshot (text "4.70", ISO text with +02:00) and by CDC (Decimal 4.7,
    aware UTC datetime) gets ONE surrogate id."""
    from datetime import datetime, timezone
    from decimal import Decimal

    from alelt_common.canonical_jsonl import canonical_snapshot_row
    from alelt_loader import surrogate_id

    types = {"AMT": "DECIMAL", "N": "INT", "AT": "INSTANT", "D": "LOCALDATE", "S": "STRING"}
    snap = canonical_snapshot_row({"AMT": "4.70", "N": "12", "AT": "2026-01-02T12:00:00+02:00", "D": "2026-01-02",
                                   "S": " x"}, types)
    cdc = {"AMT": Decimal("4.7"), "N": 12, "AT": datetime(2026, 1, 2, 10, 0, tzinfo=timezone.utc),
           "D": datetime(2026, 1, 2).date(), "S": " x"}
    cols = ["AMT", "N", "AT", "D", "S"]
    assert surrogate_id(snap, cols) == surrogate_id(cdc, cols)
    odd = canonical_snapshot_row({"AMT": "n/a", "N": "1.5"}, types)      # ambiguous: unchanged, never guessed
    assert odd == {"AMT": "n/a", "N": "1.5"}
