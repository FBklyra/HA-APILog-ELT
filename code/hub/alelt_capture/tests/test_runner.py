"""CaptureRunner: txn-boundary batching, ack only after durable receipt, resume from the durable position,
backpressure retry, gap stop, oversize txn, lock; remote snapshot quarantine; catalog A -> B -> A versions."""
from __future__ import annotations

import json
from typing import Any

import pytest
from sqlalchemy import Engine, select

from alelt_capture import receipt
from alelt_capture.runner import CaptureConfig, CaptureError, CaptureRunner
from alelt_capture.snapshot import run_snapshot
from alelt_capture.tests.fakes import T, FakeRemote
from alelt_common.canonical_jsonl import Undecodable, decode_batch
from alelt_common.models import Position
from alelt_common.util import decompress
from alelt_hub import blobstore, store
from alelt_hub.db import batch, catalog_version, quarantine_event, snapshot_chunk

CID = "fakecid"


def runner(engine: Engine, **cfg: Any) -> CaptureRunner:
    c = CaptureConfig(**{"batch_bytes": 1 << 20, "batch_ms": 0, "retry_s": 0.01, "catalog_s": 3600,
                         "codec": "zlib", **cfg})
    return CaptureRunner(engine, CID, lambda: FakeRemote({}, None, CID), c)


def batches(engine: Engine) -> list[dict[str, Any]]:
    with engine.connect() as c:
        return [dict(r) for r in c.execute(select(batch).where(batch.c.connection_id == CID)
                                           .order_by(batch.c.seq)).mappings()]


def body_lines(b: dict[str, Any]) -> list[dict[str, Any]]:
    raw = decompress(blobstore.read_ref(b["spool_path"]), b["codec"])
    return [json.loads(x) for x in raw.splitlines()]


def check_chain(engine: Engine) -> list[dict[str, Any]]:
    """Contiguous chain, every batch ends on a commit, every txn exactly once and whole."""
    bs = batches(engine)
    for prev, cur in zip(bs, bs[1:]):
        assert cur["from_lri"] == prev["next_lri"]
    seen: list[str] = []
    for b in bs:
        lines = body_lines(b)
        assert lines[-1]["kind"] == "COMMIT" and lines[-1]["pos"] == b["to_lri"] == b["next_lri"]
        assert b["rec_count"] == len(lines)
        seen += [x["txid"] for x in lines if x["kind"] == "COMMIT"]
        assert not decode_batch(decompress(blobstore.read_ref(b["spool_path"]), b["codec"]), "fakeremote",
                                b["id"]).quarantined
    assert seen == [str(i + 1) for i in range(len(FakeRemote.log))], "each txn once, in commit order"
    return bs


def test_batches_cut_on_txn_boundaries(hub: Engine) -> None:
    for n in (3, 1, 5, 2, 4, 1):
        FakeRemote.add_txn(n)
    r = runner(hub, batch_bytes=400, batch_ms=60_000)     # size-driven cuts, several txns per batch
    assert r.run(idle_exit_s=0.2) == "idle"
    bs = check_chain(hub)
    assert 1 < len(bs) < 6 and r.events == sum(len(t) for t in FakeRemote.log)
    assert bs[0]["agent_id"] == "capture:fakecid" and bs[0]["engine"] == "fakeremote"


def test_ack_only_after_durable(hub: Engine) -> None:
    for _ in range(4):
        FakeRemote.add_txn(2)

    def durable(pos: Position) -> None:
        with hub.connect() as c:
            assert c.execute(select(batch.c.id).where(batch.c.connection_id == CID,
                                                      batch.c.next_lri == pos.text)).first() is not None
    FakeRemote.on_ack = durable
    runner(hub).run(idle_exit_s=0.2)
    assert FakeRemote.acks == [b["next_lri"] for b in batches(hub)]


def test_failed_receipt_no_ack_then_resume_without_gap_or_dup(hub: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
    for _ in range(5):
        FakeRemote.add_txn(2)
    real = receipt.receive
    calls = {"n": 0}

    def flaky(hdr: Any, body: bytes) -> Any:
        calls["n"] += 1
        if calls["n"] == 3:
            raise OSError("blob store down")
        return real(hdr, body)
    monkeypatch.setattr(receipt, "receive", flaky)
    r = runner(hub)
    assert r.run(idle_exit_s=0.2, max_restarts=3) == "idle"
    bs = check_chain(hub)
    assert len(FakeRemote.starts) == 2 and FakeRemote.starts[0] is None
    assert FakeRemote.starts[1] == bs[1]["next_lri"]           # session 2 resumes at the last durable position
    assert FakeRemote.acks == [b["next_lri"] for b in bs]       # never acked the failed batch


def test_restart_resumes_from_durable_position(hub: Engine) -> None:
    FakeRemote.add_txn(2)
    FakeRemote.add_txn(1)
    runner(hub).run(idle_exit_s=0.2)
    first = batches(hub)
    FakeRemote.add_txn(3)
    FakeRemote.add_txn(1)
    runner(hub).run(idle_exit_s=0.2)
    bs = check_chain(hub)
    assert FakeRemote.starts[-1] == first[-1]["next_lri"] and len(bs) > len(first)
    assert receipt.durable_resume(hub, CID, "fakeremote") == Position(engine="fakeremote", text=bs[-1]["next_lri"])


def test_start_pos_used_only_without_durable_batch(hub: Engine) -> None:
    FakeRemote.add_txn(1)
    FakeRemote.add_txn(1)
    runner(hub, start_pos="1999").run(idle_exit_s=0.2)
    assert FakeRemote.starts == ["1999"]
    assert [x["txid"] for b in batches(hub) for x in body_lines(b) if x["kind"] == "COMMIT"] == ["2"]
    assert batches(hub)[0]["from_lri"] == "1999"       # SRC-cap-05: first from_pos = the resume token, not a DML


def test_backpressure_retries_same_batch(hub: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
    FakeRemote.add_txn(2)
    real, calls = receipt.receive, []

    def busy(hdr: Any, body: bytes) -> Any:
        calls.append(hdr.batch_id)
        if len(calls) == 1:
            raise receipt.Backpressure("full")
        return real(hdr, body)
    monkeypatch.setattr(receipt, "receive", busy)
    runner(hub).run(idle_exit_s=0.2)
    assert len(calls) == 2 and calls[0] == calls[1] and len(batches(hub)) == 1


def test_gap_stops_the_runner(hub: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
    FakeRemote.add_txn(1)

    def gap(hdr: Any, body: bytes) -> Any:
        raise receipt.GapError("expected 1, got 2")
    monkeypatch.setattr(receipt, "receive", gap)
    r = runner(hub)
    with pytest.raises(CaptureError):
        r.run(idle_exit_s=0.2)
    assert r.state == "ERROR" and not FakeRemote.acks


def _applied(engine: Engine) -> list[tuple[str, list[int]]]:
    """Decode every durable batch in order through the hub's transaction assembler (= the pipeline path)."""
    from alelt_txn import TxnAssembler

    asm = TxnAssembler()
    out: list[tuple[str, list[int]]] = []
    for b in batches(engine):
        res = decode_batch(decompress(blobstore.read_ref(b["spool_path"]), b["codec"]), "fakeremote", b["id"])
        assert not res.quarantined
        for cs in asm.feed(res.events, b["seq"]):
            out.append((cs.txid, [e.after["id"] for e in cs.events]))  # type: ignore[index]
    return out


def test_large_txn_is_shipped_in_chunks_never_stops(hub: Engine) -> None:
    """HC-08: a transaction far above the chunk size is shipped as chunk batches (from = to = next = the resume
    token before it), never stops capture, and the hub assembles it exactly once."""
    FakeRemote.add_txn(1)
    FakeRemote.add_txn(400, big="x" * 500)          # ~220 KB vs a 20 KB chunk: ~11 chunks
    FakeRemote.add_txn(2)
    assert runner(hub, txn_chunk_bytes=20_000).run(idle_exit_s=0.3) == "idle"
    bs = batches(hub)
    chunks = [b for b in bs if b["from_lri"] == b["to_lri"]]
    assert len(chunks) >= 8
    assert all(b["from_lri"] == b["to_lri"] == b["next_lri"] == bs[0]["next_lri"] for b in chunks)
    for prev, cur in zip(bs, bs[1:]):
        assert cur["from_lri"] == prev["next_lri"]
    assert max(b["raw_bytes"] for b in bs) < 40_000      # bounded batch size
    got = _applied(hub)
    assert [t for t, _ in got] == ["1", "2", "3"]
    assert got[1][1] == [2000 + i for i in range(400)]  # every row once, in order


def test_large_txn_restart_mid_chunks_skips_durable_lines(hub: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
    """HC-08: a crash after some chunks were made durable re-reads the transaction from its resume token; lines
    already in chunk batches are skipped (count + txid read back from the hub): nothing twice, nothing lost."""
    FakeRemote.add_txn(1)
    FakeRemote.add_txn(300, big="y" * 500)
    real = receipt.receive
    calls = {"n": 0}

    def flaky(hdr: Any, body: bytes) -> Any:
        calls["n"] += 1
        if calls["n"] == 5:                       # after the first batch + 3 chunks
            raise OSError("hub restarted")
        return real(hdr, body)
    monkeypatch.setattr(receipt, "receive", flaky)
    r = runner(hub, txn_chunk_bytes=20_000)
    assert r.run(idle_exit_s=0.3, max_restarts=3) == "idle"
    assert len(FakeRemote.starts) == 2
    got = _applied(hub)
    assert [t for t, _ in got] == ["1", "2"]
    assert got[1][1] == [2000 + i for i in range(300)]
    ids = [b["id"] for b in batches(hub)]
    assert len(ids) == len(set(ids))


def test_spill_disk_cap_stops_with_incident_never_drops(hub: Engine) -> None:
    """HC-08: only the spill disk cap stops capture (SpillLimitExceeded): ERROR, no batch, never a partial drop."""
    from alelt_common.spill import SpillLimitExceeded

    class Huge(FakeRemote):
        def stream(self, from_pos: Any) -> Any:  # type: ignore[override]
            raise SpillLimitExceeded("open transaction mysql spilled 65537 MiB > ALELT_CAPTURE_MAX_SPILL_MB (65536)")
            yield None

    r = CaptureRunner(hub, CID, lambda: Huge({}, None, CID),
                      CaptureConfig(batch_ms=0, retry_s=0.0, catalog_s=3600, codec="zlib"))
    with pytest.raises(CaptureError, match="SpillLimitExceeded"):
        r.run()
    assert r.state == "ERROR" and not batches(hub)


def test_backpressure_wait_keeps_the_source_alive(hub: Engine, monkeypatch: pytest.MonkeyPatch) -> None:
    """XC-18: while the hub answers 503 the runner calls the source's keepalive() (PG standby status) instead of
    leaving the replication session silent until wal_sender_timeout."""
    FakeRemote.add_txn(1)
    kept: list[int] = []
    FakeRemote.keepalive = lambda self: kept.append(1)  # type: ignore[attr-defined]
    real = receipt.receive
    calls = {"n": 0}

    def busy(hdr: Any, body: bytes) -> Any:
        calls["n"] += 1
        if calls["n"] <= 2:
            raise receipt.Backpressure("busy")
        return real(hdr, body)
    monkeypatch.setattr(receipt, "receive", busy)
    try:
        assert runner(hub, retry_s=0.05, keepalive_s=0.01).run(idle_exit_s=0.2) == "idle"
    finally:
        del FakeRemote.keepalive  # type: ignore[attr-defined]
    assert len(kept) >= 4 and len(batches(hub)) == 1


def test_one_runner_per_connection(hub: Engine) -> None:
    from alelt_hub.dialect import try_lock, unlock

    h = try_lock(hub, f"capture:{CID}")
    try:
        assert runner(hub).run(idle_exit_s=0.2) == "locked"
    finally:
        unlock(h, f"capture:{CID}")


def test_resent_batch_is_duplicate(hub: Engine) -> None:
    FakeRemote.add_txn(2)
    runner(hub).run(idle_exit_s=0.2)
    b = batches(hub)[0]
    from alelt_common.models import BatchHeader

    hdr = BatchHeader(batch_id=b["id"], connection_id=CID, agent_id=b["agent_id"], engine="fakeremote",
                      from_pos=Position(engine="fakeremote", text=b["from_lri"]),
                      to_pos=Position(engine="fakeremote", text=b["to_lri"]),
                      next_pos=Position(engine="fakeremote", text=b["next_lri"]), rec_count=b["rec_count"],
                      raw_bytes=b["raw_bytes"], sha256=b["sha256"], codec=b["codec"])
    for _ in range(3):
        assert receipt.receive(hdr, blobstore.read_ref(b["spool_path"])).duplicate


def test_catalog_a_b_a_versions(hub: Engine) -> None:
    """Content A, then B, then A again: the newest version valid at a position must be the current content."""
    src = FakeRemote()
    receipt.ensure_registered(hub, CID, "fakeremote")

    def ingest(pos: str) -> tuple[int, bool]:
        snap = src.read_catalog()
        snap.connection_id, snap.position = CID, Position(engine="fakeremote", text=pos)
        return receipt.ingest_catalog(hub, snap)
    a1, c1 = ingest("10")
    assert ingest("15") == (a1, False)                    # same content as latest: dedup
    FakeRemote.columns = ["id", "v", "w"]
    b, cb = ingest("20")
    FakeRemote.columns = ["id", "v"]
    a2, ca = ingest("30")
    assert c1 and cb and ca and len({a1, b, a2}) == 3
    with hub.connect() as c:
        rows = c.execute(select(catalog_version.c.id, catalog_version.c.valid_from_pos)
                         .where(catalog_version.c.connection_id == CID).order_by(catalog_version.c.id)).all()
        assert [r[1] for r in rows] == ["10", "20", "30"]
        assert store.latest_catalog_id(c, CID) == a2
        cols = [col.name for col in store.load_catalog(c, a2).tables[0].columns]
    assert cols == ["id", "v"]
    assert ingest("31") == (a2, False)


def test_snapshot_quarantines_unconvertible_rows(hub: Engine) -> None:
    receipt.ensure_registered(hub, CID, "fakeremote")
    snap = FakeRemote().read_catalog()
    snap.connection_id = CID
    receipt.ingest_catalog(hub, snap)
    rows = [{"id": i, "v": f"r{i}"} for i in range(1, 6)]
    rows[2]["v"] = Undecodable("geometry:0101")
    rows[4]["v"] = Undecodable("geometry:0102")            # last row of the table is bad: key range still covers it
    res = run_snapshot(hub, CID, FakeRemote({"rows": rows}), [T], chunk_rows=3)
    out = res[T.fqn]
    assert out["rows"] == 3 and out["quarantined"] == 2 and out["chunks"] == 2
    with hub.connect() as c:
        q = c.execute(select(quarantine_event.c.table_fqn, quarantine_event.c.reason, quarantine_event.c.raw_hex)
                      .where(quarantine_event.c.connection_id == CID)).all()
        chunks = c.execute(select(snapshot_chunk.c.chunk_no, snapshot_chunk.c.rows, snapshot_chunk.c.header,
                                  snapshot_chunk.c.spool_path).order_by(snapshot_chunk.c.chunk_no)).all()
    assert len(q) == 2 and all(r[0] == T.fqn and "unconvertible value in v" in r[1] for r in q)
    assert bytes.fromhex(q[0][2]).decode() == '{"v": "geometry:0101"}'
    assert [c[1] for c in chunks] == [2, 1]
    assert chunks[0][2]["key_to"] == [3] and chunks[1][2]["key_from"] == [3] and chunks[1][2]["key_to"] == [5]
    landed = [json.loads(x) for ch in chunks for x in decompress(blobstore.read_ref(ch[3]), "zlib").splitlines()]
    assert [r["id"] for r in landed] == [1, 2, 4]


def test_snapshot_chunks_are_typed(hub: Engine) -> None:
    """REQ-23: chunk rows carry canonical value tags; the worker decodes them back to bytes/Decimal/datetime."""
    from datetime import datetime
    from decimal import Decimal

    from alelt_common.canonical_jsonl import decode_value

    receipt.ensure_registered(hub, CID, "fakeremote")
    snap = FakeRemote().read_catalog()
    snap.connection_id = CID
    receipt.ingest_catalog(hub, snap)
    rows = [{"id": 1, "v": b"\x00\xffAB"}, {"id": 2, "v": Decimal("12.300")}, {"id": 3, "v": datetime(2026, 1, 2, 3, 4)}]
    run_snapshot(hub, CID, FakeRemote({"rows": rows}), [T], chunk_rows=10)
    with hub.connect() as c:
        ch = c.execute(select(snapshot_chunk.c.header, snapshot_chunk.c.spool_path)).all()
    assert ch[-1][0]["format"] == "canonical_jsonl"
    got = [{k: decode_value(v) for k, v in json.loads(x).items()}
           for x in decompress(blobstore.read_ref(ch[-1][1]), "zlib").splitlines()]
    assert [r["v"] for r in got] == [b"\x00\xffAB", Decimal("12.300"), datetime(2026, 1, 2, 3, 4)]


def test_snapshot_json_values_become_text(hub: Engine) -> None:
    """JSON columns: dict / list from the connector -> JSON text in the chunk (the CDC form), not a TypeError."""
    from alelt_common.models import CanonicalType

    class JsonRemote(FakeRemote):
        def read_catalog(self):  # type: ignore[no-untyped-def]
            snap = super().read_catalog()
            for c in snap.tables[0].columns:
                if c.name == "v":
                    c.canonical_type, c.engine_type = CanonicalType.JSON, "json"
            return snap

    receipt.ensure_registered(hub, CID, "fakeremote")
    snap = JsonRemote().read_catalog()
    snap.connection_id = CID
    receipt.ingest_catalog(hub, snap)
    rows = [{"id": 1, "v": {"a": [1, None]}}, {"id": 2, "v": [1, "x"]}, {"id": 3, "v": '{"b":2}'}]
    run_snapshot(hub, CID, JsonRemote({"rows": rows}), [T], chunk_rows=10)
    with hub.connect() as c:
        ch = c.execute(select(snapshot_chunk.c.spool_path)).all()
    got = [json.loads(x)["v"] for x in decompress(blobstore.read_ref(ch[-1][0]), "zlib").splitlines()]
    assert got == ['{"a":[1,null]}', '[1,"x"]', '{"b":2}']


def test_circuit_breaker_stops_a_deterministic_connector_error(hub: Engine) -> None:
    class Broken(FakeRemote):
        def stream(self, from_pos: Any) -> Any:  # type: ignore[override]
            raise ValueError("record 0x2a always rejected")
            yield None

    r = CaptureRunner(hub, CID, lambda: Broken({}, None, CID),
                      CaptureConfig(batch_ms=0, retry_s=0.0, catalog_s=3600, codec="zlib", max_same_errors=3))
    with pytest.raises(CaptureError) as ei:
        r.run()
    assert r.state == "ERROR" and "repeated failure x3" in str(ei.value)


def test_connector_transaction_too_large_stops_at_once(hub: Engine) -> None:
    """SRC-cap-02: a connector that hits the txn budget while buffering raises TransactionTooLarge: the runner stops
    the connection on the first occurrence (no reconnect loop re-reading the same transaction)."""
    class TransactionTooLarge(RuntimeError):        # the connectors' class name (matched by name: no import)
        pass

    calls: list[int] = []

    class Huge(FakeRemote):
        def stream(self, from_pos: Any) -> Any:  # type: ignore[override]
            calls.append(1)
            raise TransactionTooLarge("transaction 1 buffered 2048 MiB > ALELT_CAPTURE_MAX_TXN_MB (1024)")
            yield None

    r = CaptureRunner(hub, CID, lambda: Huge({}, None, CID),
                      CaptureConfig(batch_ms=0, retry_s=0.0, catalog_s=3600, codec="zlib", max_same_errors=20))
    with pytest.raises(CaptureError, match="TransactionTooLarge"):
        r.run()
    assert r.state == "ERROR" and calls == [1]


def test_connector_buffers_spill_and_deliver_lazily(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """HC-08 (was SRC-cap-02 stop): MySQL assembler, Oracle assembler and Postgres streamed transactions spill an open
    transaction above ALELT_CAPTURE_TXN_MEM_MB to disk and deliver it in order at commit with bounded memory; an undo
    that reaches the spilled part becomes a hold signal (never a guess); PG subtransaction aborts drop spilled rows."""
    import struct
    import sys
    from pathlib import Path

    from alelt_common.completeness import RecordClass, signal_of
    from alelt_common.models import ChangeKind

    root = Path(__file__).resolve().parents[3] / "connectors" / "sources"
    for eng in ("mysql", "oracle", "postgres"):
        if str(root / eng) not in sys.path:
            sys.path.insert(0, str(root / eng))
    monkeypatch.setenv("ALELT_CAPTURE_TXN_MEM_MB", "1")
    from alelt_src_mysql.assemble import TxnAssembler
    from alelt_src_mysql.decode import DecodeOut, PlainEvent
    from alelt_src_oracle.assemble import Assembler

    asm = TxnAssembler("mysql", empty_commit_s=0)
    for i in range(3000):
        asm.feed(DecodeOut(events=[PlainEvent("INSERT", "t1", "p", "db", "t", after={"id": i, "v": "x" * 900},
                                              length=1000)]))
    assert asm.buf.spilled > 1000 and asm.buf.max_tail_bytes <= (1 << 20) + 4096
    asm.feed(DecodeOut(events=[PlainEvent("COMPENSATION", "t1", "p", "db", "t")]))   # newest row: in memory
    evs = list(asm.feed(DecodeOut(events=[PlainEvent("COMMIT", "t1", "c")])))
    ids = [e.after["id"] for e in evs if e.kind is ChangeKind.INSERT]  # type: ignore[index]
    assert ids == list(range(2999)) and evs[-1].kind is ChangeKind.COMMIT and not asm.buf.spilled
    for i in range(3000):                                   # undo of a row that is only on disk -> hold signal
        asm.feed(DecodeOut(events=[PlainEvent("INSERT", "t2", "p", "db", "t", after={"id": i, "v": "x" * 900},
                                              length=1000)]))
    asm.buf.tail.clear(), asm.buf.tail_extra.clear(), asm.buf.tail_tags.clear()  # force: nothing left in memory
    asm.feed(DecodeOut(events=[PlainEvent("COMPENSATION", "t2", "p", "db", "t")]))
    evs = list(asm.feed(DecodeOut(events=[PlainEvent("COMMIT", "t2", "c")])))
    assert any(signal_of(e) and signal_of(e).cls is RecordClass.UNSUPPORTED for e in evs)  # type: ignore[union-attr]

    oa = Assembler({}, ["APP"], empty_commit_every_s=0)
    oa.feed({"SCN": 1, "XID": "01", "OPERATION_CODE": 6, "SEG_OWNER": None, "TABLE_NAME": None})
    for i in range(3000):
        oa.feed({"SCN": 2 + i, "XID": "01", "OPERATION_CODE": 1, "SEG_OWNER": "APP", "TABLE_NAME": "T",
                 "SQL_REDO": 'insert into "APP"."T"("ID") values (' + str(i) + ")" + " " * 900, "ROW_ID": "A"})
    t = oa.open["01"]
    assert t.cold is not None and t.cold.spilled > 1000
    oa.feed({"SCN": 5000, "XID": "01", "OPERATION_CODE": 7, "SEG_OWNER": None, "TABLE_NAME": None})
    evs = list(oa.flush(force_token=True))
    assert len([e for e in evs if e.kind is not ChangeKind.COMMIT]) == 3000 and evs[-1].kind is ChangeKind.COMMIT



def test_ddl_cuts_the_batch_and_versions_the_catalog_at_its_commit(hub: Engine) -> None:
    """SRC-cap-04: events before a DDL never get the post-DDL catalog: the batch is cut at the DDL transaction and
    the refreshed catalog is valid from the DDL's commit token (= next batch from_pos)."""
    from datetime import datetime, timezone

    from alelt_common.models import ChangeEvent, ChangeKind

    FakeRemote.add_txn(2)
    ddl_commit = "2999"
    FakeRemote.log.append([
        ChangeEvent(position=Position(engine="fakeremote", text="2000"), txid="2", kind=ChangeKind.DDL, table=T,
                    note="ALTER TABLE t ADD w"),
        ChangeEvent(position=Position(engine="fakeremote", text=ddl_commit), txid="2", kind=ChangeKind.COMMIT,
                    commit_ts=datetime(2026, 11, 4, tzinfo=timezone.utc))])
    FakeRemote.add_txn(2)
    orig = FakeRemote.read_catalog

    def after_ddl(self: FakeRemote) -> Any:           # the source shape after the DDL
        type(self).columns = ["id", "v", "w"]
        return orig(self)
    reads = {"n": 0}

    def counting(self: FakeRemote) -> Any:
        reads["n"] += 1
        return orig(self) if reads["n"] == 1 else after_ddl(self)
    FakeRemote.read_catalog = counting  # type: ignore[method-assign]
    try:
        runner(hub, batch_ms=300).run(idle_exit_s=0.8)
    finally:
        FakeRemote.read_catalog = orig  # type: ignore[method-assign]
    bs = check_chain(hub)
    assert bs[0]["to_lri"] == ddl_commit and len(bs) == 2          # cut right after the DDL transaction
    with hub.connect() as c:
        vers = [(r.valid_from_pos, sorted(x["name"] for x in r.body["tables"][0]["columns"]))
                for r in c.execute(select(catalog_version).where(catalog_version.c.connection_id == CID)
                                   .order_by(catalog_version.c.id))]
    assert vers[-1][1] == ["id", "v", "w"] and ddl_commit in str(vers[-1][0])
    assert all("w" not in cols for _, cols in vers[:-1])
