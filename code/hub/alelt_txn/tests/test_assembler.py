"""alelt_txn scenarios: interleaving, rollback, savepoint (compensation), spill, restart/replay (L3)."""
from __future__ import annotations

from alelt_common.models import ChangeEvent, ChangeKind, Position, TableRef
from alelt_txn import TxnAssembler

T = TableRef(schema="S", name="T")
U = TableRef(schema="S", name="U")


def e(n: int, txid: str, kind: str, table: TableRef | None = T, **after: object) -> ChangeEvent:
    return ChangeEvent(position=Position(engine="x", text=f"1:{n:016X}"), txid=txid, kind=ChangeKind(kind),
                       table=table if kind not in ("COMMIT", "ROLLBACK") else None, after=after or None)


def test_interleaved_commit_order() -> None:
    a = TxnAssembler()
    out = a.feed([e(1, "A", "INSERT", id=1), e(2, "B", "INSERT", id=2), e(3, "A", "UPDATE", id=1),
                  e(4, "B", "COMMIT"), e(5, "A", "COMMIT")])
    assert [cs.txid for cs in out] == ["B", "A"]
    assert [ev.position.text for ev in out[1].events] == ["1:0000000000000001", "1:0000000000000003"]
    assert out[0].commit_position.text == "1:0000000000000004"
    assert not a.open


def test_rollback_never_emitted() -> None:
    a = TxnAssembler()
    out = a.feed([e(1, "R", "INSERT", id=9), e(2, "R", "UPDATE", id=8),
                  e(3, "R", "COMPENSATION"), e(4, "R", "COMPENSATION"), e(5, "R", "ROLLBACK"),
                  e(6, "C", "INSERT", id=1), e(7, "C", "COMMIT")])
    assert [cs.txid for cs in out] == ["C"]
    assert a.stats.rolled_back == 1


def test_savepoint_rollback_undoes_latest() -> None:
    a = TxnAssembler()
    out = a.feed([e(1, "A", "INSERT", id=1), e(2, "A", "INSERT", table=U, id=2), e(3, "A", "INSERT", id=3),
                  e(4, "A", "COMPENSATION", table=T), e(5, "A", "COMMIT")])
    assert [ev.after for ev in out[0].events] == [{"id": 1}, {"id": 2}]


def test_spill_roundtrip(tmp_path) -> None:  # type: ignore[no-untyped-def]
    a = TxnAssembler(byte_budget=2_000, spill_dir=tmp_path)
    evs = [e(i, "BIG", "INSERT", id=i, pad="x" * 50) for i in range(1, 501)]
    a.feed(evs)
    assert a.stats.spills > 0 and list(tmp_path.iterdir())
    a.feed([e(600, "BIG", "COMPENSATION")])          # undo newest (in memory or spilled)
    out = a.feed([e(601, "BIG", "COMMIT")])
    assert len(out[0].events) == 499
    assert [x.after["id"] for x in out[0].events] == list(range(1, 500))  # type: ignore[index]
    assert not list(tmp_path.iterdir())               # spill file removed
    assert a.mem_bytes == 0


def test_spilled_rollback_cleans_up(tmp_path) -> None:  # type: ignore[no-untyped-def]
    a = TxnAssembler(byte_budget=500, spill_dir=tmp_path)
    a.feed([e(i, "X", "INSERT", id=i, pad="y" * 40) for i in range(1, 50)] + [e(99, "X", "ROLLBACK")])
    assert not list(tmp_path.iterdir()) and not a.open


def test_low_water_and_replay_identical() -> None:
    batches = [
        [e(1, "A", "INSERT", id=1), e(2, "B", "INSERT", id=2)],
        [e(3, "B", "COMMIT"), e(4, "C", "INSERT", id=3)],
        [e(5, "A", "COMMIT"), e(6, "C", "COMMIT")],
    ]
    a = TxnAssembler()
    full = [cs for seq, b in enumerate(batches) for cs in a.feed(b, seq)]
    # crash after batch 1 (seq 1): open = A (from seq 0), C (from seq 1) -> low water 0
    b = TxnAssembler()
    b.feed(batches[0], 0)
    first = b.feed(batches[1], 1)
    assert b.low_water_seq(2) == 0
    c = TxnAssembler()                                 # restarted process replays from low water
    replay = [cs for seq in range(0, 3) for cs in c.feed(batches[seq], seq)]
    applied = {cs.commit_position.text for cs in first}
    after_restart = [cs for cs in replay if cs.commit_position.text not in applied]
    assert [cs.model_dump() for cs in first + after_restart] == [cs.model_dump() for cs in full]


def test_long_open_metric() -> None:
    a = TxnAssembler(long_txn_seconds=0.0)
    a.feed([e(1, "L", "INSERT", id=1)])
    assert a.long_open() and a.oldest_open_seconds() >= 0.0
    assert a.feed([e(2, "Z", "COMMIT")]) == []          # commit of unknown txn = no-op


def test_binary_values_and_spill(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """F1: bytes (BINARY/BLOB, non-UTF-8) must not crash sizing or spilling."""
    from datetime import datetime
    from decimal import Decimal

    a = TxnAssembler(byte_budget=1_000, spill_dir=tmp_path)
    blob = bytes(range(256)) * 4
    evs = [ChangeEvent(position=Position(engine="x", text=f"1:{i:016X}"), txid="B", kind=ChangeKind.INSERT, table=T,
                       after={"id": i, "img": blob, "amt": Decimal("1.25"), "ts": datetime(2026, 1, 1)})
           for i in range(1, 21)]
    a.feed(evs)
    assert a.stats.spills > 0
    out = a.feed([e(99, "B", "COMMIT")])
    assert len(out[0].events) == 20 and out[0].events[3].after["img"] == blob  # type: ignore[index]
    assert out[0].events[0].after["amt"] == Decimal("1.25")  # type: ignore[index]
