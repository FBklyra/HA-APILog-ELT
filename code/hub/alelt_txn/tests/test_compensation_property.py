"""F-11: one compensation rule. Property: for random interleavings of DML and savepoint rollbacks across tables
(with and without spill, with and without row refs), the assembler result equals the connector-side result
(savepoint resolved in the source, as MySQL/Oracle/SQL Server connectors do)."""
from __future__ import annotations

import random

import pytest

from alelt_common.models import ChangeEvent, ChangeKind, Position, TableRef
from alelt_txn import TxnAssembler
from alelt_txn.assembler import note_ref

TABLES = [TableRef(schema="S", name=n) for n in ("A", "B", "C")]


class _Gen:
    def __init__(self) -> None:
        self.n = 0

    def ev(self, txid: str, kind: str, table: TableRef | None = None, note: str | None = None,
           **after: object) -> ChangeEvent:
        self.n += 1
        return ChangeEvent(position=Position(engine="x", text=f"1:{self.n:016X}"), txid=txid,
                           kind=ChangeKind(kind), table=table, after=after or None, note=note)


def _scenario(seed: int, with_refs: bool) -> tuple[list[ChangeEvent], list[tuple[str, int]]]:
    """Return (event stream fed to the assembler, expected (table, seq) list of surviving DML)."""
    rnd = random.Random(seed)
    g = _Gen()
    stream: list[ChangeEvent] = []
    live: list[tuple[TableRef, int, str]] = []          # connector-side view of the txn
    savepoints: list[int] = []
    seq = 0
    for _ in range(rnd.randint(5, 120)):
        r = rnd.random()
        if r < 0.15:
            savepoints.append(len(live))
        elif r < 0.27 and savepoints:
            sp = savepoints.pop(rnd.randrange(len(savepoints)))
            del savepoints[:]                              # rolling back to sp releases later savepoints
            undone = live[sp:]
            del live[sp:]
            for t, _s, ref in reversed(undone):            # undo records are written newest-first
                note = f"undo update;target_ref:{ref}" if with_refs else "undo update"
                stream.append(g.ev("T1", "COMPENSATION", table=t, note=note))
        else:
            seq += 1
            t = rnd.choice(TABLES)
            ref = f"rid{rnd.randint(1, 8)}"               # rows are revisited: refs repeat within the txn
            stream.append(g.ev("T1", rnd.choice(["INSERT", "UPDATE", "DELETE"]), table=t,
                               note=f"row_ref:{ref}" if with_refs else None, id=seq, pad="x" * 40))
            live.append((t, seq, ref))
    stream.append(g.ev("T1", "COMMIT"))
    return stream, [(t.fqn, s) for t, s, _ in live]


@pytest.mark.parametrize("with_refs", [True, False])
@pytest.mark.parametrize("budget", [64 << 20, 1_500])          # 1.5 kB forces repeated spills
def test_random_savepoints_match_connector_side(tmp_path, with_refs: bool, budget: int) -> None:  # type: ignore[no-untyped-def]
    for seed in range(150):
        stream, expected = _scenario(seed, with_refs)
        a = TxnAssembler(byte_budget=budget, spill_dir=tmp_path)
        out = a.feed(stream)
        got = [(ev.table.fqn, ev.after["id"]) for cs in out for ev in cs.events] if out else []
        assert got == expected, f"seed {seed}"
        assert not a.open and a.mem_bytes == 0


def test_target_ref_wins_over_position() -> None:
    """A compensation naming an older row of the same table undoes that row, not the newest one."""
    g = _Gen()
    t = TABLES[0]
    a = TxnAssembler()
    out = a.feed([g.ev("X", "UPDATE", t, "row_ref:r1", id=1), g.ev("X", "UPDATE", t, "row_ref:r2", id=2),
                  g.ev("X", "COMPENSATION", t, "undo update;target_ref:r1"), g.ev("X", "COMMIT")])
    assert [ev.after["id"] for ev in out[0].events] == [2]


def test_spilled_undo_matches_table_not_counter(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """Old rule: a spilled undo cancelled 'the newest spilled event' whatever its table."""
    g = _Gen()
    a, b = TABLES[0], TABLES[1]
    asm = TxnAssembler(byte_budget=600, spill_dir=tmp_path)
    evs = [g.ev("X", "INSERT", a, id=1, pad="y" * 300), g.ev("X", "INSERT", b, id=2, pad="y" * 300),
           g.ev("X", "INSERT", b, id=3, pad="y" * 300)]
    asm.feed(evs)
    assert asm.open["X"].spilled_count >= 2
    out = asm.feed([g.ev("X", "COMPENSATION", a, "undo insert"), g.ev("X", "COMMIT")])
    assert sorted(ev.after["id"] for ev in out[0].events) == [2, 3]


def test_note_ref_parsing() -> None:
    assert note_ref("undo update;target_ref:00AB12", "target_ref") == "00AB12"
    assert note_ref("lob_unavailable:X; row_ref: r9", "row_ref") == "r9"
    assert note_ref(None, "row_ref") is None and note_ref("row_ref:", "row_ref") is None
