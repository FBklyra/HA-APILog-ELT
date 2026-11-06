"""PostgreSQL source completeness (X-132): every pgoutput message classified; no silent loss, no guessed value.

Golden case `edge` (corpus/make_corpus.py --case edge, recorded from PostgreSQL 16) + synthetic messages for
situations a healthy server does not produce on demand."""
from __future__ import annotations

import json
import struct
from typing import Any

from conftest import GOLDEN

from alelt_common.canonical_jsonl import encode_event
from alelt_common.completeness import Census, RecordClass, signal_of
from alelt_common.models import ChangeEvent, ChangeKind
from alelt_src_postgres.changes import ChangeBuilder
from alelt_src_postgres.position import parse_lsn


def replay(case: str) -> tuple[list[ChangeEvent], Census]:
    types = json.loads((GOLDEN / f"{case}.types.json").read_text(encoding="utf-8"))
    b = ChangeBuilder(lambda oids: {o: types[str(o)] for o in oids if str(o) in types})
    out: list[ChangeEvent] = []
    for line in (GOLDEN / f"{case}.pgoutput.jsonl").read_text(encoding="utf-8").splitlines():
        d = json.loads(line)
        out.extend(b.feed(bytes.fromhex(d["hex"]), parse_lsn(d["data_start"])))
    return out, b.census


def test_edge_golden_bytes_match_expected() -> None:
    evs, _ = replay("edge")
    assert b"".join(encode_event(e) for e in evs) == (GOLDEN / "edge.expected.jsonl").read_bytes()


def test_edge_census_every_message_classified() -> None:
    _, c = replay("edge")
    n = len((GOLDEN / "edge.pgoutput.jsonl").read_text(encoding="utf-8").splitlines())
    assert c.total == n and c.unexplained_total == 0
    rows = {(r["record"], r["class"], r["reason"]): r["count"] for r in c.rows()}
    assert rows[("pgoutput:M", "ignored", "logical decoding message (pg_logical_emit_message: not table data)")] == 2
    assert rows[("pgoutput:O", "ignored", "origin message (replication origin of the txn)")] == 1
    assert ("pgoutput:Y", "ignored", "type message (types resolved through pg_type)") in rows
    assert rows[("pgoutput:U", "needs_keyed_reread", "unchanged TOAST not recoverable")] == 1


def test_key_changing_update_with_unchanged_toast_is_reread_by_new_key() -> None:
    evs, _ = replay("edge")
    i = next(i for i, e in enumerate(evs) if e.kind is ChangeKind.UPDATE and (e.before or {}).get("id") == 1)
    up, sig = evs[i], signal_of(evs[i + 1])
    assert up.after == {"id": 3, "flag": None} and up.note == "unchanged:big"
    assert sig is not None and sig.cls is RecordClass.NEEDS_KEYED_REREAD and sig.keys == [{"id": 3}]
    assert sig.table is not None and sig.table.name == "edge_parent" and evs[i + 1].txid == up.txid
    plain = next(e for e in evs if e.kind is ChangeKind.UPDATE and (e.after or {}).get("flag") == "f")
    assert plain.note == "unchanged:big" and not any(signal_of(e) for e in evs if e.txid == plain.txid)


def test_truncate_cascade_types_and_mid_txn_ddl() -> None:
    evs, _ = replay("edge")
    tr = [e for e in evs if e.kind is ChangeKind.TRUNCATE]
    assert [(e.table.name, e.note) for e in tr] == [("edge_parent", "cascade"), ("edge_child", "cascade")]  # type: ignore[union-attr]
    t1 = next(e for e in evs if e.kind is ChangeKind.INSERT and e.table and e.table.name == "edge_types"
              and (e.after or {}).get("id") == 1)
    a = t1.after or {}
    assert a["d"] == 5 and a["m"] == "happy" and a["r"] == "[1,10)" and a["pt"] == "(1.5,2)"
    assert a["bx"] == "(1,1),(0,0)" and a["ci"] == "<(1,1),2>" and a["ln"] == "[(0,0),(1,1)]"
    assert json.loads(a["arr"]) == ["a", "b c", None]
    assert "gen" not in (evs[1].after or {})          # generated columns are not published (catalog excludes them)
    kinds = [(e.kind, (e.after or {}).get("id")) for e in evs if e.table and e.table.name == "edge_types"]
    assert kinds[-3:] == [(ChangeKind.INSERT, 3), (ChangeKind.DDL, None), (ChangeKind.INSERT, 4)]


# ------------------------------------------------------------------ synthetic
def _cs(s: str) -> bytes:
    return s.encode() + b"\x00"


def _rel(relid: int = 16400) -> bytes:
    b = b"R" + struct.pack("!I", relid) + _cs("s") + _cs("t") + b"d" + struct.pack("!h", 2)
    for flags, name, oid in ((1, "id", 23), (0, "v", 25)):
        b += struct.pack("!b", flags) + _cs(name) + struct.pack("!Ii", oid, -1)
    return b


def _tup(*vals: Any) -> bytes:
    b = struct.pack("!h", len(vals))
    for v in vals:
        b += b"u" if v == "u" else b"t" + struct.pack("!i", len(v)) + v
    return b


def _builder(**kw: Any) -> ChangeBuilder:
    b = ChangeBuilder(shapes={("s", "t"): (("id", "int4"), ("v", "text"))}, **kw)
    b.feed(_rel(), 0)
    b.feed(b"B" + struct.pack("!qqI", 0x200, 0, 42), 0x100)
    return b


def test_unchanged_toast_in_insert_is_reread() -> None:
    b = _builder()
    out = b.feed(b"I" + struct.pack("!I", 16400) + b"N" + _tup(b"7", "u"), 0x110)
    assert out[0].after == {"id": 7} and signal_of(out[1]).keys == [{"id": 7}]  # type: ignore[union-attr]


def test_unknown_relation_resolved_by_oid_and_truncate_mixed() -> None:
    b = _builder(resolve_relation=lambda oid: ("s", "other") if oid == 555 else None)
    out = b.feed(b"U" + struct.pack("!I", 555) + b"N" + _tup(b"1", b"x"), 0x110)
    sigs = [signal_of(e) for e in out]
    assert [s.cls for s in sigs] == [RecordClass.NEEDS_CATALOG_REFRESH, RecordClass.UNSUPPORTED]  # type: ignore[union-attr]
    assert sigs[1].table.name == "other" and out[0].kind is ChangeKind.DDL  # type: ignore[union-attr]
    tr = b.feed(b"T" + struct.pack("!ibII", 2, 1, 16400, 777), 0x120)
    assert tr[0].kind is ChangeKind.TRUNCATE and tr[0].table.name == "t"  # type: ignore[union-attr]
    assert signal_of(tr[2]).table is None and signal_of(tr[2]).cls is RecordClass.UNSUPPORTED  # type: ignore[union-attr]


def test_stream_without_first_segment_is_connection_wide_unsupported() -> None:
    b = ChangeBuilder(shapes={("s", "t"): (("id", "int4"), ("v", "text"))})
    b.feed(_rel(), 0)
    b.feed(b"S" + struct.pack("!Ib", 70, 0), 0)                     # not the first segment, never saw 70 before
    b.feed(b"I" + struct.pack("!II", 70, 16400) + b"N" + _tup(b"1", b"a"), 0x10)
    b.feed(b"E", 0)
    out = b.feed(b"c" + struct.pack("!Ibqqq", 70, 0, 0x40, 0x48, 0), 0)
    assert [e.kind for e in out] == [ChangeKind.INSERT, ChangeKind.OTHER, ChangeKind.COMMIT]
    s = signal_of(out[1])
    assert s is not None and s.cls is RecordClass.UNSUPPORTED and s.table is None
    assert b.feed(b"A" + struct.pack("!II", 99, 99), 0) == []          # abort of an unseen txn: ignored
    assert ("pgoutput:A", "ignored", "stream abort of a txn not seen") in {
        (r["record"], r["class"], r["reason"]) for r in b.census.rows()}


def test_two_phase_message_is_unsupported_not_dropped() -> None:
    b = _builder()
    out = b.feed(b"P" + b"\x00" * 8, 0x300)
    assert signal_of(out[0]).cls is RecordClass.UNSUPPORTED and b.census.unexplained_total == 0  # type: ignore[union-attr]


# ------------------------------------------------------------------ slot resume gap (XC-05, XC-34)
def test_slot_resume_gap_is_a_typed_gap_error() -> None:
    import pytest

    from alelt_common.spi import is_gap_error
    from alelt_src_postgres.connector import SlotLostError, check_slot_resume

    check_slot_resume(None, 0x500, "reserved")                 # first start: nothing durable yet
    check_slot_resume(0x500, 0x500, "reserved")                # slot exactly at the durable position
    check_slot_resume(0x600, 0x500, "extended")                # slot behind: changes are re-delivered (idempotent)
    for args in ((0x500, 0x600, "reserved"), (0x500, 0x400, "lost"), (0x500, 0x400, "reserved", "wal_removed")):
        with pytest.raises(SlotLostError) as ei:
            check_slot_resume(*args)                           # type: ignore[arg-type]
        assert is_gap_error(ei.value)
