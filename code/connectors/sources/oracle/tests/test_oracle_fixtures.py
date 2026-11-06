"""Oracle assembler against tests/golden/oracle/logminer_fixtures.json (SYNTHETIC rows, see build_fixtures.py) and,
when present, logminer_live.json / expected_live.jsonl recorded from a live Oracle (record_oracle.py)."""
from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT / "hub"), str(ROOT / "connectors" / "sources" / "oracle")]

from alelt_common.canonical_jsonl import decode_batch, encode_events  # noqa: E402
from alelt_common.models import CanonicalType as CT, CatalogSnapshot, ChangeKind, ColumnInfo, TableInfo, TableRef  # noqa: E402
from alelt_src_oracle.assemble import Assembler  # noqa: E402
from alelt_src_oracle.position import Scn, compare_scn  # noqa: E402

GOLD = ROOT / "tests" / "golden" / "oracle"


def col(name: str, i: int, et: str, ct: CT, length: int | None = None, scale: int | None = None,
        key: int | None = None) -> ColumnInfo:
    return ColumnInfo(name=name, ordinal=i, engine_type=et, canonical_type=ct, length=length, scale=scale, key_seq=key)


ORDERS = TableInfo(table=TableRef(schema="ALELTTEST", name="ALELT_ORDERS"), capture_enabled=True, columns=[
    col("ORDER_ID", 0, "NUMBER", CT.INT, 9, 0, 1), col("CUSTOMER", 1, "VARCHAR2", CT.STRING, 40),
    col("STATUS", 2, "CHAR", CT.STRING, 1), col("AMOUNT", 3, "NUMBER", CT.DECIMAL, 12, 2),
    col("QTY", 4, "NUMBER", CT.SHORT, 5, 0), col("BIG", 5, "NUMBER", CT.LONG, 19, 0),
    col("RATE", 6, "BINARY_DOUBLE", CT.DOUBLE), col("RATE32", 7, "BINARY_FLOAT", CT.FLOAT),
    col("ORDER_DATE", 8, "DATE", CT.LOCALDATETIME), col("CREATED", 9, "TIMESTAMP(6)", CT.LOCALDATETIME, scale=6),
    col("EVENT_TS", 10, "TIMESTAMP(3) WITH TIME ZONE", CT.INSTANT, scale=3),
    col("LOCAL_TS", 11, "TIMESTAMP(3) WITH LOCAL TIME ZONE", CT.INSTANT, scale=3),
    col("RAW_COL", 12, "RAW", CT.BINARY, 16), col("BIGTXT", 13, "VARCHAR2", CT.STRING, 4000),
    col("NOTES", 14, "CLOB", CT.STRING), col("DOC", 15, "BLOB", CT.BINARY)])
LINES = TableInfo(table=TableRef(schema="ALELTTEST", name="ALELT_ORDER_LINES"), capture_enabled=True, columns=[
    col("ORDER_ID", 0, "NUMBER", CT.INT, 9, 0, 1), col("LINE_NO", 1, "NUMBER", CT.INT, 9, 0, 2),
    col("ITEM", 2, "VARCHAR2", CT.STRING, 30), col("QTY", 3, "NUMBER", CT.INT, 9, 0)])
TABLES = {("ALELTTEST", "ALELT_ORDERS"): ORDERS, ("ALELTTEST", "ALELT_ORDER_LINES"): LINES}


def rows_of(path: Path) -> list[dict[str, Any]]:
    doc = json.loads(path.read_text(encoding="utf-8"))
    out = []
    for r in doc["rows"]:
        r = dict(r)
        if isinstance(r.get("TIMESTAMP"), dict):
            r["TIMESTAMP"] = datetime.fromisoformat(r["TIMESTAMP"]["$ldt"])
        out.append(r)
    return out


def run(rows: list[dict[str, Any]], tables: dict[Any, TableInfo] = TABLES, skip: Scn | None = None) -> list[Any]:
    a = Assembler(tables, ["ALELTTEST"], skip=skip, empty_commit_every_s=0)
    out: list[Any] = []
    for r in rows:
        out += a.feed(r)
    return out + a.flush()


@pytest.fixture(scope="module")
def evs() -> list[Any]:
    return run(rows_of(GOLD / "logminer_fixtures.json"))


def test_txns_whole_in_commit_order(evs: list[Any]) -> None:
    commits = [e for e in evs if e.kind is ChangeKind.COMMIT]
    assert [c.txid for c in commits] == ["0A0001000000AAAA", "0A0003000000CCCC", "0A0005000000EEEE",
                                         "0A0004000000DDDD", "0A0006000000FFFF"]
    assert all(compare_scn(a.position.text, b.position.text) < 0 for a, b in zip(commits, commits[1:]))
    cur = None
    for e in evs:                                   # contiguous: no interleaving of txids
        if cur is None:
            cur = e.txid
        assert e.txid == cur
        if e.kind is ChangeKind.COMMIT:
            cur = None
    # restart SCN of C's commit token = start of D (still open) -> resume mines from there
    c_tok = Scn.parse(commits[1].position.text)
    assert c_tok.mine_from == 124


def test_csf_lob_rollback_savepoint(evs: list[Any]) -> None:
    blob = encode_events(evs).decode()
    assert "rolled back" not in blob and "SAVEPOINT_UNDONE" not in blob and "OTHER_T" not in blob
    first = evs[0]
    assert first.kind is ChangeKind.INSERT and first.after is not None
    a = first.after
    assert a["BIGTXT"] == "x" + "y" * 3989                       # CSF continuation reassembled
    assert a["CUSTOMER"] == "Acmé Ltd ✓ it's" and a["AMOUNT"] == Decimal("1234.50") and a["QTY"] == -7
    assert a["BIG"] == 9007199254740993 and a["RATE"] == 1.5 and a["RATE32"] == 0.25
    assert a["EVENT_TS"] == datetime(2026, 11, 4, 13, 0, 0, 123000, tzinfo=timezone.utc)
    assert a["CREATED"] == datetime(2026, 11, 4, 8, 0, 0, 123456) and a["RAW_COL"] == b"\x00\xff\x10"
    assert a["NOTES"] == "short clob" and a["DOC"] == b"\xde\xad\xbe\xef"   # LOB_WRITE folded into the INSERT
    lines = [e for e in evs if e.table and e.table.name == "ALELT_ORDER_LINES"]
    assert [(e.kind.value, (e.after or e.before)["LINE_NO"]) for e in lines] == [
        ("INSERT", 1), ("INSERT", 2), ("INSERT", 3), ("UPDATE", 20)]
    pk = lines[-1]
    assert pk.before and pk.before["LINE_NO"] == 2 and pk.after and pk.after["LINE_NO"] == 20
    upd = next(e for e in evs if e.kind is ChangeKind.UPDATE and e.table.name == "ALELT_ORDERS")
    assert upd.after["STATUS"] == "P" and upd.after["AMOUNT"] == Decimal("99.99")
    assert "unchanged:" in (upd.note or "") and "NOTES" not in upd.after
    dl = next(e for e in evs if e.kind is ChangeKind.DELETE)
    assert dl.before == {"ORDER_ID": 202, "STATUS": "N"}


def test_unsupported_quarantined_with_raw_not_dropped(evs: list[Any]) -> None:
    res = decode_batch(encode_events(evs), "oracle", uuid.uuid4())
    assert len(res.quarantined) == 1 and res.quarantined[0].position is not None
    assert "UNSUPPORTED" in bytes.fromhex(res.quarantined[0].raw_hex).decode("utf-8", "replace")
    assert len(res.events) == len(evs) - 1


def test_resume_after_each_commit_no_dup_no_gap(evs: list[Any]) -> None:
    rows = rows_of(GOLD / "logminer_fixtures.json")
    commits = [e for e in evs if e.kind is ChangeKind.COMMIT]
    for c in commits:
        tok = Scn.parse(c.position.text)
        tail = run([r for r in rows if int(r["SCN"]) >= tok.mine_from], skip=tok)
        exp = evs[evs.index(c) + 1:]
        assert [e.model_dump() for e in tail] == [e.model_dump() for e in exp]


LIVE = GOLD / "logminer_live.json"


@pytest.mark.skipif(not LIVE.exists(), reason="no live LogMiner recording (record_oracle.py)")
def test_live_recording_matches_expected() -> None:
    doc = json.loads(LIVE.read_text(encoding="utf-8"))
    cat = CatalogSnapshot.model_validate(doc["catalog"])
    tables = {(t.table.schema_, t.table.name): t for t in cat.tables}
    a = Assembler(tables, ["ALELTTEST"], empty_commit_every_s=3600)
    out: list[Any] = []
    for r in rows_of(LIVE):
        out += a.feed(r)
    out += a.flush()
    assert encode_events(out) == (GOLD / "expected_live.jsonl").read_bytes()
    blob = encode_events(out).decode()
    assert "rolled back" not in blob and "SAVEPOINT_UNDONE" not in blob
