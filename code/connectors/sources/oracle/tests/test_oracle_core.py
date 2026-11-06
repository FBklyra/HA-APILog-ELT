"""Unit tests: Oracle position, SQL_REDO parser and transaction assembly (no database needed)."""
from __future__ import annotations

import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT / "hub"), str(ROOT / "connectors" / "sources" / "oracle")]

from alelt_common.models import CanonicalType, ColumnInfo, TableInfo, TableRef  # noqa: E402
from alelt_src_oracle.assemble import Assembler  # noqa: E402
from alelt_src_oracle.position import Scn, compare_scn  # noqa: E402
from alelt_src_oracle.redosql import parse_stmt  # noqa: E402

TI = TableInfo(table=TableRef(schema="S", name="T"), capture_enabled=True, columns=[
    ColumnInfo(name="ID", ordinal=0, engine_type="NUMBER", canonical_type=CanonicalType.INT, length=9, scale=0,
               key_seq=1),
    ColumnInfo(name="AMT", ordinal=1, engine_type="NUMBER", canonical_type=CanonicalType.DECIMAL, length=13,
               scale=2),
    ColumnInfo(name="D", ordinal=2, engine_type="DATE", canonical_type=CanonicalType.LOCALDATETIME)])


def ins(scn: int, xid: str, i: int, rowid: str, rollback: int = 0) -> dict:
    sql = (f"insert into \"S\".\"T\"(\"ID\",\"AMT\",\"D\") values ('{i}','1.5',"
           f"TO_DATE('2026-11-03 10:00:00', 'YYYY-MM-DD HH24:MI:SS'));")
    return dict(SCN=scn, XID=xid, OPERATION_CODE=1, SEG_OWNER="S", TABLE_NAME="T", ROW_ID=rowid, ROLLBACK=rollback,
                SQL_REDO=sql)


def run(rows: list[dict], skip: Scn | None = None) -> list:
    a = Assembler({("S", "T"): TI}, ["S"], skip=skip, empty_commit_every_s=0)
    out = []
    for r in rows:
        out += a.feed(r)
    return out + a.flush()


def test_position_order() -> None:
    assert compare_scn("100", "100.0A") < 0
    assert compare_scn("100.0B~90", "100.0A") > 0
    assert Scn.parse("100.0a~90").text == "100.0A~90"
    assert Scn.parse("100.0A~90").mine_from == 90


def test_parser_literals() -> None:
    st = parse_stmt("update \"S\".\"T\" set \"AMT\" = '2' where \"ID\" = '1' and \"D\" IS NULL and "
                    "ROWID = 'AAAB';")
    assert st.values["AMT"].value == "2" and st.where["D"].is_null and st.rowid == "AAAB"


def test_commit_rollback_savepoint() -> None:
    rows = [dict(SCN=1, XID="A", OPERATION_CODE=6), ins(2, "A", 1, "R1"), ins(3, "A", 2, "R2"),
            dict(SCN=4, XID="A", OPERATION_CODE=2, SEG_OWNER="S", TABLE_NAME="T", ROW_ID="R2", ROLLBACK=1,
                 SQL_REDO="delete from \"S\".\"T\" where \"ID\" = '2';"),
            dict(SCN=5, XID="B", OPERATION_CODE=6), ins(5, "B", 3, "R3"), dict(SCN=6, XID="B", OPERATION_CODE=36),
            dict(SCN=7, XID="A", OPERATION_CODE=7)]
    out = run(rows)
    assert [e.kind.value for e in out] == ["INSERT", "COMMIT"]
    assert out[0].after == {"ID": 1, "AMT": Decimal("1.50"), "D": datetime(2026, 11, 3, 10)}
    assert out[1].position.text == "7.A~7"
    # resume after the commit token: nothing delivered twice
    assert run(rows, skip=Scn.parse(out[1].position.text)) == []


def test_savepoint_undo_of_lob_table_insert_with_zero_rowid() -> None:
    """Seen live (23ai): INSERT into a LOB table has a zero ROW_ID, then an UPDATE (LOB columns) with the real ROW_ID,
    and ROLLBACK TO SAVEPOINT writes ONE delete compensation -> the whole row history of the txn is dropped."""
    upd = dict(SCN=3, XID="A", OPERATION_CODE=3, SEG_OWNER="S", TABLE_NAME="T", ROW_ID="R2", ROLLBACK=0,
               SQL_REDO="update \"S\".\"T\" set \"AMT\" = '1.5' where \"ID\" = '2' and \"AMT\" = '1.5' and "
                        "ROWID = 'R2';")
    rows = [dict(SCN=1, XID="A", OPERATION_CODE=6), ins(2, "A", 1, "R1"), ins(2, "A", 2, "AAAAAAAAAAAAAAAAAA"), upd,
            dict(SCN=4, XID="A", OPERATION_CODE=2, SEG_OWNER="S", TABLE_NAME="T", ROW_ID="R2", ROLLBACK=1,
                 SQL_REDO="delete from \"S\".\"T\" where ROWID = 'R2';"),
            dict(SCN=5, XID="A", OPERATION_CODE=7)]
    out = run(rows)
    assert [(e.kind.value, (e.after or {}).get("ID")) for e in out] == [("INSERT", 1), ("COMMIT", None)]


def test_timestamp_tz_without_offset_is_session_utc() -> None:
    """TIMESTAMP WITH LOCAL TIME ZONE: LogMiner prints TO_TIMESTAMP_TZ('<local time>') without an offset."""
    st = parse_stmt("insert into \"S\".\"T\"(\"X\",\"Y\") values (TO_TIMESTAMP_TZ('2026-11-04 08:00:00.500000000'),"
                    "TO_TIMESTAMP_TZ('2026-11-04 08:00:00.123000000 -05:00'));")
    assert st.values["X"].value == datetime(2026, 11, 4, 8, 0, 0, 500000)
    assert st.values["Y"].value.utcoffset().total_seconds() == -5 * 3600


def test_delete_compensation_heuristic_only_with_the_zero_rowid_insert_pattern() -> None:
    """SRC-ora-09: a DELETE compensation that matches an UPDATE removes the row's earlier history only when the
    transaction holds the LOB pattern (INSERT with a zero ROW_ID); otherwise the earlier INSERT is kept."""
    upd = dict(SCN=3, XID="A", OPERATION_CODE=3, SEG_OWNER="S", TABLE_NAME="T", ROW_ID="R2", ROLLBACK=0,
               SQL_REDO="update \"S\".\"T\" set \"AMT\" = '2.5' where \"ID\" = '2' and \"AMT\" = '1.5' and "
                        "ROWID = 'R2';")
    rows = [dict(SCN=1, XID="A", OPERATION_CODE=6), ins(2, "A", 2, "R2"), upd,
            dict(SCN=4, XID="A", OPERATION_CODE=2, SEG_OWNER="S", TABLE_NAME="T", ROW_ID="R2", ROLLBACK=1,
                 SQL_REDO="delete from \"S\".\"T\" where ROWID = 'R2';"),
            dict(SCN=5, XID="A", OPERATION_CODE=7)]
    out = run(rows)
    assert [(e.kind.value, (e.after or {}).get("ID")) for e in out] == [("INSERT", 2), ("COMMIT", None)]


def test_restart_lookback_is_capped_and_long_txn_reported() -> None:
    """SRC-ora-06: with max_lookback_scns the restart SCN never lies further back than the cap (a transaction that
    started earlier becomes 'start not mined' = hold + resync after a restart); oldest_open() reports its age."""
    t = [0.0]
    a = Assembler({("S", "T"): TI}, ["S"], empty_commit_every_s=0, clock=lambda: t[0], max_lookback_scns=1000)
    a.feed(dict(SCN=10, XID="OLD", OPERATION_CODE=6))
    a.feed(ins(11, "OLD", 1, "R1"))
    t[0] = 7200.0
    assert a.oldest_open() == ("OLD", 7200.0)
    assert a.restart_scn(500) == 10                      # within the cap
    assert a.restart_scn(50_000) == 49_000               # capped
    b = Assembler({("S", "T"): TI}, ["S"], empty_commit_every_s=0)
    b.feed(dict(SCN=10, XID="OLD", OPERATION_CODE=6))
    assert b.restart_scn(50_000) == 10                   # unlimited by default


def test_timestamp_ff9_keeps_microseconds_and_notes_precision_lost() -> None:
    """XC-36: TIMESTAMP(9) -> LOCALDATETIME with microseconds; non-zero sub-microsecond digits -> precision_lost."""
    ti = TableInfo(table=TableRef(schema="S", name="T9"), capture_enabled=True, columns=[
        ColumnInfo(name="ID", ordinal=0, engine_type="NUMBER", canonical_type=CanonicalType.INT, length=9, scale=0,
                   key_seq=1),
        ColumnInfo(name="TS", ordinal=1, engine_type="TIMESTAMP(9)", canonical_type=CanonicalType.LOCALDATETIME)])
    a = Assembler({("S", "T9"): ti}, ["S"], empty_commit_every_s=0)
    out: list = []
    for i, frac in ((1, "123456789"), (2, "123456000")):
        sql = (f"insert into \"S\".\"T9\"(\"ID\",\"TS\") values ('{i}',TO_TIMESTAMP('2026-11-03 10:00:00.{frac}', "
               "'YYYY-MM-DD HH24:MI:SS.FF'));")
        out += a.feed(dict(SCN=1 + i, XID="A", OPERATION_CODE=1, SEG_OWNER="S", TABLE_NAME="T9", ROW_ID=f"R{i}",
                           ROLLBACK=0, SQL_REDO=sql))
    out += a.feed(dict(SCN=9, XID="A", OPERATION_CODE=7)) + a.flush()
    ins = [e for e in out if e.kind.value == "INSERT"]
    assert [e.after["TS"] for e in ins] == [datetime(2026, 11, 3, 10, 0, 0, 123456)] * 2
    assert "precision_lost:TS" in (ins[0].note or "") and "precision_lost" not in (ins[1].note or "")
