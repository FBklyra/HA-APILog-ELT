"""Oracle LogMiner completeness (X-132, XC-33): every V$LOGMNR_CONTENTS row classified, nothing guessed.

Synthetic rows in the V$LOGMNR_CONTENTS shapes recorded live (tests/golden/oracle/logminer_live.json)."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT / "hub"), str(ROOT / "connectors" / "sources" / "oracle"), str(Path(__file__).parent)]

from test_oracle_fixtures import TABLES  # noqa: E402

from alelt_common.completeness import RecordClass, signal_of  # noqa: E402
from alelt_common.models import ChangeKind  # noqa: E402
from alelt_src_oracle.assemble import Assembler  # noqa: E402

XID = "0A00040041020000"
LOC = ('DECLARE loc_c CLOB; buf_c VARCHAR2(100); BEGIN select "NOTES" into loc_c from "ALELTTEST"."ALELT_ORDERS" '
       'where "ORDER_ID" = \'301\' and ROWID = \'AAAR5FAAYAAAAANAAA\' for update; END;')


def r(scn: int, op: int, redo: str = "", table: str = "ALELT_ORDERS", owner: str = "ALELTTEST", xid: str = XID,
      **kw: Any) -> dict[str, Any]:
    d = {"SCN": scn, "XID": xid, "OPERATION_CODE": op, "OPERATION": str(op), "SEG_OWNER": owner, "TABLE_NAME": table,
         "ROW_ID": "AAAR5FAAYAAAAANAAA", "CSF": 0, "ROLLBACK": 0, "RS_ID": f" 0x{scn:x} ", "SSN": 0,
         "SQL_REDO": redo, "SQL_UNDO": None, "TIMESTAMP": None, "INFO": None}
    d.update(kw)
    return d


CAPTURED = {k: v.model_copy(update={"capture_enabled": True}) for k, v in TABLES.items()}


def run(rows: list[dict[str, Any]], **kw: Any) -> tuple[list[Any], Assembler]:
    a = Assembler(CAPTURED, ["ALELTTEST"], empty_commit_every_s=0, **kw)
    out: list[Any] = []
    for x in rows:
        out += a.feed(x)
    return out + a.flush(force_token=True), a


def sigs(evs: list[Any]) -> list[Any]:
    return [s for s in (signal_of(e) for e in evs) if s is not None]


def test_truncate_ddl_becomes_truncate_event() -> None:
    evs, a = run([r(10, 6), r(11, 5, 'truncate table "ALELTTEST"."ALELT_ORDERS"'), r(12, 7)])
    tr = [e for e in evs if e.kind is ChangeKind.TRUNCATE]
    assert len(tr) == 1 and tr[0].table.name == "ALELT_ORDERS" and not [e for e in evs if e.kind is ChangeKind.DDL]
    evs2, _ = run([r(10, 6), r(11, 5, "TRUNCATE TABLE alelt_order_lines DROP STORAGE", table=""), r(12, 7)])
    assert [(e.kind, e.table.name) for e in evs2 if e.table] == [(ChangeKind.TRUNCATE, "ALELT_ORDER_LINES")]
    evs3, _ = run([r(10, 6), r(11, 5, 'alter table "ALELTTEST"."ALELT_ORDERS" add (X number)'), r(12, 7)])
    assert [e.kind for e in evs3 if e.table] == [ChangeKind.DDL]
    assert a.census.unexplained_total == 0


def test_unsupported_and_unknown_operation_hold_the_table() -> None:
    evs, a = run([r(10, 6), r(11, 255, "Unsupported", INFO="Unsupported (object type)"), r(12, 99), r(13, 7)])
    s = sigs(evs)
    assert [x.cls for x in s] == [RecordClass.UNSUPPORTED, RecordClass.UNSUPPORTED] and s[0].table.name == "ALELT_ORDERS"
    assert a.census.unexplained_total == 1                     # OPERATION_CODE 99 has no rule: visible, not silent


def test_lob_write_on_unknown_prior_value_is_reread_not_guessed() -> None:
    write = "buf_c := 'abc'; dbms_lob.write(loc_c, 3, 5, buf_c);"            # offset 5: depends on the old value
    evs, _ = run([r(10, 6), r(11, 9, LOC), r(12, 10, write), r(13, 7)])
    s = sigs(evs)
    assert len(s) == 1 and s[0].cls is RecordClass.NEEDS_KEYED_REREAD and s[0].keys == [{"ORDER_ID": 301}]
    assert not [e for e in evs if e.kind is ChangeKind.UPDATE]
    full = "buf_c := 'abc'; dbms_lob.write(loc_c, 3, 1, buf_c); dbms_lob.trim(loc_c, 3);"
    evs2, _ = run([r(10, 6), r(11, 9, LOC), r(12, 10, full), r(13, 7)])
    up = [e for e in evs2 if e.kind is ChangeKind.UPDATE]
    assert not sigs(evs2) and up[0].after["NOTES"] == "abc"   # write + trim: independent of the prior value


def test_column_newer_than_catalog_refresh_and_reread() -> None:
    ins = 'insert into "ALELTTEST"."ALELT_ORDER_LINES"("ORDER_ID","LINE_NO","ITEM","QTY","NEWCOL") values (\'1\',\'2\',\'x\',\'3\',\'7\');'
    evs, _ = run([r(10, 6), r(11, 1, ins, table="ALELT_ORDER_LINES"), r(12, 7)])
    e = next(x for x in evs if x.kind is ChangeKind.INSERT)
    assert "NEWCOL" not in e.after and e.note is None             # never a best-effort value
    s = sigs(evs)
    assert [x.cls for x in s] == [RecordClass.NEEDS_CATALOG_REFRESH, RecordClass.NEEDS_KEYED_REREAD]
    assert s[1].keys == [{"ORDER_ID": 1, "LINE_NO": 2}]


def test_missing_dictionary_resolved_by_object_id() -> None:
    redo = 'insert into "UNKNOWN"."OBJ# 73001"("COL 1") values (HEXTORAW(\'c102\'));'
    rows = [r(10, 6), r(11, 1, redo, table="OBJ# 73001", owner="UNKNOWN"), r(12, 7)]
    evs, a = run(rows, resolve_obj=lambda o: ("ALELTTEST", "ALELT_ORDERS") if o == 73001 else None)
    assert [x.cls for x in sigs(evs)] == [RecordClass.NEEDS_CATALOG_REFRESH, RecordClass.UNSUPPORTED]
    evs2, a2 = run(rows, resolve_obj=lambda o: None)               # dropped object: counted skip, not critical
    assert not sigs(evs2) and ("logminer:1", "ignored") in {(x["record"], x["class"]) for x in a2.census.rows()}


def test_partial_transaction_is_connection_wide_unsupported() -> None:
    upd = 'update "ALELTTEST"."ALELT_ORDER_LINES" set "QTY" = \'4\' where "ORDER_ID" = \'1\' and "LINE_NO" = \'2\';'
    evs, _ = run([r(11, 3, upd, table="ALELT_ORDER_LINES"), r(12, 7)])  # no START row mined
    s = sigs(evs)
    assert len(s) == 1 and s[0].table is None and s[0].cls is RecordClass.UNSUPPORTED


def test_xml_doc_rows_are_reread_by_key() -> None:
    xml = ('XML DOC BEGIN:  select "NOTES" from "ALELTTEST"."ALELT_ORDERS" where "ORDER_ID" = \'301\'')
    evs, a = run([r(10, 6), r(11, 68, xml), r(12, 70, "XML DOC WRITE: ..."), r(13, 71, ""), r(14, 7)])
    s = sigs(evs)
    assert s and all(x.cls in (RecordClass.NEEDS_KEYED_REREAD, RecordClass.UNSUPPORTED) for x in s)
    assert a.census.unexplained_total == 0


def test_census_live_recording_has_no_unexplained_row() -> None:
    import json

    from alelt_common.models import CatalogSnapshot
    from test_oracle_fixtures import LIVE, rows_of
    doc = json.loads(LIVE.read_text(encoding="utf-8"))
    cat = CatalogSnapshot.model_validate(doc["catalog"])
    a = Assembler({(t.table.schema_, t.table.name): t for t in cat.tables}, ["ALELTTEST"], empty_commit_every_s=3600)
    for x in rows_of(LIVE):
        a.feed(x)
    a.flush()
    assert a.census.total == len(doc["rows"]) and a.census.unexplained_total == 0


def test_commit_ts_is_dst_safe_with_server_tz() -> None:
    """SRC-ora-05: with settings.server_tz (IANA) each commit uses the offset valid at that commit: one commit before
    and one after the 2026-03-29 Paris DST switch both land at the right UTC instant."""
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo

    from alelt_common.models import ChangeKind

    rows = [r(10, 6, xid="01"), r(11, 7, xid="01", TIMESTAMP=datetime(2026, 3, 29, 1, 30)),
            r(12, 6, xid="02"), r(13, 7, xid="02", TIMESTAMP=datetime(2026, 3, 29, 3, 30))]
    evs, _ = run(rows, db_tz=ZoneInfo("Europe/Paris"))
    ts = [e.commit_ts for e in evs if e.kind is ChangeKind.COMMIT and e.commit_ts]
    assert ts == [datetime(2026, 3, 29, 0, 30, tzinfo=timezone.utc), datetime(2026, 3, 29, 1, 30, tzinfo=timezone.utc)]


def test_insert_lob_absent_from_redo_is_reread_never_null() -> None:
    """SRC-ora-03: INSERT redo without the LOB columns and no LOB write before COMMIT: NOTES / DOC are omitted
    (never NULL) and the row is re-read by key; a LOB filled by its writes is not re-read."""
    ins = 'insert into "ALELTTEST"."ALELT_ORDERS"("ORDER_ID","CUSTOMER") values (\'301\',\'acme\')'
    evs, a = run([r(10, 6), r(11, 1, ins), r(12, 7)])
    insert = next(e for e in evs if e.kind is ChangeKind.INSERT)
    assert "NOTES" not in insert.after and "DOC" not in insert.after and insert.after["CUSTOMER"] == "acme"
    rr = [s for s in sigs(evs) if s.cls is RecordClass.NEEDS_KEYED_REREAD]
    assert len(rr) == 1 and rr[0].keys == [{"ORDER_ID": 301}] and "NOTES" in rr[0].reason
    ins_full = ('insert into "ALELTTEST"."ALELT_ORDERS"("ORDER_ID","NOTES","DOC") values '
                '(\'302\',EMPTY_CLOB(),EMPTY_BLOB())')
    evs2, _ = run([r(10, 6), r(11, 1, ins_full), r(12, 7)])
    assert not [s for s in sigs(evs2) if s.cls is RecordClass.NEEDS_KEYED_REREAD]
