"""Build tests/golden/oracle/logminer_fixtures.json - SYNTHETIC V$LOGMNR_CONTENTS rows (NOT recorded live).

Shapes follow the documented V$LOGMNR_CONTENTS columns and the SQL_REDO / SQL_UNDO forms printed by LogMiner with
DICT_FROM_ONLINE_CATALOG and the session NLS of alelt_src_oracle.logminer (quoted numbers, TO_DATE/TO_TIMESTAMP(_TZ),
HEXTORAW, ROWID predicates, SELECT_LOB_LOCATOR + LOB_WRITE PL/SQL blocks, CSF continuation, ROLLBACK=1 undo rows).
Replace / complement with logminer_live.json (record_oracle.py) whenever a live Oracle is available.

    python tests/golden/oracle/build_fixtures.py
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
O = '"ALELTTEST"."ALELT_ORDERS"'
L = '"ALELTTEST"."ALELT_ORDER_LINES"'
TS = {"$ldt": "2026-11-04T08:00:00"}


def r(scn: int, xid: str, op: int, opname: str, *, table: str | None = None, rowid: str | None = None,
      redo: str | None = None, undo: str | None = None, csf: int = 0, rollback: int = 0, rs: str = " 0x000001.0001.0010 ",
      ssn: int = 0, info: str | None = None) -> dict[str, Any]:
    return {"SCN": scn, "XID": xid, "OPERATION_CODE": op, "OPERATION": opname, "SEG_OWNER": "ALELTTEST" if table else None,
            "TABLE_NAME": table, "ROW_ID": rowid, "CSF": csf, "ROLLBACK": rollback, "RS_ID": rs, "SSN": ssn,
            "SQL_REDO": redo, "SQL_UNDO": undo, "TIMESTAMP": TS, "INFO": info}


def build() -> dict[str, Any]:
    A, B, C, D, E, F = ("0A0001000000AAAA", "0A0002000000BBBB", "0A0003000000CCCC", "0A0004000000DDDD",
                        "0A0005000000EEEE", "0A0006000000FFFF")
    big = "x" + "y" * 3989
    ins = (f'insert into {O}("ORDER_ID","CUSTOMER","STATUS","AMOUNT","QTY","BIG","RATE","RATE32","ORDER_DATE",'
           f'"CREATED","EVENT_TS","LOCAL_TS","RAW_COL","BIGTXT","NOTES","DOC") values (\'201\',\'Acmé Ltd ✓ it\'\'s\','
           "'N','1234.5','-7','9007199254740993','1.5E+000','2.5E-001',"
           "TO_DATE('2026-11-04 08:00:00', 'YYYY-MM-DD HH24:MI:SS'),"
           "TO_TIMESTAMP('2026-11-04 08:00:00.123456000'),"
           "TO_TIMESTAMP_TZ('2026-11-04 08:00:00.123000000 -05:00'),"
           "TO_TIMESTAMP('2026-11-04 08:00:00.500000000'),"
           f"HEXTORAW('00ff10'),'{big}',EMPTY_CLOB(),EMPTY_BLOB());")
    cut = 4000                                      # LogMiner splits SQL_REDO into 4000-byte pieces (CSF = 1)
    rows = [
        # A: committed multi-table insert; first INSERT continued over 2 rows (CSF); empty LOBs then LOB writes
        r(100, A, 6, "START"),
        r(101, A, 1, "INSERT", table="ALELT_ORDERS", rowid="AAAR1AAAHAAAACFAAA", redo=ins[:cut], csf=1, ssn=1),
        r(101, A, 1, "INSERT", table="ALELT_ORDERS", rowid="AAAR1AAAHAAAACFAAA", redo=ins[cut:], csf=0, ssn=1),
        r(102, A, 9, "SEL_LOB_LOCATOR", table="ALELT_ORDERS", rowid="AAAR1AAAHAAAACFAAA",
          redo=('DECLARE \n loc_c CLOB; \n buf_c VARCHAR2(6156); \n loc_b BLOB; \n buf_b RAW(6156); \n'
                'loc_nc NCLOB; \n buf_nc NVARCHAR2(6156); \nBEGIN\n select "NOTES" into loc_c from '
                f'{O} where "ORDER_ID" = \'201\' and ROWID = \'AAAR1AAAHAAAACFAAA\' for update;')),
        r(102, A, 10, "LOB_WRITE", table="ALELT_ORDERS", rowid="AAAR1AAAHAAAACFAAA",
          redo=" buf_c := 'short clob'; \n  dbms_lob.write(loc_c, 10, 1, buf_c);\nEND;"),
        r(103, A, 9, "SEL_LOB_LOCATOR", table="ALELT_ORDERS", rowid="AAAR1AAAHAAAACFAAA",
          redo=('DECLARE \n loc_c CLOB; \n buf_c VARCHAR2(6156); \n loc_b BLOB; \n buf_b RAW(6156); \nBEGIN\n'
                f' select "DOC" into loc_b from {O} where "ORDER_ID" = \'201\' and ROWID = '
                "'AAAR1AAAHAAAACFAAA' for update;")),
        r(103, A, 10, "LOB_WRITE", table="ALELT_ORDERS", rowid="AAAR1AAAHAAAACFAAA",
          redo=" buf_b := HEXTORAW('deadbeef'); \n  dbms_lob.write(loc_b, 4, 1, buf_b);\nEND;"),
        r(104, A, 1, "INSERT", table="ALELT_ORDER_LINES", rowid="AAAR2AAAHAAAACNAAA",
          redo=f'insert into {L}("ORDER_ID","LINE_NO","ITEM","QTY") values (\'201\',\'1\',\'TIRE\',\'4\');'),
        r(104, A, 1, "INSERT", table="ALELT_ORDER_LINES", rowid="AAAR2AAAHAAAACNAAB",
          redo=f'insert into {L}("ORDER_ID","LINE_NO","ITEM","QTY") values (\'201\',\'2\',\'OIL\',\'1\');'),
        r(105, A, 7, "COMMIT"),
        # B: rolled back - LogMiner shows the undo rows (ROLLBACK = 1) and a ROLLBACK record: nothing lands
        r(110, B, 6, "START"),
        r(111, B, 1, "INSERT", table="ALELT_ORDERS", rowid="AAAR1AAAHAAAACFAAC",
          redo=f'insert into {O}("ORDER_ID","STATUS","CUSTOMER") values (\'203\',\'R\',\'rolled back\');'),
        r(112, B, 2, "DELETE", table="ALELT_ORDERS", rowid="AAAR1AAAHAAAACFAAC", rollback=1,
          redo=f'delete from {O} where "ORDER_ID" = \'203\' and ROWID = \'AAAR1AAAHAAAACFAAC\';'),
        r(113, B, 36, "ROLLBACK"),
        # C: savepoint - line 4 inserted then undone by a ROLLBACK = 1 row; only line 3 lands
        r(120, C, 6, "START"),
        r(121, C, 1, "INSERT", table="ALELT_ORDER_LINES", rowid="AAAR2AAAHAAAACNAAC",
          redo=f'insert into {L}("ORDER_ID","LINE_NO","ITEM","QTY") values (\'201\',\'3\',\'WIPER\',\'2\');'),
        r(122, C, 1, "INSERT", table="ALELT_ORDER_LINES", rowid="AAAR2AAAHAAAACNAAD",
          redo=f'insert into {L}("ORDER_ID","LINE_NO","ITEM","QTY") values (\'201\',\'4\',\'SAVEPOINT_UNDONE\',\'9\');'),
        r(123, C, 2, "DELETE", table="ALELT_ORDER_LINES", rowid="AAAR2AAAHAAAACNAAD", rollback=1,
          redo=f'delete from {L} where "ORDER_ID" = \'201\' and "LINE_NO" = \'4\' and ROWID = \'AAAR2AAAHAAAACNAAD\';'),
        # D starts while C is open; commits AFTER E (commit order != start order)
        r(124, D, 6, "START"),
        r(125, D, 3, "UPDATE", table="ALELT_ORDER_LINES", rowid="AAAR2AAAHAAAACNAAB",
          redo=(f'update {L} set "LINE_NO" = \'20\' where "ORDER_ID" = \'201\' and "LINE_NO" = \'2\' and '
                "\"ITEM\" = 'OIL' and \"QTY\" = '1' and ROWID = 'AAAR2AAAHAAAACNAAB';"),
          undo=(f'update {L} set "LINE_NO" = \'2\' where "ORDER_ID" = \'201\' and "LINE_NO" = \'20\' and '
                "\"ITEM\" = 'OIL' and \"QTY\" = '1' and ROWID = 'AAAR2AAAHAAAACNAAB';")),
        r(126, C, 7, "COMMIT"),
        # E: non-LOB update (full supplemental logging: all columns in WHERE, LOBs absent => unchanged:)
        r(130, E, 6, "START"),
        r(131, E, 3, "UPDATE", table="ALELT_ORDERS", rowid="AAAR1AAAHAAAACFAAA",
          redo=(f'update {O} set "STATUS" = \'P\', "AMOUNT" = \'99.99\' where "ORDER_ID" = \'201\' and '
                "\"CUSTOMER\" = 'Acmé Ltd ✓ it''s' and \"STATUS\" = 'N' and \"AMOUNT\" = '1234.5' and "
                "ROWID = 'AAAR1AAAHAAAACFAAA';")),
        r(132, E, 255, "UNSUPPORTED", table="ALELT_ORDERS", rowid="AAAR1AAAHAAAACFAAA",
          redo="Unsupported", info="Unsupported (column-level encryption)"),
        r(133, E, 7, "COMMIT"),
        r(134, D, 7, "COMMIT"),
        # F: delete + an unrelated schema (ignored) in the same txn
        r(140, F, 6, "START"),
        r(141, F, 2, "DELETE", table="ALELT_ORDERS", rowid="AAAR1AAAHAAAACFAAB",
          redo=f'delete from {O} where "ORDER_ID" = \'202\' and "STATUS" = \'N\' and ROWID = \'AAAR1AAAHAAAACFAAB\';'),
        dict(r(142, F, 1, "INSERT", table="OTHER_T", redo='insert into "HR"."OTHER_T"("A") values (\'1\');'),
             SEG_OWNER="HR"),
        r(143, F, 7, "COMMIT"),
    ]
    return {"source": "SYNTHETIC fixtures (not recorded from a live Oracle) - see build_fixtures.py", "rows": rows}


if __name__ == "__main__":
    (HERE / "logminer_fixtures.json").write_text(json.dumps(build(), indent=1, ensure_ascii=False), encoding="utf-8")
    print("written", HERE / "logminer_fixtures.json")
