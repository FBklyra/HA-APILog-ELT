"""Record a SQL Server CDC golden corpus from a dev server (database alelttest_cdc ONLY).

    python tests/golden/sqlserver/record_sqlserver.py --host localhost --password-env ALELT_SA_PW [--dev-manual-scan]

1. applies connectors/sources/sqlserver/sql/alelttest_schema.sql (creates + CDC-enables the test tables),
2. reads the current max LSN, runs alelttest_churn.sql with a fresh id base,
3. reads the change window with the connector's own reader (``fn_cdc_get_all_changes_<ci>(.., 'all update old')``,
   ``cdc.lsn_time_mapping``) and writes ``cdc_window.json``: catalog + capture instances + raw change rows,
4. writes ``expected.jsonl`` = canonical events produced by ``assemble`` (reviewed by hand once; the unit test
   compares future decodes with it).

Refuses any database other than alelttest_cdc. Dev / test only (``--dev-manual-scan`` calls sys.sp_cdc_scan).
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import time
import uuid
from datetime import date, datetime, time as dtime
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "hub"), str(ROOT / "connectors" / "sources" / "sqlserver")]

from alelt_common.canonical_jsonl import encode_events  # noqa: E402
from alelt_src_sqlserver.cdc import CdcReader  # noqa: E402
from alelt_src_sqlserver.assemble import assemble  # noqa: E402
from alelt_src_sqlserver.connector import SqlServerSource  # noqa: E402

HERE = Path(__file__).resolve().parent
SQL = ROOT / "connectors" / "sources" / "sqlserver" / "sql"
DB = "alelttest_cdc"


def enc(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return {"$f": repr(v)}
    if isinstance(v, Decimal):
        return {"$dec": str(v)}
    if isinstance(v, (bytes, bytearray)):
        return {"$bin": base64.b64encode(bytes(v)).decode()}
    if isinstance(v, uuid.UUID):
        return {"$uuid": str(v)}
    if isinstance(v, datetime):
        return {"$ts" if v.tzinfo else "$ldt": v.isoformat()}
    if isinstance(v, date):
        return {"$date": v.isoformat()}
    if isinstance(v, dtime):
        return {"$time": v.isoformat()}
    raise TypeError(type(v))


def batches(text: str) -> list[str]:
    return [b.strip() for b in re.split(r"^\s*GO\s*$", text, flags=re.M | re.I) if b.strip()]


def run_sql(conn: Any, text: str) -> None:
    cur = conn.cursor()
    for b in batches(text):
        cur.execute(b)
        try:
            while cur.nextset():
                pass
        except Exception:  # noqa: BLE001
            pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=51433)  # alelt-test-sqledge
    ap.add_argument("--user", default="sa")
    ap.add_argument("--password-env", default="ALELT_SA_PW")
    ap.add_argument("--dev-manual-scan", action="store_true")
    ap.add_argument("--out", default=str(HERE))
    a = ap.parse_args()
    st = {"host": a.host, "port": a.port, "database": DB, "user": a.user, "schemas": "dbo",
          "tables": "dbo.ALELT_ORDERS,dbo.ALELT_ORDER_LINES,dbo.ALELT_NOT_CAPTURED", "dev_manual_scan": a.dev_manual_scan}
    src = SqlServerSource(settings=st, password=os.environ[a.password_env], connection_id="golden")
    conn = src._connect()
    cur = conn.cursor()
    cur.execute("SELECT DB_NAME()")
    if cur.fetchone()[0] != DB:
        raise SystemExit("refusing: not connected to alelttest_cdc")
    run_sql(conn, (SQL / "alelttest_schema.sql").read_text(encoding="utf-8"))
    cat = src.read_catalog()
    rd = CdcReader(conn, src._instances or {}, dev_manual_scan=a.dev_manual_scan)
    if a.dev_manual_scan:
        rd.scan()
    start = rd.max_lsn()
    cur = conn.cursor()
    cur.execute("SELECT ISNULL(MAX(ORDER_ID), 0) FROM dbo.ALELT_ORDERS")
    base = (int(cur.fetchone()[0]) // 100 + 1) * 100
    run_sql(conn, (SQL / "alelttest_churn.sql").read_text(encoding="utf-8").replace("{B}", str(base)))
    rows: list[dict[str, Any]] = []
    meta: dict[bytes, Any] = {}
    end = start
    for _ in range(20):                      # capture is asynchronous: read until the churn is harvested
        if a.dev_manual_scan:
            rd.scan()
        e = rd.window_end(end, rd.max_lsn())
        if e is not None:
            rows += rd.rows(end, e)
            meta.update(rd.txn_meta(end, e))
            end = e
        if len({bytes(r["__$start_lsn"]) for r in rows}) >= 7:
            break
        time.sleep(1)
    events = assemble(rows, rd.instances, meta, rd.server_tz)
    out = Path(a.out)
    doc = {
        "server": "recorded from a dev SQL Server (database alelttest_cdc)",
        "server_offset_min": int(rd.server_tz.utcoffset(None).total_seconds() // 60),  # type: ignore[union-attr]
        "base": base, "after_lsn": start.hex().upper(), "end_lsn": end.hex().upper(),
        "catalog": json.loads(cat.model_dump_json()) if hasattr(cat, "model_dump_json") else None,
        "instances": {n: {"table": f"{ci.table.table.schema_}.{ci.table.table.name}", "captured": ci.captured,
                          "lob_cols": sorted(ci.lob_cols)} for n, ci in rd.instances.items()},
        "meta": [{"start_lsn": k.hex().upper(), "tran_id": m.tran_id.hex().upper() if m.tran_id else None,
                  "begin": enc(m.begin), "end": enc(m.end)} for k, m in sorted(meta.items())],
        "rows": [{k: enc(v) for k, v in r.items()} for r in rows],
    }
    (out / "cdc_window.json").write_text(json.dumps(doc, indent=1, ensure_ascii=False), encoding="utf-8")
    (out / "expected.jsonl").write_bytes(encode_events(events))
    print(f"recorded {len(rows)} rows, {len(meta)} txns, {len(events)} events base={base}")
    src.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
