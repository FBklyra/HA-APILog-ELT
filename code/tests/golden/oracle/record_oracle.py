"""Record an Oracle LogMiner golden corpus from the dedicated dev container alelt-test-oracle (schema ALELTTEST only).

    python tests/golden/oracle/record_oracle.py [--dsn localhost:51521/FREEPDB1]

1. (re)creates the ALELTTEST test tables (connectors/sources/oracle/sql/alelttest_schema.sql) when missing,
2. reads CURRENT_SCN, runs alelttest_churn.sql (as ALELTTEST) with a fresh id base,
3. mines [scn, end] with the connector's own Miner as ALELT_CAPTURE (DICT_FROM_ONLINE_CATALOG, uncommitted rows)
   and writes ``logminer_live.json`` (raw V$LOGMNR_CONTENTS rows) + ``expected_live.jsonl`` (canonical events from
   the Assembler, reviewed by hand once).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "hub"), str(ROOT / "connectors" / "sources" / "oracle")]

from alelt_common.canonical_jsonl import encode_events  # noqa: E402
from alelt_src_oracle.assemble import Assembler  # noqa: E402
from alelt_src_oracle.connector import OracleSource  # noqa: E402
from alelt_src_oracle.logminer import Miner  # noqa: E402

HERE = Path(__file__).resolve().parent
SQL = ROOT / "connectors" / "sources" / "oracle" / "sql"
TABLES = "ALELTTEST.ALELT_ORDERS,ALELTTEST.ALELT_ORDER_LINES"


def stmts(text: str) -> list[str]:
    body = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
    return [s.strip() for s in body.split(";") if s.strip()]


def churn_blocks(text: str, base: int) -> list[list[str]]:
    # block separator = a line that is exactly "--/" (the header comment mentions it inline)
    return [stmts(b.replace("{B}", str(base))) for b in re.split(r"(?m)^--/[ 	]*$", text)]


def run_churn(conn: Any, base: int) -> None:
    cur = conn.cursor()
    for block in churn_blocks((SQL / "alelttest_churn.sql").read_text(encoding="utf-8"), base):
        for s in block:
            u = s.upper()
            if u == "COMMIT":
                conn.commit()
            elif u == "ROLLBACK":
                conn.rollback()
            else:
                cur.execute(s)


def ensure_schema(conn: Any) -> None:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = 'ALELT_ORDERS'")
    if cur.fetchone()[0]:
        return
    for s in stmts((SQL / "alelttest_schema.sql").read_text(encoding="utf-8")):
        cur.execute(s)


def enc(v: Any) -> Any:
    if isinstance(v, datetime):
        return {"$ldt": v.isoformat()}
    return v


def main() -> int:
    import oracledb

    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="localhost:51521/FREEPDB1")
    ap.add_argument("--out", default=str(HERE))
    a = ap.parse_args()
    owner = oracledb.connect(user="ALELTTEST", password="alelttest", dsn=a.dsn, tcp_connect_timeout=15)
    ensure_schema(owner)
    src = OracleSource(settings={"dsn": a.dsn, "user": "ALELT_CAPTURE", "schemas": "ALELTTEST", "tables": TABLES},
                       password="aleltcapture", connection_id="golden")
    cat = src.read_catalog()
    conn = src._connect()
    miner = Miner(conn, ["ALELTTEST"], flush_lag_s=0)
    start = miner.current_scn()
    cur = owner.cursor()
    cur.execute("SELECT NVL(MAX(ORDER_ID), 0) FROM ALELT_ORDERS")
    base = (int(cur.fetchone()[0]) // 100 + 1) * 100
    run_churn(owner, base)
    time.sleep(4)                                  # LGWR flush (>= 3 s)
    end = miner.current_scn()
    rows = list(miner.window(start, end))
    asm = Assembler(src._tables or {}, ["ALELTTEST"], db_tz=miner.db_tz, empty_commit_every_s=3600)
    events = []
    for r in rows:
        events += asm.feed(r)
    events += asm.flush()
    out = Path(a.out)
    doc = {"source": "recorded live from Oracle Database Free (alelt-test-oracle), per-PDB LogMiner, "
                     "DICT_FROM_ONLINE_CATALOG, uncommitted rows",
           "in_pdb": miner.in_pdb, "db_tz": str(miner.db_tz), "base": base, "start_scn": start, "end_scn": end,
           "catalog": json.loads(cat.model_dump_json()),
           "rows": [{k: enc(v) for k, v in r.items()} for r in rows]}
    (out / "logminer_live.json").write_text(json.dumps(doc, indent=1, ensure_ascii=False), encoding="utf-8")
    (out / "expected_live.jsonl").write_bytes(encode_events(events))
    print(f"recorded {len(rows)} rows, {len(events)} events, stats={asm.stats}")
    src.close()
    owner.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
