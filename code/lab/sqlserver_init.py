# SPDX-License-Identifier: Apache-2.0
"""Run lab/init/sqlserver/10-shop.sql from the host (pyodbc) - for SQL Server images without sqlcmd inside
(e.g. the azure-sql-edge local fallback on ARM64 hosts). Idempotent, same script as the in-container path.

  python lab/sqlserver_init.py [--port 47143]
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "init/sqlserver/10-shop.sql"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=int(os.environ.get("LAB_PORT_SQLSERVER", "47143")))
    ap.add_argument("--timeout", type=int, default=180)
    a = ap.parse_args()
    import pyodbc

    drv = next((d for d in ("ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server") if d in pyodbc.drivers()),
               None)
    if drv is None:
        print("[lab] sqlserver init: no Microsoft ODBC driver 17/18 on this host", file=sys.stderr)
        return 1
    cs = (f"DRIVER={{{drv}}};SERVER=127.0.0.1,{a.port};UID=sa;PWD=Lab_local_only1;"
          "Encrypt=no;TrustServerCertificate=yes")
    end = time.monotonic() + a.timeout
    while True:                                   # engine accepts TCP before logins work
        try:
            cn = pyodbc.connect(cs, autocommit=True, timeout=10)
            break
        except pyodbc.Error:
            if time.monotonic() > end:
                raise
            time.sleep(3)
    for chunk in re.split(r"^\s*GO\s*$", SCRIPT.read_text(encoding="utf-8"), flags=re.M | re.I):
        if chunk.strip():
            cur = cn.execute(chunk)
            while cur.nextset():
                pass
    print("[lab] sqlserver init OK (labdb.shop + CDC)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
