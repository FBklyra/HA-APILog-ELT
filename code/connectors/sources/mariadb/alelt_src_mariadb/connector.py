"""MariaDB source connector (engine 'mariadb'). The implementation lives in alelt_src_mysql (shared binlog core);
this package only gives MariaDB its own registration point: ALELT_SOURCES {"mariadb": "alelt_src_mariadb.connector:MariaDBSource"}.

MariaDB differences handled by the shared core (flavor 'mariadb'): GTID list positions 'domain-server-seq' with
@slave_connect_state, MariaDB GTID / GTID_LIST / ANNOTATE_ROWS events, no binlog row metadata before 10.5 (names and
types from the catalog), JSON as LONGTEXT + json_valid CHECK, exact snapshot position (binlog_snapshot_file/position
+ BINLOG_GTID_POS), privileges BINLOG MONITOR / REPLICATION SLAVE. Needs connectors/sources/mysql on PYTHONPATH.
"""
from __future__ import annotations

import sys
from pathlib import Path

try:
    from alelt_src_mysql.connector import MariaDBSource
except ImportError:                      # repo layout: sibling connector folder
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "mysql"))
    from alelt_src_mysql.connector import MariaDBSource

__all__ = ["MariaDBSource"]
