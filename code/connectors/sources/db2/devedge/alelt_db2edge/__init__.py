"""Reusable Db2 LUW edge capture library (Python stdlib only, Windows + Linux).

Used by the dev edge (`devedge.py`) and the Linux agent daemon (`edge-agent/linux/aleltd`):
  db2      Db2 CLI connection + db2ReadLog (QUERY / READ filter ON), LRI helpers, batch builder
  catalog  SYSCAT catalog snapshot (WITH UR), same shape as alelt_src_db2's catalog dict
  verify   VERIFY_TABLE (F-03): the C plugin's statements (SELECT ... WITH CS, parameter markers)

Read-only on Db2 (R1): SELECT ... WITH UR on SYSCAT/SYSIBMADM, SELECT ... WITH CS on allow-listed tables
(VERIFY_TABLE) and db2ReadLog only.
"""
from .catalog import read_catalog
from .verify import snapshot_chunks, table_allowed, valid_allow_entry, verify_table
from .db2 import (
    FILTER_HDR,
    RL_READ_TO_CURRENT,
    Db2,
    Db2Error,
    lri_key,
    make_batch,
    record_lris,
)

__all__ = ["Db2", "Db2Error", "FILTER_HDR", "RL_READ_TO_CURRENT", "lri_key", "make_batch", "read_catalog",
           "record_lris", "snapshot_chunks", "table_allowed", "valid_allow_entry", "verify_table"]
