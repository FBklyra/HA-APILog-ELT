# Oracle source (LogMiner) — `alelt_src_oracle`

Remote-capture connector (contract: [`../_spi/REMOTE.md`](../_spi/REMOTE.md)). Reads redo through the documented
**LogMiner** API (`DBMS_LOGMNR`, `V$LOGMNR_CONTENTS`) only; never parses redo files itself.

| Item | Value |
|---|---|
| Class | `alelt_src_oracle.connector:OracleSource` (registered as `oracle` in `ALELT_SOURCES`) |
| Client library | `oracledb` (`pip install oracledb`, Apache-2.0 / UPL), **thin mode** — no Oracle client install |
| Versions | 19c+ (non-CDB, or CDB with a common user); 21c+ per-PDB LogMiner (no `ADD_LOGFILE` inside a PDB) |
| Position | `<scn>[.<xid>][~<restart_scn>]`; commit token = commit SCN + XID + restart SCN (oldest open txn) (`position.py`) |

## How capture works

| Step | Module |
|---|---|
| window `[start, end]`, end ≤ *safe SCN* (an SCN seen ≥ `flush_lag_s` ago: redo on disk) | `logminer.Miner` |
| logs: archived (`V$ARCHIVED_LOG`) + online (`V$LOG`/`V$LOGFILE`), sequence-gap check ⇒ `LogNotAvailable` | `logminer.logs_for` |
| `START_LOGMNR(STARTSCN, ENDSCN, DICT_FROM_ONLINE_CATALOG)` — **uncommitted** rows (see `assemble.py` docstring for why not `COMMITTED_DATA_ONLY`) | `logminer.window` |
| SQL_REDO/SQL_UNDO → values (literals, `TO_DATE`/`TO_TIMESTAMP(_TZ)`/`HEXTORAW`/`UNISTR`/…, LOB PL/SQL blocks) | `redosql.py`, `types.py` |
| XID buffering, CSF continuation, `ROLLBACK=1` savepoint compensation by ROW_ID, rollback dropped, LOB writes folded into the row, whole txns in commit order | `assemble.Assembler` |
| unresolved object / UNSUPPORTED / XML-binary / broken CSF ⇒ `OTHER` event carrying an `Undecodable` value ⇒ quarantined at decode with raw bytes (R7); `MISSING_SCN` ⇒ raise | `assemble.py` |
| initial load: `AS OF SCN` PK-ordered chunks; start position = current SCN + oldest open txn | `catalog.py` |

## Settings

`host`, `port` (1521), `service_name` (or `dsn`), `user` (+ password secret), `schemas`, `tables`, `poll_s` (1.0),
`flush_lag_s` (5.0), `window_scns` (50000), `empty_commit_every_s` (5.0), `connect_timeout_s` (15), `call_timeout_s` (300).

## Prerequisites (fix-it SQL shown, never executed)

ARCHIVELOG; `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA`; `(ALL) COLUMNS` supplemental logging per captured table;
capture user: `CREATE SESSION`, `LOGMINING`, `SELECT ANY TRANSACTION`, `SELECT_CATALOG_ROLE`, `EXECUTE ON DBMS_LOGMNR`,
`SELECT` + `FLASHBACK` on the tables; archived-log retention ≥ the longest hub outage.

## Tests

| What | Command | Evidence |
|---|---|---|
| unit + fixtures | `python -m pytest connectors/sources/oracle/tests` | `tests/golden/oracle/logminer_fixtures.json` is **synthetic** (`build_fixtures.py`); `logminer_live.json` + `expected_live.jsonl` are **recorded live** (2026-11-04, Oracle Database 23ai Free) |
| live recording | `python tests/golden/oracle/record_oracle.py` against `alelt-test-oracle` (`gvenzl/oracle-free:23-slim`, host port 51521, setup `sql/alelttest_setup_sysdba.sql` once as SYSDBA, then `sql/alelttest_schema.sql`) | `logminer_live.json` + `expected_live.jsonl` |

Live findings (23ai) handled: `TIMESTAMP WITH LOCAL TIME ZONE` printed as `TO_TIMESTAMP_TZ('<session-tz time>')` without
offset (session TIME_ZONE = UTC); `INTERNAL` rows inside SEL_LOB_LOCATOR/LOB_WRITE/LOB_TRIM sequences; INSERT into a
LOB table has a zero ROW_ID + a LOB UPDATE with the real one, undone by ONE delete compensation; row changes of
tables without (ALL) COLUMNS logging / outside `tables` are not delivered; AS OF SCN right after table DDL raises
ORA-01466 (prepare tables before the initial snapshot).

Test schema / churn: [`sql/alelttest_schema.sql`](sql/alelttest_schema.sql), [`sql/alelttest_churn.sql`](sql/alelttest_churn.sql).
