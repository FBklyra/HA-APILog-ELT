# connectors/sources/db2/edge — Db2 LUW source plugin (`aleltsrc_db2.dll`)

Implements the agent plugin ABI `alelt_source_vtbl` (`edge-agent/core/include/alelt_plugin.h`) for
IBM Db2 LUW 11.5 on Windows, using only the documented Db2 CLI and `db2ReadLog` APIs.
IBM headers and import libraries are taken from the installed client (`DB2_HOME`), never vendored.

## Build / test

Built as part of `edge-agent/core` (CMake option `ALELT_BUILD_DB2_PLUGIN`, default ON):

```bat
cd edge-agent\core
cmake --preset x64-release -DDB2_HOME=D:/IBM/SQLLIB
cmake --build --preset x64-release
ctest --preset x64-release -R db2       :: LRI, framing, R1 SQL-table checks (no database needed)
```

Imports only `db2app64.dll` (CLI + admin API) from `%DB2PATH%\BIN` — list it in `source.dll_dirs`.
Install: `aleltsrc_db2.dll` sits next to `aleltagent.exe` (the agent loads plugins from its own folder only).
Run: `aleltagent.exe --console` with `source.plugin = "aleltsrc_db2.dll"` (see `edge-agent/core/README.md`).

## Behaviour

| Capability | Implementation |
|---|---|
| connect | `SQLConnect(<alias>)` with implicit OS authentication (service account; no password in config). `READ ONLY` access mode, CS isolation by default (every statement names its own), autocommit on, application name `ALELTAgent`. |
| `probe_position` | `COMMIT` → `db2ReadLog(QUERY)` → `nextStartLRI`. |
| `read` | `COMMIT` → `db2ReadLog(READ, start, end=(1,FFFF…,FFFF…), FILTER_ON)` into the agent buffer; framing validated (56-byte filter header + record length, exact byte count) before anything is spooled; sqlcode 2654 ⇒ `at_end`. |
| position text | type-1 LRI `"<part1 decimal>:<part2 16 upper-case hex>"`, ordered by (part1, part2). |
| `catalog_json` | `SYSCAT.TABLES`/`COLUMNS`/`SCHEMATA` + `SYSIBMADM.DBCFG` `WITH UR`; per-table `engine_ids {tbspaceid, tableid}` (V6), `capture_enabled` (DATACAPTURE Y/L), compression, columns with canonical types; `position` queried after the reads. |
| `snapshot_chunk` | table must be in the agent's local allow-list **and** exist as a base table with a primary key; `SELECT <cols> FROM "S"."T" WHERE <keyset after/to> ORDER BY <pk> FETCH FIRST n ROWS ONLY WITH CS` (F-04: no dirty reads; a rolled-back insert/update is never loaded) with quoted identifiers and parameter markers; values fetched as UTF-16 → UTF-8. |
| `verify_table` (ABI 2, `VERIFY_TABLE`, F-03) | allow-listed table; args re-validated (whitelist = `alelt_common.models.VerifyTableArgs`); per key range `SELECT COUNT(*), COUNT/MIN/MAX/SUM(CAST .. DECIMAL(31,0)) / SUM(CHARACTER_LENGTH(RTRIM(..),CODEUNITS32)) ... WITH CS` (column groups <= 900 items), keys of a range (`FETCH FIRST max_keys+1`), keyed re-read `WHERE k1 = ? AND ... WITH CS` (a key that cannot convert to the key type = missing); statement timeout, 8 MiB result cap (`truncated`), `position_before/after`. Builders in `src/db2_build.c` (pure C, unit-tested statement text). |
| probes | `current_position`, `db_health` (`MON_TRANSACTION_LOG_UTILIZATION`, `MON_GET_DATABASE`), `log_config` (`DBCFG`, incl. `cur_commit`). |

## Safety (R1, R3)

* The only SQL is the compiled-in catalog table in `src/db2_sql.c` (all `SELECT … WITH UR` on SYSCAT /
  SYSIBMADM, parameter markers) and the data builders of `src/db2_build.c` (fixed fragments +
  catalog-validated quoted identifiers, always `… WITH CS`, shape re-checked before `SQLPrepare`).
  `tests/test_db2.c` fails the build if any statement is not a read-only SELECT, if a data statement
  names UR, if `SQLExecDirect` appears, or if a statement is prepared from anywhere else.
* Prerequisite: `CUR_COMMIT ON` (Db2 default for new databases; check `db2 get db cfg for <DB> | findstr
  CUR_COMMIT`, reported by `PROBE log_config` and the console prerequisite checklist). Without it CS
  readers wait on uncommitted writers (still never dirty).
* No archive log file is ever opened; reading goes through `db2ReadLog` only.
* Needs DBADM (or SYSADM) for `db2ReadLog` — see `runbooks/agent-install.md`.

Finding: the native CLI (`db2cli.lib`) uses **32-bit SQLLEN on Win64**; do not define `ODBC64`/`CLI_WIN64`
(the source refuses to compile if they are set).
