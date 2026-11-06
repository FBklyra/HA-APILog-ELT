# SQL Server source (CDC) — `alelt_src_sqlserver`

Remote-capture connector (contract: [`../_spi/REMOTE.md`](../_spi/REMOTE.md)). Reads SQL Server **Change Data
Capture** through its documented functions only; never parses the transaction log itself.

| Item | Value |
|---|---|
| Class | `alelt_src_sqlserver.connector:SqlServerSource` (registered as `sqlserver` in `ALELT_SOURCES`) |
| Client library | `python-tds` (`pip install python-tds`, MIT, pure Python) |
| Versions | SQL Server 2016+ (any edition with CDC), Azure SQL Managed Instance; Azure SQL Edge for dev only (no Agent) |
| Position | `<commit_lsn hex20>:<seqval hex20>`; commit token `<commit_lsn>:FFFFFFFFFFFFFFFFFFFF` (`position.py`) |

## How capture works

| Step | Interface |
|---|---|
| end of captured log | `sys.fn_cdc_get_max_lsn()` |
| window `(after, end]` of ≤ `window_txns` transactions | `cdc.lsn_time_mapping` (`start_lsn` = commit LSN; also `tran_id`, `tran_end_time` → `commit_ts`) |
| changes | `cdc.fn_cdc_get_all_changes_<capture_instance>(from, to, N'all update old')` for every active instance |
| retention | `sys.fn_cdc_get_min_lsn(ci)`: resume older than it (instance already existing then) ⇒ `RetentionGapError` raised on every (re)start (runner state RETRY with the reason) until the tables are resynced — never a silent skip |
| initial load | `snapshot_rows`: PK-ordered chunks in one `SNAPSHOT` transaction (READ COMMITTED + warning when `ALLOW_SNAPSHOT_ISOLATION` is OFF); start position = max LSN read before, overlap replayed idempotently |

CDC contains committed changes only, so rolled-back transactions and savepoint rollbacks never appear. Rows of all
instances are grouped by `__$start_lsn` (one transaction), ordered by `__$seqval`; operation 3+4 are merged into one
UPDATE with full before/after images. A PK update arrives as DELETE + INSERT. LOB columns (`(n)varchar(max)`,
`varbinary(max)`, `text`, `ntext`, `image`, `xml`) not set in `__$update_mask` are omitted and noted
`unchanged:<col>`; table columns not in the capture instance likewise. Unconvertible values are sent as `Undecodable`
⇒ the line is quarantined at decode with its raw bytes (R7). `TRUNCATE` is refused by SQL Server on CDC tables.

## Settings

| Key | Default | Meaning |
|---|---|---|
| `host`, `port`, `database`, `user` (+ password secret) | —, 1433 | connection |
| `schemas`, `tables` | `dbo`, all | catalog scope (`SCHEMA.TABLE` list) |
| `poll_s` | 1.0 | idle poll interval (idle tick `None` at least every second) |
| `window_txns` | 5000 | max transactions per read window |
| `server_tz` | server's current offset | IANA zone for `lsn_time_mapping` times (set it when the server observes DST) |
| `connect_timeout_s`, `call_timeout_s` | 15, 300 | timeouts |
| `dev_manual_scan` | false | **DEV / TEST ONLY.** Calls `sys.sp_cdc_scan` before each read, for servers where SQL Server Agent does not run (Azure SQL Edge). Never on a production source: it competes with the capture job and needs `db_owner`. |

## Prerequisites (shown with fix-it SQL; ALELT never executes it)

`sys.sp_cdc_enable_db`; `sys.sp_cdc_enable_table` per table; **SQL Server Agent running** (FAIL when stopped, e.g.
Azure SQL Edge); capture job present; capture user with SELECT on the tables and the `cdc` schema (and gating role
membership); `ALLOW_SNAPSHOT_ISOLATION ON`; cleanup retention ≥ the longest hub outage; resume position still retained.

## Tests

| What | Command |
|---|---|
| unit + golden (no server) | `python -m pytest connectors/sources/sqlserver/tests` |
| re-record golden corpus (dev server, `alelttest_cdc` only) | `python tests/golden/sqlserver/record_sqlserver.py --dev-manual-scan` (password in `ALELT_SA_PW`) |

Test schema / churn: [`sql/alelttest_schema.sql`](sql/alelttest_schema.sql), [`sql/alelttest_churn.sql`](sql/alelttest_churn.sql).
