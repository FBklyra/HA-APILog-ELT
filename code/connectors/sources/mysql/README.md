# MySQL / MariaDB source (remote capture, binlog)

Hub-side connector for ALELT: reads the **binary log over the documented replication protocol**
(`COM_REGISTER_SLAVE`, `COM_BINLOG_DUMP` / `COM_BINLOG_DUMP_GTID`, MariaDB `@slave_connect_state`) and turns it
into canonical change events for the generic runner `hub/alelt_capture` (contract: `connectors/sources/_spi/REMOTE.md`).

| Engine | Class | Registration (`ALELT_SOURCES`) |
|---|---|---|
| `mysql` (8.0+, 9.x) | `alelt_src_mysql.connector:MySQLSource` | default |
| `mariadb` (10.3+) | `alelt_src_mariadb.connector:MariaDBSource` (thin variant in `connectors/sources/mariadb`) | default |

PYTHONPATH: `connectors/sources/mysql` (and `connectors/sources/mariadb` for the MariaDB entry point).

## Client library

| Package | License | Use |
|---|---|---|
| `PyMySQL>=1.1` | MIT | handshake, authentication, packet I/O only. Replication commands and every binlog event are decoded here (stdlib). |

Packaging: add `PyMySQL>=1.1` to the image requirements.

## Settings (wizard / `pipeline_definition.source.settings`)

| Key | Default | Meaning |
|---|---|---|
| `host`, `port` | -, 3306 | server |
| `user` + secret `password` | - | capture user |
| `databases` (alias `schemas`, `database`) | - | captured databases (list or comma string) |
| `tables` | all base tables | `db.table, ...` filter; other tables get `capture_enabled = false` |
| `server_id` | derived from the connection id | replica id for the dump (must be unique on the source) |
| `start_pos` | current end | first start only (no durable batch yet), e.g. the position recorded before the initial snapshot |
| `heartbeat_s` | 1.0 | server heartbeat; the stream yields an idle tick at least every second |
| `empty_commit_s` | 30 | at most one empty COMMIT per period for transactions on non-captured tables (position keeps up) |
| `ssl` / `ssl_ca`, `connect_timeout` | off, 10 | transport |

## Positions

`<gtid-state>@<file>:<pos>`: MySQL GTID set (`uuid:1-57`) or MariaDB GTID list (`0-1-57`), then the end offset of
the event. Every `COMMIT` position is a resume token: the GTID state is the set of transactions already delivered,
so a restart asks the server for exactly the others (no gap, no duplicate). Without GTIDs: `@file:pos`.

## What the stream delivers

| Source fact | Event |
|---|---|
| committed transaction | its DML / TRUNCATE / DDL, then `COMMIT` (`commit_ts` = original commit time, UTC) |
| InnoDB `ROLLBACK` | nothing (never written to the binlog) |
| `ROLLBACK TO SAVEPOINT` | undone rows removed in the connector (MySQL already truncates InnoDB caches) |
| `ROLLBACK` of non-transactional work, XA | `ROLLBACK` only |
| transaction cut by a server crash | dropped (the server did not commit it) |
| UPDATE / DELETE | full `before` image (`binlog_row_image = FULL`) |
| value without canonical form (e.g. `TIME '-01:00:00'`), undecodable row, schema drift | quarantine line with the raw event bytes (R7) |

Types: integers incl. unsigned (`bigint unsigned` → `DECIMAL(20,0)`), `DECIMAL`, `FLOAT` (shortest round-trip),
`DOUBLE`, `BIT(1)` → BOOLEAN, `BIT(n)` / binary / blob / spatial → BINARY, character sets → text, `ENUM` / `SET`
labels, `DATE`, `TIME(n)` → LOCALTIME, `DATETIME(n)` → LOCALDATETIME, `TIMESTAMP(n)` → INSTANT (UTC), `YEAR`,
JSON: MariaDB (LONGTEXT + `json_valid`) → JSON text; **MySQL JSON columns are re-read by primary key**
(`needs_keyed_reread`, SQL returns JSON text) - the binary value in row events is not decoded by the core (X-133). An
optional, separately distributed decoder can be plugged in: see [JSON-BINARY-PLUGIN.md](JSON-BINARY-PLUGIN.md).
MariaDB `INET4` / `INET6` → text (network byte order, documented); MariaDB `UUID` → re-read by key.

Completeness (X-132, every binlog event classified in the source census): compressed transaction payloads
(`binlog_transaction_compression = ON`, ZSTD) and MariaDB `log_bin_compress` events are uncompressed and decoded;
tagged GTIDs (`uuid:tag:n`) are decoded and resumed from (tagged COM_BINLOG_DUMP_GTID encoding); row images without
every column (`binlog_row_image = MINIMAL / NOBLOB`) are re-read by key when the destination cannot keep the missing
values (INSERT, key change); TABLE_MAP / catalog drift → catalog refresh + keyed re-read (key from the TABLE_MAP with
`binlog_row_metadata = FULL`) else the table is held; `binlog_row_value_options = PARTIAL_JSON` → table held
(prerequisite FAIL with fix-it SQL); a purged binlog (error 1236) → `BinlogPurgedError` (resync incident, no retry
loop).

## Snapshot

`snapshot_rows()` opens `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY` (REPEATABLE READ) per table and pages
by primary key (keyset). The snapshot's binlog position is recorded in `snapshot_positions[table]`:
MariaDB exact (`binlog_snapshot_file/position` + `BINLOG_GTID_POS`), MySQL a low watermark read just before the
snapshot (the overlap is replayed idempotently with full row images).

## Prerequisites (checked read-only; fix-it SQL shown, never executed)

`log_bin = ON` (restart), `binlog_format = ROW`, `binlog_row_image = FULL`, `server_id <> 0`, checksum CRC32/NONE,
no transaction compression / partial JSON, retention >= 3 days (info), `gtid_mode = ON` (MySQL, recommended),
`binlog_row_metadata = FULL` (optional), privileges `REPLICATION SLAVE` + `REPLICATION CLIENT` (MariaDB:
`BINLOG MONITOR`) + `SELECT` on the captured databases, primary keys and InnoDB (info).


## Tests

| What | Command |
|---|---|

Modules: `position`, `types`, `values`, `jsonhook` (optional JSON decoder hook), `events`, `rows`, `decode` (transaction state machine),
`assemble` (REMOTE.md stream rules), `framing` (corpus format), `catalog`, `protocol`, `connector`.
