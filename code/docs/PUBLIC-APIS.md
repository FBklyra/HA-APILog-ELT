# Only public log APIs

> **ALELT reads changes only through each database vendor's official, publicly documented log / change API, and writes only through each destination's public, documented API.**
> No undocumented log-file parsing. No reverse engineering. No vendor code or headers included. Vendor client libraries are obtained by the user under the vendor's own licence.

## Statements

| # | Statement |
|---|---|
| 1 | Every source connector asks the **database server** for changes through a documented API; the server reads its own log. ALELT never opens, lists, copies, maps or parses database log, archive, redo, WAL or binlog **files**. |
| 2 | Built solely from vendors' **public documentation** and public product information ([`legal/PROVENANCE.md`](legal/PROVENANCE.md)). |
| 3 | **No reverse engineering**: no decompiled, disassembled, debugged or traced vendor binaries; no undocumented interfaces or files (data is read only through the documented API); no NDA or confidential material. Where the documentation does not spell out every byte of a record the documented API returns, the layout was observed on our own test databases and is listed per field in [PROVENANCE.md](legal/PROVENANCE.md#per-field-provenance-of-the-db2-log-layouts-f-25). |
| 4 | **No vendor code or headers** are included in the repository, images or release assets. |
| 5 | Vendor client libraries / drivers (e.g. IBM Db2 client, Oracle client) are **obtained by the user** from the vendor and used under the vendor's licence. Open-source drivers are used under their own licences (see the release SBOM). |
| 6 | Not affiliated with or endorsed by any vendor; trademarks belong to their owners ([`NOTICE`](../NOTICE)). |
| 7 | Enforced by an automated guard test: [`tests/static/test_public_api_only.py`](../tests/static/test_public_api_only.py) fails the build if connector, hub or agent code opens database log / archive files directly (`S*.LOG`, redo `.log/.arc/.dbf`, `.ldf/.mdf`, `pg_wal`, binlog files…). |

## Sources

| Engine | Public API used | What the server returns | Official documentation |
|---|---|---|---|
| IBM Db2 LUW | `db2ReadLog` administrative API (via Db2 CLI connection), filter on propagatable records | log records of tables with `DATA CAPTURE CHANGES`, in the documented record formats | [db2ReadLog API](https://www.ibm.com/docs/en/db2/11.5?topic=apis-db2readlog-read-log-records) · [Log records](https://www.ibm.com/docs/en/db2/11.5?topic=apis-db2-log-records) · [DATA CAPTURE](https://www.ibm.com/docs/en/db2/11.5?topic=statements-create-table) |
| PostgreSQL | Logical decoding over the streaming replication protocol, output plugin `pgoutput` (publications, replication slot) | logical replication messages (Begin, Relation, Insert, Update, Delete, Truncate, Commit) | [Logical decoding](https://www.postgresql.org/docs/current/logicaldecoding.html) · [Logical replication protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html) · [Message formats](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html) |
| MySQL | Replication protocol: `COM_REGISTER_SLAVE`, `COM_BINLOG_DUMP(_GTID)` with `binlog_format=ROW` | binlog events (FORMAT_DESCRIPTION, TABLE_MAP, WRITE/UPDATE/DELETE_ROWS, XID, GTID) streamed by the server | [Replication protocol](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication.html) · [Binlog events](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html) |
| MariaDB | Replication protocol (`COM_BINLOG_DUMP`, MariaDB GTID) | binlog events streamed by the server | [Replication protocol](https://mariadb.com/kb/en/replication-protocol/) · [Binlog event header](https://mariadb.com/kb/en/2-binlog-event-header/) |
| Oracle Database | LogMiner: `DBMS_LOGMNR.ADD_LOGFILE` / `START_LOGMNR`, query `V$LOGMNR_CONTENTS` | `SQL_REDO` / `SQL_UNDO`, SCN, XID — produced by the database server | [LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html) · [DBMS_LOGMNR](https://docs.oracle.com/en/database/oracle/oracle-database/23/arpls/DBMS_LOGMNR.html) |
| Microsoft SQL Server | Change Data Capture: `sys.sp_cdc_enable_table`, table functions `cdc.fn_cdc_get_all_changes_<capture_instance>`, `sys.fn_cdc_get_min_lsn / max_lsn`, `cdc.lsn_time_mapping` | change rows with `__$start_lsn`, `__$seqval`, `__$operation` | [About CDC](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server) · [fn_cdc_get_all_changes](https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql) |

Notes:

- Oracle `ADD_LOGFILE` passes a log **name** to the database server, which reads it; ALELT never opens it.
- MySQL JSON columns: the binary JSON value in row events is **not** decoded (its format is not publicly documented,
  X-133); the row is re-read by primary key with a plain `SELECT` (`needs_keyed_reread`). Optional decoder hook:
  `connectors/sources/mysql/JSON-BINARY-PLUGIN.md` (separately licensed plugins only).
- Every source classifies every record it reads (decoded / ignored / needs_keyed_reread / needs_catalog_refresh /
  unsupported, `alelt_common/completeness.py`); undocumented forms are re-read by key with `SELECT`, never guessed.
- Snapshots (initial load, re-sync) use ordinary `SELECT` statements with documented isolation (e.g. Db2 `WITH UR`, PostgreSQL exported snapshot, MySQL `START TRANSACTION WITH CONSISTENT SNAPSHOT`, Oracle `AS OF SCN`, SQL Server `SNAPSHOT`).
- Catalog reads use the documented catalog views (`SYSCAT.*`, `pg_catalog`, `information_schema`, `ALL_TAB_COLUMNS`, `sys.*`).

## Destinations

| Destination | Public API used | Official documentation |
|---|---|---|
| PostgreSQL | SQL over the libpq protocol (`INSERT … ON CONFLICT`, transactions) | [INSERT](https://www.postgresql.org/docs/current/sql-insert.html) |
| MySQL / MariaDB | SQL (`INSERT … ON DUPLICATE KEY UPDATE`, transactions) | [MySQL INSERT](https://dev.mysql.com/doc/refman/9.3/en/insert-on-duplicate.html) · [MariaDB INSERT](https://mariadb.com/kb/en/insert-on-duplicate-key-update/) |
| SQL Server / Fabric Warehouse | T-SQL over TDS (`MERGE`, transactions) | [MERGE](https://learn.microsoft.com/en-us/sql/t-sql/statements/merge-transact-sql) · [Fabric connectivity](https://learn.microsoft.com/en-us/fabric/data-warehouse/connectivity) |
| Oracle | SQL (`MERGE`) | [MERGE](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/MERGE.html) |
| IBM Db2 | SQL (`MERGE`) | [MERGE](https://www.ibm.com/docs/en/db2/11.5?topic=statements-merge) |
| SQLite / DuckDB | SQL (`INSERT … ON CONFLICT`) | [SQLite UPSERT](https://www.sqlite.org/lang_upsert.html) · [DuckDB INSERT](https://duckdb.org/docs/stable/sql/statements/insert) |
| Snowflake | SQL: `PUT`, `COPY INTO`, `MERGE`, key-pair authentication | [COPY INTO](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table) · [MERGE](https://docs.snowflake.com/en/sql-reference/sql/merge) · [Key-pair auth](https://docs.snowflake.com/en/user-guide/key-pair-auth) |
| ClickHouse | HTTP interface + SQL | [HTTP interface](https://clickhouse.com/docs/interfaces/http) |
| Apache Iceberg | Iceberg table spec + REST catalog OpenAPI, S3 API | [Table spec](https://iceberg.apache.org/spec/) · [REST catalog](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) |
| Delta Lake | Delta transaction-log protocol (open specification) | [Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) |
| Databricks | SQL Statement Execution API / Unity Catalog | [Statement execution](https://docs.databricks.com/api/workspace/statementexecution) |
| Kafka API (e.g. Redpanda) | Kafka wire protocol (producer, transactions) | [Kafka protocol](https://kafka.apache.org/protocol) |
| Google BigQuery | BigQuery REST / Storage Write API | [BigQuery API](https://cloud.google.com/bigquery/docs/reference/rest) |
| Amazon Redshift | SQL (`COPY`, `MERGE`) | [COPY](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) |
| S3-compatible object storage (hub spool) | S3 REST API with SigV4 | [Amazon S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) |

Connector maturity (stable / beta / planned): see the matrix in the [README](../README.md#supported-sources-and-destinations).

## Contributor rule

A connector that needs anything not covered by public documentation is not accepted. Record every documentation source in [`legal/PROVENANCE.md`](legal/PROVENANCE.md) in the same pull request.
