# alelt_dst_sql — generic SQL destination (engine `sql`)

Any transactional database as target through a dialect adapter. `alelt_dst_sql:SqlDestination(url, schema, control_schema="alelt_control", output_profile, compare, dialect=None, password=None, system_columns_on=None, golden_dir=None, enforce_nullability=False)`.

| Dialect | URL | Driver (licence) | Upsert | Also covers |
|---|---|---|---|---|
| postgres | `postgresql://u:p@h:5432/db` | psycopg (LGPL, dynamic) | `INSERT … ON CONFLICT DO UPDATE` | Aurora PG, AlloyDB, Cloud SQL, CockroachDB, YugabyteDB |
| mysql | `mysql://u:p@h:3306` · `tidb://…` | PyMySQL (MIT) | multi-row `INSERT … AS alelt_new ON DUPLICATE KEY UPDATE` (TiDB: `VALUES()` form) | MySQL 8.0.19+/9, Aurora MySQL, TiDB |
| mariadb | `mariadb://u:p@h:3306` | PyMySQL (MIT) | multi-row `ON DUPLICATE KEY UPDATE c = VALUES(c)` | MariaDB 10.5+ |
| sqlserver | `mssql://u:p@h:1433/db` | python-tds (MIT) | multi-row `MERGE … WITH (HOLDLOCK) USING (VALUES (CAST(? AS t)…))` | Azure SQL DB/MI, SQL Edge; Fabric Warehouse not supported yet (no enforced PK / MERGE) |
| oracle | `oracle://u:p@h:1521/SERVICE` | python-oracledb thin (Apache-2.0/UPL) | `MERGE … USING (SELECT :1 … FROM dual)` + array DML | 19c+; schema = user (`?create_schema=1` to create) |
| db2 | `db2:///<ALIAS>` (local alias) · `db2://u:p@h:50000/DB` | local Db2 CLI via ctypes (`db2cli.py`) · ibm_db (Apache-2.0) for remote | multi-row `MERGE … USING (VALUES (CAST(? AS t)…))` | Db2 → Db2 replica |
| sqlite | `sqlite:///C:/x/land.db` | sqlite3 (stdlib) | `ON CONFLICT DO UPDATE` | one attached file per schema (`land.<schema>.db`) |
| duckdb | `duckdb:///C:/x/land.duckdb` | duckdb (MIT) | `ON CONFLICT DO UPDATE` | single writing process per file |

Registration: `ALELT_DESTINATION=alelt_dst_sql:SqlDestination`, `ALELT_DEST_URL=<url>` (password may come from `ALELT_DEST_PASSWORD`). The worker's `make_destination(url=, schema=, output_profile=, compare=)` needs no change. URL options: `system_columns=off`, `schema=`, `control_schema=`, `upsert=values`, `driver=ibm_db`, `create_schema=1` (oracle). Wizard: `alelt_dst_sql.catalog()` (fields per dialect), `url_from_fields()`, `GET /wizard/destinations`, `POST /wizard/destination/test` with `engine: sql`.

| Area | Behaviour |
|---|---|
| Landing table | `<schema>.<dest_name>`: source columns typed by `config/type_maps/canonical_to_<dialect>.yaml` (`ALELT_SQL_TYPE_MAP_DIR` overrides) + profile system columns (synced = INSTANT type, deleted = BOOLEAN type); keyless tables: `<id_column>` (32-char surrogate, same algorithm as the loader) as PK. Identifiers folded per dialect (lower: pg/mysql/mariadb/sqlite/duckdb; upper: oracle/db2; as-is: sqlserver) and always quoted. Keys + system columns NOT NULL; data columns nullable unless `enforce_nullability=True`. |
| Golden DDL (R8) | `ensure_table(golden_ddl=…)` or `<golden_dir>/<dialect>/<TABLE>.sql` (`ALELT_SQL_GOLDEN_DIR`), `{{SCHEMA}}`/`{{TABLE}}` placeholders; executed only when the table is absent; missing columns are then added (additive drift). |
| System columns off | Pure mirror: no synced/deleted columns, DELETE = hard delete, TRUNCATE = `DELETE FROM` (transactional). Also `output_profile: none`. |
| apply() | ONE transaction: row-lock the checkpoint (`UPDATE … SET batch_id = batch_id`) → no-op if checkpoint ≥ to_pos → bulk statements per (table, op, column set), flushed when a key repeats (order per key kept) → checkpoint → audit row → COMMIT; any error = rollback. PK update = DELETE old key + upsert new key (loader). Partial images (LOB unavailable) update only present columns. Insert/update counts from a key pre-select. |
| Control | `<control>.checkpoint(stream PK, position, batch_id, updated_at)`, `<control>.audit_batch(id, batch_id, stream, from/to_position, rows_by_table JSON text, total_rows, duration_ms, status, applied_at)`. |
| Verify | `row_count`, `column_aggregates(cols, key_col, key_range)` (count/min/max/sum), `sample_rows(key_cols, n, key_range)`, `describe_schema()`, `fetch_rows`, `audit_rows`, `publish_view`. `key_bucket_hashes` not implemented (optional). |

Value notes: INSTANT stored as UTC without zone on mysql/mariadb/oracle/db2; Oracle LOCALTIME = ISO text, BOOLEAN = number(1), '' = NULL, timestamps bound as TIMESTAMP (the driver default DATE would drop fractions), non-integer NUMBER fetched as Decimal; Db2 TIME keeps whole seconds; SQLite DECIMAL/dates = exact ISO text; JSON = native json/jsonb where available, else text.

Engine limits (operator notes):

| Engine | Limit | What the destination does |
|---|---|---|
| MySQL / MariaDB | InnoDB row limit 65 535 bytes (VARCHAR counted at declared size, utf8mb4 = 4 bytes/char) | non-key VARCHARs become MEDIUMTEXT, largest first, until the row fits (DST-sql-04) |
| MySQL / MariaDB | `INSERT … ON DUPLICATE KEY UPDATE` fires on ANY unique index | landing tables get the primary key only; a golden DDL that adds other UNIQUE indexes can make an upsert update the wrong row - keep extra indexes non-unique (DST-sql-06) |
| Oracle | `''` is NULL; `varchar2(n char)` is still capped at 4000 **bytes** with `MAX_STRING_SIZE = STANDARD` | empty strings land as NULL (documented, parity checks treat them equal); multibyte text over 4000 bytes fails loudly (ORA-12899): use `MAX_STRING_SIZE = EXTENDED` or map long strings to CLOB in the type map (DST-sql-05) |
| SQLite | one writer at a time; ATTACHed per-schema files commit atomically only with the rollback journal on ONE local filesystem | `journal_mode = DELETE`, files next to each other; not for network filesystems, one worker (DST-sql-11) |
| any | DECIMAL above the engine maximum | integer digits kept, the scale gives way (DST-sql-02); Oracle `number` keeps every digit |
| any | checkpoint / audit positions | 4000 characters (Db2: 1000), widened in place on upgrade (DST-sql-03) |

Tests: the dialect test suite is coming in a later release.
