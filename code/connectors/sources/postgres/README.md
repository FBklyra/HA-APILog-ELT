# PostgreSQL source (`alelt_src_postgres`) — remote capture

Logical decoding with the built-in `pgoutput` plugin over the streaming replication protocol (both public and
documented in the PostgreSQL manual). Captured from the hub by `hub/alelt_capture` (contract: `../_spi/REMOTE.md`).

| Item | Detail |
|---|---|
| Registration | `ALELT_SOURCES` default `"postgres": "alelt_src_postgres.connector:PostgresSource"`; PYTHONPATH `connectors/sources/postgres` |
| Client | `psycopg2-binary>=2.9.9` (LGPL-3.0 with linking exception, dynamic library) |
| Server | PostgreSQL 10+ (`proto_version` 1); 14+ for `proto_version: 2` + `streaming: true` (large in-progress txns) |
| DBA prerequisites (fix-it SQL shown by the wizard, never executed) | `wal_level=logical` (restart), free slot + WAL sender, role `WITH REPLICATION` + `SELECT`, `CREATE PUBLICATION alelt FOR TABLE ...`, `pg_create_logical_replication_slot('<slot>', 'pgoutput')`, `REPLICA IDENTITY FULL` on keyless tables |
| Settings | `host port database user sslmode schemas publication slot proto_version streaming create_slot snapshot_name connect_timeout_s` (see `connector.py` docstring) |
| Positions | LSN text `X/Y`; COMMIT token = commit `end_lsn`; `ack()` = `confirm_flush` (slot releases WAL only after the hub batch is durable) |
| Values | text output format (session: ISO dates, UTC, `bytea_output=hex`, `extra_float_digits=3`); type map `types.py`; arrays → JSON text; `infinity` / BC dates / unknown types → `Undecodable` (quarantined, R7) |
| TOAST | unchanged out-of-line values are omitted + `note=unchanged:<col>` (destination keeps its value) |

## Files

| Path | What |
|---|---|
| `alelt_src_postgres/` | `pgoutput.py` (wire decoder v1–v4), `changes.py` (messages → events, stream buffering), `types.py`, `catalog.py`, `position.py`, `connector.py` |
| `sql/` | `alelttest_schema.sql` (ERP-like tables + `pg_types` with every mapped type + publication), `alelttest_seed.sql`, `alelttest_churn.sql` |
| `tests/` | `pytest connectors/sources/postgres/tests` — golden corpus + units (no server needed) |

## Run

```powershell
docker run -d --name alelt-test-postgres -e POSTGRES_USER=alelttest -e POSTGRES_PASSWORD=<local only> -p 55433:5432 `
  postgres:16-bookworm -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10
python -m pytest connectors/sources/postgres/tests hub/alelt_capture/tests
```
