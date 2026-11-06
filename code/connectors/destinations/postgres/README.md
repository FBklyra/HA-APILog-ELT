# alelt_dst_postgres — local test destination (PostgreSQL)

`alelt_dst_postgres.connector:PostgresDestination(url, schema, control_schema="alelt_control", output_profile, compare)`

| Area | Detail |
|---|---|
| Landing tables | source columns (types from `config/type_maps/canonical_to_postgres.yaml`) + profile system columns (default `_alelt_synced timestamptz`, `_alelt_deleted boolean`); identifiers lower-cased; keyless tables get `<id_column> varchar(32)` PK = `alelt_loader.surrogate_id` |
| Control schema | `checkpoint(stream, position, batch_id, updated_at)`, `audit_batch(batch_id, stream, from/to position, rows_by_table, total_rows, duration_ms, status)` |
| `apply()` | one transaction: lock checkpoint row → no-op if checkpoint ≥ to_pos → upserts / soft deletes / truncate → checkpoint → audit row. Partial images update only the columns present. |

Test: `ALELT_TEST_PG_URL=postgresql://user:pw@localhost:15435 pytest connectors/destinations/postgres/tests` (throw-away database).
