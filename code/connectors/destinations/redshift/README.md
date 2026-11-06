# alelt_dst_redshift — Amazon Redshift destination

`alelt_dst_redshift.destination:RedshiftDestination` (engine name `redshift`). Driver: `psycopg` (default, PostgreSQL wire protocol) or `redshift_connector` (Apache-2.0, IAM auth) with `driver=redshift_connector`.

| Area | Detail |
|---|---|
| URL (`ALELT_DEST_URL`) | `redshift://user:${ALELT_RS_PASSWORD}@cluster.xxxx.region.redshift.amazonaws.com:5439/dev?sslmode=require` |
| Transaction (R6) | LOCK checkpoint → re-check checkpoint → per table: [truncate] → per op group: CREATE TEMP TABLE + executemany INSERT → MERGE (upsert / soft delete) or DELETE … WHERE EXISTS (hard delete) → checkpoint DELETE+INSERT → audit row → COMMIT. Any error ⇒ ROLLBACK, nothing visible. |
| Types | `config/type_maps/canonical_to_redshift.yaml` (VARBYTE, SUPER for JSON, TIMESTAMPTZ). |
| Control | `<control>.checkpoint`, `<control>.audit_batch`. |
| Local stand-in | `compat=postgres` runs against PostgreSQL >= 15 as a rough approximation (VARBYTE/SUPER → bytea/text). Redshift-only behaviour (distribution/sort keys, SUPER, late binding, MERGE restrictions) is NOT tested locally. |

Verified: SQL-generation unit tests; conformance. Not run against a real cluster.

Tests: `pytest connectors/destinations/redshift/tests` with `ALELT_TEST_REDSHIFT_PG_URL=postgresql://<user>:<pw>@localhost:55433` (a throw-away database is created). e2e: `e2e_dest.py --dest redshift --dest-url "redshift://<user>:<pw>@localhost:55433/alelt_e2e_redshift?compat=postgres&sslmode=disable"`.
