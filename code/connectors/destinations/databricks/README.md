# alelt_dst_databricks — Databricks SQL warehouse destination

`alelt_dst_databricks.destination:DatabricksDestination` (engine name `databricks`). Client: `databricks-sql-connector` (Apache-2.0).

| Area | Detail |
|---|---|
| URL (`ALELT_DEST_URL`) | `databricks://token:${DATABRICKS_TOKEN}@adb-123.azuredatabricks.net/?http_path=/sql/1.0/warehouses/abc&catalog=main` |
| Tables | `<catalog>.<schema>.<table>` (Delta, `timestampNtz` feature enabled); control `<catalog>.<control>.checkpoint` / `audit_batch`. Types: `config/type_maps/canonical_to_databricks.yaml`. |
| Exactly-once | Databricks SQL commits each statement separately (no multi-table transaction), so every write is an idempotent Delta write: the session sets `spark.databricks.delta.write.txnAppId = <stream>` and `spark.databricks.delta.write.txnVersion = <seq> * 10000 + <statement no>`; Delta skips an (appId, version) already recorded for that table. The checkpoint is re-read before the first write (compare-and-set) and written by a conditional MERGE (`WHEN MATCHED AND seq = <planned seq>`, single-table atomic) that runs last and is read back (`CheckpointConflictError` when another writer won); a replay after a crash re-runs the batch and the committed statements are skipped. |
| Alternative | Without a SQL warehouse: write the Delta files with `alelt_dst_delta` (same txn mechanism) and register them in Unity Catalog. |
| Limits | Atomicity per statement/table, never partial within a table. |

Verified: SQL-generation unit tests only — there is no local Databricks environment or emulator. Live verification needs a workspace + SQL warehouse (free trial: see the console Local test lab guidance).

Tests: `pytest connectors/destinations/databricks/tests`.
