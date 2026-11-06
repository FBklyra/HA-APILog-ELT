# Re-sync a table (snapshot + catch-up)

Use when: new table enabled, DATA CAPTURE CHANGES was lost (`AleltCaptureLost`), archive logs needed for catch-up are gone, or recon shows drift that reprocessing cannot fix. Prefer [reprocess-range.md](reprocess-range.md) when the raw archive still covers the range — it puts **no load on Db2**.

| # | Step | Who | Check |
|---|---|---|---|
| 1 | Confirm cause. If capture was lost: DBA re-enables DATA CAPTURE CHANGES (their change; ALELT never runs DDL — R1) | DBA | Catalog snapshot shows `capture_enabled=true` |
| 2 | Schedule in the **off-peak window** agreed with the DBA (prod) | DataOperator | Window in ticket |
| 3 | Console → Tables → *table* → **Re-sync** → type table name → confirm (second approver if configured) | DataOperator | Audit event |
| 4 | Hub records start position, sends `SNAPSHOT_TABLE{table, key_range, chunk_rows}`; agent reads `WITH UR` in chunks under the governor | automatic | Table state `SNAPSHOTTING`, chunk progress |
| 5 | Loader replaces target rows from the snapshot and marks rows absent from the source `<DELETED>`=TRUE (soft delete, never physical) | automatic | — |
| 6 | Changes since start position applied from the log (catch-up); state → `IN_SYNC` | automatic | Lag back under target |
| 7 | Run recon (count, key set, checksums) | DataOperator | Reconciliation screen green |
| 8 | If the table is already switched to ALELT, consider a temporary `SWITCH_SOURCE(table,'LEGACY')` during the snapshot ([rollback.md](rollback.md)) | DataOperator | dbt unaffected |

Abort: Console → Tables → *table* → **Cancel re-sync**, or [kill-switch.md](kill-switch.md) if Db2 load is a concern.
