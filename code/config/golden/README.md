# config/golden — destination contract (R8)

| File | Produced by | Content |
|---|---|---|
| `ddl/<TABLE>.sql` | `alelt import ddl --schema X` | `GET_DDL('TABLE', ...)` of the existing landing table, normalized (trailing `;`, LF). |
| `columns.csv` | same | `table,column,ordinal,data_type,is_nullable` from `INFORMATION_SCHEMA.COLUMNS`, sorted by table, ordinal. |
| `type_map.yaml` | `alelt import type-map` | Source engine type -> destination type **as observed** in golden DDL; unmatched => `UNRESOLVED`. |
| `system_columns.csv` | `alelt import system-columns` | Every system column (configured names/prefixes) per table and its type. |

Rules
- Never hand-edit; re-run the importer and review the Git diff.
- When a golden DDL exists, `SnowflakeDestination.ensure_table` uses it (rewritten to a non-destructive `CREATE TABLE IF NOT EXISTS` on the landing schema); the canonical type map is only a fallback.
- Exports are read-only (`GET_DDL`, `SELECT` on `INFORMATION_SCHEMA`); ALELT never writes to the existing landing schema of the legacy pipeline (R2).
