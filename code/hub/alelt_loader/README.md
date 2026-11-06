# alelt_loader — engine-agnostic loader

| Module | Role |
|---|---|
| `policy.py` | Per-connection policy from `config/tables.yaml` (`ALELT_TABLES_CONFIG` = file or directory): enabled schemas/tables/columns, `name_in_destination`, `landing_schema`, hashed columns (SHA-256 hex **placeholder**, TODO OQ-03). Output profile by name from `config/output_profiles.yaml` + `config/extensions.yaml: profile_files` (+ `ALELT_OUTPUT_PROFILES`); `from_golden` profiles resolve from `config/golden/columns.csv` or are absent (never assumed). Only `alelt` is built in. |
| `loader.py` | Micro-batching (rows / latency), final op per key, PK change → delete old key + upsert new, partial images merged (`lob_unavailable`), keyless tables keyed by `surrogate_id` (same algorithm as the Snowflake destination; the destination writes the id column), replay filter (commit ≤ checkpoint skipped), snapshot hold (`hold(table)`, X-22), one atomic `DestinationConnector.apply()` per flush, `Backpressure` when the buffer is full. |

Test: `pytest hub/alelt_loader/tests`.
