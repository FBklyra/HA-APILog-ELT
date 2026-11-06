# alelt_schema — drift, policy, view interface, dbt

| Module | Role |
|---|---|
| `config.py` | `ConnectionConfig` (config/tables.yaml), `OutputProfile` loaders (core + extension files), `config/extensions.yaml` loader |
| `drift.py` | `diff_catalogs(old, new)`: TABLE_ADDED/DROPPED, COLUMN_ADDED/DROPPED/ALTERED, KEY_CHANGED, CAPTURE_LOST (alert), CAPTURE_ENABLED, COMPRESSION_CHANGED — deterministic order |
| `policy.py` | `decide(event, ALLOW_ALL/ALLOW_COLUMNS/BLOCK_ALL, enabled)` -> APPLY / PENDING / IGNORE; UNKNOWN type => PENDING (R7) |
| `manager.py` | `SchemaManager.process()` applies column adds / new tables via the Destination SPI, records `PendingApproval`s, alerts, regenerates views; `approve()` / `reject()` |
| `views.py` | Explicit view column list: golden order if present, else data columns + retained dropped columns + profile system columns |
| `dbt.py` | `sources.yml` over `<SOURCE>_RAW` (var `alelt_source_schema`), `alelt_shadow` target + vars building from the landing schema |

Columns are never dropped in the landing table; a source drop needs approval and the column stays in the view.
Tests: `cd hub && python -m unittest discover -s alelt_schema/tests -t .`
