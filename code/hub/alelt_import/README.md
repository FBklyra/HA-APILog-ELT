# alelt_import — golden-spec importers (read-only, vendor-neutral)

| Command | Output |
|---|---|
| `alelt import ddl --database D --schema X` | `config/golden/ddl/<TABLE>.sql` (GET_DDL, LF, `;`) + `columns.csv` (INFORMATION_SCHEMA.COLUMNS) |
| `alelt import type-map --catalog snap.json` | `config/golden/type_map.yaml`: source type -> observed destination type, `ambiguous`, `UNRESOLVED` |
| `alelt import system-columns [--profile P] [--name N] [--prefix X]` | `config/golden/system_columns.csv` |
| `alelt import diff --current new.yaml [--committed config/tables.yaml]` | path-level diff; exit 1 on drift |
| `alelt import check-golden` | contract test: exit 1 if an enabled table lacks golden DDL |

Extension commands are registered from `config/extensions.yaml` (`import_commands`).
Run as `python -m alelt_import <command>` (the umbrella `alelt` CLI should dispatch `import` to `alelt_import.cli:main`).
Connection: `ALELT_IMPORT_SNOWFLAKE_ACCOUNT/USER/ROLE/WAREHOUSE` + `..._PRIVATE_KEY_PATH|PRIVATE_KEY[_PASSPHRASE]`; every statement passes a SELECT/SHOW-only guard.
Tests (recorded fixtures, no network): `cd hub && python -m unittest discover -s alelt_import/tests -t .`

## Migration importer (console)

| Module | Role |
|---|---|
| `plugin.py` | `ImportPlugin` SPI: `discover(credentials)` (live, read-only) / `discover_files(bundle)` (offline) → `InventoryModel`; registry = core `yaml_manifest` + `extensions.yaml: import_plugins` |
| `models.py` | `InventoryModel`, `MigrationPlan`, `PlanEdits` (secrets masked; only `secret_fields` names) |
| `plan.py` | inventory → plan (engines, tables.yaml docs, latency from schedule, triggers → webhook / dbt job, users → roles, warnings), edits, diff, two-person check |
| `files.py` / `golden.py` | dropped files (JSON, ZIP, YAML, DDL, columns CSV) → bundle; DDL parser; golden spec + contract check |
| `manifest.py` | generic file plugin for any legacy tool (format in the module docstring) |
| `guard.py` | production deny list (hosts, wildcards, IP/CIDR, host:port, account ids, URLs) |
| `secrets.py` | masking + reference-based secret store (`ALELT_SECRETS_DIR`) |
| `wizard.py` | parallel-test wizard defaults (preselection, ordering, latency, profile, tables.yaml) |

Hub API: `hub/alelt_hub/importer.py`. Tests: `python -m unittest alelt_import.tests.test_migration` (from `hub/`).
