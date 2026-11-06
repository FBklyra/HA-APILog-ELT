# config/ — file formats

| File | Owner | Purpose |
|---|---|---|
| `tables.yaml` | hand-written or an import extension | Normalized per-connection config: tables, columns, policy, output profile |
| `output_profiles.yaml` | platform | System-column names per output profile (`alelt`, `custom` example) |
| `extensions.yaml` | platform | Optional plugins (import subcommands, extra profile files); empty in the public distribution |
| `type_maps/db2_to_canonical.yaml` | Db2 source | Engine type -> canonical type |
| `type_maps/canonical_to_<dest>.yaml` | destinations | Canonical -> physical fallback when no golden DDL |
| `golden/` | `alelt import ddl / type-map / system-columns` | Destination contract (R8), see `golden/README.md` |

## tables.yaml

| Key | Type | Notes |
|---|---|---|
| `connection_id` | str | Stream/connection identity |
| `source_engine`, `destination_engine` | str | Plugin names |
| `output_profile` | str | Profile name; default `alelt` (`_ALELT_SYNCED`, `_ALELT_DELETED`, `_ALELT_ID`) |
| `schema_change_handling` | `ALLOW_ALL` \| `ALLOW_COLUMNS` \| `BLOCK_ALL` | Drift policy (alelt_schema) |
| `source_name` | str | `<SOURCE>` in `ALELT_<SOURCE>` and `<SOURCE>_RAW` |
| `landing_schema`, `view_schema` | str | Destination schemas (defaults derived from `source_name`) |
| `legacy_schema` | str \| null | Existing landing schema of the legacy pipeline, read-only; target of `SWITCH_SOURCE(...,'LEGACY')` |
| `snapshot_window` | `{start, end, tz}` | Off-peak window for snapshots; `end < start` wraps midnight |
| `schemas.<S>.tables.<T>` | map | `enabled`, `name_in_destination`, `sync_mode` (`SOFT_DELETE` \| `HISTORY`), `columns.<C>` = `{enabled, hashed, is_primary_key}` (only non-default columns need listing) |

Ordering: maps are written with sorted keys so Git diffs are stable.

## output_profiles.yaml

`profiles.<name>` = `{synced_column, deleted_column, id_column, synced_type, deleted_type, id_type}`; `default` names the fallback.
Python code receives the profile object, never literal system-column names; migrations do not reference them.

## extensions.yaml

`extensions[]` = `{path, import_commands{name: 'module:register'}, profile_files[]}`. Loaded by `alelt_import.cli` and the
profile loaders. Extensions are separate, optionally private packages.

## type maps

`types.<ENGINE_TYPE>` = canonical name, or `{canonical, for_bit_data}`. Destination maps use `{p}`, `{s}`, `{n}` placeholders;
`UNKNOWN: null` means the column is never created and its values are quarantined (R7).

## Initial load, replay_from, allow-list (ADR-012)

| Key | Level | Values | Meaning |
|---|---|---|---|
| `initial_load` | connection, table | `auto` (default) \| `none` | `auto`: every enabled table is snapshotted, caught up, verified, then streams; `none`: tables start empty and stream from the current position. Env default `ALELT_INITIAL_LOAD`. |
| `replay_from` | connection, table | source position text | test / ops only: no snapshot, the table's history is replayed from this position through the catch-up pass |
| `schedule` | table | `window` / `latency_target_s` | gates **changes** of STREAMING tables only: an initial load, catch-up or resync lands whatever the window |
| `snapshot.allowed_tables` | agent config | `SCHEMA.TABLE` \| `SCHEMA.*` | agent-side allow-list (docs/AGENT-COMMANDS.md); `*.*` refused |
