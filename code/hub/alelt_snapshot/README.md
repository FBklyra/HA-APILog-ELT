# alelt_snapshot — initial sync and re-sync

| Module | Role |
|---|---|
| `planner.py` | `plan(table, cardinality, chunk_rows, key_min/key_max | boundaries)` -> half-open key ranges; composite/text keys without boundaries => one range paged by the edge |
| `state.py` | `NEW -> SNAPSHOTTING -> CATCHING_UP -> IN_SYNC`, `* -> RESYNC -> SNAPSHOTTING`; `SnapshotJob`; Memory/File job stores (atomic JSON) |
| `window.py` | `OffPeakWindow` (tz-aware, wraps midnight) |
| `orchestrator.py` | `SnapshotOrchestrator`: start/resync (records `start_pos` via probe), pause/resume, `step()` one chunk inside the window, `on_chunk()` for edge rows, `recover()` after restart (in-flight chunks re-dispatched; reload is idempotent), `caught_up(verified)` gates IN_SYNC on alelt_verify |

Runners: `EdgeChunkRunner` (whitelisted `SNAPSHOT_TABLE` command, `WITH UR`) and `RemoteChunkRunner` (`SourceConnector.snapshot_rows`).
Loader contract: while `holds_changes(table)` the loader must not apply that table; after `on_catchup(job)` it replays from `job.start_pos`.
Tests: `cd hub && python -m unittest discover -s alelt_snapshot/tests -t .`
