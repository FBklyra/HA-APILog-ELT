# alelt_dst_clickhouse — ClickHouse destination

`alelt_dst_clickhouse.destination:ClickHouseDestination` (engine name `clickhouse`). Client: `clickhouse-connect` (Apache-2.0, HTTP).

| Area | Detail |
|---|---|
| URL (`ALELT_DEST_URL`) | `clickhouse://user:${ALELT_CH_PASSWORD}@host:8123/?secure=0&delete_mode=soft` |
| Landing table | `<schema>.<table>`: source columns (types: `config/type_maps/canonical_to_clickhouse.yaml`, `Nullable()` except key) + profile `_alelt_synced DateTime64(6,'UTC')`, `_alelt_deleted Bool` + `_alelt_ver UInt64`, `_alelt_is_deleted UInt8`. `ENGINE = ReplacingMergeTree(_alelt_ver, _alelt_is_deleted) ORDER BY <PK>`; keyless tables use the profile id column (surrogate). |
| Read | `SELECT … FROM t FINAL WHERE _alelt_is_deleted = 0` or the view `<table>_current`. |
| Control | `<control>.checkpoint` (ReplacingMergeTree(seq) by stream), `<control>.audit_batch`, `<control>.stg_*` (per-stream stage). |
| Exactly-once | rows staged → **one INSERT of the checkpoint row = commit point** → stage moved into the target. On restart: committed stage rows rolled forward, uncommitted discarded. `_alelt_ver = seq << 24 | ordinal` so a re-move/replay is idempotent by version. Replay of an applied batch = no-op (checkpoint ≥ to_pos). Committed-but-unmoved stage rows are rolled forward on every control-state read. **CAS is best effort** (no transactions in ClickHouse): checkpoint re-read before staging and right before the commit-point INSERT, duplicate seq detected and reported after it (`CheckpointConflictError`); strict exactly-once = version-based idempotence + one writer per stream. Dates outside 1900-01-01..2299-12-31 are rejected, never clamped. |
| Deletes | soft (default): row kept with `_alelt_deleted = true`; `delete_mode=hard`: `_alelt_is_deleted = 1` (hidden by FINAL, removed by merges). Truncate: all live rows re-versioned as deleted. |
| PK update | delete(old key) + upsert(new key) (alelt_dst_kit.plan). Partial images completed from the current row. |
| Limits | No multi-table transaction in ClickHouse: after the commit point tables become visible one by one (each INSERT atomic). One writer per stream. `LOCALTIME` stored as ISO text. |

Verified: unit + conformance on the lab ClickHouse (`lab up clickhouse`, 12 passed). Local date-times are stored as wall clock in `DateTime64(6, 'UTC')` (no host-time-zone shift).

Tests: `pytest connectors/destinations/clickhouse/tests`; conformance: `ALELT_TEST_CLICKHOUSE_URL=clickhouse://lab:lab_local_only@127.0.0.1:47823/?secure=0` (`deploy/local/TARGETS.md`).

## Single-writer rule (strict exactly-once)
ClickHouse has neither multi-statement transactions nor a conditional INSERT, so the destination cannot arbitrate
two concurrent appliers by itself (KeeperMap would need ClickHouse Keeper on every deployment; not enabled).
Rule: **exactly one applier per (control schema, stream)** - ENFORCED (DST-clickhouse-01):
- `apply()` of a stream refuses (`WriterFenceError`, nothing written) unless the calling thread holds a live hub lock
  covering the stream (`pipeline:<cid>` for the main stream, `pipeline:`/`replay:<cid>` for `<cid>#...` streams; the
  hub registers its locks in `alelt_common.spi` when `dialect.try_lock` takes them). The lock is re-checked right before
  the commit point.
- `<control>.writer` (stream, writer, epoch): a new lock holder claims the stream with epoch + 1; a writer that finds
  another writer's claim at its commit point aborts (fenced out), its stage is truncated.
- Tools that own the database exclusively: `?single_writer=off` (logged), or `alelt_common.spi.hold_writer("stream:*", ...)`.
On top: checkpoint re-read before staging and before the commit-point INSERT (duplicate seq reported as
`CheckpointConflictError`) and version-based idempotence. Row versions `_alelt_ver = ver << 24 | ordinal` come from ONE
version line for all streams and snapshots of the control schema (`checkpoint.ver`, `<control>.version`), so a
catch-up / re-read stream that writes a table after the main stream wins, and CDC after a snapshot wins over it
(DST-clickhouse-08).
Type widening on drift: `Decimal(P,S)` / integers MODIFY'ed in place; `String` is unbounded.
