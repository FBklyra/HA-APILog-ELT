# Known issues — ALELT (HA-APILog-ELT) v1.0.0

**All issues below are known and actively being fixed.**

v1.0.0 is an early release. The items below are tracked for the next releases; each one is being worked on.

## 1. Test-suite order dependence (hub)

Every hub test module passes when run on its own (SQLite and PostgreSQL metadata). The failures below show up
only when the whole hub suite runs in one session against a shared session database.

### 1.1 `test_initial_load.py::test_initial_snapshot_lands_rows_that_existed_before_capture` (sqlite, postgres)

- Symptom: `not converged within 120s` in the full-suite run.
- Suspected cause: leftover control-lane jobs from earlier test modules in the same session database (completion
  jobs of other connections re-enqueueing themselves, a `decode` job in an older payload shape without
  `connection_id`, a `resync` job for a connection with no start position).
- Status: In progress — fix being worked on

### 1.2 `test_initial_load.py::test_catalog_shaped_schema_with_out_of_row_values_converges` (sqlite, postgres)

- Symptom: very slow in the full-suite run (222 tables), then fails its convergence bound; passes on its own with
  30 tables. The 222-table case in isolation was not re-run on the final code.
- Suspected cause: the same cross-test job load as 1.1, plus scale on a shared queue.
- Status: In progress — fix being worked on

### 1.3 `test_lifecycle.py::test_restart_mid_snapshot_resumes_with_key_from` (sqlite)

- Symptom: fails in the full-suite order only.
- Suspected cause: leftover control-lane work delays the re-issue past the test bound.
- Status: In progress — fix being worked on

### 1.4 `test_lifecycle.py::test_restart_in_catch_up_bulk_and_before_streaming_exactly_once` (sqlite)

- Symptom: fails in the full-suite order only.
- Suspected cause: same as 1.3.
- Status: In progress — fix being worked on

### 1.5 Other full-run failures in the PostgreSQL half

- Symptom: about six further failures in a full run that was stopped before the names were captured.
- Suspected cause: most likely the same order-dependent causes as 1.1–1.4.
- Status: In progress — fix being worked on

## 2. Verification not yet repeated on the release code

### 2.1 Db2 → hub → warehouse end-to-end run

- The Db2 → hub → warehouse (Snowflake simulator) end-to-end run (local e2e with the Python and C agents, and the
  catalog-shaped e2e) has not been re-run on the final code of the initial-load fix.
- Status: In progress — fix being worked on

### 2.2 Independent code review of the initial-load fix (work packages WP0–WP6)

- The fix for the Db2 → warehouse initial-load convergence problem (work packages WP0–WP6) has not yet had an
  independent code review.
- Status: In progress — fix being worked on

### 2.3 Test suites not re-run after the product rename

- All internal identifiers were renamed to `alelt` for this release (Python packages `alelt_*`, environment
  variables `ALELT_*`, output profile `alelt`, switch target `ALELT`, agent `aleltagent.exe` / `aleltd`, metadata
  columns `_ALELT_*`, control schema `ALELT_CONTROL`, spool / binlog-frame magic `ALB1` / `ALBE` / `ALBL`). The
  rename was verified mechanically (byte-compile, test collection, console type check, clean C build, compose
  validation); the test suites were not re-run afterwards. Recorded MySQL / MariaDB golden binlog captures and
  Db2 golden log captures were patched in place with same-length names.
- Existing development databases created by the pre-rename setup scripts (for example the old Db2 test schema) must be
  re-created with the renamed scripts.
- Status: In progress — fix being worked on

### 2.4 Component version strings

- The release is tagged v1.0.0 and the container image carries version label 1.0.0, but the Python packages and
  the Windows agent still report component version `0.1.0` (kept unchanged because the upgrade tests key on it).
- Status: In progress — fix being worked on

## 3. Windows (C) agent

### 3.1 Dedicated command thread (design decision D-17)

- The C agent does not yet run hub commands on a dedicated command thread with a second plugin context; commands
  are handled on the capture thread, so a long command can delay capture.
- Status: In progress — fix being worked on

### 3.2 Agent hardening gaps

- The libFuzzer / ASan fuzz targets are not built in CI, and a fuzz invariant on the command range is stale.
- On service stop the agent can wait up to about 180 s without updating its service checkpoint.
- The Windows service test harness needs an elevated session and is not part of automated runs.
- Status: In progress — fix being worked on

## 4. Open items from the internal code review (summarised)

### 4.1 Cross-cutting data semantics

- DELETE of a key unknown to the destination can give different results per destination; conformance cases are
  missing.
- No canonical rule yet for timestamps with more than 6 fractional digits (Db2 TIMESTAMP(7..12), SQL Server
  datetime2(7), lake destinations truncate to microseconds silently); time `24:00:00` is not mapped.
- No canonical edge-value conformance matrix (NaN, NUL characters, decimal scale, date range) and no shared
  per-engine type-map fixture; the destination conformance suite misses big decimals, out-of-range dates, time zones
  and snapshot-plus-CDC cases.
- Status: In progress — fix being worked on

### 4.2 Capture runtime

- Remote capture ships whole transactions only; a transaction larger than the configured cap stops capture
  (no streaming / spill of very large transactions).
- The capture runner does not send source keepalives while it waits on hub back-pressure (HTTP 503).
- Status: In progress — fix being worked on

### 4.3 Sources

- Db2: some common non-UTF-8 CCSIDs are mapped but not tested; agent vs hub canonical-type recompute and the
  "log reused" warning path have no dedicated tests.
- SQL Server: about three queries per window per instance and whole-window fetches; fixed server time-zone offset
  (DST) not tested; UNKNOWN types are passed through.
- MySQL / MariaDB: snapshot start-position race when `gtid_mode` is OFF (blocked by a prerequisite check, no unit
  test); table names are lower-cased (collisions on case-sensitive servers); derived `server_id` not checked
  against replicas.
- Oracle: a long open transaction re-adds archived logs on every window; the LOB delete-compensation heuristic is
  not version-gated.
- PostgreSQL: snapshot hand-off window between per-table sessions (no exported snapshot); numeric NaN accepted.
- Status: In progress — fix being worked on

### 4.4 Destinations

- Databricks: the idempotent-write mechanism has not been run on a real workspace; autocommit per statement;
  inline multi-row statements are slow.
- Microsoft Fabric: validated against a SQL Server stand-in only; VARCHAR sizing is in characters, not UTF-8 bytes.
- Redshift: VARCHAR sizing is in characters, not bytes.
- ClickHouse: no `ON CLUSTER` / replicated engines; `FINAL` on every lookup / verify.
- Iceberg: copy-on-write rewrite on every flush.
- Snowflake: six or more statements per table per flush, outside one transaction.
- Status: In progress — fix being worked on
