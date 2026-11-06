# ALELT — Design (v1.0)

ALELT ("Universal HA API Log ELT") is an open-source, engine-agnostic change-data-capture (CDC) / ELT platform. It reads committed changes from operational databases **only through their public log or change APIs**, turns them into one canonical change model, and applies them to warehouses, lakes, SQL databases and streams with the **data and the checkpoint in one destination transaction**.

This document is the design of record: what the system guarantees, how, and what is not (yet) guaranteed. Diagrams: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md). Decisions: [`docs/adr/`](docs/adr/README.md). Per-module detail: each module's `README.md` / `DESIGN.md`. Internal Python packages and tables keep the historical `alelt_` prefix.

| Section | Topic |
|---|---|
| [1](#1-guarantees-and-their-status) | Guarantees and their status |
| [2](#2-architecture) | Architecture |
| [3](#3-canonical-model) | Canonical model |
| [4](#4-source-spi) | Source SPI (edge and remote capture, completeness contract) |
| [5](#5-destination-spi) | Destination SPI (atomic apply, checkpoint compare-and-set) |
| [6](#6-checkpoint-watermark-and-integrity-model) | Checkpoint, watermark and integrity model |
| [7](#7-transaction-assembly) | Transaction assembly |
| [8](#8-snapshots-verification-certification) | Snapshots, verification, certification |
| [9](#9-durability-ha-and-failure-model) | Durability, HA and failure model |
| [10](#10-security) | Security |
| [11](#11-public-api-only-capture-and-provenance) | Public-API-only capture and provenance |
| [12](#12-configuration-upgrades-operations) | Configuration, upgrades, operations |
| [13](#13-test-strategy-and-evidence) | Test strategy and evidence |
| [14](#14-known-limitations) | Known limitations |

---

## 1. Guarantees and their status

A guarantee is stated only with the evidence that supports it. "Designed" means implemented and unit/integration tested; "proven" means an end-to-end run against the real engine recorded a PASS.

| # | Guarantee | Mechanism | Status |
|---|---|---|---|
| G1 | **Read-only on the source.** Nothing is written to a source database. | Capture via public log APIs; snapshots/verification are `SELECT` only, typed and whitelisted (§10). | Proven (static guard + every e2e) |
| G2 | **Durable before ACK.** A batch is acknowledged only after it is persisted; the agent frees its spool only up to the ACK. | Receipt state machine (§9.1). | Proven (kill -9 e2e, crash-point tests) |
| G3 | **Rolled-back work is never applied.** | Transaction assembly emits on COMMIT only; savepoint compensation (§7). | Proven on golden corpora (6 engines) + randomized property test |
| G4 | **Never drop silently.** Every record is decoded, ignored by documented design, or signalled; an unexplained record holds its table. | Completeness contract (§4.3) + integrity state machine (§6.3). | Designed; Db2 record census in progress |
| G5 | **Exactly-once per destination.** Rows and checkpoint commit in one destination transaction; the checkpoint is compare-and-set inside that transaction. | Destination SPI (§5), checkpoint model (§6). | Per destination, see §5.3. **Not claimed for Snowflake until a real-account run passes.** |
| G6 | **Ordered per stream.** Change sets are applied in source commit order; positions are opaque and compared only by the source connector. | Canonical `Position` (§3.1). | Proven |
| G7 | **No silent gap.** A position gap on the wire or at the source is an error, never skipped. | Chain check 409 (§9.1), `SourceGapError` (§4.2). | Proven (wire); source gaps per engine |
| G8 | **HA without split brain.** One capture writer per connection; a stale writer is fenced. | Capture lease + epoch fencing (§9.2). | Proven for the hub and the Python lease client |

## 2. Architecture

Three zones (diagrams: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)):

| Zone | Components |
|---|---|
| Source | the source database; for engines whose log API is local-only (Db2 `db2ReadLog`) an **edge agent** on the database host (C service on Windows, Python daemon on Linux) — outbound connections only |
| Hub (one image, roles `api` / `worker` / `capture` / `migrate` / `all`) | agent gateway, console API, remote-capture runners, workers (decode → transaction assembly → load; snapshot, schema, verify, diagnose jobs); metadata in PostgreSQL (or SQLite for one host); spool/raw archive in a blob store (filesystem or any S3-compatible store) |
| Destination | warehouses, lakes, SQL databases, Kafka-API streams through the destination SPI |

Separation of concerns:

| Layer | Knows | Never knows |
|---|---|---|
| Source connector | the engine's log format, catalog, positions | destinations, other engines |
| Core (`alelt_common`, `alelt_txn`, `alelt_loader`, `alelt_hub`) | the canonical model, the two SPIs | any engine byte format or SQL dialect |
| Destination connector | its engine's DDL/DML, transactions | sources |

Raw first, decode later: edge agents ship **raw** log records; all decoding happens on the hub (minimal CPU on the database host, re-decode from the raw archive without touching the source — ADR-002, ADR-007).

## 3. Canonical model

Defined in `hub/alelt_common/models.py`; the only types that cross module boundaries (ADR-011).

### 3.1 Position

`Position{engine, text}` — opaque, totally ordered **per stream**, compared only by the source connector's comparator. Db2 LRI, PostgreSQL LSN, MySQL GTID set / file:pos, Oracle SCN(+XID, restart SCN), SQL Server LSN:seqval. A COMMIT position is a **resume token**: `stream(p)` returns the transactions committed after `p`.

### 3.2 Canonical types

`SHORT INT LONG DECIMAL(p,s) FLOAT DOUBLE BOOLEAN STRING BINARY LOCALDATE LOCALTIME LOCALDATETIME INSTANT JSON XML UNKNOWN`. Values are Python canonical values in process (`Decimal`, `bytes`, `datetime`), tagged canonical JSON on the wire and in spill files (`alelt_common/canonical_jsonl.py`). A value that cannot be represented is an `Undecodable` — quarantined, never coerced.

### 3.3 ChangeEvent and ChangeSet

`ChangeEvent{position, txid, kind, table, before, after, commit_ts, raw_ref, note}` with `kind ∈ INSERT UPDATE DELETE TRUNCATE COMPENSATION COMMIT ROLLBACK DDL OTHER`. `note` carries `;`-separated decoder tokens with defined meaning: `unchanged:<col>` (documented "value not logged" semantics), `row_ref:<id>` / `target_ref:<id>` (§7), `alelt-signal:<json>` (§4.3).

`ChangeSet{txid, commit_position, commit_ts, events[]}` — one committed transaction, DML only, in log order.

### 3.4 Catalog snapshot

`CatalogSnapshot{connection_id, engine, taken_at, position, tables[{table, capture_enabled, compression, columns[{name, ordinal, engine_type, canonical_type, length, scale, nullable, key_seq, codepage}]}]}`. Stored as **versions** valid from a position: the catalog used for a batch is the newest version with `valid_from_pos <= batch.from_pos` (source comparator). A catalog always carries a position; when an agent omits it the hub uses its durable position at receipt.

### 3.5 Keyless tables

Tables without a key get a surrogate id = first 32 hex of sha256 over the canonical JSON of all data columns in ordinal order. **Known semantic:** identical duplicate rows share one surrogate (the landing holds one row for them; a DELETE of one duplicate removes "the" row). Parity for keyless tables is therefore a **multiset** comparison (count per identical row), never a key-set comparison (§8.2). Reproducing another platform's surrogate algorithm needs a contract decision per deployment.

## 4. Source SPI

`hub/alelt_common/spi.py::SourceConnector`; contract for hub-side sources: [`connectors/sources/_spi/REMOTE.md`](connectors/sources/_spi/REMOTE.md).

### 4.1 Edge capture (local-only log APIs)

The agent reads raw log records with the engine's public API (Db2: `db2ReadLog`, CLI `SQLConnect` — ADR-001, ADR-010), spools them locally, and ships batches (`zlib` or `none`; the Linux daemon `zstd` when available — the hub accepts `zstd`, `zlib`, `none`) to `POST /agent/v1/blocks`. Batch chain on the wire: `from_pos` = previous `next_pos`; a jump is legal only right after an acknowledged `START_CAPTURE{from_pos}` (`X-ALELT-Reposition`). Spool full ⇒ the agent pauses reading; it never drops. The hub talks to the agent only through **typed, whitelisted commands** (`START_CAPTURE PAUSE RESUME SET_LIMITS SNAPSHOT_CATALOG SNAPSHOT_TABLE VERIFY_TABLE PROBE SHUTDOWN`) the agent pulls (long-poll); there is no arbitrary SQL on the wire. Command contract (arguments, results, typed error codes, concurrency with capture and heartbeats, allow-list semantics): [`docs/AGENT-COMMANDS.md`](docs/AGENT-COMMANDS.md).

### 4.2 Remote capture (network-protocol log APIs)

A hub-side runner per connection (`alelt_capture`, lock `capture:<cid>`) calls `stream(from_pos)` of a live connector (PostgreSQL `pgoutput`, MySQL/MariaDB binlog replication protocol, Oracle LogMiner, SQL Server CDC functions). `stream` yields whole committed transactions in commit order, `None` as an idle tick; `ack(pos)` is called only after the hub batch is durable. Batches are canonical JSONL through the **same durable receipt** as edge batches. A position the source can no longer serve raises `SourceGapError` → connection ERROR, incident `POSITION_GAP`, resync required — never a silent skip.

### 4.3 Completeness contract

Every record a source reads is classified (`hub/alelt_common/completeness.py`):

| Class | Meaning | Hub action |
|---|---|---|
| `decoded` | canonical events with documented semantics only | apply |
| `ignored(reason)` | carries no row change by documented design | census count |
| `needs_keyed_reread(table, keys)` | a row change that cannot be decoded completely (out-of-row values) | apply the partial image; complete the listed columns by key through the data path (§6.4, ADR-013) — no hold, no incident; a signal without keys is routed like `unsupported` |
| `needs_catalog_refresh(table)` | shape unknown / changed | refresh the catalog, re-decode from the position |
| `unsupported(table)` | cannot be decoded, no key | hold the table (or the connection), resync |

Signals travel as ordinary `OTHER`/`DDL` events with `note="alelt-signal:<json>"` inside the transaction where they occurred, so the batch format is unchanged. Each connector keeps a per-instance **census** (record class × reason → count); "nothing quarantined" is never accepted as completeness evidence on its own.

## 5. Destination SPI

`hub/alelt_common/spi.py::DestinationConnector`; shared implementation kit and conformance suite: `connectors/destinations/_spi/alelt_dst_kit`.

### 5.1 Atomic apply with checkpoint compare-and-set

`apply(stream, changesets, to_pos, batch_id, table_map, tables, *, expected=UNSET, watermarks=None)` must, in **one destination transaction**: apply the rows, set `checkpoint(stream) = to_pos`, write the per-table watermark rows passed in `watermarks`, write the audit row — and **compare-and-set** the checkpoint: the value found inside the transaction must equal `expected` (default: the value read at the start of `apply`), else `CheckpointConflict` and nothing is committed. `apply` is a no-op when `checkpoint >= to_pos`. This makes the destination, not a hub lock, the arbiter between two appliers (stale requeued job, replay stream).

### 5.2 Other duties

Table creation and additive evolution from the canonical catalog; soft delete (`deleted` system column); system column names from the **output profile** (default `alelt`: `_ALELT_SYNCED`, `_ALELT_DELETED`; `golden_spec`: names taken from an exported landing DDL); snapshot load; partial images keep the destination value (`unchanged:<col>`); verification helpers (row counts, aggregates, keys) used by `alelt_verify`.

**DELETE rule (one rule for every destination, XC-07).** The loader passes every DELETE with the whole before image the source delivered (key-only images stay key-only; nothing is invented). A destination that holds the key sets the deleted flag and the synced column and keeps every other stored column. A destination that does **not** hold the key writes a **tombstone**: a row with the key, the deleted flag, the synced column and whatever before-image columns were delivered (others NULL). It never drops the delete silently, so delete parity and row counts are the same on every engine. Landing columns other than the key are nullable, so a key-only tombstone is always writable. Each destination must prove the rule with the conformance cases `delete_unknown_key` and `delete_key_only_before` in the destination kit. Until those cases pass for a destination, its row is not counted as proof.

### 5.3 Exactly-once status per destination

| Destination | Atomicity mechanism | Evidence |
|---|---|---|
| PostgreSQL, generic SQL (MySQL, MariaDB, SQL Server, SQLite, DuckDB, Db2) | one SQL transaction, checkpoint row locked inside it | contract suite incl. crash before COMMIT; e2e replay ×3; destination-level replay of a real flushed call (hub test) |
| Kit destinations (Iceberg, Delta, ClickHouse, Kafka API, BigQuery, Redshift, Fabric, Databricks) | engine-specific commit protocol (snapshot CAS, transactional producer, MERGE + checkpoint) | kit conformance incl. crash mid-apply, against local engines / emulators; Databricks SQL generation only |
| **Snowflake** | `BEGIN … COMMIT` with checkpoint compare-and-set inside the transaction | **unit tests against a mock executor only. No exactly-once claim until a real-account run passes.** |

## 6. Checkpoint, watermark and integrity model

### 6.1 Streams and the truth

The **destination checkpoint is the truth** (ADR-005). The hub keeps a mirror (`checkpoint_mirror`) for restart speed and display, updated after the destination commit; a crash between the two is harmless — on restart the worker replays from the mirror's `restart_seq` and the destination skips everything at or below its checkpoint.

| Row | Meaning |
|---|---|
| `checkpoint(<cid>)` | main stream: every table of the connection not held is applied through this position |
| `checkpoint(<cid>#t:<table>)` | per-table **watermark** of a held table: the table is applied exactly through this position (written in the same transaction as the main checkpoint) |
| replay pass streams | one checkpoint per replay pass, so an interrupted pass resumes exactly-once |

Restart rule: per table, the applied position is `watermark(table)` when the table is held, else the main checkpoint. A table that leaves a hold is replayed from its watermark up to the main stream, then re-attached under the pipeline lock.

### 6.2 Holds and replay

A **hold** (`table_hold`: snapshot, schedule window, integrity) stops a table's apply at a position while the main stream continues for other tables; its changes stay in the spool. Replays run as **one multi-table pass** over the spool (not per table), read **decoded canonical events** from a cache keyed by batch + catalog version + decoder version (raw records are decoded once), and run under a replay lock, not the pipeline lock, so the main stream is not blocked. Schedule windows hold tables from pipeline start (a held table is "not yet applied", not "applied then held").

### 6.3 Integrity state machine

Per table: `IN_SYNC → SUSPECT → REDECODING → OUT_OF_SYNC → RESYNCING → IN_SYNC`. Integrity is for **unexplained**
records and the two other signal classes (`unsupported`, `needs_catalog_refresh`); out-of-row values are completion
work (§6.4). Any unexplained quarantined record or `unsupported` signal attributable to a table makes it SUSPECT at
that position; with the default policy `on_quarantine: hold_table` its watermark never passes the record, an incident
opens, certification and switch are blocked, and recovery runs automatically: re-decode with a refreshed catalog →
resync (snapshot + catch-up) → operator. A table whose completion cannot converge (no key, no fetch capability,
overflow) enters the machine with reason `completion unavailable` (step resync). `continue` (explicit opt-in) keeps
applying but keeps the table SUSPECT and loud; `stop_stream` stops the connection.

### 6.4 Out-of-row completion

`alelt_hub/completion.py`, ADR-013. A `needs_keyed_reread` signal with keys is queued when its transaction commits (one
`table_completion` entry per table + key, folded: `columns` union, `last_pos` = newest commit position). Fetches:
`VERIFY_TABLE{rows, columns = key + signalled columns}` through the agent (<= 1000 keys / 48 KiB per command, <= 4 in
flight per connection) or `SourceConnector.keyed_reread` for remote sources (control lane). Results become UPDATE
events with only the fetched out-of-row columns, placed in the main-stream buffer at their read position R (later
buffered changes of the key win) and flushed with the checkpoint (same transaction). An entry is done when a fetch read
at `R >= last_pos` was flushed; a newer signal re-queues it; a DELETE committed after the newest signal cancels it.
Failures back off (`2^attempts` s, max 300); no answer for `ALELT_COMPLETION_STALL_S` → `COMPLETION_STALLED` (stream keeps
flowing, auto-resolved when the table drains); a single oversize row → `COMPLETION_OVERSIZE`, retried hourly.
Completion floor `<cid>#t:<table>` (ADR-005 amendment); DR rebuild `alelt completion rebuild <cid>`. Tables frozen by
an integrity / schema / snapshot / DDL hold or not STREAMING are not fetched; schedule-held tables are completed
(only signalled columns of rows a pass already applied). Pending entries block certification, switch and settled
verification (soft gate, never a hold). Metrics `alelt_completion_pending`, `alelt_completion_oldest_seconds`,
`alelt_completion_commands_total`, `alelt_completion_rows_total{result}`.

## 7. Transaction assembly

`hub/alelt_txn`: events are buffered per `txid` in log order, emitted as a `ChangeSet` on COMMIT, discarded on ROLLBACK; DDL/OTHER do not reach the loader (signals are consumed by the pipeline, §4.3). Large transactions spill to process-private files (length-prefixed canonical JSON with CRC32; no pickle). **One compensation rule for every source:** a `COMPENSATION` undoes one live DML of the same transaction, matched (1) by `target_ref:<id>` = DML `row_ref:<id>` (same table, newest first), else (2) positionally — the newest live event of the named table (undo records are written in strict reverse order). Spilled events are matched the same way. Sources that resolve savepoints themselves emit no compensation; a randomized property test (interleaved savepoints across tables, with and without spill and refs) requires both paths to give the same result. The assembler is in-memory; restart rebuilds open transactions from `restart_seq` (first batch of the oldest open transaction).

## 8. Snapshots, verification, certification

### 8.1 Snapshots and the table lifecycle

ADR-012, `alelt_hub/lifecycle.py`. One `table_lifecycle` row per enabled table: `NEW → INITIAL_LOAD → CATCH_UP →
STREAMING` (+ `RESYNC`, `DISABLED`); only `lifecycle.move()` writes the phase, holds stay the gate mechanism.

- **Entry points:** every path that makes a table enabled goes through `lifecycle.sync` — catalog ingest (edge agent
  and remote runner), config revisions (file seed, wizard, import, console), a new table under `allow_new_tables`.
  Initial load is the **default**; `initial_load: none` (STREAMING at once) and `replay_from: <pos>` (test / ops:
  catch-up from that position, no snapshot) are explicit. Pre-lifecycle connections that already flushed are migrated
  as `STREAMING, mode=legacy`.
- **Driver:** `lifecycle.drive` (control lane, idempotent): NEW → snapshot start (bounded by
  `ALELT_SNAPSHOT_INFLIGHT_TABLES`, stream-class tables first, then deferred, then window); last chunk loaded → CATCH_UP →
  one replay pass from the start position (bulk in the data lane, attach tail under the pipeline lock) → STREAMING.
- **Gate precedence:** integrity > schema > initial load / catch-up / resync (ignore the schedule window: a schedule
  is a latency class for changes) > schedule (placed at the `CATCH_UP → STREAMING` transition under the pipeline lock,
  at the attach position: exact boundary) > completion (soft gate).
- **Handoff positions:** hold at the flushed position `S0`; the agent's chunk header carries `start_pos = R_a` read
  before its first chunk; catch-up from `max(S0, R_a)`; out-of-row signals inside the catch-up range are queued for
  completion, those before it are dropped (the snapshot rows carry the value). Edge chunks are read `WITH CS` and
  carry `last_key`: an unanswered / failed SNAPSHOT_TABLE is re-issued with `key_from` and the same snapshot id
  (`ALELT_SNAPSHOT_CHUNK_TIMEOUT_S`, `ALELT_SNAPSHOT_MAX_REISSUES`, then paused = terminal).
- **Verification:** post-snapshot reconciliation sets `verified_at` (required by certification); a settled MISMATCH
  moves the table to RESYNC (bounded by `ALELT_LIFECYCLE_MAX_RESYNC`).

Remote engines use their consistent-snapshot facility; Db2 reads `WITH CS` (currently-committed semantics), never
uncommitted data.

### 8.2 Verification

`alelt_verify` default suite: row count + column aggregates (count, min, max, exact sum) per key range + sampled rows; exact key diff only inside ranges that differ; canonical comparison (CHAR padding, decimal scale, time zones, bytes). Keyless tables add a `ROW_MULTISET` check. Edge sources are verified **on the agent** by the whitelisted `VERIFY_TABLE` command (server-side aggregates, `WITH CS`), so the source table is never materialised in the hub. Parity against an existing pipeline is a **destination-side** compare of two landing schemas in the same destination; vendor-specific column mappings are data provided by extensions.

### 8.3 Certification

Per table: row-count, key-set (multiset for keyless), column-checksum, delete parity, `LATENCY_P95`, model shadow diff, 7 consecutive clean days. Computed **only from recorded evidence**: `LATENCY_P95` is the p95 (nearest rank, 24 h) of the recorded commit → apply latency of each flush (`batch_table_stat.created_at − commit_ts_min`, an upper bound of the per-row latency), `UNKNOWN` without evidence — never the capture lag. The clean-day streak is computed by the single evaluator `alelt_verify.certification.evaluate` (every required check PASS on each of the last 7 complete UTC days, no failure today). Sign-off is refused unless every criterion is PASS and the table is IN_SYNC.

## 9. Durability, HA and failure model

### 9.1 Receipt state machine (`POST /agent/v1/blocks`, same path for remote runners)

| Step | Action | Failure ⇒ |
|---|---|---|
| S0 | decompress, check sha256 / raw size | 422, nothing stored |
| S1 | lease pre-check | 423, nothing stored |
| S2 | blob `put`, confirmed by the store (fsync + rename / S3 PUT 200 with Content-MD5) | 5xx, orphan object possible |
| S3 | one metadata transaction: lock connection, re-check duplicate, re-check fence, chain check, insert `batch` + `decode` job | 409 gap / 423 fence (object deleted unless a committed row references it) |
| S4 | ACK `durable_pos` | lost ACK ⇒ re-send ⇒ `duplicate: true` |

Duplicates are detected by `batch_id` (+ sha256; different content ⇒ 409). Backlog over the limit ⇒ 503 + `Retry-After`; the agent keeps its spool. Orphan objects from S2/S3 failures are removed by a mark-and-sweep GC (`alelt_hub/spool_gc.py`) that only touches batch/snapshot key layouts and only after two sightings at least an hour apart.

### 9.2 Capture lease and fencing

One lease row per connection (`holder, epoch, lease_id, expires_at`), TTL by the **hub clock** only. Batches must carry the holder's epoch once a lease exists (else 423). A standby acquires after expiry (`takeover`, epoch + 1), discards its local spool and resumes at the hub's durable position; an active agent that cannot renew within `ttl − renew` stops reading immediately (self-fencing). Agent wall clocks are never used for fencing.

### 9.3 Workers and jobs

Jobs live in the metadata database (atomic claim on both dialects). A running job is heart-beaten every 30 s; only jobs without a heartbeat for 120 s are requeued, and `finish`/`fail` are conditional on still owning the job, so a zombie worker cannot complete a job another worker took over. One worker per stream (lock `pipeline:<cid>`). **SQLite metadata = exactly one worker process** (a second worker exits 78).

- **Lanes:** one process, two claim loops — `data` (`decode`, bulk `replay`, `snapshot_load`; `ALELT_WORKER_DATA_THREADS`)
  and `control` (`integrity` 10, `completion` 20, `lifecycle` 30, `snapshot` 40, `schema` 50, `verify` 60,
  `resync`/`reprocess` 70, `maintain` 90; `ALELT_WORKER_CONTROL_THREADS`). Claim order `(priority, run_after, id)`; each
  lane thread has its own pipeline objects (stream state lives in the data lane).
- **Stream marker:** a received batch queues one `decode` marker per connection (dedup key `decode:<cid>`, cleared on
  claim so a batch received during a run queues the next one). A decode run is a time slice (`ALELT_DECODE_SLICE_S`,
  20 s); a continuation is the same marker, so other connections' markers run in between.
- **Under the pipeline lock** (data lane, bounded): decode → assemble → flush of the main stream, the completion step
  (collect / issue: metadata only), attach tails (<= `ALELT_ATTACH_TAIL_MAX_BATCHES`, else another bulk round first),
  placing / releasing holds, schedule tick. **Not under it:** bulk replay passes (catch-up included), fetch waits,
  snapshot chunk loads, destination DDL (control-lane `schema` job: approved schema changes, DDL retries), snapshot
  dispatch, verify, integrity steps. Rule: under the pipeline lock nothing waits on a network peer, a destination DDL
  or an unbounded replay. A control step that needs the lock writes `pipeline_request(cid)`; the running slice ends at
  its next chunk (cooperative yield).
- **Flush discipline:** a flush with nothing to apply opens no destination transaction (mirror advances,
  `dest_position` kept); a checkpoint-only transaction at most every `ALELT_CHECKPOINT_IDLE_S` and at shutdown; the
  slice-end flush runs only when the buffer holds rows.
- **Backpressure / lag:** `max_unloaded_batches` → 503 + `Retry-After` (the agent keeps its spool);
  `alelt_decode_lag_batches` / `alelt_decode_lag_seconds`, alert `AleltDecodeLag` (> 5 min for 10 min).

### 9.4 Recovery objectives

| Event | Outcome |
|---|---|
| agent / runner killed | resume from the hub durable position; no gap, no duplicate |
| worker killed (any point, incl. after destination COMMIT before the mirror update) | fresh worker replays from `restart_seq`; destination skips applied change sets |
| hub replica lost (HA) | other replicas continue; spool on S3, metadata on PostgreSQL HA |
| hub spool lost, standalone | re-read from the source at the durable position — **RPO 0 only while source log retention ≥ hub rebuild time** (archive-horizon metric and alert; source log retention is owned by the DBA) |
| destination down | batches accumulate (bounded by backpressure); apply resumes, exactly-once by checkpoint |

HA ↔ standalone switch: `alelt topology plan|apply` (drain, copy spool with verification, migrate metadata, generated config) — [`deploy/HA.md`](deploy/HA.md).

## 10. Security

| Control | Design |
|---|---|
| Outbound only | agents open connections to the hub; no listening port on the database host |
| Agent identity | mTLS (thumbprint pinned per agent) terminated at a proxy whose address is allow-listed, or per-agent credentials issued through one-time enrolment tokens bound to one connection; shared static tokens are refused when `ALELT_ENV=prod` |
| Whitelisted commands | typed commands only; the agent validates arguments against the catalog and a local allow list; no SQL text on the wire |
| Source privileges | read-only capture user; Db2 `db2ReadLog` privilege minimum documented per release (DBADM fallback), credentials in the OS secret store, not in config files |
| Production guard | deny list (hosts, CIDRs, accounts) enforced by the hub **and** the agent; TEST/PROD separation; host-mapping step on import |
| Console | OIDC, roles Viewer / DataOperator / PlatformAdmin / Auditor, reason required on mutations, two-person rule for switches, everything audited |
| Secrets | references (`file:` / `env:` / vault) only; never in config, logs or Git; support bundle redacted |
| Diagnosis | deterministic rules first; an optional LLM sees only redacted evidence, its output is never executed; code-fix proposals are exported for human review as untrusted input |

## 11. Public-API-only capture and provenance

Every capture uses each vendor's official, publicly documented log/change API ([`docs/PUBLIC-APIS.md`](docs/PUBLIC-APIS.md)); every destination only its public APIs. No vendor code or headers are included; client libraries are obtained by the user. Where documentation does not spell out every byte of a record the documented API returns, the layout was **observed on our own test databases** — never obtained by decompilation, disassembly or tracing of vendor binaries — and each such field is listed in [`docs/legal/PROVENANCE.md`](docs/legal/PROVENANCE.md). Undocumented record kinds are not decoded: they are signalled and the affected rows are re-read by key with a plain `SELECT`. The static test `tests/static/test_public_api_only.py` is a lint (file and API names), not a proof.

## 12. Configuration, upgrades, operations

- **One config source of truth:** the metadata database; every change is a `config_revision`; YAML in Git is import/export (`alelt config export|import`), drift is reported, never applied silently (ADR-008).
- **Upgrades:** forward-only, additive schema migrations plus numbered steps; a database newer than the code is refused; `alelt migrate --check` gates rollouts; hub ≥ agent, agent N-1 supported ([`deploy/UPGRADE.md`](deploy/UPGRADE.md)).
- **Sync modes:** soft delete / log (default), snapshot-only. **HISTORY mode is not supported in 0.1** and is rejected by config validation and the importer.
- **Initial load / lifecycle (tables.yaml):** `initial_load: auto | none` per connection and per table (default `auto`,
  env default `ALELT_INITIAL_LOAD`), `replay_from: <pos>` (test / ops only). Schedules (`window`, deferred
  `latency_target_s`) gate **changes** of STREAMING tables only; an initial load, catch-up or resync lands whatever the
  window.
- **Knobs (this release):** `ALELT_COMPLETION_KEYS_PER_CMD` (1000), `ALELT_COMPLETION_CMD_BYTES` (48 KiB),
  `ALELT_COMPLETION_INFLIGHT_CMDS` (4), `ALELT_COMPLETION_STALL_S` (600), `ALELT_COMPLETION_PENDING_MAX` (200 000),
  `ALELT_COMPLETION_CMD_TIMEOUT_S` (360), `ALELT_CHECKPOINT_IDLE_S` (flush cadence, min 60), `ALELT_SNAPSHOT_INFLIGHT_TABLES`
  (8), `ALELT_SNAPSHOT_CHUNK_TIMEOUT_S` (600), `ALELT_SNAPSHOT_MAX_REISSUES` (5), `ALELT_LIFECYCLE_MAX_RESYNC` (2),
  `ALELT_WORKER_DATA_THREADS` / `ALELT_WORKER_CONTROL_THREADS` (1 / 1), `ALELT_ATTACH_TAIL_MAX_BATCHES` (200),
  `ALELT_DECODE_SLICE_S` (20).
- **Metrics / incidents / support bundle:** Prometheus `/metrics`, deterministic incident rules with dedup, redacted support bundle.
- **Throughput (measured, local, one worker, PostgreSQL landing, `tests/ladder/L6/soak_local.py`):** receipt ≈ 49 000 rows/s, apply ≈ 1 500 rows/s per stream; per-engine warehouse figures are published only from real runs. Shared flush cadence (F-22): the main stream applies all tables of a connection in ONE destination transaction per cadence = `ALELT_FLUSH_CADENCE_S` (else `ALELT_LOAD_MAX_LATENCY_S`), bounded by the smallest `schedule.latency_target_s` of its enabled tables; row / buffer caps still flush earlier. A connection whose tables accept 15 min costs one warehouse transaction per 15 min; deferred schedule classes are released in shared replay passes.

## 13. Test strategy and evidence

| Layer | What it proves | Where |
|---|---|---|
| Unit / property | canonical values, assembler (incl. randomized savepoints × spill), verify suite, certification evaluator, jobs liveness | `hub/*/tests` |
| Contract / conformance | every destination: atomic apply, crash before COMMIT, replay idempotence, partial images, types | `connectors/destinations/*/tests`, `alelt_dst_kit` |
| Integration (in-process hub, SQLite **and** PostgreSQL metadata) | receipt crash points (after put, after commit), duplicate race, worker crash after destination COMMIT + destination-level replay ×3, long transaction across worker loss with spill, clock skew, orphan GC | `hub/alelt_hub/tests` |
| Local e2e | real source engines → hub → real destinations, kill -9 of runners, HA failover, topology switch | `tests/ladder/L4-local` |
| Formal ladder L0–L10 | service lifecycle → production shadow → cutover | `tests/ladder/L*`; the console shows only formal `result.json` files and flags results older than 24 h as **stale**; SKIP is never evidence |
| Publish | clean-export grep (vendor neutrality, internal references, AI-attribution trailers, vendor binaries) | `tools/publish_check.py` |

## 14. Known limitations

| Limitation | Consequence | Plan |
|---|---|---|
| Snowflake verified against mocks only | no exactly-once claim for Snowflake | real-account contract run (two concurrent appliers, PUT/COPY failure, suspended warehouse) |
| Db2 decoder: out-of-row LOBs / long VARCHAR, DECFLOAT, XML stored out of row | partial image applied at once, out-of-row values completed by key (bounded lag, ADR-013); keyless tables with out-of-row columns: snapshot-only (resync) | decode LOB-manager records only from documented layouts |
| Db2 decoder: overflow/pointer sub-records, value compression, non-UTF-8 code pages | signalled → held; never applied as a guess | decode only from documented layouts; certification exclusion until then |
| Keyless duplicate rows collapse | landing holds one row per distinct value | documented; multiset parity |
| Column added at the source before the catalog refresh | value may be dropped until the refresh (integration test marked expected-fail) | catalog-refresh signal on unknown column |
| SQLite metadata | one host, one worker | PostgreSQL for HA / scale |
| Formal ladder L0–L2, L4–L10 not yet run on current code; L3 has one open case (out-of-row LOB now signalled) | no production claim | lab L6 (restored production-shaped database, chaos) before any shadow run |
