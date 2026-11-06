# alelt_hub — agent gateway, durable receipt, job queue, console API

| Area | Module |
|---|---|
| Settings (`ALELT_*` env; secrets only from env / secret store, R9) | `settings.py` |
| Metadata schema (ERD + `job`, `catalog_version`, `checkpoint_mirror`, `agent_command`, `snapshot_chunk`, console tables), portable PostgreSQL / SQLite | `db.py`; `python -m alelt_hub.migrate [--check]` (create + additive columns) |
| Dialect layer (the only backend-aware code): engine + SQLite pragmas, UTC timestamps, upsert, job claim, advisory lock, migration DDL | `dialect.py` |
| Agent gateway, protocol v1 (`X-ALELT-Protocol: 1`): register, blocks, catalog, snapshot, heartbeat, `WS /agent/v1/control`, long-poll `GET /agent/v1/commands` + `POST /agent/v1/commands/{id}/ack` (all command results), `POST /agent/v1/events` (`AgentEvent`, agent-originated events, stored in the audit log) | `gateway.py` |
| Durable receipt: sha256 → spool (fsync + atomic rename) → `batch` row + `decode` job → `ACK{durable_pos}`; idempotent on `batch_id`; gap check; 503 backpressure | `gateway.py`, `spool.py` |
| Job queue (atomic claim: `FOR UPDATE SKIP LOCKED` on Postgres, `BEGIN IMMEDIATE` + `UPDATE … RETURNING` on SQLite; backoff, stale requeue) | `jobs.py` |
| Worker `python -m alelt_hub.worker`: decode → alelt_txn → alelt_loader → destination; per-connection advisory lock (Postgres advisory lock / OS file lock on SQLite); snapshot hold + `catch_up()` (X-22); metrics `:9100`; liveness file | `worker.py`, `pipeline.py` |
| Console API `/api/v1` — all endpoints of `console/API_CONTRACT.md`; permission matrix = `console/src/lib/rbac.ts`; `reason` + `confirm_text` + two-person rule enforced server-side; one `audit_event` per mutation with `X-ALELT-Correlation-Id` | `console.py`, `auth.py` |
| `/metrics` (names per `deploy/hub/observability/METRICS.md`), `/healthz`, `/readyz` | `app.py`, `metrics.py` |
| Incidents & self-diagnosis (`/api/v1/incidents`, `/diagnose/explain`, `/admin/llm`, agent-gateway observer); engine in `hub/alelt_diagnose` (rules, redaction, LLM providers, code-fix, source-data) | `diagnose_api.py` |
| Test doubles: `FakeSource` (JSON-lines decoder), `FakeAgent` | `testing.py` |

## Choosing the metadata database
One setting, `ALELT_DB_URL`; nothing else changes. PostgreSQL stays the default and the recommendation for production.

| | SQLite | PostgreSQL (default) |
|---|---|---|
| `ALELT_DB_URL` | `sqlite:////data/alelt.db` (container, volume `/data`) · Windows `sqlite:///C:/ALELT/data/alelt.db` | `postgresql+psycopg://user:pw@host:5432/alelt` |
| Setup | zero: file + parent dir created on first start | a Postgres 16 server/database |
| Topology | **single host**: one API + a few workers (≈ 1–4) on the same local volume | any: many workers, several hosts, HA |
| Size | small installs (few connections, modest batch rate) | production, large backlogs, many streams |
| Backups / HA | file copy while stopped, or `sqlite3 alelt.db ".backup x.db"`; no replication | standard Postgres tooling (PITR, replicas) |
| Concurrency | one writer at a time (`BEGIN IMMEDIATE`, `busy_timeout` 15 s); readers never block (WAL) | row-level locks, `SKIP LOCKED` |

SQLite connections set `journal_mode=WAL`, `busy_timeout=15000`, `foreign_keys=ON`, `synchronous=NORMAL`.
Limits: **no network filesystems** (NFS/SMB/Azure Files: WAL and file locks are unreliable there); keep `<db>.locks/` (per-stream
worker locks) next to the database file; all processes on the same host. Move to Postgres for HA, more workers or many hosts.
Switching backends is a fresh install of the metadata (no built-in copy tool); the destination checkpoint stays the truth.

Schema rules for new tables / columns (both backends): types `UUID`, `J`, `TS` (UTC), `BIGID` for autoincrement keys from `db.py`;
defaults `utc_now()`, `false()`, `true()`, constants; upserts via `dialect.insert`; no raw Postgres SQL in shared code (put it in
`dialect.py`). Additive columns (via `migrate`) must be nullable or have a constant default (SQLite rejects `utc_now()`/UNIQUE there).

## Identity
- Agents: `ALELT_AGENT_AUTH=mtls` → nginx-forwarded `X-SSL-Client-Fingerprint` must be in `ALELT_AGENT_THUMBPRINTS`; `token` (dev) → `X-ALELT-Agent-Token` in `ALELT_AGENT_TOKENS`.
- Console: Entra ID JWT (`ALELT_OIDC_ISSUER`, `ALELT_OIDC_AUDIENCE`, `ALELT_OIDC_JWKS_URL`; app-roles claim). Dev: `ALELT_DEV_AUTH=1` + `X-ALELT-Dev-Role` / `X-ALELT-Dev-User`; startup refuses unless `ALELT_ENV` is dev/local/test.

## Exactly-once / restart
Destination checkpoint (same transaction as the data) is the truth. `checkpoint_mirror.restart_seq` = first batch still needed by an
open source transaction; restart replays from there and skips change sets with commit position ≤ checkpoint.

## Run / test
Tests: `pytest hub/alelt_hub/tests` runs every hub test on SQLite, and again on Postgres when
`ALELT_TEST_PG_URL=postgresql://user:pw@localhost:15435` (container `alelt-test-pgland`) is set (throw-away databases; the Postgres landing destination also needs it).
`test_dialect.py`: 3 worker processes claim 300 jobs (each exactly once), cross-process locks, UTC, upsert, migrate.
