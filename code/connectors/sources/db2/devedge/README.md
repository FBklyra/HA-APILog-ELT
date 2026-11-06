# devedge — Db2 dev edge agent (Python, stdlib only)

Stand-in for the C Windows service; speaks the same agent ↔ hub protocol v1. **Dev only**: token authentication only (no mTLS client certificate, no server pinning, no watchdog / service integration); production edges are `edge-agent/core` (Windows) and `edge-agent/linux` (`aleltd`). Per-agent credential (`credential.v1`) is stored in `credential_file` (0600, atomic); state and spool writes are atomic (fsync + rename + dir fsync); `START_CAPTURE {from_pos, discard_spool: true}` discards an unshippable spool. Needs an **x64** Python (Db2 DLLs are x64); tested on 3.10.

| Step | Detail |
|---|---|
| Connect | Db2 CLI `SQLConnect` only (ADR-010), autocommit off, read-only (R1) |
| Catalog | SYSCAT tables/columns/keys `WITH UR` → COMMIT → `db2ReadLog QUERY` → `POST /agent/v1/catalog` (same shape as `alelt_src_db2.catalog`) |
| Capture | `db2ReadLog READ` (filter ON) from the resume position; body = raw records exactly as returned; zlib; sha256 of the uncompressed body |
| Positions | `from_pos` = start of the READ chain (= previous `next_pos`), `to_pos` = last record LRI, `next_pos` = nextStartLRI |
| Spool | fsync'd local file per batch; deleted only up to the hub's `durable_pos`; spool cap → stop reading (never drop) |
| Resume | local spool → hub `resume_from` → state file → config `start` (`current` / `oldest` / LRI) |
| Control | polls `GET /agent/v1/commands`; PAUSE, RESUME, SET_LIMITS, PROBE (whitelist), SNAPSHOT_CATALOG, START_CAPTURE (sends `X-ALELT-Reposition`), SHUTDOWN; acks each |
| Ops | heartbeat (CPU %, working set, spool bytes), CPU governor (`limits.cpu_pct`) |
| HA | `"ha": {"mode": "active_passive", "lease_ttl_s": 30, "renew_s": 10}` (X-48, same as `aleltagent.json`): STANDBY until the hub grants the capture lease; renews every `renew_s`; self-fences after `ttl - renew` without renew or on 409/423; takeover ⇒ local spool discarded, resume at the hub `resume_from`; batches carry `X-ALELT-Lease-Epoch`. Logic: `hub/alelt_common/lease_client.py` (shared). e2e: `e2e_local.py --ha` |

Db2 code (ctypes CLI + db2ReadLog, catalog, batch builder) is the shared library `alelt_db2edge/` (Windows + Linux), also used by the Linux agent `edge-agent/linux/aleltd`.

Config: `devedge.json` (relative paths resolve against the file); agent token from env `ALELT_AGENT_TOKEN` (R9).

Run: `hub\scripts\start-devedge.ps1 [-Once]`. End-to-end examples are coming in a later release.
