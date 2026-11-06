# edge-agent/core — `aleltagent.exe` (Windows service `ALELTAgent`)

Generic C11 edge agent: SCM service with built-in installer, outbound-only hub link
(WinHTTP, mTLS + server SPKI pinning), durable spool with ACK-based deletion and
backpressure, whitelisted commands (R5), governor, Job Object self-limits, structured
JSON log + Event Log. Engine code lives in source plugins
(`connectors/sources/<engine>/edge`, ABI in `include/alelt_plugin.h`).

## Build

Requirements: Visual Studio 2022+/18 C++ x64 tools (MSVC), CMake ≥ 3.21 + Ninja (bundled with VS),
Windows SDK, and a Db2 client SDK for the Db2 plugin (`DB2_HOME`, default `D:/IBM/SQLLIB`;
IBM headers/libs are used in place, never copied into the repo).

```powershell
.\build.ps1                 # finds VS (vswhere, incl. Insiders), loads x64 vcvars, configure + build + ctest
.\build.ps1 -Preset x64-debug -NoTest
```

or manually from a "x64 Native Tools" / VsDevCmd -arch=x64 prompt:

```bat
cd edge-agent\core
cmake --preset x64-release            :: build dir: edge-agent\core\build\x64-release (git-ignored)
cmake --build --preset x64-release
ctest --preset x64-release            :: unit tests + zlib cross-check (Python) + Db2 plugin tests
```

All targets compile with `/W4 /WX /sdl /guard:cf` (link `/guard:cf /DYNAMICBASE /NXCOMPAT /CETCOMPAT`).
Fuzzing: `cmake --preset fuzz-clang && cmake --build --preset fuzz-clang` (needs clang-cl with
libFuzzer), then `build\fuzz-clang\bin\fuzz_cmd.exe` / `fuzz_json.exe`.

Output (`build\<preset>\bin`): `aleltagent.exe`, `aleltsrc_db2.dll`, `alelttests.exe`, `db2tests.exe`.

## Run in the console (dev)

```bat
aleltagent.exe --init-config --config C:\aleltdev\aleltagent.json --mode dev --hub-url http://127.0.0.1:18080 --database <DATABASE>
:: edit C:\aleltdev\aleltagent.json (spool/log dirs, source.settings.catalog_schemas, snapshot.allowed_tables)
set ALELT_AGENT_TOKEN=devtoken              :: dev mode only; or: aleltagent.exe --set-dev-token (DPAPI file)
aleltagent.exe --console --config C:\aleltdev\aleltagent.json     :: Ctrl+C stops
```

A Postgres-free mock hub for quick runs:
`python tests\mock_hub.py --out <dir> [--script cmds.json]`.

## Install / uninstall (elevated)

```bat
aleltagent.exe --install --config C:\ProgramData\ALELT\Agent\aleltagent.json --account "CORP\svc-alelt$" ^
            --hub-url https://hub:8443 --agent-id edge-01 --connection-id erp --database <DATABASE> ^
            --thumbprint <client cert SHA-1> --pin <hub SPKI SHA-256> --spool-dir E:\ALELT\spool
aleltagent.exe --status | --start | --stop
aleltagent.exe --uninstall                  :: keeps config, spool and logs
```

`--install` creates/updates the service (auto start, delayed), recovery actions (restart 60/120/300 s,
also on non-zero exit), unrestricted service SID, Event Log source, "Log on as a service" for user
accounts, and ACLs (config: SYSTEM/Administrators full, service read; spool/log: service modify).
Passwords are read with `--password-stdin` only (never on the command line). Default account:
`NT SERVICE\ALELTAgent`. Production: see `runbooks/agent-install.md` and `deploy/agent`.

## Configuration (`aleltagent.json`)

Strict: unknown keys are rejected (keys starting with `_` are comments). See `aleltagent.example.json`.
`limits` are clamped to `limits_max` (DBA-approved ceilings); hub `SET_LIMITS` is clamped the same way.
`require_job_limits` (default true) refuses to run if the Job Object cannot be applied.
`snapshot.allowed_tables` is the **local** allow-list: `SNAPSHOT_TABLE` for anything else is refused.

## High availability (active/passive)

Two agents can serve one connection: exactly one captures, the other waits. The hub grants a
**capture lease** per connection (holder, `lease_id`, `epoch` fencing token, `ttl_s`; see `deploy/HA.md`).

```json
"ha": { "mode": "active_passive", "lease_ttl_s": 30, "renew_s": 10 }
```

| Key | Values | Default |
|---|---|---|
| `ha.mode` | `standalone` (no lease, as before) · `active_passive` | `standalone` (also when `ha` is absent) |
| `ha.lease_ttl_s` | 5..300 (the hub may cap it) | 30 |
| `ha.renew_s` | 1..`lease_ttl_s`/2 | 10 (`lease_ttl_s`/3 if 10 does not fit) |

Behaviour (`src/agent/ha.c` = pure decision logic with unit tests, `ha` thread in `agent.c`):

| State | What the agent does |
|---|---|
| **STANDBY** (heartbeat `state:"STANDBY"`) | registers and heartbeats; calls `POST /agent/v1/lease/acquire` every `renew_s`; never opens the source, never READs, never ships; plugin commands (`START_CAPTURE`, `SNAPSHOT_*`, `PROBE`) are acked `ok:false`; `PAUSE`/`RESUME`/`SET_LIMITS`/`SHUTDOWN` still apply (a drain PAUSE survives a takeover). |
| granted, `takeover:false` | same holder as before (restart inside the ttl, short hub outage): local spool kept, capture continues. |
| granted, `takeover:true` | another agent held the lease: the local spool is stale ⇒ every spooled batch is deleted and capture restarts at `resume_from` (hub durable position = last batch `next_pos`, X-25 chain stays contiguous). No `resume_from` (hub has no batch yet) ⇒ capture waits for `START_CAPTURE`. |
| **ACTIVE** | renews every `renew_s`; every batch carries `X-ALELT-Lease-Epoch`. The worker checks the lease before every READ and again (under a lock) before spooling. |
| lease lost | renew answered 409 `lease_lost`, a batch answered 423 `lease_fenced`, or **no successful renew for `lease_ttl_s - renew_s`** (self-fencing: the agent stops before the hub can hand the lease over) ⇒ reading stops immediately, back to STANDBY, spool kept until the next grant decides. |
| clean stop | the lease is released, so the standby takes over at its next acquire. |

A standalone agent whose connection runs active/passive at the hub gets 423 for every batch; it
keeps its spool, logs an error and retries slowly (set `ha.mode`).

Db2 HADR pairing: install one agent on the HADR primary host and one on the standby host, same
`connection_id`, different `agent_id`, both `ha.mode = active_passive`. Only the lease holder connects
to Db2 (an HADR standby database is not connectable); after an HADR takeover the agent on the new
primary takes the lease (the old one fails to read and its lease expires). LRIs are the same log
stream on both hosts, so capture resumes at the hub durable position without a gap.

Failover timing: crash of the active ⇒ standby captures after at most `lease_ttl_s + renew_s`
(≈ 40 s with defaults; measured 10 s with `lease_ttl_s=10, renew_s=3` against the real hub). Clean
stop ⇒ about `renew_s`. Process crashes on the same host are also covered by the service recovery
actions (`--install`: restart after 60/120/300 s). No data loss and no duplicates: the hub rejects
batches from a non-holder or a stale epoch, and the destination checkpoint stays the truth.
Drill: `runbooks/agent-failover-drill.md`.

## Wire protocol v1 (matches `hub/alelt_common/models.py` field-for-field)

Headers on every request: `X-ALELT-Protocol: 1`, `X-ALELT-Agent-Id`; dev mode adds `X-ALELT-Agent-Token`.

| Call | Request | Response |
|---|---|---|
| `POST /agent/v1/register` | `RegisterRequest` JSON | `RegisterResponse` (`resume_from`, `limits` honoured) |
| `POST /agent/v1/heartbeat` | `Heartbeat` JSON every `heartbeat_s` | 2xx; 4xx ⇒ agent re-registers |
| `GET /agent/v1/commands?agent_id=..&wait=N` | long-poll | `[Command…]` (also accepts `{"commands":[…]}` / 204) |
| `POST /agent/v1/commands/{id}/ack?agent_id=..` | `{ok, result}` for **every** command (accepted or rejected) | 2xx |
| `POST /agent/v1/blocks` | header `X-ALELT-Batch: <BatchHeader JSON>` (+ `X-ALELT-Reposition: <command_id>` after `START_CAPTURE{from_pos}`, + `X-ALELT-Lease-Epoch` when active/passive), body = codec(raw) | `BatchAck`; 409 gap ⇒ kept + retried, 423 fenced ⇒ STANDBY, 503 ⇒ backoff |
| `POST /agent/v1/catalog` | `CatalogSnapshot` JSON | 2xx |
| `POST /agent/v1/snapshot` | header `X-ALELT-Snapshot: <SnapshotChunkHeader JSON>`, body = zlib(JSON lines) | 2xx |
| `POST /agent/v1/events` | `AgentEvent {agent_id,type,command_id?,detail{},at}` (best effort) | 2xx |
| `POST /agent/v1/lease/acquire\|renew\|release` | `{agent_id, connection_id, ttl_s, lease_id?, epoch?}` (only `ha.mode=active_passive`) | `LeaseGrant`; renew 409 = lease lost |

* Batch body: concatenation of plugin records, engine-native. Db2: the db2ReadLog filter-ON buffer
  verbatim (`[56-byte db2ReadLogFilterData][log record]…`, DESIGN V3). `sha256` = hex of the
  uncompressed body; `raw_bytes` = its length; `codec` = `zlib` (or `none` when not smaller).
* `zlib`: in-house RFC 1950/1951 encoder (fixed-Huffman + LZ77, stored fallback), decodable by any
  inflater; chosen over zstd to stay dependency-free. The hub accepts `none|zlib|zstd`.
* Spool deletion: only batches with `to_pos <= durable_pos` of an ACK (plugin comparator). An ACK
  whose `durable_pos` is behind the batch keeps it and retries with backoff.
* Command results: ack `result` = `{"message": ..}`, the probe JSON for `PROBE`, `{"from_pos": ..}` for
  `START_CAPTURE`; rejected commands are acked `ok=false` (hub marks them FAILED). `AgentEvent` is only
  for agent-originated events: `PAUSED_BY_GOVERNOR`, `GOVERNOR_RESUMED`, `COMMAND_REJECTED` when the
  command id is unreadable. Stored by the hub in the audit log as `agent.event.<type>`.
* Positions (X-25): `from_pos` = start of the READ chain = previous batch `next_pos`; empty READs make no
  batch and do not move `from_pos` (only the in-memory read cursor). `START_CAPTURE{from_pos}` requires an
  empty spool and makes the agent send `X-ALELT-Reposition` with the first batch starting there.
* Commands (R5): exactly `command_id, name, args, issued_at`; per-command args are strict:
  `START_CAPTURE{from_pos?: Position|text}`, `PAUSE`, `RESUME`, `SHUTDOWN` (no args),
  `SET_LIMITS{cpu_pct?,mem_mb?,batch_kb?,max_rate_kbps?,poll_ms?}`, `SNAPSHOT_CATALOG{schemas?}`,
  `SNAPSHOT_TABLE{table:{schema,name}, key_from?, key_to?, chunk_rows?, snapshot_id?}`,
  `PROBE{name ∈ current_position|db_health|log_config}`. Anything else ⇒ rejected, logged, audited.
* `SNAPSHOT_TABLE`: `key_from` is an **exclusive** lower bound, `key_to` inclusive; chunks are
  keyed by primary key; each header's `key_to` is the last key in that chunk (resume point).
  Rows are JSON objects `{column: text|null}` (Db2 CLI character rendering; binary as hex).
* `max_rate_kbps` is kilobits per second (upload pacing).

## Position semantics

Resume point after restart = `next_pos` of the newest spooled batch, else `state.json`, else
`RegisterResponse.resume_from`, else (on `START_CAPTURE` without `from_pos`) the current end of log.
`state.json` also persists `capturing`, so a crash/kill resumes capture with no gap.

## Layout

`include/` ABI + common headers · `src/common` buffer, JSON, sha256 (BCrypt), zlib, OS helpers ·
`src/agent` service, config, log, Job Object, WinHTTP link, hub protocol, spool, commands, governor,
runtime · `res/` version + Event Log message table · `tests/` unit tests, fuzz targets, mock hub.

## Production guard, credentials, recovery (review fixes AHS-04/05/08/09/11/25)

- `"environment": "PROD"` starts only when the DBA created the local file `allow_prod.dba` next to `aleltagent.json`
  (any content). `"allow_prod"` in the JSON is ignored: that file may be generated by the hub.
- `deny_hosts`: IPv4/IPv6 literals and CIDRs, wildcards, trailing-dot FQDNs. A source host name that does not resolve
  while an address entry has to be checked = refused (fail closed). A catalogued Db2 alias is only checked by name.
- Token mode: the enrollment token is exchanged at the first registration for a per-agent credential, stored DPAPI
  (machine scope) in `<spool dir>\credential.bin`, used from then on and rotated at 2/3 of its lifetime.
  The devtoken.bin file is preferred over the environment variable. `http://` to a non-loopback hub is refused
  unless `hub.allow_insecure_http: true` (isolated labs only).
- `hub.proxy`: `""` = direct (default, no WPAD discovery), `"system"` = WinHTTP automatic proxy, or `host:port`.
- Spool blocked by a position gap (409): event `SPOOL_BLOCKED_409`; recover with the audited command
  `START_CAPTURE {"from_pos": "<pos>", "discard_spool": true}` (console confirm text `DISCARD SPOOL <agent>`).
  A batch the hub already covers (`expected_from` past its `to_pos`) is deleted automatically (`SPOOL_BATCH_COVERED`).
- `--install` never starts a service whose config does not load; an invalid config or a guard refusal stops the
  service cleanly (no restart loop). Test: `python tests/e2e_mock_scenarios.py` (credential + 409, no database).

Command contract (arguments, results, typed error codes, command thread, allow-list `SCHEMA.TABLE` / `SCHEMA.*`):
[`docs/AGENT-COMMANDS.md`](../../docs/AGENT-COMMANDS.md); shared allow-list vectors:
[`edge-agent/shared/allowlist_vectors.json`](../shared/allowlist_vectors.json).
