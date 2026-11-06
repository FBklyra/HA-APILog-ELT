# edge-agent/linux — `aleltd` (Linux service `alelt.service`)

Python (>= 3.10) counterpart of the Windows service `aleltagent.exe` (`edge-agent/core`): same `aleltagent.json` keys,
same protocol v1, same spool / `state.json` / HA lease rules. Install + operations: [`deploy/linux/README.md`](../../deploy/linux/README.md).

| Module | Role |
|---|---|
| `__main__.py` | CLI, signal handlers (only set events; wakeup-fd watcher so a blocked main thread still starts the deadline) |
| `daemon.py` | main loop (register, lease, heartbeat, commands, governor, ship spool, capture step), `Event.wait` sleeps, SIGHUP reload, deadline + watchdog threads |
| `checkpoint.py` | `save_checkpoint()`: flush in-flight → atomic `state.json` → drain spool (bounded) → release lease; `atomic_write` |
| `spool.py` | fsync'd `<seq>.batch` / `<seq>.catalog` entries, release up to the hub durable position, cap = backpressure |
| `config.py` | strict validation; Windows-only keys accepted and ignored; Linux keys (`client_cert_file`, `token_env`, `shutdown`, ...) |
| `hub.py` | outbound HTTP(S), token or mTLS client cert, SPKI SHA-256 pinning, timeouts on every call |
| `lease.py` | adapter over `hub/alelt_common/lease_client.py` (X-64) |
| `guard.py` | `deny_hosts` (alelt_import.guard syntax) + `environment: PROD` needs `allow_prod` |
| `logging_setup.py` / `sdnotify.py` | JSON lines stdout/stderr, journald priority prefix; sd_notify over `NOTIFY_SOCKET` |
| `sources/db2.py` | raw db2ReadLog blocks via the shared library `connectors/sources/db2/devedge/alelt_db2edge` (also used by `devedge.py`) |
| `sources/remote.py` | postgres/mysql/mariadb/sqlserver/oracle: `SourceConnector.stream()` → canonical JSON Lines batches (REMOTE.md §4, same batch ids as `hub/alelt_capture`) |
| `sources/fake.py` | deterministic test source (refused with `environment: PROD`) |

Run in a console (any OS): `PYTHONPATH=edge-agent/linux:hub:connectors/sources/db2/devedge python -m aleltd --config aleltagent.json`
(Ctrl+C / Ctrl+Break stop gracefully).

## Robustness contract (review LNX-01..32)

| Situation | Behaviour |
|---|---|
| Corrupt / truncated spool entry, body sha256 or `raw_bytes` mismatch, hub 422 / 413 | entry (and every later one) moved to `spool/quarantine/`, event `SPOOL_ENTRY_QUARANTINED`, capture re-reads from its `from_pos` (or the hub position); 413 also halves `batch_kb`; the same 422 three times = shipping held + `BATCH_REJECTED` |
| Hub 409 | batch already covered by `expected_from` = deleted (`SPOOL_BATCH_COVERED`); else `SPOOL_BLOCKED_409` + slow retry. Operator way out: `START_CAPTURE {from_pos, discard_spool: true}` (same contract as the Windows agent; `SPOOL_DISCARDED`) |
| Full spool volume (ENOSPC / < 64 MB free) | backpressure like `spool.max_mb` (`SPOOL_FULL`), never fatal |
| Hub 423 with `ha.mode=standalone` | exit 78 (`HA_MODE_MISMATCH`); 426 = `PROTOCOL_UPGRADE_REQUIRED` + slowest pace; 429/503 honour `Retry-After`; every backoff has +-20 % jitter |
| Source plugin / settings refused at startup, invalid config | exit 78 |
| Forced stop at `shutdown.deadline_s` | exit 1 + `SHUTDOWN_FORCED` |
| Source failures | `DB2_AUTH_FAILED`, `LOG_NOT_AVAILABLE` (REQ-17, throttled 60 s), `TXN_TOO_LARGE` (raise `capture.max_txn_mb`) |
| Connector contract | SPI S1: `stream()` yields `None` at least every ~1 s. The agent is single-threaded: a longer block stalls heartbeats, lease and watchdog (`SOURCE_STALLED` event; systemd's watchdog restarts a truly hung connector). `sources.remote.check_idle_ticks()` is the conformance check for new connectors. Every hub call is capped below `WatchdogSec/2`. |
| Hub URL | `https://` required except a loopback hub, `hub.mode=dev` or `hub.allow_insecure_http: true` (isolated labs only) |
| Secrets | never inline in `source.settings` (refused): `source.password_env` / `password_file` (`LoadCredential=`) |
| Deny list | normalised (case, trailing dot, `[v6]:port`), unresolvable host = refused (fail closed); Db2: re-checked after connect against `SYSIBMADM.ENV_SYS_INFO.HOST_NAME` (an alias resolves through the node directory) |
| Per-agent credential (REQ-26) | capability `credential.v1`; `alelta_` stored in `<spool_dir>/credential.json` (0600, atomic), used instead of the enrollment token incl. after restart, rotated 7 days (or 10 % of the lifetime) before expiry; a 401 on it falls back once to `ALELT_AGENT_TOKEN` with `AGENT_CREDENTIAL_REJECTED` (then re-enroll) |
| `ALELT_SOURCES` | environment JSON that maps engines to `module:Class` = arbitrary module import: keep `agent.env` `0640 root:alelt` (the unit's `EnvironmentFile=`), never writable by the agent user |

Not yet: `SNAPSHOT_TABLE` (acked `ok:false`, use the hub-side snapshot); Db2 `governor.max_log_util_pct`.
A remote-engine batch shipped by the agent is decoded by the hub only if that engine is in the hub's `ALELT_SOURCES`.

## Tests

| Command | Scope |
|---|---|
| `python -m pytest edge-agent/linux/tests` (or `python -m unittest` in `tests/`) | config, atomic checkpoint + kill between tmp/rename, `save_checkpoint` order + deadline, logging split + journald, sd_notify (Linux), signal → exit 0, stop/restart no gap, forced deadline exit, deny_hosts 78, spool-full backpressure, HA handover |
| `python tests/e2e_container_sigterm.py` | Linux containers: tests inside Linux, `docker stop` → exit 0 + state.json, restart → resume, no gap |

Command contract (arguments, results, typed error codes, command thread, allow-list `SCHEMA.TABLE` / `SCHEMA.*`):
[`docs/AGENT-COMMANDS.md`](../../docs/AGENT-COMMANDS.md); shared allow-list vectors:
[`edge-agent/shared/allowlist_vectors.json`](../shared/allowlist_vectors.json).
