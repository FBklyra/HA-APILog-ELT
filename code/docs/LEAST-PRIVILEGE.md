# Least privilege per source and the hub's security controls (F-12, F-14, F-15)

The capture account should hold only what the public capture API of each engine needs. The wizard and the agent
`PROBE` show fix-it SQL. **ALELT never runs that SQL itself.** The DBA runs it.

## 1. Capture account per source

| Source | Minimum for change capture | Snapshot / re-sync | Never grant | Credential storage |
|---|---|---|---|---|
| Db2 LUW 11.5 (Windows edge agent) | `db2ReadLog` requires SYSADM or DBADM. Use `DBADM WITHOUT DATAACCESS WITHOUT ACCESSCTRL` (database-scoped). IBM lists no other authority (SQLADM / DATAACCESS are not documented as sufficient). X-122: verified on the dev database with SYSADM only (dedicated OS test users need an elevated admin); the DBA runs the 4-user matrix in `runbooks/agent-install.md` §1 *Minimum authority* and grants the lowest passing row; plain `DBADM` is the fallback; record the decision. | `SELECT` on the approved tables only. `EXECUTE` on `SYSPROC.MON_GET_DATABASE` for probes. | SYSADM, SECADM, ACCESSCTRL, any write privilege | Implicit OS authentication (gMSA / service account): no password stored. The agent's hub credential goes in DPAPI / Windows Credential Manager, never in `aleltagent.json`. |
| PostgreSQL (remote) | Role `WITH REPLICATION LOGIN` plus a publication created by the DBA (`CREATE PUBLICATION ... FOR TABLE ...`) and a logical slot | `SELECT` on the published tables | superuser, `CREATEROLE`, ownership of application tables | Hub secret store (`secret_refs`, R9) |
| MySQL 8/9 | `REPLICATION SLAVE`, `REPLICATION CLIENT` | `SELECT` on the captured schemas | `SUPER`, `ALL`, any DML | Hub secret store |
| MariaDB | `REPLICATION SLAVE`, `BINLOG MONITOR` | `SELECT` on the captured schemas | `SUPER`, `ALL` | Hub secret store |
| SQL Server (CDC) | Enabling CDC needs `db_owner` (DBA, once). The capture user only needs `SELECT ON SCHEMA::cdc` plus membership in each capture instance's gating role. | `SELECT` on the captured tables | `db_owner`, `sysadmin`, `ALTER` | Hub secret store |
| Oracle (LogMiner) | `CREATE SESSION`, `LOGMINING`, `SELECT ANY TRANSACTION`, `SELECT_CATALOG_ROLE` (or `SELECT` on the listed `V$` views), `EXECUTE ON SYS.DBMS_LOGMNR` | `SELECT, FLASHBACK` on the captured tables | `DBA`, `SYSDBA`, `ANY TABLE` write privileges (the prerequisite check flags `DBA`) | Hub secret store |

Archive and redo retention belongs to the DBA (R3). Record the guaranteed retention in the console
(`PUT /api/v1/config/archive-retention/{connection}`) or in `ALELT_ARCHIVE_RETENTION_HOURS`. The archive-horizon alerts
(`AleltArchiveRetentionUnknown`, `AleltArchiveHorizonApproaching`, `AleltArchiveHorizonCritical`, `AleltArchiveHorizonPassed`)
are a release requirement (X-129).

## 2. Controls on the agent host

- The agent runs only commands from its compiled whitelist (R5). It never accepts SQL text. A command name or probe
  outside the whitelist is rejected on both sides (`test_security.py::test_command_whitelist_fuzz`).
- Outbound only (R4). The connection is read-only, and every statement is compiled in.

## 3. Hub controls (defaults)

| Control | Default / where |
|---|---|
| Agent identity via mTLS | The certificate header is trusted only from `ALELT_TRUSTED_PROXIES` (the real TCP peer is captured before `X-Forwarded-For`). The proxy deletes and overwrites the header (`deploy/hub/nginx`, `deploy/ha/lb/haproxy-mtls.cfg`, Helm `ingress.agentMtls`). A certificate is bound to the agent row that registered with it. |
| Static shared agent tokens | Refused when `ALELT_ENV=prod`. Use enrollment (`alelte_`) → per-agent credential (`alelta_`) with rotation and expiry (deploy/UPGRADE.md §4). |
| Binding | Every agent call is checked against the bound agent id and connection (heartbeat, batches, catalog, commands, lease, WebSocket). An agent cannot be re-homed to another connection without admin action. |
| Console | OIDC JWT (RS256). OIDC groups are mapped to roles through `group_mapping`. Dev role headers are refused outside dev envs. RBAC is enforced on every route (matrix test). |
| CSRF / CORS | `ALELT_CORS_ORIGINS` allow-list (`*` refused in prod). A state-changing `/api/` request from a foreign `Origin` gets 403. The console authenticates with headers, never cookies. |
| Rate limits | `ALELT_RATE_LIMIT_PER_MIN` (600) per client on `/api/`. `ALELT_AUTH_FAIL_LIMIT_PER_MIN` (30) failed authentications → 429 on every path. |
| Payloads | `ALELT_MAX_BODY_MB` compressed. Inflating is capped by the declared `raw_bytes` and `ALELT_MAX_RAW_MB`, which blocks decompression bombs. |
| Secrets in logs | The JSON log formatter masks agent tokens, URL passwords, `password=`, bearer tokens and PEM keys. The LLM evidence redactor does the same (golden set in `test_security.py`). |
| Serialization | No `pickle` anywhere: transaction spill uses canonical JSON with CRC32 (static test). |
| LLM code-fix proposals | Treat them as **untrusted input**. They come from a model that saw redacted evidence, and are exported for human review only. Never apply one without code review. |

## Agent command thread and allow-list (docs/AGENT-COMMANDS.md)

- Each agent opens a **second read-only source connection** for its command thread (SNAPSHOT_TABLE / VERIFY_TABLE /
  PROBE run there so capture and heartbeats continue). Same user, same privileges: `SELECT ... WITH CS` on
  allow-listed tables, catalog reads `WITH UR`; nothing else. The CPU governor throttles the command thread like the
  capture thread. Plan for one more Db2 connection per agent.
- `snapshot.allowed_tables`: `SCHEMA.TABLE` (case-insensitive after upper-casing) or `SCHEMA.*` (every base table of
  the schema); a schema wildcard (`*.*`) is refused at config load. Identical matcher in the C agent, `aleltd` and
  the dev edge. Installers emit the explicit list of enabled tables by default; `SCHEMA.*` is the documented choice for
  `allow_new_tables: true` connections. The deny list (`deny_hosts`, production guard) is evaluated first.
