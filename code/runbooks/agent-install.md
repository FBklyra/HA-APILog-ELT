# Runbook — install the ALELT Agent on the Db2 server

> Audience: platform team + DBA. The agent runs **on the production ERP Db2 server**:
> read-only (R1), outbound-only (R4), whitelisted commands (R5), never touches archive logs (R3).
> Every step below that changes the server is reversible (see *Rollback*).

## 1. Prerequisites (DBA sign-off required)

| # | Item | Who | Check |
|---|---|---|---|
| P1 | Archive logging on (`LOGARCHMETH1` ≠ OFF) — `db2ReadLog` requires a recoverable database | DBA | `db2 get db cfg for <DB> | findstr LOGARCHMETH` |
| P2 | `DATA CAPTURE CHANGES` on every in-scope table (OQ-02). Without it the table is invisible with filter ON (V8) | DBA | `SELECT TABSCHEMA, TABNAME, DATACAPTURE FROM SYSCAT.TABLES WHERE TABSCHEMA='<SCHEMA>' WITH UR` |
| P3 | Dedicated service account (gMSA preferred: `CORP\svc-alelt$`; else a domain user with a vaulted password). Not an admin, not interactive | AD team | |
| P4 | Db2 authority for that account (below) | DBA | |
| P5 | Spool volume **different from every Db2 data, active log, mirror log and archive log volume**, ≥ 2 GB free + headroom | Platform + DBA | `db2 get db cfg` (paths) vs `spool.dir` |
| P6 | Client certificate for mTLS in `LocalMachine\My`, private key readable by the service account; hub SPKI pin | Platform | `certlm.msc` |
| P7 | Outbound TCP from the server to the hub (443/8443) only; **no inbound rule** | Network | |

### DBA grant (P4) — needed for `db2ReadLog` and read-only catalog/monitor queries

```sql
-- db2ReadLog requires SYSADM or DBADM. DBADM is database-scoped (preferred):
GRANT DBADM WITHOUT DATAACCESS WITHOUT ACCESSCTRL ON DATABASE TO USER "CORP\SVC-ALELT$";
-- Snapshot (initial load / re-sync) needs SELECT on the approved tables only:
GRANT SELECT ON TABLE <SCHEMA>.<TABLE> TO USER "CORP\SVC-ALELT$";
-- Monitor probes (db_health) use MON_GET_DATABASE / SYSIBMADM views:
GRANT EXECUTE ON FUNCTION SYSPROC.MON_GET_DATABASE TO USER "CORP\SVC-ALELT$";
```

Notes for the DBA:
* `db2ReadLog` exposes table data in the log regardless of LBAC (IBM documentation) — this is why the
  account is dedicated, the agent is read-only and its commands are whitelisted.
* If `WITHOUT DATAACCESS` prevents `db2ReadLog` on your build, use plain `DBADM` and record the decision.

### Minimum authority (X-122) — status and test procedure

IBM documents the authorization of `db2ReadLog` as **SYSADM or DBADM** (no other authority is listed). On the dev
database the API was verified with a SYSADM login only: creating the dedicated OS test users needs an elevated
administrator, so the matrix below is run once by the DBA (Db2 on Windows authenticates against OS accounts).

| Test user | Grant (besides `CONNECT`) | Expected per IBM docs |
|---|---|---|
| `alelt_sqladm` | `GRANT SQLADM ON DATABASE` | `db2ReadLog` refused |
| `alelt_dataacc` | `GRANT DATAACCESS ON DATABASE` | `db2ReadLog` refused |
| `alelt_dbadm_nd` | `GRANT DBADM WITHOUT DATAACCESS WITHOUT ACCESSCTRL ON DATABASE` | `db2ReadLog` allowed — **the target minimum** |
| `alelt_dbadm` | `GRANT DBADM ON DATABASE` | allowed (fallback) |

Procedure (dev/test database only; elevated PowerShell for the OS users):

```powershell
# 1. OS users (local test accounts, no admin rights), one password held in an env var for the run
foreach ($u in 'alelt_sqladm','alelt_dataacc','alelt_dbadm_nd','alelt_dbadm') { net user $u $env:ALELT_PW /add /passwordchg:no }
```
```sql
-- 2. Db2 grants (db2 CLP as the instance owner / SECADM)
GRANT CONNECT ON DATABASE TO USER ALELT_SQLADM, USER ALELT_DATAACC, USER ALELT_DBADM_ND, USER ALELT_DBADM;
GRANT SQLADM ON DATABASE TO USER ALELT_SQLADM;
GRANT DATAACCESS ON DATABASE TO USER ALELT_DATAACC;
GRANT DBADM WITHOUT DATAACCESS WITHOUT ACCESSCTRL ON DATABASE TO USER ALELT_DBADM_ND;
GRANT DBADM ON DATABASE TO USER ALELT_DBADM;
```
```powershell
# 3. Matrix (read-only: connect, db2ReadLog QUERY + one READ, catalog SELECT); keep the JSON with the change record
python tools\testenv\check_readlog_privs.py --database <DB> --db2-home <SQLLIB> --password-env ALELT_PW `
  --user alelt_sqladm --user alelt_dataacc --user alelt_dbadm_nd --user alelt_dbadm
# 4. Clean up: REVOKE the grants, `net user <u> /delete`
```

Decision rule: grant the production service account the **lowest row that passes `readlog_query` and
`readlog_read`** (expected: `DBADM WITHOUT DATAACCESS WITHOUT ACCESSCTRL`); if only `alelt_dbadm` passes, grant plain
`DBADM` and record it. Snapshot / keyed re-read still need `SELECT` on the approved tables (above): `WITHOUT
DATAACCESS` does not grant table reads.
* The agent connects with implicit OS authentication (`SQLConnect <alias>` with no user/password), so the
  Db2 authorization ID is the service account. No credentials are stored by the agent (R9).
* The agent never writes to the database: connection is `READ ONLY`, every statement is a compiled-in
  `SELECT … WITH UR` (unit-tested), and it issues `COMMIT` only to end its own read units of work.

## 2. Install

Elevated PowerShell on the Db2 server:

```powershell
# a) MSI (preferred)
msiexec /i ALELTAgent.msi /qn /l*v C:\Temp\aleltagent-msi.log `
  HUBURL=https://alelt-hub.corp:8443 AGENTID=edge-01 CONNECTIONID=erp DATABASE=<DATABASE> `
  CERTTHUMBPRINT=<sha1> HUBPIN=<sha256-spki> SPOOLDIR=E:\ALELT\spool SERVICEACCOUNT='CORP\svc-alelt$'

# b) or scripted
.\install-agent.ps1 -SourceDir .\bin -HubUrl https://alelt-hub.corp:8443 -AgentId edge-01 `
  -Database <DATABASE> -CertThumbprint <sha1> -HubPin <sha256-spki> -SpoolDir E:\ALELT\spool `
  -Account 'CORP\svc-alelt$' -NoStart
```

Then:

1. Edit `C:\ProgramData\ALELT\Agent\aleltagent.json` with the DBA: `limits_max` (DBA-approved ceilings:
   default 1 core, 512 MB), `governor` thresholds, `source.settings.catalog_schemas`, `snapshot.allowed_tables`.
2. Grant the service account read access to the client certificate private key
   (`certlm.msc` → Personal → cert → All Tasks → Manage Private Keys → Read).
3. `aleltagent.exe --check-config` then `Start-Service ALELTAgent`.
4. `verify-agent.ps1` must report **All checks passed** (service running, no listening sockets,
   Job Object, BELOW_NORMAL, ACL). Attach the output to the change ticket.

Hub pin: SHA-256 of the hub certificate's SubjectPublicKeyInfo:
`openssl x509 -in hub.crt -pubkey -noout | openssl pkey -pubin -outform der | openssl dgst -sha256`.
Configure two pins (current + next key) before rotating the hub certificate.

## 3. Operate

| Task | Command |
|---|---|
| Status | `aleltagent.exe --status`, `verify-agent.ps1` |
| Logs | `%ProgramData%\ALELT\Agent\log\aleltagent.log` (JSON lines, rotated); errors also in Application Event Log, source `ALELTAgent` |
| Pause capture | hub `PAUSE` command (or stop the service — positions are durable) |
| Spool | `<spool>\*.aleltb` = batches not yet acknowledged; `state.json` = durable/next positions. Never delete by hand unless the hub has re-synced. |
| Spool full | reading pauses (backpressure), nothing dropped; fix hub connectivity, it drains automatically |
| Governor | `PAUSED_BY_GOVERNOR` when host CPU ≥ `host_cpu_pause_pct` or log utilisation ≥ `max_log_util_pct` for `sustain_samples`; auto-resumes |

## 4. Rollback / uninstall

```powershell
msiexec /x ALELTAgent.msi /qn        # or: .\uninstall-agent.ps1   (keeps config/spool/logs)
.\uninstall-agent.ps1 -Purge               # also removes %ProgramData%\ALELT\Agent
```
DBA: `REVOKE DBADM ON DATABASE FROM USER "CORP\SVC-ALELT$";` (and the SELECT grants).
Nothing in the ERP database or its logs needs to be undone: the agent created no objects (R1)
and never touched archive logs (R3).

## 5. Elevation note

`--install`, `--uninstall`, `--post-install`, MSI install and ACL changes need an elevated prompt.
`--console`, `--check-config`, `--init-config` (to a user-writable path) and `--status` do not.
