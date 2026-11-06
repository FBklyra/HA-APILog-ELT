# Install the edge agent (Db2 server, Windows)

The installer (MSI, PowerShell install/uninstall, certificate enrollment) is owned by the agent stream: see [`deploy/agent/`](../deploy/agent/). This page lists only the operator checkpoints.

| # | Checkpoint | Who |
|---|---|---|
| 1 | Change ticket approved; DBA present. Dedicated Db2 user (DBADM for `db2ReadLog`, **not** the legacy pipeline's user — R2) | DBA |
| 2 | Archive logging on and DATA CAPTURE CHANGES confirmed on in-scope tables (DBA change; ALELT never issues DDL — R1) | DBA |
| 3 | Client cert enrolled in `LocalMachine\My` from the internal CA; thumbprint added to hub `ALELT_AGENT_THUMBPRINTS` | PlatformAdmin |
| 4 | Install MSI; service `ALELTAgent` registered; hub URL = agent host | PlatformAdmin |
| 5 | `netstat -ano \| findstr LISTENING` shows **no** port owned by `aleltagent.exe` (R4) | PlatformAdmin |
| 6 | Console → Agent: state `Idle`, version, cert expiry, limits (CPU/mem caps) as approved | PlatformAdmin |
| 7 | Kill switch tested once ([kill-switch.md](kill-switch.md)) before `START_CAPTURE` | PlatformAdmin + DBA |
