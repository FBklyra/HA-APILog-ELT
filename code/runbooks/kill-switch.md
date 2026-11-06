# Kill switch — stop all reading on the Db2 server

Use when: Db2 performance concern, DBA request, suspected agent fault, security incident. **Anyone on-call may pull it; no approval needed to stop.** Resuming needs PlatformAdmin.

## Stop

| # | Option | Action | Effect |
|---|---|---|---|
| A | Console (preferred) | Agent → **Pause capture (kill switch)** → type the agent name → confirm | Hub sends `PAUSE`; agent finishes current read, stops `db2ReadLog`, keeps connection + spool; audited |
| B | Db2 server (hub unreachable / immediate) | `sc stop ALELTAgent` (admin cmd) | Service stops; Db2 CLI connection closed; spool kept on disk |
| C | Hard stop | `sc config ALELTAgent start= disabled` then B | Survives reboot |
| D | Last resort, DBA | `db2 force application (<agent appl handle>)` | Agent sees disconnect and backs off; disable the service as in C to stop reconnects |

None of these touch Db2 data or archive logs (R1, R3). The destination keeps the last committed checkpoint — no data is lost, capture resumes from it.

## Diagnose (before resuming)

| Check | Where |
|---|---|
| Governor events, CPU/mem vs caps | Console → Agent |
| Last durable position vs Db2 current position | Console → Agent / `PROBE current_position` |
| Archive retention headroom (`AleltArchiveRetentionHeadroomLow`) | Prometheus / Overview — long pauses can outrun DBA retention; escalate to DBA early |
| Agent log | `C:\ProgramData\ALELT\logs\` (JSON) |

## Resume

| # | Step |
|---|---|
| 1 | Root cause noted in the incident ticket; DBA agrees |
| 2 | If service was disabled: `sc config ALELTAgent start= auto` and `sc start ALELTAgent` |
| 3 | Console → Agent → **Resume** (or `SET_LIMITS` lower first) |
| 4 | Lag trends down; no gap alert; recon on next cycle is clean |
