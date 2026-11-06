# Cutover per wave (L9)

A wave switches a set of certified tables from the legacy pipeline to ALELT by repointing the stable consumer views (`ALELT_CONTROL.SWITCH_SOURCE(table,'ALELT')`). The legacy pipeline keeps running (hot rollback) until **all** waves are stable 30 days.

| # | Step | Who | Check |
|---|---|---|---|
| 1 | Pre-check: every table in the wave certified (§10) and certification < 7 days old | DataOperator | Certification screen all green |
| 2 | Rollback drilled for this wave in sandbox/shadow within the last 7 days | DataOperator | Drill record |
| 3 | Announce window to dbt / BI owners; pick slot **between dbt runs** | DataOperator | Comms sent |
| 4 | Console → Certification → Wave → **Switch wave** → type wave name; **second approver** confirms (two-person rule) | Operator + approver | Audit shows both identities |
| 5 | Hub calls `SWITCH_WAVE` → `SWITCH_SOURCE(table,'ALELT')` per table; view DDL only, no data copy | automatic | Switch state per table = ALELT |
| 6 | Run dbt (production job) | dbt owner | dbt green |
| 7 | Automated post-checks: row counts / freshness on views, `<SYNCED>` monotonic, recon | automatic | `tests/ladder/L9/l9_switch.py` result green |
| 8 | Any failure → [rollback.md](rollback.md) immediately (target < 5 min) | Operator | — |
| 9 | Monitor 48 h with heightened alerting; daily parity continues against legacy | DataOperator | — |

## Decommission (L10)

All tables certified + 30 days stable → legacy pipeline paused by its owner (ALELT never touches it — R2) → 30 days → retired. Checklist: `tests/ladder/L10/l10_checklist.md`.
