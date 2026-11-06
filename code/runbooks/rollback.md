# Rollback a table or wave to the legacy pipeline (< 5 min)

Possible only while the legacy pipeline is still running (until L10). Any DataOperator on-call may execute; no second approver needed for rollback (speed over ceremony; it is audited).

| # | Step | Time |
|---|---|---|
| 1 | Console → Certification → table/wave → **Rollback to legacy** → type table/wave name | 1 min |
| 2 | Hub calls `ALELT_CONTROL.SWITCH_SOURCE(table,'LEGACY')` per table (view repoint, no data movement) | < 1 min |
| 3 | Console unavailable → run on the destination as `ALELT_ADMIN`: `CALL ALELT_CONTROL.SWITCH_SOURCE('<TABLE>','LEGACY');` | 1 min |
| 4 | Verify: view DDL (`SELECT GET_DDL('VIEW','<VIEW_SCHEMA>.<TABLE>')`) references the legacy schema; switch state = LEGACY | 1 min |
| 5 | Re-run affected dbt models | per dbt |
| 6 | ALELT **keeps replicating** in shadow (do not pause) so the table can be re-certified | — |
| 7 | Incident ticket + root cause; table re-enters certification (7 clean days) | — |

Drill: once per wave before cutover, timed; `tests/ladder/L9/l9_switch.py --rollback-drill` records the duration (must be < 300 s).
