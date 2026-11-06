# L9 - Production

**Scope.** Per-wave switch to `ALELT` via the view procedure; dbt run; rollback drill to `LEGACY`.

**Exit criterion (test ladder).** dbt green on ALELT; rollback < 5 min

| File | Purpose |
|---|---|
| `l9_switch.py` | Dry run by default. `--execute --confirm "SWITCH WAVE n"` only *requests* the switch; a second DataOperator approves in the console (two-person rule). Measures rollback approval -> applied. |

## Run

```
python l9_switch.py --hub https://hub --wave 1
python l9_switch.py --hub https://hub --wave 1 --execute --confirm "SWITCH WAVE 1" --dbt-cmd "dbt build -s tag:wave1"
```

Writes `L9/result.json` (schema: `tests/ladder/_common/ladder_result.py`). Exit code 0 = PASS, 1 = FAIL, 2 = SKIP (prerequisite missing or gate closed).

**Gate.** Refuses to run production steps unless `L8/result.json` is PASS.
