# Test ladder harnesses

`tests/ladder/L*/` hold the formal ladder harnesses; `L4-local/` runs local end-to-end tests against real engines
and throw-away databases on the test containers (`alelt-test-*`, never `local-*`). Each harness writes a
`result*.json` (`_common/ladder_result.py`); SKIP is never evidence.

## Convergence waits (`_common/convergence.py`)

Every wait is bounded (`--drain-timeout-s`, `--initial-load-timeout-s`, `ALELT_E2E_CONVERGE_TIMEOUT_S`) and ends early:

| Terminal (fail fast) | Transient (wait, bounded) |
|---|---|
| integrity `step=manual`; stream stopped (`*` held); snapshot paused with `last_error` (incl. re-issued `ALELT_SNAPSHOT_MAX_REISSUES` times); a data-path job FAILED; `COMPLETION_OVERSIZE` incident; agent process exited | SUSPECT / REDECODING / OUT_OF_SYNC under recovery; lifecycle INITIAL_LOAD / CATCH_UP / RESYNC; schedule held; completion pending; `COMPLETION_STALLED` (counted, fails at the timeout) |

A wait also ends when nothing moves for `ALELT_E2E_STALL_S` (default 300 s); the fingerprint covers batch / job / chunk
counts, pending completions, the newest agent heartbeat and the newest command ack. The diagnosis lists every table not
converged (phase, holds, pending completions, chunks loaded / last, recovery step, incident) plus the agent liveness
line and the last 5 incidents.

## Expectations per schedule class (`e2e_catalog.py`, ADR-012)

| Table class | Initial load | Changes during churn | Drain criterion |
|---|---|---|---|
| stream (`latency_target_s < defer_min`) | lands before churn (STREAMING, verified, live counts = Db2) | within `latency_target_s + tick_grace` | live counts = Db2 |
| deferred (`>= defer_min`, no window) | same | by the next round (scaled target + tick_grace) | live counts = Db2 after one round past the last change |
| window | same, while the window is closed, no extra rows | nothing before the window opens (`early == 0`); after it one release pass, latency <= 2 x tick_grace from the opening | live counts = Db2 after the window opened |
| LOB tables | snapshot rows carry the values | partial image at once, out-of-row columns completed | `table_completion` empty, no TABLE_OUT_OF_SYNC |

Flags: `--window-open-after-s` (moves every daily window to open N s after the start), `--schedule-scale` (scales
deferred targets and `ALELT_SCHEDULE_DEFER_MIN_S`), `--no-schedule`, `--initial-load-timeout-s`, `--drain-timeout-s`,
`--dry-run` (prints the expectations per class from a tables.yaml, starts nothing). A run shorter than the longest
schedule is refused (exit 2) without a schedule override. `--start-position` is removed: the edge starts at `current`
and the lifecycle takes the snapshot.

## Runs

`L4-local/run_e2e.sh` (parameterised by `ALELT_*` environment variables) runs `e2e_local.py` (py edge + Postgres
metadata, C agent + SQLite metadata) and `e2e_catalog.py`. Reviews happen before e2e runs that churn a Db2 test
database.
