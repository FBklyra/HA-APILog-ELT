# L6 - Lab (restored ERP backup, same Db2 build)

**Scope.** Workload replay 1x/3x peak; throttle; chaos: kill agent, kill hub, destination down 1 h, spool full, network flap, DR rebuild of the hub VM.

**Exit criterion (test ladder).** No gaps, no duplicates, auto-recovery, CPU cap respected

| File | Purpose |
|---|---|
| `l6_load.py` | Runs the lab replay driver (`--workload`), samples agent CPU vs cap and lag through the hub, then recon (gaps / duplicates). |
| `l6_chaos.py` | Scenario runner; injection/restore commands come from a lab config (`l6_chaos.example.json`). `--list` prints the scenarios. |
| `l6_chaos.example.json` | Template; copy to `l6_chaos.lab.json` for the lab. |

## Run

```
python l6_load.py --hub https://hub-lab --environment lab --workload "powershell -File replay.ps1 -Multiplier {mult} -Minutes {minutes}"
python l6_chaos.py --hub https://hub-lab --environment lab --config l6_chaos.lab.json [--only kill_agent]
```

Writes `L6/result.json` (schema: `tests/ladder/_common/ladder_result.py`). Exit code 0 = PASS, 1 = FAIL, 2 = SKIP (prerequisite missing or gate closed).

Refuses unless `--environment lab`. Parts `result.load.json` + `result.chaos.json` are merged into `result.json`.
