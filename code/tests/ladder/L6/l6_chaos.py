"""L6 chaos: fault injection on the lab while a background workload runs; prove auto-recovery, no gap, no duplicate.

Scenarios (prompt 12): kill_agent, kill_hub, destination_down_1h, spool_full, network_flap, dr_rebuild.
How to inject/restore is lab-specific and lives in a JSON config (see l6_chaos.example.json):
  {"background_workload": "<cmd>", "scenarios": {"kill_agent": {"inject": "<cmd>", "restore": "<cmd or null>",
   "hold_s": 60, "recover_within_s": 300, "expect_during": {"agent_state": "OFFLINE"}}, ...}}

  python l6_chaos.py --hub https://hub-lab --environment lab --config l6_chaos.lab.json [--only kill_agent]
After each scenario: agent back to CAPTURING, lag <= target within recover_within_s, recon gaps=0 duplicates=0,
spool never dropped (agent pauses reading when full, DESIGN.md §9). Writes result.chaos.json, then merges L6.
"""
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common.hub import Hub, items  # noqa: E402
from _common.ladder_result import LadderResult, merge_parts  # noqa: E402
from l6_load import recon  # noqa: E402

SCENARIOS = {
    "kill_agent": "Kill aleltagent.exe (taskkill /F); SCM restarts it; capture resumes from durable LRI",
    "kill_hub": "Kill hub-api + workers (docker kill); agent spools; ACK resumes; no duplicate batch",
    "destination_down_1h": "Block destination for 1 h; loader retries; checkpoint unchanged; catch-up after",
    "spool_full": "Shrink spool cap with hub unreachable; agent pauses reading (backpressure), never drops",
    "network_flap": "Drop agent<->hub traffic 30 s on / 30 s off x10; WebSocket reconnects; no gap",
    "dr_rebuild": "Destroy hub VM, rebuild from code (DR runbook in runbooks/), resume from destination checkpoint",
}


def sh(cmd: Optional[str]) -> int:
    if not cmd:
        return 0
    return subprocess.run(shlex.split(cmd, posix=False), check=False).returncode


def agent_state(hub: Hub) -> str:
    ags = items(hub.get("/agents"))
    return str(ags[0].get("state", "UNKNOWN")) if ags else "NONE"


def run_scenario(hub: Hub, name: str, sc: dict[str, Any], a: argparse.Namespace, res: LadderResult) -> None:
    before = hub.get("/overview")
    res.check(f"{name}: healthy before", agent_state(hub) == "CAPTURING",
              f"state={agent_state(hub)} lag={before.get('capture_lag_s')}s")
    rc = sh(sc.get("inject"))
    res.check(f"{name}: injected", rc == 0, sc.get("inject", "")[:120])
    t_end = time.time() + int(sc.get("hold_s", 60))
    seen_states: set[str] = set()
    while time.time() < t_end:
        try:
            seen_states.add(agent_state(hub))
        except OSError:
            seen_states.add("HUB_DOWN")
        time.sleep(5)
    want = (sc.get("expect_during") or {}).get("agent_state")
    if want:
        res.check(f"{name}: expected state during fault", want in seen_states, f"seen {sorted(seen_states)}")
    rc = sh(sc.get("restore"))
    res.check(f"{name}: restored", rc == 0, (sc.get("restore") or "auto")[:120])
    t0 = time.time()
    limit = int(sc.get("recover_within_s", 600))
    recovered = False
    while time.time() - t0 < limit:
        try:
            if agent_state(hub) == "CAPTURING" and (hub.get("/overview").get("capture_lag_s") or 0) <= a.lag_target_s:
                recovered = True
                break
        except OSError:
            pass
        time.sleep(10)
    res.check(f"{name}: auto-recovered", recovered, f"{time.time() - t0:.0f}s (limit {limit}s)")
    rr = recon(hub, f"L6 chaos {name}", a.recon_wait_s)
    res.check(f"{name}: no gaps", rr.get("gaps", -1) == 0, f"gaps={rr.get('gaps')}")
    res.check(f"{name}: no duplicates", rr.get("duplicates", -1) == 0, f"duplicates={rr.get('duplicates')}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hub")
    ap.add_argument("--environment", default="dev")
    ap.add_argument("--config", help="lab chaos config (see l6_chaos.example.json)")
    ap.add_argument("--only", action="append", default=[], choices=sorted(SCENARIOS))
    ap.add_argument("--lag-target-s", type=int, default=120)
    ap.add_argument("--recon-wait-s", type=int, default=3600)
    ap.add_argument("--list", action="store_true", help="print scenarios and exit")
    a = ap.parse_args()
    if a.list:
        for k, v in SCENARIOS.items():
            print(f"{k:22} {v}")
        return
    res = LadderResult("L6", "lab chaos scenarios", environment=a.environment)
    hub = Hub(a.hub, dev_role="PlatformAdmin")
    if a.environment != "lab":
        res.skip("lab environment", "L6 runs only on the lab (--environment lab); dev/prod refused")
    elif not hub.configured or not a.config:
        res.skip("prerequisites", "need --hub and --config (lab hub + injection commands not available yet)")
    else:
        cfg = json.loads(Path(a.config).read_text(encoding="utf-8"))
        bg = subprocess.Popen(shlex.split(cfg["background_workload"], posix=False)) if cfg.get(
            "background_workload") else None
        try:
            for name in a.only or list(SCENARIOS):
                sc = cfg.get("scenarios", {}).get(name)
                if not sc:
                    res.skip(f"{name}", "not configured")
                    continue
                try:
                    run_scenario(hub, name, sc, a, res)
                except OSError as e:
                    res.check(f"{name}: run", False, str(e))
        finally:
            if bg:
                bg.terminate()
    res.write(part="chaos")
    merge_parts("L6", ("load", "chaos"))
    sys.exit({"PASS": 0, "SKIP": 2}.get(res.status, 1))


if __name__ == "__main__":
    main()
