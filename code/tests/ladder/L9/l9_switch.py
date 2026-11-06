"""L9 production cutover drill per wave: switch views to ALELT, dbt green, rollback to legacy < 5 min.

The switch is executed by the hub (destination SWITCH_SOURCE procedure); the harness never touches
legacy-pipeline objects (R2). Switch targets: ALELT | LEGACY. Two-person rule: the harness only *requests*;
a second DataOperator approves in the console (screen 8). Default is a dry run (pre-checks only). Gate: L8 PASS.

  python l9_switch.py --hub https://hub --wave 1                                  # dry run
  python l9_switch.py --hub https://hub --wave 1 --execute --confirm "SWITCH WAVE 1" --dbt-cmd "dbt build -s tag:wave1"
Drill: request switch -> wait approval + apply -> dbt -> post-checks -> request rollback -> time to applied.
"""
from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common.hub import Hub, items  # noqa: E402
from _common.ladder_result import LadderResult, require_green  # noqa: E402

ROLLBACK_LIMIT_S = 300


def wait_applied(hub: Hub, req_id: str, timeout_s: int) -> dict[str, Any]:
    deadline = time.time() + timeout_s
    r: dict[str, Any] = {}
    while time.time() < deadline:
        r = hub.get(f"/switch/{req_id}")
        if r.get("status") in ("APPLIED", "REJECTED", "FAILED"):
            break
        time.sleep(5)
    return r


def seconds(r: dict[str, Any], a: str, b: str) -> float:
    try:
        return (datetime.fromisoformat(r[b].replace("Z", "+00:00"))
                - datetime.fromisoformat(r[a].replace("Z", "+00:00"))).total_seconds()
    except (KeyError, ValueError, AttributeError):
        return float("inf")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hub")
    ap.add_argument("--wave", type=int)
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--confirm", default="", help='must equal "SWITCH WAVE <n>" with --execute')
    ap.add_argument("--dbt-cmd", help="dbt command run between switch and rollback (must exit 0)")
    ap.add_argument("--approval-timeout-s", type=int, default=3600)
    ap.add_argument("--keep-switched", action="store_true", help="skip the rollback drill (real cutover)")
    a = ap.parse_args()
    res = LadderResult("L9", f"switch + rollback drill, wave {a.wave}", environment="prod")
    if not require_green("L8", res):
        res.finish()
    hub = Hub(a.hub, dev_role="DataOperator")
    if not hub.configured or a.wave is None:
        res.skip("prerequisites", "need --hub and --wave")
        res.finish()
    try:
        wave = [t for t in items(hub.get("/certification")) if t.get("wave") == a.wave]
        res.check("wave has tables", bool(wave), ", ".join(str(t.get("table")) for t in wave))
        uncert = [t.get("table") for t in wave if t.get("status") != "CERTIFIED"]
        res.check("all wave tables certified", not uncert, ", ".join(map(str, uncert)))
        if not a.execute:
            res.skip("switch drill", "dry run (use --execute --confirm)")
            res.finish()
        if a.confirm != f"SWITCH WAVE {a.wave}":
            res.check("confirmation text", False, f'expected "SWITCH WAVE {a.wave}"')
            res.finish()
        req = hub.post("/switch", {"wave": a.wave, "target": "ALELT", "confirm": a.confirm,
                                   "reason": f"L9 drill wave {a.wave}"})
        print(f"switch request {req.get('id')} created; waiting for second approver in the console...")
        sw = wait_applied(hub, req["id"], a.approval_timeout_s)
        res.check("switch approved by a second person", sw.get("status") == "APPLIED"
                  and sw.get("approved_by") not in (None, sw.get("requested_by")),
                  f"status={sw.get('status')} requested_by={sw.get('requested_by')} "
                  f"approved_by={sw.get('approved_by')}")
        if sw.get("status") != "APPLIED":
            res.finish()
        if a.dbt_cmd:
            rc = subprocess.run(shlex.split(a.dbt_cmd, posix=False), check=False).returncode
            res.check("dbt green on ALELT", rc == 0, f"exit {rc}")
        else:
            res.skip("dbt green on ALELT", "no --dbt-cmd")
        post = hub.get(f"/switch/{req['id']}/postchecks")
        res.check("post-switch checks", all(x.get("status") == "PASS" for x in items(post)),
                  f"{len(items(post))} check(s)")
        if a.keep_switched:
            res.skip("rollback drill", "--keep-switched (cutover kept; drill done in an earlier run)")
            res.finish()
        rb = hub.post("/switch", {"wave": a.wave, "target": "LEGACY", "confirm": f"ROLLBACK WAVE {a.wave}",
                                  "reason": f"L9 rollback drill wave {a.wave}"})
        rbr = wait_applied(hub, rb["id"], a.approval_timeout_s)
        dt = seconds(rbr, "approved_at", "applied_at")
        res.check("rollback applied", rbr.get("status") == "APPLIED", str(rbr.get("status")))
        res.check("rollback < 5 min", dt < ROLLBACK_LIMIT_S, f"{dt:.0f}s from approval to applied")
    except (OSError, KeyError) as e:
        res.check("hub calls", False, f"{type(e).__name__}: {e}")
    res.finish()


if __name__ == "__main__":
    main()
