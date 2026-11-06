"""L6 load: synthetic workload replay at 1x and 3x peak on the lab (restored ERP backup, same Db2 build).

Proves: agent CPU stays within its cap (Job Object + governor), lag recovers, no gaps / no duplicates (recon).
The workload driver is environment-specific (--workload: a command that replays N minutes of peak DML on the
LAB database at a given multiplier, e.g. `powershell -File replay.ps1 -Multiplier {mult} -Minutes {minutes}`).
NEVER point this at production ERP (R1): the harness refuses unless --environment lab.

  python l6_load.py --hub https://hub-lab --environment lab --workload "<cmd with {mult} {minutes}>" [--minutes 60]
Writes result.load.json; result.json = merge(load, chaos) (see l6_chaos.py).
"""
from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common.hub import Hub, items  # noqa: E402
from _common.ladder_result import LadderResult, merge_parts  # noqa: E402


def sample(hub: Hub, out: list[dict[str, Any]], stop: threading.Event, every_s: float) -> None:
    while not stop.is_set():
        try:
            ov = hub.get("/overview")
            for ag in items(hub.get("/agents")):
                out.append({"t": time.time(), "cpu_pct": ag.get("cpu_pct", 0.0),
                            "cpu_cap": (ag.get("limits") or {}).get("cpu_pct", 100),
                            "governor_pauses": ag.get("governor_pauses", 0),
                            "lag_s": ov.get("capture_lag_s"), "spool_pct": ag.get("spool_pct", 0.0)})
        except OSError:
            out.append({"t": time.time(), "error": True})
        stop.wait(every_s)


def recon(hub: Hub, reason: str, wait_s: int) -> dict[str, Any]:
    """Start a full key-set reconciliation and wait for it (shared with l6_chaos.py)."""
    run = hub.post("/recon", {"scope": "all", "kind": "full_key_set", "reason": reason})
    rid = run.get("id") if isinstance(run, dict) else None
    deadline = time.time() + wait_s
    rr: dict[str, Any] = {}
    while rid and time.time() < deadline:
        rr = hub.get(f"/recon/{rid}")
        if rr.get("status") in ("DONE", "FAILED"):
            break
        time.sleep(15)
    return rr


def run_level(hub: Hub, a: argparse.Namespace, mult: int, res: LadderResult) -> None:
    samples: list[dict[str, Any]] = []
    stop = threading.Event()
    th = threading.Thread(target=sample, args=(hub, samples, stop, a.sample_s), daemon=True)
    th.start()
    cmd = a.workload.format(mult=mult, minutes=a.minutes)
    rc = subprocess.run(shlex.split(cmd, posix=False), check=False).returncode
    deadline = time.time() + a.drain_s                       # let the pipeline drain
    while time.time() < deadline and (hub.get("/overview").get("capture_lag_s") or 0) > a.lag_target_s:
        time.sleep(a.sample_s)
    stop.set()
    th.join()
    ok = [s for s in samples if not s.get("error")]
    res.check(f"{mult}x: workload ran", rc == 0, f"exit {rc}")
    over = [s for s in ok if s["cpu_pct"] > s["cpu_cap"] * 1.05]
    res.check(f"{mult}x: agent CPU within cap", bool(ok) and not over,
              f"max {max((s['cpu_pct'] for s in ok), default=0):.1f}% of cap {ok[0]['cpu_cap'] if ok else '?'}%; "
              f"{len(over)} sample(s) over")
    lag = hub.get("/overview").get("capture_lag_s") or 0
    res.check(f"{mult}x: lag recovered <= target", lag <= a.lag_target_s, f"lag {lag}s target {a.lag_target_s}s")
    res.check(f"{mult}x: hub API available throughout", len(ok) == len(samples),
              f"{len(samples) - len(ok)} failed sample(s)")
    rr = recon(hub, f"L6 load {mult}x", a.recon_wait_s)
    res.check(f"{mult}x: no gaps", rr.get("gaps", -1) == 0, f"gaps={rr.get('gaps')}")
    res.check(f"{mult}x: no duplicates", rr.get("duplicates", -1) == 0, f"duplicates={rr.get('duplicates')}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hub")
    ap.add_argument("--environment", default="dev")
    ap.add_argument("--workload", help="replay command; {mult} and {minutes} are substituted")
    ap.add_argument("--multipliers", default="1,3")
    ap.add_argument("--minutes", type=int, default=60)
    ap.add_argument("--lag-target-s", type=int, default=120)
    ap.add_argument("--drain-s", type=int, default=1800)
    ap.add_argument("--recon-wait-s", type=int, default=3600)
    ap.add_argument("--sample-s", type=float, default=15.0)
    a = ap.parse_args()
    res = LadderResult("L6", "lab workload replay 1x/3x", environment=a.environment)
    hub = Hub(a.hub, dev_role="PlatformAdmin")
    if a.environment != "lab":
        res.skip("lab environment", "L6 runs only on the lab (--environment lab); dev/prod refused")
    elif not hub.configured or not a.workload:
        res.skip("prerequisites", "need --hub and --workload (lab hub + replay driver not available yet)")
    else:
        for m in (int(x) for x in a.multipliers.split(",")):
            try:
                run_level(hub, a, m, res)
            except OSError as e:
                res.check(f"{m}x: run", False, str(e))
    res.write(part="load")
    merge_parts("L6", ("load", "chaos"))
    sys.exit({"PASS": 0, "SKIP": 2}.get(res.status, 1))


if __name__ == "__main__":
    main()
