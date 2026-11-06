"""E2E in Linux containers: aleltd as PID 1, `docker stop` (SIGTERM) -> graceful shutdown -> restart -> resume.

  python edge-agent/linux/tests/e2e_container_sigterm.py [--image alelt:local] [--keep]

Throw-away hub (`alelt-test-lxhub`, SQLite metadata, role all) + agent (`alelt-test-lxagent`, fake source, repo
mounted read-only, state volume `alelt-test-lxstate`, --memory 512m so require_job_limits=true is satisfied).
Also runs the unit tests inside the Linux container (sd_notify datagram tests included). Everything it creates
is removed at the end (unless --keep).
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
NET, HUB, AGENT, VOL = "alelt-test-lxnet", "alelt-test-lxhub", "alelt-test-lxagent", "alelt-test-lxstate"
TOKEN = "lx-container-test-token"
CID = "lxfake"

COUNT = """
import json, sqlite3
c = sqlite3.connect('/data/alelt.db')
rows = c.execute("SELECT seq, from_lri, coalesce(next_lri, to_lri) FROM batch WHERE connection_id='lxfake' ORDER BY seq").fetchall()
print(json.dumps(rows))
"""


def sh(*args: str, check: bool = True, timeout: float = 300) -> subprocess.CompletedProcess:
    r = subprocess.run(list(args), capture_output=True, text=True, timeout=timeout)
    if check and r.returncode:
        raise RuntimeError(f"{' '.join(args[:4])}...: {r.stderr[-800:]}")
    return r


def batches() -> list:
    r = sh("docker", "exec", HUB, "python", "-c", COUNT, check=False)
    return json.loads(r.stdout) if r.returncode == 0 and r.stdout.strip() else []


def wait_batches(n: int, timeout: float = 90) -> list:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        b = batches()
        if len(b) >= n:
            return b
        time.sleep(1)
    raise TimeoutError(f"fewer than {n} batches at the hub")


def state(image: str) -> dict:
    r = sh("docker", "run", "--rm", "--entrypoint", "cat", "-v", f"{VOL}:/s", image, "/s/spool/state.json")
    return json.loads(r.stdout)


def cleanup() -> None:
    for c in (AGENT, HUB):
        sh("docker", "rm", "-f", c, check=False)
    sh("docker", "volume", "rm", "-f", VOL, check=False)
    sh("docker", "network", "rm", NET, check=False)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--image", default="alelt:local")
    ap.add_argument("--keep", action="store_true")
    a = ap.parse_args()
    res: dict = {"checks": []}

    def check(name: str, ok: bool, detail: str = "") -> None:
        res["checks"].append({"check": name, "ok": bool(ok), "detail": detail})
        print(("PASS " if ok else "FAIL ") + name + (f"  ({detail})" if detail else ""), flush=True)

    cfgdir = Path(tempfile.mkdtemp(prefix="alelt_lx_"))
    cfg = {"agent_id": "lx-container", "connection_id": CID,
           "hub": {"url": f"http://{HUB}:8080", "mode": "token", "heartbeat_s": 5, "backoff_max_s": 5,
                   "allow_insecure_http": True},
           "spool": {"dir": "/var/lib/alelt/spool", "max_mb": 256}, "log": {"level": "INFO"},
           "limits": {"batch_kb": 64, "poll_ms": 200}, "require_job_limits": True,
           "source": {"engine": "fake", "settings": {"rate_tps": 20, "rows_per_txn": 3, "idle_tick_s": 0.1}},
           "capture": {"autostart": True, "batch_ms": 500}, "shutdown": {"deadline_s": 45, "drain_s": 10}}
    (cfgdir / "aleltagent.json").write_text(json.dumps(cfg), encoding="utf-8")
    src = f"{REPO}:/src:ro"
    agent_run = ["docker", "run", "-d", "--name", AGENT, "--network", NET, "--memory", "512m",
                 "--stop-timeout", "60", "-e", f"ALELT_AGENT_TOKEN={TOKEN}",
                 "-e", "PYTHONPATH=/src/edge-agent/linux:/src/hub", "-e", "PYTHONDONTWRITEBYTECODE=1",
                 "-v", src, "-v", f"{cfgdir}:/cfg:ro", "-v", f"{VOL}:/var/lib/alelt", "-w", "/tmp",
                 "--entrypoint", "python", a.image, "-m", "aleltd", "--config", "/cfg/aleltagent.json"]
    cleanup()
    try:
        sh("docker", "network", "create", NET)
        sh("docker", "volume", "create", VOL)
        # like systemd StateDirectory=: the state directory belongs to the (non-root) service user of the image
        sh("docker", "run", "--rm", "--user", "0", "-v", f"{VOL}:/s", "--entrypoint", "chown", a.image,
           "10001:10001", "/s")
        ut = sh("docker", "run", "--rm", "-v", src, "-e", "PYTHONDONTWRITEBYTECODE=1", "-w",
                "/src/edge-agent/linux/tests", "--entrypoint", "python", a.image, "-m", "unittest", "test_units",
                "test_daemon", check=False, timeout=600)
        tail = (ut.stderr.strip().splitlines() or [""])[-1]
        check("unit + daemon tests inside Linux (incl. sd_notify)", ut.returncode == 0, tail)
        sh("docker", "run", "-d", "--name", HUB, "--network", NET, "-e", "ALELT_METADATA=sqlite",
           "-e", "ALELT_DB_URL=sqlite:////data/alelt.db", "-e", "ALELT_ENV=local", "-e", "ALELT_DEV_AUTH=1",
           "-e", "ALELT_AGENT_AUTH=token", "-e", f"ALELT_AGENT_TOKENS={TOKEN}", "-e", "ALELT_ROLE=all", a.image, "all")
        time.sleep(20)
        sh(*agent_run)
        b1 = wait_batches(5)
        time.sleep(2)
        t0 = time.monotonic()
        sh("docker", "stop", "-t", "60", AGENT, timeout=90)
        took = time.monotonic() - t0
        code = sh("docker", "inspect", "-f", "{{.State.ExitCode}}", AGENT).stdout.strip()
        logs = sh("docker", "logs", AGENT).stdout + sh("docker", "logs", AGENT).stderr
        check("docker stop (SIGTERM to PID 1) -> exit code 0", code == "0", f"exit={code} in {took:.1f}s")
        check("graceful path logged (checkpoint, drain, stopped)",
              "saving checkpoint" in logs and '"msg": "stopped"' in logs)
        check("journald-ready JSON logs on stdout/stderr", all(ln.startswith("{") for ln in logs.splitlines() if ln))
        st1, hub1 = state(a.image), batches()
        check("state.json next_pos == hub last next_pos after stop", st1.get("next_pos") == hub1[-1][2],
              f"state={st1.get('next_pos')} hub={hub1[-1][2]} reason={st1.get('reason')}")
        check("state.json capturing kept for restart", st1.get("capturing") is True)
        sh("docker", "start", AGENT)
        wait_batches(len(hub1) + 5)
        sh("docker", "stop", "-t", "60", AGENT, timeout=90)
        code2 = sh("docker", "inspect", "-f", "{{.State.ExitCode}}", AGENT).stdout.strip()
        st2, hub2 = state(a.image), batches()
        contiguous = all(hub2[i][1] == hub2[i - 1][2] for i in range(1, len(hub2)))
        first_after = hub2[len(hub1)][1] if len(hub2) > len(hub1) else None
        check("restart resumes exactly at the saved position", first_after == st1.get("next_pos"),
              f"first from_pos after restart={first_after}")
        check("hub chain contiguous across stop/start (no gap, no overlap)", contiguous, f"{len(hub2)} batches")
        check("second stop exit 0, state == hub", code2 == "0" and st2.get("next_pos") == hub2[-1][2])
        res["batches"] = len(hub2)
        res["first_batches"] = len(b1)
    except Exception as e:  # noqa: BLE001
        check("harness", False, f"{type(e).__name__}: {e}")
    finally:
        if not a.keep:
            cleanup()
    res["pass"] = all(c["ok"] for c in res["checks"])
    (HERE / "result-e2e-container.json").write_text(json.dumps(res, indent=1), encoding="utf-8")
    print("PASS" if res["pass"] else "FAIL")
    return 0 if res["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
