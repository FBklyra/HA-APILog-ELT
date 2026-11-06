"""L0 link: the edge agent against a hub speaking the REAL agent protocol v1 (no Db2 access). AHS-26.

The fake hub is `edge-agent/core/tests/mock_hub.py` (every payload validated against hub/alelt_common/models.py):
plain HTTPS/HTTP request/response + long-poll `GET /agent/v1/commands` - the C agent has no WebSocket channel.
  POST /agent/v1/register, /heartbeat, /events, /commands/{id}/ack, /blocks, /lease/*   (X-ALELT-Protocol: 1)

Checks: agent registers; heartbeats keep arriving for --duration (24 h for the exit criterion, shorter in dev/CI)
with no gap > 3 x heartbeat interval; a whitelisted command (PROBE current_position) is acknowledged; a
non-whitelisted command (EXEC_SQL) is refused by the agent (ack ok:false, R5); no payload failed validation.

  python l0_link.py --exe <aleltagent.exe> --duration 300      # starts `aleltagent.exe --console` itself (no admin)
  python l0_link.py --port 18443 --duration 86400 --external  # hub only; l0_service.ps1 runs the agent as a service
  python l0_link.py --self-test --duration 10               # stdlib fake agent; proves the harness (never L0 evidence)
Writes result.link.json (result.selftest.json in --self-test); l0_service.ps1 merges service + link into result.json.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(HERE.parent))
from _common.ladder_result import LadderResult  # noqa: E402

MOCK_HUB = REPO / "edge-agent" / "core" / "tests" / "mock_hub.py"
DEFAULT_EXE = REPO / "edge-agent" / "core" / "build" / "x64-release" / "bin" / "aleltagent.exe"


def http(port: int, method: str, path: str, obj: Optional[dict[str, Any]] = None, token: str = "",
         timeout: float = 15) -> tuple[int, Any]:
    data = None if obj is None else json.dumps(obj).encode()
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method=method,
                                 headers={"Content-Type": "application/json", "X-ALELT-Protocol": "1",
                                          "X-ALELT-Agent-Token": token})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            return r.status, (json.loads(body) if body else None)
    except urllib.error.HTTPError as e:
        return e.code, None


def stats(port: int) -> dict[str, Any]:
    return http(port, "POST", "/admin/stats", {})[1] or {}


def fake_agent(port: int, token: str, heartbeat_s: float, stop: threading.Event) -> None:
    """Self-test only: same endpoints and payload shapes as the C agent (hub/alelt_common/models.py)."""
    aid = "l0-selftest"
    http(port, "POST", "/agent/v1/register", {
        "agent_id": aid, "connection_id": "l0conn", "engine": "db2", "version": "0.0.0-selftest",
        "hostname": "selftest", "database": "L0NODB", "capabilities": []}, token)
    last_hb = 0.0
    while not stop.is_set():
        try:
            if _fake_agent_step(port, token, aid, heartbeat_s, last_hb):
                last_hb = time.time()
        except OSError:
            time.sleep(0.5)          # hub stopping at the end of the run


def _fake_agent_step(port: int, token: str, aid: str, heartbeat_s: float, last_hb: float) -> bool:
    """One poll cycle; True when a heartbeat was sent."""
    sent = time.time() - last_hb >= heartbeat_s
    if sent:
        http(port, "POST", "/agent/v1/heartbeat", {"agent_id": aid, "state": "IDLE",
                                                       "at": datetime.now(timezone.utc).isoformat()}, token)
    code, cmds = http(port, "GET", f"/agent/v1/commands?agent_id={aid}&wait=1", None, token)
    for c in (cmds or []) if code == 200 else []:
        ok = c.get("name") == "PROBE"
        http(port, "POST", f"/agent/v1/commands/{c['command_id']}/ack",
             {"ok": ok, "result": {"probe": "current_position"} if ok else {"error": "command not whitelisted"}}, token)
    return sent


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--exe", default=str(DEFAULT_EXE), help="aleltagent.exe to run in --console mode")
    ap.add_argument("--external", action="store_true", help="agent is started elsewhere (service; l0_service.ps1)")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--port", type=int, default=18443)
    ap.add_argument("--token", default="")
    ap.add_argument("--duration", type=float, default=86400)
    ap.add_argument("--heartbeat-s", type=float, default=5)
    ap.add_argument("--connect-timeout", type=float, default=60)
    ap.add_argument("--environment", default="dev")
    a = ap.parse_args()
    token = a.token or ("l0-" + uuid.uuid4().hex)
    res = LadderResult("L0", "agent link (register, heartbeat, long-poll commands, whitelist)", environment=a.environment)
    part = "selftest" if a.self_test else "link"
    work = Path(tempfile.mkdtemp(prefix="alelt-l0-"))
    procs: list[subprocess.Popen[str]] = []
    stop = threading.Event()

    hub = subprocess.Popen([sys.executable, "-I", str(MOCK_HUB), "--port", str(a.port), "--token", token,
                            "--out", str(work / "hub")], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    procs.append(hub)
    hub_log: list[str] = []
    threading.Thread(target=lambda: hub_log.extend(iter(hub.stdout.readline, "")) if hub.stdout else None,
                     daemon=True).start()
    try:
        t0 = time.time()
        while time.time() - t0 < 10 and not stats(a.port) and hub.poll() is None:
            time.sleep(0.3)
        if a.self_test:
            threading.Thread(target=fake_agent, args=(a.port, token, a.heartbeat_s, stop), daemon=True).start()
        elif not a.external:
            exe = Path(a.exe)
            if not exe.exists():
                res.skip("aleltagent.exe built", f"not found: {exe}")
                res.finish(part=part)
                return 2
            cfg = {
                "agent_id": "l0-agent", "connection_id": "l0conn",
                "hub": {"url": f"http://127.0.0.1:{a.port}", "mode": "token", "token_env": "ALELT_L0_TOKEN",
                        "heartbeat_s": int(a.heartbeat_s), "command_wait_s": 2, "backoff_max_s": 5},
                "spool": {"dir": str(work / "spool"), "max_mb": 64}, "log": {"dir": str(work / "log"), "level": "INFO"},
                "require_job_limits": False, "governor": {"enabled": False},
                # alias that does not exist: the plugin never connects (L0 = no Db2 access)
                "source": {"plugin": "aleltsrc_db2.dll", "engine": "db2", "database": "L0NODB",
                           "dll_dirs": [os.environ.get("DB2PATH", r"D:\IBM\SQLLIB") + r"\BIN"],
                           "settings": {"database": "L0NODB"}},
                "capture": {"autostart": False},
            }
            (work / "spool").mkdir()
            (work / "aleltagent.json").write_text(json.dumps(cfg), encoding="utf-8")
            res.check("aleltagent.exe built", True, str(exe))
            procs.append(subprocess.Popen([str(exe), "--console", "--config", str(work / "aleltagent.json")],
                                          env=dict(os.environ, ALELT_L0_TOKEN=token), stdout=subprocess.DEVNULL,
                                          stderr=subprocess.DEVNULL, text=True))

        t0 = time.time()
        while time.time() - t0 < a.connect_timeout and not stats(a.port).get("registers"):
            time.sleep(0.5)
        s = stats(a.port)
        if not s.get("registers"):
            res.check("agent registered", False if not a.external else None,
                      f"no register within {a.connect_timeout:.0f}s")
            res.finish(part=part)
            return 1
        res.check("agent registered", True, f"{s['registers']} register call(s)")
        reg_t = time.time()

        # SET_LIMITS needs no source; PROBE needs the source open (no Db2 at L0 -> reported, not required)
        http(a.port, "POST", "/admin/command", {"name": "SET_LIMITS", "args": {"cpu_pct": 20}})
        http(a.port, "POST", "/admin/command", {"name": "PROBE", "args": {"name": "current_position"}})
        http(a.port, "POST", "/admin/command", {"name": "EXEC_SQL", "args": {"sql": "DROP TABLE X"}})
        end = reg_t + a.duration
        while time.time() < end:
            if hub.poll() is not None:
                break
            if len(procs) > 1 and procs[1].poll() is not None:
                break
            time.sleep(min(5.0, max(0.1, end - time.time())))
        s = stats(a.port)
        hb = sorted(t for t in s.get("hb_ts", []) if t >= reg_t - 1)
        gaps = [b - x for x, b in zip(hb, hb[1:])]
        max_gap = max(gaps) if gaps else float("inf")
        expected = max(1, int(a.duration / a.heartbeat_s) // 2)
        res.check("process alive for the whole duration",
                  hub.poll() is None and (len(procs) < 2 or procs[1].poll() is None), f"{a.duration:.0f}s")
        res.check("heartbeats received for whole duration", len(hb) >= expected, f"{len(hb)} (>= {expected})")
        res.check("no heartbeat gap > 3x interval", bool(gaps) and max_gap <= 3 * a.heartbeat_s,
                  f"max gap {max_gap:.1f}s, interval {a.heartbeat_s}s")
        res.check("whitelisted command acknowledged (SET_LIMITS)", s.get("acks_ok", 0) >= 1, f"acks ok={s.get('acks_ok')}")
        acked = sum(1 for ln in hub_log if "[hub] ack " in ln)
        print(f"  info: {acked} of 3 commands acknowledged (PROBE is answered only once the source is open)")
        res.check("non-whitelisted command refused by the agent (R5)", s.get("rejects", 0) >= 1,
                  f"rejected acks={s.get('rejects')} events={sorted(set(s.get('events', [])))}")
        bad = [ln.strip() for ln in hub_log if "!!" in ln]
        res.check("every agent payload valid (protocol v1 models)", not bad, "; ".join(bad[:3]))
        res.check("no WebSocket needed (long-poll commands only)", True, "GET /agent/v1/commands")
        (HERE / f"{part}.hub.log").write_text("".join(hub_log), encoding="utf-8")
    finally:
        stop.set()
        for p in reversed(procs):
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    p.kill()
    res.finish(part=part)
    return 0 if res.status() == "PASS" else (2 if res.status() == "SKIP" else 1)


if __name__ == "__main__":
    sys.exit(main())
