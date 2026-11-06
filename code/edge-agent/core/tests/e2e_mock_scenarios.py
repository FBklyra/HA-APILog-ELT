"""aleltagent.exe against mock_hub.py: per-agent credential (REQ-26 / AHS-04) and 409 recovery (AHS-05).

No database is touched: the source alias does not exist, so the plugin never connects; batches are
pre-seeded into the spool. Scenarios:
  1. enrollment token -> register(credential.v1) -> alelta_ credential stored (DPAPI) and used from then on;
     short TTL -> rotated; restart -> the stored credential is reused (no new issue, enrollment token unused);
     no token value in the agent log or console output.
  2. 409 with expected_from past the head batch -> batch deleted (SPOOL_BATCH_COVERED); a straddling batch
     -> SPOOL_BLOCKED_409 once, kept; gap cleared -> shipped.

  python edge-agent/core/tests/e2e_mock_scenarios.py [--exe <aleltagent.exe>] [--port 18197]
Exit 0 = PASS. Windows only.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import struct
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
EXE = HERE.parent / "build" / "x64-release" / "bin" / "aleltagent.exe"
ENROLL = "alelte_" + uuid.uuid4().hex


def post(port: int, path: str, obj: dict) -> dict:
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=json.dumps(obj).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read() or b"{}")


def seed_batch(spool: Path, seq: int, frm: str, to: str, nxt: str) -> str:
    rec = b"\0" * 56 + struct.pack("<I", 20) + b"\0" * 16          # one filter-ON framed record
    bid = str(uuid.uuid4())
    hdr = json.dumps({"batch_id": bid, "connection_id": "h2conn", "agent_id": "h2-agent", "engine": "db2",
                      "from_pos": {"engine": "db2", "text": frm}, "to_pos": {"engine": "db2", "text": to},
                      "next_pos": {"engine": "db2", "text": nxt}, "rec_count": 1, "raw_bytes": len(rec),
                      "sha256": hashlib.sha256(rec).hexdigest(), "codec": "none", "protocol": 1}).encode()
    data = b"ALB1" + struct.pack("<I", len(hdr)) + hdr + struct.pack("<Q", len(rec)) + rec + b"ALBE"
    (spool / f"{seq:016x}.aleltb").write_bytes(data)
    return bid


def wait(pred, timeout: float, what: str) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return
        time.sleep(0.5)
    raise AssertionError(f"timeout waiting for {what}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--exe", default=str(EXE))
    ap.add_argument("--port", type=int, default=18197)
    a = ap.parse_args()
    work = Path(tempfile.mkdtemp(prefix="alelt-h2-e2e-"))
    spool, logd, out = work / "spool", work / "log", work / "hub"
    spool.mkdir()
    cfg = {
        "agent_id": "h2-agent", "connection_id": "h2conn",
        "hub": {"url": f"http://127.0.0.1:{a.port}", "mode": "token", "token_env": "ALELT_H2_E2E_TOKEN",
                "heartbeat_s": 5, "command_wait_s": 1, "backoff_max_s": 5},
        "spool": {"dir": str(spool), "max_mb": 64}, "log": {"dir": str(logd), "level": "DEBUG"},
        "require_job_limits": False, "governor": {"enabled": False},
        "source": {"plugin": "aleltsrc_db2.dll", "engine": "db2", "database": "H2NODB",
                   "dll_dirs": [os.environ.get("DB2PATH", r"D:\IBM\SQLLIB") + r"\BIN"],
                   "settings": {"database": "H2NODB"}},
        "capture": {"autostart": False},
    }
    (work / "aleltagent.json").write_text(json.dumps(cfg))
    seed_batch(spool, 1, "1:0000000000000100", "1:0000000000000110", "1:0000000000000111")
    seed_batch(spool, 2, "1:0000000000000111", "1:0000000000000120", "1:0000000000000121")
    env = dict(os.environ, ALELT_H2_E2E_TOKEN=ENROLL)
    hub = subprocess.Popen([sys.executable, "-I", str(HERE / "mock_hub.py"), "--port", str(a.port), "--token", ENROLL,
                            "--enroll", "--cred-ttl-s", "30", "--out", str(out)],
                           stdout=open(work / "hub-console.log", "w"), stderr=subprocess.STDOUT)
    agents: list[subprocess.Popen] = []
    logs: list[str] = []

    def start_agent() -> subprocess.Popen:
        # console output to a file, never an unread pipe (a full pipe would block the agent's logger)
        f = open(work / f"agent-run{len(agents)}.out", "w")
        p = subprocess.Popen([a.exe, "--console", "--config", str(work / "aleltagent.json")], env=env,
                             stdout=f, stderr=subprocess.STDOUT)
        p.alelt_out = f  # type: ignore[attr-defined]
        agents.append(p)
        return p

    def stop(p: subprocess.Popen) -> None:
        p.terminate()
        try:
            p.wait(timeout=20)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
        p.alelt_out.close()  # type: ignore[attr-defined]
        logs.append(Path(p.alelt_out.name).read_text(errors="replace"))  # type: ignore[attr-defined]

    def stats() -> dict:
        return post(a.port, "/admin/stats", {})

    try:
        time.sleep(1.5)
        post(a.port, "/admin/gap", {"expected_from": "1:0000000000000115", "count": 1000})
        ag = start_agent()
        wait(lambda: stats()["cred"]["issued"] == 1, 30, "credential issued")
        wait(lambda: stats()["cred"]["rotated"] >= 1, 45, "credential rotated at 2/3 of its ttl")
        wait(lambda: "SPOOL_BLOCKED_409" in stats()["events"], 40, "SPOOL_BLOCKED_409")
        s = stats()
        assert "SPOOL_BATCH_COVERED" in s["events"], s
        assert s["events"].count("SPOOL_BLOCKED_409") == 1, s
        assert not (spool / "0000000000000001.aleltb").exists() and (spool / "0000000000000002.aleltb").exists()
        post(a.port, "/admin/gap", {"expected_from": "", "count": 0})
        wait(lambda: stats()["batches"] == 1, 30, "blocked batch shipped once the gap is gone")
        cred_file = spool / "credential.bin"
        assert cred_file.exists() and b"alelta_" not in cred_file.read_bytes(), "credential not DPAPI-protected"
        stop(ag)
        before = stats()["cred"]
        ag = start_agent()                       # restart: stored credential, no new issue
        wait(lambda: stats()["cred"]["used"] > before["used"] + 2, 30, "stored credential used after restart")
        after = stats()["cred"]
        assert after["issued"] == 1 and after["enroll_after_claim"] == 0, after
        stop(ag)
        text = "\n".join(logs) + "".join(f.read_text(errors="replace") for f in logd.glob("*.log"))
        assert "alelta_" not in text and ENROLL not in text, "a token value reached the agent log"
        print("PASS e2e_mock_scenarios:", json.dumps(after))
        return 0
    except AssertionError as e:
        print("FAIL:", e)
        for p in agents:
            if p.poll() is None:
                stop(p)
        (work / "agent-console.log").write_text("\n".join(logs))
        print("agent output:", work / "agent-console.log", "| mock hub stats:", json.dumps(stats()))
        return 1
    finally:
        for p in agents:
            if p.poll() is None:
                p.kill()
        hub.terminate()
        try:
            hub.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            hub.kill()


if __name__ == "__main__":
    sys.exit(main())
