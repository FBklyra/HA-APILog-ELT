"""aleltagent.exe + the mock source plugin (aleltsrc_mock.dll) against mock_hub.py - no database.

Scenarios (each asserted, result JSON written next to this file as result.mock-plugin.json):
  source_down   open() fails with a SQL30082N message holding control / 0x7F / non-ASCII bytes:
                PROBE and SNAPSHOT_CATALOG are acknowledged ok:false with the source error (never left
                queued), DB2_AUTH_FAILED event delivered with a clean message (AHS-15).
  interleave    capture keeps shipping batches while a slow multi-chunk SNAPSHOT_TABLE runs, the
                governor (log utilisation from the plugin's db_health probe) pauses the running snapshot
                and capture, and releases them; snapshot completes; VERIFY_TABLE acknowledged (AHS-06).
  bad_chunk_key a plugin returning more key columns than the ABI allows -> ack ok:false, agent alive (AHS-12).
  no_position   probe_position failing -> SNAPSHOT_TABLE refused ok:false, no chunk posted (AHS-14).
  chain         every batch from_pos == previous next_pos (no gap, no duplicate).

  python edge-agent/core/tests/e2e_mock_plugin.py [--build build/x64-release] [--port 18213]
Exit 0 = PASS. Windows only.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
TOKEN = "mock-" + uuid.uuid4().hex


def post(port: int, path: str, obj: dict) -> dict:
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=json.dumps(obj).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read() or b"{}")


def wait(pred, timeout: float, what: str) -> None:  # type: ignore[no-untyped-def]
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return
        time.sleep(0.3)
    raise AssertionError(f"timeout waiting for {what}")


class Env:
    def __init__(self, build: Path, port: int) -> None:
        self.port = port
        self.work = Path(tempfile.mkdtemp(prefix="alelt-mock-plugin-"))
        self.bin = self.work / "bin"
        self.bin.mkdir()
        shutil.copy2(build / "bin" / "aleltagent.exe", self.bin)
        shutil.copy2(build / "testbin" / "aleltsrc_mock.dll", self.bin)
        self.control = self.work / "control.json"
        self.set_control()
        self.hub = subprocess.Popen([sys.executable, "-I", str(HERE / "mock_hub.py"), "--port", str(port), "--token",
                                     TOKEN, "--out", str(self.work / "hub")],
                                    stdout=open(self.work / "hub-console.log", "w"), stderr=subprocess.STDOUT)
        self.agents: list[subprocess.Popen] = []
        time.sleep(1.5)

    def set_control(self, **kw: object) -> None:
        tmp = self.control.with_suffix(".tmp")
        tmp.write_text(json.dumps(kw))
        tmp.replace(self.control)

    def start_agent(self, name: str, settings: dict, governor: dict | None = None) -> subprocess.Popen:
        spool = self.work / f"spool-{name}"
        cfg = {
            "agent_id": f"mock-{name}", "connection_id": "mockconn",
            "hub": {"url": f"http://127.0.0.1:{self.port}", "mode": "token", "token_env": "ALELT_MOCK_E2E_TOKEN",
                    "heartbeat_s": 5, "command_wait_s": 1, "backoff_max_s": 5},
            "spool": {"dir": str(spool), "max_mb": 64}, "log": {"dir": str(self.work / f"log-{name}"), "level": "DEBUG"},
            "require_job_limits": False, "governor": governor or {"enabled": False},
            "source": {"plugin": "aleltsrc_mock.dll", "engine": "db2", "database": "MOCKDB",
                       "settings": {"control_file": str(self.control), **settings}},
            "snapshot": {"allowed_tables": ["MOCK.T1", "MOCK.BADKEY"], "max_chunk_rows": 100},
            "capture": {"autostart": False},
        }
        path = self.work / f"aleltagent-{name}.json"
        path.write_text(json.dumps(cfg))
        out = open(self.work / f"agent-{name}.out", "w")
        p = subprocess.Popen([str(self.bin / "aleltagent.exe"), "--console", "--config", str(path)],
                             env={**__import__("os").environ, "ALELT_MOCK_E2E_TOKEN": TOKEN}, stdout=out,
                             stderr=subprocess.STDOUT)
        p.alelt_out = out  # type: ignore[attr-defined]
        self.agents.append(p)
        return p

    def stop_agent(self, p: subprocess.Popen) -> None:
        p.terminate()
        try:
            p.wait(timeout=20)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
        p.alelt_out.close()  # type: ignore[attr-defined]

    def stats(self) -> dict:
        return post(self.port, "/admin/stats", {})

    def command(self, name: str, args: dict) -> str:
        return post(self.port, "/admin/command", {"name": name, "args": args})["command_id"]

    def ack(self, cid: str) -> dict | None:
        return next((a for a in self.stats()["ack_log"] if a["id"] == cid), None)

    def close(self) -> None:
        for p in self.agents:
            if p.poll() is None:
                self.stop_agent(p)
        self.hub.terminate()
        try:
            self.hub.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            self.hub.kill()


def source_down(e: Env, checks: list) -> None:
    msg = "SQL30082N Security processing failed with reason \"24\" \u0001\u007fé (\"USERNAME AND/OR PASSWORD INVALID\")"
    ag = e.start_agent("down", {"fail_open": msg})
    probe = e.command("PROBE", {"name": "db_health"})
    cat = e.command("SNAPSHOT_CATALOG", {"schemas": ["MOCK"]})
    wait(lambda: e.ack(probe) is not None and e.ack(cat) is not None, 40, "acks while the source is down")
    pa, ca = e.ack(probe), e.ack(cat)
    assert pa["ok"] is False and ca["ok"] is False, (pa, ca)
    assert "source unavailable: SQL30082N" in json.dumps(pa["result"]), pa
    ev = [x for x in e.stats()["event_log"] if x["type"] == "DB2_AUTH_FAILED"]
    assert ev, "DB2_AUTH_FAILED not delivered"
    m = ev[0]["detail"]["message"]
    assert all(0x20 <= ord(ch) < 0x7F for ch in m) and ev[0]["detail"]["sqlcode"] == -30082, ev[0]
    checks.append({"name": "source down: PROBE/CATALOG acked ok:false, DB2_AUTH_FAILED clean", "status": "PASS",
                   "detail": json.dumps(pa["result"])[:200]})
    e.stop_agent(ag)


def interleave(e: Env, checks: list) -> None:
    gov = {"enabled": True, "interval_s": 1, "sustain_samples": 1, "host_cpu_pause_pct": 100,
           "host_cpu_resume_pct": 99, "max_log_util_pct": 90}
    e.set_control(snapshot_delay_ms=1500, log_util=10)
    ag = e.start_agent("main", {"rate_per_s": 200, "snapshot_rows": 900}, gov)
    sc = e.command("START_CAPTURE", {})
    wait(lambda: (e.ack(sc) or {}).get("ok") is True, 30, "START_CAPTURE ack")
    wait(lambda: e.stats()["batches"] >= 3, 30, "capture batches")
    snap = e.command("SNAPSHOT_TABLE", {"table": {"schema": "MOCK", "name": "T1"}, "chunk_rows": 100})
    wait(lambda: len([c for c in e.stats()["chunk_log"] if c["table"] == "MOCK.T1"]) >= 2, 30, "two chunks")
    e.set_control(snapshot_delay_ms=1500, log_util=95)       # governor: log utilisation too high
    wait(lambda: any(x["type"] == "PAUSED_BY_GOVERNOR" for x in e.stats()["event_log"]), 30, "governor pause")
    paused_at = time.time()
    time.sleep(6)
    s = e.stats()
    late_chunks = [c for c in s["chunk_log"] if c["ts"] > paused_at + 2.5]
    late_batches = [b for b in s["batch_log"] if b["ts"] > paused_at + 2.5]
    assert not late_chunks, f"snapshot kept running while the governor paused: {late_chunks}"
    assert not late_batches, f"capture kept reading while the governor paused: {late_batches[:3]}"
    e.set_control(snapshot_delay_ms=1500, log_util=10)
    wait(lambda: any(x["type"] == "GOVERNOR_RESUMED" for x in e.stats()["event_log"]), 30, "governor release")
    wait(lambda: e.ack(snap) is not None, 90, "snapshot ack")
    assert e.ack(snap)["ok"] is True, e.ack(snap)
    s = e.stats()
    chunks = [c for c in s["chunk_log"] if c["table"] == "MOCK.T1"]
    assert len(chunks) == 9 and chunks[-1]["last"] and all(c["start_pos"] for c in chunks), chunks
    t_first, t_last = chunks[0]["ts"], chunks[-1]["ts"]
    during = [b for b in s["batch_log"] if t_first < b["ts"] < t_last]
    assert len(during) >= 2, f"capture starved during the snapshot ({len(during)} batches between chunks)"
    checks.append({"name": "capture interleaved with a slow snapshot; governor pauses + releases it (AHS-06)",
                   "status": "PASS", "detail": f"{len(chunks)} chunks over {t_last - t_first:.1f}s, "
                                               f"{len(during)} batches meanwhile, paused >= 6 s"})
    vid = str(uuid.uuid4())
    ver = e.command("VERIFY_TABLE", {"table": {"schema": "MOCK", "name": "T1"}, "verify_id": vid,
                                     "checks": ["row_count"]})
    wait(lambda: e.ack(ver) is not None, 60, "verify ack")
    assert e.ack(ver)["ok"] is True and "mock" in json.dumps(e.ack(ver)["result"]), e.ack(ver)
    checks.append({"name": "VERIFY_TABLE through the plugin ABI 2", "status": "PASS", "detail": ""})

    n_chunks = len(e.stats()["chunk_log"])
    bad = e.command("SNAPSHOT_TABLE", {"table": {"schema": "MOCK", "name": "BADKEY"}, "chunk_rows": 10})
    wait(lambda: e.ack(bad) is not None, 30, "BADKEY ack")
    assert e.ack(bad)["ok"] is False and "invalid chunk key" in json.dumps(e.ack(bad)["result"]), e.ack(bad)
    assert len(e.stats()["chunk_log"]) == n_chunks, "a chunk with an invalid key was posted"
    assert ag.poll() is None, "agent died on a bad plugin key"
    checks.append({"name": "plugin chunk key > ALELT_MAX_KEYCOLS refused before use (AHS-12)", "status": "PASS",
                   "detail": ""})

    e.set_control(position_fail=True)
    nop = e.command("SNAPSHOT_TABLE", {"table": {"schema": "MOCK", "name": "T1"}, "chunk_rows": 100})
    wait(lambda: e.ack(nop) is not None, 30, "no-position ack")
    assert e.ack(nop)["ok"] is False and "position" in json.dumps(e.ack(nop)["result"]), e.ack(nop)
    assert len(e.stats()["chunk_log"]) == n_chunks, "snapshot without a start position posted chunks"
    e.set_control()
    checks.append({"name": "snapshot without a start position refused (AHS-14)", "status": "PASS",
                   "detail": json.dumps(e.ack(nop)["result"])[:160]})

    time.sleep(2)
    bl = e.stats()["batch_log"]
    breaks = [(a, b) for a, b in zip(bl, bl[1:]) if b["from"] != a["next"]]
    assert not breaks and len(bl) >= 5, breaks[:3]
    checks.append({"name": "batch chain from_pos == previous next_pos", "status": "PASS",
                   "detail": f"{len(bl)} batches, {sum(b['recs'] for b in bl)} records"})
    e.stop_agent(ag)


def stop_aborts_inflight(e: Env, checks: list) -> None:
    """AHS-23 (console part): a stop while a hub request hangs (black-hole hub, request_timeout 600 s) ends within
    seconds: in-flight WinHTTP requests are aborted, the process exits; nothing in the spool is lost."""
    import signal
    import socket
    import threading

    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(16)
    port = srv.getsockname()[1]
    conns: list = []

    def accept() -> None:
        while True:
            try:
                c, _ = srv.accept()
            except OSError:
                return
            conns.append(c)                      # read nothing, answer nothing

    threading.Thread(target=accept, daemon=True).start()
    cfg = {"agent_id": "mock-hang", "connection_id": "mockconn",
           "hub": {"url": f"http://127.0.0.1:{port}", "mode": "token", "token_env": "ALELT_MOCK_E2E_TOKEN",
                   "heartbeat_s": 5, "command_wait_s": 1, "backoff_max_s": 5, "request_timeout_ms": 600000},
           "spool": {"dir": str(e.work / "spool-hang"), "max_mb": 64}, "log": {"dir": str(e.work / "log-hang")},
           "require_job_limits": False, "governor": {"enabled": False},
           "source": {"plugin": "aleltsrc_mock.dll", "engine": "db2", "database": "MOCKDB", "settings": {}},
           "capture": {"autostart": False}}
    path = e.work / "aleltagent-hang.json"
    path.write_text(json.dumps(cfg))
    out = open(e.work / "agent-hang.out", "w")
    p = subprocess.Popen([str(e.bin / "aleltagent.exe"), "--console", "--config", str(path)],
                         env={**__import__("os").environ, "ALELT_MOCK_E2E_TOKEN": TOKEN}, stdout=out,
                         stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
    wait(lambda: len(conns) >= 1, 30, "agent request to the black-hole hub")
    time.sleep(3)
    t0 = time.time()
    p.send_signal(signal.CTRL_BREAK_EVENT)
    try:
        p.wait(timeout=60)
    finally:
        if p.poll() is None:
            p.kill()
        out.close()
        srv.close()
    took = time.time() - t0
    txt = Path(out.name).read_text(errors="replace")
    assert took < 30, f"stop took {took:.0f}s with a hanging hub request"
    assert "aborted" in txt, txt[-800:]
    checks.append({"name": "stop aborts in-flight hub requests (AHS-23, console)", "status": "PASS",
                   "detail": f"stopped in {took:.1f}s with request_timeout 600 s"})


def install_refuses_invalid_config(e: Env, checks: list) -> None:
    """AHS-09: `--install` with a config that does not load returns 2 BEFORE the SCM is touched (works without
    elevation: no service is registered). The generated mtls config without a thumbprint is such a config."""
    cfgp = e.work / "install-test" / "aleltagent.json"
    cfgp.parent.mkdir()
    r = subprocess.run([str(e.bin / "aleltagent.exe"), "--install", "--config", str(cfgp), "--hub-url",
                        "https://hub.invalid:8443", "--agent-id", "inst-test", "--connection-id", "instconn",
                        "--database", "MOCKDB"], capture_output=True, text=True, timeout=60)
    txt = r.stdout + r.stderr
    assert r.returncode == 2, (r.returncode, txt[-600:])
    assert "NOT installed" in txt and "OpenSCManager" not in txt and "CreateService" not in txt, txt[-600:]
    assert cfgp.exists(), "starter config not written"
    checks.append({"name": "--install refuses an invalid config before the SCM (AHS-09)", "status": "PASS",
                   "detail": txt.strip().splitlines()[-1][:200]})


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", default=str(HERE.parent / "build" / "x64-release"))
    ap.add_argument("--port", type=int, default=18213)
    a = ap.parse_args()
    build = Path(a.build)
    if not (build / "testbin" / "aleltsrc_mock.dll").exists():
        print("SKIP: aleltsrc_mock.dll not built (build.ps1)")
        return 2
    checks: list = []
    e = Env(build, a.port)
    status = "PASS"
    try:
        source_down(e, checks)
        interleave(e, checks)
        stop_aborts_inflight(e, checks)
        install_refuses_invalid_config(e, checks)
    except AssertionError as ex:
        status = "FAIL"
        checks.append({"name": "scenario", "status": "FAIL", "detail": str(ex)[:500]})
        print("FAIL:", ex, "| work dir:", e.work)
    finally:
        e.close()
    res = {"name": "aleltagent.exe + mock source plugin vs mock_hub", "status": status, "checks": checks,
           "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    (HERE / "result.mock-plugin.json").write_text(json.dumps(res, indent=1))
    print(status, json.dumps(checks, indent=1))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
