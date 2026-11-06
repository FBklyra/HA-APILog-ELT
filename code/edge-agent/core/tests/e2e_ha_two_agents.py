"""XC-26: TWO aleltagent.exe --console instances (active/passive, same connection) against a throw-away REAL hub.

No database source and no service install: both agents load the mock source plugin (aleltsrc_mock.dll) in wall-clock
mode, so they read the same synthetic log. The hub is `uvicorn alelt_hub.app:app` on a scratch SQLite metadata
database (no worker: batches are only received). Checked:
  1. exactly one agent holds the capture lease, the other is STANDBY (its commands are refused);
  2. the active agent ships batches;
  3. the active agent is frozen (suspended) past the lease TTL -> the standby takes over with epoch + 1 and continues
     from the hub durable position;
  4. the old holder is resumed: it self-fences (or is refused with 423) and never gets a batch accepted after the
     takeover;
  5. the new holder is killed -> the old one takes over again (epoch + 2);
  6. the batch chain in the hub is gap- and duplicate-free across both takeovers (from_lri == previous next_lri).

  python edge-agent/core/tests/e2e_ha_two_agents.py [--build build/x64-release]
Writes result.ha-two-agents.json next to this file. Exit 0 = PASS. Windows only.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

try:
    import psutil
    import uvicorn  # noqa: F401 - the hub is started with `python -m uvicorn`
except ImportError as _e:   # prerequisites missing -> skipped (ctest SKIP_RETURN_CODE 2)
    print(f"SKIP: {_e}")
    sys.exit(2)

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
TTL, RENEW = 10, 3


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def wait(pred, timeout: float, what: str) -> None:  # type: ignore[no-untyped-def]
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if pred():
                return
        except sqlite3.OperationalError:
            pass
        time.sleep(0.5)
    raise AssertionError(f"timeout waiting for {what}")


def main() -> int:  # noqa: C901 - one linear scenario
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", default=str(HERE.parent / "build" / "x64-release"))
    a = ap.parse_args()
    build = Path(a.build)
    if not (build / "testbin" / "aleltsrc_mock.dll").exists():
        print("SKIP: aleltsrc_mock.dll not built (build.ps1)")
        return 2
    work = Path(tempfile.mkdtemp(prefix="alelt-ha2-"))
    binw = work / "bin"
    binw.mkdir()
    shutil.copy2(build / "bin" / "aleltagent.exe", binw)
    shutil.copy2(build / "testbin" / "aleltsrc_mock.dll", binw)
    token = uuid.uuid4().hex
    port = free_port()
    dbf = work / "meta.db"
    env = {**os.environ, "ALELT_DB_URL": f"sqlite:///{dbf.as_posix()}", "ALELT_SPOOL_DIR": str(work / "hub-spool"),
           "ALELT_AGENT_TOKENS": token, "ALELT_DEV_AUTH": "1", "ALELT_ENV": "test", "ALELT_HA_AGENT": token,
           "PYTHONPATH": str(REPO / "hub"), "PYTHONIOENCODING": "utf-8"}
    hub_log = open(work / "hub.log", "wb")
    hub = subprocess.Popen([sys.executable, "-m", "uvicorn", "alelt_hub.app:app", "--host", "127.0.0.1", "--port",
                            str(port), "--no-access-log"], env=env, stdout=hub_log, stderr=subprocess.STDOUT)
    base_ms = int(time.time() * 1000)
    agents: dict[str, subprocess.Popen] = {}
    outs: dict[str, object] = {}
    checks: list[dict] = []
    status = "PASS"

    def check(name: str, ok: bool, detail: str = "") -> None:
        checks.append({"name": name, "status": "PASS" if ok else "FAIL", "detail": detail[:400]})
        if not ok:
            raise AssertionError(f"{name}: {detail}")

    def start(x: str) -> None:
        cfg = {
            "agent_id": f"ha2-{x}", "connection_id": "ha2conn",
            "hub": {"url": f"http://127.0.0.1:{port}", "mode": "token", "token_env": "ALELT_HA_AGENT",
                    "heartbeat_s": 5, "command_wait_s": 1, "backoff_max_s": 5},
            "spool": {"dir": str(work / f"spool-{x}"), "max_mb": 64},
            "log": {"dir": str(work / f"log-{x}"), "level": "DEBUG"},
            "require_job_limits": False, "governor": {"enabled": False},
            "source": {"plugin": "aleltsrc_mock.dll", "engine": "db2", "database": "MOCKDB",
                       "settings": {"rate_per_s": 100, "base_unix_ms": base_ms}},
            "capture": {"autostart": True},
            "ha": {"mode": "active_passive", "lease_ttl_s": TTL, "renew_s": RENEW},
        }
        p = work / f"aleltagent-{x}.json"
        p.write_text(json.dumps(cfg))
        f = open(work / f"agent-{x}.out", "a")
        outs[x] = f
        agents[x] = subprocess.Popen([str(binw / "aleltagent.exe"), "--console", "--config", str(p)], env=env,
                                     stdout=f, stderr=subprocess.STDOUT)

    def q(sql: str, args: tuple = ()) -> list[tuple]:
        with sqlite3.connect(dbf, timeout=10) as c:
            return c.execute(sql, args).fetchall()

    def lease() -> tuple[str | None, int]:
        r = q("SELECT holder, epoch FROM capture_lease WHERE connection_id = 'ha2conn'")
        return (r[0][0], int(r[0][1])) if r else (None, 0)

    def batches() -> list[tuple]:
        return q("SELECT seq, agent_id, from_lri, to_lri, next_lri, received_at FROM batch "
                 "WHERE connection_id = 'ha2conn' ORDER BY seq")

    def console(method: str, path: str, body: dict | None = None) -> dict:
        req = urllib.request.Request(f"http://127.0.0.1:{port}/api/v1{path}", method=method,
                                     data=json.dumps(body).encode() if body is not None else None,
                                     headers={"Content-Type": "application/json", "X-ALELT-Dev-Role": "PlatformAdmin",
                                              "X-ALELT-Dev-User": "ha2-test"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read() or b"{}")

    def cmd_row(cid: str) -> tuple | None:
        r = q("SELECT status, result FROM agent_command WHERE id IN (?, ?)", (cid, uuid.UUID(cid).hex))
        return r[0] if r else None

    try:
        t0 = time.time()
        while time.time() - t0 < 60:
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=2)
                break
            except (urllib.error.URLError, ConnectionError, OSError):
                time.sleep(0.5)
        start("a")
        time.sleep(2)
        start("b")
        wait(lambda: lease()[0] is not None, 60, "a capture lease holder")
        holder, ep1 = lease()
        other = "ha2-b" if holder == "ha2-a" else "ha2-a"
        x_act, x_sb = holder[-1], other[-1]  # type: ignore[index]
        wait(lambda: len(q("SELECT id FROM agent WHERE connection_id='ha2conn'")) == 2, 30, "both agents registered")
        check("one holder, one standby", holder in ("ha2-a", "ha2-b"), f"holder {holder} epoch {ep1}")
        # standby refuses commands; START_CAPTURE goes to the holder (first start: no durable position yet)
        rej = console("POST", f"/agents/{other}/commands", {"name": "PROBE", "args": {"name": "db_health"},
                                                            "reason": "ha2 standby check"})["command_id"]
        pos0 = f"1:{256 + int((time.time() * 1000 - base_ms) * 0.1):016X}"
        sc = console("POST", f"/agents/{holder}/commands", {"name": "START_CAPTURE", "args": {"from_pos": pos0},
                                                            "reason": "ha2 start"})["command_id"]
        wait(lambda: len(batches()) >= 5, 60, "batches from the holder")
        wait(lambda: (cmd_row(rej) or ("PENDING",))[0] in ("FAILED", "ACKED"), 30, "standby answers")
        row = cmd_row(rej)
        check("standby refuses source commands", bool(row) and row[0] == "FAILED" and "standby" in str(row[1]).lower(),
              str(row)[:300])
        check("active ships batches", all(b[1] == holder for b in batches()), f"{len(batches())} batches")

        # 3. freeze the holder past the TTL -> takeover
        n_before = len(batches())
        ps = psutil.Process(agents[x_act].pid)
        ps.suspend()
        frozen_at = time.time()
        wait(lambda: lease()[0] == other, TTL * 4, "takeover by the standby")
        h2, ep2 = lease()
        check("takeover with epoch + 1", h2 == other and ep2 == ep1 + 1, f"{h2} epoch {ep1} -> {ep2}")
        wait(lambda: any(b[1] == other for b in batches()), 60, "batches from the new holder")
        takeover_seq = min(b[0] for b in batches() if b[1] == other)

        # 4. resume the old holder: it must never get a batch accepted after the takeover
        ps.resume()
        time.sleep(RENEW * 4)
        late = [b for b in batches() if b[1] == holder and b[0] > takeover_seq]
        log_old = Path(outs[x_act].name).read_text(errors="replace")  # type: ignore[attr-defined]
        fenced = ("STANDBY" in log_old and ("self-fencing" in log_old or "423" in log_old or "lease lost" in log_old
                                            or "lease refused" in log_old))
        check("old holder fenced after resume (self-fence or 423), no late batch", not late and fenced,
              f"late={late[:2]} frozen {time.time() - frozen_at:.0f}s")

        # 5. kill the new holder -> the old one takes over again
        agents[x_sb].kill()
        wait(lambda: lease()[0] == holder, TTL * 4, "second takeover")
        h3, ep3 = lease()
        check("second takeover epoch + 2", h3 == holder and ep3 == ep1 + 2, f"{h3} epoch {ep3}")
        nb = len(batches())
        wait(lambda: len(batches()) >= nb + 3, 60, "batches after the second takeover")

        # 6. chain
        bl = batches()
        breaks = [(p, n) for p, n in zip(bl, bl[1:]) if n[2] != p[4]]
        dups = len(bl) - len({b[3] for b in bl})
        check("hub batch chain gap- and duplicate-free across both takeovers", not breaks and dups == 0,
              f"{len(bl)} batches ({n_before} before the 1st takeover), breaks={breaks[:2]}, dup to_lri={dups}")
        # 7. lease moved away under a running holder (operator / another hub node): its next batch is refused
        #    with 423 and it self-fences on that answer, before any renew
        with sqlite3.connect(dbf, timeout=10) as c:
            c.execute("UPDATE capture_lease SET holder = 'ha2-ghost', epoch = epoch + 1, "
                      "expires_at = '2999-01-01 00:00:00' WHERE connection_id = 'ha2conn'")
        moved_seq = max(b[0] for b in batches())
        out_h = Path(outs[holder[-1]].name)  # type: ignore[attr-defined, index]
        wait(lambda: "423" in out_h.read_text(errors="replace") or "lease refused" in out_h.read_text(errors="replace"),
             30, "holder reacts to the moved lease")
        txt = out_h.read_text(errors="replace")
        time.sleep(RENEW * 2)
        late2 = [b for b in batches() if b[0] > moved_seq]
        check("moved lease: holder fenced by 423 (or refused renew), no batch accepted",
              not late2 and "now STANDBY" in txt.split("epoch 3")[-1],
              "423 path" if "fenced the batch (423)" in txt else "renew refused path")
        del sc
    except AssertionError as e:
        status = "FAIL"
        print("FAIL:", e, "| work dir:", work)
    finally:
        for p in agents.values():
            if p.poll() is None:
                try:
                    psutil.Process(p.pid).resume()
                except psutil.Error:
                    pass
                p.terminate()
                try:
                    p.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    p.kill()
        hub.terminate()
        try:
            hub.wait(timeout=10)
        except subprocess.TimeoutExpired:
            hub.kill()
    res = {"name": "two C agents active/passive vs the real hub (mock source)", "status": status, "checks": checks,
           "work_dir": str(work), "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    (HERE / "result.ha-two-agents.json").write_text(json.dumps(res, indent=1))
    print(status, json.dumps(checks, indent=1))
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
