"""Daemon behaviour against the stdlib fake hub with the fake source (Windows + Linux):
signal -> event -> loop exit -> save_checkpoint -> exit 0; restart resumes with no gap / no duplicate; shutdown
deadline forces exit with state.json written; deny_hosts refusal (78); backpressure; HA active/passive handover."""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any, Callable

import _support
from _support import FakeHub, TOKEN, child_env, fake_config, txn_numbers, write_config
from aleltd import config as cfgmod, daemon
from aleltd.__main__ import install_signals
from aleltd.daemon import Agent

os.environ.setdefault("ALELT_AGENT_TOKEN", TOKEN)
WIN = sys.platform == "win32"


def wait_for(cond: Callable[[], bool], timeout: float = 20.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(0.05)
    return False


def spawn(cfg_path: Path) -> subprocess.Popen:
    kw: dict = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if WIN else {}
    return subprocess.Popen([sys.executable, "-m", "aleltd", "--config", str(cfg_path)], env=child_env(),
                            cwd=str(_support.LINUX), stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kw)


def terminate(p: subprocess.Popen) -> None:
    os.kill(p.pid, signal.CTRL_BREAK_EVENT if WIN else signal.SIGTERM)   # type: ignore[attr-defined]


def state(tmp: Path) -> dict:
    return json.loads((tmp / "spool" / "state.json").read_text(encoding="utf-8"))


class DaemonTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="aleltd_"))
        self.hub = FakeHub()

    def tearDown(self) -> None:
        self.hub.close()

    def agent(self, **over: Any) -> Agent:
        return Agent(cfgmod.load(str(write_config(self.tmp, fake_config(self.tmp, self.hub.url, **over)))))

    def test_signal_sets_event_loop_exits_checkpoint_exit0(self) -> None:
        ag = self.agent()
        calls = []
        real = daemon.save_checkpoint

        def spy(*a: Any, **k: Any) -> bool:
            calls.append(a[1])
            return real(*a, **k)
        daemon.save_checkpoint = spy
        old = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
        try:
            install_signals(ag)

            def fire() -> None:
                wait_for(lambda: len(self.hub.batches) >= 3)
                signal.raise_signal(signal.SIGINT)
            threading.Thread(target=fire, daemon=True).start()
            t0 = time.monotonic()
            code = ag.run()
        finally:
            daemon.save_checkpoint = real
            signal.set_wakeup_fd(-1)
            for s, h in old.items():
                signal.signal(s, h)
        self.assertEqual(code, 0)
        self.assertEqual(calls, ["SIGINT"])
        self.assertTrue(ag.stop_event.is_set())
        self.assertLess(time.monotonic() - t0, 20)
        st = state(self.tmp)
        self.assertEqual(st["next_pos"], self.hub.last_next())        # drained: hub == local position
        self.assertEqual(st["durable_pos"], self.hub.last_next())
        self.assertTrue(st["capturing"])
        self.assertEqual(st["reason"], "SIGINT")
        self.assertFalse([p for p in (self.tmp / "spool").iterdir() if p.suffix in (".batch", ".catalog")])
        self.assertEqual(self.hub.heartbeats[-1]["state"], "STOPPING")

    def test_process_stop_restart_resumes_without_gap(self) -> None:
        cfgp = write_config(self.tmp, fake_config(self.tmp, self.hub.url))
        for rnd in (1, 2):
            n0 = len(self.hub.batches)
            p = spawn(cfgp)
            try:
                self.assertTrue(wait_for(lambda: len(self.hub.batches) >= n0 + 3), "no batches acked")
                terminate(p)
                out, err = p.communicate(timeout=30)
            finally:
                if p.poll() is None:
                    p.kill()
            self.assertEqual(p.returncode, 0, err.decode(errors="replace")[-2000:])
            self.assertIn(b"saving checkpoint", out)
            self.assertEqual(state(self.tmp)["next_pos"], self.hub.last_next(), f"round {rnd}")
        nums = txn_numbers(self.hub)
        self.assertEqual(nums, list(range(1, len(nums) + 1)))          # contiguous, no duplicate, no gap
        self.assertEqual(self.hub.rejects, [])
        self.assertEqual(self.hub.registers, 2)

    def test_shutdown_deadline_forces_exit_with_state(self) -> None:
        cfg = fake_config(self.tmp, self.hub.url, shutdown={"deadline_s": 5, "drain_s": 1})
        cfg["source"]["settings"]["hang_after_s"] = 1.5                  # main loop blocks inside the source
        p = spawn(write_config(self.tmp, cfg))
        try:
            self.assertTrue(wait_for(lambda: len(self.hub.batches) >= 1))
            time.sleep(1.5)
            t0 = time.monotonic()
            terminate(p)
            p.communicate(timeout=20)
            took = time.monotonic() - t0
        finally:
            if p.poll() is None:
                p.kill()
        self.assertEqual(p.returncode, 1)            # LNX-30: a forced stop is not reported as a clean one
        self.assertGreaterEqual(took, 4.0)
        self.assertLess(took, 9.0)
        st = state(self.tmp)
        self.assertEqual(st["reason"], "deadline")
        self.assertEqual(st["next_pos"], self.hub.last_next())

    def test_deny_hosts_refuses_with_78(self) -> None:
        over = {"deny_hosts": ["*.prod.corp.local"]}
        ag = self.agent(**over)
        ag.cfg.source.settings["host"] = "pg1.prod.corp.local"
        self.assertEqual(ag.run(), 78)
        self.assertEqual(self.hub.registers, 0)

    def test_spool_full_pauses_reading(self) -> None:
        sp = self.tmp / "spool"
        sp.mkdir(parents=True, exist_ok=True)
        hdr = {"batch_id": "00000000-0000-0000-0000-000000000001", "from_pos": {"text": "000000000000999"},
               "to_pos": {"text": "000000000001999"}, "next_pos": {"text": "000000000001999"}}
        (sp / "0000000000000001.batch").write_bytes(json.dumps(hdr).encode() + b"\n" + b"x" * (1 << 20))
        cfg = fake_config(self.tmp, "http://127.0.0.1:9", spool={"max_mb": 1})          # hub down
        ag = Agent(cfgmod.load(str(write_config(self.tmp, cfg))))
        t = threading.Thread(target=ag.run, daemon=True)
        t.start()
        self.assertTrue(wait_for(lambda: ag.state_name == "SPOOL_FULL", 10))
        time.sleep(0.5)
        ag.request_stop("test")
        t.join(30)
        self.assertEqual(len(list(sp.glob("*.batch"))), 1)          # nothing read, nothing dropped
        self.assertEqual(state(self.tmp)["next_pos"], "000000000001999")


class HaTests(unittest.TestCase):
    def test_active_passive_handover_on_clean_stop(self) -> None:
        hub = FakeHub(lease=True)
        tmp = Path(tempfile.mkdtemp(prefix="aleltha_"))
        ags = []
        try:
            for name in ("a", "b"):
                d = tmp / name
                d.mkdir()
                cfg = fake_config(d, hub.url, agent_id=f"lx-{name}", ha={"mode": "active_passive",
                                                                         "lease_ttl_s": 6, "renew_s": 1})
                ags.append(Agent(cfgmod.load(str(write_config(d, cfg)))))
            ta = threading.Thread(target=ags[0].run, daemon=True)
            ta.start()
            self.assertTrue(wait_for(lambda: len(hub.batches) >= 3))
            tb = threading.Thread(target=ags[1].run, daemon=True)
            tb.start()
            self.assertTrue(wait_for(lambda: ags[1].state_name == "STANDBY"))
            n = len(hub.batches)
            time.sleep(1.0)
            self.assertFalse(any(b["agent_id"] == "lx-b" for b in hub.batches))
            ags[0].request_stop("test")
            ta.join(30)
            self.assertEqual(hub.released, 1)
            self.assertTrue(wait_for(lambda: any(b["agent_id"] == "lx-b" for b in hub.batches), 15), "no takeover")
            self.assertTrue(wait_for(lambda: len(hub.batches) >= n + 3))
            ags[1].request_stop("test")
            tb.join(30)
        finally:
            for a in ags:
                a.request_stop("cleanup")
            hub.close()
        nums = txn_numbers(hub)
        self.assertEqual(nums, list(range(1, len(nums) + 1)))
        self.assertEqual(hub.lease["epoch"], 2)
        self.assertEqual(hub.rejects, [])


if __name__ == "__main__":
    unittest.main()
