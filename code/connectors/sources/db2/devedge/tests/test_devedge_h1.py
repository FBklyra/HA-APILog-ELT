"""Dev edge review fixes: atomic state + spool (LNX-25), protocol alignment (LNX-27: {"message"}, from_pos object,
command id validation, discard_spool), per-agent credential (REQ-26). No Db2 needed (connection is lazy)."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE.parents[4] / "hub"))

import devedge  # noqa: E402

CRED = "alelta_" + "C" * 40


class FakeHub:
    def __init__(self, cmds: List[Dict[str, Any]], reject: str = "") -> None:
        self.cmds, self.reject = cmds, reject
        self.h = {"X-ALELT-Agent-Token": "env-token"}
        self.env_token = "env-token"
        self.acks: List[Any] = []
        self.paths: List[str] = []

    def set_token(self, t: str) -> None:
        self.h["X-ALELT-Agent-Token"] = t

    def call(self, method: str, path: str, body: Any = None, headers: Any = None, timeout: Any = None) -> Any:
        self.paths.append(path)
        if self.reject and self.h["X-ALELT-Agent-Token"] == self.reject:
            raise devedge.HubError(401, "{}")
        if path.startswith("/agent/v1/commands?"):
            c, self.cmds = self.cmds, []
            return c
        return {}

    def post_json(self, path: str, obj: Any) -> Any:
        self.paths.append(path)
        if self.reject and self.h["X-ALELT-Agent-Token"] == self.reject:
            raise devedge.HubError(401, "{}")
        if "/ack" in path:
            self.acks.append((path, obj))
        if path == "/agent/v1/register":
            return {"accepted": True, "resume_from": None, "credential": CRED,
                    "credential_expires_at": "2099-01-01T00:00:00+00:00"}
        return {}


class DevEdge(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="aleltde_"))
        os.environ["ALELT_AGENT_TOKEN"] = "env-token"
        self.cfg = devedge.Config(hub_url="http://127.0.0.1:9", agent_id="de1", connection_id="c1",
                                  spool_dir=str(self.tmp / "spool"), state_file=str(self.tmp / "state.json"),
                                  credential_file=str(self.tmp / "cred.json"))

    def agent(self, hub: FakeHub) -> Any:
        a = devedge.Agent(self.cfg)
        a.hub = hub
        return a

    def test_state_atomic_and_corrupt_state_ignored(self) -> None:
        a = self.agent(FakeHub([]))
        a.batch_from = "1:000000000000000A"
        a.save_state()
        self.assertEqual(json.loads((self.tmp / "state.json").read_text())["batch_from"], "1:000000000000000A")
        self.assertFalse(list(self.tmp.glob("*.tmp")))
        (self.tmp / "state.json").write_text("{trunc")
        self.assertEqual(a.load_state(), {})
        (self.tmp / "spool" / "000000000009.batch.tmp").write_bytes(b"partial")
        devedge.Spool(self.tmp / "spool", 1 << 20)
        self.assertFalse(list((self.tmp / "spool").glob("*.tmp")))

    def test_commands_protocol_shape(self) -> None:
        a = self.agent(FakeHub([]))
        a.spool.put({"batch_id": "b", "to_pos": {"text": "1:0000000000000001"},
                     "next_pos": {"text": "1:0000000000000002"}}, b"x")
        a.hub.cmds = [{"command_id": "../evil", "name": "PAUSE"},
                      {"command_id": "aaaaaaaa-0000-0000-0000-000000000001", "name": "START_CAPTURE",
                       "args": {"from_pos": {"text": "1:0000000000000005"}}},
                      {"command_id": "aaaaaaaa-0000-0000-0000-000000000002", "name": "START_CAPTURE",
                       "args": {"from_pos": {"text": "1:0000000000000005"}, "discard_spool": True}},
                      {"command_id": "aaaaaaaa-0000-0000-0000-000000000003", "name": "SNAPSHOT_TABLE"}]
        a.poll_commands(inline=True)
        self.assertFalse(a.paused)                                  # invalid id: ignored, never acked
        acks = a.hub.acks
        self.assertEqual(len(acks), 3)
        self.assertFalse(acks[0][1]["ok"])
        self.assertIn("discard_spool", acks[0][1]["result"]["message"])
        self.assertTrue(acks[1][1]["ok"])
        self.assertEqual((a.cursor, a.reposition), ("1:0000000000000005", "aaaaaaaa-0000-0000-0000-000000000002"))
        self.assertEqual(a.spool.files(), [])
        self.assertIn("message", acks[2][1]["result"])
        self.assertNotIn("error", acks[2][1]["result"])
        self.assertEqual(acks[2][1]["result"]["code"], "NOT_ALLOWED")      # typed error code

    def test_heavy_commands_run_on_the_command_thread_once(self) -> None:
        """SNAPSHOT_TABLE / VERIFY_TABLE are queued for the command thread (capture and heartbeats continue); a
        re-delivered command id is executed once."""
        import threading
        import time as _t

        a = self.agent(FakeHub([]))
        cmd = {"command_id": "bbbbbbbb-0000-0000-0000-000000000001", "name": "VERIFY_TABLE",
               "args": {"table": {"schema": "S", "name": "T"}}}
        a.hub.cmds = [cmd, dict(cmd)]
        a.poll_commands()
        self.assertEqual(a.hub.acks, [])                             # nothing ran on the capture thread
        self.assertEqual(a.cmdq.qsize(), 1)                          # duplicate delivery ignored
        a.hub.cmds = [dict(cmd)]
        a.poll_commands()                                            # hub re-delivery while it runs: ignored
        self.assertEqual(a.cmdq.qsize(), 1)
        a.cmd_thread = threading.Thread(target=a.command_loop, daemon=True)
        a.cmd_thread.start()
        t0 = _t.monotonic()
        while not a.hub.acks and _t.monotonic() - t0 < 5:
            _t.sleep(0.05)
        a.stop = True
        a.cmd_thread.join(5)
        self.assertEqual(len(a.hub.acks), 1)
        self.assertEqual(a.hub.acks[0][1]["result"]["code"], "NOT_ALLOWED")

    def test_credential_exchange_reuse_and_fallback(self) -> None:
        a = self.agent(FakeHub([]))
        a.register()
        self.assertEqual(a.hub.h["X-ALELT-Agent-Token"], CRED)
        f = self.tmp / "cred.json"
        self.assertEqual(json.loads(f.read_text())["credential"], CRED)
        if sys.platform != "win32":
            self.assertEqual(f.stat().st_mode & 0o777, 0o600)
        b = devedge.Agent(self.cfg)                                 # restart: stored credential used
        self.assertEqual(b.hub.h["X-ALELT-Agent-Token"], CRED)
        hub = FakeHub([], reject=CRED)
        b.hub = hub
        hub.set_token(CRED)
        b.flush_spool() if b.spool.files() else None
        self.assertTrue(b._credential_rejected())
        self.assertEqual(hub.h["X-ALELT-Agent-Token"], "env-token")
        self.assertFalse(f.exists())

    def test_takeover_of_paused_connection_stays_paused(self) -> None:
        """AHS-24: grant capturing=false -> paused after TAKEOVER; capturing absent -> runs."""
        from alelt_common.lease_client import LeaseEvent

        for cap, want in ((False, True), (None, False)):
            a = self.agent(FakeHub([]))
            from types import SimpleNamespace

            a.lease = SimpleNamespace(enabled=True, may_read=lambda: True,
                                      tick=lambda c=cap: LeaseEvent("takeover", 2, "de1",
                                                                    {"text": "1:0000000000000009"}, capturing=c))
            self.assertTrue(a.ha_step(None))
            self.assertEqual((a.paused, a.cursor), (want, "1:0000000000000009"))

    def test_post_connect_deny_check_wired_into_connect(self) -> None:
        """LNX-08 / AHS-08 (Python edge): connect() asks the server for its real host and closes + exits 78 when
        it is deny-listed or cannot be identified (fail closed)."""
        closed: List[bool] = []

        class FakeDb2:
            host: Any = None

            def __init__(self, *a: Any) -> None:
                pass

            def server_info(self) -> Dict[str, Any]:
                return {"host": FakeDb2.host}

            def close(self) -> None:
                closed.append(True)

        real = devedge.Db2
        devedge.Db2 = FakeDb2  # type: ignore[misc]
        try:
            self.cfg.deny_hosts = ["*.prod.corp.local"]
            for host, refused in (("db2p.prod.corp.local", True), (None, True), ("db2t.test.corp.local", False)):
                FakeDb2.host = host
                closed.clear()
                a = self.agent(FakeHub([]))
                a.db = None
                if refused:
                    with self.assertRaises(SystemExit) as e:
                        a.connect()
                    self.assertEqual((e.exception.code, closed, a.db), (78, [True], None))
                else:
                    self.assertIsNotNone(a.connect())
        finally:
            devedge.Db2 = real  # type: ignore[misc]
            self.cfg.deny_hosts = []

    def test_probe_commits_before_next_read(self) -> None:
        """LNX-24: every catalog probe ends its unit of work (no open read transaction before db2ReadLog)."""
        from alelt_db2edge.db2 import Db2

        log: List[str] = []
        db = Db2.__new__(Db2)
        db.query = lambda sql, *a: (log.append("query"), [(1,)] if "DUMMY" in sql else [("logfilsiz", "1024")])[1]  # type: ignore[method-assign]
        db.commit = lambda: log.append("commit")  # type: ignore[method-assign]
        db.query_lri = lambda: ("1:0", "1:9")  # type: ignore[method-assign]
        for name in ("db_health", "log_config", "current_position"):
            log.clear()
            db.probe(name)
            self.assertEqual(log[-1], "commit", name)
        with self.assertRaises(ValueError):
            db.probe("drop_table")


if __name__ == "__main__":
    unittest.main()
