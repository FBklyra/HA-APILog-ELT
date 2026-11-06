"""Review fixes LNX-01..32 + REQ-26 (per-agent credential): corrupt / sha-mismatched spool entries, hub refusals
(409 / 413 / 422 / 423 / 426 / 503 + Retry-After / non-JSON / 3xx), ENOSPC backpressure, reposition across restart,
takeover fallback, command whitelist, config refusals, lease fence timing, credential exchange / rotation /
fallback, deny-list normalisation, source error classification, SPI idle-tick conformance."""
from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import sys
import tempfile
import threading
import unittest
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import _support
from _support import TOKEN, fake_config, write_config
from aleltd import config as cfgmod, guard
from aleltd.config import ConfigError
from aleltd.credential import CredentialStore
from aleltd.daemon import EXIT_CONFIG, Agent
from aleltd.hub import HubClient, HubError
from aleltd.sources import SourceError
from aleltd.sources.base import TxnTooLarge, classify_source_error
from aleltd.sources.fake import FakeConnector
from aleltd.sources.remote import check_idle_ticks
from aleltd.spool import CorruptEntry, Spool, SpoolFull, verify_body

from alelt_common.lease_client import LeaseState

os.environ.setdefault("ALELT_AGENT_TOKEN", TOKEN)
WIN = sys.platform == "win32"


def pos(n: int) -> str:
    return f"{n:015d}"


def batch(frm: int, to: int, raw: bytes = b'{"kind":"COMMIT"}\n') -> Tuple[Dict[str, Any], bytes]:
    hdr = {"batch_id": f"00000000-0000-0000-0000-{to:012d}", "connection_id": "fake1", "agent_id": "lx-test",
           "engine": "fake", "from_pos": {"engine": "fake", "text": pos(frm)},
           "to_pos": {"engine": "fake", "text": pos(to)}, "next_pos": {"engine": "fake", "text": pos(to)},
           "rec_count": 1, "raw_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(), "codec": "zlib",
           "protocol": 1}
    return hdr, zlib.compress(raw)


class StubHub:
    """In-process HubClient stand-in: scripted answers per path, every call recorded."""

    def __init__(self, resume: Optional[str] = None) -> None:
        self.calls: List[Tuple[str, str, Dict[str, str]]] = []
        self.events: List[Dict[str, Any]] = []
        self.blocks: List[Dict[str, Any]] = []
        self.script: Dict[str, Callable[[Dict[str, str], Any], Any]] = {}
        self.resume = resume
        self.env_token: Optional[str] = TOKEN
        self.headers: Dict[str, str] = {"X-ALELT-Agent-Token": TOKEN}

    @property
    def token(self) -> Optional[str]:
        return self.headers.get("X-ALELT-Agent-Token")

    def set_token(self, t: Optional[str]) -> None:
        if t:
            self.headers["X-ALELT-Agent-Token"] = t
        else:
            self.headers.pop("X-ALELT-Agent-Token", None)

    def call(self, method: str, path: str, body: Optional[bytes] = None, headers: Optional[Dict[str, str]] = None,
             timeout: Optional[float] = None) -> Any:
        self.calls.append((method, path, dict(headers or {})))
        p = path.split("?")[0]
        obj = json.loads(body) if body and (headers or {}).get("Content-Type") == "application/json" else body
        for prefix, fn in self.script.items():
            if p.startswith(prefix):
                return fn(headers or {}, obj)
        if p == "/agent/v1/register":
            return {"accepted": True, "resume_from": {"engine": "fake", "text": self.resume} if self.resume else None}
        if p == "/agent/v1/events":
            self.events.append(obj)
            return {"ok": True}
        if p == "/agent/v1/blocks":
            hdr = json.loads((headers or {})["X-ALELT-Batch"])
            self.blocks.append(hdr)
            return {"batch_id": hdr["batch_id"], "durable_pos": hdr["to_pos"], "duplicate": False}
        if p == "/agent/v1/commands":
            return []
        return {"ok": True}

    def post_json(self, path: str, obj: Any, timeout: Optional[float] = None) -> Any:
        return self.call("POST", path, json.dumps(obj).encode(), {"Content-Type": "application/json"}, timeout)

    def event_types(self) -> List[str]:
        return [e["type"] for e in self.events]


class Base(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="aleltr_"))
        self.sp = self.tmp / "spool"

    def agent(self, hub: Optional[StubHub] = None, start: bool = True, **over: Any) -> Agent:
        self.hub = hub or StubHub()
        ag = Agent(cfgmod.load(str(write_config(self.tmp, fake_config(self.tmp, "http://127.0.0.1:9", **over)))),
                   hub=self.hub)  # type: ignore[arg-type]
        if start:
            ag.startup()
        return ag


# ---------------------------------------------------------------------------------------------- spool (LNX-01/02/29)
class SpoolTests(Base):
    def test_corrupt_entry_quarantined_never_fatal(self) -> None:
        sp = Spool(str(self.sp))
        sp.put_batch(*batch(0, 1999))
        (self.sp / "0000000000000002.batch").write_bytes(b"garbage without newline")
        (self.sp / "notes.batch").write_bytes(b"x")                       # unexpected name
        sp2 = Spool(str(self.sp))
        self.assertEqual(len(sp2.entries()), 2)
        self.assertEqual(sp2.last_next(), pos(1999))                      # corrupt newest skipped + quarantined
        self.assertTrue((self.sp / "quarantine" / "0000000000000002.batch").exists())
        self.assertTrue((self.sp / "quarantine" / "notes.batch").exists())

    def test_header_read_without_body_and_verify(self) -> None:
        sp = Spool(str(self.sp))
        p = sp.put_batch(*batch(0, 1999))
        sp2 = Spool(str(self.sp))
        self.assertEqual(sp2.header(p)["to_pos"]["text"], pos(1999))
        hdr, body = sp2.get(p)
        verify_body(hdr, body)
        with self.assertRaises(CorruptEntry):
            verify_body(hdr, body[:-2])                                   # truncated zlib trailer
        with self.assertRaises(CorruptEntry):
            verify_body({**hdr, "sha256": "0" * 64}, body)

    def test_enospc_is_spoolfull(self) -> None:
        import errno
        sp = Spool(str(self.sp))
        real = os.fsync

        def boom(fd: int) -> None:
            raise OSError(errno.ENOSPC, "No space left on device")
        os.fsync = boom  # type: ignore[assignment]
        try:
            with self.assertRaises(SpoolFull):
                sp.put_batch(*batch(0, 1999))
        finally:
            os.fsync = real  # type: ignore[assignment]
        self.assertEqual(sp.entries(), [])
        self.assertFalse(list(self.sp.glob("*.tmp")))


# ---------------------------------------------------------------------------------------------- shipping
class ShipTests(Base):
    def test_corrupt_head_entry_quarantined_and_reread_from_hub(self) -> None:
        self.sp.mkdir(parents=True)
        (self.sp / "0000000000000001.batch").write_bytes(b"{not json\n" + b"x")
        ag = self.agent(StubHub(resume=pos(999)))
        self.assertTrue(ag.chain_ready)
        self.assertEqual(ag.chain, pos(999))                              # from the hub, not from the broken file
        self.assertTrue((self.sp / "quarantine" / "0000000000000001.batch").exists())

    def test_sha_mismatch_quarantined_and_reread_from_its_from_pos(self) -> None:
        ag = self.agent(StubHub(resume=pos(999)))
        hdr, body = batch(999, 1999)
        p = ag.spool.put_batch(hdr, body)
        ag.spool.put_batch(*batch(1999, 2999))
        p.write_bytes(p.read_bytes()[:-3] + b"zzz")                       # bit rot after spooling
        ag.spool = Spool(str(self.sp))
        ag.ship_spool()
        self.assertEqual(self.hub.blocks, [])                             # nothing corrupt was sent
        self.assertEqual(ag.chain, pos(999))
        self.assertEqual(ag.spool.entries(), [])                          # it and everything after: quarantined
        self.assertIn("SPOOL_ENTRY_QUARANTINED", self.hub.event_types())

    def test_422_reread_then_hold(self) -> None:
        hub = StubHub(resume=pos(999))
        hub.script["/agent/v1/blocks"] = lambda h, b: (_ for _ in ()).throw(HubError(422, '{"detail":"bad"}'))
        ag = self.agent(hub)
        for _ in range(3):
            ag.spool.put_batch(*batch(999, 1999))
            ag.ship_spool()
        self.assertIsNotNone(ag.hold_reason)
        self.assertFalse(ag.may_ship())
        self.assertEqual(len(ag.spool.batches()), 1)                      # held, kept
        self.assertIn("BATCH_REJECTED", hub.event_types())

    def test_409_covered_deleted_else_blocked_event(self) -> None:
        hub = StubHub(resume=pos(999))
        exp = {"v": pos(5999)}
        hub.script["/agent/v1/blocks"] = lambda h, b: (_ for _ in ()).throw(
            HubError(409, json.dumps({"detail": {"error": "gap", "expected_from": exp["v"]}})))
        ag = self.agent(hub)
        ag.spool.put_batch(*batch(999, 1999))
        ag.ship_spool()
        self.assertEqual(ag.spool.batches(), [])
        self.assertIn("SPOOL_BATCH_COVERED", hub.event_types())
        exp["v"] = pos(999)
        ag.spool.put_batch(*batch(2999, 3999))
        ag.ship_spool()
        self.assertEqual(len(ag.spool.batches()), 1)
        self.assertIn("SPOOL_BLOCKED_409", hub.event_types())

    def test_423_standalone_exits_78(self) -> None:
        hub = StubHub(resume=pos(999))
        hub.script["/agent/v1/blocks"] = lambda h, b: (_ for _ in ()).throw(HubError(423, "{}"))
        ag = self.agent(hub)
        ag.spool.put_batch(*batch(999, 1999))
        ag.ship_spool()
        self.assertEqual(ag.exit_code, EXIT_CONFIG)
        self.assertTrue(ag.stop_event.is_set())

    def test_503_retry_after_honoured(self) -> None:
        hub = StubHub(resume=pos(999))
        hub.script["/agent/v1/blocks"] = lambda h, b: (_ for _ in ()).throw(
            HubError(503, "{}", {"retry-after": "7"}))
        ag = self.agent(hub, start=False)
        ag.cfg.hub.backoff_max_s = 30
        ag.startup()
        ag.spool.put_batch(*batch(999, 1999))
        t0 = ag.clock()
        ag.ship_spool()
        self.assertGreaterEqual(ag.next_ship - t0, 6.9)
        self.assertLessEqual(ag.next_ship - t0, 7 * 1.2 + 0.5)

    def test_standby_never_ships(self) -> None:
        ag = self.agent(ha={"mode": "active_passive", "lease_ttl_s": 30, "renew_s": 10})
        ag.spool.put_batch(*batch(999, 1999))
        self.assertFalse(ag.may_ship())
        self.assertEqual(ag.ship_spool(), 0)
        self.assertEqual(self.hub.blocks, [])

    def test_enospc_backpressure_in_capture(self) -> None:
        ag = self.agent(StubHub(resume=pos(999)))

        def full(*a: Any) -> Any:
            raise SpoolFull(28, "spool volume full")
        ag.spool.put_batch = full  # type: ignore[assignment]
        ag.spool.put_catalog = full  # type: ignore[assignment]
        ag.registered = True
        self.assertEqual(ag.capture_step(), 1.0)
        self.assertEqual(ag.state_name, "SPOOL_FULL")
        self.assertIn("SPOOL_FULL", self.hub.event_types())


# ---------------------------------------------------------------------------------------------- positions
class PositionTests(Base):
    def test_reposition_survives_restart_before_first_batch(self) -> None:
        ag = self.agent(StubHub(resume=pos(999)))
        ok, _ = ag.execute("START_CAPTURE", {"from_pos": {"text": pos(5999)}}, "11111111-1111-1111-1111-111111111111")
        self.assertTrue(ok)
        ag2 = self.agent(StubHub(resume=pos(999)))                      # restart, spool empty, hub still at 999
        self.assertEqual(ag2.chain, pos(5999))
        self.assertEqual(ag2.reposition_cmd, "11111111-1111-1111-1111-111111111111")

    def test_takeover_without_resume_uses_state(self) -> None:
        ag = self.agent(StubHub(resume=None), ha={"mode": "active_passive", "lease_ttl_s": 30, "renew_s": 10})
        ag.registered = True
        ag.saved["next_pos"] = pos(4999)
        ag.decide_position(None, takeover=True)
        self.assertEqual(ag.chain, pos(4999))
        self.assertIn("TAKEOVER_WITHOUT_RESUME", self.hub.event_types())

    def test_takeover_of_operator_paused_connection_stays_paused(self) -> None:
        """AHS-24: the grant says capturing=false (PAUSE went to the old holder): the new holder starts paused."""
        from alelt_common.lease_client import LeaseState

        st = LeaseState(ttl_s=30, renew_s=10)
        ev = st.on_grant({"granted": True, "holder": "lx-b", "lease_id": "l1", "epoch": 2, "ttl_s": 30,
                          "takeover": True, "capturing": False}, 0.0)
        self.assertEqual((ev.kind, ev.capturing), ("takeover", False))
        for ha_cap, want in ((False, True), (None, False), (True, False)):
            ag = self.agent(StubHub(resume=pos(999)), ha={"mode": "active_passive", "lease_ttl_s": 30, "renew_s": 10})
            ag.registered = True
            ag.lease.due = lambda now: True  # type: ignore[method-assign]

            def tick(timeout_s: float, cap: Any = ha_cap) -> Tuple[str, Optional[str]]:
                ag.lease.capturing = cap
                return "takeover", pos(1999)

            ag.lease.tick = tick  # type: ignore[method-assign]
            ag.tick_lease(0.0)
            self.assertEqual(ag.paused, want, ha_cap)
            self.assertEqual(ag.chain, pos(1999))


# ---------------------------------------------------------------------------------------------- commands (LNX-11/21)
class CommandTests(Base):
    def test_whitelist_and_failures_never_raise(self) -> None:
        ag = self.agent(StubHub(resume=pos(999)))
        self.assertFalse(ag.execute("DROP_TABLE", {}, "c")[0])
        self.assertFalse(ag.execute("PROBE", {"name": "rm -rf"}, "c")[0])
        ag.source.probe = lambda n: (_ for _ in ()).throw(RuntimeError("plugin bug"))  # type: ignore[assignment]
        ok, res = ag.execute("PROBE", {"name": "current_position"}, "c")
        self.assertFalse(ok)
        self.assertIn("RuntimeError", res["message"])
        self.assertIn("COMMAND_REJECTED", self.hub.event_types())
        ok, res = ag.execute("SET_LIMITS", {"cpu_pct": 10 ** 6, "batch_kb": 1, "evil": 5}, "c")
        self.assertTrue(ok)
        self.assertEqual(res["limits"]["cpu_pct"], 100)
        self.assertEqual(res["limits"]["batch_kb"], 16)
        self.assertNotIn("evil", res["limits"])

    def test_start_capture_non_empty_spool_and_discard(self) -> None:
        ag = self.agent(StubHub(resume=pos(999)))
        ag.spool.put_batch(*batch(999, 1999))
        ok, res = ag.execute("START_CAPTURE", {"from_pos": pos(7999)}, "c1")
        self.assertFalse(ok)
        self.assertIn("discard_spool", res["message"])
        ok, _ = ag.execute("START_CAPTURE", {"from_pos": pos(7999), "discard_spool": True}, "c2")
        self.assertTrue(ok)
        self.assertEqual(ag.spool.batches(), [])
        self.assertEqual((ag.chain, ag.reposition_cmd), (pos(7999), "c2"))
        self.assertIn("SPOOL_DISCARDED", self.hub.event_types())

    def test_command_id_validated_and_url_quoted(self) -> None:
        hub = StubHub(resume=pos(999))
        hub.script["/agent/v1/commands"] = lambda h, b: [
            {"command_id": "../../x?y", "name": "PAUSE"},
            {"command_id": "22222222-2222-2222-2222-222222222222", "name": "PAUSE", "args": {}}]
        ag = self.agent(hub)
        ag.poll_commands()
        acks = [c[1] for c in hub.calls if "/ack" in c[1]]
        self.assertEqual(len(acks), 1)
        self.assertTrue(acks[0].startswith("/agent/v1/commands/22222222-2222-2222-2222-222222222222/ack?agent_id="))
        self.assertTrue(ag.paused)


# ---------------------------------------------------------------------------------------------- config / startup
class ConfigTests(Base):
    def parse(self, **over: Any) -> Any:
        return cfgmod.parse(fake_config(self.tmp, "http://127.0.0.1:9", **over), self.tmp)

    def test_http_only_loopback_dev_or_opt_in(self) -> None:
        with self.assertRaises(ConfigError):
            self.parse(hub={"url": "http://hub.corp:8080"})
        self.parse(hub={"url": "http://[::1]:8080"})
        self.parse(hub={"url": "http://hub.corp:8080", "mode": "dev"})
        self.parse(hub={"url": "http://hub.corp:8080", "allow_insecure_http": True})

    def test_inline_secrets_refused(self) -> None:
        for k in ("password", "db_pwd", "api_key", "auth_token"):
            with self.assertRaises(ConfigError, msg=k):
                self.parse(source={"engine": "fake", "settings": {k: "s3cret"}})
        self.parse(source={"engine": "fake", "settings": {"password_env": "X", "ssl": {"key_file": "/k"}}})

    def test_limits_max_minimum(self) -> None:
        with self.assertRaises(ConfigError):
            self.parse(limits_max={"cpu_pct": 0})
        c = self.parse(limits_max={"max_rate_kbps": 0, "batch_kb": 64})
        self.assertEqual(c.clamp({"batch_kb": 10 ** 6})["batch_kb"], 64)

    def test_source_error_at_startup_is_exit_78(self) -> None:
        cfg = cfgmod.load(str(write_config(self.tmp, fake_config(self.tmp, "http://127.0.0.1:9"))))

        def bad(_: Any) -> Any:
            raise SourceError("connector alelt_src_x.connector:X unavailable")
        self.assertEqual(Agent(cfg, hub=StubHub(), source_factory=bad).run(), EXIT_CONFIG)  # type: ignore[arg-type]

    def test_watchdog_not_starved_by_governor_sleep_or_slow_hub(self) -> None:
        """LNX-04: a long governor sleep (cpu_pct 10 -> 9x the busy time) and hub calls that use their whole
        timeout never make the watchdog thread withhold its ping (WatchdogSec = 2 s here)."""
        import time as _t

        ag = self.agent(start=False)
        pings: List[float] = []
        withheld: List[str] = []
        ag.notifier.watchdog_s = 2.0
        ag.notifier.watchdog = lambda: pings.append(_t.monotonic())  # type: ignore[method-assign]
        rec = logging.Handler()
        rec.emit = lambda r: withheld.append(r.getMessage()) if "withholding" in r.getMessage() else None  # type: ignore[method-assign]
        logging.getLogger("aleltd").addHandler(rec)
        try:
            ag._touch()
            th = threading.Thread(target=ag._watchdog, daemon=True)
            th.start()
            ag.limits = {"cpu_pct": 10}
            ag._governor_sleep(0.5)                        # 4.5 s of throttling sleep
            for _ in range(3):                             # slow hub: every call uses its whole bounded timeout
                ag._touch()
                _t.sleep(ag._net_timeout())
            ag.done_event.set()
            th.join(5)
        finally:
            logging.getLogger("aleltd").removeHandler(rec)
        self.assertLessEqual(ag._net_timeout(), 1.0)
        self.assertFalse(withheld, withheld)
        self.assertGreaterEqual(len(pings), 5)
        self.assertLess(max(b - a for a, b in zip(pings, pings[1:])), 2.0)

    def test_net_timeout_below_watchdog(self) -> None:
        ag = self.agent(start=False)
        ag.notifier.watchdog_s = 20.0
        self.assertLessEqual(ag._net_timeout(), 8.0)


# ---------------------------------------------------------------------------------------------- hub client (LNX-16)
class _Odd(BaseHTTPRequestHandler):
    def log_message(self, *a: Any) -> None:
        pass

    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if self.path.startswith("/redirect"):
            self.send_response(302)
            self.send_header("Location", "http://elsewhere/")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        data = b"<html>proxy login</html>"
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class HubClientTests(unittest.TestCase):
    def test_non_json_and_redirect_are_huberrors(self) -> None:
        srv = ThreadingHTTPServer(("127.0.0.1", 0), _Odd)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            cfg = cfgmod.HubCfg(url=f"http://127.0.0.1:{srv.server_address[1]}", mode="token")
            h = HubClient(cfg, "a")
            with self.assertRaises(HubError) as e1:
                h.post_json("/x", {})
            self.assertEqual(e1.exception.status, 502)
            with self.assertRaises(HubError) as e2:
                h.post_json("/redirect", {})
            self.assertEqual(e2.exception.status, 302)
        finally:
            srv.shutdown()
            srv.server_close()


# ---------------------------------------------------------------------------------------------- REQ-26 credential
class CredentialTests(Base):
    CRED = "alelta_" + "A" * 40

    def test_exchange_store_restart_reuse_and_never_logged(self) -> None:
        hub = StubHub(resume=pos(999))
        hub.script["/agent/v1/register"] = lambda h, b: {"accepted": True, "resume_from": None,
                                                         "credential": self.CRED,
                                                         "credential_expires_at": "2099-01-01T00:00:00+00:00"}
        buf = io.StringIO()
        from aleltd import logging_setup
        logging_setup.setup("DEBUG", "never", stdout=buf, stderr=buf)
        try:
            ag = self.agent(hub)
        finally:
            logging_setup.setup("WARNING", "never")
        reg = [c for c in hub.calls if c[1] == "/agent/v1/register"][0]
        self.assertEqual(hub.token, self.CRED)
        f = Path(ag.cfg.credential_file)
        self.assertEqual(json.loads(f.read_text())["credential"], self.CRED)
        if not WIN:
            self.assertEqual(f.stat().st_mode & 0o777, 0o600)
        self.assertNotIn(self.CRED, buf.getvalue())
        self.assertTrue(reg)
        hub2 = StubHub(resume=pos(999))
        self.agent(hub2)                                                  # restart: stored credential reused
        self.assertEqual(hub2.token, self.CRED)

    def test_rotation_persists_new_before_switch(self) -> None:
        st = CredentialStore(str(self.tmp / "c.json"))
        st.save(self.CRED, "2000-01-01T00:00:00+00:00")
        self.assertTrue(st.rotate_due())
        hub = StubHub(resume=pos(999))
        new = "alelta_" + "B" * 40
        hub.script["/agent/v1/credential/rotate"] = lambda h, b: {"credential": new,
                                                                  "credential_expires_at": "2099-01-01T00:00:00Z"}
        (self.sp).mkdir(parents=True, exist_ok=True)
        CredentialStore(str(self.sp / "credential.json")).save(self.CRED, "2000-01-02T00:00:00+00:00")
        ag = self.agent(hub)
        ag.maybe_rotate_credential()
        self.assertEqual(hub.token, new)
        self.assertEqual(json.loads((self.sp / "credential.json").read_text())["credential"], new)

    def test_401_with_stored_credential_falls_back_once(self) -> None:
        self.sp.mkdir(parents=True, exist_ok=True)
        CredentialStore(str(self.sp / "credential.json")).save(self.CRED, "2099-01-01T00:00:00+00:00")
        hub = StubHub(resume=pos(999))

        def reg(h: Dict[str, str], b: Any) -> Any:
            if h.get("X-ALELT-Agent-Token") == self.CRED or hub.token == self.CRED:
                raise HubError(401, '{"detail":"revoked"}')
            return {"accepted": True, "resume_from": None}
        hub.script["/agent/v1/register"] = reg
        ag = self.agent(hub)
        self.assertFalse(ag.registered)
        self.assertEqual(hub.token, TOKEN)                                # fell back to the env token
        self.assertFalse((self.sp / "credential.json").exists())
        self.assertTrue(ag.try_register())


# ---------------------------------------------------------------------------------------------- misc
class MiscTests(unittest.TestCase):
    def test_lease_fence_clock_starts_at_send(self) -> None:
        s = LeaseState(True, ttl_s=30, renew_s=10)
        s.on_grant({"granted": True, "epoch": 1, "lease_id": "x", "ttl_s": 30}, now=14.0, sent_at=0.0)
        self.assertTrue(s.must_fence(20.0))                               # 0 + (30 - 10): a 14 s reply costs margin
        st = LeaseState(True, ttl_s=30, renew_s=10)
        st.on_error(100.0)
        self.assertGreaterEqual(st.next_at - 100.0, 8.0)                   # standby: renew_s +-20 %, not 1 s

    def test_deny_list_normalised_and_fail_closed(self) -> None:
        with self.assertRaises(guard.GuardError):
            guard.check("TEST", False, ["*.prod.corp.local"], {"host": "db2prod.PROD.corp.local."})
        with self.assertRaises(guard.GuardError):
            guard.check("TEST", False, ["[::1]:5432"], {"server": "[::1]:5432"})
        with self.assertRaises(guard.GuardError):
            guard.check("TEST", False, ["10.0.0.0/8"], {"host": "no-such-host.invalid"})
        with self.assertRaises(guard.GuardError):
            guard.check_connected(["*.prod.corp.local"], {"host": None, "error": "SQL0551N"})
        with self.assertRaises(guard.GuardError):
            guard.check_connected(["*.prod.corp.local"], {"host": "db2p.prod.corp.local"})
        guard.check_connected(["*.prod.corp.local"], {"host": "db2t.test.corp.local"})

    def test_source_error_classes(self) -> None:
        self.assertEqual(classify_source_error("SQL30082N Security processing failed")[0], "DB2_AUTH_FAILED")
        self.assertEqual(classify_source_error("db2ReadLog: SQLCODE -1273 x")[0], "LOG_NOT_AVAILABLE")
        self.assertIsNone(classify_source_error("connection reset"))
        self.assertTrue(issubclass(TxnTooLarge, SourceError))

    def test_fake_connector_meets_idle_tick_contract(self) -> None:
        self.assertLess(check_idle_ticks(FakeConnector({"rate_tps": 5, "idle_tick_s": 0.05}), None, 1.0, 1.0), 1.0)
        class Slow(FakeConnector):
            def stream(self, from_pos: Any) -> Any:
                import time
                while True:
                    time.sleep(0.3)
                    yield None
        with self.assertRaises(AssertionError):
            check_idle_ticks(Slow({}), None, 0.7, 0.2)



class PostConnectTests(Base):
    def test_db2_post_connect_check_is_wired(self) -> None:
        """LNX-08: the Db2 capture asks the connected server for its real host (alias -> node directory) and the
        daemon's deny list refuses it: connection closed, nothing captured; unidentifiable server = refused."""
        from types import SimpleNamespace

        from aleltd.sources.db2 import Db2Capture

        closed: List[bool] = []

        class FakeDb:
            def __init__(self, host: Optional[str]) -> None:
                self.host = host

            def server_info(self) -> Dict[str, Any]:
                return {"host": self.host} if self.host else {"host": None, "error": "SQL0551N"}

            def close(self) -> None:
                closed.append(True)

        ag = self.agent(start=False, deny_hosts=["*.prod.corp.local"])
        for host, refused in (("db2p.prod.corp.local", True), (None, True), ("db2t.test.corp.local", False)):
            cap = Db2Capture.__new__(Db2Capture)
            cap.cfg = SimpleNamespace(source=SimpleNamespace(user_env=None, dll_dirs=[], database="ALIAS1",
                                                             password=lambda: None))
            cap.db = None
            cap.lib = SimpleNamespace(Db2=lambda *a, h=host: FakeDb(h), Db2Error=RuntimeError)
            cap.on_connect = ag._on_source_connect          # what startup() wires (daemon.py)
            closed.clear()
            if refused:
                with self.assertRaises(guard.GuardError):
                    cap._conn()
                self.assertEqual((cap.db, closed), (None, [True]))
            else:
                self.assertIsNotNone(cap._conn())
                self.assertEqual(closed, [])


if __name__ == "__main__":
    unittest.main()


class StreamReadTimeoutTests(Base):
    def test_blocked_stream_never_freezes_the_loop_and_reconnects(self) -> None:
        """LNX-31: a connector whose stream() blocks (no idle tick) is read on a reader thread: step() returns
        within its budget with the stall measured (-> SOURCE_STALLED), and after stall_reconnect_s the source is
        closed and an error raised (the daemon reconnects from the chain); the next stream works again."""
        import time as _t

        from aleltd.sources.remote import StreamCapture

        release = threading.Event()
        calls = {"stream": 0, "closed": 0}

        class Blocking(FakeConnector):
            def stream(self, from_pos: Any) -> Any:
                calls["stream"] += 1
                if calls["stream"] == 1:
                    release.wait(30)              # stuck network read, never yields
                    return
                yield None

            def close(self) -> None:
                calls["closed"] += 1
                release.set()                     # closing the connection unblocks the read (like a socket close)

        ag = self.agent(start=False)
        cap = StreamCapture(ag.cfg, lambda: Blocking({"idle_tick_s": 0.05}, "fake1"), native=True)
        cap.stall_reconnect_s = 2.5
        t0 = _t.monotonic()
        st = cap.step(None, {"batch_kb": 64})
        self.assertLess(_t.monotonic() - t0, 1.5)             # the daemon loop got control back
        self.assertEqual(st.batches, [])
        self.assertGreater(cap.last_stall_s, 0.5)
        with self.assertRaises(SourceError):
            for _ in range(6):                                # ~1 s per step until the reconnect threshold
                cap.step(None, {"batch_kb": 64})
        self.assertGreaterEqual(calls["closed"], 1)
        cap.step(None, {"batch_kb": 64})                      # new connection + stream: idle tick, no stall
        self.assertLess(cap.last_stall_s, 0.5)
        cap.close()


class LongPollTests(Base):
    def test_long_poll_only_when_idle(self) -> None:
        """LNX-18: an idle agent long-polls commands (wait = min(command_wait_s, heartbeat margin, timeout - 5));
        an agent with batches to ship polls without waiting."""
        ag = self.agent(StubHub(resume=pos(999)))
        ag.cfg.hub.request_timeout_ms, ag.cfg.hub.heartbeat_s, ag.cfg.hub.command_wait_s = 30000, 20, 10

        def last_wait() -> int:
            path = [p for _, p, _ in self.hub.calls if p.startswith("/agent/v1/commands?")][-1]
            return int(path.rsplit("wait=", 1)[1])

        ag.capturing = False
        ag.poll_commands()
        self.assertEqual(last_wait(), 10)
        assert ag.spool is not None
        ag.spool.put_batch(*batch(999, 1999))
        ag.poll_commands()
        self.assertEqual(last_wait(), 0)
