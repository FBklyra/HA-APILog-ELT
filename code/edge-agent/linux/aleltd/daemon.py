"""Main loop of the Linux agent (one thread does all source and hub I/O; helper threads only watch).

Signals only set events: SIGTERM / SIGINT -> stop, SIGHUP -> reload config. Every wait is `Event.wait()`, so a
signal ends a sleep at once. On stop (or a fatal error) `save_checkpoint()` runs before exit; a deadline thread
forces the exit (after writing state.json) if the graceful path overruns `shutdown.deadline_s`.

Exit codes: 0 = graceful stop / hub SHUTDOWN, 1 = runtime failure (systemd restarts) or a stop that had to be
forced at the deadline (hung source), 78 = configuration or safety-guard refusal (EX_CONFIG; the unit has
RestartPreventExitStatus=78): invalid config, source plugin/settings refused at startup, deny list, standalone
agent on a hub that runs the connection active/passive (423). A protocol window refusal (426) is retried at the
slowest pace with an event (recovers by itself once the hub / agent is upgraded).

Never fatal at runtime: a corrupt / sha-mismatched spool entry (quarantined, re-read from its position), a hub
422 / 413 (same), a full spool volume (backpressure, like the size cap), a command that fails (acked ok:false).
"""
from __future__ import annotations

import json
import logging
import os
import queue
import random
import re
import socket
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote

from . import VERSION, config as cfgmod, guard, logging_setup
from .checkpoint import Deadline, StateStore, save_checkpoint
from .config import Config, ConfigError, table_allowed
from .credential import CAPABILITY as CREDENTIAL_CAPABILITY, CredentialStore
from .hub import HubClient, HubError, retry_after_s
from .lease import ACTIVE, LOST, TAKEOVER, Lease
from .sdnotify import Notifier
from .sources import CaptureSource, SourceError, make_source
from .sources.base import TxnTooLarge, classify_source_error
from .spool import CorruptEntry, Spool, SpoolFull, no_space, verify_body

log = logging.getLogger("aleltd")

EXIT_OK, EXIT_FAIL, EXIT_CONFIG = 0, 1, 78
COMMAND_POLL_S = 2.0
PLUGIN_COMMANDS = ("START_CAPTURE", "SNAPSHOT_CATALOG", "SNAPSHOT_TABLE", "PROBE", "VERIFY_TABLE")
HEAVY_COMMANDS = ("SNAPSHOT_TABLE", "VERIFY_TABLE", "PROBE")    # command thread (docs/AGENT-COMMANDS.md)
EVENT_THROTTLE_S = 60.0
STALL_EVENT_S = 30.0                # a source call blocking longer than this violates SPI S1 (LNX-31)
MAX_422_PER_POS = 2                 # same from_pos refused twice with 422 -> stop re-reading, hold + event
_CMD_ID = re.compile(r"^[0-9A-Fa-f-]{8,64}$")

try:                                # monotonic that keeps counting across host suspend (LNX-10)
    from alelt_common.lease_client import boot_clock as _default_clock
except ImportError:                 # pragma: no cover
    _default_clock = time.monotonic


class FatalConfig(RuntimeError):
    pass


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _jitter(seconds: float, frac: float = 0.2) -> float:
    return max(0.0, seconds * (1.0 + random.uniform(-frac, frac)))


def rss_mb() -> float:
    try:
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE") / 1048576
    except (OSError, ValueError, AttributeError, IndexError):
        return 0.0


def cgroup_memory_bounded() -> Optional[bool]:
    """True/False under cgroup v2 (memory.max set or 'max'); None when not determinable (not Linux)."""
    try:
        with open("/proc/self/cgroup") as f:
            line = next((ln for ln in f.read().splitlines() if ln.startswith("0::")), "")
        if not line:
            return None
        with open(os.path.join("/sys/fs/cgroup", line[3:].lstrip("/"), "memory.max")) as f:
            return f.read().strip() != "max"
    except OSError:
        return None


class HostCpu:
    """Host CPU % from /proc/stat between two samples (Linux); None elsewhere."""

    def __init__(self) -> None:
        self.prev = self._read()

    @staticmethod
    def _read() -> Optional[List[int]]:
        try:
            with open("/proc/stat") as f:
                v = [int(x) for x in f.readline().split()[1:]]
            return v if len(v) >= 4 else None
        except (OSError, ValueError):
            return None

    def sample(self) -> Optional[float]:
        cur = self._read()
        prev, self.prev = self.prev, cur
        if not cur or not prev:
            return None
        idle = (cur[3] + (cur[4] if len(cur) > 4 else 0)) - (prev[3] + (prev[4] if len(prev) > 4 else 0))
        total = sum(cur) - sum(prev)
        return 100.0 * (total - idle) / total if total > 0 else None


class Agent:
    def __init__(self, cfg: Config, notifier: Optional[Notifier] = None, hub: Optional[HubClient] = None,
                 source_factory: Optional[Callable[[Config], CaptureSource]] = None,
                 clock: Callable[[], float] = _default_clock) -> None:
        self.cfg = cfg
        self.notifier = notifier or Notifier()
        self.clock = clock
        self.stop_event = threading.Event()
        self.reload_event = threading.Event()
        self.wake = threading.Event()
        self.done_event = threading.Event()
        self.lock = threading.RLock()
        self.stop_reason = ""
        self.stop_at: Optional[float] = None
        self.exit_code = EXIT_OK
        self._hub = hub
        self._source_factory = source_factory or make_source
        self.hub: Optional[HubClient] = None
        self.source: Optional[CaptureSource] = None
        self.spool: Optional[Spool] = None
        self.store = StateStore(cfg.state_file)
        self.cred = CredentialStore(cfg.credential_file)
        self.lease = Lease(cfg.ha.mode == "active_passive", cfg.ha.lease_ttl_s, cfg.ha.renew_s, cfg.agent_id,
                           cfg.connection_id, clock)
        self.limits = dict(cfg.limits)
        # positions (text) - the chain is the from_pos of the next batch (= previous batch next_pos, X-25)
        self.chain: Optional[str] = None
        self.chain_ready = False
        self.durable: Optional[str] = None
        self.last_read: Optional[str] = None
        self.capturing = cfg.capture.autostart
        self.reposition_cmd: Optional[str] = None
        self.reposition_pos: Optional[str] = None
        self.saved: Dict[str, Any] = {}
        # runtime
        self.registered = False
        self.hub_resume: Optional[str] = None
        self.redecide = False                 # chain must be re-decided (hub position) after a quarantine
        self.paused = False
        self.gov_paused = False
        self.cmdq: "queue.Queue[tuple[str, str, Dict[str, Any]]]" = queue.Queue(maxsize=64)
        self.cmd_seen: Dict[str, float] = {}
        self.cmd_thread: Optional[threading.Thread] = None
        self.cmd_source: Optional[CaptureSource] = None
        self.gov_high = 0
        self.state_name = "STARTING"
        self.hold_reason: Optional[str] = None  # shipping held until an operator acts (422 twice, ...)
        self.sent = self.errors = 0
        self.alive = clock()
        self.next_register = self.next_hb = self.next_cmd = self.next_ship = self.next_gov = 0.0
        self.next_source = 0.0
        self.next_rotate = 0.0
        self.ship_fail = self.reg_fail = self.src_fail = 0
        self.catalog_due = True
        self.refused_422: Dict[str, int] = {}
        self._ev_last: Dict[str, float] = {}
        self.host_cpu = HostCpu()
        self._cpu0 = (time.process_time(), clock())
        self._cpu_pct = 0.0

    # ================================================================== control (called from signal handlers)
    def request_stop(self, reason: str) -> None:
        if not self.stop_event.is_set():
            self.stop_reason = reason
            self.stop_at = self.clock()
        self.stop_event.set()
        self.wake.set()

    def request_reload(self) -> None:
        self.reload_event.set()
        self.wake.set()

    def _sleep(self, seconds: float) -> None:
        if seconds > 0 and not self.stop_event.is_set():
            self.wake.wait(seconds)
        self.wake.clear()

    def _touch(self) -> None:
        """Liveness for the watchdog thread: refreshed before every bounded blocking call (LNX-04)."""
        self.alive = self.clock()

    def _fatal_config(self, why: str) -> None:
        log.error("%s: stopping (exit 78, systemd will not restart)", why)
        self.exit_code = EXIT_CONFIG
        self.request_stop("config")

    # ================================================================== run
    def run(self) -> int:
        threading.Thread(target=self._deadline_watch, name="alelt-deadline", daemon=True).start()
        if self.notifier.watchdog_s:
            threading.Thread(target=self._watchdog, name="alelt-watchdog", daemon=True).start()
        code = EXIT_OK
        try:
            self.startup()
            self.cmd_thread = threading.Thread(target=self.command_loop, name="alelt-commands", daemon=True)
            self.cmd_thread.start()
            self.notifier.ready(f"capturing {self.cfg.connection_id}" if self.capturing else "started")
            log.info("agent started", extra={"correlation_id": self.cfg.agent_id})
            self.loop()
            code = self.exit_code
        except (FatalConfig, ConfigError, guard.GuardError) as e:
            log.error("refusing to run: %s", e)
            self.request_stop("config")
            code = EXIT_CONFIG
        except BaseException as e:  # noqa: BLE001 - every fatal path checkpoints before exit
            log.critical("fatal error: %s: %s", type(e).__name__, e, exc_info=not isinstance(e, KeyboardInterrupt))
            self.request_stop("fatal")
            code = EXIT_FAIL if not isinstance(e, KeyboardInterrupt) else EXIT_OK
        finally:
            code = self.shutdown(code)
        return code

    def shutdown(self, code: int) -> int:
        self.state_name = "STOPPING"
        reason = self.stop_reason or "exit"
        budget = max(1.0, self.cfg.shutdown.deadline_s - 2.0)
        started = self.stop_at if self.stop_at is not None else self.clock()
        deadline = Deadline(max(0.5, budget - (self.clock() - started)), self.clock)
        self.notifier.stopping(f"stopping ({reason}), checkpoint")
        log.info("graceful stop: saving checkpoint", extra={"correlation_id": reason})
        safe = True
        if self.spool is not None:
            safe = save_checkpoint(self, reason, deadline, self.cfg.shutdown.drain_s,
                                   network=self.hub is not None and code != EXIT_CONFIG)
            if self.hub is not None and not deadline.expired():
                self.heartbeat(timeout=min(3.0, deadline.remaining()))
        if self.source is not None:
            self.source.close()
        log.info("stopped", extra={"position": self.chain, "correlation_id":
                                   f"reason={reason} durable={self.durable} spool={len(self.spool.entries()) if self.spool else 0}"})
        self.done_event.set()
        self.notifier.close()
        if not safe and code == EXIT_OK:
            return EXIT_FAIL
        return code

    def _deadline_watch(self) -> None:
        self.stop_event.wait()
        started = self.stop_at if self.stop_at is not None else self.clock()
        left = self.cfg.shutdown.deadline_s - (self.clock() - started)
        if self.done_event.wait(max(0.0, left)):
            return
        log.error("graceful stop exceeded shutdown.deadline_s=%ss (source or hub call hung): forcing exit 1",
                  self.cfg.shutdown.deadline_s)
        if self.lock.acquire(timeout=2.0):
            try:
                self.write_state("deadline")
            except Exception:  # noqa: BLE001
                log.exception("state.json write failed at deadline")
            finally:
                self.lock.release()
        self.event("SHUTDOWN_FORCED", {"deadline_s": self.cfg.shutdown.deadline_s, "state": self.state_name},
                   timeout=2.0)
        for h in logging.getLogger().handlers:
            try:
                h.flush()
            except Exception:  # noqa: BLE001
                pass
        os._exit(EXIT_FAIL)             # LNX-30: a forced stop is never reported as a clean one

    def _watchdog(self) -> None:
        iv = self.notifier.watchdog_s or 30.0
        while not self.done_event.wait(iv / 2):
            if self.clock() - self.alive < iv or self.stop_event.is_set():
                self.notifier.watchdog()
            else:
                log.error("main loop stalled for %.0fs: withholding watchdog ping", self.clock() - self.alive)

    # ================================================================== startup
    def startup(self) -> None:
        cfg = self.cfg
        if cfg.require_job_limits and sys.platform.startswith("linux"):
            if cgroup_memory_bounded() is False:
                raise FatalConfig("require_job_limits=true but the agent's cgroup has no memory limit "
                                  "(run under alelt.service with MemoryMax=, or set require_job_limits=false)")
        try:
            self.source = self._source_factory(cfg)
        except SourceError as e:        # LNX-15: plugin missing / settings refused = configuration, not a crash
            raise FatalConfig(f"source: {e}") from None
        guard.check(cfg.environment, cfg.allow_prod, cfg.deny_hosts, self.source.targets())
        self.source.on_connect = self._on_source_connect
        self.spool = Spool(cfg.spool_dir)
        for name, why in self.spool.quarantined:
            log.error("spool entry %s quarantined at startup: %s", name, why)
        self.saved = self.store.load()
        self.durable = self.saved.get("durable_pos")
        self.last_read = self.saved.get("last_read_pos")
        if self.saved.get("capturing"):
            self.capturing = True
        if self.saved.get("reposition_command_id"):     # LNX-03: kept until a batch carrying it is ACKed
            self.reposition_cmd = self.saved.get("reposition_command_id")
            self.reposition_pos = self.saved.get("reposition_pos")
        has_cred = cfg.hub.mode in ("token", "dev") and self.cred.load()
        try:
            self.hub = self._hub or HubClient(cfg.hub, cfg.agent_id, credential_present=bool(has_cred))
        except (ValueError, OSError) as e:
            raise FatalConfig(str(e)) from None
        if has_cred and self.cred.token:
            self.hub.set_token(self.cred.token)
            log.info("using the stored per-agent credential", extra={"correlation_id": str(self.cred.describe())})
        self.lease.hub = self.hub
        log.info("aleltd %s: connection=%s engine=%s spool=%s (%d entries) ha=%s", VERSION, cfg.connection_id,
                 cfg.source.engine, cfg.spool_dir, len(self.spool.entries()), cfg.ha.mode)
        self.try_register()
        if not self.lease.enabled:
            self.decide_position()

    def _on_source_connect(self, server: Dict[str, Any]) -> None:
        guard.check_connected(self.cfg.deny_hosts, server)

    def try_register(self) -> bool:
        assert self.hub is not None
        cfg = self.cfg
        self._touch()
        try:
            r = self.hub.post_json("/agent/v1/register", {
                "agent_id": cfg.agent_id, "connection_id": cfg.connection_id, "engine": cfg.source.engine,
                "version": f"{VERSION}-linux", "hostname": socket.gethostname(),
                "database": cfg.source.database or str(cfg.source.settings.get("database") or cfg.source.engine),
                "capabilities": ["capture", "catalog", "commands:long-poll", "ha:lease", "platform:linux",
                                 CREDENTIAL_CAPABILITY]},
                timeout=self._net_timeout())
            if not isinstance(r, dict):
                raise HubError(502, f"register answer is not an object: {str(r)[:100]}")
        except HubError as e:
            if e.status == 426:                  # XC-16: same as the C agent - loud, slowest pace, recovers by itself
                log.error("hub refused the agent protocol version (426): %s - upgrade the hub or the agent; "
                          "retrying slowly", e.body[:200])
                self.reg_fail = max(self.reg_fail, 12)
                return self._register_failed(e)
            if e.status == 401 and self._credential_rejected("register"):
                self.next_register = self.clock()            # retry at once with the enrollment token
                return False
            return self._register_failed(e)
        except OSError as e:
            return self._register_failed(e)
        self.registered, self.reg_fail = True, 0
        rf = r.get("resume_from")
        self.hub_resume = rf.get("text") if isinstance(rf, dict) and isinstance(rf.get("text"), str) else None
        if isinstance(r.get("limits"), dict):
            self.limits = self.cfg.clamp(r["limits"])
        if r.get("credential"):
            self._store_credential(r["credential"], r.get("credential_expires_at"), "issued at registration")
        log.info("registered", extra={"position": self.hub_resume})
        return True

    def _register_failed(self, e: Exception) -> bool:
        self.reg_fail += 1
        self.next_register = self.clock() + self._backoff(self.reg_fail)
        log.warning("register failed (%s); retrying in %.0fs", e, self.next_register - self.clock())
        return False

    # ------------------------------------------------------------------ REQ-26 per-agent credential
    def _store_credential(self, token: Any, expires_at: Any, why: str) -> None:
        assert self.hub is not None
        try:
            self.cred.save(str(token), expires_at)      # persisted BEFORE the old token is dropped
        except (ValueError, OSError) as e:
            log.error("per-agent credential could not be stored (%s): keeping the current token", type(e).__name__)
            return
        self.hub.set_token(self.cred.token)
        log.info("per-agent credential %s and stored", why, extra={"correlation_id": str(self.cred.describe())})

    def _credential_rejected(self, where: str) -> bool:
        """401 while using the stored credential: drop it, fall back to the enrollment token (env) once, with an
        event and a clear log line. True when a fallback token is available (caller retries)."""
        assert self.hub is not None
        if not self.cred.token or self.hub.token != self.cred.token:
            return False
        log.error("hub rejected the stored per-agent credential (%s: revoked, expired or rotated away); falling "
                  "back to %s - re-enroll the agent if this persists", where,
                  f"the token in {self.cfg.hub.token_env}" if self.hub.env_token else "no token (none configured)")
        self.cred.discard()
        self.hub.set_token(self.hub.env_token)
        self.registered = False
        if self.hub.env_token:
            self.event("AGENT_CREDENTIAL_REJECTED", {"where": where}, force=True)
        return bool(self.hub.env_token)

    def maybe_rotate_credential(self) -> None:
        assert self.hub is not None
        now = self.clock()
        if now < self.next_rotate or not self.cred.token or self.hub.token != self.cred.token:
            return
        self.next_rotate = now + 3600
        if not self.cred.rotate_due():
            return
        self._touch()
        try:
            r = self.hub.post_json("/agent/v1/credential/rotate", {"agent_id": self.cfg.agent_id},
                                   timeout=self._net_timeout())
        except HubError as e:
            if e.status == 401:
                self._credential_rejected("rotate")
            else:
                self.next_rotate = now + 300
                log.warning("credential rotation failed (HTTP %s); retrying later", e.status)
            return
        except OSError as e:
            self.next_rotate = now + 300
            log.warning("credential rotation failed (%s); retrying later", e)
            return
        if isinstance(r, dict) and r.get("credential"):
            self._store_credential(r["credential"], r.get("credential_expires_at"), "rotated")

    # ------------------------------------------------------------------ position
    def decide_position(self, resume_from: Optional[str] = None, takeover: bool = False) -> None:
        """Resume point: newest spooled batch next_pos > operator reposition (state.json) > hub durable position >
        state.json > capture.start. Takeover: hub position > own state.json (event) > capture.start."""
        assert self.spool is not None and self.source is not None
        spooled = None if takeover else self.spool.last_next()
        hub = resume_from if (resume_from is not None or takeover) else self.hub_resume
        state = self.saved.get("next_pos")
        if spooled:
            pos, why = spooled, "spool"
        elif not takeover and self.reposition_cmd and self.reposition_pos:
            pos, why = self.reposition_pos, f"operator reposition {self.reposition_cmd} (state.json)"
            if hub and hub != pos:
                log.warning("hub durable position %s ignored: START_CAPTURE %s repositioned to %s (not yet "
                            "acknowledged by a batch)", hub, self.reposition_cmd, pos)
        elif hub:
            if state and state != hub and not takeover:
                log.warning("state.json next_pos %s differs from the hub durable position %s; using the hub",
                            state, hub)
            pos, why = hub, "hub"
        elif takeover and state:        # LNX-20: hub has no position for the connection -> our own last position
            pos, why = state, "state.json (takeover without hub resume_from)"
            log.warning("takeover without a hub resume position: resuming at this agent's last position %s", state)
            self.event("TAKEOVER_WITHOUT_RESUME", {"position": state, "epoch": self.lease.epoch}, force=True)
        elif state:
            pos, why = state, "state.json"
        elif not self.registered and not takeover:
            return                                 # wait for the hub before choosing a first position
        else:
            if not self.capturing:
                return                             # first start waits for START_CAPTURE (capture.autostart=false)
            if takeover:
                self.event("TAKEOVER_WITHOUT_RESUME", {"position": None, "start": self.cfg.capture.start,
                                                       "epoch": self.lease.epoch}, force=True)
            try:
                pos, why = self.source.initial_position(self.cfg.capture.start), f"capture.start={self.cfg.capture.start}"
            except SourceError as e:
                self._source_failed(e)
                return
        with self.lock:
            self.chain, self.chain_ready = pos, True
            self.redecide = False
            self.source.reset(pos)
            self.catalog_due = True
            self._save_state("position")
        log.info("capture position decided (%s)", why, extra={"position": pos})

    # ================================================================== main loop
    def loop(self) -> None:
        while not self.stop_event.is_set():
            self._touch()
            now = self.clock()
            if self.reload_event.is_set():
                self.reload()
            if not self.registered and now >= self.next_register and self.try_register() and not self.lease.enabled \
                    and not self.chain_ready:
                self.decide_position()
            if self.registered:
                self.tick_lease(now)
                if self.redecide and not self.chain_ready and (not self.lease.enabled or self.lease.active):
                    self.decide_position()
                if self.clock() >= self.next_hb:
                    self.heartbeat()
                if self.clock() >= self.next_cmd:
                    self.poll_commands()
                self.maybe_rotate_credential()
            self.tick_governor(now)
            if self.stop_event.is_set():
                break
            if self.registered and self.clock() >= self.next_ship and self.may_ship():
                self.ship_spool()
            wait = self.capture_step()
            nxt = min(self.next_hb, self.next_cmd, self.lease.next_at if self.lease.enabled else 1e18,
                      self.next_ship if self.spool and self.spool.entries() and self.may_ship() else 1e18,
                      self.next_register if not self.registered else 1e18)
            self._sleep(min(wait, max(0.0, nxt - self.clock())) if wait > 0 else 0)

    def may_ship(self) -> bool:
        """LNX-12: a standby never ships (its spool is discarded at takeover anyway); a hold waits for an operator."""
        return (not self.lease.enabled or self.lease.active) and self.hold_reason is None

    def reload(self) -> None:
        self.reload_event.clear()
        self.notifier.reloading()
        try:
            new = cfgmod.load(self.cfg.path) if self.cfg.path else self.cfg
            assert self.source is not None
            guard.check(new.environment, new.allow_prod, new.deny_hosts, self.source.targets())
        except guard.GuardError as e:
            log.error("reload: source now refused by the safety guard: %s", e)
            self.exit_code = EXIT_CONFIG
            self.request_stop("guard")
            return
        except (ConfigError, OSError) as e:
            log.error("reload failed, keeping the running configuration: %s", e)
            self.notifier.ready()
            return
        restart = cfgmod.diff_restart_required(self.cfg, new)
        if restart:
            log.warning("reload: %s changed - restart the service to apply", ", ".join(restart))
        self.cfg = cfgmod.merged(self.cfg, new)
        self.limits = self.cfg.clamp(self.limits)
        logging_setup.set_level(self.cfg.log_level)
        log.info("configuration reloaded", extra={"correlation_id": ",".join(cfgmod.RELOADABLE)})
        self.notifier.ready("reloaded")

    # ------------------------------------------------------------------ HA
    def tick_lease(self, now: float) -> None:
        if not self.lease.enabled:
            return
        was_active = self.lease.active
        if not self.lease.due(now) and not self.lease.must_fence(now):
            return
        self._touch()
        ev, resume = self.lease.tick(min(self._net_timeout(), float(self.lease.renew_s)))
        if ev == TAKEOVER:
            assert self.spool is not None
            n = self.spool.purge()
            log.warning("lease taken over (epoch %s): %d stale spool entries discarded", self.lease.epoch, n)
            with self.lock:
                self.reposition_cmd = self.reposition_pos = None
            self.decide_position(resume, takeover=True)
            if getattr(self.lease, "capturing", None) is False:   # AHS-24: the operator paused this connection
                self.paused = True
                log.warning("takeover of a connection paused by the operator: staying paused until RESUME")
                self.event("PAUSED_BY_HUB_GRANT", {"epoch": self.lease.epoch}, force=True)
        elif ev == ACTIVE:
            log.info("lease granted: ACTIVE", extra={"correlation_id": f"epoch={self.lease.epoch}"})
            self.decide_position(resume)
        elif ev == LOST or (was_active and not self.lease.active):
            self._lease_lost("lease lost")

    def _lease_lost(self, why: str) -> None:
        log.warning("%s: back to STANDBY, reading stopped (spool kept)", why)
        if self.lease.active:
            self.lease.on_lost(why)
        with self.lock:
            self.chain_ready = False
        if self.source is not None:
            self.source.close()

    def can_capture(self) -> bool:
        return (self.capturing and self.chain_ready and not self.paused and not self.gov_paused
                and (not self.lease.enabled or (self.lease.active and not self.lease.must_fence(self.clock()))))

    # ------------------------------------------------------------------ capture
    def capture_step(self) -> float:
        assert self.spool is not None and self.source is not None
        if self.lease.enabled and not self.lease.active:
            self.state_name = "STANDBY"
            return 1.0
        if self.paused:
            self.state_name = "PAUSED"
            return 1.0
        if self.gov_paused:
            self.state_name = "PAUSED_BY_GOVERNOR"
            return 1.0
        if not self.capturing or not self.chain_ready:
            self.state_name = "IDLE" if self.hold_reason is None else "ERROR"
            return 1.0
        if self.spool.full(self.cfg.spool_max_mb << 20):
            return self._spool_full("disk reserve" if self.spool.disk_low() else "spool.max_mb")
        if self.clock() < self.next_source:
            return self.next_source - self.clock()
        if not self.can_capture():
            return 1.0
        self._touch()
        try:
            if self.catalog_due:
                self.spool.put_catalog(self.source.catalog(self.chain))
                self.catalog_due = False
            t0 = time.process_time()
            st = self.source.step(self.chain, self.limits)
        except SpoolFull as e:
            return self._spool_full(str(e))
        except SourceError as e:
            return self._source_failed(e)
        self._check_stall()
        self.src_fail = 0
        self.state_name = "CAPTURING" if self.hold_reason is None else "ERROR"
        if st.batches or st.catalog:
            with self.lock:
                if not self.can_capture():              # lease lost while reading: drop the read, not the data
                    self.source.reset(self.chain)
                    return 0.0
                chain0 = self.chain
                try:
                    if st.catalog:
                        self.spool.put_catalog(st.catalog)
                    for hdr, body in st.batches:
                        self._spool_batch(hdr, body)
                except (SpoolFull, OSError) as e:       # LNX-07: disk full = backpressure; re-read the rest later
                    self.source.reset(self.chain)
                    if self.chain != chain0:
                        self._save_state("batch")
                    if isinstance(e, SpoolFull) or no_space(e):
                        return self._spool_full(str(e))
                    raise
                self._save_state("batch")
        self._governor_sleep(time.process_time() - t0)
        return st.wait_s

    def _spool_full(self, why: str) -> float:
        if self.state_name != "SPOOL_FULL":
            log.warning("spool full (%s): reading paused, nothing dropped", why)
            assert self.spool is not None
            self.event("SPOOL_FULL", {"reason": why, "spool_bytes": self.spool.bytes(),
                                      "spool_max_mb": self.cfg.spool_max_mb})
        self.state_name = "SPOOL_FULL"                  # backpressure: stop reading, never drop
        return 1.0

    def _check_stall(self) -> None:
        assert self.source is not None
        s = float(getattr(self.source, "last_stall_s", 0.0) or 0.0)
        if s >= STALL_EVENT_S:
            log.error("source call blocked %.0fs without an idle tick (SPI S1 contract: <= ~1 s): heartbeats, the "
                      "lease and the watchdog stall meanwhile", s)
            self.event("SOURCE_STALLED", {"blocked_s": round(s, 1), "engine": self.cfg.source.engine})

    def _spool_batch(self, hdr: Dict[str, Any], body: bytes) -> None:
        assert self.spool is not None
        self.spool.put_batch(hdr, body)
        self.chain = (hdr.get("next_pos") or hdr["to_pos"])["text"]
        self.last_read = hdr["to_pos"]["text"]

    def _source_failed(self, e: Exception) -> float:
        self.errors += 1
        self.src_fail += 1
        self.state_name = "ERROR"
        msg = str(e)
        if isinstance(e, TxnTooLarge):                  # LNX-14: visible, capped, slow - not a hot loop
            wait = self._backoff(self.src_fail + 4)
            self.event("TXN_TOO_LARGE", {"message": msg[:500], "position": self.chain,
                                         "max_txn_mb": self.cfg.capture.max_txn_mb})
        else:
            wait = self._backoff(self.src_fail, cap=60)
            cls = classify_source_error(msg)            # REQ-17 / LNX-06
            if cls:
                self.event(cls[0], {"message": msg[:500], "sqlcode": cls[1]})
        log.error("source error (%s); reconnecting from the chain position in %.0fs", msg, wait,
                  extra={"position": self.chain})
        if self.source is not None:
            self.source.close()
            self.source.reset(self.chain)
        self.catalog_due = True
        self.next_source = self.clock() + wait
        return wait

    def _governor_sleep(self, busy_s: float) -> None:
        pct = max(1, min(100, int(self.limits.get("cpu_pct", 100))))
        if pct < 100 and busy_s > 0:
            left = busy_s * (100 - pct) / pct
            wd = self.notifier.watchdog_s
            chunk = min(5.0, wd / 4) if wd else 5.0          # a chunk never outlives a watchdog check (LNX-04)
            while left > 0 and not self.stop_event.is_set():   # chunks: the watchdog sees a live loop (LNX-04)
                step = min(left, chunk)
                self._touch()
                self.stop_event.wait(step)
                left -= step

    def tick_governor(self, now: float) -> None:
        g = self.cfg.governor
        if not g.enabled or now < self.next_gov:
            return
        self.next_gov = now + g.interval_s
        cpu = self.host_cpu.sample()
        if cpu is None:
            return
        if not self.gov_paused:
            self.gov_high = self.gov_high + 1 if cpu >= g.host_cpu_pause_pct else 0
            if self.gov_high >= g.sustain_samples:
                self.gov_paused = True
                log.warning("host CPU %.0f%% >= %d%%: capture paused by the governor", cpu, g.host_cpu_pause_pct)
                self.event("PAUSED_BY_GOVERNOR", {"host_cpu_pct": round(cpu, 1)}, force=True)
        elif cpu <= g.host_cpu_resume_pct:
            self.gov_paused, self.gov_high = False, 0
            log.info("host CPU %.0f%%: governor resumed capture", cpu)
            self.event("GOVERNOR_RESUMED", {"host_cpu_pct": round(cpu, 1)}, force=True)
        mb = rss_mb()
        if mb > self.limits.get("mem_mb", 0) > 0:
            log.warning("resident memory %.0f MB above limits.mem_mb=%d", mb, self.limits["mem_mb"])

    # ------------------------------------------------------------------ hub: ship spool
    def ship_spool(self, until: Optional[Deadline] = None) -> int:
        """Send spool entries oldest first. Returns batches acked. Stops at the first refusal (kept, retried)."""
        assert self.spool is not None and self.hub is not None and self.source is not None
        acked = 0
        for p in self.spool.entries():
            if p not in self.spool.entries():           # quarantined / released meanwhile
                continue
            if until is None and self.stop_event.is_set():
                break
            if until is not None and until.expired():
                break
            if not self.may_ship():
                break
            timeout = self._net_timeout() if until is None else max(0.5, min(self._net_timeout(), until.remaining()))
            self._touch()
            hdr = None
            try:
                hdr, body = self.spool.get(p)
                if hdr.get("kind") != "catalog":
                    verify_body(hdr, body)              # LNX-02: never ship a body that changed on disk
            except FileNotFoundError:
                self.spool.delete(p)
                continue
            except CorruptEntry as e:
                self._reread(p, hdr, f"corrupt spool entry: {e}")
                return acked
            ack: Any = None
            try:
                if hdr.get("kind") == "catalog":
                    self.hub.call("POST", "/agent/v1/catalog", body, {"Content-Type": "application/json"}, timeout)
                    self.spool.delete(p)
                    continue
                headers = {"X-ALELT-Batch": json.dumps(hdr, separators=(",", ":")), "Content-Type": "application/octet-stream"}
                if self.reposition_cmd and hdr["from_pos"]["text"] == self.reposition_pos:
                    headers["X-ALELT-Reposition"] = self.reposition_cmd
                headers.update(self.lease.headers())
                ack = self.hub.call("POST", "/agent/v1/blocks", body, headers, timeout)
                durable = ack["durable_pos"]["text"] if isinstance(ack, dict) else None
                if not isinstance(durable, str) or not durable:
                    raise HubError(502, f"ACK without durable_pos: {str(ack)[:200]}")
            except HubError as e:
                self._ship_refused(e, hdr, p)
                return acked
            except (KeyError, TypeError):
                self._ship_backoff(f"malformed ACK from the hub: {str(ack)[:200]}")
                return acked
            except OSError as e:
                self._ship_backoff(f"hub unreachable ({e}); spool kept")
                return acked
            with self.lock:
                self.durable = durable
                if headers.get("X-ALELT-Reposition"):
                    self.reposition_cmd = self.reposition_pos = None
                self.spool.release(durable, self.source.compare)
                self._save_state("ack")
            self.refused_422.pop(hdr["from_pos"]["text"], None)
            self.source.acked(durable)
            self.sent += 1
            acked += 1
            self.ship_fail = 0
            log.info("batch durable at hub", extra={"batch_id": hdr.get("batch_id"), "position": durable,
                                                    "correlation_id": f"dup={ack.get('duplicate')}"})
            self._pace(len(body), until)
        return acked

    def _reread(self, p: Any, hdr: Optional[Dict[str, Any]], why: str) -> None:
        """Quarantine `p` and every entry after it, then capture again from `p`'s from_pos (or, when its header is
        unreadable, from the hub durable position). Nothing is lost: the source still has the log (LNX-01/02)."""
        assert self.spool is not None and self.source is not None
        frm0 = (hdr or {}).get("from_pos", {}).get("text") if hdr else None
        self.event("SPOOL_ENTRY_QUARANTINED", {"file": getattr(p, "name", str(p)), "reason": why[:500],
                                               "reread_from": frm0}, force=True)
        with self.lock:
            moved = self.spool.entries_from(p)
            for q in moved:
                self.spool.quarantine(q, why if q == p else f"after quarantined {p.name}")
            self.errors += 1
            frm = (hdr or {}).get("from_pos", {}).get("text") if hdr else None
            if frm and (not self.lease.enabled or self.lease.active):
                self.chain, self.chain_ready = frm, True
                self.source.reset(frm)
            else:
                self.chain_ready = False
                self.redecide = True
                self.registered = False                  # fresh hub resume_from at the next registration
                self.next_register = self.clock()
            self.catalog_due = True
            self._save_state("quarantine")
        log.error("%s: %d spool entr%s moved to %s; capture re-reads from %s", why, len(moved),
                  "y" if len(moved) == 1 else "ies", self.spool.qdir, frm or "the hub durable position",
                  extra={"batch_id": (hdr or {}).get("batch_id")})

    def _ship_refused(self, e: HubError, hdr: Dict[str, Any], p: Any) -> None:
        assert self.source is not None and self.spool is not None
        self.errors += 1
        bid = hdr.get("batch_id", "catalog")
        if e.status == 423:
            if not self.lease.enabled:                   # LNX-05: config mismatch, never a silent idle
                self.event("HA_MODE_MISMATCH", {"batch_id": bid}, force=True)
                self._fatal_config("hub runs this connection active/passive (423) but ha.mode=standalone: set "
                                   "ha.mode=active_passive")
                return
            self._lease_lost(f"batch {bid} fenced by the hub (423)")
            self._ship_backoff("fenced", slow=True)
        elif e.status in (413, 422) and hdr.get("kind") == "catalog":
            self.spool.quarantine(p, f"catalog refused by the hub ({e.status})")
            self.catalog_due = True                      # a fresh snapshot is taken before the next batch
            log.error("catalog snapshot refused by the hub (%s): quarantined, a new one is taken: %s", e.status,
                      e.body[:200])
            self.event("SPOOL_ENTRY_QUARANTINED", {"file": getattr(p, "name", str(p)), "status": e.status,
                                                   "kind": "catalog"}, force=True)
            self.next_ship = self.clock()
        elif e.status == 409:
            self._ship_conflict(e, hdr, p)
        elif e.status == 422 and hdr.get("kind") != "catalog":
            frm = hdr["from_pos"]["text"]
            n = self.refused_422[frm] = self.refused_422.get(frm, 0) + 1
            if n > MAX_422_PER_POS:                       # the re-read gives the same refusal: hold, operator acts
                self.hold_reason = f"batch at {frm} refused by the hub (422) {n} times: {e.body[:200]}"
                log.error("%s; shipping held (spool kept) until START_CAPTURE{discard_spool} or a restart",
                          self.hold_reason)
                self.event("BATCH_REJECTED", {"status": 422, "from_pos": frm, "detail": e.body[:500],
                                              "held": True}, force=True)
                return
            self._reread(p, hdr, f"hub refused batch {bid} (422): {e.body[:200]}")
        elif e.status == 413 and hdr.get("kind") != "catalog":
            kb = max(16, int(self.limits.get("batch_kb", 1024)) // 2)
            self.limits = self.cfg.clamp({**self.limits, "batch_kb": kb})
            self.event("BATCH_TOO_LARGE", {"batch_id": bid, "batch_kb": self.limits["batch_kb"]})
            self._reread(p, hdr, f"hub refused batch {bid} as too large (413): batch_kb now {self.limits['batch_kb']}")
        elif e.status == 426:                            # XC-16 (same as the C agent): event + slowest pace
            self.event("PROTOCOL_UPGRADE_REQUIRED", {"detail": e.body[:300]})
            self.ship_fail = max(self.ship_fail, 12)
            self._ship_backoff(f"hub refused the protocol version (426): {e.body[:200]}", slow=True)
        elif e.status in (429, 503):
            self._ship_backoff(f"hub backpressure ({e.status}); spool kept", retry_after=retry_after_s(e.headers))
        elif e.status == 401 and self._credential_rejected("blocks"):
            self.next_ship = self.clock()
        elif e.status in (401, 403, 404):
            log.error("hub refused the agent (%s): re-registering", e)
            self.registered = False
            self._ship_backoff("unauthorized")
        else:
            self._ship_backoff(f"batch refused: {e}", retry_after=retry_after_s(e.headers))

    def _ship_conflict(self, e: HubError, hdr: Dict[str, Any], p: Any) -> None:
        """409 gap/overlap (AHS-05 equivalent): a batch the hub already covers is deleted; otherwise the spool is
        blocked -> event + slow retry; START_CAPTURE{from_pos, discard_spool: true} is the operator's way out."""
        assert self.source is not None and self.spool is not None
        d = e.detail() if hasattr(e, "detail") else None
        exp = d.get("expected_from") if isinstance(d, dict) else None
        exp = exp.get("text") if isinstance(exp, dict) else exp
        to = hdr.get("to_pos", {}).get("text")
        nxt = (hdr.get("next_pos") or hdr.get("to_pos") or {}).get("text")
        if isinstance(exp, str) and to:
            try:
                c = self.source.compare(to, exp)
            except Exception:  # noqa: BLE001 - malformed position from the hub: treat as not covered
                c = 1
            if c < 0 or (c == 0 and nxt == to):
                log.warning("batch %s ends at %s, the hub already has data up to %s: deleted as covered",
                            hdr.get("batch_id"), to, exp)
                self.spool.delete(p)
                self.event("SPOOL_BATCH_COVERED", {"batch_id": hdr.get("batch_id"), "to_pos": to,
                                                   "expected_from": exp})
                self.next_ship = self.clock()
                return
        log.error("hub rejected %s (position gap/overlap): %s", hdr.get("batch_id", "catalog"), e.body[:300])
        self.state_name = "ERROR"
        self.event("SPOOL_BLOCKED_409", {"batch_id": hdr.get("batch_id"), "from_pos": hdr.get("from_pos"),
                                         "to_pos": hdr.get("to_pos"), "expected_from": exp,
                                         "hint": "START_CAPTURE{from_pos, discard_spool: true}"})
        self._ship_backoff("gap", slow=True)

    def _ship_backoff(self, why: str, slow: bool = False, retry_after: Optional[float] = None) -> None:
        self.ship_fail += 1
        wait = self._backoff(self.ship_fail + (3 if slow else 0))
        if retry_after is not None:                     # LNX-13: the hub knows best, bounded by backoff_max_s
            wait = min(float(self.cfg.hub.backoff_max_s), max(retry_after, 0.5)) * random.uniform(1.0, 1.2)
        self.next_ship = self.clock() + wait
        log.warning("%s; next send in %.0fs", why, wait)

    def _pace(self, nbytes: int, until: Optional[Deadline]) -> None:
        kbps = int(self.limits.get("max_rate_kbps", 0) or 0)
        if kbps > 0:
            t = nbytes * 8 / 1000 / kbps
            if until is not None:
                t = min(t, until.remaining())
            self.stop_event.wait(t) if until is None else time.sleep(t)

    # ------------------------------------------------------------------ hub: heartbeat / commands / events
    def heartbeat(self, timeout: Optional[float] = None) -> None:
        assert self.hub is not None
        self.next_hb = self.clock() + self.cfg.hub.heartbeat_s
        cpu, wall = time.process_time(), self.clock()
        if wall > self._cpu0[1]:
            self._cpu_pct = 100.0 * (cpu - self._cpu0[0]) / (wall - self._cpu0[1])
        self._cpu0 = (cpu, wall)
        eng = self.cfg.source.engine
        hb = {"agent_id": self.cfg.agent_id, "state": self.state_name,
              "last_read_pos": {"engine": eng, "text": self.last_read} if self.last_read else None,
              "durable_pos": {"engine": eng, "text": self.durable} if self.durable else None,
              "spool_bytes": self.spool.bytes() if self.spool else 0, "cpu_pct": round(self._cpu_pct, 2),
              "mem_mb": round(rss_mb(), 1), "batches_sent": self.sent, "errors": self.errors, "at": _now_iso()}
        self._touch()
        try:
            self.hub.post_json("/agent/v1/heartbeat", hb, timeout=timeout or self._net_timeout())
        except HubError as e:
            if e.status == 401 and self._credential_rejected("heartbeat"):
                return
            if 400 <= e.status < 500:
                self.registered = False
            log.warning("heartbeat failed: %s", e)
        except OSError as e:
            log.warning("heartbeat failed: %s", e)

    def event(self, typ: str, detail: Dict[str, Any], command_id: Optional[str] = None, force: bool = False,
              timeout: Optional[float] = None) -> None:
        """AgentEvent (best effort). Same type at most once per EVENT_THROTTLE_S unless `force`."""
        now = self.clock()
        if not force and now - self._ev_last.get(typ, -1e18) < EVENT_THROTTLE_S:
            return
        self._ev_last[typ] = now
        if self.hub is None or not self.registered:
            log.info("event %s not sent (not registered)", typ)
            return
        try:
            self.hub.post_json("/agent/v1/events", {"agent_id": self.cfg.agent_id, "type": typ, "detail": detail,
                                                    "command_id": command_id, "at": _now_iso()},
                               timeout=timeout or self._net_timeout())
        except (HubError, OSError) as e:
            log.warning("event %s not delivered: %s", typ, e)

    def _idle(self) -> bool:
        """Nothing to read or ship right now: a long command poll costs no latency (LNX-18)."""
        busy_capture = self.can_capture() and self.state_name == "CAPTURING"
        return not busy_capture and not (self.spool and self.spool.entries() and self.may_ship())

    def poll_commands(self) -> None:
        assert self.hub is not None
        wait = 0
        if self.cfg.hub.command_wait_s and self._idle():
            margin = float(self.lease.renew_s) / 2 if self.lease.enabled else float(self.cfg.hub.heartbeat_s)
            wait = int(max(0.0, min(float(self.cfg.hub.command_wait_s), margin, self._net_timeout() - 5)))
        self.next_cmd = self.clock() + (0.2 if wait else COMMAND_POLL_S)
        self._touch()
        try:
            cmds = self.hub.call("GET", f"/agent/v1/commands?agent_id={quote(self.cfg.agent_id, safe='')}&wait={wait}",
                                 timeout=self._net_timeout() + wait)
        except HubError as e:
            if e.status == 401 and self._credential_rejected("commands"):
                return
            log.warning("command poll failed: %s", e)
            return
        except OSError as e:
            log.warning("command poll failed: %s", e)
            return
        if isinstance(cmds, dict):
            cmds = cmds.get("commands") or []
        if not isinstance(cmds, list):
            log.warning("command poll: unexpected answer %s", str(cmds)[:200])
            return
        for c in cmds:
            if not isinstance(c, dict):
                continue
            cid = str(c.get("command_id", ""))
            if not _CMD_ID.match(cid):                  # LNX-21: never put hub-supplied text into a URL path
                log.warning("command with an invalid command_id %r ignored", cid[:80])
                self.event("COMMAND_REJECTED", {"reason": "invalid command_id", "name": str(c.get("name"))[:64]})
                continue
            self._touch()
            name = str(c.get("name"))
            args = c.get("args") if isinstance(c.get("args"), dict) else {}
            if cid in self.cmd_seen:
                continue                                 # re-delivery of a queued / running command: once only
            self.cmd_seen[cid] = self.clock()
            if name in HEAVY_COMMANDS and getattr(self, "cmd_thread", None) is not None:
                try:                                     # command thread: capture + heartbeats continue meanwhile
                    self.cmdq.put_nowait((cid, name, args))
                except queue.Full:
                    self.cmd_seen.pop(cid, None)         # the hub delivers it again
                continue
            self._run_command(cid, name, args)

    def _run_command(self, cid: str, name: str, args: Dict[str, Any]) -> None:
        assert self.hub is not None
        ok, result = self.execute(name, args, cid)
        log.info("command %s -> ok=%s", name, ok, extra={"correlation_id": cid})
        self._touch()
        try:
            self.hub.post_json(f"/agent/v1/commands/{quote(cid, safe='')}/ack"
                               f"?agent_id={quote(self.cfg.agent_id, safe='')}",
                               {"ok": ok, "result": result}, timeout=self._net_timeout())
        except (HubError, OSError) as e:
            log.warning("command ack failed: %s", e)

    def command_loop(self) -> None:
        """Command thread: one heavy command at a time (FIFO), own source context, governor applied."""
        while not self.stop_event.is_set():
            try:
                cid, name, args = self.cmdq.get(timeout=0.5)
            except queue.Empty:
                continue
            t0 = time.process_time()
            try:
                self._run_command(cid, name, args)
            except Exception:  # noqa: BLE001 - the thread never dies
                log.exception("command %s failed", name)
            pct = max(1, min(100, int(self.limits.get("cpu_pct", 100))))
            busy = time.process_time() - t0
            if pct < 100 and busy > 0:
                self.stop_event.wait(busy * (100 - pct) / pct)
        cs = getattr(self, "cmd_source", None)
        if cs is not None:
            try:
                cs.close()
            except Exception:  # noqa: BLE001
                pass

    def execute(self, name: str, args: Dict[str, Any], command_id: str) -> "tuple[bool, Dict[str, Any]]":
        """Whitelisted commands only (R5); anything else is rejected and acked ok=false. Never raises (LNX-11)."""
        assert self.source is not None and self.spool is not None
        if name in PLUGIN_COMMANDS and self.lease.enabled and not self.lease.active:
            return False, {"message": "agent is STANDBY (not the lease holder)"}
        try:
            if name == "PAUSE":
                self.paused = True
            elif name == "RESUME":
                self.paused = False
            elif name == "SHUTDOWN":
                self.exit_code = EXIT_OK
                self.request_stop("hub SHUTDOWN")
            elif name == "SET_LIMITS":
                self.limits = self.cfg.clamp({**self.limits, **args})
                return True, {"limits": self.limits}
            elif name == "SNAPSHOT_CATALOG":
                self.catalog_due = True
            elif name == "PROBE":
                return True, self.source.probe(str(args.get("name")))
            elif name == "START_CAPTURE":
                return self._start_capture(args.get("from_pos"), command_id, args.get("discard_spool") is True)
            elif name == "SNAPSHOT_TABLE":
                return self._snapshot(args)
            elif name == "VERIFY_TABLE":
                return self._verify(args)
            else:
                return False, {"code": "UNSUPPORTED", "message": f"command {name!r} not whitelisted"}
        except NotImplementedError as e:
            return False, {"code": "UNSUPPORTED", "message": str(e)}
        except SourceError as e:
            return False, {"code": "SOURCE_ERROR", "message": f"{type(e).__name__}: {e}"}
        except (ValueError, KeyError, TypeError) as e:
            return False, {"code": "TOO_LARGE" if "larger than" in str(e) else "SOURCE_ERROR",
                           "message": f"{type(e).__name__}: {e}"}
        except guard.GuardError:
            raise
        except Exception as e:  # noqa: BLE001 - a plugin bug never takes the agent down
            log.error("command %s failed: %s: %s", name, type(e).__name__, e, extra={"correlation_id": command_id})
            self.event("COMMAND_REJECTED", {"name": name, "error": f"{type(e).__name__}: {str(e)[:300]}"},
                       command_id=command_id, force=True)
            return False, {"code": "SOURCE_ERROR", "message": f"{type(e).__name__}: {e}"}
        return True, {"message": "ok"}

    def _verify(self, args: Dict[str, Any]) -> "tuple[bool, Dict[str, Any]]":
        """VERIFY_TABLE (F-03): local allow-list (snapshot.allowed_tables), refused while paused (operator or
        governor: the hub retries later), whitelisted args, read-only source statements."""
        assert self.source is not None
        t = args.get("table") if isinstance(args.get("table"), dict) else {}
        fq = f"{t.get('schema')}.{t.get('name')}".upper()
        if not table_allowed(self.cfg.allowed_tables, str(t.get("schema") or ""), str(t.get("name") or "")):
            return False, {"code": "NOT_ALLOWED", "message": f"table {fq} not in snapshot.allowed_tables"}
        if self.paused or self.gov_paused:
            return False, {"code": "PAUSED", "message": "paused (governor or operator): verify refused, retry later"}
        try:
            return True, self._command_source().verify_table(args)
        except ValueError as e:                 # VerifyArgsError: refused by the whitelist (R5)
            return False, {"code": "TOO_LARGE" if "larger than" in str(e) else "NOT_ALLOWED",
                           "message": f"refused: {e}"}

    def _command_source(self) -> Any:
        """Command thread: its own source context (second Db2 connection, read-only); else the capture source."""
        if threading.current_thread() is not getattr(self, "cmd_thread", None):
            return self.source
        if getattr(self, "cmd_source", None) is None:
            self.cmd_source = self._source_factory(self.cfg)
            self.cmd_source.on_connect = self._on_source_connect
        return self.cmd_source

    def _snapshot(self, args: Dict[str, Any]) -> "tuple[bool, Dict[str, Any]]":
        """SNAPSHOT_TABLE{table, snapshot_id, chunk_rows[, key_from, chunk_from]} (docs/AGENT-COMMANDS.md): allow-list,
        keyset-paged reads WITH CS on the command source, chunks POSTed to /agent/v1/snapshot with last_key."""
        import uuid as _uuid
        import zlib

        t = args.get("table") if isinstance(args.get("table"), dict) else {}
        schema, name = str(t.get("schema") or ""), str(t.get("name") or "")
        if not table_allowed(self.cfg.allowed_tables, schema, name):
            return False, {"code": "NOT_ALLOWED", "message": f"table {schema}.{name} not in snapshot.allowed_tables"}
        if self.paused or self.gov_paused:
            return False, {"code": "PAUSED", "message": "paused (governor or operator): snapshot refused, retry later"}
        sid = str(_uuid.UUID(str(args.get("snapshot_id"))))
        rows_per = max(1, min(int(args.get("chunk_rows") or self.cfg.max_chunk_rows), int(self.cfg.max_chunk_rows)))
        kf = args.get("key_from")
        if kf is not None and not (isinstance(kf, list) and all(isinstance(x, (str, int, float)) for x in kf)):
            raise ValueError("key_from must be a list of key values")
        n = int(args.get("chunk_from") or 0)
        src = self._command_source()
        start = src.snapshot_start()
        total = 0
        assert self.hub is not None
        for chunk, key_from, last, last_key in src.snapshot_chunks(schema, name, rows_per, kf):
            body = zlib.compress(b"".join(json.dumps(r, ensure_ascii=False).encode() + b"\n" for r in chunk))
            hdr = {"snapshot_id": sid, "table": {"schema": schema, "name": name}, "chunk_no": n,
                   "key_from": key_from or None, "last_key": last_key, "rows": len(chunk), "last": bool(last),
                   "start_pos": {"engine": src.engine, "text": start}, "codec": "zlib", "format": "jsonl"}
            self.hub.call("POST", "/agent/v1/snapshot", body, {"X-ALELT-Snapshot": json.dumps(hdr),
                                                               "Content-Type": "application/octet-stream",
                                                               **(self.lease.headers() if self.lease.enabled else {})},
                          timeout=self._net_timeout())
            n += 1
            total += len(chunk)
            if self.stop_event.is_set():
                return False, {"code": "TIMEOUT", "message": "agent stopping: snapshot interrupted (resume with key_from)"}
            if last:
                break
        return True, {"snapshot_id": sid, "chunks": n, "rows": total, "start_pos": start}

    def _start_capture(self, from_pos: Any, command_id: str, discard_spool: bool = False) -> "tuple[bool, Dict[str, Any]]":
        assert self.source is not None and self.spool is not None
        if isinstance(from_pos, dict):
            from_pos = from_pos.get("text")
        with self.lock:
            if from_pos:
                pos = self.source.validate(str(from_pos))
                if self.spool.batches():
                    if not discard_spool:
                        return False, {"message": "spool not empty; cannot reposition (add discard_spool: true to "
                                                  "discard the unshipped batches)"}
                    n = self.spool.purge()                # operator decision (audited by the hub command + event)
                    log.warning("START_CAPTURE %s: %d spool entries discarded by operator request", command_id, n)
                    self.event("SPOOL_DISCARDED", {"entries": n, "from_pos": pos}, command_id=command_id, force=True)
                self.capturing = True
                self.hold_reason = None
                self.refused_422.clear()
                self.chain, self.chain_ready = pos, True
                self.redecide = False
                self.reposition_cmd, self.reposition_pos = command_id, pos
                self.source.reset(pos)
                self.catalog_due = True
                self.next_ship = self.clock()
            else:
                self.capturing = True
                if not self.chain_ready:
                    self.decide_position()
            self._save_state("start")
        return True, {"from_pos": self.chain}

    # ------------------------------------------------------------------ checkpoint hooks (save_checkpoint)
    def flush_inflight(self) -> int:
        if self.source is None or self.spool is None or not self.chain_ready:
            return 0
        if self.lease.enabled and (not self.lease.active or self.lease.must_fence(self.clock())):
            return 0
        with self.lock:
            batches = self.source.flush(self.chain)
            for hdr, body in batches:
                self._spool_batch(hdr, body)
        return len(batches)

    def write_state(self, reason: str) -> None:
        with self.lock:
            self.store.save({"durable_pos": self.durable, "next_pos": self.chain if self.chain_ready else
                             self.saved.get("next_pos"), "last_read_pos": self.last_read,
                             "capturing": self.capturing, "reposition_command_id": self.reposition_cmd,
                             "reposition_pos": self.reposition_pos, "agent_id": self.cfg.agent_id,
                             "connection_id": self.cfg.connection_id, "engine": self.cfg.source.engine,
                             "lease_epoch": self.lease.epoch if self.lease.enabled else None, "reason": reason})
            if self.chain_ready:
                self.saved["next_pos"] = self.chain

    def _save_state(self, reason: str) -> None:
        """Runtime state.json write: the spool is the primary record, so a full disk only warns (LNX-07)."""
        try:
            self.write_state(reason)
        except OSError as e:
            if not no_space(e):
                raise
            log.warning("state.json not written (%s): the spool keeps the position", e)

    def drain_spool(self, budget_s: float) -> int:
        if self.hub is None or not self.registered or budget_s <= 0 or self.spool is None or not self.spool.entries():
            return 0
        if not self.may_ship():
            return 0
        n = self.ship_spool(Deadline(budget_s, self.clock))
        log.info("shutdown drain: %d batch(es) acked, %d entries left", n, len(self.spool.entries()))
        return n

    def release_lease(self, timeout_s: float) -> bool:
        if not self.lease.enabled or not self.lease.active:
            return False
        ok = self.lease.release(timeout_s)
        log.info("lease released" if ok else "lease release: not the holder",
                 extra={"correlation_id": f"epoch={self.lease.epoch}"})
        return ok

    # ------------------------------------------------------------------ helpers
    def _net_timeout(self) -> float:
        t = self.cfg.hub.request_timeout_ms / 1000
        wd = self.notifier.watchdog_s
        if wd:                                          # LNX-04: one call never outlives half the watchdog
            t = min(t, max(1.0, wd / 2 - 2))
        if self.stop_event.is_set():
            t = min(t, 10.0)
        return t

    def _backoff(self, n: int, cap: Optional[float] = None) -> float:
        base = float(min(cap or self.cfg.hub.backoff_max_s, 2 ** min(max(n, 1), 12)))
        return _jitter(base)                            # LNX-13: +-20 %, a fleet does not retry in lockstep
