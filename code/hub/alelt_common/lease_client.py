"""Capture lease client (agent side of X-47 / X-48), reusable by every Python agent (dev edge, Linux daemon, ...).

Pure stdlib. Two layers:
  LeaseState   decision logic only (no I/O), same rules as the C agent `edge-agent/core/src/agent/ha.c`
  LeaseClient  drives LeaseState over HTTP with a caller-supplied transport `post(path, body) -> (status, json)`

Rules (X-48): standby polls `acquire` every renew_s; the holder renews every renew_s; on 409 (renew) / 423 (batch)
or when ttl_s - renew_s passes without a successful renew the holder stops reading at once (self-fencing) and turns
standby. A grant with `takeover: true` means another agent held the lease: discard the local spool and resume at
`resume_from` (hub durable position). Batches carry `X-ALELT-Lease-Epoch` while active (`headers()`).

Typical loop (one thread):
    lc = LeaseClient(post, agent_id, connection_id, ttl_s=30, renew_s=10)
    while running:
        ev = lc.tick()                      # acquire / renew when due, self-fence when late
        if ev.kind == "takeover": purge_spool(); position = ev.resume_from
        if lc.may_read(): read + ship with headers=lc.headers(); on 423 -> lc.on_lost("fenced")
        sleep(min(lc.wait_s(), 1))
    lc.release()
"""
from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

# event kinds (C: ALELT_HA_NONE / ACTIVE / TAKEOVER / RENEWED / LOST)
NONE, ACTIVE, TAKEOVER, RENEWED, LOST = "none", "active", "takeover", "renewed", "lost"
EPOCH_HEADER = "X-ALELT-Lease-Epoch"
Post = Callable[[str, dict[str, Any]], tuple[int, Any]]


def boot_clock() -> float:
    """Monotonic seconds that keep counting while the host is suspended (CLOCK_BOOTTIME on Linux), so a resumed
    VM never believes its lease is younger than the hub does (LNX-10). Falls back to time.monotonic."""
    return time.clock_gettime(_BOOTTIME) if _BOOTTIME is not None else time.monotonic()


_BOOTTIME = getattr(time, "CLOCK_BOOTTIME", None)
if _BOOTTIME is not None:
    try:
        time.clock_gettime(_BOOTTIME)
    except OSError:
        _BOOTTIME = None


def _jitter(seconds: float, frac: float = 0.2) -> float:
    return seconds * (1.0 + random.uniform(-frac, frac))


@dataclass
class LeaseEvent:
    kind: str = NONE
    epoch: int = -1
    holder: str | None = None
    resume_from: dict[str, Any] | None = None    # Position JSON {engine, text} on takeover / acquire
    detail: str = ""
    # AHS-24: False = the operator paused this connection (the new holder starts paused), None = not stated
    capturing: bool | None = None


@dataclass
class LeaseState:
    """Decision logic, time in monotonic seconds (injectable for tests)."""
    enabled: bool = True
    ttl_s: int = 30
    renew_s: int = 10
    active: bool = field(init=False)
    epoch: int = field(init=False, default=-1)
    lease_id: str | None = field(init=False, default=None)
    last_ok: float = field(init=False, default=0.0)
    next_at: float = field(init=False, default=0.0)

    def __post_init__(self) -> None:
        self.ttl_s = self.ttl_s if self.ttl_s > 0 else 30
        if not (0 < self.renew_s <= self.ttl_s // 2):
            self.renew_s = max(1, self.ttl_s // 3)
        self.active = not self.enabled

    def due(self, now: float) -> bool:
        return self.enabled and now >= self.next_at

    def _fence_at(self) -> float:
        return self.last_ok + max(1, self.ttl_s - self.renew_s)

    def must_fence(self, now: float) -> bool:
        return self.enabled and self.active and now >= self._fence_at()

    def wait_s(self, now: float) -> float:
        if not self.enabled:
            return 60.0
        t = self.next_at
        if self.active:
            t = min(t, self._fence_at())
        return max(0.0, t - now)

    def on_grant(self, g: dict[str, Any], now: float, sent_at: float | None = None) -> LeaseEvent:
        """`sent_at` = when the request left (LNX-10 / AHS-07): the hub started the TTL when it processed the
        request, so the local fence clock starts no later than the send time, never at the (slow) reply."""
        if not self.enabled:
            return LeaseEvent()
        epoch = int(g.get("epoch") or 0)
        if not g.get("granted"):
            retry = float(self.renew_s)
            ra = float(g.get("retry_after_s") or 0)
            if 0 < ra < retry:
                retry = ra
            self.next_at = now + max(0.25, retry)
            if self.active:
                self.active = False
                return LeaseEvent(LOST, epoch, g.get("holder"), detail="lease refused")
            return LeaseEvent(NONE, epoch, g.get("holder"))
        was = self.active
        lid = str(g.get("lease_id") or "")
        changed = epoch != self.epoch or lid != (self.lease_id or "")
        self.epoch, self.lease_id = epoch, lid
        ttl = int(g.get("ttl_s") or 0)
        if 0 < ttl < self.ttl_s:
            self.ttl_s = ttl
            if self.renew_s > ttl // 2:
                self.renew_s = max(1, ttl // 2)
        self.last_ok = min(now, sent_at) if sent_at is not None else now
        self.next_at = self.last_ok + self.renew_s
        self.active = True
        kind = TAKEOVER if g.get("takeover") else ACTIVE if (not was or changed) else RENEWED
        cap = g.get("capturing")
        return LeaseEvent(kind, epoch, g.get("holder"), g.get("resume_from"),
                          capturing=cap if isinstance(cap, bool) else None)

    def on_error(self, now: float) -> LeaseEvent:
        if not self.enabled:
            return LeaseEvent()
        # active: retry soon (must renew before the fence); standby: every renew_s with jitter, so N standbys do
        # not hammer a hub that is down (LNX-19)
        self.next_at = now + (min(1.0, max(0.25, self.renew_s / 4)) if self.active else _jitter(self.renew_s))
        if self.must_fence(now):
            self.active = False
            self.next_at = now + self.renew_s
            return LeaseEvent(LOST, self.epoch, detail="not renewed in time (self-fencing)")
        return LeaseEvent()

    def on_lost(self, now: float, why: str = "lease lost") -> LeaseEvent:
        if not self.enabled:
            return LeaseEvent()
        self.next_at = now + self.renew_s
        if self.active:
            self.active = False
            return LeaseEvent(LOST, self.epoch, detail=why)
        return LeaseEvent()

    def header_epoch(self) -> int:
        return self.epoch if (self.enabled and self.active) else -1


class LeaseClient:
    """HTTP driver. `post(path, body)` returns (status_code, parsed JSON or None); raise on transport errors.

    Thread-safe: `tick()` may run in its own thread while the capture thread calls `may_read()` / `headers()` /
    `on_lost()`. `lock` is held while state changes, so a capture thread that checks `may_read()` under `lock` right
    before spooling a batch never spools after the lease was lost.
    """

    def __init__(self, post: Post, agent_id: str, connection_id: str, ttl_s: int = 30, renew_s: int = 10,
                 enabled: bool = True, clock: Callable[[], float] = boot_clock,
                 log: Callable[[str], None] | None = None) -> None:
        self.post, self.agent_id, self.connection_id = post, agent_id, connection_id
        self.state = LeaseState(enabled, ttl_s, renew_s)
        self.clock, self.log = clock, log or (lambda _m: None)
        self.lock = threading.RLock()
        self.holder: str | None = None

    @property
    def enabled(self) -> bool:
        return self.state.enabled

    def _body(self, with_lease: bool) -> dict[str, Any]:
        b: dict[str, Any] = {"agent_id": self.agent_id, "connection_id": self.connection_id,
                             "ttl_s": max(5, min(300, self.state.ttl_s))}
        if with_lease:
            b["lease_id"], b["epoch"] = self.state.lease_id, self.state.epoch
        return b

    def tick(self) -> LeaseEvent:
        """Self-fence if late; acquire (standby) / renew (active) when due. Never raises on transport errors."""
        with self.lock:
            now = self.clock()
            if self.state.must_fence(now):
                ev = self.state.on_lost(now, "no successful renew within ttl_s - renew_s (self-fencing)")
                self._say(ev)
                return ev
            if not self.state.due(now):
                return LeaseEvent()
            active = self.state.active
            body = self._body(active)
            sent_at = self.clock()
        op = "renew" if active else "acquire"
        try:
            code, js = self.post(f"/agent/v1/lease/{op}", body)
        except Exception as e:  # noqa: BLE001 - hub unreachable: retry, self-fence on time
            with self.lock:
                ev = self.state.on_error(self.clock())
            self.log(f"lease {op} failed: {e}")
            self._say(ev)
            return ev
        with self.lock:
            now = self.clock()
            if code == 409:
                ev = self.state.on_lost(now, "hub answered lease_lost on renew")
            elif code == 200 and isinstance(js, dict):
                ev = self.state.on_grant(js, now, sent_at)
                self.holder = js.get("holder")
            else:
                ev = self.state.on_error(now)
                self.log(f"lease {op}: HTTP {code} {str(js)[:200]}")
        self._say(ev)
        return ev

    def on_lost(self, why: str = "batch fenced (423)") -> LeaseEvent:
        with self.lock:
            ev = self.state.on_lost(self.clock(), why)
        self._say(ev)
        return ev

    def may_read(self) -> bool:
        with self.lock:
            return self.state.active and not self.state.must_fence(self.clock())

    def headers(self) -> dict[str, str]:
        e = self.state.header_epoch()
        return {EPOCH_HEADER: str(e)} if e >= 0 else {}

    def wait_s(self) -> float:
        with self.lock:
            return self.state.wait_s(self.clock())

    def release(self) -> bool:
        """Give the lease up now (clean shutdown): the standby may take over without waiting for the TTL."""
        with self.lock:
            if not (self.state.enabled and self.state.active):
                return False
            body = self._body(True)
            self.state.on_lost(self.clock(), "released")
        try:
            code, js = self.post("/agent/v1/lease/release", body)
            return code == 200 and bool((js or {}).get("released"))
        except Exception as e:  # noqa: BLE001
            self.log(f"lease release failed ({e}): the standby takes over after the ttl")
            return False

    def _say(self, ev: LeaseEvent) -> None:
        if ev.kind == TAKEOVER:
            self.log(f"capture lease acquired by TAKEOVER (epoch {ev.epoch}): discard the local spool, "
                     f"resume at {(ev.resume_from or {}).get('text', '-')}")
        elif ev.kind == ACTIVE:
            self.log(f"capture lease held (epoch {ev.epoch}): ACTIVE")
        elif ev.kind == LOST:
            self.log(f"capture lease lost ({ev.detail}): reading stopped, now STANDBY")
