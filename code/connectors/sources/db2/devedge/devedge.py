"""ALELT dev edge agent for Db2 LUW (Python stdlib only; x64 Python because the Db2 DLLs are x64).

Stand-in for the C Windows service: speaks the identical agent <-> hub protocol v1.
  register -> catalog snapshot (SYSCAT WITH UR) -> POST /catalog -> loop:
    db2ReadLog READ (filter ON) from the resume position -> batch (zlib, sha256 of the uncompressed body)
    -> local spool (fsync) -> POST /blocks -> delete spool <= durable_pos after ACK
  plus command polling (whitelisted, R5), heartbeat, CPU governor, spool cap (backpressure, never drop).
Commands (docs/AGENT-COMMANDS.md): SNAPSHOT_TABLE / VERIFY_TABLE / PROBE / SNAPSHOT_CATALOG run on a COMMAND THREAD
with its own Db2 connection (serial FIFO queue, governor applied), so capture and heartbeats continue during a long
snapshot; the poll loop only enqueues them (a command id is executed once, re-deliveries are ignored). Snapshots are
resumable (`key_from`, `chunk_from`; chunk header `last_key`). Errors are acked with a typed `code`.

Read-only on Db2 (R1): only SELECT ... WITH UR on SYSCAT and db2ReadLog. Outbound HTTP only (R4).
Batch positions: from_pos = LRI the READ chain started at (= previous batch next_pos), to_pos = LRI of the
last record, next_pos = nextStartLRI. Body = raw records exactly as db2ReadLog returned them.

Db2 access (ctypes CLI + db2ReadLog, catalog, batch builder) lives in the shared library `alelt_db2edge`
(next to this file), also used by the Linux agent daemon (edge-agent/linux/aleltd).

DEV ONLY (LNX-28): token authentication only - no mTLS client certificate, no server certificate pinning, no
watchdog, no systemd/service integration. Production edges are the Windows service (edge-agent/core) and the Linux
daemon (edge-agent/linux/aleltd). A plain http:// hub URL to a non-loopback host is warned about.

Run: python devedge.py --config devedge.json [--once]
"""
from __future__ import annotations

import argparse
import ctypes as C
import ipaddress
import json
import logging
import os
import queue
import random
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

VERSION = "0.1.0-devedge"
log = logging.getLogger("devedge")

# ------------------------------------------------------------------ Db2 capture (shared library: alelt_db2edge)
sys.path.insert(0, str(Path(__file__).resolve().parent))
from alelt_db2edge import (RL_READ_TO_CURRENT, Db2, Db2Error, lri_key, make_batch, read_catalog,  # noqa: E402
                        snapshot_chunks, table_allowed, valid_allow_entry, verify_table)

# capture lease client (active/passive, X-48), shared with the other Python agents: hub/alelt_common/lease_client.py
_HUB = Path(__file__).resolve().parents[4] / "hub"
if (_HUB / "alelt_common" / "lease_client.py").exists() and str(_HUB) not in sys.path:
    sys.path.append(str(_HUB))
from alelt_common.lease_client import TAKEOVER, LeaseClient  # noqa: E402

CREDENTIAL_CAPABILITY = "credential.v1"


def fsync_dir(path: Path) -> None:
    if os.name == "nt":                      # directories cannot be opened for fsync on Windows
        return
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write(path: Path, data: bytes, mode: Optional[int] = None) -> None:
    """tmp -> fsync -> rename -> fsync(dir): readers see the old file or the new one, never a partial one."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    old = os.umask(0o077) if mode is not None else None
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
    finally:
        if old is not None:
            os.umask(old)
    if mode is not None:
        try:
            os.chmod(tmp, mode)
        except OSError:
            pass
    os.replace(tmp, path)
    fsync_dir(path.parent)


def _loopback(url: str) -> bool:
    h = (urllib.parse.urlsplit(url).hostname or "").lower()
    if h == "localhost":
        return True
    try:
        return ipaddress.ip_address(h).is_loopback
    except ValueError:
        return False


# ------------------------------------------------------------------ hub link
class HubError(RuntimeError):
    def __init__(self, status: int, body: str, headers: Optional[Dict[str, str]] = None) -> None:
        super().__init__(f"HTTP {status}: {body[:300]}")
        self.status, self.body, self.headers = status, body, headers or {}


class Hub:
    def __init__(self, url: str, token: str, timeout: float = 30.0) -> None:
        self.url, self.timeout = url.rstrip("/"), timeout
        self.h = {"X-ALELT-Protocol": "1", "X-ALELT-Agent-Token": token}
        self.env_token = token

    def set_token(self, token: str) -> None:
        self.h["X-ALELT-Agent-Token"] = token

    def call(self, method: str, path: str, body: Optional[bytes] = None, headers: Optional[Dict[str, str]] = None,
             timeout: Optional[float] = None) -> Any:
        req = urllib.request.Request(self.url + path, data=body, method=method, headers={**self.h, **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout) as r:
                data = r.read()
        except urllib.error.HTTPError as e:
            hdrs = {k.lower(): v for k, v in (e.headers or {}).items()}
            raise HubError(e.code, e.read().decode(errors="replace"), hdrs) from None
        if not data:
            return None
        try:
            return json.loads(data)
        except ValueError:
            raise HubError(502, "non-JSON answer from the hub") from None

    def post_json(self, path: str, obj: Any) -> Any:
        return self.call("POST", path, json.dumps(obj).encode(), {"Content-Type": "application/json"})


# ------------------------------------------------------------------ spool
class Spool:
    """Local batch spool: <seq>.batch = header JSON line + compressed body; fsync before send."""

    def __init__(self, root: Path, cap_bytes: int) -> None:
        self.root, self.cap = root, cap_bytes
        root.mkdir(parents=True, exist_ok=True)
        for t in root.glob("*.tmp"):           # interrupted writes were never part of the spool (LNX-25)
            t.unlink()

    def files(self) -> List[Path]:
        return sorted(self.root.glob("*.batch"))

    def bytes(self) -> int:
        return sum(f.stat().st_size for f in self.files())

    def full(self) -> bool:
        return self.bytes() >= self.cap

    def put(self, hdr: Dict[str, Any], body: bytes) -> Path:
        seq = int(self.files()[-1].stem) + 1 if self.files() else 1
        p = self.root / f"{seq:012d}.batch"
        tmp = p.with_suffix(".batch.tmp")
        with open(tmp, "wb") as f:
            f.write(json.dumps(hdr).encode() + b"\n" + body)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)
        fsync_dir(self.root)
        return p

    def quarantine(self, p: Path) -> None:
        q = self.root / "quarantine"
        q.mkdir(exist_ok=True)
        os.replace(p, q / p.name)

    @staticmethod
    def get(p: Path) -> Tuple[Dict[str, Any], bytes]:
        data = p.read_bytes()
        line, _, body = data.partition(b"\n")
        return json.loads(line), body

    def release(self, durable_pos: str) -> int:
        """Delete spooled batches whose to_pos <= durable_pos (only after hub ACK)."""
        n, d = 0, lri_key(durable_pos)
        for p in self.files():
            try:
                hdr, _ = self.get(p)
            except (ValueError, KeyError, OSError):
                break
            if lri_key(hdr["to_pos"]["text"]) <= d:
                p.unlink()
                n += 1
        return n

    def last_next(self) -> Optional[str]:
        f = self.files()
        return self.get(f[-1])[0]["next_pos"]["text"] if f else None


# ------------------------------------------------------------------ agent
@dataclass
class Config:
    hub_url: str
    agent_id: str
    connection_id: str
    database: str = "SAMPLE"
    schemas: List[str] = field(default_factory=list)
    token_env: str = "ALELT_AGENT_TOKEN"
    db2_bin: str = r"D:\IBM\SQLLIB\BIN"
    db_user_env: str = ""
    db_password_env: str = ""
    spool_dir: str = ".alelt/devedge-spool"
    spool_max_mb: int = 256
    codec: str = "zlib"
    start: str = "current"            # current | oldest | '<part1>:<part2hex16>' (used only with no hub resume)
    heartbeat_s: float = 15.0
    command_poll_s: float = 2.0
    state_file: str = ".alelt/devedge-state.json"
    limits: Dict[str, Any] = field(default_factory=lambda: {"cpu_pct": 50, "mem_mb": 512, "batch_kb": 256,
                                                            "max_rate_kbps": 0, "poll_ms": 1000})
    ha: Dict[str, Any] = field(default_factory=lambda: {"mode": "standalone", "lease_ttl_s": 30, "renew_s": 10})
    # production guard (REQ-12, X-55): same entries as the hub deny list; PROD refused unless allow_prod (local)
    deny_hosts: List[str] = field(default_factory=list)
    # VERIFY_TABLE / SNAPSHOT_TABLE: local allow-list "SCHEMA.TABLE" or "SCHEMA.*" (= aleltagent.json
    # snapshot.allowed_tables)
    allowed_tables: List[str] = field(default_factory=list)
    environment: str = "TEST"
    allow_prod: bool = False
    credential_file: str = ".alelt/devedge-credential.json"     # REQ-26 per-agent credential, 0600
    host: str = ""
    port: int = 0

    @classmethod
    def load(cls, path: str) -> "Config":
        d = json.loads(Path(path).read_text(encoding="utf-8"))
        # dev-box overrides (a local test database without editing the shared sample config)
        for env, key in (("ALELT_DB2_DATABASE", "database"), ("ALELT_CONNECTION_ID", "connection_id")):
            if os.environ.get(env):
                d[key] = os.environ[env]
        if os.environ.get("ALELT_DB2_SCHEMA"):
            d["schemas"] = [x.strip() for x in os.environ["ALELT_DB2_SCHEMA"].split(",") if x.strip()]
        base = Path(path).resolve().parent
        cfg = cls(**d)
        bad = [a for a in cfg.allowed_tables if not valid_allow_entry(a)]
        if bad:                                          # same syntax as aleltd / the C agent (*.* refused)
            raise SystemExit(f"allowed_tables: invalid entries {bad} (SCHEMA.TABLE or SCHEMA.*)")
        for k in ("spool_dir", "state_file", "credential_file"):
            if not Path(getattr(cfg, k)).is_absolute():
                setattr(cfg, k, str(base / getattr(cfg, k)))
        return cfg


def _mem_mb() -> float:
    class PMC(C.Structure):
        _fields_ = [("cb", C.c_uint32), ("PageFaultCount", C.c_uint32), ("PeakWorkingSetSize", C.c_size_t),
                    ("WorkingSetSize", C.c_size_t), ("a", C.c_size_t), ("b", C.c_size_t), ("c", C.c_size_t),
                    ("d", C.c_size_t), ("PagefileUsage", C.c_size_t), ("PeakPagefileUsage", C.c_size_t)]
    try:
        pmc = PMC(cb=C.sizeof(PMC))
        k32 = C.WinDLL("kernel32")
        k32.GetCurrentProcess.restype = C.c_void_p
        k32.K32GetProcessMemoryInfo(C.c_void_p(k32.GetCurrentProcess()), C.byref(pmc), pmc.cb)
        return pmc.WorkingSetSize / 1048576
    except Exception:  # noqa: BLE001
        return 0.0


# Limit bounds = the C agent (command.c SET_LIMITS) so every edge treats a hub value the same way (XC-15).
LIMIT_BOUNDS = {"cpu_pct": (1, 6400), "mem_mb": (64, 65536), "batch_kb": (64, 65536), "max_rate_kbps": (0, 10_000_000),
                "poll_ms": (50, 600_000)}


def clamp_limits(cur: Dict[str, Any], new: Dict[str, Any], ceiling: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Known integer limits only, clamped to LIMIT_BOUNDS and (register) to the local config as ceiling."""
    out = dict(cur)
    for k, (lo, hi) in LIMIT_BOUNDS.items():
        v = new.get(k)
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            continue
        v = max(lo, min(hi, int(v)))
        if ceiling is not None and isinstance(ceiling.get(k), int) and k != "poll_ms" and ceiling[k] > 0:
            v = min(v, int(ceiling[k]))
        out[k] = v
    return out


def guard(cfg: Config) -> None:
    """REQ-12: refuse (exit 78, no retry) a database alias / host on deny_hosts, or PROD without allow_prod."""
    env = (cfg.environment or "TEST").upper()
    if env not in ("TEST", "PROD"):
        raise SystemExit(78)
    if env == "PROD" and not cfg.allow_prod:
        log.error("environment=PROD: refused - set \"allow_prod\": true in the local config (DBA decision)")
        raise SystemExit(78)
    if not cfg.deny_hosts:
        return
    from alelt_import.guard import DenyList

    deny = DenyList.of(cfg.deny_hosts)
    for target, port in ((cfg.host, cfg.port or None), (cfg.database, None)):
        is_host = bool(target and target == cfg.host)
        m = deny.match(target, port, resolve=is_host, fail_closed=is_host) if target else None
        if m:
            log.error("source %r matches the production deny list entry %r: refusing to connect", target, m)
            raise SystemExit(78)


def guard_connected(cfg: Config, db: Any) -> None:
    """LNX-08: re-check the deny list against the server the alias really reached (node directory)."""
    if not cfg.deny_hosts:
        return
    from alelt_import.guard import DenyList

    info = db.server_info() if hasattr(db, "server_info") else {}
    host = (info or {}).get("host")
    if not host:
        log.error("deny_hosts is set but the connected Db2 server could not be identified: refusing (fail closed)")
        raise SystemExit(78)
    m = DenyList.of(cfg.deny_hosts).match(host, None, resolve=True, fail_closed=True)
    if m:
        log.error("connected Db2 server %r matches the production deny list entry %r: refusing", host, m)
        raise SystemExit(78)


class Agent:
    def __init__(self, cfg: Config) -> None:
        guard(cfg)
        self.cfg = cfg
        if cfg.hub_url.startswith("http://") and not _loopback(cfg.hub_url):
            log.warning("dev edge: plain http to a non-loopback hub sends the agent token in clear (dev only)")
        self.cred: Dict[str, Any] = self._load_credential()
        token = os.environ.get(cfg.token_env)
        if not token and not self.cred:
            raise SystemExit(f"agent token missing: set environment variable {cfg.token_env} (R9)")
        self.hub = Hub(cfg.hub_url, token or "")
        if self.cred:
            self.hub.set_token(self.cred["credential"])
        self.spool = Spool(Path(cfg.spool_dir), cfg.spool_max_mb << 20)
        self.limits = dict(cfg.limits)
        self.state = "IDLE"
        self.paused = False
        self.stop = False
        self.db: Optional[Db2] = None
        self.cursor: Optional[str] = None        # next READ start
        self.batch_from: Optional[str] = None    # from_pos of the next batch
        self.durable: Optional[str] = None
        self.last_read: Optional[str] = None
        self.sent = self.errors = 0
        self.reposition: Optional[str] = None    # START_CAPTURE command id to present with the next batch
        self._cpu0 = (time.process_time(), time.monotonic())
        self._cpu_pct = 0.0
        ha = {"mode": "standalone", "lease_ttl_s": 30, "renew_s": 10, **(cfg.ha or {})}
        if ha["mode"] not in ("standalone", "active_passive"):
            raise SystemExit(f"ha.mode must be standalone or active_passive, not {ha['mode']!r}")
        self.lease = LeaseClient(self._lease_post, cfg.agent_id, cfg.connection_id, int(ha["lease_ttl_s"]),
                                 int(ha["renew_s"]), enabled=ha["mode"] == "active_passive",
                                 log=lambda m: log.warning("ha: %s", m))
        # command thread (own Db2 connection): heavy commands never stop capture / heartbeats
        self.cmdq: "queue.Queue[Tuple[str, str, Dict[str, Any]]]" = queue.Queue(maxsize=64)
        self.cmd_db: Optional[Db2] = None
        self.cmd_seen: Dict[str, float] = {}        # command id -> time first seen (re-deliveries ignored)
        self.cmd_thread: Optional[threading.Thread] = None

    def _lease_post(self, path: str, body: Dict[str, Any]) -> Tuple[int, Any]:
        try:
            return 200, self.hub.post_json(path, body)
        except HubError as e:
            try:
                return e.status, json.loads(e.body)
            except ValueError:
                return e.status, None

    def ha_step(self, resume: Optional[str]) -> bool:
        """Active/passive: acquire / renew the capture lease. Returns True when this agent may read and ship."""
        if not self.lease.enabled:
            return True
        ev = self.lease.tick()
        if ev.kind == TAKEOVER:                    # another agent shipped meanwhile: the hub position is the truth
            n = len(self.spool.files())
            for p in self.spool.files():
                p.unlink()
            pos = (ev.resume_from or {}).get("text")
            self.cursor = self.batch_from = pos or self.initial_position(None)
            self.reposition = None
            log.warning("ha: TAKEOVER at epoch %s: %d stale spooled batch(es) discarded, capture resumes at %s",
                        ev.epoch, n, self.cursor)
            if ev.capturing is False:              # AHS-24: the operator paused this connection on the old holder
                self.paused = True
                log.warning("ha: connection paused by the operator: staying paused until RESUME")
        elif ev.kind == "active" and self.cursor is None:
            self.cursor = self.batch_from = self.initial_position((ev.resume_from or {}).get("text") or resume)
            log.info("ha: ACTIVE at epoch %s, capture starts at %s", ev.epoch, self.cursor)
        return self.lease.may_read() and self.cursor is not None

    # ---------------------------------------------------------------- lifecycle
    def connect(self) -> Db2:
        if self.db is None:
            u = os.environ.get(self.cfg.db_user_env) if self.cfg.db_user_env else None
            pw = os.environ.get(self.cfg.db_password_env) if self.cfg.db_password_env else None
            db = Db2(self.cfg.database, self.cfg.db2_bin, u, pw)
            try:
                guard_connected(self.cfg, db)
            except SystemExit:
                db.close()
                raise
            self.db = db
        return self.db

    def connect_cmd(self) -> Db2:
        """Second Db2 connection, used only by the command thread (read-only, WITH CS reads, same user)."""
        if self.cmd_db is None:
            u = os.environ.get(self.cfg.db_user_env) if self.cfg.db_user_env else None
            pw = os.environ.get(self.cfg.db_password_env) if self.cfg.db_password_env else None
            db = Db2(self.cfg.database, self.cfg.db2_bin, u, pw)
            try:
                guard_connected(self.cfg, db)
            except SystemExit:
                db.close()
                raise
            self.cmd_db = db
        return self.cmd_db

    # ---------------------------------------------------------------- REQ-26 per-agent credential
    def _load_credential(self) -> Dict[str, Any]:
        try:
            d = json.loads(Path(self.cfg.credential_file).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        return d if isinstance(d, dict) and str(d.get("credential", "")).startswith("alelta_") else {}

    def _store_credential(self, token: Any, expires_at: Any) -> None:
        if not isinstance(token, str) or not token.startswith("alelta_"):
            log.error("hub returned a malformed agent credential; ignored")
            return
        d = {"credential": token, "credential_expires_at": str(expires_at) if expires_at else None,
             "issued_at": datetime.now(timezone.utc).isoformat()}
        atomic_write(Path(self.cfg.credential_file), json.dumps(d).encode(), mode=0o600)   # before switching
        self.cred = d
        self.hub.set_token(token)
        log.info("per-agent credential stored (%s***, expires %s)", token[:4], d["credential_expires_at"])

    def _credential_rejected(self) -> bool:
        """401 with the stored credential: discard it, fall back to the env token once (True = retry)."""
        if not self.cred or self.hub.h.get("X-ALELT-Agent-Token") != self.cred.get("credential"):
            return False
        log.error("hub rejected the stored per-agent credential; falling back to %s - re-enroll if this persists",
                  self.cfg.token_env if self.hub.env_token else "nothing (no token configured)")
        try:
            os.replace(self.cfg.credential_file, self.cfg.credential_file + ".rejected")
        except OSError:
            pass
        self.cred = {}
        self.hub.set_token(self.hub.env_token)
        return bool(self.hub.env_token)

    def maybe_rotate(self, now: Optional[datetime] = None) -> None:
        exp = self.cred.get("credential_expires_at") if self.cred else None
        if not exp:
            return
        try:
            end = datetime.fromisoformat(str(exp).replace("Z", "+00:00"))
            end = end if end.tzinfo else end.replace(tzinfo=timezone.utc)
        except ValueError:
            return
        if (now or datetime.now(timezone.utc)) < end - timedelta(days=7):
            return
        try:
            r = self.hub.post_json("/agent/v1/credential/rotate", {"agent_id": self.cfg.agent_id})
        except HubError as e:
            if e.status == 401:
                self._credential_rejected()
            log.warning("credential rotation failed: HTTP %s", e.status)
            return
        except OSError as e:
            log.warning("credential rotation failed: %s", e)
            return
        if isinstance(r, dict) and r.get("credential"):
            self._store_credential(r["credential"], r.get("credential_expires_at"))

    def register(self) -> Optional[str]:
        """Register (retried with capped, jittered backoff, LNX-27). Returns the hub resume position text."""
        n = 0
        while True:
            err: Exception
            try:
                r = self.hub.post_json("/agent/v1/register", {
                    "agent_id": self.cfg.agent_id, "connection_id": self.cfg.connection_id, "engine": "db2",
                    "version": VERSION, "hostname": socket.gethostname(), "database": self.cfg.database,
                    "capabilities": ["db2ReadLog", "catalog", "commands:long-poll", "verify_table",
                                     "snapshot_table", CREDENTIAL_CAPABILITY]})
                if not isinstance(r, dict):
                    raise HubError(502, "register answer is not an object")
                break
            except HubError as e:
                if e.status == 401 and self._credential_rejected():
                    continue
                if e.status in (400, 403, 426):
                    raise SystemExit(f"hub refused the registration: {e}") from None
                err = e
            except OSError as e:
                err = e
            n += 1
            wait = min(60.0, 2.0 ** min(n, 6)) * random.uniform(0.8, 1.2)
            log.warning("register failed (%s); retrying in %.0fs", err, wait)
            if self.stop:
                raise SystemExit(1)
            time.sleep(wait)
        if isinstance(r.get("limits"), dict):    # XC-15: hub values are clamped, never above the local config
            self.limits = clamp_limits(self.limits, r["limits"], self.cfg.limits)
        if r.get("credential"):
            self._store_credential(r["credential"], r.get("credential_expires_at"))
        res = r.get("resume_from")
        return res.get("text") if isinstance(res, dict) else None

    def send_catalog(self, db: Optional[Db2] = None) -> None:
        cat = read_catalog(db or self.connect(), self.cfg.schemas, self.cfg.connection_id)
        r = self.hub.post_json("/agent/v1/catalog", cat)
        log.info("catalog posted: version=%s tables=%s position=%s", r.get("version"), r.get("tables"),
                 cat["position"]["text"])

    def initial_position(self, resume: Optional[str]) -> str:
        spooled = self.spool.last_next()
        if spooled:
            return spooled
        if resume:
            return resume
        saved = self.load_state().get("batch_from")
        if saved:
            return str(saved)
        db = self.connect()
        db.commit()
        initial, current = db.query_lri()
        if self.cfg.start == "current":
            return current
        if self.cfg.start == "oldest":
            return initial
        lri_key(self.cfg.start)
        return self.cfg.start

    def load_state(self) -> Dict[str, Any]:
        try:
            d = json.loads(Path(self.cfg.state_file).read_text(encoding="utf-8"))
            return d if isinstance(d, dict) else {}
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as e:            # corrupt state never crashes the edge (LNX-25)
            log.warning("state file unreadable (%s); position comes from spool / hub / start", e)
            return {}

    def save_state(self) -> None:
        atomic_write(Path(self.cfg.state_file),
                     json.dumps({"batch_from": self.batch_from, "cursor": self.cursor, "durable": self.durable}).encode())

    # ---------------------------------------------------------------- capture
    def read_once(self) -> Tuple[int, int]:
        """One READ; spools a batch when records were returned. Returns (records, sqlcode)."""
        assert self.cursor and self.batch_from
        buf = max(16, int(self.limits.get("batch_kb", 256))) * 1024
        raw, recs, nxt, code = self.connect().read(self.cursor, buf)
        if recs:
            hdr, body, last = make_batch(raw, recs, nxt, self.batch_from, self.cfg.connection_id, self.cfg.agent_id,
                                         self.cfg.codec)
            with self.lease.lock:                  # never spool after the lease was lost (self-fencing)
                if not self.lease.may_read():
                    return 0, code
                self.spool.put(hdr, body)
            self.batch_from = nxt
            self.last_read = last
        self.cursor = nxt
        self.save_state()
        return recs, code

    def flush_spool(self) -> bool:
        """Send spooled batches oldest first. Returns False if the hub refused (retry later)."""
        for p in self.spool.files():
            try:
                hdr, body = self.spool.get(p)
            except (ValueError, OSError) as e:
                log.error("corrupt spool file %s quarantined (%s); restart re-reads from the hub position", p.name, e)
                self.spool.quarantine(p)
                return False
            extra = {"X-ALELT-Batch": json.dumps(hdr), "Content-Type": "application/octet-stream"}
            if self.reposition:
                extra["X-ALELT-Reposition"] = self.reposition
            if self.lease.enabled:
                if not self.lease.may_read():
                    return False
                extra.update(self.lease.headers())
            try:
                ack = self.hub.call("POST", "/agent/v1/blocks", body, extra, timeout=120)
            except HubError as e:
                self.errors += 1
                if e.status == 401 and self._credential_rejected():
                    return False
                if e.status == 423:
                    self.lease.on_lost(f"batch fenced by the hub: {e.body[:200]}")
                elif e.status == 409:
                    log.error("hub rejected batch %s (position gap/overlap): %s", hdr["batch_id"], e.body)
                    self.state = "ERROR"
                elif e.status == 503:
                    log.warning("hub backpressure, keeping spool")
                else:
                    log.error("batch %s refused: %s", hdr["batch_id"], e)
                return False
            except OSError as e:
                self.errors += 1
                log.warning("hub unreachable (%s); spool kept", e)
                return False
            self.reposition = None
            self.durable = ack["durable_pos"]["text"]
            self.sent += 1
            self.spool.release(self.durable)
            log.info("ACK %s durable=%s dup=%s", hdr["batch_id"], self.durable, ack.get("duplicate"))
        return True

    # ---------------------------------------------------------------- control
    HEAVY = ("SNAPSHOT_TABLE", "VERIFY_TABLE", "PROBE", "SNAPSHOT_CATALOG")

    def poll_commands(self, inline: bool = False) -> None:
        """Fetch commands; light ones (PAUSE, RESUME, SET_LIMITS, START_CAPTURE, SHUTDOWN) run here, heavy ones go to
        the command thread (`inline`: run them here too - `--once` runs and tests)."""
        try:
            aid = urllib.parse.quote(self.cfg.agent_id, safe="")
            cmds = self.hub_call("GET", f"/agent/v1/commands?agent_id={aid}&wait=0")
        except (HubError, OSError) as e:
            log.warning("command poll failed: %s", e)
            return
        if isinstance(cmds, dict):
            cmds = cmds.get("commands") or []
        if not isinstance(cmds, list):
            return
        now = time.monotonic()
        for k in [k for k, t in self.cmd_seen.items() if now - t > 3600]:
            self.cmd_seen.pop(k, None)
        for c in cmds:
            if not isinstance(c, dict):
                continue
            cid = str(c.get("command_id") or "")
            if not cid or len(cid) > 64 or not all(ch in "0123456789abcdefABCDEF-" for ch in cid):
                log.warning("command with an invalid command_id ignored")
                continue
            name, args = str(c.get("name")), c.get("args") if isinstance(c.get("args"), dict) else {}
            if cid in self.cmd_seen:
                continue                                  # re-delivery of a command already queued / done
            self.cmd_seen[cid] = now
            if name in self.HEAVY and not inline:
                try:
                    self.cmdq.put_nowait((cid, name, args))
                except queue.Full:
                    self.cmd_seen.pop(cid, None)          # delivered again later
                continue
            self.execute(cid, name, args)

    def execute(self, cid: str, name: str, args: Dict[str, Any]) -> None:
        ok, result = True, {}
        try:
            if name == "PAUSE":
                self.paused = True
            elif name == "RESUME":
                self.paused = False
            elif name == "SET_LIMITS":           # LNX-26 / XC-15: typed, bounded like the C agent
                self.limits = clamp_limits(self.limits, args, None)
                result = {"limits": self.limits}
            elif name == "SNAPSHOT_CATALOG":
                self.send_catalog(self.connect_cmd() if threading.current_thread() is self.cmd_thread else None)
            elif name == "PROBE":
                result = self.probe(str(args.get("name")))
            elif name == "START_CAPTURE":
                fp = args.get("from_pos")
                if isinstance(fp, dict):
                    fp = fp.get("text")
                if not fp:                       # XC-17: {} = resume (spool > hub > state > config)
                    result = {"message": f"resuming at {self.cursor or self.batch_from}"}
                else:
                    pos = str(fp)
                    lri_key(pos)
                    if self.spool.files():
                        if args.get("discard_spool") is not True:
                            raise RuntimeError("spool not empty; cannot reposition (discard_spool: true "
                                               "discards the unshipped batches)")
                        for f in self.spool.files():
                            f.unlink()
                        log.warning("START_CAPTURE %s: unshipped spool discarded by operator request", cid)
                    self.cursor = self.batch_from = pos
                    self.reposition = cid
            elif name == "SHUTDOWN":
                self.stop = True
            elif name == "VERIFY_TABLE":
                result = self.verify(args)
            elif name == "SNAPSHOT_TABLE":
                result = self.snapshot(args)
            else:
                ok, result = False, {"code": "UNSUPPORTED", "message": f"{name} not supported by devedge"}
        except Exception as e:  # noqa: BLE001
            ok, result = False, {"code": error_code(e), "message": f"{type(e).__name__}: {e}"}
        log.info("command %s -> ok=%s %s", name, ok, str(result)[:300])
        try:
            self.hub_post_json(f"/agent/v1/commands/{urllib.parse.quote(cid, safe='')}/ack"
                               f"?agent_id={urllib.parse.quote(self.cfg.agent_id, safe='')}",
                               {"ok": ok, "result": result})
        except (HubError, OSError) as e:
            log.warning("ack failed: %s", e)

    def command_loop(self) -> None:
        """Command thread: serial FIFO, own Db2 connection, governor applied after every command."""
        while not self.stop:
            try:
                cid, name, args = self.cmdq.get(timeout=0.5)
            except queue.Empty:
                continue
            t0 = time.process_time()
            if self.paused and name in ("SNAPSHOT_TABLE", "VERIFY_TABLE"):
                self.ack_error(cid, "PAUSED", "agent paused by the governor / operator: retry later")
                continue
            try:
                self.execute(cid, name, args)
            except Exception:  # noqa: BLE001 - the thread never dies
                log.exception("command %s failed", name)
            self.governor_sleep(time.process_time() - t0)

    def ack_error(self, cid: str, code: str, message: str) -> None:
        try:
            self.hub_post_json(f"/agent/v1/commands/{urllib.parse.quote(cid, safe='')}/ack"
                               f"?agent_id={urllib.parse.quote(self.cfg.agent_id, safe='')}",
                               {"ok": False, "result": {"code": code, "message": message}})
        except (HubError, OSError) as e:
            log.warning("ack failed: %s", e)

    def hub_call(self, method: str, path: str, body: Optional[bytes] = None,
                 headers: Optional[Dict[str, str]] = None, timeout: Optional[float] = None) -> Any:
        if timeout is None:                               # urllib requests are independent: thread-safe
            return self.hub.call(method, path, body, headers)
        return self.hub.call(method, path, body, headers, timeout=timeout)

    def hub_post_json(self, path: str, obj: Any) -> Any:
        return self.hub.post_json(path, obj)

    def _db_for_command(self) -> Db2:
        return self.connect_cmd() if threading.current_thread() is self.cmd_thread else self.connect()

    def probe(self, name: str) -> Dict[str, Any]:
        return self._db_for_command().probe(name)

    def snapshot(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """SNAPSHOT_TABLE{table, snapshot_id, chunk_rows[, key_from, chunk_from]}: allow-listed tables only;
        keyset-paged SELECT ... WITH CS (alelt_db2edge.verify.snapshot_chunks, = the C plugin); chunks POSTed to
        /agent/v1/snapshot (text values, format jsonl, zlib), header start_pos = the log position read before the first
        chunk, last_key = key of the chunk's last row (a re-issued command resumes after it: key_from)."""
        import uuid
        import zlib

        t = args.get("table") or {}
        schema, name = str(t.get("schema") or ""), str(t.get("name") or "")
        if not table_allowed(self.cfg.allowed_tables, schema, name):
            raise PermissionError(f"table {schema}.{name} not in allowed_tables")
        sid = str(args.get("snapshot_id") or uuid.uuid4())
        uuid.UUID(sid)
        kf = args.get("key_from")
        if kf is not None and not (isinstance(kf, list) and all(isinstance(x, (str, int, float)) for x in kf)):
            raise ValueError("key_from must be a list of key values")
        n = int(args.get("chunk_from") or 0)
        db = self._db_for_command()
        db.commit()
        start = db.query_lri()[1]
        rows = 0
        for chunk, key_from, last, last_key in snapshot_chunks(db, schema, name, int(args.get("chunk_rows") or 50_000),
                                                               key_from=kf):
            t0 = time.process_time()
            body = zlib.compress(b"".join(json.dumps(r, ensure_ascii=False).encode() + b"\n" for r in chunk))
            hdr = {"snapshot_id": sid, "table": {"schema": schema, "name": name}, "chunk_no": n,
                   "key_from": key_from or None, "last_key": last_key, "rows": len(chunk), "last": bool(last),
                   "start_pos": {"engine": "db2", "text": start}, "codec": "zlib", "format": "jsonl"}
            self.hub_call("POST", "/agent/v1/snapshot", body, {
                "X-ALELT-Snapshot": json.dumps(hdr), "X-ALELT-Agent-Id": self.cfg.agent_id,
                "Content-Type": "application/octet-stream", **(self.lease.headers() if self.lease.enabled else {})},
                timeout=120)
            n += 1
            rows += len(chunk)
            self.governor_sleep(time.process_time() - t0)
            if self.stop:
                raise TimeoutError("agent stopping: snapshot interrupted (resumed by the hub with key_from)")
            if last:
                break
        log.info("snapshot %s.%s: %d rows in %d chunk(s), start %s", schema, name, rows, n, start)
        return {"snapshot_id": sid, "chunks": n, "rows": rows, "start_pos": start}

    def verify(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """VERIFY_TABLE: allow-listed tables only; compiled-in SELECT ... WITH CS (alelt_db2edge.verify)."""
        t = args.get("table") or {}
        fq = f"{t.get('schema')}.{t.get('name')}".upper()
        if not table_allowed(self.cfg.allowed_tables, str(t.get("schema") or ""), str(t.get("name") or "")):
            raise PermissionError(f"table {fq} not in allowed_tables")
        db = self._db_for_command()

        def pos() -> Optional[str]:
            db.commit()
            return db.query_lri()[1]
        return verify_table(db, args, pos)

    def heartbeat(self) -> None:
        cpu, wall = time.process_time(), time.monotonic()
        dt = wall - self._cpu0[1]
        if dt > 0:
            self._cpu_pct = 100.0 * (cpu - self._cpu0[0]) / dt
        self._cpu0 = (cpu, wall)
        hb = {"agent_id": self.cfg.agent_id, "state": self.state,
              "last_read_pos": {"engine": "db2", "text": self.last_read} if self.last_read else None,
              "durable_pos": {"engine": "db2", "text": self.durable} if self.durable else None,
              "spool_bytes": self.spool.bytes(), "cpu_pct": round(self._cpu_pct, 2), "mem_mb": round(_mem_mb(), 1),
              "batches_sent": self.sent, "errors": self.errors, "at": datetime.now(timezone.utc).isoformat()}
        try:
            self.hub_post_json("/agent/v1/heartbeat", hb)
        except (HubError, OSError) as e:
            log.warning("heartbeat failed: %s", e)

    def governor_sleep(self, busy_s: float) -> None:
        """Keep CPU under limits.cpu_pct of one core: sleep proportionally to the work just done."""
        pct = max(1, min(100, int(self.limits.get("cpu_pct", 100))))
        if pct < 100 and busy_s > 0:
            time.sleep(busy_s * (100 - pct) / pct)

    # ---------------------------------------------------------------- main loop
    def run(self, once: bool = False) -> int:
        resume = self.register()
        self.send_catalog()
        if not once:
            self.cmd_thread = threading.Thread(target=self.command_loop, name="commands", daemon=True)
            self.cmd_thread.start()
        if self.lease.enabled:
            self.state = "STANDBY"
            log.info("ha: active/passive (ttl %ss, renew %ss): STANDBY until the hub grants the capture lease",
                     self.lease.state.ttl_s, self.lease.state.renew_s)
        else:
            self.cursor = self.batch_from = self.initial_position(resume)
            log.info("capture starts at %s (hub resume=%s)", self.cursor, resume)
            self.state = "CAPTURING"
        last_hb = last_cmd = 0.0
        total = 0
        while not self.stop:
            now = time.monotonic()
            if now - last_cmd >= self.cfg.command_poll_s:
                self.poll_commands(inline=once)
                self.maybe_rotate()
                last_cmd = now
            if not self.ha_step(resume):           # standby: keep heartbeats + commands (PAUSE is kept)
                if self.state != "STANDBY":
                    self.state = "STANDBY"
                    self.heartbeat()
                    last_hb = now
                elif now - last_hb >= self.cfg.heartbeat_s:
                    self.heartbeat()
                    last_hb = now
                time.sleep(max(0.05, min(1.0, self.lease.wait_s())))
                continue
            if now - last_hb >= self.cfg.heartbeat_s:
                self.heartbeat()
                last_hb = now
            sent_ok = self.flush_spool()
            if self.state == "ERROR":
                time.sleep(5)
                self.state = "CAPTURING"
                continue
            if self.paused:
                self.state = "PAUSED"
                time.sleep(1)
                continue
            if self.spool.full():
                self.state = "SPOOL_FULL"             # backpressure: stop reading, never drop
                time.sleep(2)
                continue
            self.state = "CAPTURING"
            t0 = time.process_time()
            try:
                recs, code = self.read_once()
            except Db2Error as e:
                self.errors += 1
                log.error("read failed: %s", e)
                self.db = None
                time.sleep(5)
                continue
            total += recs
            self.governor_sleep(time.process_time() - t0)
            at_end = code == RL_READ_TO_CURRENT or recs == 0
            if at_end:
                if once:
                    self.flush_spool()
                    break
                if sent_ok:
                    time.sleep(max(0.05, self.limits.get("poll_ms", 1000) / 1000))
        if self.lease.release():
            log.info("ha: capture lease released: the standby may take over now")
        self.heartbeat()
        self.stop = True
        if self.cmd_thread is not None:
            self.cmd_thread.join(timeout=30)
        if self.cmd_db is not None:
            self.cmd_db.close()
        if self.db is not None:
            self.db.close()
        log.info("stopped: %d records read, %d batches acked, spool=%d files", total, self.sent,
                 len(self.spool.files()))
        return 0 if not self.spool.files() else 2


def error_code(e: BaseException) -> str:
    """Typed command error codes (docs/AGENT-COMMANDS.md): the hub branches on them (refused vs retry later)."""
    msg = str(e).lower()
    if isinstance(e, PermissionError):
        return "NOT_ALLOWED"
    if isinstance(e, LookupError) or "not a base table" in msg or "not found" in msg:
        return "NOT_FOUND"
    if "no primary key" in msg or "without a primary key" in msg:
        return "NO_KEY"
    if isinstance(e, TimeoutError):
        return "TIMEOUT"
    if "too large" in msg or "larger than" in msg:
        return "TOO_LARGE"
    if isinstance(e, NotImplementedError):
        return "UNSUPPORTED"
    return "SOURCE_ERROR"


def main() -> int:
    ap = argparse.ArgumentParser(description="ALELT dev edge agent (Db2, stdlib)")
    ap.add_argument("--config", default="devedge.json")
    ap.add_argument("--once", action="store_true", help="read to the current end of the log, ship, exit")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format='{"ts":"%(asctime)s","level":"%(levelname)s","component":"devedge","msg":%(message)r}')
    return Agent(Config.load(args.config)).run(once=args.once)


if __name__ == "__main__":
    sys.exit(main())
