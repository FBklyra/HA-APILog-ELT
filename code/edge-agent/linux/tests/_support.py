"""Test support: a stdlib fake hub (protocol v1 subset with the real chain / lease rules) and config builders.
Not the real hub - the e2e tests use hub/alelt_hub."""
from __future__ import annotations

import hashlib
import json
import os
import sys
import threading
import uuid
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional

HERE = Path(__file__).resolve().parent
LINUX = HERE.parent
REPO = LINUX.parents[1]
for p in (LINUX, REPO / "hub", REPO / "connectors" / "sources" / "db2" / "devedge"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))
PYTHONPATH = os.pathsep.join(str(p) for p in (LINUX, REPO / "hub", REPO / "connectors/sources/db2/devedge"))
TOKEN = "test-token"


class FakeHub:
    def __init__(self, lease: bool = False) -> None:
        self.lock = threading.Lock()
        self.batches: List[Dict[str, Any]] = []
        self.catalogs = 0
        self.heartbeats: List[Dict[str, Any]] = []
        self.registers = 0
        self.commands: List[Dict[str, Any]] = []
        self.acks: List[Dict[str, Any]] = []
        self.rejects: List[int] = []
        self.lease_on = lease
        self.lease: Dict[str, Any] = {}
        self.released = 0
        hub = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a: Any) -> None:
                pass

            def _send(self, code: int, obj: Any = None) -> None:
                data = json.dumps(obj).encode() if obj is not None else b""
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:
                if self.headers.get("X-ALELT-Agent-Token") != TOKEN:
                    return self._send(401, {"detail": "bad token"})
                with hub.lock:
                    cmds, hub.commands = hub.commands, []
                self._send(200, cmds)

            def do_POST(self) -> None:
                if self.headers.get("X-ALELT-Agent-Token") != TOKEN or self.headers.get("X-ALELT-Protocol") != "1":
                    return self._send(401, {"detail": "bad token"})
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                p = self.path.split("?")[0]
                code, obj = hub.handle(p, body, self.headers)
                self._send(code, obj)

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.srv.shutdown()
        self.srv.server_close()

    def last_next(self) -> Optional[str]:
        return self.batches[-1]["next_pos"]["text"] if self.batches else None

    def handle(self, p: str, body: bytes, headers: Any) -> "tuple[int, Any]":
        with self.lock:
            if p == "/agent/v1/register":
                self.registers += 1
                nxt = self.last_next()
                return 200, {"accepted": True, "resume_from": {"engine": "x", "text": nxt} if nxt else None,
                             "limits": None}
            if p == "/agent/v1/heartbeat":
                self.heartbeats.append(json.loads(body))
                return 200, {"ok": True}
            if p == "/agent/v1/catalog":
                json.loads(body)
                self.catalogs += 1
                return 200, {"version": self.catalogs, "created": True, "tables": 1}
            if p == "/agent/v1/events":
                return 200, {"ok": True}
            if p.startswith("/agent/v1/commands/"):
                self.acks.append(json.loads(body))
                return 200, {"ok": True}
            if p.startswith("/agent/v1/lease/"):
                return self._lease(p.rsplit("/", 1)[1], json.loads(body))
            if p == "/agent/v1/blocks":
                hdr = json.loads(headers["X-ALELT-Batch"])
                if hdr["codec"] == "zstd":
                    import zstandard
                    raw = zstandard.ZstdDecompressor().decompress(body, max_output_size=1 << 28)
                else:
                    raw = zlib.decompress(body) if hdr["codec"] == "zlib" else body
                if hashlib.sha256(raw).hexdigest() != hdr["sha256"] or len(raw) != hdr["raw_bytes"]:
                    self.rejects.append(422)
                    return 422, {"detail": "sha"}
                if any(b["batch_id"] == hdr["batch_id"] for b in self.batches):
                    return 200, {"batch_id": hdr["batch_id"], "durable_pos": hdr["to_pos"], "duplicate": True}
                if self.lease_on and self.lease and str(self.lease.get("epoch")) != headers.get("X-ALELT-Lease-Epoch"):
                    self.rejects.append(423)
                    return 423, {"detail": {"error": "lease_fenced"}}
                nxt = self.last_next()
                if nxt is not None and hdr["from_pos"]["text"] != nxt and not headers.get("X-ALELT-Reposition"):
                    self.rejects.append(409)
                    return 409, {"detail": {"error": "position gap/overlap", "expected_from": nxt}}
                hdr["_raw"] = raw
                self.batches.append(hdr)
                return 200, {"batch_id": hdr["batch_id"], "durable_pos": hdr["to_pos"], "duplicate": False}
            return 404, {"detail": "not found"}

    def _lease(self, op: str, d: Dict[str, Any]) -> "tuple[int, Any]":
        import time
        now, L = time.time(), self.lease
        alive = L.get("holder") and L.get("expires", 0) > now
        if op == "acquire":
            if alive and L["holder"] != d["agent_id"]:
                return 200, {"granted": False, "connection_id": d["connection_id"], "retry_after_s": 1.0}
            takeover = bool(L.get("holder")) and L["holder"] != d["agent_id"]
            if L.get("holder") != d["agent_id"] or not L.get("lease_id"):
                L.update(epoch=int(L.get("epoch", 0)) + 1, lease_id=str(uuid.uuid4()))
            L.update(holder=d["agent_id"], expires=now + d.get("ttl_s", 30))
            nxt = self.last_next()
            return 200, {"granted": True, "connection_id": d["connection_id"], "holder": L["holder"],
                         "lease_id": L["lease_id"], "epoch": L["epoch"], "ttl_s": d.get("ttl_s", 30),
                         "takeover": takeover, "resume_from": {"engine": "x", "text": nxt} if nxt else None}
        if op == "renew":
            if L.get("holder") != d["agent_id"] or L.get("epoch") != d.get("epoch"):
                return 409, {"detail": {"error": "lease_lost"}}
            L["expires"] = now + d.get("ttl_s", 30)
            return 200, {"granted": True, "connection_id": d["connection_id"], "holder": L["holder"],
                         "lease_id": L["lease_id"], "epoch": L["epoch"], "ttl_s": d.get("ttl_s", 30)}
        if L.get("holder") == d["agent_id"]:
            L["expires"] = 0
            self.released += 1
            return 200, {"released": True}
        return 200, {"released": False}


def fake_config(tmp: Path, hub_url: str, **over: Any) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "agent_id": "lx-test", "connection_id": "fake1",
        "hub": {"url": hub_url, "mode": "token", "token_env": "ALELT_AGENT_TOKEN", "heartbeat_s": 1,
                "request_timeout_ms": 5000, "backoff_max_s": 2},
        "spool": {"dir": str(tmp / "spool"), "max_mb": 64},
        "limits": {"batch_kb": 64, "poll_ms": 100},
        "source": {"engine": "fake", "settings": {"rate_tps": 50, "rows_per_txn": 2, "idle_tick_s": 0.05}},
        "capture": {"autostart": True, "batch_ms": 100},
        "governor": {"enabled": False},
        "require_job_limits": False,
        "shutdown": {"deadline_s": 20, "drain_s": 5},
    }
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(cfg.get(k), dict):
            cfg[k] = {**cfg[k], **v}
        else:
            cfg[k] = v
    return cfg


def write_config(tmp: Path, cfg: Dict[str, Any]) -> Path:
    p = tmp / "aleltagent.json"
    p.write_text(json.dumps(cfg), encoding="utf-8")
    return p


def child_env() -> Dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k not in ("NOTIFY_SOCKET", "WATCHDOG_USEC", "JOURNAL_STREAM")}
    env.update(PYTHONPATH=PYTHONPATH, ALELT_AGENT_TOKEN=TOKEN, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
    return env


def txn_numbers(hub: FakeHub) -> List[int]:
    """Committed fake transaction numbers in hub order (to check no gap / no duplicate)."""
    out: List[int] = []
    for b in hub.batches:
        for line in b["_raw"].splitlines():
            ev = json.loads(line)
            if ev["kind"] == "COMMIT":
                out.append(int(ev["pos"]) // 1000)
    return out
