"""Minimal dev hub for exercising aleltagent.exe --console (protocol v1, dev mode).
The real hub is hub/alelt_hub; this one needs no Postgres.

Validates every agent payload against hub/alelt_common/models.py (pydantic, extra="forbid"),
verifies batch sha256 / raw_bytes / Db2 filter-ON framing, ACKs durable_pos = to_pos,
and serves scripted commands. NOT the real hub - test harness only.

  python mock_hub.py --port 18080 --token devtoken --out <dir> [--script cmds.json]
  POST /admin/command  (JSON body = Command args: {"name":..,"args":{..}})  to enqueue more.
  POST /admin/lease/expire  force the capture lease to expire (simulated failover)

Capture lease (X-47, active/passive agents): POST /agent/v1/lease/acquire|renew|release with the
real hub semantics (epoch +1 and takeover:true only when the holder changes, renew by a non-holder
-> 409 lease_lost). Once any agent took a lease, /blocks needs X-ALELT-Lease-Epoch from the holder,
else 423 lease_fenced.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import struct
import sys
import threading
import time
import uuid
import zlib
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "hub"))
from alelt_common.models import (  # noqa: E402
    AgentEvent, BatchAck, BatchHeader, CatalogSnapshot, Command, Heartbeat, RegisterRequest, RegisterResponse,
    SnapshotChunkHeader,
)

LOCK = threading.Lock()
QUEUE: list[dict] = []
STATS = {"batches": 0, "records": 0, "bytes": 0, "heartbeats": 0, "rejects": 0, "acks_ok": 0, "registers": 0,
         "hb_ts": [], "events": [], "batch_log": [], "chunk_log": [], "ack_log": [], "event_log": []}
ARGS = None
LEASE: dict = {}          # {holder, epoch, lease_id, expires} once an agent acquired (HA mode)
LAST_NEXT: dict = {}      # last acked batch next_pos -> resume_from on takeover
LEASE_TTL_MAX = 300
TOKENS: dict = {}         # accepted agent tokens -> kind ("static" | "enrollment" | "credential"), expiry (REQ-26)
GAP: dict = {}            # /admin/gap: {"expected_from": pos, "count": n} -> next n batches answer 409 (AHS-05)
CRED = {"issued": 0, "rotated": 0, "used": 0, "enroll_after_claim": 0}


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def enqueue(name: str, args: dict | None = None, raw: dict | None = None) -> str:
    cmd = raw or {"command_id": str(uuid.uuid4()), "name": name, "args": args or {}, "issued_at": now()}
    with LOCK:
        QUEUE.append(cmd)
    return cmd["command_id"]


def walk_db2(raw: bytes) -> int:
    """Count records in a filter-ON db2ReadLog buffer; raises on bad framing (DESIGN V3/V4)."""
    off = n = 0
    while off < len(raw):
        if len(raw) - off < 60:
            raise ValueError(f"truncated filter header at {off}")
        size = struct.unpack_from("<I", raw, off + 56)[0]
        if size < 20 or off + 56 + size > len(raw):
            raise ValueError(f"bad record size {size} at {off}")
        off += 56 + size
        n += 1
    return n


def lease_grant(granted: bool, takeover: bool = False) -> dict:
    exp = LEASE.get("expires", 0.0)
    return {"granted": granted, "connection_id": LEASE.get("connection_id"), "holder": LEASE.get("holder"),
            "lease_id": LEASE.get("lease_id"), "epoch": LEASE.get("epoch", 0), "ttl_s": LEASE.get("ttl_s", 30),
            "expires_at": datetime.fromtimestamp(exp, timezone.utc).isoformat() if exp else None,
            "takeover": takeover,
            "resume_from": {"engine": "db2", "text": LAST_NEXT["pos"]} if LAST_NEXT.get("pos") else None,
            "retry_after_s": max(0.5, min(5.0, exp - time.time())) if not granted else 0}


def lease_op(op: str, d: dict) -> tuple[int, dict]:
    assert set(d) <= {"agent_id", "connection_id", "ttl_s", "lease_id", "epoch"}, f"bad LeaseRequest keys {set(d)}"
    me, ttl = d["agent_id"], max(5, min(LEASE_TTL_MAX, int(d.get("ttl_s") or 30)))
    t = time.time()
    with LOCK:
        live = LEASE and LEASE["expires"] > t
        if op == "acquire":
            if live and LEASE["holder"] != me:
                return 200, lease_grant(False)
            takeover = bool(LEASE) and LEASE["holder"] != me
            if not LEASE or takeover:
                LEASE.update(holder=me, epoch=LEASE.get("epoch", 0) + 1, lease_id=str(uuid.uuid4()),
                             connection_id=d["connection_id"])
                takeover = takeover or False
            LEASE.update(expires=t + ttl, ttl_s=ttl)
            return 200, lease_grant(True, takeover)
        if op == "renew":
            if not LEASE or LEASE["holder"] != me or d.get("epoch") != LEASE["epoch"] \
                    or d.get("lease_id") != LEASE["lease_id"]:
                return 409, {"error": "lease_lost", "holder": LEASE.get("holder"), "epoch": LEASE.get("epoch")}
            LEASE.update(expires=t + ttl, ttl_s=ttl)
            return 200, lease_grant(True)
        if LEASE and LEASE["holder"] == me and d.get("epoch") == LEASE["epoch"]:
            LEASE["expires"] = t
            return 200, {"released": True}
        return 200, {"released": False}


class H(BaseHTTPRequestHandler):
    def log_message(self, fmt, *a):  # quiet
        pass

    def _send(self, code: int, obj=None):
        body = b"" if obj is None else json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _auth(self) -> bool:
        if self.headers.get("X-ALELT-Protocol") != "1":
            self._send(400, {"detail": "X-ALELT-Protocol: 1 required"})
            return False
        tok = self.headers.get("X-ALELT-Agent-Token")
        with LOCK:
            t = TOKENS.get(tok)
            if t and t.get("until") and time.time() > t["until"]:
                t = None
            if t and t["kind"] == "credential":
                CRED["used"] += 1
            if t and t["kind"] == "enrollment" and t.get("claimed"):
                CRED["enroll_after_claim"] += 1
        if ARGS.token and not t:
            self._send(401, {"detail": "bad token"})
            return False
        self.alelt_token = tok
        return True

    def _body(self) -> bytes:
        n = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(n) if n else b""

    def do_GET(self):
        if not self._auth():
            return
        if self.path.startswith("/agent/v1/commands"):
            with LOCK:
                out = QUEUE[:]
                QUEUE.clear()
            if not out:
                time.sleep(1.0)
                self._send(204)
                return
            print(f"[hub] -> {len(out)} command(s): {[c.get('name') for c in out]}", flush=True)
            self._send(200, out)
            return
        self._send(404, {"detail": "not found"})

    def do_POST(self):
        p = self.path
        if p == "/admin/lease/expire":
            with LOCK:
                if LEASE:
                    LEASE["expires"] = time.time()
            self._send(200, {"expired": True})
            return
        if p == "/admin/gap":
            with LOCK:
                GAP.clear()
                GAP.update(json.loads(self._body()))
            self._send(200, {"gap": True})
            return
        if p == "/admin/stats":
            with LOCK:
                out = {"cred": dict(CRED), "batches": STATS["batches"], "events": [e.type for e in STATS["events"]],
                       "heartbeats": STATS["heartbeats"], "hb_ts": STATS["hb_ts"][-2000:], "rejects": STATS["rejects"],
                       "acks_ok": STATS["acks_ok"], "registers": STATS["registers"],
                       # time-stamped logs for the mock-plugin scenarios (additive)
                       "batch_log": STATS["batch_log"][-5000:], "chunk_log": STATS["chunk_log"][-2000:],
                       "ack_log": STATS["ack_log"][-500:], "event_log": STATS["event_log"][-500:]}
            self._send(200, out)
            return
        if p == "/admin/command":
            d = json.loads(self._body())
            cid = enqueue(d["name"], d.get("args"))
            self._send(200, {"queued": True, "command_id": cid})
            return
        if not self._auth():
            return
        body = self._body()
        try:
            if p == "/agent/v1/register":
                r = RegisterRequest.model_validate_json(body)
                STATS["registers"] += 1
                print(f"[hub] register {r.agent_id} {r.engine} db={r.database} v{r.version} host={r.hostname}", flush=True)
                resp = RegisterResponse(accepted=True)
                with LOCK:
                    t = TOKENS.get(self.alelt_token) or {}
                    if t.get("kind") == "enrollment" and "credential.v1" in r.capabilities:
                        cred = "alelta_" + uuid.uuid4().hex
                        TOKENS[cred] = {"kind": "credential", "until": time.time() + ARGS.cred_ttl_s}
                        t["claimed"] = True          # first use of the credential retires it (hub X-136)
                        CRED["issued"] += 1
                        resp = RegisterResponse(accepted=True, credential=cred, credential_expires_at=datetime.fromtimestamp(
                            time.time() + ARGS.cred_ttl_s, timezone.utc))
                self._send(200, json.loads(resp.model_dump_json()))
            elif p == "/agent/v1/credential/rotate":
                with LOCK:
                    t = TOKENS.get(self.alelt_token) or {}
                    if t.get("kind") != "credential":
                        self._send(409, {"detail": "only a per-agent credential can be rotated"})
                        return
                    t["until"] = time.time() + 300            # grace for the old one
                    cred = "alelta_" + uuid.uuid4().hex
                    TOKENS[cred] = {"kind": "credential", "until": time.time() + ARGS.cred_ttl_s}
                    CRED["rotated"] += 1
                exp = datetime.fromtimestamp(time.time() + ARGS.cred_ttl_s, timezone.utc).isoformat()
                print(f"[hub] credential rotated (expires {exp})", flush=True)
                self._send(200, {"credential": cred, "credential_expires_at": exp})
            elif p == "/agent/v1/heartbeat":
                hb = Heartbeat.model_validate_json(body)
                STATS["heartbeats"] += 1
                STATS["hb_ts"].append(time.time())
                print(f"[hub] heartbeat state={hb.state} last={hb.last_read_pos and hb.last_read_pos.text} "
                      f"durable={hb.durable_pos and hb.durable_pos.text} spool={hb.spool_bytes} cpu={hb.cpu_pct} "
                      f"mem={hb.mem_mb} sent={hb.batches_sent} err={hb.errors}", flush=True)
                self._send(200, {})
            elif p.startswith("/agent/v1/lease/") and p.rsplit("/", 1)[1] in ("acquire", "renew", "release"):
                op = p.rsplit("/", 1)[1]
                code, out = lease_op(op, json.loads(body))
                print(f"[hub] lease {op} {json.loads(body).get('agent_id')} -> {code} "
                      f"granted={out.get('granted')} holder={out.get('holder')} epoch={out.get('epoch')} "
                      f"takeover={out.get('takeover')}", flush=True)
                self._send(code, out)
            elif p == "/agent/v1/blocks":
                h = BatchHeader.model_validate_json(self.headers["X-ALELT-Batch"])
                with LOCK:
                    fenced = bool(LEASE) and (h.agent_id != LEASE["holder"]
                                              or self.headers.get("X-ALELT-Lease-Epoch") != str(LEASE["epoch"]))
                    lease_now = dict(LEASE)
                if fenced:
                    print(f"[hub] batch {h.batch_id} from {h.agent_id} epoch={self.headers.get('X-ALELT-Lease-Epoch')} "
                          f"FENCED (holder {lease_now.get('holder')} epoch {lease_now.get('epoch')})", flush=True)
                    self._send(423, {"error": "lease_fenced", "holder": lease_now.get("holder"),
                                     "epoch": lease_now.get("epoch")})
                    return
                with LOCK:
                    gap = dict(GAP) if GAP.get("count", 0) > 0 else None
                    if gap:
                        GAP["count"] -= 1
                if gap:
                    print(f"[hub] batch {h.batch_id} {h.from_pos.text} -> 409 (expected_from {gap['expected_from']})", flush=True)
                    self._send(409, {"detail": {"error": "position gap/overlap", "expected_from": gap["expected_from"],
                                                "got": h.from_pos.text}})
                    return
                raw = zlib.decompress(body) if h.codec == "zlib" else body
                assert len(raw) == h.raw_bytes, "raw_bytes mismatch"
                assert hashlib.sha256(raw).hexdigest() == h.sha256, "sha256 mismatch"
                n = walk_db2(raw)
                assert n == h.rec_count, f"rec_count {h.rec_count} != framed {n}"
                (Path(ARGS.out) / f"batch_{h.batch_id}.bin").write_bytes(raw)
                STATS["batches"] += 1
                STATS["batch_log"].append({"ts": time.time(), "agent": h.agent_id, "from": h.from_pos.text,
                                           "to": h.to_pos.text, "next": h.next_pos.text if h.next_pos else None,
                                           "recs": n})
                STATS["records"] += n
                STATS["bytes"] += len(raw)
                print(f"[hub] batch {h.batch_id} {h.from_pos.text}..{h.to_pos.text} next={h.next_pos.text if h.next_pos else None} "
                      f"recs={n} raw={len(raw)} wire={len(body)} codec={h.codec} OK", flush=True)
                if h.next_pos:
                    LAST_NEXT["pos"] = h.next_pos.text
                ack = BatchAck(batch_id=h.batch_id, durable_pos=h.to_pos)
                self._send(200, json.loads(ack.model_dump_json()))
            elif p == "/agent/v1/catalog":
                c = CatalogSnapshot.model_validate_json(body)
                (Path(ARGS.out) / "catalog.json").write_bytes(body)
                cols = sum(len(t.columns) for t in c.tables)
                print(f"[hub] catalog OK: {len(c.tables)} tables, {cols} columns, codepage={c.database_codepage}, "
                      f"position={c.position and c.position.text}", flush=True)
                self._send(200, {})
            elif p == "/agent/v1/snapshot":
                h = SnapshotChunkHeader.model_validate_json(self.headers["X-ALELT-Snapshot"])
                data = zlib.decompress(body) if h.codec == "zlib" else body
                lines = [json.loads(x) for x in data.decode("utf-8").splitlines() if x]
                assert len(lines) == h.rows, f"rows {h.rows} != {len(lines)}"
                STATS["chunk_log"].append({"ts": time.time(), "table": h.table.fqn, "chunk_no": h.chunk_no,
                                           "rows": h.rows, "last": h.last,
                                           "start_pos": h.start_pos.text if h.start_pos else None})
                with open(Path(ARGS.out) / f"snapshot_{h.snapshot_id}.jsonl", "ab") as f:
                    f.write(data)
                print(f"[hub] snapshot {h.table.fqn} chunk {h.chunk_no} rows={h.rows} last={h.last} "
                      f"key_from={h.key_from} key_to={h.key_to} start={h.start_pos and h.start_pos.text} OK", flush=True)
                self._send(200, {})
            elif p == "/agent/v1/events":
                e = AgentEvent.model_validate_json(body)
                STATS["events"].append(e)
                STATS["event_log"].append({"ts": time.time(), "type": e.type, "detail": e.detail})
                print(f"[hub] event {e.type} cmd={e.command_id} detail={json.dumps(e.detail)[:300]}", flush=True)
                self._send(200, {"ok": True})
            elif p.startswith("/agent/v1/commands/") and "/ack" in p:
                cid = p.split("/")[4]
                uuid.UUID(cid)
                a = json.loads(body)
                assert set(a) <= {"ok", "result"} and isinstance(a.get("ok"), bool), "bad CommandAck"
                STATS["ack_log"].append({"ts": time.time(), "id": cid, "ok": a["ok"], "result": a.get("result")})
                if not a["ok"]:
                    STATS["rejects"] += 1
                else:
                    STATS["acks_ok"] += 1
                print(f"[hub] ack {cid} ok={a['ok']} result={json.dumps(a.get('result'))[:300]}", flush=True)
                self._send(200, {"ok": True})
            else:
                self._send(404, {"detail": "not found"})
        except Exception as ex:  # validation failure = protocol bug
            print(f"[hub] !! {p}: {type(ex).__name__}: {ex}", flush=True)
            self._send(422, {"detail": str(ex)[:500]})


def main() -> None:
    global ARGS
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=18080)
    ap.add_argument("--token", default="devtoken")
    ap.add_argument("--out", required=True)
    ap.add_argument("--script", help="JSON list of {name,args} (or full Command objects) queued at start")
    ap.add_argument("--enroll", action="store_true", help="--token is an enrollment token: register with "
                    "credential.v1 returns a per-agent alelta_ credential (REQ-26)")
    ap.add_argument("--cred-ttl-s", type=int, default=90 * 86400)
    ARGS = ap.parse_args()
    if ARGS.token:
        TOKENS[ARGS.token] = {"kind": "enrollment" if ARGS.enroll else "static"}
    Path(ARGS.out).mkdir(parents=True, exist_ok=True)
    if ARGS.script:
        for c in json.loads(Path(ARGS.script).read_text()):
            if "command_id" in c:
                enqueue("", raw=c)
            else:
                enqueue(c["name"], c.get("args"))
    srv = ThreadingHTTPServer(("127.0.0.1", ARGS.port), H)
    print(f"[hub] mock hub on http://127.0.0.1:{ARGS.port} (dev token required)", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
