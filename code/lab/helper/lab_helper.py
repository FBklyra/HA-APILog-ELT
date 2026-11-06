# SPDX-License-Identifier: Apache-2.0
"""ALELT lab helper: tiny host-side HTTP service so the console "Local test lab" screen can pull / start / stop /
reset lab profiles. Python 3.10+ stdlib only.

  python lab/helper/lab_helper.py [--port 47999] [--console http://localhost:8080] [--ttl-hours 8]

Safety model
- Binds 127.0.0.1 only (never 0.0.0.0); rejects requests whose Host header is not localhost (DNS rebinding).
- Random token per start (printed once); every call needs header X-Lab-Token; constant-time compare; expires (TTL).
- CORS only for the console origin(s) given with --console (default http://localhost:8080 and :5173 dev server).
- Whitelisted actions only (pull / up / down / reset) on catalog profile ids; never runs arbitrary commands, never
  touches containers outside the compose project "alelt-lab". The hub container never gets the Docker socket.

API (JSON): GET /lab/v1/catalog · GET /lab/v1/status · POST /lab/v1/{pull,up,down,reset} {profiles: [...]}
            · GET /lab/v1/ops/<op_id>
"""
from __future__ import annotations

import argparse
import hmac
import json
import os
import secrets
import subprocess
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import quote

VERSION = "1"
LAB = Path(__file__).resolve().parents[1]
COMPOSE = LAB / "docker-compose.lab.yml"
PROJECT = "alelt-lab"
sys.path.insert(0, str(LAB))
import images  # noqa: E402 - lab/images.py: local image choice (catalog local_fallbacks, lab/.env.local)
SQLSERVER_INIT = ["docker", "exec", "alelt-lab-sqlserver", "/opt/mssql-tools18/bin/sqlcmd", "-C", "-S", "localhost",
                  "-U", "sa", "-P", "Lab_local_only1", "-b", "-i", "/lab-init/10-shop.sql"]


def load_catalog() -> dict[str, Any]:
    return json.loads((LAB / "catalog.json").read_text(encoding="utf-8"))


class Op:
    def __init__(self, action: str, profiles: list[str]) -> None:
        self.id = uuid.uuid4().hex[:12]
        self.action, self.profiles = action, profiles
        self.state, self.exit_code = "running", None
        self.log: list[str] = []
        self.lock = threading.Lock()

    def add(self, line: str) -> None:
        with self.lock:
            self.log.append(line.rstrip()[:500])
            del self.log[:-400]

    def view(self) -> dict[str, Any]:
        with self.lock:
            return {"op_id": self.id, "action": self.action, "profiles": self.profiles, "state": self.state,
                    "exit_code": self.exit_code, "log": list(self.log)}


class Lab:
    def __init__(self, timeout_s: int = 1800, retries: int = 3) -> None:
        self.catalog = load_catalog()
        self.by_id = {p["id"]: p for p in self.catalog["profiles"]}
        self.ops: dict[str, Op] = {}
        self.busy = threading.Lock()          # one docker operation at a time
        self.timeout_s, self.retries = timeout_s, retries

    # ------------------------------------------------------------------ helpers
    def select(self, ids: Any) -> list[dict[str, Any]]:
        if not isinstance(ids, list) or not ids or not all(isinstance(i, str) for i in ids):
            raise ValueError("profiles: non-empty list of profile ids")
        out = []
        for i in ids:
            p = self.by_id.get(i)
            if p is None:
                raise ValueError(f"unknown profile {i!r}")
            if p["availability"] != "image":
                raise ValueError(f"{i!r} has no local image (guidance only)")
            if p.get("optional") and os.environ.get("LAB_DB2_CONTAINER") != "1":
                raise ValueError(f"{i!r} is optional and not started here: {p.get('native', '')}")
            out.append(p)
        return out

    def run(self, op: Op, cmd: list[str], env: dict[str, str] | None = None, timeout: int | None = None) -> int:
        op.add("$ " + " ".join(cmd))
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                    env={**os.environ, **(env or {})}, encoding="utf-8", errors="replace")
        except FileNotFoundError:
            op.add("docker not found on PATH")
            return 127
        deadline = time.monotonic() + (timeout or self.timeout_s)
        assert proc.stdout is not None
        killer = threading.Timer(max(1.0, deadline - time.monotonic()), proc.kill)
        killer.start()
        try:
            for line in proc.stdout:
                op.add(line)
            return proc.wait()
        finally:
            killer.cancel()

    def compose(self, op: Op, profiles: list[str], *args: str, extra: dict[str, str] | None = None) -> int:
        return self.run(op, ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE), *args],
                        env={**(extra or {}), "COMPOSE_PROFILES": ",".join(profiles)})

    # ------------------------------------------------------------------ actions
    def start(self, action: str, ids: Any) -> Op:
        sel = self.select(ids)
        op = Op(action, [p["id"] for p in sel])
        self.ops[op.id] = op
        threading.Thread(target=self._do, args=(op, sel), daemon=True).start()
        return op

    def _do(self, op: Op, sel: list[dict[str, Any]]) -> None:
        with self.busy:
            try:
                rc = getattr(self, "_" + op.action)(op, sel)
            except Exception as e:  # noqa: BLE001 - reported to the console
                op.add(f"error: {type(e).__name__}: {e}")
                rc = 1
            op.exit_code, op.state = rc, ("done" if rc == 0 else "failed")

    def _pull(self, op: Op, sel: list[dict[str, Any]]) -> int:
        rc = 0
        _, notes, need = images.resolve(sel)      # pull only what has no compatible local tag
        for n in notes:
            op.add(f"image: {n}")
        if not need:
            op.add("all images (or compatible local tags) present - nothing to pull")
        for img in need:
            p = next((p for p in sel if img in p.get("images", [])), None)
            plat = [] if p is None or "arm64" in p.get("arch", []) else ["--platform", "linux/amd64"]
            ok = False
            for i in range(1, self.retries + 1):
                op.add(f"pull {img} (try {i}/{self.retries})")
                if self.run(op, ["docker", "pull", *plat, img]) == 0:
                    ok = True
                    break
                time.sleep(5 * i)
            rc = rc or (0 if ok else 1)
        return rc

    def _up(self, op: Op, sel: list[dict[str, Any]]) -> int:
        ids = [p["id"] for p in sel if p.get("services")]
        chosen, notes, _ = images.resolve(sel)
        for n in notes:
            op.add(f"image: {n}")
        rc = self.compose(op, ids, "up", "-d", "--wait", "--wait-timeout", "900", extra=chosen) if ids else 0
        if rc == 0 and any(p["id"] == "sqlserver" for p in sel):
            has_sqlcmd = subprocess.run(["docker", "exec", "alelt-lab-sqlserver", "test", "-x", SQLSERVER_INIT[3]],
                                        capture_output=True, timeout=30).returncode == 0
            rc = self.run(op, SQLSERVER_INIT if has_sqlcmd else [sys.executable, str(LAB / "sqlserver_init.py")])
        if any(p["id"] == "files" for p in sel):
            (LAB / "data").mkdir(exist_ok=True)
        return rc

    def _down(self, op: Op, sel: list[dict[str, Any]]) -> int:
        ids = [p["id"] for p in sel if p.get("services")]
        return self.compose(op, ids, "rm", "-s", "-f") if ids else 0

    def _reset(self, op: Op, sel: list[dict[str, Any]]) -> int:
        rc = self._down(op, sel)
        for p in sel:
            for svc in p.get("services", []):
                if subprocess.run(["docker", "volume", "inspect", svc], capture_output=True).returncode == 0:
                    rc = rc or self.run(op, ["docker", "volume", "rm", svc])
            if p["id"] == "files":
                for f in (LAB / "data").glob("landing.*"):
                    f.unlink()
                    op.add(f"removed {f.name}")
        return rc

    # ------------------------------------------------------------------ status
    def status(self) -> dict[str, Any]:
        out = subprocess.run(["docker", "ps", "-a", "--filter", f"label=com.docker.compose.project={PROJECT}",
                              "--format", "{{json .}}"], capture_output=True, text=True, timeout=30)
        rows = [json.loads(line) for line in out.stdout.splitlines() if line.strip()]
        by_name = {r.get("Names"): r for r in rows}
        profiles = []
        for p in self.catalog["profiles"]:
            if p["availability"] != "image":
                continue
            # present = pinned tag or a compatible local tag (catalog local_fallbacks) for every image
            present = (not images.resolve([p])[2]) if p.get("local_fallbacks") else \
                all(images.present(i) for i in p.get("images", []))
            states = []
            for svc in p.get("services", []):
                r = by_name.get(svc)
                if r is None:
                    states.append("absent")
                    continue
                st = (r.get("Status") or "").lower()
                if svc.endswith("-init"):
                    continue
                states.append("healthy" if "(healthy)" in st else "unhealthy" if "unhealthy" in st else
                              "starting" if "starting" in st else "running" if st.startswith("up") else "stopped")
            if not p.get("services"):
                state = "healthy" if (LAB / "data").is_dir() else "absent"
            elif states and all(s == "absent" for s in states):
                state = "absent"
            else:
                order = ["unhealthy", "stopped", "absent", "starting", "running", "healthy"]
                state = min(states, key=order.index) if states else "absent"
            profiles.append({"id": p["id"], "state": state, "image_present": present})
        return {"profiles": profiles, "helper": {"version": VERSION}}


def make_handler(lab: Lab, token: str, origins: set[str], expires_at: float) -> type[BaseHTTPRequestHandler]:
    class H(BaseHTTPRequestHandler):
        server_version = "alelt-lab-helper/" + VERSION

        def log_message(self, fmt: str, *args: Any) -> None:  # quiet, no token in logs
            sys.stderr.write("[lab-helper] %s %s\n" % (self.command, self.path.split("?")[0]))

        def _cors(self) -> None:
            origin = self.headers.get("Origin")
            if origin in origins:
                self.send_header("Access-Control-Allow-Origin", origin)
                self.send_header("Vary", "Origin")
                self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Lab-Token")
                self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

        def _send(self, code: int, body: Any) -> None:
            data = json.dumps(body).encode()
            self.send_response(code)
            self._cors()
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _guard(self) -> bool:
            host = (self.headers.get("Host") or "").rsplit(":", 1)[0]
            if host not in ("127.0.0.1", "localhost"):
                self._send(403, {"error": "bad_host"})
                return False
            origin = self.headers.get("Origin")
            if origin is not None and origin not in origins:
                self._send(403, {"error": "origin_not_allowed"})
                return False
            if time.time() > expires_at:
                self._send(401, {"error": "token_expired", "detail": "restart the helper (lab helper)"})
                return False
            if not hmac.compare_digest(self.headers.get("X-Lab-Token", ""), token):
                self._send(401, {"error": "bad_token"})
                return False
            return True

        def do_OPTIONS(self) -> None:  # noqa: N802 - CORS preflight
            self.send_response(204)
            self._cors()
            self.end_headers()

        def do_GET(self) -> None:  # noqa: N802
            if not self._guard():
                return
            path = self.path.split("?")[0]
            if path == "/lab/v1/catalog":
                self._send(200, lab.catalog)
            elif path == "/lab/v1/status":
                self._send(200, lab.status())
            elif path.startswith("/lab/v1/ops/"):
                op = lab.ops.get(path.rsplit("/", 1)[1])
                self._send(200, op.view()) if op else self._send(404, {"error": "unknown_op"})
            else:
                self._send(404, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            if not self._guard():
                return
            action = self.path.split("?")[0].removeprefix("/lab/v1/")
            if action not in ("pull", "up", "down", "reset"):
                self._send(404, {"error": "not_found"})
                return
            try:
                n = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(min(n, 65536)) or b"{}")
                ids = body.get("profiles")
                if action in ("down", "reset") and not ids:
                    ids = [p["id"] for p in lab.catalog["profiles"] if p["availability"] == "image" and not p.get("optional")]
                op = lab.start(action, ids)
            except (ValueError, json.JSONDecodeError) as e:
                self._send(400, {"error": "bad_request", "detail": str(e)})
                return
            self._send(202, {"op_id": op.id})

    return H


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--port", type=int, default=load_catalog()["helper"]["port"])
    ap.add_argument("--console", action="append", default=None,
                    help="allowed console origin (repeatable); default http://localhost:8080 + :5173")
    ap.add_argument("--ttl-hours", type=float, default=8.0)
    a = ap.parse_args(argv)
    origins = set(a.console or ["http://localhost:8080", "http://127.0.0.1:8080", "http://localhost:5173"])
    token = secrets.token_urlsafe(24)
    lab = Lab()
    srv = ThreadingHTTPServer(("127.0.0.1", a.port), make_handler(lab, token, origins, time.time() + a.ttl_hours * 3600))
    helper = f"http://127.0.0.1:{a.port}"
    console = (a.console or ["http://localhost:8080"])[0]
    print(f"[lab-helper] listening on {helper} (127.0.0.1 only), token valid {a.ttl_hours:g} h", flush=True)
    print(f"[lab-helper] open: {console}/lab#helper={quote(helper, safe='')}&token={token}", flush=True)
    print("[lab-helper] Ctrl+C to stop. The token is shown only here.", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
