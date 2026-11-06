"""HTTP hardening middleware (security defaults, F-14 / design review §12).

- `PeerCaptureMiddleware` (outermost): records the real TCP peer in `scope["alelt.peer"]` and only then applies
  X-Forwarded-For from ALELT_FORWARDED_ALLOW_IPS (uvicorn's ProxyHeadersMiddleware, run in-app; `alelt api` starts
  uvicorn with proxy_headers=False). The agent identity header is trusted only from that real peer.
- `SecurityMiddleware`: CORS (ALELT_CORS_ORIGINS allow-list; default same-origin only), CSRF (a state-changing
  `/api/` request with a foreign Origin is refused; the console authenticates with headers, never cookies),
  per-client rate limits (ALELT_RATE_LIMIT_PER_MIN on `/api/`; ALELT_AUTH_FAIL_LIMIT_PER_MIN failed authentications on
  every path -> 429), security response headers.
"""
from __future__ import annotations

import json
import threading
import time
from collections import defaultdict, deque
from typing import Any
from urllib.parse import urlsplit

from .settings import get_settings

_MUTATING = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_HEADERS = [(b"x-content-type-options", b"nosniff"), (b"x-frame-options", b"DENY"),
            (b"referrer-policy", b"no-referrer"), (b"cache-control", b"no-store")]


class PeerCaptureMiddleware:
    def __init__(self, app: Any) -> None:
        import os

        from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

        allow = os.environ.get("ALELT_FORWARDED_ALLOW_IPS", "127.0.0.1")   # '*' refused outside dev (settings)
        self.app = ProxyHeadersMiddleware(app, trusted_hosts=allow)

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] in ("http", "websocket") and "alelt.peer" not in scope:
            scope["alelt.peer"] = scope.get("client")
        await self.app(scope, receive, send)


class _Window:
    """Sliding one-minute counters per key (in-process; each API replica limits on its own)."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.hits: dict[str, deque[float]] = defaultdict(deque)

    def count(self, key: str, add: bool = True) -> int:
        now = time.monotonic()
        with self.lock:
            q = self.hits[key]
            while q and now - q[0] > 60:
                q.popleft()
            if add:
                q.append(now)
            if not q:
                self.hits.pop(key, None)
            return len(q)

    def clear(self) -> None:
        with self.lock:
            self.hits.clear()


REQUESTS = _Window()
AUTH_FAILS = _Window()


def _origin_ok(origin: str, host: str, allowed: list[str]) -> bool:
    from .settings import DEV_ENVS

    if origin in allowed:
        return True
    try:
        u = urlsplit(origin)
    except ValueError:
        return False
    if u.netloc.lower() == host.lower():
        return True
    # dev/test: the console dev server (Vite proxy, other port on localhost) is same-machine
    return get_settings().env in DEV_ENVS and (u.hostname or "") in ("localhost", "127.0.0.1")


class SecurityMiddleware:
    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        s = get_settings()
        path: str = scope["path"]
        method: str = scope["method"]
        headers = {k.decode().lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}
        client = str((scope.get("client") or ("?", 0))[0])
        origin = headers.get("origin")
        cors: list[tuple[bytes, bytes]] = []
        if origin and path.startswith("/api/"):
            if origin in s.cors_origins:
                cors = [(b"access-control-allow-origin", origin.encode()), (b"vary", b"Origin"),
                        (b"access-control-allow-headers", b"authorization, content-type, x-alelt-correlation-id"),
                        (b"access-control-allow-methods", b"GET, POST, PUT, DELETE")]
            if method == "OPTIONS":
                await self._send(send, 204 if cors else 403, {} if cors else _err("forbidden", "origin not allowed"),
                                 cors)
                return
            if method in _MUTATING and not _origin_ok(origin, headers.get("host", ""), s.cors_origins):
                await self._send(send, 403, _err("forbidden", "cross-origin request refused (CSRF)"))
                return
        if AUTH_FAILS.count(client, add=False) >= s.auth_fail_limit_per_min > 0:
            await self._send(send, 429, _err("rate_limited", "too many failed authentications; retry later"),
                             [(b"retry-after", b"60")])
            return
        if path.startswith("/api/") and s.rate_limit_per_min > 0 and REQUESTS.count(client) > s.rate_limit_per_min:
            await self._send(send, 429, _err("rate_limited", "too many requests"), [(b"retry-after", b"60")])
            return

        async def wrapped(msg: dict[str, Any]) -> None:
            if msg["type"] == "http.response.start":
                if msg["status"] in (401, 403) and (path.startswith("/api/") or path.startswith("/agent/")):
                    if msg["status"] == 401:
                        AUTH_FAILS.count(client)
                have = {k.lower() for k, _ in msg.get("headers", [])}
                msg["headers"] = [*msg.get("headers", []), *[h for h in _HEADERS if h[0] not in have], *cors]
            await send(msg)

        await self.app(scope, receive, wrapped)

    @staticmethod
    async def _send(send: Any, code: int, body: dict[str, Any], extra: list[tuple[bytes, bytes]] | None = None) -> None:
        data = json.dumps(body).encode() if code != 204 else b""
        await send({"type": "http.response.start", "status": code,
                    "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(data)).encode()),
                                *_HEADERS, *(extra or [])]})
        await send({"type": "http.response.body", "body": data})


def _err(code: str, msg: str) -> dict[str, Any]:
    return {"error": {"code": code, "message": msg}, "detail": msg}


def validation_error_body(errors: list[dict]) -> list[dict]:  # type: ignore[type-arg]
    """AHS-22: pydantic validation errors without the submitted `input` / `ctx` (they echo passwords back)."""
    return [{k: v for k, v in e.items() if k in ("type", "loc", "msg")} for e in errors]


def install_validation_handler(app: Any) -> None:
    from fastapi.exceptions import RequestValidationError
    from fastapi.responses import JSONResponse

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Any, exc: RequestValidationError) -> JSONResponse:
        body = validation_error_body(list(exc.errors()))
        return JSONResponse({"error": {"code": "INVALID_REQUEST", "message": "request validation failed",
                                       "correlation_id": request.headers.get("x-alelt-correlation-id")},
                             "detail": body}, status_code=422)
