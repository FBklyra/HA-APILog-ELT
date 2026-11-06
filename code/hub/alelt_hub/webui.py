"""Serve the built console (console/dist) from the hub app: static assets, runtime `/config.js`, SPA fallback.

Used by the single-image product (no separate web server needed). Behind nginx (deploy/hub) nginx may serve
dist/ itself; both work. `ALELT_CONSOLE_DIR` overrides the location; absent directory = API only.
"""
from __future__ import annotations

import json
import os
from collections.abc import Awaitable, Callable
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles

from .settings import get_settings

_RESERVED = ("api/", "agent/", "config.js", "metrics", "healthz", "readyz", "docs", "redoc", "openapi.json", "version")


def console_dir() -> Path:
    return Path(os.environ.get("ALELT_CONSOLE_DIR") or Path(__file__).resolve().parents[2] / "console" / "dist")


def runtime_config() -> dict[str, object]:
    """Non-secret settings the SPA reads at runtime (overrides its build-time defaults)."""
    s = get_settings()
    return {"devAuth": s.auth_mode == "dev", "mock": False, "apiBase": "/api/v1", "env": s.env,
            "entraTenantId": os.environ.get("ALELT_CONSOLE_TENANT_ID", ""),
            "entraClientId": os.environ.get("ALELT_CONSOLE_CLIENT_ID", ""),
            "entraApiScope": os.environ.get("ALELT_CONSOLE_API_SCOPE", "")}


def mount(app: FastAPI) -> None:
    root = console_dir()

    @app.get("/config.js", include_in_schema=False)
    def config_js() -> Response:
        body = "window.__ALELT_CONFIG__ = " + json.dumps(runtime_config()) + ";\n"
        return Response(body, media_type="application/javascript", headers={"Cache-Control": "no-store"})

    index = root / "index.html"
    if not index.is_file():
        return
    if (root / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=root / "assets"), name="console-assets")
    base = root.resolve()

    # Middleware (not a catch-all route) so routers registered later are never shadowed.
    @app.middleware("http")
    async def spa(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        path = request.url.path.lstrip("/")
        if request.method != "GET" or path.startswith(_RESERVED):
            return await call_next(request)
        if path:
            f = (root / path).resolve()
            if f.is_file() and base in f.parents:
                return FileResponse(f)
        resp = await call_next(request)
        if resp.status_code == 404 and "text/html" in request.headers.get("accept", "text/html"):
            return FileResponse(index, headers={"Cache-Control": "no-cache"})
        return resp
