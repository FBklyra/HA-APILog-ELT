"""Tiny hub /api/v1 client for ladder harnesses (stdlib). Contract: console/API_CONTRACT.md.

Auth: bearer token from ALELT_HUB_TOKEN (Entra ID access token), or, against a dev hub only, X-ALELT-Dev-Role.
"""
from __future__ import annotations

import json
import os
import ssl
import urllib.parse
import urllib.request
from typing import Any, Optional


class Hub:
    def __init__(self, base: Optional[str] = None, dev_role: Optional[str] = None) -> None:
        self.base = (base or os.environ.get("ALELT_HUB_URL", "")).rstrip("/")
        self.token = os.environ.get("ALELT_HUB_TOKEN")
        self.dev_role = dev_role or os.environ.get("ALELT_DEV_ROLE")
        ca = os.environ.get("ALELT_HUB_CA")
        self.ctx = ssl.create_default_context(cafile=ca) if ca else ssl.create_default_context()

    @property
    def configured(self) -> bool:
        return bool(self.base)

    def _req(self, method: str, path: str, body: Any = None, params: Optional[dict[str, Any]] = None) -> Any:
        url = f"{self.base}/api/v1{path}"
        if params:
            url += "?" + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method=method)  # noqa: S310
        req.add_header("Accept", "application/json")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        elif self.dev_role:
            req.add_header("X-ALELT-Dev-Role", self.dev_role)
        ctx = self.ctx if url.startswith("https") else None
        with urllib.request.urlopen(req, timeout=30, context=ctx) as r:  # noqa: S310
            raw = r.read()
        return json.loads(raw.decode()) if raw else None

    def get(self, path: str, **params: Any) -> Any:
        return self._req("GET", path, params=params)

    def post(self, path: str, body: Any) -> Any:
        return self._req("POST", path, body=body)


def items(resp: Any) -> list[dict[str, Any]]:
    """Accept both a bare list and a paged {items:[...]} response."""
    if isinstance(resp, dict) and "items" in resp:
        return list(resp["items"])
    return list(resp or [])
