"""Hub link (outbound HTTPS only, R4): protocol v1 headers, token or mTLS client certificate, optional server
SPKI SHA-256 pinning (same `hub.server_pins_sha256` as the Windows agent), explicit timeouts on every call."""
from __future__ import annotations

import hashlib
import http.client
import json
import os
import ssl
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

from .config import HubCfg


def retry_after_s(headers: Dict[str, str]) -> Optional[float]:
    """Retry-After in seconds (delta form; an HTTP date is ignored)."""
    v = (headers or {}).get("retry-after")
    try:
        return max(0.0, float(v)) if v is not None else None
    except ValueError:
        return None


class HubError(RuntimeError):
    def __init__(self, status: int, body: str, headers: Optional[Dict[str, str]] = None) -> None:
        super().__init__(f"HTTP {status}: {body[:300]}")
        self.status, self.body, self.headers = status, body, headers or {}

    def detail(self) -> Any:
        try:
            return json.loads(self.body).get("detail")
        except (ValueError, AttributeError):
            return None


# ------------------------------------------------------------------ SPKI pinning (minimal DER walk, stdlib only)
def _tlv(der: bytes, off: int) -> Tuple[int, int, int]:
    """(tag, value offset, end offset) of the DER element at `off`."""
    tag = der[off]
    ln = der[off + 1]
    off += 2
    if ln & 0x80:
        n = ln & 0x7F
        ln = int.from_bytes(der[off:off + n], "big")
        off += n
    return tag, off, off + ln


def spki_sha256(cert_der: bytes) -> str:
    """hex SHA-256 of the certificate's DER subjectPublicKeyInfo (RFC 7469 pin)."""
    _, cv, _ = _tlv(cert_der, 0)                 # Certificate
    _, tv, tend = _tlv(cert_der, cv)             # tbsCertificate
    off = tv
    tag, _, end = _tlv(cert_der, off)
    if tag == 0xA0:                              # [0] version
        off = end
    for _ in range(5):                           # serial, signature, issuer, validity, subject
        _, _, off = _tlv(cert_der, off)
    tag, _, end = _tlv(cert_der, off)
    if tag != 0x30 or end > tend:
        raise ValueError("certificate has no subjectPublicKeyInfo")
    return hashlib.sha256(cert_der[off:end]).hexdigest()


class _PinnedHTTPS(http.client.HTTPSConnection):
    pins: List[str] = []

    def connect(self) -> None:
        super().connect()
        if self.pins:
            der = self.sock.getpeercert(binary_form=True) if self.sock else None   # type: ignore[union-attr]
            if not der or spki_sha256(der) not in self.pins:
                self.close()
                raise ssl.SSLError("hub certificate does not match hub.server_pins_sha256")


class HubClient:
    def __init__(self, cfg: HubCfg, agent_id: str, credential_present: bool = False) -> None:
        self.cfg = cfg
        u = urlsplit(cfg.url)
        self.https = u.scheme == "https"
        self.host = u.hostname or ""
        self.port = u.port or (443 if self.https else 80)
        self.prefix = u.path.rstrip("/")
        self.headers = {"X-ALELT-Protocol": "1", "X-ALELT-Agent-Id": agent_id, "User-Agent": "aleltd"}
        self.env_token: Optional[str] = None
        if cfg.mode in ("token", "dev"):
            token = os.environ.get(cfg.token_env)
            if not token and not credential_present:
                raise ValueError(f"agent token missing: set environment variable {cfg.token_env} (R9)")
            self.env_token = token or None
            if token:
                self.headers["X-ALELT-Agent-Token"] = token
        self.ctx: Optional[ssl.SSLContext] = None
        if self.https:
            self.ctx = ssl.create_default_context(cafile=cfg.ca_file) if cfg.ca_file else ssl.create_default_context()
            self.ctx.minimum_version = ssl.TLSVersion.TLSv1_2
            if cfg.client_cert_file:
                self.ctx.load_cert_chain(cfg.client_cert_file, cfg.client_key_file)

    def _conn(self, timeout: float) -> http.client.HTTPConnection:
        if self.https:
            c = _PinnedHTTPS(self.host, self.port, timeout=timeout, context=self.ctx)
            c.pins = self.cfg.server_pins_sha256
            return c
        return http.client.HTTPConnection(self.host, self.port, timeout=timeout)

    def call(self, method: str, path: str, body: Optional[bytes] = None, headers: Optional[Dict[str, str]] = None,
             timeout: Optional[float] = None) -> Any:
        t = timeout if timeout is not None else self.cfg.request_timeout_ms / 1000
        t = max(0.5, t)
        c = self._conn(min(t, self.cfg.connect_timeout_ms / 1000))
        try:
            c.connect()
            if c.sock is not None:
                c.sock.settimeout(t)
            c.request(method, self.prefix + path, body=body, headers={**self.headers, **(headers or {})})
            r = c.getresponse()
            data = r.read()
            hdrs = {k.lower(): v for k, v in r.getheaders()}
            if not 200 <= r.status < 300:            # 3xx too (LNX-16): a proxy redirect is not an answer
                raise HubError(r.status, data.decode(errors="replace"), hdrs)
            if not data:
                return None
            try:
                return json.loads(data)
            except ValueError:
                raise HubError(502, f"hub answered {r.status} with a non-JSON body: "
                                    f"{data[:120].decode(errors='replace')}", hdrs) from None
        finally:
            c.close()

    def set_token(self, token: Optional[str]) -> None:
        """Switch the bearer credential (REQ-26: per-agent `alelta_` credential replaces the enrollment token)."""
        if token:
            self.headers["X-ALELT-Agent-Token"] = token
        else:
            self.headers.pop("X-ALELT-Agent-Token", None)

    @property
    def token(self) -> Optional[str]:
        return self.headers.get("X-ALELT-Agent-Token")

    def post_json(self, path: str, obj: Any, timeout: Optional[float] = None) -> Any:
        return self.call("POST", path, json.dumps(obj).encode(), {"Content-Type": "application/json"}, timeout)

