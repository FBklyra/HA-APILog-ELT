"""Per-agent hub credential (REQ-26 / F-14): an agent enrolled with a one-time `alelte_` token declares capability
`credential.v1` at registration; the hub answers once with a `alelta_` credential + `credential_expires_at`. The
agent keeps it in a 0600 file next to state.json (atomic write: tmp -> fsync -> rename -> fsync dir), uses it
instead of the enrollment token from then on (also after a restart) and rotates it before it expires
(`POST /agent/v1/credential/rotate`; the new one is persisted before the old one is dropped; the hub keeps the old
one valid for a grace period). The value is never logged: `masked()` is the only printable form.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from .checkpoint import atomic_write

CAPABILITY = "credential.v1"
ROTATE_BEFORE = timedelta(days=7)          # rotate when less than this is left (or 10 % of the lifetime)


def masked(token: Optional[str]) -> str:
    return f"{token[:4]}***" if token else "-"


def _parse_ts(v: Any) -> Optional[datetime]:
    if not v:
        return None
    try:
        d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


class CredentialStore:
    def __init__(self, path: str) -> None:
        self.path = path
        self.token: Optional[str] = None
        self.expires_at: Optional[datetime] = None
        self.issued_at: Optional[datetime] = None

    # ------------------------------------------------------------------ persistence
    def load(self) -> bool:
        try:
            d = json.loads(Path(self.path).read_text(encoding="utf-8"))
        except FileNotFoundError:
            return False
        except (OSError, ValueError):
            return False
        tok = d.get("credential") if isinstance(d, dict) else None
        if not isinstance(tok, str) or not tok.startswith("alelta_"):
            return False
        self.token, self.expires_at = tok, _parse_ts(d.get("credential_expires_at"))
        self.issued_at = _parse_ts(d.get("issued_at"))
        return True

    def save(self, token: str, expires_at: Any) -> None:
        if not isinstance(token, str) or not token.startswith("alelta_"):
            raise ValueError("hub returned a malformed agent credential")
        exp = _parse_ts(expires_at)
        now = datetime.now(timezone.utc)
        data = json.dumps({"credential": token, "credential_expires_at": exp.isoformat() if exp else None,
                           "issued_at": now.isoformat()}).encode()
        old = os.umask(0o077)
        try:
            atomic_write(self.path, data)
        finally:
            os.umask(old)
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass
        self.token, self.expires_at, self.issued_at = token, exp, now

    def discard(self) -> None:
        self.token = self.expires_at = self.issued_at = None
        try:
            os.replace(self.path, self.path + ".rejected")     # kept for support; never reused
        except OSError:
            pass

    # ------------------------------------------------------------------ policy
    def rotate_due(self, now: Optional[datetime] = None) -> bool:
        if not self.token or self.expires_at is None:
            return False
        now = now or datetime.now(timezone.utc)
        before = ROTATE_BEFORE
        if self.issued_at is not None:
            before = min(before, max(timedelta(minutes=5), (self.expires_at - self.issued_at) / 10))
        return now >= self.expires_at - before

    def expired(self, now: Optional[datetime] = None) -> bool:
        return bool(self.expires_at is not None and (now or datetime.now(timezone.utc)) >= self.expires_at)

    def describe(self) -> Dict[str, Any]:
        return {"credential": masked(self.token),
                "expires_at": self.expires_at.isoformat() if self.expires_at else None}
