"""Secret handling for importers and wizards (R9).

- `mask()` replaces secret-looking values recursively and reports which keys were masked.
- `SecretStore` keeps values outside config and metadata: references only (`env:NAME`, `file:NAME`) are stored.
  The file backend writes into ALELT_SECRETS_DIR (the tmpfs the deploy materialises from the vault; dev default
  `.alelt/secrets`). Values never reach the hub database, the audit log or API responses.
"""
from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .models import MASK

SECRET_KEY = re.compile(r"(pass(word|phrase)?|secret|token|private_key|api_key|apikey|credential|client_key|"
                        r"certificate|ssh_key|access_key|signing_key|auth_key)", re.I)
_URL_CRED = re.compile(r"(?<=://)([^/@:\s]+):([^/@\s]+)@")
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_REPO = Path(__file__).resolve().parents[2]


def is_secret_key(key: str) -> bool:
    return bool(SECRET_KEY.search(key)) and not key.lower().startswith(("is_", "has_"))


def mask(obj: Any, path: str = "") -> tuple[Any, list[str]]:
    """Returns (masked copy, dotted paths of masked values). Credentials embedded in URLs are masked too."""
    found: list[str] = []
    if isinstance(obj, Mapping):
        out: dict[str, Any] = {}
        for k, v in sorted(obj.items(), key=lambda kv: str(kv[0])):
            p = f"{path}.{k}" if path else str(k)
            if is_secret_key(str(k)) and v not in (None, "", False) and not isinstance(v, (dict, list)):
                out[str(k)] = MASK
                found.append(p)
            else:
                out[str(k)], sub = mask(v, p)
                found += sub
        return out, found
    if isinstance(obj, list):
        items = []
        for i, v in enumerate(obj):
            mv, sub = mask(v, f"{path}[{i}]")
            items.append(mv)
            found += sub
        return items, found
    if isinstance(obj, str) and _URL_CRED.search(obj):
        found.append(path)
        return _URL_CRED.sub(rf"\1:{MASK}@", obj), found
    return obj, found


def secrets_dir() -> Path:
    return Path(os.environ.get("ALELT_SECRETS_DIR") or _REPO / ".alelt" / "secrets")


class SecretStore:
    """Reference-based secret store. `put` returns the reference to persist; `resolve` reads it back."""

    def __init__(self, root: Path | None = None) -> None:
        self.root = root or secrets_dir()

    @staticmethod
    def name_for(key: str) -> str:
        n = re.sub(r"[^A-Za-z0-9_.-]+", "_", key).strip("_.-")[:120]
        if not n or not _NAME.match(n):
            raise ValueError(f"bad secret key {key!r}")
        return n

    def put(self, key: str, value: str) -> str:
        name = self.name_for(key)
        self.root.mkdir(parents=True, exist_ok=True)
        tmp = self.root / f".{name}.tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(value)
        os.replace(tmp, self.root / name)
        return f"file:{name}"

    def exists(self, ref: str) -> bool:
        try:
            self.resolve(ref)
            return True
        except KeyError:
            return False

    def resolve(self, ref: str) -> str:
        kind, _, name = ref.partition(":")
        if kind == "env" and name and name in os.environ:
            return os.environ[name]
        if kind == "file" and _NAME.match(name or ""):
            p = self.root / name
            if p.is_file():
                return p.read_text(encoding="utf-8").rstrip("\r\n")
        raise KeyError(f"secret reference {ref!r} not found")


def valid_ref(ref: str) -> bool:
    kind, _, name = ref.partition(":")
    return kind in ("env", "file") and bool(_NAME.match(name or ""))
