"""Shared utilities: codecs, hashing, structured JSON logging, plugin registry."""
from __future__ import annotations

import hashlib
import importlib
import json
import re
import logging
import logging.handlers
import os
import socket
import sys
import zlib
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------- codecs
def compress(data: bytes, codec: str) -> bytes:
    if codec == "none":
        return data
    if codec == "zlib":
        return zlib.compress(data, 6)
    if codec == "zstd":
        import zstandard  # optional dependency

        return zstandard.ZstdCompressor(level=3).compress(data)
    raise ValueError(f"unknown codec {codec}")


class DecompressionLimit(ValueError):
    """Decompressed output would exceed the declared / allowed size (decompression bomb, AHS-03)."""


def decompress(data: bytes, codec: str, max_out: int | None = None) -> bytes:
    """Decompress; with `max_out` never inflates more than that many bytes (raises DecompressionLimit)."""
    if codec == "none":
        if max_out is not None and len(data) > max_out:
            raise DecompressionLimit(f"body larger than {max_out} bytes")
        return data
    if codec == "zlib":
        if max_out is None:
            return zlib.decompress(data)
        d = zlib.decompressobj()
        out = d.decompress(data, max_out + 1)
        if len(out) > max_out or d.unconsumed_tail:
            raise DecompressionLimit(f"zlib output exceeds {max_out} bytes")
        out += d.flush()
        if len(out) > max_out:
            raise DecompressionLimit(f"zlib output exceeds {max_out} bytes")
        if not d.eof:
            raise zlib.error("incomplete zlib stream")
        return out
    if codec == "zstd":
        import zstandard

        cap = (1 << 30) if max_out is None else max_out
        parts, n = [], 0
        with zstandard.ZstdDecompressor().stream_reader(data, read_across_frames=True) as r:
            while True:
                chunk = r.read(min(1 << 20, cap + 1 - n))
                if not chunk:
                    break
                parts.append(chunk)
                n += len(chunk)
                if n > cap:
                    raise DecompressionLimit(f"zstd output exceeds {cap} bytes")
        return b"".join(parts)
    raise ValueError(f"unknown codec {codec}")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------- logging
_SECRET_PATTERNS = (
    (re.compile(r"\b(alelt[ea]_)[A-Za-z0-9_\-]{8,}"), r"\1***"),                     # enrolment / agent credentials
    (re.compile(r"(://[^:/@\s]+:)[^@\s]+@"), r"\1***@"),                           # URL passwords
    (re.compile(r"(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|private[_-]?key)(\s*[=:]\s*)([^\s,;&\"']+)"),
     r"\1\2***"),
    (re.compile(r"(?i)(authorization:\s*bearer\s+)[A-Za-z0-9._\-]+"), r"\1***"),
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----"), "<private key>"),
)


def scrub_secrets(text: str) -> str:
    """Mask credentials in log text (R9: secrets never in logs)."""
    for rx, rep in _SECRET_PATTERNS:
        text = rx.sub(rep, text)
    return text


class JsonFormatter(logging.Formatter):
    """Structured JSON lines; extra fields: component, position, table, batch_id, correlation_id."""

    KEYS = ("component", "position", "table", "batch_id", "correlation_id")

    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": scrub_secrets(record.getMessage()),
        }
        for k in self.KEYS:
            v = getattr(record, k, None)
            if v is not None:
                out[k] = scrub_secrets(str(v))
        if record.exc_info:
            out["exc"] = scrub_secrets(self.formatException(record.exc_info))
        return json.dumps(out, ensure_ascii=False)


def setup_logging(level: str = "INFO") -> None:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [h]
    log_dir = os.environ.get("ALELT_LOG_DIR")     # optional rotating JSON log file (support bundle reads it)
    if log_dir:
        try:
            os.makedirs(log_dir, exist_ok=True)
            name = f"{os.environ.get('ALELT_LOG_NAME') or 'alelt'}-{socket.gethostname()}.log"
            fh = logging.handlers.RotatingFileHandler(os.path.join(log_dir, name), maxBytes=10 << 20,
                                                      backupCount=3, encoding="utf-8")
            fh.setFormatter(JsonFormatter())
            root.handlers.append(fh)
        except OSError:
            pass
    root.setLevel(level)


# ---------------------------------------------------------------- registry
def load(ref: str) -> Any:
    """Load 'package.module:Class' and return the attribute."""
    mod, _, attr = ref.partition(":")
    return getattr(importlib.import_module(mod), attr)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
