"""Incident signatures: same failure, same signature (volatile tokens - ids, numbers, positions - removed)."""
from __future__ import annotations

import hashlib
import re

_NORMALIZE: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I), "<uuid>"),
    (re.compile(r"\b\d+:[0-9a-f]{8,}\b", re.I), "<pos>"),
    (re.compile(r"\b(0x)?[0-9a-f]{12,}\b", re.I), "<hex>"),
    (re.compile(r"'[^']*'|\"[^\"]*\""), "<q>"),
    (re.compile(r"\b\d+(\.\d+)?\b"), "<n>"),
    (re.compile(r"\s+"), " "),
]


def normalize(text: str | None) -> str:
    """First error line, lower-cased, with volatile tokens replaced. Used for dedup and known-issue matching."""
    s = (text or "").strip().splitlines()[0] if (text or "").strip() else ""
    s = s.lower()
    for rx, rep in _NORMALIZE:
        s = rx.sub(rep, s)
    return s.strip()[:300]


def signature(kind: str, connection_id: str | None, table_fqn: str | None, error: str | None) -> str:
    raw = "|".join([kind, connection_id or "", table_fqn or "", normalize(error)])
    return hashlib.sha256(raw.encode()).hexdigest()[:32]
