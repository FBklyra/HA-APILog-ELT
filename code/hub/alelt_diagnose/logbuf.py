"""Recent structured log records: in-memory ring buffer (this process) + JSON log files under ALELT_LOG_DIR."""
from __future__ import annotations

import collections
import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

from alelt_common.util import JsonFormatter

_KEYS = JsonFormatter.KEYS


class RingHandler(logging.Handler):
    def __init__(self, size: int = 2000) -> None:
        super().__init__(level=logging.INFO)
        self.buf: collections.deque[dict[str, Any]] = collections.deque(maxlen=size)
        self._fmt = JsonFormatter()
        self._lock2 = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            d = json.loads(self._fmt.format(record))
        except Exception:  # noqa: BLE001 - logging must never fail
            return
        with self._lock2:
            self.buf.append(d)

    def records(self) -> list[dict[str, Any]]:
        with self._lock2:
            return list(self.buf)


_RING: RingHandler | None = None


def install() -> RingHandler:
    """Attach the ring buffer to the root logger (idempotent; survives setup_logging() handler resets)."""
    global _RING
    if _RING is None:
        _RING = RingHandler()
    root = logging.getLogger()
    if _RING not in root.handlers:
        root.addHandler(_RING)
    return _RING


def _file_records(limit: int) -> list[dict[str, Any]]:
    d = os.environ.get("ALELT_LOG_DIR")
    if not d or not Path(d).is_dir():
        return []
    out: list[dict[str, Any]] = []
    for f in sorted(Path(d).glob("*.log"), key=lambda p: p.stat().st_mtime)[-4:]:
        try:
            lines = f.read_text(encoding="utf-8", errors="replace").splitlines()[-limit:]
        except OSError:
            continue
        for line in lines:
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
    return out


def recent(batch_id: str | None = None, table: str | None = None, correlation_id: str | None = None,
           connection_id: str | None = None, limit: int = 40) -> list[dict[str, Any]]:
    """Records that match batch / table / correlation / connection (none without a filter: unrelated noise)."""
    keys = {k for k in (batch_id, table, correlation_id, connection_id) if k}
    if not keys:
        return []
    recs = (_RING.records() if _RING else []) + _file_records(2000)
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for r in reversed(recs):
        hit = any(str(r.get(k, "")) in keys for k in _KEYS) or any(k and k in str(r.get("msg", "")) for k in keys)
        if not hit:
            continue
        sig = f"{r.get('ts')}{r.get('msg')}"
        if sig in seen:
            continue
        seen.add(sig)
        out.append({k: r[k] for k in ("ts", "level", "logger", "msg", "exc", *_KEYS) if k in r})
        if len(out) >= limit:
            break
    return list(reversed(out))
