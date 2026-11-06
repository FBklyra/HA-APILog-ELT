"""Logging to stdout/stderr only (journald captures both); no log files.

JSON lines (alelt_common.util.JsonFormatter) - DEBUG/INFO -> stdout, WARNING and above -> stderr.
Under journald (JOURNAL_STREAM matches our stdout/stderr device:inode) each line gets a `<N>` syslog priority
prefix, which journald strips and stores as PRIORITY (so `journalctl -p warning` works), and the `ts` field is
dropped unless log.timestamps=always (journald already timestamps every line).
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Optional, TextIO

try:                                     # shared formatter (hub/alelt_common, stdlib only)
    from alelt_common.util import JsonFormatter as _Base
except ImportError:                      # pragma: no cover - agent installed without the hub tree
    class _Base(logging.Formatter):      # type: ignore[no-redef]
        KEYS = ("component", "position", "table", "batch_id", "correlation_id")

        def format(self, record: logging.LogRecord) -> str:
            out: Any = {"ts": datetime.now(timezone.utc).isoformat(), "level": record.levelname,
                        "logger": record.name, "msg": record.getMessage()}
            for k in self.KEYS:
                v = getattr(record, k, None)
                if v is not None:
                    out[k] = str(v)
            if record.exc_info:
                out["exc"] = self.formatException(record.exc_info)
            return json.dumps(out, ensure_ascii=False)

_SYSLOG = {logging.DEBUG: 7, logging.INFO: 6, logging.WARNING: 4, logging.ERROR: 3, logging.CRITICAL: 2}


class AgentFormatter(_Base):
    """JsonFormatter + component default + optional journald priority prefix / timestamp removal."""

    def __init__(self, journal: bool = False, timestamps: bool = True) -> None:
        super().__init__()
        self.journal, self.timestamps = journal, timestamps

    def format(self, record: logging.LogRecord) -> str:
        if getattr(record, "component", None) is None:
            record.component = "aleltd"
        line = super().format(record)
        if not self.timestamps:
            d = json.loads(line)
            d.pop("ts", None)
            line = json.dumps(d, ensure_ascii=False)
        if self.journal:
            line = f"<{_SYSLOG.get(record.levelno, 6)}>{line}"
        return line


class _MaxLevel(logging.Filter):
    def __init__(self, below: int) -> None:
        super().__init__()
        self.below = below

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < self.below


def journal_connected(stream: TextIO) -> bool:
    """True when `stream` is the journald stream systemd announced in JOURNAL_STREAM=<dev>:<ino>."""
    js = os.environ.get("JOURNAL_STREAM", "")
    if not js or ":" not in js:
        return False
    try:
        st = os.fstat(stream.fileno())
        dev, ino = js.split(":", 1)
        return st.st_dev == int(dev) and st.st_ino == int(ino)
    except (OSError, ValueError, AttributeError):
        return False


def setup(level: str = "INFO", timestamps: str = "auto", stdout: Optional[TextIO] = None,
          stderr: Optional[TextIO] = None) -> None:
    out, err = stdout or sys.stdout, stderr or sys.stderr
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    for stream, lo, hi in ((out, logging.NOTSET, logging.WARNING), (err, logging.WARNING, None)):
        journal = journal_connected(stream)
        ts = timestamps == "always" or (timestamps == "auto" and not journal)
        h = logging.StreamHandler(stream)
        h.setFormatter(AgentFormatter(journal=journal, timestamps=ts))
        h.setLevel(lo)
        if hi is not None:
            h.addFilter(_MaxLevel(hi))
        root.addHandler(h)
    root.setLevel(level.upper())


def set_level(level: str) -> None:
    logging.getLogger().setLevel(level.upper())
