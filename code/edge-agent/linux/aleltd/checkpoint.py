"""Checkpoint = everything needed to restart at the exact log position (HA-safe):

1. flush the in-flight batch (complete transactions already read) into the spool, fsync'd;
2. write `state.json` atomically (tmp -> fsync -> rename -> fsync dir): last read / last durable / next position,
   capturing flag, reposition marker, lease epoch;
3. within the shutdown deadline: ship the spool to the hub (shorter failover gap), re-write state.json;
4. release the HA lease so the standby takes over at its next acquire.

Steps 1-2 are local and always run (also on fatal errors). Steps 3-4 are network, bounded by the deadline.
`state.json` keys are those of the Windows agent (edge-agent/core/src/agent/spool.c) plus Linux extras.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Protocol

from .spool import fsync_dir

log = logging.getLogger("aleltd.checkpoint")
STATE_VERSION = 1


def atomic_write(path: str, data: bytes, _before_rename: Optional[Callable[[], None]] = None) -> None:
    """Crash-safe replace: readers see the old file or the new one, never a partial one."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    if _before_rename is not None:                  # test hook: simulate a kill between write and rename
        _before_rename()
    os.replace(tmp, p)
    fsync_dir(p.parent)


class StateStore:
    def __init__(self, path: str) -> None:
        self.path = path

    def load(self) -> Dict[str, Any]:
        p = Path(self.path)
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            if not isinstance(d, dict):
                raise ValueError("not an object")
            return d
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as e:
            log.warning("state.json unreadable (%s); positions come from spool/hub", e)
            return {}

    def save(self, state: Dict[str, Any], _before_rename: Optional[Callable[[], None]] = None) -> None:
        d = {"version": STATE_VERSION, **state, "saved_at": datetime.now(timezone.utc).isoformat()}
        atomic_write(self.path, json.dumps(d, indent=1).encode(), _before_rename)


class Checkpointable(Protocol):
    def flush_inflight(self) -> int: ...
    def write_state(self, reason: str) -> None: ...
    def drain_spool(self, budget_s: float) -> int: ...
    def release_lease(self, timeout_s: float) -> bool: ...


class Deadline:
    def __init__(self, seconds: float, clock: Callable[[], float] = time.monotonic) -> None:
        self.clock = clock
        self.end = clock() + seconds

    def remaining(self) -> float:
        return max(0.0, self.end - self.clock())

    def expired(self) -> bool:
        return self.remaining() <= 0


def save_checkpoint(agent: Checkpointable, reason: str, deadline: Deadline, drain_s: float = 10.0,
                    network: bool = True) -> bool:
    """Returns True when the position is safe on disk (spool + state.json). Never raises."""
    safe = True
    try:
        n = agent.flush_inflight()
        if n:
            log.info("checkpoint: in-flight batch flushed to spool", extra={"correlation_id": reason})
    except Exception:  # noqa: BLE001 - the read position stays at the last spooled batch: nothing is lost
        log.exception("checkpoint: in-flight flush failed; resume re-reads from the last spooled batch")
    try:
        agent.write_state(reason)
    except Exception:  # noqa: BLE001
        safe = False
        log.exception("checkpoint: state.json write failed")
    if network and not deadline.expired():
        try:
            sent = agent.drain_spool(min(drain_s, deadline.remaining()))
            if sent:
                agent.write_state(reason)
        except Exception:  # noqa: BLE001
            log.warning("checkpoint: spool drain failed; spool kept for the next start", exc_info=True)
    if network and not deadline.expired():
        try:
            agent.release_lease(min(5.0, deadline.remaining()))
        except Exception:  # noqa: BLE001
            log.warning("checkpoint: lease release failed (expires after its ttl)", exc_info=True)
    return safe
