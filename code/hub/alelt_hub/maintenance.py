"""Destination maintenance (lake destinations: compaction / snapshot expiry / metadata cleanup, `dest.maintain()`).

Scheduled by the worker sweep, run as job kind `maintain` {connection_id}:
- cadence: tables.yaml `maintenance: {every_s, window: {start, end, tz}, enabled}` of the connection, else
  ALELT_DEST_MAINTAIN_EVERY_S_<ENGINE> / ALELT_DEST_MAINTAIN_EVERY_S (default 86400); window: `maintenance.window`, else the
  connection's `snapshot_window` (off-peak), else always. Last run: hub_setting `maintain.last:<cid>`.
- never concurrent with an apply on the same table: the job takes the pipeline lock (main stream applies) AND the
  replay lock (replay passes apply); tables loaded by a running snapshot (held `snapshot` rows) are left out. A busy
  lock re-queues the job shortly; nothing waits.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from typing import Any

from sqlalchemy import Engine, select

from alelt_common.util import utcnow
from alelt_snapshot.window import OffPeakWindow

from . import holds, jobs
from .db import hub_setting
from .dialect import insert, try_lock, unlock

log = logging.getLogger("alelt_hub.maintenance")
KEY = "maintain.last:"


def _doc(p: Any) -> dict[str, Any]:
    from . import config_store

    with p.engine.connect() as c:
        d = config_store.base_doc(c, p.cid, p.s.tables_config) or {}
    return dict(d.get("maintenance") or {}) if isinstance(d, dict) else {}


def settings_for(p: Any) -> tuple[bool, float, OffPeakWindow]:
    m = _doc(p)
    eng = str(getattr(p.dest, "engine", "") or type(p.dest).__name__).upper()
    every = float(m.get("every_s") or os.environ.get(f"ALELT_DEST_MAINTAIN_EVERY_S_{eng}")
                  or os.environ.get("ALELT_DEST_MAINTAIN_EVERY_S", "86400"))
    w = m.get("window") or p.policy.snapshot_window
    win = OffPeakWindow.parse(str(w["start"]), str(w["end"]), str(w.get("tz") or "UTC")) if w else OffPeakWindow.always()
    return bool(m.get("enabled", True)), every, win


def last_run(engine: Engine, cid: str) -> datetime | None:
    with engine.connect() as c:
        v = c.execute(select(hub_setting.c.value).where(hub_setting.c.key == KEY + cid)).scalar()
    return datetime.fromisoformat(v["at"]) if isinstance(v, dict) and v.get("at") else None


def due(p: Any, now: datetime | None = None) -> bool:
    if not callable(getattr(p.dest, "maintain", None)):
        return False
    enabled, every, win = settings_for(p)
    now = now or utcnow()
    if not enabled or not win.contains(now):
        return False
    last = last_run(p.engine, p.cid)
    return last is None or (now - last).total_seconds() >= every


def schedule(worker: Any, now: datetime | None = None) -> list[str]:
    """Sweep: enqueue a `maintain` job for every known connection whose destination maintenance is due."""
    out = []
    for cid, p in list(worker.pipelines.items()):
        try:
            if due(p, now):
                with worker.engine.begin() as c:
                    jobs.enqueue(c, "maintain", {"connection_id": cid},
                                 dedup_key=f"maintain:{cid}:{int(time.time() // 300)}")
                out.append(cid)
        except Exception:  # noqa: BLE001 - maintenance never breaks the sweep
            log.exception("maintenance scheduling failed", extra={"component": "alelt_hub", "correlation_id": cid})
    return out


def run(worker: Any, payload: dict[str, Any]) -> dict[str, str] | None:
    """`maintain` job. Returns the destination's per-table report, None when re-queued (lock busy / not due)."""
    cid = str(payload["connection_id"])
    p = worker.pipeline(cid)
    if not due(p) and not payload.get("force"):
        return None
    locks = []
    try:
        for key in (f"pipeline:{cid}", f"replay:{cid}"):
            lk = try_lock(worker.engine, key)
            if lk is None:
                with worker.engine.begin() as c:
                    jobs.enqueue(c, "maintain", payload, delay_s=30,
                                 dedup_key=f"maintain-busy:{cid}:{int(time.time() // 30)}")
                return None
            locks.append((lk, key))
        snapshotting = {r["table_fqn"] for r in holds.rows(worker.engine, cid, "snapshot") if r["held"]}
        names = sorted({p.policy.dest_name(t) for t in p.tables if p.policy.table(t).enabled
                        and t.fqn not in snapshotting})
        t0 = time.monotonic()
        report = p.dest.maintain(names) if names else {}
        val = {"at": utcnow().isoformat(), "seconds": round(time.monotonic() - t0, 3), "tables": len(names),
               "skipped_snapshotting": sorted(snapshotting),
               "errors": {k: v for k, v in (report or {}).items() if v not in ("ok", "unsupported")}}
        with worker.engine.begin() as c:
            c.execute(insert(hub_setting).values(key=KEY + cid, value=val).on_conflict_do_update(
                index_elements=["key"], set_={"value": val}))
        log.info("destination maintenance", extra={"component": "alelt_hub", "correlation_id": f"{cid}: {val}"})
        return dict(report or {})
    finally:
        for lk, key in reversed(locks):
            unlock(lk, key)
