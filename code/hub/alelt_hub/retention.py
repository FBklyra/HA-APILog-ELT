"""Retention of hub data owned by the pipeline (HC-05): finished jobs, the decoded-event cache, spooled batches.

Nothing that a replay may still need is removed. Per connection the *replay floor* is the smallest batch seq any
replay can start from: the main stream's restart_seq (oldest open transaction), every held table's recorded
restart_seq (alelt_hub.replay / ADR-005), every unfinished snapshot catch-up and the first signal batch of every pending
out-of-row completion (alelt_hub.completion, DR rebuild). A held row without a recorded
restart_seq (older hub version) or an unfinished catch-up pins the floor at the first batch whose to_pos reaches its
start position minus ALELT_CATCHUP_LOOKBACK_BATCHES.

- jobs: DONE older than ALELT_JOB_RETENTION_DAYS (7), FAILED older than 30 days.
- decoded cache (`decoded/<cid>/<batch>...`): objects of batches below the floor and older than
  ALELT_DECODED_CACHE_DAYS (2).
- batches (raw archive): only when `retention.raw_archive_days` (console) or ALELT_RAW_ARCHIVE_DAYS is set: LOADED
  batches below the floor and older than that, row + spool object (ADR-007 raw archive window). Objects whose
  delete failed become orphans and are collected by alelt_hub.spool_gc.
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import Engine, delete, func, or_, select

from alelt_common.models import Position
from alelt_common.util import utcnow

from . import holds
from .blobstore import delete_ref, get_store
from .db import batch, checkpoint_mirror, hub_setting, job, table_completion

log = logging.getLogger("alelt_hub.retention")


def _days(name: str, default: float | None) -> float | None:
    v = os.environ.get(name)
    return float(v) if v else default


def replay_floor(engine: Engine, cid: str, compare: Any, engine_name: str, lookback: int) -> int:
    """Smallest batch seq a replay of `cid` may still read (0 = everything)."""
    with engine.connect() as c:
        r = c.execute(select(checkpoint_mirror.c.restart_seq).where(checkpoint_mirror.c.stream == cid)).scalar()
    floor = int(r) if r is not None else 0
    with engine.connect() as c:                  # pending out-of-row completions: their signals stay re-scannable (DR)
        cf = c.execute(select(func.min(table_completion.c.first_seq)).where(
            table_completion.c.connection_id == cid, table_completion.c.first_seq.is_not(None))).scalar()
    if cf is not None:
        floor = min(floor, int(cf))
    pins: list[str | None] = []
    for h in holds.rows(engine, cid):
        b = h["body"] or {}
        unfinished_catchup = h["kind"] == "snapshot" and h["state"] == "CATCHING_UP" and \
            b.get("catchup_done") != b.get("snapshot_id")
        if not (h["held"] or unfinished_catchup):
            continue
        if b.get("restart_seq") is not None and not unfinished_catchup:
            floor = min(floor, int(b["restart_seq"]))
        else:
            pins.append(h["start_pos"])
    if any(x is None for x in pins):
        return 0
    if pins:
        with engine.connect() as c:
            light = c.execute(select(batch.c.seq, batch.c.to_lri).where(batch.c.connection_id == cid)
                              .order_by(batch.c.seq)).all()
        for pos in pins:
            st = Position(engine=engine_name, text=str(pos))
            idx = next((i for i, (_s, to) in enumerate(light) if compare(Position(engine=engine_name, text=to), st) >= 0),
                       len(light))
            floor = min(floor, int(light[max(0, idx - lookback)][0]) if light else 0)
    return max(0, floor)


def purge(engine: Engine, worker: Any) -> dict[str, int]:
    out = {"jobs": 0, "decoded": 0, "batches": 0}
    now = utcnow()
    with engine.begin() as c:
        out["jobs"] = c.execute(delete(job).where(or_(
            (job.c.status == "DONE") & (job.c.finished_at < now - timedelta(days=_days("ALELT_JOB_RETENTION_DAYS", 7) or 7)),
            (job.c.status == "FAILED") & (job.c.created_at < now - timedelta(days=30))))).rowcount or 0
        ret = c.execute(select(hub_setting.c.value).where(hub_setting.c.key == "retention")).scalar() or {}
        cids = [str(x) for x, in c.execute(select(func.distinct(batch.c.connection_id)))]
    raw_days = _days("ALELT_RAW_ARCHIVE_DAYS", None)
    if raw_days is None and isinstance(ret, dict) and ret.get("raw_archive_days"):
        raw_days = float(ret["raw_archive_days"])
    dec_days = _days("ALELT_DECODED_CACHE_DAYS", 2) or 2
    st = get_store()
    for cid in cids:
        try:
            p = worker.pipeline(cid)
        except Exception:  # noqa: BLE001 - connection without a usable pipeline: nothing purged for it
            continue
        floor = replay_floor(engine, cid, p.source.compare, p.engine_name, p.s.catchup_lookback)
        with engine.connect() as c:
            old = {str(i): (int(s), ts, stt, ref) for i, s, ts, stt, ref in c.execute(select(
                batch.c.id, batch.c.seq, batch.c.received_at, batch.c.status, batch.c.spool_path).where(
                batch.c.connection_id == cid, batch.c.seq < floor))}
        safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in cid)
        try:
            for key, _size in list(st.list(f"decoded/{safe}/")):
                bid = key.rsplit("/", 1)[-1].split(".", 1)[0]
                b = old.get(bid)
                if b is not None and b[1] is not None and b[1] < now - timedelta(days=dec_days):
                    st.delete(key)
                    out["decoded"] += 1
        except Exception:  # noqa: BLE001
            log.warning("decoded cache purge failed", extra={"component": "alelt_hub", "correlation_id": cid})
        if raw_days is None:
            continue
        gone = [uuid.UUID(i) for i, (_s, ts, stt, _r) in old.items()
                if stt == "LOADED" and ts is not None and ts < now - timedelta(days=raw_days)]
        for i in gone:
            try:
                delete_ref(old[str(i)][3])
            except Exception:  # noqa: BLE001 - orphan, spool_gc collects it
                pass
        if gone:
            with engine.begin() as c:
                out["batches"] += c.execute(delete(batch).where(batch.c.id.in_(gone))).rowcount or 0
    if any(out.values()):
        log.info("retention purge", extra={"component": "alelt_hub", "correlation_id": str(out)})
    return out
