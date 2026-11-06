"""Per-table schedule acted on (tables.yaml `schedule: {class, latency_target_s, window: {start, end, tz}}`, X-110).

- `window` (daily): the table is applied only inside its window. Outside the window it is held (`table_hold` kind
  `schedule`, start_pos = position the main stream had applied for it); when the window opens it is released by ONE
  multi-table replay pass for every table due at that moment (alelt_hub.replay: bulk phase in a `replay` job, short
  attach tail under the pipeline lock) and streams until the window closes again.
- deferred latency: `latency_target_s` >= ALELT_SCHEDULE_DEFER_MIN_S (default 900) and no window -> the table is held and
  applied in rounds every latency_target_s; a round advances the table watermark in the background (bulk phase only)
  and the table stays held, so the main stream is never blocked by deferred replays.
- anything else streams at the connection latency (unchanged).

Holds are effective from the very first batch (F-08 / X-125): `ensure()` runs before every chunk of the main stream,
also during the initial catch-up, so a daily-window table never lands outside its window. A hold can be placed at any
time: its start_pos is the last position the main stream FLUSHED and the hold is evaluated at flush time, so buffered
changes of the table are simply not applied by the main stream (they are replayed by the release).
Tables under an integrity hold (alelt_hub.integrity) are never released / advanced by the schedule.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any

from alelt_common.util import utcnow
from alelt_loader.policy import TableSchedule
from alelt_snapshot.window import OffPeakWindow

from . import holds, integrity, replay

log = logging.getLogger("alelt_hub.schedule")
KIND = "schedule"


def defer_min_s() -> float:
    return float(os.environ.get("ALELT_SCHEDULE_DEFER_MIN_S", "900"))


def window_of(sch: TableSchedule) -> OffPeakWindow | None:
    if not sch.window:
        return None
    w = sch.window
    return OffPeakWindow.parse(str(w["start"]), str(w["end"]), str(w.get("tz") or "UTC"))


def mode(sch: TableSchedule | None) -> str:
    """window | deferred | stream"""
    if sch is None:
        return "stream"
    if sch.window:
        return "window"
    if sch.latency_target_s and sch.latency_target_s >= defer_min_s():
        return "deferred"
    return "stream"


def scheduled_tables(policy: Any) -> dict[str, TableSchedule]:
    return {fqn: tp.schedule for fqn, tp in policy.tables.items()
            if tp.enabled and tp.schedule is not None and mode(tp.schedule) != "stream"}


def _inside(sch: TableSchedule, now: datetime) -> bool:
    win = window_of(sch)
    return win.contains(now) if win else True


def _streaming_only(pipeline: Any, want: dict[str, TableSchedule]) -> dict[str, TableSchedule]:
    """Schedules act on STREAMING tables only (§2.3 item 3): an initial load / catch-up / resync lands whatever the
    window; the gate is placed at the CATCH_UP -> STREAMING transition (lifecycle.caught_up)."""
    from . import lifecycle

    skip = lifecycle.not_streaming(pipeline.engine, pipeline.cid)
    return {f: s for f, s in want.items() if f not in skip}


def ensure(pipeline: Any, held: set[str], now: datetime | None = None, only: set[str] | None = None) -> list[str]:
    """Hold every scheduled STREAMING table that must be held now and is not (closed window, deferred table without a
    hold). Cheap; called before every chunk of the main stream. `only`: restrict to these tables (the handoff of one
    table). Returns ['hold:S.T', ...]."""
    want = _streaming_only(pipeline, scheduled_tables(pipeline.policy))
    if only is not None:
        want = {f: s for f, s in want.items() if f in only}
    if not want:
        return []
    now = now or utcnow()
    rows = {r["table_fqn"]: r for r in holds.rows(pipeline.engine, pipeline.cid, KIND)}
    acts: list[str] = []
    for fqn, sch in want.items():
        r = rows.get(fqn)
        if r and r["held"]:
            held.add(fqn)
            continue
        if r and (replay.pass_of(r) or {}):
            continue
        m = mode(sch)
        if m == "window" and _inside(sch, now):
            continue
        body: dict[str, Any] = {"mode": m, "window": sch.window} if m == "window" else {
            "mode": m, "next_due": (now + timedelta(seconds=float(sch.latency_target_s or 0))).isoformat()}
        _hold(pipeline, fqn, held, body)
        acts.append(f"hold:{fqn}")
    if acts:
        log.info("schedule holds", extra={"component": "alelt_hub", "correlation_id": f"{pipeline.cid}: {','.join(acts)}"})
    return acts


def tick(pipeline: Any, held: set[str], now: datetime | None = None) -> list[str]:
    """Hold / release scheduled tables of one pipeline (decode job, pipeline lock). Releases and deferred rounds are
    collected into ONE replay pass per kind of follow-up (attach / rehold) and run by a `replay` job.
    Returns actions, e.g. ['hold:S.T', 'release:S.T', 'round:S.T']."""
    now = now or utcnow()
    acts = ensure(pipeline, held, now)
    eng, cid = pipeline.engine, pipeline.cid
    want = _streaming_only(pipeline, scheduled_tables(pipeline.policy))
    rows = {r["table_fqn"]: r for r in holds.rows(eng, cid, KIND)}
    attach: list[tuple[str, str]] = []
    rounds: list[tuple[str, str]] = []
    for fqn, sch in want.items():
        r = rows.get(fqn)
        if not (r and r["held"]) or replay.pass_of(r) or integrity.blocks_replay(eng, cid, fqn):
            continue
        if mode(sch) == "window":
            if _inside(sch, now):
                attach.append((fqn, KIND))
                acts.append(f"release:{fqn}")
        else:
            due = datetime.fromisoformat(str((r["body"] or {}).get("next_due") or now.isoformat()))
            if now >= due:
                lat = float(sch.latency_target_s or 0)
                holds.set_body(eng, cid, fqn, KIND, next_due=(now + timedelta(seconds=lat)).isoformat())
                rounds.append((fqn, KIND))
                acts.append(f"round:{fqn}")
    for fqn, r in rows.items():                 # schedule removed / table disabled: release what is held
        if r["held"] and fqn not in want and not replay.pass_of(r) and not integrity.blocks_replay(eng, cid, fqn):
            attach.append((fqn, KIND))
            acts.append(f"release:{fqn}")
    replay.mark_pass(pipeline, attach, "attach")
    replay.mark_pass(pipeline, rounds, "rehold")
    if acts:
        log.info("schedule tick", extra={"component": "alelt_hub", "correlation_id": f"{cid}: {','.join(acts)}"})
    return acts


def _hold(pipeline: Any, fqn: str, held: set[str], body: dict[str, Any]) -> None:
    start = integrity.watermark_for_new_hold(pipeline, fqn)
    rseq = integrity.restart_for_new_hold(pipeline, fqn)
    body = {**body, "since": utcnow().isoformat()}
    if rseq is not None:
        body["restart_seq"] = rseq
    holds.put(pipeline.engine, pipeline.cid, fqn, KIND, "HELD", True, start, body)
    held.add(fqn)
