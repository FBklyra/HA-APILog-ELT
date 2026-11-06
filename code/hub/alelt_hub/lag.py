"""Capture lag that an idle source cannot fake (HC-16).

`now - last_commit_ts` grows forever on a source with no commits, raising LAG_ABOVE_TARGET every minute and failing
latency criteria on a quiet weekend. Lag is therefore:

- 0 when the connection is caught up: no received batch is waiting (RECEIVED / DECODED) AND the source is alive
  (an agent heart-beating within ALELT_LAG_ALIVE_S (300 s) in state CAPTURING / IDLE with an empty spool; without
  agent rows: a batch received within that window) - an idle source is not late;
- else the age of the newest applied commit (`last_commit_ts`), but at least the age of the oldest waiting batch
  (work received but not applied is late by at least that long);
- None when nothing is known yet (no checkpoint, no waiting batch).

A source that stopped sending is reported by its own incidents (agent offline / capture stuck), and once it is not
alive the classic commit age applies again.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Connection, func, select

from alelt_common.util import utcnow

from .db import agent, batch


CAUGHT_UP_STATES = ("CAPTURING", "IDLE")


def _aware(v: datetime | None) -> datetime | None:
    if v is None:
        return None
    return v if v.tzinfo else v.replace(tzinfo=utcnow().tzinfo)


def alive_s() -> float:
    return float(os.environ.get("ALELT_LAG_ALIVE_S", "300"))


def effective_lag_s(c: Connection, cid: str, last_commit_ts: datetime | None, now: datetime | None = None) -> float | None:
    now = now or utcnow()
    oldest_wait = _aware(c.execute(select(func.min(batch.c.received_at)).where(
        batch.c.connection_id == cid, batch.c.status.in_(("RECEIVED", "DECODED")))).scalar())
    since = now - timedelta(seconds=alive_s())
    agents = [(_aware(hb), st, sp) for hb, st, sp in c.execute(select(
        agent.c.last_heartbeat, agent.c.status, agent.c.spool_bytes).where(agent.c.connection_id == cid))]
    recent = [a for a in agents if a[0] is not None and a[0] >= since]
    last_rx = _aware(c.execute(select(func.max(batch.c.received_at)).where(batch.c.connection_id == cid)).scalar())
    if recent:      # an agent speaks for the source: caught up only while it captures normally with an empty spool
        alive = any(st in CAUGHT_UP_STATES and not sp for _, st, sp in recent)
    else:
        alive = last_rx is not None and last_rx >= since
    if oldest_wait is None and alive:
        return 0.0
    ts = _aware(last_commit_ts)
    lag = (now - ts).total_seconds() if ts is not None else None
    if oldest_wait is not None:
        w = (now - oldest_wait).total_seconds()
        lag = w if lag is None else max(lag, w)
    return None if lag is None else max(0.0, lag)


def lags(c: Connection, rows: Any) -> dict[str, float | None]:
    """{connection_id: lag} for (connection_id, last_commit_ts) rows; the worst stream of a connection wins."""
    out: dict[str, float | None] = {}
    for cid, ts in rows:
        v = effective_lag_s(c, str(cid), ts)
        prev = out.get(str(cid))
        out[str(cid)] = v if prev is None else (prev if v is None else max(prev, v))
    return out
