"""Per-table hold when the destination DDL (`ensure_table`) of one table fails (DST-loader-01, hub side).

The loader raises `TableDDLError` naming the table; the main stream then holds ONLY that table (kind `ddl`, watermark =
what the main stream applied for it) and flushes the others again, so one bad table never stalls the connection.
The decode job retries the DDL with exponential backoff (5 s .. 15 min); when it succeeds the table rejoins through an
attach pass (its changes since the watermark replayed exactly once), the same path as schedule / schema holds.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any

from alelt_common.models import TableRef
from alelt_common.util import utcnow

from . import holds
from .metrics import REGISTRY

log = logging.getLogger("alelt_hub")

KIND = "ddl"
STATE = "DDL_FAILED"


def _delay(n: int) -> float:
    return min(900.0, 5.0 * 2 ** max(0, n - 1))


def _wake(p: Any, delay_s: float) -> None:
    """A `schema` job after the backoff runs retry_due even when no new batch arrives."""
    from . import jobs

    with p.engine.begin() as c:                 # the DDL retry runs in the control lane (`schema` job)
        jobs.enqueue(c, "schema", {"connection_id": p.cid}, delay_s=delay_s,
                     dedup_key=f"ddl:{p.cid}:{int(time.time() // max(1.0, delay_s))}:{delay_s:.0f}")


def on_ddl_error(p: Any, err: Any, held: set[str]) -> None:
    """Hold err.table (the flush applied nothing). Idempotent."""
    from . import integrity

    fqn = err.table.fqn
    r = holds.get(p.engine, p.cid, fqn, KIND)
    if r is not None and r["held"]:
        held.add(fqn)
        return
    start = integrity.watermark_for_new_hold(p, fqn)
    rseq = integrity.restart_for_new_hold(p, fqn)
    n = int(getattr(err, "attempts", 1) or 1)
    body: dict[str, Any] = {"reason": f"destination DDL failed for {err.dest}: {err.error}"[:500], "dest": err.dest,
                            "attempts": n, "since": utcnow().isoformat(),
                            "retry_at": (utcnow() + timedelta(seconds=_delay(n))).isoformat()}
    if rseq is not None:
        body["restart_seq"] = rseq
    holds.put(p.engine, p.cid, fqn, KIND, STATE, True, start, body)
    held.add(fqn)
    REGISTRY.inc("alelt_destination_ddl_failures_total", connection_id=p.cid, table=fqn)
    _wake(p, _delay(n))
    log.error("destination DDL failed: table held, other tables keep flowing",
              extra={"component": "alelt_hub", "table": fqn, "correlation_id": body["reason"][:200]})


def retry_due(p: Any, held: set[str], now: datetime | None = None) -> list[str]:
    """Control lane (`schema` job): retry the DDL of every due `ddl` hold (held meanwhile); success -> attach pass."""
    from . import replay

    now = now or utcnow()
    released: list[str] = []
    for r in holds.rows(p.engine, p.cid, KIND):
        if not r["held"] or replay.pass_of(r):
            continue
        body = r["body"] or {}
        due = body.get("retry_at")
        if due and datetime.fromisoformat(due) > now:
            continue
        fqn = r["table_fqn"]
        s, _, n = fqn.partition(".")
        ref = TableRef(schema=s, name=n)
        info = p.tables.get(ref)
        if info is None:
            continue
        dest = p.policy.dest_name(ref)
        try:
            p.loader.dest.ensure_table(p.policy.project(info), dest, None)
        except Exception as e:  # noqa: BLE001 - recorded on the hold, retried later
            k = int(body.get("attempts") or 1) + 1
            holds.modify(p.engine, p.cid, fqn, KIND, attempts=k, reason=f"destination DDL failed for {dest}: "
                         f"{type(e).__name__}: {e}"[:500], retry_at=(now + timedelta(seconds=_delay(k))).isoformat())
            _wake(p, _delay(k))
            continue
        getattr(p.loader, "_ddl_fail", {}).pop(dest, None)      # loader backoff state for this table is stale now
        replay.mark_pass(p, [(fqn, KIND)], "attach")
        released.append(fqn)
        log.warning("destination DDL succeeded: table rejoins via attach pass",
                    extra={"component": "alelt_hub", "table": fqn, "correlation_id": f"{time.time():.0f}"})
    return released


__all__ = ["KIND", "on_ddl_error", "retry_due"]
