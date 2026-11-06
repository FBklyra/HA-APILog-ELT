"""Table lifecycle (RC2 / RC3, ADR-012): one row per enabled table (`table_lifecycle`), the single owner of its phase.

  NEW -> INITIAL_LOAD -> CATCH_UP -> STREAMING        (+ RESYNC, DISABLED)

| Phase | Meaning |
|---|---|
| NEW | enabled table known to the catalog, nothing applied: the main stream skips it (no table_hold row) |
| INITIAL_LOAD | snapshot hold placed at the start position; SNAPSHOT_TABLE / remote read running or waiting for the window |
| CATCH_UP | last chunk loaded: one replay pass from the start position (bulk, data lane) then the attach tail |
| STREAMING | the main stream applies the table; the schedule gate may hold it; completion may be pending |
| RESYNC | INITIAL_LOAD with resync (the snapshot replaces) |
| DISABLED | disabled in config / dropped at the source: holds released, nothing applied |

Transitions are written only by `move()`. Gates (`table_hold` kinds schedule / integrity / schema) stay orthogonal;
precedence (`gates()`): integrity > schema > initial load / catch-up / resync (ignore the schedule window: a schedule
is a latency class for CHANGES) > schedule (from the CATCH_UP -> STREAMING transition, placed in the same locked
section at the attach position) > completion (soft gate: certification / switch / settled verification only).

Entry points all go through `sync()`: catalog ingest (agent POST /catalog, remote runner), config revisions (file seed,
wizard, import, console), `allow_new_tables` + a new table, operator / integrity resync (SnapshotService.start).
Modes: `initial_load: auto` (default) | `none` (STREAMING at once, tables start empty) | `replay_from: <pos>` (test /
ops only: CATCH_UP from that position, no snapshot) | `legacy` (migration: flushed before the lifecycle existed).

`drive(worker, cid)` (control lane `lifecycle` job, every sweep) is idempotent and continues from the rows after a
restart: NEW -> start snapshots (bounded, stream-class tables first, then deferred, then window), INITIAL_LOAD with
the snapshot closed -> CATCH_UP, CATCH_UP -> one replay pass `then=attach`, unanswered SNAPSHOT_TABLE -> re-issued
with `key_from` (same snapshot id), attach done -> STREAMING (`caught_up`, under the pipeline lock).
"""
from __future__ import annotations

import logging
import os
import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import Connection, Engine, select, update

from alelt_common.models import TableRef
from alelt_common.util import utcnow

from . import jobs
from .db import table_lifecycle
from .dialect import insert

log = logging.getLogger("alelt_hub.lifecycle")
NEW, INITIAL_LOAD, CATCH_UP, STREAMING, RESYNC, DISABLED = (
    "NEW", "INITIAL_LOAD", "CATCH_UP", "STREAMING", "RESYNC", "DISABLED")
PHASES = (NEW, INITIAL_LOAD, CATCH_UP, STREAMING, RESYNC, DISABLED)
LOADING = (INITIAL_LOAD, RESYNC)


def _ref(fqn: str) -> TableRef:
    s, _, n = fqn.partition(".")
    return TableRef(schema=s, name=n)


def inflight_tables() -> int:
    return max(1, int(os.environ.get("ALELT_SNAPSHOT_INFLIGHT_TABLES", "8")))


def chunk_timeout_s() -> float:
    return float(os.environ.get("ALELT_SNAPSHOT_CHUNK_TIMEOUT_S", "600"))


def max_reissues() -> int:
    return int(os.environ.get("ALELT_SNAPSHOT_MAX_REISSUES", "5"))


def default_mode() -> str:
    v = os.environ.get("ALELT_INITIAL_LOAD", "auto").strip().lower()
    return "none" if v in ("none", "off", "false", "no") else "auto"


def mode_for(policy: Any, ref: TableRef) -> str:
    """Per table `initial_load` / `replay_from`, else the connection's, else ALELT_INITIAL_LOAD (default auto)."""
    tp = policy.table(ref)
    rf = getattr(tp, "replay_from", None) or getattr(policy, "replay_from", None)
    if rf:
        return f"replay_from:{rf}"
    v = (getattr(tp, "initial_load", None) or getattr(policy, "initial_load", None) or default_mode())
    return "none" if str(v).strip().lower() in ("none", "off", "false", "no") else "auto"


# ------------------------------------------------------------------ queries
def rows(engine: Engine, cid: str) -> list[dict[str, Any]]:
    with engine.connect() as c:
        return [dict(r) for r in c.execute(select(table_lifecycle).where(table_lifecycle.c.connection_id == cid)
                                           .order_by(table_lifecycle.c.table_fqn)).mappings()]


def get(engine: Engine, cid: str, fqn: str) -> dict[str, Any] | None:
    with engine.connect() as c:
        r = c.execute(select(table_lifecycle).where(table_lifecycle.c.connection_id == cid,
                                                    table_lifecycle.c.table_fqn == fqn)).mappings().first()
    return dict(r) if r else None


def phase_of(engine: Engine, cid: str, fqn: str) -> str | None:
    r = get(engine, cid, fqn)
    return str(r["phase"]) if r else None


def not_streaming(engine: Engine, cid: str) -> set[str]:
    """Tables whose phase is not STREAMING (schedule gates and completion skip them)."""
    with engine.connect() as c:
        return {str(f) for f, in c.execute(select(table_lifecycle.c.table_fqn).where(
            table_lifecycle.c.connection_id == cid, table_lifecycle.c.phase != STREAMING))}


def unapplied(engine: Engine, cid: str) -> set[str]:
    """Tables the main stream must not apply without a table_hold row: NEW (initial load not placed yet) and
    DISABLED. INITIAL_LOAD / CATCH_UP / RESYNC are held by their snapshot hold."""
    with engine.connect() as c:
        return {str(f) for f, in c.execute(select(table_lifecycle.c.table_fqn).where(
            table_lifecycle.c.connection_id == cid, table_lifecycle.c.phase.in_((NEW, DISABLED))))}


# ------------------------------------------------------------------ the one writer of `phase`
def move(eng_or_c: Engine | Connection, cid: str, fqn: str, phase: str, **fields: Any) -> None:
    """Set the phase of a table (creating the row if needed). `fields`: start_pos, snapshot_id, mode, verified_at,
    resync_count, body (merged)."""
    if phase not in PHASES:
        raise ValueError(f"unknown lifecycle phase {phase}")

    def run(c: Connection) -> None:
        cur = c.execute(select(table_lifecycle).where(table_lifecycle.c.connection_id == cid,
                                                      table_lifecycle.c.table_fqn == fqn)).mappings().first()
        body = {**((cur or {}).get("body") or {}), **(fields.pop("body", None) or {})}
        vals: dict[str, Any] = {"phase": phase, "updated_at": utcnow(), "body": body, **fields}
        if cur is None or cur["phase"] != phase:
            vals["since"] = utcnow()
            hist = list(body.get("history") or [])[-9:]
            hist.append({"phase": phase, "at": utcnow().isoformat()})
            body["history"] = hist
        if cur is None:
            c.execute(insert(table_lifecycle).values(connection_id=cid, table_fqn=fqn, **vals)
                      .on_conflict_do_update(index_elements=["connection_id", "table_fqn"], set_=vals))
        else:
            c.execute(update(table_lifecycle).where(table_lifecycle.c.connection_id == cid,
                                                    table_lifecycle.c.table_fqn == fqn).values(**vals))
    if isinstance(eng_or_c, Connection):
        run(eng_or_c)
    else:
        with eng_or_c.begin() as c:
            run(c)
    log.info("lifecycle %s -> %s", fqn, phase, extra={"component": "alelt_hub", "table": fqn, "correlation_id": cid})


def wake(eng_or_c: Engine | Connection, cid: str, delay_s: float = 0) -> None:
    """Queue the lifecycle driver of `cid` (one marker per connection)."""
    def run(c: Connection) -> None:
        jobs.enqueue(c, "lifecycle", {"connection_id": cid}, dedup_key=jobs.marker_key("lifecycle", cid),
                     delay_s=delay_s)
    if isinstance(eng_or_c, Connection):
        run(eng_or_c)
    else:
        with eng_or_c.begin() as c:
            run(c)


# ------------------------------------------------------------------ entry points
def _enabled_tables(c: Connection, cid: str, policy: Any) -> dict[str, Any]:
    from . import store

    vid = store.latest_catalog_id(c, cid)
    if vid is None:
        return {}
    cat = store.load_catalog(c, vid)
    flagged = any(t.capture_enabled for t in cat.tables)      # sources that report capture (Db2 DATA CAPTURE)
    return {t.table.fqn: t for t in cat.tables if policy.table(t.table).enabled
            and (t.capture_enabled or not flagged)}


def sync(c: Connection, cid: str, reason: str = "sync", policy: Any = None) -> list[str]:
    """Every enabled table of the newest catalog gets a row (NEW, or STREAMING for `initial_load: none`); a table that
    is no longer enabled / no longer in the catalog becomes DISABLED (its holds are released); a re-enabled table
    starts over (NEW, fresh initial load). Same transaction as the caller (catalog ingest / config revision)."""
    from . import store
    from .settings import get_settings

    if policy is None:
        try:
            policy = store.connection_policy(c, cid, get_settings().tables_config)
        except Exception:  # noqa: BLE001 - no usable policy yet: nothing to decide
            log.exception("lifecycle sync: no policy for %s", cid, extra={"component": "alelt_hub"})
            return []
    tabs = _enabled_tables(c, cid, policy)
    if not tabs and c.execute(select(table_lifecycle.c.table_fqn).where(
            table_lifecycle.c.connection_id == cid).limit(1)).first() is None:
        return []
    cur = {str(r["table_fqn"]): dict(r) for r in c.execute(select(table_lifecycle).where(
        table_lifecycle.c.connection_id == cid)).mappings()}
    acts: list[str] = []
    for fqn, info in tabs.items():
        r = cur.get(fqn)
        if r is not None and r["phase"] != DISABLED:
            continue
        mode = mode_for(policy, info.table)
        phase = STREAMING if mode == "none" else NEW
        move(c, cid, fqn, phase, mode=mode, start_pos=None, snapshot_id=None, verified_at=None,
             body={"registered_by": reason})
        acts.append(f"{phase.lower()}:{fqn}")
    for fqn, r in cur.items():
        if fqn not in tabs and r["phase"] != DISABLED:
            move(c, cid, fqn, DISABLED, body={"disabled_by": reason})
            _release_holds(c, cid, fqn)
            acts.append(f"disabled:{fqn}")
    if acts:
        wake(c, cid)
        log.info("lifecycle sync (%s): %s", reason, ", ".join(acts[:20]) + (" ..." if len(acts) > 20 else ""),
                 extra={"component": "alelt_hub", "correlation_id": cid})
    return acts


def _release_holds(c: Connection, cid: str, fqn: str) -> None:
    from .db import table_hold

    c.execute(update(table_hold).where(table_hold.c.connection_id == cid, table_hold.c.table_fqn == fqn,
                                       table_hold.c.kind.in_(("snapshot", "schedule")))
              .values(held=False, updated_at=utcnow()))


def sync_catalog(c: Connection, cid: str) -> list[str]:
    return sync(c, cid, "catalog")


def sync_policy(c: Connection, cid: str) -> list[str]:
    return sync(c, cid, "config revision")


def resync(engine: Engine, worker: Any, cid: str, fqn: str, reason: str) -> list[str]:
    """Operator / integrity resync of one table: RESYNC (snapshot replaces every value), then catch-up."""
    from .snapshots import SnapshotService

    return SnapshotService(engine, worker.pipeline(cid)).start([fqn], resync=True, reason=reason)


def gates(engine: Engine, cid: str, fqn: str) -> list[str]:
    """Active gates of a table in precedence order (for the console and the harness)."""
    from . import completion, holds

    out = []
    for kind in ("integrity", "schema", "snapshot", "schedule"):
        h = holds.get(engine, cid, fqn, kind)
        if h and h["held"]:
            out.append(kind)
    if completion.blocking(engine, cid, fqn):
        out.append("completion")
    return out


# ------------------------------------------------------------------ the driver (control lane)
def _class_rank(policy: Any, fqn: str) -> int:
    from . import schedule

    m = schedule.mode(policy.table(_ref(fqn)).schedule)
    return {"stream": 0, "deferred": 1, "window": 2}.get(m, 0)


def drive(engine: Engine, worker: Any, cid: str) -> list[str]:
    """One idempotent step of every table of `cid`. Returns actions (logs / tests)."""
    from alelt_snapshot import TableState

    from . import holds, integrity, replay
    from .snapshots import PipelineBusy, SnapshotService

    lc = rows(engine, cid)
    if not lc:
        return []
    p = worker.pipeline(cid)
    snaps = {r["table_fqn"]: r for r in holds.rows(engine, cid, "snapshot")}
    acts: list[str] = []
    # 1. snapshot closed (every chunk loaded) -> CATCH_UP; finished catch-up after a crash -> STREAMING
    for r in lc:
        fqn, ph = str(r["table_fqn"]), str(r["phase"])
        h = snaps.get(fqn)
        hb = (h or {}).get("body") or {}
        if ph in LOADING and h is not None and h["state"] == TableState.CATCHING_UP.value:
            move(engine, cid, fqn, CATCH_UP, start_pos=h["start_pos"])
            r["phase"] = CATCH_UP
            acts.append(f"catch_up:{fqn}")
        if r["phase"] == CATCH_UP and h is not None and hb.get("catchup_done") and \
                hb.get("catchup_done") == hb.get("snapshot_id"):
            # the catch-up was applied (attach tail, or an integrity recovery replay) but the handoff did not run:
            # run it now under the pipeline lock (schedule gate at the flushed position, verification queued)
            try:
                with_pipeline_lock(engine, cid, lambda f=fqn: caught_up(p, f, p.refresh_held()))
            except PipelineBusy:
                wake(engine, cid, 2)
                continue
            r["phase"] = STREAMING
            acts.append(f"streaming:{fqn}")
    # 2. CATCH_UP -> one replay pass (bulk in the data lane, attach tail under the pipeline lock)
    items = []
    for r in lc:
        fqn = str(r["table_fqn"])
        if r["phase"] != CATCH_UP:
            continue
        h = snaps.get(fqn)
        if h is None and str(r["mode"]).startswith("replay_from:"):
            _place_replay_hold(p, fqn, str(r["mode"]).partition(":")[2])
            h = holds.get(engine, cid, fqn, "snapshot")
        if h is None or replay.pass_of(h) or integrity.blocks_replay(engine, cid, fqn, ("snapshot",)):
            continue
        items.append((fqn, "snapshot"))
    if items:
        pid = replay.mark_pass(p, items, "attach")
        if pid:
            acts.append(f"pass:{pid}:{len(items)}")
    # 3. snapshots in flight: re-issue unanswered / failed SNAPSHOT_TABLE with key_from (same snapshot id)
    acts += _reissue(engine, p, lc, snaps)
    # 4. NEW / RESYNC without a running snapshot -> start (bounded per agent, stream class first)
    loading = sum(1 for r in lc if r["phase"] in LOADING and (snaps.get(r["table_fqn"]) or {}).get("state")
                  in (TableState.SNAPSHOTTING.value, TableState.RESYNC.value))
    room = inflight_tables() - loading
    todo: list[tuple[str, bool]] = []
    for r in lc:
        fqn, ph, mode = str(r["table_fqn"]), str(r["phase"]), str(r["mode"])
        if ph == NEW and mode == "none":
            move(engine, cid, fqn, STREAMING)
            acts.append(f"streaming:{fqn}")
        elif ph == NEW and mode.startswith("replay_from:"):
            _place_replay_hold(p, fqn, mode.partition(":")[2])
            move(engine, cid, fqn, CATCH_UP, start_pos=mode.partition(":")[2])
            acts.append(f"replay_from:{fqn}")
        elif ph == NEW:
            todo.append((fqn, False))
        elif ph == RESYNC and (snaps.get(fqn) or {}).get("state") not in (TableState.SNAPSHOTTING.value,
                                                                          TableState.RESYNC.value,
                                                                          TableState.CATCHING_UP.value):
            todo.append((fqn, True))
    todo.sort(key=lambda x: (_class_rank(p.policy, x[0]), x[0]))
    if todo and room > 0:
        svc = SnapshotService(engine, p)
        if not svc.remote and svc._edge_agent() is None:
            acts.append("waiting:agent")
        else:
            for resync_, group in ((False, [f for f, rs in todo if not rs]), (True, [f for f, rs in todo if rs])):
                group = group[:room]
                if not group:
                    continue
                try:
                    started = svc.start(group, resync=resync_, reason="lifecycle " + ("resync" if resync_ else
                                                                                       "initial load"))
                except PipelineBusy:
                    wake(engine, cid, 2)
                    acts.append("busy")
                    break
                except Exception as e:  # noqa: BLE001 - retried by the next drive; tables stay unapplied meanwhile
                    log.warning("initial load not started: %s", e, extra={"component": "alelt_hub", "correlation_id": cid})
                    acts.append(f"error:{type(e).__name__}")
                    break
                room -= len(started)
                acts += [f"{'resync' if resync_ else 'initial_load'}:{f}" for f in started]
    if acts:
        log.info("lifecycle drive: %s", ", ".join(acts[:20]), extra={"component": "alelt_hub", "correlation_id": cid})
    # no self-wake: events wake the driver (catalog / config sync, last chunk loaded, snapshot step, handoff, busy
    # retry) and the worker sweep re-drives every connection with tables not STREAMING (agent offline, window)
    return acts


def with_pipeline_lock(engine: Engine, cid: str, fn: Any) -> Any:
    """Run `fn` under `pipeline:<cid>` (cooperative yield: the running slice ends at its next chunk)."""
    from .dialect import unlock
    from .snapshots import SnapshotService

    lk = SnapshotService._lock_for(engine, cid)
    try:
        return fn()
    finally:
        unlock(lk, f"pipeline:{cid}")


def _place_replay_hold(p: Any, fqn: str, pos: str) -> None:
    """`replay_from:<pos>` (test / ops): a snapshot-kind hold without a snapshot, CATCHING_UP from `pos`, so the same
    catch-up pass applies the table's history from the log."""
    from . import holds

    if holds.get(p.engine, p.cid, fqn, "snapshot") is not None:
        return
    sid = str(uuid.uuid4())
    holds.put(p.engine, p.cid, fqn, "snapshot", "CATCHING_UP", True, pos,
              {"snapshot_id": sid, "table": fqn, "state": "CATCHING_UP", "replay_from": pos, "kind": "replay_from"})


def _reissue(engine: Engine, p: Any, lc: list[dict[str, Any]], snaps: dict[str, dict[str, Any]]) -> list[str]:
    """§2.4 / §4.4: an edge SNAPSHOT_TABLE that FAILED / EXPIRED, or whose next chunk did not arrive within
    ALELT_SNAPSHOT_CHUNK_TIMEOUT_S, is re-issued with `key_from` = last_key of the last contiguous LOADED chunk and the
    SAME snapshot id (start_pos stays the original: older = safe). After ALELT_SNAPSHOT_MAX_REISSUES the snapshot is
    paused with `last_error` (terminal for the harness)."""
    from . import holds, store
    from .db import agent_command, snapshot_chunk
    from alelt_common.models import CommandName
    from alelt_snapshot import TableState

    if getattr(p.source, "capture_mode", "edge") == "remote":
        return []
    acts: list[str] = []
    for r in lc:
        fqn = str(r["table_fqn"])
        h = snaps.get(fqn)
        if r["phase"] not in LOADING or h is None or h["state"] not in (TableState.SNAPSHOTTING.value,
                                                                         TableState.RESYNC.value):
            continue
        b = h["body"] or {}
        cmd = b.get("agent_command_id")
        if not cmd or b.get("paused"):
            continue
        with engine.connect() as c:
            st = c.execute(select(agent_command.c.status, agent_command.c.result, agent_command.c.issued_at)
                           .where(agent_command.c.id == uuid.UUID(cmd))).first()
            ch = c.execute(select(snapshot_chunk.c.chunk_no, snapshot_chunk.c.status, snapshot_chunk.c.header,
                                  snapshot_chunk.c.received_at).where(
                snapshot_chunk.c.snapshot_id == uuid.UUID(str(b["snapshot_id"])))).all()
        if st is None:
            continue
        status, result, issued = st[0], st[1] or {}, st[2]
        last_seen = max([_aware(x[3]) for x in ch if x[3] is not None] + [_aware(issued)], default=None)
        stale = last_seen is not None and utcnow() - last_seen > timedelta(seconds=chunk_timeout_s())
        failed = status in ("FAILED", "EXPIRED")
        if not (failed or (stale and status in ("PENDING", "SENT", "ACKED"))):
            continue
        if status == "ACKED" and any(bool((x[2] or {}).get("last")) for x in ch):
            continue                                    # complete: the loads are still running
        code = str(result.get("code") or "")
        if failed and code in ("NOT_ALLOWED", "NOT_FOUND", "NO_KEY", "UNSUPPORTED"):
            continue                                    # refused for good: SnapshotService pauses it (operator)
        n = int(b.get("reissues") or 0)
        if n >= max_reissues():
            holds.modify(engine, p.cid, fqn, "snapshot", paused=True,
                         last_error=f"SNAPSHOT_TABLE re-issued {n} times without completing ({status} {code})")
            acts.append(f"paused:{fqn}")
            continue
        loaded = sorted(int(x[0]) for x in ch if x[1] == "LOADED")
        upto = -1
        for k in loaded:
            if k != upto + 1:
                break
            upto = k
        key_from = None
        if upto >= 0:
            hdr = next((x[2] for x in ch if int(x[0]) == upto), None) or {}
            key_from = hdr.get("last_key")
            if key_from is None:                         # agent without last_key: restart the table from the top
                upto = -1
        with engine.begin() as c:                       # chunks after the resume point are read again
            c.execute(snapshot_chunk.delete().where(snapshot_chunk.c.snapshot_id == uuid.UUID(str(b["snapshot_id"])),
                                                    snapshot_chunk.c.chunk_no > upto))
            if not failed:
                c.execute(update(agent_command).where(agent_command.c.id == uuid.UUID(cmd),
                                                      agent_command.c.status.in_(("PENDING", "SENT")))
                          .values(status="EXPIRED", acked_at=utcnow()))
            args: dict[str, Any] = {"table": {"schema": _ref(fqn).schema_, "name": _ref(fqn).name},
                                    "snapshot_id": str(b["snapshot_id"]),
                                    "chunk_rows": int(((b.get("plan") or {}).get("chunk_rows")) or 50_000)}
            if key_from is not None:
                args["key_from"] = key_from
                args["chunk_from"] = upto + 1
            aid = c.execute(select(agent_command.c.agent_id).where(agent_command.c.id == uuid.UUID(cmd))).scalar()
            new = store.issue_command(c, str(aid), CommandName.SNAPSHOT_TABLE, args, "system:lifecycle",
                                      reason=f"resume snapshot of {fqn} ({status})")
        holds.modify(engine, p.cid, fqn, "snapshot", agent_command_id=str(new.command_id), reissues=n + 1,
                     dispatched_at=utcnow().isoformat(), resumed_from=key_from)
        acts.append(f"reissue:{fqn}:{n + 1}")
    return acts


def _aware(t: Any) -> Any:
    return t if t is None or t.tzinfo else t.replace(tzinfo=utcnow().tzinfo)


# ------------------------------------------------------------------ handoff (pipeline lock, replay.release)
def caught_up(p: Any, fqn: str, held: set[str]) -> None:
    """The catch-up attach tail of `fqn` ran under the pipeline lock: the table rejoins the main stream at the
    flushed position P. In the same locked section the schedule gate is placed at P (exact boundary, §2.3 item 4),
    then the post-snapshot reconciliation is queued (sets `verified_at` on MATCH)."""
    from . import holds, schedule
    from .edge_verify import request_post_snapshot_verify

    h = holds.get(p.engine, p.cid, fqn, "snapshot")
    sid = ((h or {}).get("body") or {}).get("snapshot_id")
    if h is not None:
        holds.modify(p.engine, p.cid, fqn, "snapshot", held=False, catchup_done=sid)
    move(p.engine, p.cid, fqn, STREAMING)
    p.refresh_held()
    schedule.ensure(p, held, only={fqn})
    if ((h or {}).get("body") or {}).get("kind") == "replay_from":
        verified(p.engine, p.cid, fqn, "replay_from: no snapshot to reconcile")
    else:
        request_post_snapshot_verify(p.engine, p.cid, fqn)
    wake(p.engine, p.cid)


def verified(engine: Engine, cid: str, fqn: str, detail: str = "MATCH") -> None:
    r = get(engine, cid, fqn)
    if r is None:
        return
    move(engine, cid, fqn, str(r["phase"]), verified_at=utcnow(), body={"verify": detail[:300]})


__all__ = ["CATCH_UP", "DISABLED", "INITIAL_LOAD", "NEW", "PHASES", "RESYNC", "STREAMING", "caught_up", "drive",
           "gates", "get", "mode_for", "move", "not_streaming", "resync", "rows", "sync", "sync_catalog",
           "sync_policy", "unapplied", "verified", "wake"]
