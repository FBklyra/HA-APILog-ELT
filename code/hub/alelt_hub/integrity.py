"""Per-table integrity state machine (F-01 / X-120): an unexplained quarantined record never silently diverges a table.

States (table_hold kind `integrity`, one row per table that ever left IN_SYNC; `*` = connection-wide row):

  IN_SYNC      normal; no row, or a released row.
  SUSPECT      an unexplained record of the table was quarantined. `hold_table` (default): the table is held from
               the last position the main stream applied for it (its watermark never passes the record), its later
               changes stay in the spool, certification + switch are blocked, an incident TABLE_OUT_OF_SYNC opens and
               automatic recovery starts. `continue` (explicit opt-in): not held, but SUSPECT, loud (critical incident,
               metric, error log) and certification/switch blocked until a resync / operator acknowledgement.
  REDECODING   recovery step 1: catalog refreshed (remote: read live; edge: SNAPSHOT_CATALOG), the held range is
               re-decoded with the newest catalog; when no record of the table is quarantined any more the range is
               applied (bulk replay job) and the table re-attaches (tail under the pipeline lock) -> IN_SYNC.
  OUT_OF_SYNC  re-decode still quarantines: step 2 resync (snapshot + catch-up), else operator action.
  RESYNCING    a resync snapshot owns the table (snapshot hold); IN_SYNC when the snapshot job reaches IN_SYNC.

Policy: tables.yaml `on_quarantine: hold_table | stop_stream | continue` (connection default, per table override).
`stop_stream`: the whole connection stops before the offending batch (row `*`, held) until an operator resumes it; on
resume the offending tables become ordinary `hold_table` holds with automatic recovery.

Quarantines without a table (the decoder could not attribute the record) put the connection row `*` into SUSPECT
(not held): certification of every table of the connection is blocked until the decoder attributes such records or
an operator acknowledges them. Source-policy QUARANTINE_SKIP records are explained (chosen by an operator): no hold.

Out-of-row values (`needs_keyed_reread` signals WITH keys) are NOT integrity events: they are completion work in the
data path (alelt_hub.completion, ADR-013). Integrity keeps every unexplained record and the other signal classes
(`unsupported`, `needs_catalog_refresh`, a keyed re-read signal without keys - routed like `unsupported`: resync), and
the tables whose completion cannot converge (`completion_unavailable`: no fetch capability, no key, overflow).

Engine hooks (`RecoveryHooks`; a SourceConnector may expose `recovery_hooks`, or `register_hooks(engine, hooks)`):
  refresh_catalog(engine, pipeline, fqn) -> "done" | "requested" | "unavailable"
  verify_table(engine, pipeline, fqn) -> "MATCH" | "MISMATCH" | "pending" | None      (edge stream: VERIFY_TABLE)
"""
from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import Connection, Engine, func, select, update

from alelt_common.models import CommandName, Position, TableRef
from alelt_common.util import utcnow

from . import holds, jobs
from .db import agent, agent_command, catalog_version, quarantine_event, table_hold
from .metrics import REGISTRY

log = logging.getLogger("alelt_hub.integrity")
KIND = "integrity"
CONN = "*"
IN_SYNC, SUSPECT, REDECODING, OUT_OF_SYNC, RESYNCING = "IN_SYNC", "SUSPECT", "REDECODING", "OUT_OF_SYNC", "RESYNCING"
STATES = (IN_SYNC, SUSPECT, REDECODING, OUT_OF_SYNC, RESYNCING)
ACTOR = "system:integrity"


class StreamStopped(RuntimeError):
    """on_quarantine: stop_stream - the connection stopped before the offending batch."""


def _wait_s(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


def _ref(fqn: str) -> TableRef:
    s, _, n = fqn.partition(".")
    return TableRef(schema=s, name=n)


# ------------------------------------------------------------------ hooks
class RecoveryHooks:
    """Default recovery hooks (remote sources read live from the hub; edge sources go through whitelisted agent
    commands, R4/R5). Engine streams override single methods."""

    def refresh_catalog(self, engine: Engine, pipeline: Any, fqn: str) -> str:
        if getattr(pipeline.source, "capture_mode", "edge") == "remote":
            from alelt_capture.config import connections
            from alelt_capture.receipt import ingest_catalog

            rc = connections(engine).get(pipeline.cid)
            if rc is None:
                return "unavailable"
            src = rc.factory()()
            try:
                ingest_catalog(engine, src.read_catalog())
            finally:
                src.close()
            return "done"
        from . import store

        with engine.begin() as c:
            aid = c.execute(select(agent.c.id).where(agent.c.connection_id == pipeline.cid,
                                                     ~agent.c.id.like("capture:%"))
                            .order_by(agent.c.last_heartbeat.desc()).limit(1)).scalar()
            if aid is None:
                return "unavailable"
            cmd = store.issue_command(c, str(aid), CommandName.SNAPSHOT_CATALOG, {}, ACTOR,
                                      reason=f"integrity: re-decode {fqn}")
        holds.set_body(engine, pipeline.cid, fqn, KIND, catalog_command=str(cmd.command_id))
        return "requested"

    def verify_table(self, engine: Engine, pipeline: Any, fqn: str) -> str | None:
        if getattr(pipeline.source, "capture_mode", "edge") != "remote":
            return None                   # edge: VERIFY_TABLE through the agent (edge stream hook)
        from .ops import verify_table

        return str(verify_table(engine, pipeline, fqn).outcome)


_HOOKS: dict[str, Any] = {}
_DEFAULT = RecoveryHooks()


def register_hooks(engine_name: str, hooks: Any) -> None:
    _HOOKS[engine_name] = hooks


def _hook(pipeline: Any, name: str) -> Any:
    for h in (getattr(pipeline.source, "recovery_hooks", None), _HOOKS.get(pipeline.engine_name)):
        fn = getattr(h, name, None) if h is not None else None
        if callable(fn):
            return fn
    return getattr(_DEFAULT, name)


# ------------------------------------------------------------------ queries
def row(engine: Engine, cid: str, fqn: str) -> dict[str, Any] | None:
    return holds.get(engine, cid, fqn, KIND)


def state_of(engine: Engine, cid: str, fqn: str) -> str:
    r = row(engine, cid, fqn)
    return str(r["state"]) if r else IN_SYNC


def blocks_replay(engine: Engine, cid: str, fqn: str, except_kinds: tuple[str, ...] = ()) -> bool:
    """An integrity hold (or a schema change waiting for approval, HC-10) freezes the table watermark: no schedule /
    catch-up replay may pass it. `except_kinds`: the kinds the calling pass itself is releasing."""
    for kind in (KIND, "schema"):
        if kind in except_kinds:
            continue
        r = holds.get(engine, cid, fqn, kind)
        if r and r["held"]:
            return True
    return False


def blocking(c: Connection, cid: str, fqn: str) -> str | None:
    """Why certification / switch must be refused for the table (None = integrity OK)."""
    rs = c.execute(select(table_hold.c.table_fqn, table_hold.c.state, table_hold.c.body).where(
        table_hold.c.connection_id == cid, table_hold.c.kind == KIND,
        table_hold.c.table_fqn.in_([fqn, CONN]))).all()
    for f, st, body in rs:
        if st != IN_SYNC:
            what = "connection" if f == CONN else "table"
            return f"{what} integrity {st}: {((body or {}).get('reason') or '')[:200]}"
    return None


def table_states(c: Connection, cid: str | None = None) -> dict[tuple[str, str], dict[str, Any]]:
    """(connection, fqn) -> {integrity, held_by, holds, phase, verified_at, pending_completions} for console /
    metrics / harness diagnosis."""
    q = select(table_hold)
    if cid is not None:
        q = q.where(table_hold.c.connection_id == cid)
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for r in c.execute(q).mappings():
        d = out.setdefault((r["connection_id"], r["table_fqn"]), {"integrity": IN_SYNC, "held_by": [], "holds": []})
        b = r["body"] or {}
        if r["kind"] == KIND:
            d["integrity"] = r["state"]
            d["reason"] = b.get("reason")
            d["incident_id"] = b.get("incident_id")
            d["since"] = b.get("since")
            d["step"] = b.get("step")
        if r["held"]:
            d["held_by"].append(r["kind"])
        d["holds"].append({"kind": r["kind"], "state": r["state"], "held": bool(r["held"]),
                           "start_pos": r["start_pos"], "since": b.get("since"),
                           "pass": (b.get("pass") or {}).get("phase")})
    from .db import table_completion, table_lifecycle

    def blank() -> dict[str, Any]:
        return {"integrity": IN_SYNC, "held_by": [], "holds": []}
    q = select(table_completion.c.connection_id, table_completion.c.table_fqn, func.count()).where(
        table_completion.c.status != "CANCELLED")
    if cid is not None:
        q = q.where(table_completion.c.connection_id == cid)
    for a, f, n in c.execute(q.group_by(table_completion.c.connection_id, table_completion.c.table_fqn)):
        out.setdefault((a, f), blank())["pending_completions"] = int(n)
    q2 = select(table_lifecycle.c.connection_id, table_lifecycle.c.table_fqn, table_lifecycle.c.phase,
                table_lifecycle.c.verified_at)
    if cid is not None:
        q2 = q2.where(table_lifecycle.c.connection_id == cid)
    for a, f, ph, va in c.execute(q2):
        d = out.setdefault((a, f), blank())
        d["phase"], d["verified_at"] = ph, va.isoformat() if va is not None else None
    return out


# ------------------------------------------------------------------ detection (pipeline, first decode of a batch)
def _min_pos(p: Any, cands: list[str | None]) -> str | None:
    if not cands or any(x is None for x in cands):
        return None
    best = cands[0]
    for x in cands[1:]:
        if p.source.compare(Position(engine=p.engine_name, text=x), Position(engine=p.engine_name, text=best)) < 0:
            best = x
    return best


def watermark_for_new_hold(p: Any, fqn: str) -> str | None:
    """What the destination certainly holds for the table: min(main stream position, every held row, a snapshot
    catch-up not done yet)."""
    cands: list[str | None] = [p.flushed_position()]
    for r in holds.rows(p.engine, p.cid):
        if r["table_fqn"] != fqn:
            continue
        b = r["body"] or {}
        if r["held"] or (r["kind"] == "snapshot" and r["state"] == "CATCHING_UP"
                         and b.get("catchup_done") != b.get("snapshot_id")):
            cands.append(r["start_pos"])
    return _min_pos(p, cands)


def restart_for_new_hold(p: Any, fqn: str) -> int | None:
    """Scan start for replays of a new hold (HC-09): the main stream's restart_seq (first batch of the oldest open
    transaction at its last flush), lowered by every other held row of the table; None when unknown (lookback)."""
    from . import replay

    seqs: list[Any] = [replay.mirror3(p)[2]]
    for r in holds.rows(p.engine, p.cid):
        if r["table_fqn"] == fqn and r["held"]:
            seqs.append((r["body"] or {}).get("restart_seq"))
    if any(x is None for x in seqs):
        return None
    return min(int(x) for x in seqs)


def _incident(engine: Engine, cid: str, fqn: str | None, b: dict[str, Any], qs: list[Any], what: str) -> str | None:
    from alelt_diagnose.incidents import open_incident
    from alelt_diagnose.models import IncidentKind, Trigger

    try:
        q = qs[0]
        t = Trigger(kind=IncidentKind.TABLE_OUT_OF_SYNC, connection_id=cid, table_fqn=None if fqn == CONN else fqn,
                    batch_id=str(b["id"]) if b.get("id") else None,
                    position=q.position.text if getattr(q, "position", None) else None, stage="decode",
                    error=f"{what}: {q.reason}"[:500],
                    detail={"records": len(qs), "policy": what, "resync_needed": what != "continue"})
        return open_incident(t=t, engine=engine, severity="critical")[0]
    except Exception:  # noqa: BLE001 - diagnosis never breaks the data path
        log.exception("integrity incident failed", extra={"component": "alelt_hub"})
        return None


def _reasons(old: dict[str, int] | None, qs: list[Any]) -> dict[str, int]:
    out = dict(old or {})
    for q in qs:
        k = (q.reason or "").split(":")[0][:80]
        out[k] = out.get(k, 0) + 1
    return out


def on_quarantine(p: Any, b: dict[str, Any], recs: list[Any], held: set[str]) -> list[str]:
    """Decoder quarantines of one batch (first decode only). Holds / marks tables; raises StreamStopped for
    `stop_stream` (nothing of this batch is applied). Returns actions, e.g. ['hold:S.T', 'suspect:*']."""
    by_table: dict[str, list[Any]] = {}
    loose: list[Any] = []
    for q in recs:
        (by_table.setdefault(q.table.fqn, []) if q.table is not None else loose).append(q)
    acts: list[str] = []
    stop: dict[str, list[Any]] = {}
    for fqn, qs in sorted(by_table.items()):
        ref = _ref(fqn)
        if not p.policy.table(ref).enabled:
            REGISTRY.inc("alelt_quarantine_out_of_scope_total", connection_id=p.cid)
            continue                                       # not replicated: counted, never held
        pol = p.policy.quarantine_policy(ref)
        if pol == "stop_stream":
            stop[fqn] = qs
        elif pol == "continue":
            _mark(p, fqn, qs, b, held, hold=False)
            acts.append(f"suspect:{fqn}")
        else:
            _mark(p, fqn, qs, b, held, hold=True)
            acts.append(f"hold:{fqn}")
    if loose:
        _mark(p, CONN, loose, b, held, hold=False)
        acts.append("suspect:*")
        if any(q.reason.startswith("signal unsupported") for q in loose):   # X-132: unsupported(None) = all tables
            stop[CONN] = [q for q in loose if q.reason.startswith("signal unsupported")]
    if stop:
        r = row(p.engine, p.cid, CONN)
        body = {**((r or {}).get("body") or {}), "stopped": {"batch_id": str(b["id"]), "seq": int(b["seq"]),
                                                             "tables": sorted(stop), "at": utcnow().isoformat(),
                                                             "position": p.flushed_position()},
                "reason": f"stop_stream: {next(iter(stop.values()))[0].reason}"[:500], "since": utcnow().isoformat(),
                "policy": "stop_stream"}
        holds.put(p.engine, p.cid, CONN, KIND, OUT_OF_SYNC, True, p.flushed_position(), body)
        iid = _incident(p.engine, p.cid, CONN, b, [q for qs in stop.values() for q in qs], "stop_stream")
        holds.set_body(p.engine, p.cid, CONN, KIND, incident_id=iid)
        REGISTRY.inc("alelt_integrity_events_total", connection_id=p.cid, event="stop_stream")
        log.error("stream stopped by on_quarantine=stop_stream", extra={
            "component": "alelt_hub", "batch_id": str(b["id"]), "table": ",".join(sorted(stop))})
        raise StreamStopped(f"{p.cid}: stopped before batch seq {b['seq']} ({', '.join(sorted(stop))})")
    return acts


def _mark(p: Any, fqn: str, qs: list[Any], b: dict[str, Any], held: set[str], hold: bool) -> None:
    r = row(p.engine, p.cid, fqn)
    first = next((q.position.text for q in qs if q.position), None)
    if r is not None and r["state"] != IN_SYNC and (r["held"] or not hold):
        body = r["body"] or {}
        holds.set_body(p.engine, p.cid, fqn, KIND, count=int(body.get("count") or 0) + len(qs),
                       reasons=_reasons(body.get("reasons"), qs), last_seen=utcnow().isoformat())
        return
    start = watermark_for_new_hold(p, fqn) if hold else None
    rseq = restart_for_new_hold(p, fqn) if hold else None
    # a keyed re-read signal WITHOUT keys (key range / no key): neither a catalog refresh nor a re-decode can help and
    # completion needs keys -> straight to resync (routed like `unsupported`)
    step = ("resync" if all(is_reread_signal(q.reason) for q in qs) else "catalog") if hold else None
    body = {"policy": "hold_table" if hold else ("continue" if fqn != CONN else "unattributed"),
            "reason": qs[0].reason[:500], "reasons": _reasons(None, qs), "count": len(qs), "first_pos": first,
            "first_batch": str(b["id"]), "first_seq": int(b["seq"]), "since": utcnow().isoformat(),
            "step": step, "attempts": []}
    if rseq is not None:
        body["restart_seq"] = rseq
    holds.put(p.engine, p.cid, fqn, KIND, SUSPECT, hold, start, body)
    iid = _incident(p.engine, p.cid, fqn, b, qs, body["policy"])
    holds.set_body(p.engine, p.cid, fqn, KIND, incident_id=iid)
    REGISTRY.inc("alelt_integrity_events_total", connection_id=p.cid, event="hold" if hold else "suspect")
    log.error("table integrity %s", "HELD (on_quarantine=hold_table)" if hold else
              "SUSPECT, still applying (on_quarantine=continue / unattributed)",
              extra={"component": "alelt_hub", "table": fqn, "batch_id": str(b["id"]), "lri": first,
                     "correlation_id": f"incident={iid} reason={qs[0].reason[:120]}"})
    if hold:
        held.add(fqn)
        enqueue(p.engine, p.cid, fqn)


REREAD_SIGNAL = "signal needs_keyed_reread"


def is_reread_signal(reason: str | None) -> bool:
    return (reason or "").startswith(REREAD_SIGNAL)


def completion_unavailable(p: Any, fqn: str, reason: str) -> None:
    """Out-of-row completion of `fqn` cannot converge (agent without verify_table, remote connector without
    keyed_reread, no key, more than ALELT_COMPLETION_PENDING_MAX entries): integrity SUSPECT, held, recovery = resync
    (the snapshot replaces every value; the table's completion entries are dropped when the resync snapshot starts)."""
    r = row(p.engine, p.cid, fqn)
    if r is not None and r["held"] and r["state"] != IN_SYNC:
        return
    start = watermark_for_new_hold(p, fqn)
    rseq = restart_for_new_hold(p, fqn)
    body: dict[str, Any] = {"policy": "hold_table", "reason": reason[:500], "reasons": {"completion unavailable": 1},
                            "count": 1, "since": utcnow().isoformat(), "step": "resync", "attempts": []}
    if rseq is not None:
        body["restart_seq"] = rseq
    holds.put(p.engine, p.cid, fqn, KIND, SUSPECT, True, start, body)
    iid = _incident(p.engine, p.cid, fqn, {"id": None}, [_Reason(reason)], "hold_table")
    holds.set_body(p.engine, p.cid, fqn, KIND, incident_id=iid)
    p.held.add(fqn)
    REGISTRY.inc("alelt_integrity_events_total", connection_id=p.cid, event="completion_unavailable")
    log.error("table integrity HELD: %s", reason[:200], extra={"component": "alelt_hub", "table": fqn})
    enqueue(p.engine, p.cid, fqn)


@dataclass
class _Reason:
    reason: str
    position: Any = None


def enqueue(engine: Engine, cid: str, fqn: str, delay_s: float = 0, tag: str | None = None) -> None:
    with engine.begin() as c:
        jobs.enqueue(c, "integrity", {"connection_id": cid, "table": fqn},
                     dedup_key=f"integrity:{cid}:{fqn}:{tag or uuid.uuid4().hex}", delay_s=delay_s)


# ------------------------------------------------------------------ recovery (integrity job)
def _attempt(engine: Engine, cid: str, fqn: str, step: str, result: str, **body: Any) -> None:
    r = row(engine, cid, fqn)
    att = list(((r or {}).get("body") or {}).get("attempts") or [])[-19:]
    att.append({"step": step, "result": result[:300], "at": utcnow().isoformat()})
    holds.modify(engine, cid, fqn, KIND, attempts=att, **body)
    log.warning("integrity recovery %s: %s", step, result[:200], extra={"component": "alelt_hub", "table": fqn,
                                                                        "correlation_id": cid})


def _set(engine: Engine, cid: str, fqn: str, state: str, held: bool | None = None, **body: Any) -> None:
    if held is None:
        holds.modify(engine, cid, fqn, KIND, state=state, **body)
    else:
        holds.modify(engine, cid, fqn, KIND, state=state, held=held, **body)


def _records(engine: Engine, cid: str, fqn: str) -> list[dict[str, Any]]:
    with engine.connect() as c:
        return [dict(x) for x in c.execute(select(
            quarantine_event.c.id, quarantine_event.c.batch_id, quarantine_event.c.lri, quarantine_event.c.reason,
            quarantine_event.c.raw_ref, quarantine_event.c.raw_hex).where(
            quarantine_event.c.connection_id == cid, quarantine_event.c.table_fqn == fqn,
            quarantine_event.c.status == "OPEN").order_by(quarantine_event.c.created_at).limit(10_000)).mappings()]


def recover(engine: Engine, worker: Any, payload: dict[str, Any]) -> str:
    """One recovery step for a held table. Returns the step outcome (for logs / tests)."""
    from . import replay
    from .dialect import try_lock, unlock

    cid, fqn = str(payload["connection_id"]), str(payload["table"])
    r = row(engine, cid, fqn)
    if r is None or fqn == CONN:
        return "nothing"
    body = r["body"] or {}
    if replay.pass_of(r):
        return "pass in flight"
    step = body.get("step") or "catalog"
    if step == "verify":
        return _verify_step(engine, worker.pipeline(cid), fqn)
    if r["state"] == IN_SYNC:
        return "nothing"
    if step in ("catalog", "redecode", "reread") and not r["held"]:
        return "not held (continue policy)"
    p = worker.pipeline(cid)
    if step == "catalog":
        try:
            res = _hook(p, "refresh_catalog")(engine, p, fqn)
        except Exception as e:  # noqa: BLE001 - recorded; the re-decode still runs with what exists
            res = f"failed: {type(e).__name__}: {e}"
        with engine.connect() as c:
            vid = c.execute(select(func.max(catalog_version.c.id)).where(catalog_version.c.connection_id == cid)).scalar()
        _set(engine, cid, fqn, REDECODING, step="redecode",
             catalog={"result": res, "requested_at": utcnow().isoformat(), "vid_before": vid})
        _attempt(engine, cid, fqn, "catalog", str(res))
        enqueue(engine, cid, fqn, 0 if res != "requested" else 2)
        return f"catalog {res}"
    if step == "redecode":
        cat = body.get("catalog") or {}
        if cat.get("result") == "requested" and not _catalog_arrived(engine, cid, cat, body.get("catalog_command")):
            enqueue(engine, cid, fqn, 5)
            return "waiting for catalog"
        lock = try_lock(engine, f"replay:{cid}")
        if lock is None:
            enqueue(engine, cid, fqn, 2)
            return "replay busy"
        try:
            p.refresh_policy()
            seq, pos, rseq = replay.mirror3(p)
            n = len(body.get("attempts") or [])
            res = replay.run(p, [replay._target(p, fqn)], f"{cid}#redecode:{fqn}:{n}", seq,
                             latest_catalog=True, strict=True, bypass_cache=True,
                             watermarks={replay.wm_stream(cid, fqn): pos})
        finally:
            unlock(lock, f"replay:{cid}")
        if res.aborted:
            q = res.quarantined.get(fqn) or []
            _set(engine, cid, fqn, OUT_OF_SYNC, step="resync")
            _attempt(engine, cid, fqn, "redecode", f"still quarantined: {len(q)} record(s): "
                                                   f"{q[0].reason if q else ''}")
            enqueue(engine, cid, fqn)
            return "redecode failed"
        replay.advance_watermark(p, fqn, pos, rseq)
        holds.set_body(engine, cid, fqn, KIND, **{"pass": {"id": uuid.uuid4().hex[:12], "phase": "attach",
                                                          "then": "attach", "latest_catalog": True, "strict": True,
                                                          "purpose": "redecode"}})
        _attempt(engine, cid, fqn, "redecode", f"clean: {res.applied.get(fqn, 0)} change(s) re-applied "
                                               f"through {pos}")
        _decode_soon(engine, cid, f"redecode:{fqn}:{n}")
        return "redecoded"
    if step == "reread":                                     # rows written before the completion stage: resync
        step = "resync"
        _set(engine, cid, fqn, OUT_OF_SYNC, step="resync")
    if step == "resync":
        from .snapshots import PipelineBusy, SnapshotService

        try:
            started = SnapshotService(engine, p).start([fqn], resync=True, reason=f"integrity: {body.get('reason')}")
        except PipelineBusy:
            enqueue(engine, cid, fqn, 10)
            return "resync busy"
        except Exception as e:  # noqa: BLE001 - operator action needed
            _set(engine, cid, fqn, OUT_OF_SYNC, step="manual", last_error=f"{type(e).__name__}: {e}"[:500])
            _attempt(engine, cid, fqn, "resync", f"cannot start: {type(e).__name__}: {e}")
            return "resync impossible"
        snap = holds.get(engine, cid, fqn, "snapshot")
        _set(engine, cid, fqn, RESYNCING, held=False, step="resync_wait",
             snapshot_id=((snap or {}).get("body") or {}).get("snapshot_id"))
        _attempt(engine, cid, fqn, "resync", f"snapshot started: {started}")
        return "resync started"
    return f"step {step}: waiting"


def _decode_soon(engine: Engine, cid: str, tag: str) -> None:
    with engine.begin() as c:
        jobs.enqueue(c, "decode", {"connection_id": cid}, dedup_key=f"attach:{cid}:{tag}")


def _catalog_arrived(engine: Engine, cid: str, cat: dict[str, Any], command_id: str | None) -> bool:
    with engine.connect() as c:
        vid = c.execute(select(func.max(catalog_version.c.id)).where(catalog_version.c.connection_id == cid)).scalar()
        if vid is not None and (cat.get("vid_before") is None or int(vid) > int(cat["vid_before"])):
            return True
        if command_id:
            st = c.execute(select(agent_command.c.status).where(agent_command.c.id == uuid.UUID(command_id))).scalar()
            if st in ("ACKED", "FAILED"):
                return True                # same content (dedup): nothing newer will come
    at = datetime.fromisoformat(str(cat.get("requested_at") or utcnow().isoformat()))
    return utcnow() >= at + timedelta(seconds=_wait_s("ALELT_INTEGRITY_CATALOG_WAIT_S", 120))


def pass_aborted(p: Any, rows: list[dict[str, Any]], res: Any) -> None:
    """A strict (re-decode) pass found quarantined records again: next recovery step."""
    for r in rows:
        if r["kind"] != KIND:
            continue
        q = res.quarantined.get(r["table_fqn"]) or []
        _set(p.engine, p.cid, r["table_fqn"], OUT_OF_SYNC, step="resync")
        _attempt(p.engine, p.cid, r["table_fqn"], "attach", f"tail still quarantined: {len(q)} record(s)")
        enqueue(p.engine, p.cid, r["table_fqn"])


def attached(p: Any, rows: list[dict[str, Any]], res: Any) -> None:
    """Integrity rows re-attached (released by the attach phase): resolve their quarantine records, verify."""
    for r in rows:
        if r["kind"] != KIND:
            continue
        fqn = r["table_fqn"]
        purpose = ((r["body"] or {}).get("pass") or {}).get("purpose") or "replay"
        with p.engine.begin() as c:
            n = c.execute(update(quarantine_event).where(
                quarantine_event.c.connection_id == p.cid, quarantine_event.c.table_fqn == fqn,
                quarantine_event.c.status == "OPEN").values(status="RESOLVED")).rowcount
        snap = holds.get(p.engine, p.cid, fqn, "snapshot")
        if snap and snap["state"] == "CATCHING_UP":       # the recovery replay covered the catch-up range
            holds.modify(p.engine, p.cid, fqn, "snapshot", held=False,
                         catchup_done=(snap["body"] or {}).get("snapshot_id"))
        _set(p.engine, p.cid, fqn, IN_SYNC, held=False, recovered_by=purpose, recovered_at=utcnow().isoformat(),
             step="verify", resolved_records=n)
        _attempt(p.engine, p.cid, fqn, "attach", f"re-attached ({purpose}); {n} quarantine record(s) resolved")
        REGISTRY.inc("alelt_integrity_events_total", connection_id=p.cid, event=f"recovered_{purpose}")
        log.warning("table integrity restored", extra={"component": "alelt_hub", "table": fqn,
                                                       "correlation_id": f"recovered_by={purpose}"})
        enqueue(p.engine, p.cid, fqn)


def _verify_step(engine: Engine, p: Any, fqn: str) -> str:
    """After a recovery: reconcile when the engine can (remote live read / edge VERIFY_TABLE hook); a mismatch
    starts a resync. 'not available' is recorded as such, never as a PASS."""
    try:
        out = _hook(p, "verify_table")(engine, p, fqn)
    except Exception as e:  # noqa: BLE001
        out = f"error: {type(e).__name__}: {e}"
    if out == "pending":
        enqueue(engine, p.cid, fqn, 15)
        return "verify pending"
    if out == "MISMATCH":
        _set(engine, p.cid, fqn, OUT_OF_SYNC, held=False, step="resync", verify="MISMATCH")
        _attempt(engine, p.cid, fqn, "verify", "MISMATCH after recovery: resync")
        enqueue(engine, p.cid, fqn)
        return "verify mismatch"
    _set(engine, p.cid, fqn, IN_SYNC, held=False, step=None,
         verify=out if out else "not available for this source (reconcile with a recon run)")
    _attempt(engine, p.cid, fqn, "verify", str(out or "not available"))
    return f"verify {out}"


# ------------------------------------------------------------------ sweep / operator actions / metrics
def sweep(engine: Engine, worker: Any) -> int:
    """Periodic: finish RESYNCING tables whose snapshot reached IN_SYNC; keep recovery of held tables going after a
    restart (idempotent job enqueue per minute)."""
    n = 0
    bucket = int(utcnow().timestamp() // 60)
    for r in holds.rows(engine, kind=KIND):
        cid, fqn, b = r["connection_id"], r["table_fqn"], r["body"] or {}
        if fqn == CONN or r["state"] == IN_SYNC:
            continue
        if r["state"] == RESYNCING:
            snap = holds.get(engine, cid, fqn, "snapshot")
            if snap and snap["state"] == "IN_SYNC" and (not b.get("snapshot_id")
                                                        or (snap["body"] or {}).get("snapshot_id") == b["snapshot_id"]):
                holds.put(engine, cid, fqn, KIND, IN_SYNC, False, None,
                          {**b, "step": None, "recovered_by": "resync", "recovered_at": utcnow().isoformat()})
                with engine.begin() as c:
                    c.execute(update(quarantine_event).where(
                        quarantine_event.c.connection_id == cid, quarantine_event.c.table_fqn == fqn,
                        quarantine_event.c.status == "OPEN").values(status="RESOLVED"))
                n += 1
            continue
        if r["held"] and b.get("step") not in ("manual", None) and not (b.get("pass") or {}):
            enqueue(engine, cid, fqn, tag=f"sweep:{bucket}")
            n += 1
    return n


def resume_stream(engine: Engine, cid: str, actor: str) -> list[str]:
    """Operator: resume a connection stopped by on_quarantine=stop_stream. The offending tables become hold_table
    holds (automatic recovery); the stopped batch is not quarantined twice."""
    from .db import batch

    r = row(engine, cid, CONN)
    if r is None or not r["held"]:
        return []
    st = (r["body"] or {}).get("stopped") or {}
    tabs = [t for t in st.get("tables") or [] if t != CONN]
    for fqn in tabs:
        holds.put(engine, cid, fqn, KIND, SUSPECT, True, st.get("position") or r["start_pos"],
                  {"policy": "hold_table", "reason": (r["body"] or {}).get("reason"), "count": 1,
                   "first_batch": st.get("batch_id"), "first_seq": st.get("seq"), "since": utcnow().isoformat(),
                   "step": "catalog", "attempts": [], "converted_from": "stop_stream"})
        enqueue(engine, cid, fqn)
    with engine.begin() as c:
        if st.get("batch_id"):
            c.execute(update(batch).where(batch.c.id == uuid.UUID(st["batch_id"]), batch.c.status == "RECEIVED")
                      .values(status="DECODED", updated_at=utcnow()))
        from . import store

        store.audit(c, actor, "integrity.resume_stream", cid, {"tables": tabs})
    holds.put(engine, cid, CONN, KIND, IN_SYNC, False, None,
              {**(r["body"] or {}), "resumed_by": actor, "resumed_at": utcnow().isoformat()})
    _decode_soon(engine, cid, f"resume:{uuid.uuid4().hex[:8]}")
    return tabs


def acknowledge(engine: Engine, cid: str, fqn: str, actor: str, reason: str) -> bool:
    """Operator: accept a SUSPECT table / connection after own verification (audited); does not release a hold."""
    from . import store

    r = row(engine, cid, fqn)
    if r is None or r["held"]:
        return False
    holds.put(engine, cid, fqn, KIND, IN_SYNC, False, None,
              {**(r["body"] or {}), "acknowledged_by": actor, "acknowledged_at": utcnow().isoformat(),
               "acknowledge_reason": reason})
    with engine.begin() as c:
        store.audit(c, actor, "integrity.acknowledge", f"{cid}:{fqn}", {"reason": reason, "state": r["state"]})
    return True


def collect(reg: Any, c: Connection) -> None:
    """/metrics: alelt_table_integrity{connection_id,table,state} (1 = current state) and alelt_table_held{...,kind}."""
    for r in c.execute(select(table_hold.c.connection_id, table_hold.c.table_fqn, table_hold.c.kind,
                              table_hold.c.state, table_hold.c.held)).all():
        cid, fqn, kind, st, held = r
        reg.set("alelt_table_held", 1 if held else 0, connection_id=cid, table=fqn, kind=kind)
        if kind == KIND:
            for s in STATES:
                reg.set("alelt_table_integrity", 1 if s == st else 0, connection_id=cid, table=fqn, state=s)


__all__ = ["IN_SYNC", "KIND", "OUT_OF_SYNC", "REDECODING", "RESYNCING", "RecoveryHooks", "SUSPECT",
           "StreamStopped", "acknowledge", "blocking", "collect", "completion_unavailable", "on_quarantine", "recover",
           "register_hooks",
           "resume_stream", "state_of", "sweep", "table_states"]
