"""Bounded convergence waits for ladder harnesses: never hang, fail fast with a diagnosis (RC5).

`wait_converged(check, timeout_s, engine=...)` polls `check() -> (done, detail)` until it is done, the timeout expires,
a table reaches a TERMINAL state (waiting cannot fix it), or nothing moves for `stall_s` (default 300 s).

| Terminal (fail fast) | Transient (wait, bounded) |
|---|---|
| integrity `step=manual`; stream stopped (`*` held); snapshot `paused` with `last_error` (incl. SNAPSHOT_TABLE re-issued ALELT_SNAPSHOT_MAX_REISSUES times); job FAILED of a data-path kind; `COMPLETION_OVERSIZE` incident; agent process exited | SUSPECT / REDECODING / OUT_OF_SYNC under recovery; lifecycle INITIAL_LOAD / CATCH_UP / RESYNC; schedule held; completion pending; `COMPLETION_STALLED` (counted, the wait then fails at the timeout) |

The progress fingerprint covers the check detail, the per-table diagnosis, batch / job / snapshot chunk counts, the
pending completions, the newest agent heartbeat and the newest command ack. The diagnosis lists every table that is
not converged (integrity state, holds, lifecycle phase, pending completions, snapshot chunks loaded / last, recovery
step, reasons, incident) plus the agent liveness line and the last 5 incidents by kind.
"""
from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

BAD_STATES = ("SUSPECT", "REDECODING", "OUT_OF_SYNC", "RESYNCING")
TRANSIENT_PHASES = ("NEW", "INITIAL_LOAD", "CATCH_UP", "RESYNC")
DATA_PATH_KINDS = ("decode", "replay", "snapshot_load", "attach", "integrity", "completion", "lifecycle", "snapshot")
TERMINAL_INCIDENTS = ("COMPLETION_OVERSIZE",)
COUNTED_INCIDENTS = ("COMPLETION_STALLED", "TABLE_OUT_OF_SYNC", "AGENT_OFFLINE")


@dataclass
class Converge:
    ok: bool
    seconds: float
    reason: str                                   # converged | timeout | terminal | stalled | process_exited
    detail: str = ""
    diagnosis: list[dict[str, Any]] = field(default_factory=list)
    queue: dict[str, int] = field(default_factory=dict)
    extra: dict[str, Any] = field(default_factory=dict)   # agent liveness, incidents, terminal reasons

    def summary(self, limit: int = 8) -> str:
        if self.ok:
            return f"converged in {self.seconds:.0f}s"
        rows = "; ".join(
            f"{d['table']}: {d['integrity']}"
            f"{' phase ' + str(d['phase']) if d.get('phase') else ''}"
            f"{' held by ' + ','.join(d['held_by']) if d['held_by'] else ''}"
            f"{' pending ' + str(d['pending_completions']) if d.get('pending_completions') else ''}"
            f"{' chunks ' + str(d['chunks']) if d.get('chunks') else ''}"
            f"{' step ' + str(d['step']) if d.get('step') else ''}"
            f"{' reasons ' + str(d['quarantine']) if d.get('quarantine') else ''}"
            f"{' incident ' + str(d['incident_id']) if d.get('incident_id') else ''}"
            for d in self.diagnosis[:limit])
        more = f" (+{len(self.diagnosis) - limit} more)" if len(self.diagnosis) > limit else ""
        q = f" | queue {self.queue}" if self.queue else ""
        ex = f" | {self.extra}" if self.extra else ""
        return (f"{self.reason} after {self.seconds:.0f}s: {self.detail[:300]}" + (f" | {rows}{more}" if rows else "")
                + q + ex)


def default_timeout(name: str = "ALELT_E2E_CONVERGE_TIMEOUT_S", default: float = 600) -> float:
    return float(os.environ.get(name, str(default)))


def default_stall(name: str = "ALELT_E2E_STALL_S", default: float = 300) -> float:
    return float(os.environ.get(name, str(default)))


def diagnosis(engine: Any, connection_id: str | None = None) -> list[dict[str, Any]]:
    """Tables not converged (hub metadata): integrity, holds, lifecycle phase, pending completions, snapshot chunks,
    quarantine reasons (counts), incident ids."""
    from sqlalchemy import func, select

    from alelt_hub import integrity
    from alelt_hub.db import quarantine_event, snapshot_chunk, table_hold

    out: list[dict[str, Any]] = []
    with engine.connect() as c:
        st = integrity.table_states(c, connection_id)
        q: dict[tuple[str, str], dict[str, int]] = {}
        stmt = select(quarantine_event.c.connection_id, quarantine_event.c.table_fqn, quarantine_event.c.reason,
                      func.count()).where(quarantine_event.c.status == "OPEN").group_by(
            quarantine_event.c.connection_id, quarantine_event.c.table_fqn, quarantine_event.c.reason)
        if connection_id is not None:
            stmt = stmt.where(quarantine_event.c.connection_id == connection_id)
        for cid, fqn, reason, n in c.execute(stmt):
            q.setdefault((cid, fqn or "*"), {})[(reason or "")[:80]] = int(n)
        snap: dict[tuple[str, str], dict[str, Any]] = {}
        sq = select(table_hold.c.connection_id, table_hold.c.table_fqn, table_hold.c.state, table_hold.c.body).where(
            table_hold.c.kind == "snapshot")
        if connection_id is not None:
            sq = sq.where(table_hold.c.connection_id == connection_id)
        for cid, fqn, state, body in c.execute(sq):
            b = body or {}
            snap[(cid, fqn)] = {"state": state, "snapshot_id": b.get("snapshot_id"), "reissues": b.get("reissues"),
                                "error": str(b["last_error"])[:200] if b.get("paused") and b.get("last_error") else None}
        chunks: dict[str, tuple[int, int, bool]] = {}
        cq = select(snapshot_chunk.c.snapshot_id, snapshot_chunk.c.status, snapshot_chunk.c.last)
        if connection_id is not None:
            cq = cq.where(snapshot_chunk.c.connection_id == connection_id)
        for sid, cst, last in c.execute(cq):
            n, ld, la = chunks.get(str(sid), (0, 0, False))
            chunks[str(sid)] = (n + 1, ld + (1 if cst == "LOADED" else 0), la or bool(last))
    for (cid, fqn), v in sorted(st.items()):
        s = snap.get((cid, fqn)) or {}
        phase = v.get("phase")
        pend = int(v.get("pending_completions") or 0)
        if (v["integrity"] == "IN_SYNC" and not v["held_by"] and not s.get("error") and not pend
                and phase in (None, "STREAMING", "DISABLED")):
            continue
        ch = chunks.get(str(s.get("snapshot_id"))) if s.get("snapshot_id") else None
        out.append({"connection_id": cid, "table": fqn, "integrity": v["integrity"], "held_by": v["held_by"],
                    "phase": phase, "pending_completions": pend, "verified_at": v.get("verified_at"),
                    "step": v.get("step"), "reason": v.get("reason"), "incident_id": v.get("incident_id"),
                    "quarantine": q.get((cid, fqn), {}), "snapshot": s.get("state"),
                    "snapshot_error": s.get("error"), "reissues": s.get("reissues"),
                    "chunks": f"{ch[1]}/{ch[0]}{' last' if ch[2] else ''}" if ch else None})
    return out


def integrity_failures(diag: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Table-level integrity problems (the connection row `*` = unattributed records, reported, not fatal here)."""
    return [d for d in diag if d["table"] != "*" and (d["integrity"] in BAD_STATES or "integrity" in d["held_by"])]


def terminal_failures(diag: list[dict[str, Any]], engine: Any = None,
                      connection_id: str | None = None) -> list[dict[str, Any]]:
    """States no amount of waiting fixes (module docstring table, left column)."""
    out = []
    for d in diag:
        if d["table"] == "*":
            if "integrity" in d["held_by"]:                       # stop_stream: the whole connection stopped
                out.append({**d, "terminal": "stream stopped"})
            continue
        if d.get("step") == "manual":
            out.append({**d, "terminal": "integrity recovery needs an operator"})
        elif d.get("snapshot_error"):
            out.append({**d, "terminal": "snapshot paused"})
    if engine is not None:
        out += [{"table": "*", "terminal": t} for t in terminal_events(engine, connection_id)]
    return out


def terminal_events(engine: Any, connection_id: str | None = None) -> list[str]:
    """Hub-wide terminal signals: FAILED data-path jobs, COMPLETION_OVERSIZE incidents."""
    from sqlalchemy import func, select

    from alelt_hub.db import job

    out: list[str] = []
    with engine.connect() as c:
        for kind, n in c.execute(select(job.c.kind, func.count()).where(
                job.c.status == "FAILED", job.c.kind.in_(DATA_PATH_KINDS)).group_by(job.c.kind)):
            out.append(f"{n} FAILED {kind} job(s)")
        try:
            from alelt_diagnose.tables import incident

            iq = select(incident.c.kind, func.count()).where(incident.c.kind.in_(TERMINAL_INCIDENTS),
                                                            incident.c.status != "RESOLVED")
            if connection_id is not None:
                iq = iq.where(incident.c.connection_id == connection_id)
            for kind, n in c.execute(iq.group_by(incident.c.kind)):
                out.append(f"{n} open {kind} incident(s)")
        except Exception:  # noqa: BLE001 - diagnosis tables missing: nothing to report
            pass
    return out


def classify(d: dict[str, Any]) -> str:
    """'terminal' | 'transient' for one diagnosis row (documented table above; used by tests and reports)."""
    if d.get("step") == "manual" or d.get("snapshot_error") or (d["table"] == "*" and "integrity" in d["held_by"]):
        return "terminal"
    return "transient"


def progress(engine: Any, connection_id: str | None = None) -> tuple[Any, ...]:
    """Fingerprint of hub activity: batch / job / snapshot chunk statuses, pending completions, newest agent
    heartbeat and newest command ack. Unchanged = no progress."""
    from sqlalchemy import func, select

    from alelt_hub.db import agent, agent_command, batch, job, snapshot_chunk, table_completion

    with engine.connect() as c:
        qb = select(batch.c.status, func.count()).group_by(batch.c.status)
        qc = select(snapshot_chunk.c.status, func.count()).group_by(snapshot_chunk.c.status)
        qp = select(table_completion.c.status, func.count()).group_by(table_completion.c.status)
        qa = select(func.max(agent.c.last_heartbeat))
        qk = select(func.max(agent_command.c.acked_at))
        if connection_id is not None:
            qb = qb.where(batch.c.connection_id == connection_id)
            qc = qc.where(snapshot_chunk.c.connection_id == connection_id)
            qp = qp.where(table_completion.c.connection_id == connection_id)
            qa = qa.where(agent.c.connection_id == connection_id)
            qk = qk.where(agent_command.c.agent_id.in_(select(agent.c.id).where(agent.c.connection_id == connection_id)))
        b = tuple(sorted((str(s), int(n)) for s, n in c.execute(qb)))
        j = tuple(sorted((str(s), int(n)) for s, n in c.execute(
            select(job.c.status, func.count()).group_by(job.c.status))))
        s = tuple(sorted((str(x), int(n)) for x, n in c.execute(qc)))
        p = tuple(sorted((str(x), int(n)) for x, n in c.execute(qp)))
        hb = c.execute(qa).scalar()
        ack = c.execute(qk).scalar()
    return b, j, s, p, str(hb), str(ack)


def agent_liveness(engine: Any, connection_id: str | None = None) -> dict[str, Any]:
    """Newest agent heartbeat age and newest command ack (one line in the diagnosis)."""
    from sqlalchemy import func, select

    from alelt_common.util import utcnow
    from alelt_hub.db import agent, agent_command

    with engine.connect() as c:
        qa = select(agent.c.id, agent.c.status, agent.c.last_heartbeat).order_by(agent.c.last_heartbeat.desc())
        if connection_id is not None:
            qa = qa.where(agent.c.connection_id == connection_id)
        r = c.execute(qa.limit(1)).first()
        ack = c.execute(select(func.max(agent_command.c.acked_at))).scalar()
    if r is None:
        return {"agent": None}
    hb = r[2] if r[2] is None or r[2].tzinfo else r[2].replace(tzinfo=utcnow().tzinfo)
    ack = ack if ack is None or ack.tzinfo else ack.replace(tzinfo=utcnow().tzinfo)
    return {"agent": r[0], "status": r[1], "heartbeat_age_s": round((utcnow() - hb).total_seconds()) if hb else None,
            "last_ack_age_s": round((utcnow() - ack).total_seconds()) if ack else None}


def recent_incidents(engine: Any, connection_id: str | None = None, n: int = 5) -> list[str]:
    try:
        from sqlalchemy import select

        from alelt_diagnose.tables import incident

        q = select(incident.c.kind, incident.c.table_fqn, incident.c.status, incident.c.count).order_by(
            incident.c.last_seen.desc()).limit(n)
        if connection_id is not None:
            q = q.where(incident.c.connection_id == connection_id)
        with engine.connect() as c:
            return [f"{k} {t or '*'} {s} x{cnt}" for k, t, s, cnt in c.execute(q)]
    except Exception:  # noqa: BLE001
        return []


def queue_state(engine: Any) -> dict[str, int]:
    from sqlalchemy import func, select

    from alelt_hub.db import job

    with engine.connect() as c:
        return {f"{k}:{s}": int(n) for k, s, n in c.execute(
            select(job.c.kind, job.c.status, func.count()).where(job.c.status.in_(("QUEUED", "RUNNING")))
            .group_by(job.c.kind, job.c.status))}


def wait_converged(check: Callable[[], tuple[bool, str]], timeout_s: float, *, engine: Any = None,
                   connection_id: str | None = None, interval_s: float = 5.0, fail_on_integrity: bool = True,
                   alive: Callable[[], bool] | None = None, stall_s: float | None = None) -> Converge:
    """Bounded wait (timeout_s), early end on a terminal state (fail_on_integrity) or on `stall_s` without progress."""
    t0 = time.monotonic()
    stall = default_stall() if stall_s is None else stall_s
    detail = ""
    last_fp: Any = None
    moved = t0
    while True:
        done, detail = check()
        el = time.monotonic() - t0
        if done:
            return Converge(True, el, "converged", detail)
        diag = diagnosis(engine, connection_id) if engine is not None else []
        qs = queue_state(engine) if engine is not None else {}

        def extra() -> dict[str, Any]:
            if engine is None:
                return {}
            return {"agent": agent_liveness(engine, connection_id),
                    "incidents": recent_incidents(engine, connection_id)}
        term = terminal_failures(diag, engine, connection_id) if fail_on_integrity else []
        if term:
            return Converge(False, el, "terminal", detail, diag, qs,
                            {**extra(), "terminal": [f"{t['table']}: {t['terminal']}" for t in term][:10]})
        if alive is not None and not alive():
            return Converge(False, el, "process_exited", detail, diag, qs, extra())
        if el >= timeout_s:
            return Converge(False, el, "timeout", detail, diag, qs, extra())
        fp = (detail, repr([(d["table"], d["integrity"], d["held_by"], d.get("step"), d.get("snapshot"),
                             d.get("phase"), d.get("pending_completions"), d.get("chunks")) for d in diag]),
              progress(engine, connection_id) if engine is not None else None)
        if fp != last_fp:
            last_fp, moved = fp, time.monotonic()
        elif stall and time.monotonic() - moved >= stall:
            return Converge(False, el, "stalled", f"no progress for {stall:.0f}s: {detail}", diag, qs, extra())
        time.sleep(min(interval_s, max(0.1, timeout_s - el)))
