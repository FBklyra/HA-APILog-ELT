"""Triggers. Called from the worker (job failure, quarantine, periodic scan) and the hub API (gateway responses).
Every hook swallows its own errors: diagnosis must never break the data path."""
from __future__ import annotations

import json
import logging
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import Engine, func, select

from alelt_common.models import TableRef
from alelt_common.util import utcnow
from alelt_hub.db import agent, audit_event, batch, checkpoint_mirror, hub_setting, quarantine_event, recon_result, source_table

from . import logbuf
from .incidents import open_incident
from .models import IncidentKind, Trigger

log = logging.getLogger("alelt_diagnose")
SCAN_KEY = "diag.scan"
QUARANTINE_BURST = 10            # records in 15 min on one connection => critical
AGENT_OFFLINE_S = 300
_DEST_MARKERS = ("psycopg", "operationalerror", "snowflake", "programmingerror", "warehouse", "merge", "destination",
                 "permission denied", "insufficient privilege", "password authentication", "connection refused",
                 "could not connect", "integrityerror", "dataerror", "numeric field overflow")


def _safe(fn: Any) -> Any:
    def wrap(*a: Any, **kw: Any) -> Any:
        try:
            logbuf.install()
            return fn(*a, **kw)
        except Exception:  # noqa: BLE001
            log.exception("diagnosis hook %s failed", fn.__name__, extra={"component": "alelt_diagnose"})
            return None
    wrap.__name__ = fn.__name__
    wrap.__doc__ = fn.__doc__
    return wrap


def job_trigger(j: dict[str, Any], error: str, tb: str | None = None) -> Trigger:
    """Classify a failed job: destination errors vs decode/load/snapshot/verify failures."""
    p = j.get("payload") or {}
    stage = {"decode": "decode", "snapshot_load": "snapshot", "verify": "verify", "resync": "snapshot",
             "reprocess": "decode"}.get(str(j.get("kind")), str(j.get("kind")))
    low = f"{error}\n{tb or ''}".lower()
    kind = IncidentKind.DESTINATION_ERROR if any(m in low for m in _DEST_MARKERS) else IncidentKind.JOB_FAILED
    if kind is IncidentKind.DESTINATION_ERROR and stage == "decode":
        stage = "load"
    return Trigger(kind=kind, connection_id=p.get("connection_id"), batch_id=p.get("batch_id"), job_id=j.get("id"),
                   table_fqn=p.get("table"), stage=stage, error=error,
                   detail={"attempts": j.get("attempts"), "traceback": (tb or "")[-4000:]})


def agent_event_kind(typ: str) -> IncidentKind:
    t = typ.upper()
    if t in ("PAUSED_BY_GOVERNOR", "GOVERNOR_PAUSED"):
        return IncidentKind.GOVERNOR_PAUSE
    if "SPOOL" in t:
        return IncidentKind.BACKPRESSURE
    if "CERT" in t or "AUTH" in t or "TOKEN" in t:
        return IncidentKind.AGENT_AUTH
    return IncidentKind.AGENT_ERROR


@_safe
def job_failed(engine: Engine, j: dict[str, Any], exc: BaseException) -> str | None:
    """Worker hook: a job raised. Opens/recurs an incident linked to job / batch / connection."""
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    t = job_trigger(j, f"{type(exc).__name__}: {exc}", tb)
    if t.batch_id is None and t.connection_id and t.stage in ("decode", "load"):
        with engine.connect() as c:   # decode jobs carry the connection only: link the first unloaded batch
            r = c.execute(select(batch.c.id, batch.c.from_lri).where(
                batch.c.connection_id == t.connection_id, batch.c.status.in_(["RECEIVED", "DECODED"]))
                .order_by(batch.c.seq).limit(1)).first()
        if r is not None:
            t = t.model_copy(update={"batch_id": str(r[0]), "position": r[1]})
    return open_incident(engine, t)[0]


@_safe
def capture_gap(engine: Engine, connection_id: str, error: str, position: str | None = None) -> str | None:
    """Remote capture runner hook (REQ-20): the source lost the changes after the resume position -> resync needed."""
    t = Trigger(kind=IncidentKind.POSITION_GAP, connection_id=connection_id, position=position, stage="capture",
                error=error, detail={"resync_needed": True, "source": "alelt_capture"})
    return open_incident(engine, t)[0]


@_safe
def capture_stuck(engine: Engine, connection_id: str, error: str, attempts: int) -> str | None:
    """Remote capture runner hook: the same connector error repeated without progress (deterministic failure:
    retrying cannot help) -> the runner stops the connection; operator action / resync needed."""
    t = Trigger(kind=IncidentKind.AGENT_ERROR, connection_id=connection_id, stage="capture", error=error,
                detail={"source": "alelt_capture", "repeated_failures": attempts, "circuit_breaker": True})
    return open_incident(engine, t)[0]


@_safe
def quarantined(engine: Engine, connection_id: str, b: dict[str, Any], recs: list[Any]) -> list[str]:
    """Pipeline hook: records quarantined for a batch. One incident per (table, reason signature)."""
    with engine.connect() as c:
        recent = c.execute(select(func.count()).select_from(quarantine_event).where(
            quarantine_event.c.connection_id == connection_id,
            quarantine_event.c.created_at >= utcnow() - timedelta(minutes=15))).scalar_one()
    sev = "critical" if recent >= QUARANTINE_BURST else None
    out, seen = [], set()
    for q in recs:
        table = q.table.fqn if q.table else None
        key = (table, q.reason.split(":")[0])
        if key in seen:
            continue
        seen.add(key)
        t = Trigger(kind=IncidentKind.QUARANTINE, connection_id=connection_id, table_fqn=table, batch_id=str(b["id"]),
                    position=q.position.text if q.position else None, stage="decode", error=q.reason,
                    detail={"records_in_batch": len(recs), "quarantined_15m": int(recent)})
        out.append(open_incident(engine, t, severity=sev)[0])
    return out


SOURCE_SAMPLES = 20


@_safe
def source_checks(engine: Engine, connection_id: str, b: dict[str, Any], events: list[Any],
                  tables: dict[Any, Any]) -> list[str]:
    """Pipeline hook (first decode of a batch): bounded value checks -> SOURCE_DATA incidents.
    Disabled with ALELT_DIAG_SOURCE_CHECKS=0. Raw samples go to diag_sensitive_sample only."""
    import os

    from sqlalchemy import insert

    from .redact import describe
    from .source_data import check_events
    from .tables import sensitive_sample

    if os.environ.get("ALELT_DIAG_SOURCE_CHECKS", "1") == "0":
        return []
    by_table: dict[Any, list[Any]] = {}
    for e in events[:2000]:
        if e.table is not None and e.after:
            by_table.setdefault(e.table, []).append(e)
    found: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for ref, evs in by_table.items():
        info = tables.get(ref)
        if info is None:
            continue
        for it in check_events(info, evs):
            found.setdefault((ref.fqn, it["column"], it["issue"]), []).append(it)
    out = []
    for (fqn, col, issue), items in found.items():
        positions = [i["position"] for i in items if i["position"]]
        info = next(v for k, v in tables.items() if getattr(k, "fqn", None) == fqn)
        cm = next((c for c in info.columns if c.name == col), None)
        meta = {"engine_type": cm.engine_type, "canonical_type": cm.canonical_type.value, "length": cm.length,
                "scale": cm.scale} if cm else {}
        t = Trigger(kind=IncidentKind.SOURCE_DATA, connection_id=connection_id, table_fqn=fqn, batch_id=str(b["id"]),
                    position=positions[0] if positions else None, stage="decode",
                    error=f"source data {issue.lower()} in {fqn}.{col}",
                    detail={"source_issue": {"issue": issue, "column": col, "column_meta": meta,
                                             "key_columns": [c.name for c in info.key_columns()]},
                            "positions": positions[:20],
                            "count": len(items), "sample_keys": [{k: describe(v) for k, v in i["key"].items()}
                                                                 for i in items[:5]]})
        iid, _ = open_incident(engine, t)
        with engine.begin() as c:
            have = c.execute(select(func.count()).select_from(sensitive_sample).where(
                sensitive_sample.c.incident_id == iid)).scalar_one()
            for i in items[: max(0, SOURCE_SAMPLES - int(have))]:
                c.execute(insert(sensitive_sample).values(
                    incident_id=iid, column_name=col, position=i["position"], at=utcnow(),
                    key={k: str(v) for k, v in i["key"].items()}, value=str(i["value"])[:200]))
        out.append(iid)
    return out


@_safe
def gateway_response(engine: Engine, path: str, status: int, headers: dict[str, str]) -> str | None:
    """Hub API middleware hook for agent endpoints: gap (409), backpressure (503), identity rejected (401/403)."""
    hdr: dict[str, Any] = {}
    if headers.get("x-alelt-batch"):
        try:
            hdr = json.loads(headers["x-alelt-batch"])
        except ValueError:
            hdr = {}
    cid, aid = hdr.get("connection_id"), hdr.get("agent_id")
    got = (hdr.get("from_pos") or {}).get("text")
    if path.endswith("/blocks") and status == 409 and cid:
        with engine.connect() as c:
            prev = c.execute(select(batch.c.next_lri, batch.c.to_lri).where(batch.c.connection_id == cid)
                             .order_by(batch.c.seq.desc()).limit(1)).first()
        expected = (prev[0] or prev[1]) if prev else None
        if expected is None or expected == got:
            return None                        # 409 for another reason (batch_id reused)
        return open_incident(engine, Trigger(
            kind=IncidentKind.POSITION_GAP, connection_id=cid, agent_id=aid, position=got,
            error="position gap/overlap: batch from_pos does not follow the previous next_pos",
            detail={"expected_from": expected, "got": got, "batch_id": hdr.get("batch_id")}))[0]
    if path.endswith("/blocks") and status == 503:
        return open_incident(engine, Trigger(kind=IncidentKind.BACKPRESSURE, connection_id=cid, agent_id=aid,
                                             error="hub backlog full (503): agent paused by backpressure"))[0]
    if status in (401, 403, 426):
        reason = {401: "bad agent token", 403: "client certificate not allow-listed",
                  426: "X-ALELT-Protocol must be 1"}[status]
        return open_incident(engine, Trigger(kind=IncidentKind.AGENT_AUTH, connection_id=cid, agent_id=aid,
                                             error=f"agent request rejected: {reason}",
                                             detail={"status": status, "path": path}))[0]
    return None


def _aware(v: datetime | None) -> datetime | None:
    return v.replace(tzinfo=timezone.utc) if v is not None and v.tzinfo is None else v


@_safe
def scan(engine: Engine, settings: Any | None = None) -> int:
    """Periodic sweep (worker, every minute): agent offline/error/governor, capture lost, lag, recon mismatch,
    agent events posted since the last sweep. Returns the number of triggers raised."""
    from alelt_hub.settings import get_settings
    from alelt_hub.store import connection_policy

    s = settings or get_settings()
    now = utcnow()
    triggers: list[Trigger] = []
    with engine.begin() as c:
        state = dict(c.execute(select(hub_setting.c.value).where(hub_setting.c.key == SCAN_KEY)).scalar() or {})
        since_s = state.get("since")
        since = datetime.fromisoformat(since_s) if since_s else now - timedelta(hours=1)
        for aid, cid, st, hb in c.execute(select(agent.c.id, agent.c.connection_id, agent.c.status,
                                                 agent.c.last_heartbeat)):
            hb = _aware(hb)
            if hb is not None and (now - hb).total_seconds() > AGENT_OFFLINE_S:
                triggers.append(Trigger(kind=IncidentKind.AGENT_OFFLINE, agent_id=aid, connection_id=cid,
                                        error="agent heartbeat missing",
                                        detail={"silent_s": int((now - hb).total_seconds())}))
            elif st == "ERROR":
                triggers.append(Trigger(kind=IncidentKind.AGENT_ERROR, agent_id=aid, connection_id=cid,
                                        error="agent reports state ERROR"))
            elif st == "PAUSED_BY_GOVERNOR":
                triggers.append(Trigger(kind=IncidentKind.GOVERNOR_PAUSE, agent_id=aid, connection_id=cid,
                                        error="agent paused_by_governor"))
            elif st == "SPOOL_FULL":
                triggers.append(Trigger(kind=IncidentKind.BACKPRESSURE, agent_id=aid, connection_id=cid,
                                        error="agent spool full"))
        policies: dict[str, Any] = {}
        for cid, sch, tab in c.execute(select(source_table.c.connection_id, source_table.c.src_schema,
                                              source_table.c.src_table).where(source_table.c.capture_enabled.is_(False))):
            if cid not in policies:
                policies[cid] = connection_policy(c, cid, s.tables_config)
            if policies[cid].table(TableRef(schema=sch, name=tab)).enabled:
                triggers.append(Trigger(kind=IncidentKind.CAPTURE_LOST, connection_id=cid, table_fqn=f"{sch}.{tab}",
                                        error="data capture changes disabled on an in-scope table"))
        from alelt_hub.lag import lags as _lags                  # HC-16: an idle, caught-up source is not late

        for cid, lag in _lags(c, c.execute(select(checkpoint_mirror.c.connection_id,
                                                  checkpoint_mirror.c.last_commit_ts)).all()).items():
            if lag is not None and lag > s.latency_target_s:
                triggers.append(Trigger(kind=IncidentKind.LAG_ABOVE_TARGET, connection_id=cid,
                                        error="capture lag above target",
                                        detail={"lag_s": int(lag), "target_s": s.latency_target_s}))
        for tid, sch, tab, cid, detail in c.execute(
                select(recon_result.c.table_id, source_table.c.src_schema, source_table.c.src_table,
                       source_table.c.connection_id, recon_result.c.detail)
                .join(source_table, source_table.c.id == recon_result.c.table_id)
                .where(recon_result.c.outcome == "MISMATCH", recon_result.c.created_at >= since)):
            triggers.append(Trigger(kind=IncidentKind.RECON_MISMATCH, connection_id=cid, table_fqn=f"{sch}.{tab}",
                                    error="reconciliation mismatch", detail={"recon": detail}))
        for act, target, d in c.execute(select(audit_event.c.action, audit_event.c.target, audit_event.c.detail)
                                        .where(audit_event.c.action.like("agent.event.%"), audit_event.c.at >= since)):
            typ = str(act).removeprefix("agent.event.")
            if typ in ("GOVERNOR_RESUMED", "RESUMED", "STARTED"):
                continue
            triggers.append(Trigger(kind=agent_event_kind(typ), agent_id=target, error=f"agent event {typ}",
                                    detail={"event": typ, "detail": (d or {}).get("detail", {})}))
        value = {"since": now.isoformat()}
        from sqlalchemy import insert, update

        if c.execute(update(hub_setting).where(hub_setting.c.key == SCAN_KEY).values(value=value)).rowcount == 0:
            c.execute(insert(hub_setting).values(key=SCAN_KEY, value=value))
    for t in triggers:
        open_incident(engine, t)
    return len(triggers)


class ScanTimer:
    """Rate-limits scan() inside a polling loop."""

    def __init__(self, every_s: float = 60.0) -> None:
        self.every_s, self.last = every_s, 0.0

    def tick(self, engine: Engine, settings: Any | None = None) -> None:
        if time.monotonic() - self.last >= self.every_s:
            self.last = time.monotonic()
            scan(engine, settings)
