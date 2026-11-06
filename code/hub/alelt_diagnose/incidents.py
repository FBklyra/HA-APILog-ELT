"""Incident lifecycle: open (dedup by signature) -> diagnose (Stage A, optional B) -> actions (approval) -> resolve
(learning loop: confirmed cause becomes a known-issue signature)."""
from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Connection, Engine, func, insert, select, update

from alelt_common.models import CommandName
from alelt_common.util import utcnow
from alelt_hub import store
from alelt_hub.db import audit_event, job, quarantine_event, source_table

from . import evidence, llm, logbuf
from .models import (
    ALERT_KINDS,
    SAFE_IDEMPOTENT,
    SEVERITY,
    EvidencePack,
    ExplainResult,
    Finding,
    IncidentKind,
    Trigger,
)
from .redact import redact, scrub
from .rules import KnownIssue, evaluate
from .signature import normalize, signature
from .tables import action, diag_run, incident, incident_event, known_issue, sensitive_sample

log = logging.getLogger("alelt_diagnose")
SYSTEM_ACTOR = "alelt_diagnose"
OPEN_STATES = ("OPEN", "DIAGNOSED")
_SEV_ORDER = {"info": 0, "warning": 1, "critical": 2}


class NotFound(LookupError):
    pass


class Conflict(RuntimeError):
    pass


def auto_actions() -> frozenset[str]:
    """Safe+idempotent action kinds allowed to run without approval (ALELT_DIAG_AUTO_ACTIONS, default none)."""
    raw = {x.strip().upper() for x in os.environ.get("ALELT_DIAG_AUTO_ACTIONS", "").split(",") if x.strip()}
    return frozenset(raw & SAFE_IDEMPOTENT)


def _iso(v: datetime | None) -> str | None:
    return v.isoformat().replace("+00:00", "Z") if v else None


def _level(c: Connection) -> str:
    return llm.load_config(c).redaction


def _title(t: Trigger) -> str:
    where = " · ".join(x for x in (t.table_fqn, t.connection_id, t.agent_id) if x)
    head = scrub(t.error.splitlines()[0] if t.error else "", "strict")[:160]
    label = f"{t.kind.value}{f' ({t.stage})' if t.stage else ''}"
    return " - ".join(x for x in (label, where, head) if x)


def _event(c: Connection, iid: str, kind: str, actor: str, detail: dict[str, Any] | None = None) -> None:
    c.execute(insert(incident_event).values(incident_id=iid, at=utcnow(), kind=kind, actor=actor, detail=detail or {}))


# ------------------------------------------------------------------ open
def open_incident(engine: Engine, t: Trigger, diagnose: bool = True, severity: str | None = None) -> tuple[str, bool]:
    """Open or recur an incident for `t`. Returns (incident_id, created). Stage A runs on creation."""
    with engine.begin() as c:
        level = _level(c)
        red = Trigger.model_validate(redact(t.model_dump(mode="json"), level))
        sig = signature(t.kind.value, t.connection_id, t.table_fqn, red.error)
        sev = severity or SEVERITY.get(t.kind, "warning")
        row = c.execute(select(incident.c.id, incident.c.count, incident.c.severity).where(
            incident.c.signature == sig, incident.c.status.in_(OPEN_STATES)).limit(1)).first()
        now = utcnow()
        if row is not None:
            iid, n = str(row[0]), int(row[1]) + 1
            vals: dict[str, Any] = {"count": n, "last_seen": now}
            if _SEV_ORDER.get(sev, 0) > _SEV_ORDER.get(str(row[2]), 0):
                vals["severity"] = sev
            c.execute(update(incident).where(incident.c.id == iid).values(**vals))
            if n <= 20 or n % 100 == 0:
                _event(c, iid, "RECURRED", SYSTEM_ACTOR, {"count": n, "batch_id": t.batch_id, "job_id": t.job_id,
                                                          "position": t.position})
            return iid, False
        iid = str(uuid.uuid4())
        c.execute(insert(incident).values(
            id=iid, signature=sig, kind=t.kind.value, severity=sev, status="OPEN", title=_title(red),
            connection_id=t.connection_id, table_fqn=t.table_fqn, batch_id=t.batch_id, job_id=t.job_id,
            agent_id=t.agent_id, position=t.position, error=red.error[:4000],
            trigger=red.model_dump(mode="json"), count=1, first_seen=now, last_seen=now))
        logs = redact(logbuf.recent(batch_id=t.batch_id, table=t.table_fqn, connection_id=t.connection_id), level)
        _event(c, iid, "OPENED", SYSTEM_ACTOR, {"trigger": red.model_dump(mode="json"), "logs": logs})
    log.warning("incident opened", extra={"component": "alelt_diagnose", "correlation_id": iid, "table": t.table_fqn,
                                          "batch_id": t.batch_id})
    if diagnose:
        try:
            diagnose_incident(engine, iid, SYSTEM_ACTOR)
        except Exception:  # noqa: BLE001 - diagnosis must never break the caller
            log.exception("stage A diagnosis failed", extra={"component": "alelt_diagnose", "correlation_id": iid})
    return iid, True


# ------------------------------------------------------------------ diagnose
def _known(c: Connection) -> list[KnownIssue]:
    return [KnownIssue(str(r.id), r.kind, r.pattern, r.title, r.root_cause, list(r.fix_steps or []),
                       r.category or "UNKNOWN") for r in c.execute(select(known_issue))]


def _incident_row(c: Connection, iid: str) -> dict[str, Any]:
    r = c.execute(select(incident).where(incident.c.id == iid)).mappings().first()
    if r is None:
        raise NotFound(iid)
    return dict(r)


def _stored_logs(c: Connection, iid: str) -> list[dict[str, Any]]:
    d = c.execute(select(incident_event.c.detail).where(incident_event.c.incident_id == iid,
                                                        incident_event.c.kind == "OPENED").limit(1)).scalar()
    return list((d or {}).get("logs") or [])


def evidence_for(engine: Engine, iid: str, probe_destination: bool = False) -> EvidencePack:
    with engine.connect() as c:
        row = _incident_row(c, iid)
        level = _level(c)
        stored = _stored_logs(c, iid)
    t = Trigger.model_validate(row["trigger"])
    pack = evidence.collect(engine, t, level, probe_destination)
    seen = {(x.get("ts"), x.get("msg")) for x in pack.logs}
    pack.logs = [x for x in stored if (x.get("ts"), x.get("msg")) not in seen] + pack.logs
    return pack


def _run(c: Connection, *, iid: str | None, ref_kind: str, ref_id: str, actor: str, stage: str,
         findings: list[Finding] | None = None, **kw: Any) -> str:
    rid = str(uuid.uuid4())
    c.execute(insert(diag_run).values(id=rid, incident_id=iid, ref_kind=ref_kind, ref_id=ref_id, at=utcnow(),
                                      actor=actor, stage=stage,
                                      findings=[f.model_dump(mode="json") for f in findings or []], **kw))
    return rid


def _llm_stage(c: Connection, pack: EvidencePack, findings: list[Finding], iid: str | None, ref_kind: str,
               ref_id: str, actor: str, question: str | None = None) -> tuple[llm.Analysis | None, str | None, str]:
    cfg = llm.load_config(c)
    try:
        a = llm.analyze(c, pack, findings, cfg, question=question)
    except llm.LlmError as e:
        rid = _run(c, iid=iid, ref_kind=ref_kind, ref_id=ref_id, actor=actor, stage="B", provider=cfg.provider,
                   model=cfg.model_name, error=str(e)[:2000])
        return None, str(e), rid
    rid = _run(c, iid=iid, ref_kind=ref_kind, ref_id=ref_id, actor=actor, stage="B", provider=a.provider,
               model=a.model, prompt_hash=a.prompt_hash, answer=a.answer.model_dump(mode="json"), tokens=a.tokens,
               duration_ms=a.duration_ms)
    return a, None, rid


def diagnose_incident(engine: Engine, iid: str, actor: str, use_llm: bool = False, role: str | None = None,
                      probe_destination: bool = False, question: str | None = None) -> dict[str, Any]:
    pack = evidence_for(engine, iid, probe_destination)
    with engine.begin() as c:
        known = _known(c)
        findings = evaluate(pack, known)
        top = findings[0]
        _run(c, iid=iid, ref_kind="incident", ref_id=iid, actor=actor, stage="A", findings=findings)
        c.execute(update(incident).where(incident.c.id == iid).values(
            rule_id=top.rule_id, root_cause=top.root_cause, explanation=top.explanation, confidence=top.confidence,
            fix_steps=top.fix_steps, status="DIAGNOSED", classification=top.category.value,
            source_data=top.source_data.model_dump(mode="json") if top.source_data else None)
            .where(incident.c.status != "RESOLVED"))
        for k in known:
            if top.rule_id == f"known:{k.id}":
                c.execute(update(known_issue).where(known_issue.c.id == k.id).values(hits=known_issue.c.hits + 1))
        _event(c, iid, "DIAGNOSED", actor, {"stage": "A", "rule_id": top.rule_id, "confidence": top.confidence,
                                            "matched": [f.rule_id for f in findings]})
        proposed = _propose(c, iid, findings)
        llm_err = None
        if use_llm:
            a, llm_err, rid = _llm_stage(c, pack, findings, iid, "incident", iid, actor, question)
            _event(c, iid, "AI_ANALYSIS", actor, {"run_id": rid, "error": llm_err,
                                                  "root_cause": a.answer.root_cause if a else None})
            if a is not None and a.answer.classification and top.category.value == "UNKNOWN":
                c.execute(update(incident).where(incident.c.id == iid).values(   # rules first, LLM second
                    classification=a.answer.classification.value))
            store.audit(c, actor, "incident.diagnose.ai", iid, {"run_id": rid, "error": llm_err}, role)
    for aid, kind in proposed:
        if kind in auto_actions():
            try:
                decide_action(engine, aid, SYSTEM_ACTOR + ":auto", None, "auto: safe+idempotent (ALELT_DIAG_AUTO_ACTIONS)",
                              approve=True)
            except Exception:  # noqa: BLE001
                log.exception("auto action failed", extra={"component": "alelt_diagnose", "correlation_id": aid})
    out = get_incident(engine, iid)
    out["llm_error"] = llm_err
    return out


def _propose(c: Connection, iid: str, findings: list[Finding]) -> list[tuple[str, str]]:
    out = []
    for f in findings[:2]:
        a = f.action
        if a is None:
            continue
        dup = c.execute(select(action.c.id).where(action.c.incident_id == iid, action.c.kind == a.kind,
                                                  action.c.status == "PROPOSED")).first()
        if dup is not None:
            continue
        aid = str(uuid.uuid4())
        c.execute(insert(action).values(id=aid, incident_id=iid, kind=a.kind, params=a.params,
                                        description=a.description, safe=a.safe, rule_id=f.rule_id,
                                        status="PROPOSED", created_at=utcnow()))
        _event(c, iid, "ACTION_PROPOSED", SYSTEM_ACTOR, {"action_id": aid, "kind": a.kind, "safe": a.safe})
        out.append((aid, a.kind))
    return out


# ------------------------------------------------------------------ actions (human approval)
def _table_id(c: Connection, cid: str | None, fqn: str | None) -> str | None:
    if not fqn or "." not in fqn:
        return None
    s, _, n = fqn.partition(".")
    q = select(source_table.c.id).where(source_table.c.src_schema == s, source_table.c.src_table == n)
    if cid:
        q = q.where(source_table.c.connection_id == cid)
    v = c.execute(q.limit(1)).scalar()
    return str(v) if v else None


def _execute(c: Connection, kind: str, params: dict[str, Any], actor: str, role: str | None,
             reason: str) -> dict[str, Any]:
    if kind == "RETRY_JOB":
        jid = params.get("job_id")
        r = c.execute(update(job).where(job.c.id == jid, job.c.status.in_(["QUEUED", "FAILED"])).values(
            status="QUEUED", run_after=utcnow(), locked_by=None))
        return {"job_id": jid, "requeued": bool(r.rowcount)}
    if kind in ("REDECODE_RANGE", "RESYNC_TABLE"):
        tid = _table_id(c, params.get("connection_id"), params.get("table"))
        payload = {"table_id": tid, "table": params.get("table"), "connection_id": params.get("connection_id"),
                   "source": "alelt_diagnose"}
        if kind == "REDECODE_RANGE":
            if not (params.get("from_pos") and params.get("to_pos")):
                raise Conflict("from_pos and to_pos are required")
            payload |= {"from_pos": params["from_pos"], "to_pos": params["to_pos"]}
        jk = "reprocess" if kind == "REDECODE_RANGE" else "resync"
        jid = c.execute(insert(job).values(kind=jk, payload=payload).returning(job.c.id)).scalar_one()
        return {"job_id": int(jid), "job_kind": jk}
    if kind == "SOURCE_DATA_POLICY":
        from alelt_hub.db import hub_setting

        if params.get("option") not in ("QUARANTINE_SKIP", "COERCE", "ACCEPT_AS_IS"):
            raise Conflict("option must be QUARANTINE_SKIP, COERCE or ACCEPT_AS_IS")
        cur = dict(c.execute(select(hub_setting.c.value).where(hub_setting.c.key == "diag.source_policy")).scalar()
                   or {})
        key = f"{params.get('table')}.{params.get('column') or '*'}"
        cur.setdefault(key, {})[str(params.get("issue"))] = {"option": params["option"], "by": actor,
                                                            "at": utcnow().isoformat(), "reason": reason}
        if c.execute(update(hub_setting).where(hub_setting.c.key == "diag.source_policy")
                     .values(value=cur)).rowcount == 0:
            c.execute(insert(hub_setting).values(key="diag.source_policy", value=cur))
        return {"policy": key, "issue": params.get("issue"), "option": params["option"]}
    if kind in ("SNAPSHOT_CATALOG", "RESUME_AGENT"):
        aid = params.get("agent_id")
        if not aid:
            raise Conflict("agent_id is required")
        name = CommandName.SNAPSHOT_CATALOG if kind == "SNAPSHOT_CATALOG" else CommandName.RESUME
        cmd = store.issue_command(c, str(aid), name, {}, actor, role, reason)
        return {"command_id": str(cmd.command_id), "command": name.value}
    raise Conflict(f"unknown action {kind}")


def decide_action(engine: Engine, action_id: str, actor: str, role: str | None, reason: str,
                  approve: bool) -> dict[str, Any]:
    """Approve (execute) or reject a proposed action. Audited. LLM suggestions never become actions."""
    with engine.begin() as c:
        a = c.execute(select(action).where(action.c.id == action_id).with_for_update()).mappings().first()
        if a is None:
            raise NotFound(action_id)
        if a["status"] != "PROPOSED":
            raise Conflict(f"action is {a['status']}")
        if not approve:
            c.execute(update(action).where(action.c.id == action_id).values(
                status="REJECTED", decided_by=actor, decided_at=utcnow(), reason=reason))
            _event(c, a["incident_id"], "ACTION_REJECTED", actor, {"action_id": action_id, "reason": reason})
            store.audit(c, actor, "incident.action.reject", a["incident_id"],
                        {"action_id": action_id, "kind": a["kind"], "reason": reason}, role)
            return {"id": action_id, "status": "REJECTED"}
        try:
            with c.begin_nested():
                result = _execute(c, a["kind"], dict(a["params"] or {}), actor, role, reason)
            status = "EXECUTED"
        except Exception as e:  # noqa: BLE001 - record the failure, keep the audit trail
            result, status = {"error": f"{type(e).__name__}: {scrub(str(e))}"[:500]}, "FAILED"
        c.execute(update(action).where(action.c.id == action_id).values(
            status=status, decided_by=actor, decided_at=utcnow(), reason=reason, result=result))
        _event(c, a["incident_id"], f"ACTION_{status}", actor, {"action_id": action_id, "kind": a["kind"],
                                                                "result": result})
        store.audit(c, actor, "incident.action.approve", a["incident_id"],
                    {"action_id": action_id, "kind": a["kind"], "params": a["params"], "result": result,
                     "reason": reason}, role)
    return {"id": action_id, "status": status, "result": result}


# ------------------------------------------------------------------ resolve + learning loop
def resolve(engine: Engine, iid: str, actor: str, role: str | None, reason: str, confirmed_cause: str | None,
            fix_steps: list[str] | None = None, save_known_issue: bool = True) -> dict[str, Any]:
    with engine.begin() as c:
        row = _incident_row(c, iid)
        if row["status"] == "RESOLVED":
            raise Conflict("incident already resolved")
        cause = (confirmed_cause or "").strip()
        c.execute(update(incident).where(incident.c.id == iid).values(
            status="RESOLVED", resolved_by=actor, resolved_at=utcnow(), confirmed_cause=cause or None))
        kid = None
        if cause and save_known_issue:
            kid = str(uuid.uuid4())
            c.execute(insert(known_issue).values(
                id=kid, kind=row["kind"], pattern=normalize(row["error"]), title=cause.splitlines()[0][:200],
                root_cause=cause, fix_steps=fix_steps or list(row["fix_steps"] or []), source_incident=iid,
                created_by=actor, created_at=utcnow(), hits=0, category=row["classification"]))
        _event(c, iid, "RESOLVED", actor, {"confirmed_cause": cause or None, "known_issue_id": kid, "reason": reason})
        store.audit(c, actor, "incident.resolve", iid, {"confirmed_cause": cause or None, "known_issue_id": kid,
                                                        "reason": reason}, role)
    return get_incident(engine, iid)


def regression_note(engine: Engine, iid: str) -> str:
    """Markdown note for a regression case (decoder issues: new golden corpus case under tests/golden/)."""
    with engine.connect() as c:
        r = _incident_row(c, iid)
        q = []
        if r["batch_id"]:
            q = c.execute(select(quarantine_event.c.lri, quarantine_event.c.reason, quarantine_event.c.raw_ref)
                          .where(quarantine_event.c.batch_id == uuid.UUID(r["batch_id"])).limit(5)).all()
    decoder = r["rule_id"] in ("codepage_mismatch", "catalog_drift", "unsupported_type", "value_compression",
                               "out_of_row_lob") or r["kind"] == "QUARANTINE"
    lines = [f"# Regression case - incident {iid}", "",
             f"- kind: `{r['kind']}`  severity: `{r['severity']}`  occurrences: {r['count']}",
             f"- connection / table: `{r['connection_id']}` / `{r['table_fqn']}`",
             f"- position: `{r['position']}`  batch: `{r['batch_id']}`",
             f"- signature (normalized): `{normalize(r['error'])}`",
             f"- matched rule: `{r['rule_id']}` (confidence {r['confidence']})",
             f"- confirmed root cause: {r['confirmed_cause'] or r['root_cause'] or '-'}", "", "## Reproduce", ""]
    if decoder:
        lines += ["Golden corpus case (decoder): copy the raw record(s) from the hub raw archive into "
                  "`tests/golden/db2/<case>/` (raw bytes + expected values from a SELECT on the dev copy, never "
                  "production values), add the catalog version valid at the position, and assert the decoder output.",
                  "", "| position | reason | raw_ref |", "|---|---|---|"]
        lines += [f"| `{lri}` | {scrub(reason)} | `{ref}` |" for lri, reason, ref in q] or ["| - | - | - |"]
    else:
        lines += ["Hub test: inject the same fault (see hub/alelt_hub/tests/test_zz_diagnose_e2e.py) and assert the "
                  f"incident is matched by rule `{r['rule_id']}`."]
    return "\n".join(lines) + "\n"


# ------------------------------------------------------------------ read
def _incident_dict(r: Any) -> dict[str, Any]:
    return {"id": r["id"], "kind": r["kind"], "severity": r["severity"], "status": r["status"], "title": r["title"],
            "connection_id": r["connection_id"], "table": r["table_fqn"], "batch_id": r["batch_id"],
            "job_id": r["job_id"], "agent_id": r["agent_id"], "position": r["position"], "error": r["error"],
            "count": r["count"], "first_seen": _iso(r["first_seen"]), "last_seen": _iso(r["last_seen"]),
            "rule_id": r["rule_id"], "root_cause": r["root_cause"], "explanation": r["explanation"],
            "confidence": r["confidence"], "fix_steps": list(r["fix_steps"] or []), "resolved_by": r["resolved_by"],
            "resolved_at": _iso(r["resolved_at"]), "confirmed_cause": r["confirmed_cause"],
            "classification": r["classification"] or "UNKNOWN", "source_data": r["source_data"]}


def list_incidents(engine: Engine, status: str | None = None, severity: str | None = None,
                   limit: int = 200) -> list[dict[str, Any]]:
    q = select(incident).order_by(incident.c.last_seen.desc()).limit(limit)
    if status == "ACTIVE":
        q = q.where(incident.c.status.in_(OPEN_STATES))
    elif status:
        q = q.where(incident.c.status == status)
    if severity:
        q = q.where(incident.c.severity == severity)
    with engine.connect() as c:
        return [_incident_dict(r) for r in c.execute(q).mappings().all()]


def get_incident(engine: Engine, iid: str) -> dict[str, Any]:
    with engine.connect() as c:
        out = _incident_dict(_incident_row(c, iid))
        out["timeline"] = [{"at": _iso(e["at"]), "kind": e["kind"], "actor": e["actor"],
                            "detail": {k: v for k, v in (e["detail"] or {}).items() if k != "logs"}}
                           for e in c.execute(select(incident_event).where(incident_event.c.incident_id == iid)
                                              .order_by(incident_event.c.id)).mappings().all()]
        out["actions"] = [{"id": a["id"], "kind": a["kind"], "params": a["params"], "description": a["description"],
                           "safe": bool(a["safe"]), "rule_id": a["rule_id"], "status": a["status"],
                           "created_at": _iso(a["created_at"]), "decided_by": a["decided_by"],
                           "decided_at": _iso(a["decided_at"]), "reason": a["reason"], "result": a["result"]}
                          for a in c.execute(select(action).where(action.c.incident_id == iid)
                                             .order_by(action.c.created_at)).mappings().all()]
        runs = c.execute(select(diag_run).where(diag_run.c.incident_id == iid)
                         .order_by(diag_run.c.at.desc()).limit(10)).mappings().all()
        out["findings"] = next((list(r["findings"] or []) for r in runs if r["stage"] == "A"), [])
        out["ai"] = [{"id": r["id"], "at": _iso(r["at"]), "actor": r["actor"], "provider": r["provider"],
                      "model": r["model"], "prompt_hash": r["prompt_hash"], "answer": r["answer"],
                      "tokens": r["tokens"], "error": r["error"]} for r in runs if r["stage"] == "B"]
        out["sensitive_samples"] = int(c.execute(select(func.count()).select_from(sensitive_sample).where(
            sensitive_sample.c.incident_id == iid)).scalar_one())
    from .code_fix import list_for

    out["code_fixes"] = list_for(engine, iid)
    return out


def reveal(engine: Engine, iid: str, actor: str, role: str | None, reason: str) -> list[dict[str, Any]]:
    """Raw source values of a SOURCE_DATA incident (incident.reveal; audited). Never part of an evidence pack."""
    with engine.begin() as c:
        _incident_row(c, iid)
        rows = [{"column": r["column_name"], "position": r["position"], "key": r["key"], "value": r["value"],
                 "at": _iso(r["at"])} for r in c.execute(select(sensitive_sample).where(
                     sensitive_sample.c.incident_id == iid).order_by(sensitive_sample.c.id)).mappings().all()]
        store.audit(c, actor, "incident.source_values.reveal", iid, {"reason": reason, "rows": len(rows)}, role)
    return rows


def summary(engine: Engine) -> dict[str, Any]:
    with engine.connect() as c:
        rows = c.execute(select(incident.c.severity, incident.c.kind, func.count())
                         .where(incident.c.status.in_(OPEN_STATES)).group_by(incident.c.severity, incident.c.kind)).all()
    by_sev: dict[str, int] = {}
    by_kind: dict[str, int] = {}
    for sev, kind, n in rows:
        by_sev[sev] = by_sev.get(sev, 0) + int(n)
        by_kind[kind] = by_kind.get(kind, 0) + int(n)
    return {"open": sum(by_sev.values()), "critical": by_sev.get("critical", 0), "warning": by_sev.get("warning", 0),
            "info": by_sev.get("info", 0), "by_kind": by_kind}


# ------------------------------------------------------------------ explain any operational error
EXPLAIN_KINDS = ("incident", "job", "batch", "quarantine", "agent_event", "alert")


def trigger_for(engine: Engine, kind: str, ref_id: str, labels: dict[str, str] | None = None) -> Trigger:
    from alelt_hub.db import batch

    with engine.connect() as c:
        if kind == "job":
            from .hooks import job_trigger

            r = c.execute(select(job).where(job.c.id == int(ref_id))).mappings().first()
            if r is None:
                raise NotFound(ref_id)
            return job_trigger(dict(r), r["error"] or "")
        if kind == "batch":
            r = c.execute(select(batch).where(batch.c.id == uuid.UUID(ref_id))).mappings().first()
            if r is None:
                raise NotFound(ref_id)
            failing = c.execute(select(job.c.id, job.c.error).where(
                job.c.kind == "decode", job.c.error.is_not(None),
                job.c.payload["connection_id"].as_string() == r["connection_id"])  # type: ignore[index]
                .order_by(job.c.id.desc()).limit(1)).first() if r["status"] != "LOADED" else None
            return Trigger(kind=IncidentKind.JOB_FAILED, connection_id=r["connection_id"], batch_id=str(r["id"]),
                           position=r["from_lri"], stage="decode", agent_id=r["agent_id"],
                           job_id=int(failing[0]) if failing else None,
                           error=r["error"] or (failing[1] if failing else "") or f"batch status {r['status']}")
        if kind == "quarantine":
            r = c.execute(select(quarantine_event).where(quarantine_event.c.id == uuid.UUID(ref_id))).mappings().first()
            if r is None:
                raise NotFound(ref_id)
            return Trigger(kind=IncidentKind.QUARANTINE, connection_id=r["connection_id"], table_fqn=r["table_fqn"],
                           batch_id=str(r["batch_id"]) if r["batch_id"] else None, position=r["lri"],
                           error=r["reason"], stage="decode")
        if kind == "agent_event":
            from .hooks import agent_event_kind

            r = c.execute(select(audit_event).where(audit_event.c.id == uuid.UUID(ref_id))).mappings().first()
            if r is None:
                raise NotFound(ref_id)
            typ = str(r["action"]).removeprefix("agent.event.")
            return Trigger(kind=agent_event_kind(typ), agent_id=r["target"], error=f"agent event {typ}",
                           detail={"event": typ, "detail": (r["detail"] or {}).get("detail", {})})
    if kind == "alert":
        lab = labels or {}
        return Trigger(kind=ALERT_KINDS.get(ref_id, IncidentKind.ALERT), connection_id=lab.get("connection_id"),
                       table_fqn=lab.get("table"), agent_id=lab.get("agent_id"), error=f"alert {ref_id}",
                       detail={"alert": ref_id, "labels": lab})
    raise NotFound(f"{kind}:{ref_id}")


def explain(engine: Engine, kind: str, ref_id: str, actor: str, allow_llm: bool, role: str | None = None,
            labels: dict[str, str] | None = None) -> ExplainResult:
    """Root cause + plain-language explanation for any operational error. Stage A always; LLM if allowed+configured."""
    if kind not in EXPLAIN_KINDS:
        raise NotFound(kind)
    if kind == "incident":
        pack = evidence_for(engine, ref_id)
        iid: str | None = ref_id
    else:
        t = trigger_for(engine, kind, ref_id, labels)
        with engine.connect() as c:
            level = _level(c)
        pack = evidence.collect(engine, t, level)
        iid = None
    with engine.begin() as c:
        cfg = llm.load_config(c)
        findings = evaluate(pack, _known(c))
        top = findings[0]
        rid = _run(c, iid=iid, ref_kind=kind, ref_id=ref_id, actor=actor, stage="A", findings=findings)
        res = ExplainResult(kind=kind, ref_id=ref_id, source="rules", rule_id=top.rule_id, root_cause=top.root_cause,
                            explanation=top.explanation, fix_steps=top.fix_steps, confidence=top.confidence,
                            needs_human=True, llm_configured=cfg.configured, findings=findings, run_id=rid,
                            classification=top.category)
        if allow_llm and cfg.configured:
            a, err, rid_b = _llm_stage(c, pack, findings, iid, kind, ref_id, actor)
            store.audit(c, actor, "diagnose.explain.ai", f"{kind}:{ref_id}", {"run_id": rid_b, "error": err}, role)
            if a is not None:
                ans = a.answer
                res = res.model_copy(update={"source": "llm", "root_cause": ans.root_cause,
                                             "explanation": ans.reasoning_summary, "fix_steps": ans.fix_steps,
                                             "confidence": ans.confidence, "needs_human": ans.needs_human,
                                             "run_id": rid_b,
                                             "classification": top.category if top.category.value != "UNKNOWN"
                                             else (ans.classification or top.category)})
            else:
                res = res.model_copy(update={"llm_error": err})
    return res
