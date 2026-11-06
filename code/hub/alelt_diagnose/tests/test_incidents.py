"""Incident lifecycle on SQLite (same code runs on Postgres in hub/alelt_hub/tests/test_diagnose_e2e.py)."""
from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import insert, select

from alelt_common.util import utcnow
from alelt_diagnose import hooks, incidents as inc
from alelt_diagnose.bundle import incidents_summary
from alelt_diagnose.models import IncidentKind, Trigger


def _job(engine: Any, error: str) -> int:
    from alelt_hub.db import connection, job

    with engine.begin() as c:
        if c.execute(select(connection.c.id).where(connection.c.id == "c1")).first() is None:
            c.execute(insert(connection).values(id="c1", engine="fake"))
        return int(c.execute(insert(job).values(kind="decode", payload={"connection_id": "c1"}, status="QUEUED",
                                                attempts=1, error=error).returning(job.c.id)).scalar_one())


def test_open_dedup_diagnose_approve_resolve_learn(engine: Any) -> None:
    jid = _job(engine, "PipelineError: spool corrupt for batch x")
    err = RuntimeError("spool corrupt for batch 1b2c3d4e-0000-0000-0000-000000000001")
    iid = hooks.job_failed(engine, {"id": jid, "kind": "decode", "payload": {"connection_id": "c1"}, "attempts": 1}, err)
    again = hooks.job_failed(engine, {"id": jid, "kind": "decode", "payload": {"connection_id": "c1"}, "attempts": 2},
                             RuntimeError("spool corrupt for batch 9f2c3d4e-0000-0000-0000-000000000009"))
    assert iid and again == iid                                            # dedup by signature
    d = inc.get_incident(engine, iid)
    assert d["count"] == 2 and d["status"] == "DIAGNOSED" and d["rule_id"] == "spool_corrupt"
    assert [e["kind"] for e in d["timeline"]][:3] == ["OPENED", "DIAGNOSED", "ACTION_PROPOSED"]
    assert "traceback" in str(inc.evidence_for(engine, iid).trigger.detail)
    (act,) = d["actions"]
    assert act["kind"] == "RETRY_JOB" and act["safe"] and act["status"] == "PROPOSED"

    r = inc.decide_action(engine, act["id"], "op@x", "DataOperator", "retry after restore", approve=True)
    assert r["status"] == "EXECUTED" and r["result"]["job_id"] == jid
    with pytest.raises(inc.Conflict):
        inc.decide_action(engine, act["id"], "op@x", "DataOperator", "again", approve=True)

    res = inc.resolve(engine, iid, "op@x", "DataOperator", "fixed", "Spool disk was restored from an old backup")
    assert res["status"] == "RESOLVED" and res["confirmed_cause"].startswith("Spool disk")
    note = inc.regression_note(engine, iid)
    assert "Regression case" in note and "spool_corrupt" in note

    # learning loop: next occurrence is matched by the known issue (new incident: old one resolved)
    iid2 = hooks.job_failed(engine, {"id": jid, "kind": "decode", "payload": {"connection_id": "c1"}, "attempts": 3},
                            RuntimeError("spool corrupt for batch 7a2c3d4e-0000-0000-0000-000000000007"))
    d2 = inc.get_incident(engine, iid2)
    assert iid2 != iid and d2["rule_id"].startswith("known:") and "old backup" in d2["root_cause"]

    from alelt_hub.db import audit_event

    with engine.connect() as c:
        acts = {r[0] for r in c.execute(select(audit_event.c.action))}
    assert {"incident.action.approve", "incident.resolve"} <= acts


def test_reject_auto_actions_explain_summary_bundle(engine: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    jid = _job(engine, "OperationalError: connection refused")
    monkeypatch.setenv("ALELT_DIAG_AUTO_ACTIONS", "RETRY_JOB,RESYNC_TABLE")          # RESYNC is never auto (not safe)
    assert inc.auto_actions() == frozenset({"RETRY_JOB"})
    iid = hooks.job_failed(engine, {"id": jid, "kind": "decode", "payload": {"connection_id": "c1"}, "attempts": 1},
                           ConnectionError("psycopg.OperationalError: connection refused"))
    d = inc.get_incident(engine, iid)
    assert d["kind"] == "DESTINATION_ERROR" and d["rule_id"] == "network_tls"
    assert d["actions"][0]["status"] == "EXECUTED" and d["actions"][0]["decided_by"] == "alelt_diagnose:auto"

    monkeypatch.delenv("ALELT_DIAG_AUTO_ACTIONS")
    qid, _ = inc.open_incident(engine, Trigger(kind=IncidentKind.QUARANTINE, connection_id="c1", table_fqn="S.T",
                                               position="1:0A", error="row image: BAD_CODEPAGE: NAME codepage 37"))
    a = inc.get_incident(engine, qid)["actions"][0]
    assert a["kind"] == "REDECODE_RANGE"
    assert inc.decide_action(engine, a["id"], "op", "DataOperator", "not yet", approve=False)["status"] == "REJECTED"

    ex = inc.explain(engine, "job", str(jid), "viewer", allow_llm=False)
    assert ex.source == "rules" and ex.rule_id == "network_tls" and not ex.llm_configured and ex.fix_steps
    ex = inc.explain(engine, "alert", "AleltAgentOffline", "viewer", allow_llm=True, labels={"agent_id": "a9"})
    assert ex.rule_id == "agent_offline"

    s = inc.summary(engine)
    assert s["open"] == 2 and s["critical"] == 1 and s["by_kind"]["QUARANTINE"] == 1
    b = incidents_summary(engine)
    assert b["summary"]["open"] == 2 and len(b["incidents"]) == 2 and "error" not in b["incidents"][0]


def test_scan_triggers(engine: Any) -> None:
    from alelt_hub.db import agent, audit_event, checkpoint_mirror, connection

    with engine.begin() as c:
        c.execute(insert(connection).values(id="c1", engine="fake"))
        c.execute(insert(agent).values(id="a1", connection_id="c1", status="CAPTURING",
                                       last_heartbeat=utcnow() - timedelta(minutes=30)))
        c.execute(insert(agent).values(id="a2", connection_id="c1", status="PAUSED_BY_GOVERNOR", last_heartbeat=utcnow()))
        c.execute(insert(checkpoint_mirror).values(stream="c1", connection_id="c1",
                                                   last_commit_ts=utcnow() - timedelta(hours=2)))
        c.execute(insert(audit_event).values(id=uuid.uuid4(), actor="agent:a2", action="agent.event.SPOOL_FULL",
                                             target="a2", detail={"detail": {}}))
    assert hooks.scan(engine) == 4
    kinds = {i["kind"] for i in inc.list_incidents(engine, "ACTIVE")}
    assert kinds == {"AGENT_OFFLINE", "GOVERNOR_PAUSE", "LAG_ABOVE_TARGET", "BACKPRESSURE"}
    assert hooks.scan(engine) == 3                                       # audit events are consumed once
    assert len(inc.list_incidents(engine, "ACTIVE")) == 4                # recurrences dedup


def test_log_sequence_reused_event_opens_an_agent_incident(engine: Any) -> None:
    """SRC-db2-11: the C agent reports db2ReadLog SQLCODE 2653 (log sequence reused after restore / rollforward) as
    event LOG_SEQUENCE_REUSED; the hub scan turns it into an AGENT_ERROR incident carrying the event detail."""
    from alelt_hub.db import agent, audit_event, connection

    with engine.begin() as c:
        c.execute(insert(connection).values(id="c9", engine="db2"))
        c.execute(insert(agent).values(id="a9", connection_id="c9", status="CAPTURING", last_heartbeat=utcnow()))
        c.execute(insert(audit_event).values(id=uuid.uuid4(), actor="agent:a9", action="agent.event.LOG_SEQUENCE_REUSED",
                                             target="a9", detail={"detail": {"sqlcode": 2653}}))
    hooks.scan(engine)
    got = [i for i in inc.list_incidents(engine, "ACTIVE") if i.get("agent_id") == "a9" or "a9" in str(i)]
    assert got and got[0]["kind"] == "AGENT_ERROR" and "LOG_SEQUENCE_REUSED" in str(got[0])
