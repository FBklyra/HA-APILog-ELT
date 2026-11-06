"""Console API contract (console/API_CONTRACT.md): permission matrix, reason/confirm rules, two-person rule,
lockout, retention bounds, revisions, import, schema decisions, recon, certification, switch."""
from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from alelt_common.util import utcnow
from alelt_hub.testing import FakeAgent, fake_catalog

from .conftest import TOKEN, role

CONN = "contractconn"


@pytest.fixture(scope="module")
def tables(client: Any) -> dict[str, str]:
    """Register a separate connection with KEYED, LOG and a non-captured NOCAP table; return {name: id}."""
    from alelt_common.models import TableInfo, TableRef

    a = FakeAgent(client, TOKEN, agent_id="contract-agent", connection_id=CONN)
    a.register()
    cat = fake_catalog(CONN)
    cat.tables.append(TableInfo(table=TableRef(schema="S", name="NOCAP"), capture_enabled=False,
                                columns=cat.tables[1].columns))
    a.catalog(cat)
    items = client.get(f"/api/v1/tables?connection_id={CONN}", headers=role("Viewer")).json()["items"]
    return {t["table"]["name"]: t["id"] for t in items}


def code(r: Any) -> str:
    return str(r.json()["error"]["code"])


MATRIX = {   # GET endpoint -> roles allowed (rbac.ts / endpointPermissions.ts)
    "/ladder": {"Viewer", "DataOperator", "PlatformAdmin", "Auditor"},
    "/tables": {"Viewer", "DataOperator", "PlatformAdmin"},
    "/batches": {"Viewer", "DataOperator"},
    "/quarantine": {"Viewer", "DataOperator"},
    "/agents": {"PlatformAdmin"},
    "/revisions": {"DataOperator"},
    "/import/runs": {"DataOperator"},
    "/schema-changes": {"DataOperator"},
    "/certification": {"DataOperator", "Auditor"},
    "/switch/requests": {"DataOperator", "Auditor"},
    "/hub/health": {"PlatformAdmin"},
    "/retention": {"PlatformAdmin"},
    "/users": {"PlatformAdmin"},
    "/audit": {"PlatformAdmin", "Auditor"},
}


@pytest.mark.parametrize("path", sorted(MATRIX))
def test_permission_matrix(client: Any, tables: dict[str, str], path: str) -> None:
    for r in ("Viewer", "DataOperator", "PlatformAdmin", "Auditor"):
        resp = client.get(f"/api/v1{path}", headers=role(r))
        want = 200 if r in MATRIX[path] else 403
        assert resp.status_code == want, (path, r, resp.text[:200])
        if want == 403:
            assert code(resp) == "FORBIDDEN"


def test_me_dev(client: Any) -> None:
    me = client.get("/api/v1/me", headers=role("Auditor", "carol@x")).json()
    assert me == {"oid": "carol@x", "name": "carol@x", "email": "carol@x", "dev": True, "roles": ["Auditor"]}


def test_reason_retention_and_audit(client: Any) -> None:
    adm = role("PlatformAdmin")
    r = client.put("/api/v1/retention", headers=adm, json={"raw_archive_days": 45})
    assert r.status_code == 400 and code(r) == "REASON_REQUIRED"
    r = client.put("/api/v1/retention", headers=adm, json={"raw_archive_days": 29, "reason": "too short"})
    assert r.status_code == 422 and code(r) == "OUT_OF_RANGE"
    before = client.get("/api/v1/audit?action=retention.update", headers=adm).json()["total"]
    r = client.put("/api/v1/retention", headers={**adm, "X-ALELT-Correlation-Id": "corr-7"},
                   json={"raw_archive_days": 45, "reason": "policy"})
    assert r.status_code == 200 and r.json()["raw_archive_days"] == 45
    assert client.get("/api/v1/retention", headers=adm).json()["raw_archive_days"] == 45
    ev = client.get("/api/v1/audit?action=retention.update", headers=adm).json()
    assert ev["total"] == before + 1 and ev["items"][0]["correlation_id"] == "corr-7"
    csv_ = client.get("/api/v1/audit/export?format=csv", headers=role("Auditor"))
    assert csv_.status_code == 200 and csv_.headers["content-type"].startswith("text/csv")
    assert "retention.update" in csv_.text


def test_lockout(client: Any) -> None:
    adm = role("PlatformAdmin")
    only_view = {"reason": "x" * 4, "group_mappings": [{"group_id": "g1", "group_name": "Readers", "role": "Viewer"}]}
    r = client.put("/api/v1/users/group-mappings", headers=adm, json=only_view)
    assert r.status_code == 409 and code(r) == "LOCKOUT"
    ok = {"reason": "initial", "group_mappings": [*only_view["group_mappings"],
                                                  {"group_id": "g2", "group_name": "Admins", "role": "PlatformAdmin"}]}
    r = client.put("/api/v1/users/group-mappings", headers=adm, json=ok)
    assert r.status_code == 200 and len(r.json()["group_mappings"]) == 2


def test_table_config_rules_and_revisions(client: Any, tables: dict[str, str]) -> None:
    op = role("DataOperator")
    url = f"/api/v1/tables/{tables['KEYED']}/config"
    r = client.put(f"/api/v1/tables/{tables['NOCAP']}/config", headers=op, json={"reason": "on", "enabled": True})
    assert r.status_code == 409 and code(r) == "CAPTURE_DISABLED"
    r = client.put(url, headers=op, json={"reason": "no", "columns": [{"name": "ID", "enabled": False, "hashed": False}]})
    assert r.status_code == 409 and code(r) == "KEY_COLUMN"
    r1 = client.put(url, headers=op, json={"reason": "hash name", "columns": [{"name": "NAME", "enabled": True,
                                                                             "hashed": True}]}).json()
    r2 = client.put(url, headers=op, json={"reason": "unhash", "columns": [{"name": "NAME", "enabled": True,
                                                                          "hashed": False}]}).json()
    assert r2["revision"] > r1["revision"]
    assert not next(c for c in r2["columns"] if c["name"] == "NAME")["hashed"]
    rev1 = r1["revision"]
    d = client.get(f"/api/v1/revisions/{rev1}/diff", headers=op).json()
    assert any(e["path"] == "tables.S.KEYED.columns.NAME.hashed" and e["after"] is True for e in d)
    bad = client.post(f"/api/v1/revisions/{rev1}/rollback", headers=op, json={"reason": "back", "confirm_text": "nope"})
    assert bad.status_code == 400 and code(bad) == "CONFIRM_MISMATCH"
    rb = client.post(f"/api/v1/revisions/{rev1}/rollback", headers=op,
                     json={"reason": "back", "confirm_text": f"REVISION_ROLLBACK {rev1}"})
    assert rb.status_code == 200 and rb.json()["source"] == "rollback" and rb.json()["current"] is True
    det = client.get(f"/api/v1/tables/{tables['KEYED']}", headers=op).json()
    assert next(c for c in det["columns"] if c["name"] == "NAME")["hashed"] is True
    y = client.get(f"/api/v1/revisions/{rb.json()['id']}/export", headers=op)
    assert y.status_code == 200 and "schemas:" in y.text and "connection_id:" in y.text


def test_resync_reprocess(client: Any, tables: dict[str, str]) -> None:
    op = role("DataOperator")
    base = f"/api/v1/tables/{tables['KEYED']}"
    r = client.post(f"{base}/resync", headers=op, json={"reason": "fix", "confirm_text": "RESYNC S.X"})
    assert r.status_code == 400 and code(r) == "CONFIRM_MISMATCH"
    r = client.post(f"{base}/resync", headers=op, json={"reason": "fix", "confirm_text": "RESYNC S.KEYED"})
    assert r.status_code == 200 and r.json()["job_id"].isdigit()
    r = client.post(f"{base}/reprocess", headers=op, json={"reason": "fix", "confirm_text": "REPROCESS S.KEYED"})
    assert r.status_code == 400
    r = client.post(f"{base}/reprocess", headers=op, json={"reason": "fix", "confirm_text": "REPROCESS S.KEYED",
                                                          "from_pos": "1:0", "to_pos": "1:F"})
    assert r.status_code == 200
    assert client.post(f"{base}/resync", headers=role("Viewer"),
                       json={"reason": "x", "confirm_text": "RESYNC S.KEYED"}).status_code == 403


def test_console_actions_dedup_and_job_retry(client: Any, tables: dict[str, str]) -> None:
    """HC-28: a double click (same action, same target, job still queued) is ONE job; XC-10: operator retry."""
    op = role("DataOperator")
    base = f"/api/v1/tables/{tables['KEYED']}"
    body = {"reason": "fix", "confirm_text": "RESYNC S.KEYED"}
    j1 = client.post(f"{base}/resync", headers=op, json=body).json()["job_id"]
    j2 = client.post(f"{base}/resync", headers=op, json=body).json()["job_id"]
    assert j1 == j2
    rp = {"reason": "fix", "confirm_text": "REPROCESS S.KEYED", "from_pos": "1:0", "to_pos": "1:F"}
    k1 = client.post(f"{base}/reprocess", headers=op, json=rp).json()["job_id"]
    assert client.post(f"{base}/reprocess", headers=op, json=rp).json()["job_id"] == k1
    other = client.post(f"{base}/reprocess", headers=op, json={**rp, "to_pos": "1:E"}).json()["job_id"]
    assert other != k1                                               # another range = another job
    rr = {"reason": "check", "table_ids": [tables["KEYED"]], "check_kind": "ROW_COUNT"}
    a = client.post("/api/v1/recon/runs", headers=role("DataOperator"), json=rr)
    if a.status_code == 200:
        b = client.post("/api/v1/recon/runs", headers=role("DataOperator"), json=rr)
        assert b.json()["id"] == a.json()["id"]
    r = client.post(f"/api/v1/jobs/{j1}/retry", headers=op, json={"reason": "now"})
    assert r.status_code == 200 and r.json()["status"] == "QUEUED"
    assert client.post("/api/v1/jobs/999999999/retry", headers=op, json={"reason": "x"}).status_code == 404
    assert client.post(f"/api/v1/jobs/{j1}/retry", headers=role("Viewer"), json={"reason": "x"}).status_code == 403


def test_schema_change_decision(client: Any, tables: dict[str, str]) -> None:
    import uuid

    from alelt_hub.db import get_engine, schema_change_event

    sid = uuid.uuid4()
    with get_engine().begin() as c:
        c.execute(schema_change_event.insert().values(id=sid, table_id=uuid.UUID(tables["KEYED"]), kind="ADD_COLUMN",
                                                      detail={"column": "NEW_COL"}))
    op = role("DataOperator")
    lst = client.get("/api/v1/schema-changes", headers=op).json()
    assert any(x["id"] == str(sid) and x["status"] == "PENDING" for x in lst)
    r = client.post(f"/api/v1/schema-changes/{sid}/decision", headers=op, json={"reason": "ok", "decision": "APPROVE"})
    assert r.status_code == 200 and r.json()["status"] == "APPROVED"
    r = client.post(f"/api/v1/schema-changes/{sid}/decision", headers=op, json={"reason": "ok", "decision": "REJECT"})
    assert r.status_code == 409 and code(r) == "NOT_PENDING"


def test_recon_and_certification(client: Any, tables: dict[str, str]) -> None:
    op = role("DataOperator")
    r = client.post("/api/v1/recon/runs", headers=op, json={"reason": "check", "table_ids": [tables["KEYED"]],
                                                            "check_kind": "NOPE"})
    assert r.status_code == 400
    r = client.post("/api/v1/recon/runs", headers=op, json={"reason": "check", "table_ids": [tables["KEYED"]],
                                                            "check_kind": "ROW_COUNT"})
    assert r.status_code == 200 and r.json()["check_kind"] == "ROW_COUNT"
    cert = client.get("/api/v1/certification", headers=role("Auditor")).json()
    keyed = next(x for x in cert if x["table_id"] == tables["KEYED"])
    assert [c["criterion"] for c in keyed["criteria"]][0] == "ROW_COUNT_PARITY"
    r = client.post(f"/api/v1/certification/{tables['KEYED']}/signoff", headers=op, json={"reason": "sign"})
    assert r.status_code == 409 and code(r) == "CRITERIA_NOT_MET"


def test_import_runs(client: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    op = role("DataOperator")
    monkeypatch.delenv("ALELT_IMPORT_FILE", raising=False)
    r = client.post("/api/v1/import/runs", headers=op, json={"reason": "import"}).json()
    assert r["status"] == "FAILED"
    acc = client.post(f"/api/v1/import/runs/{r['id']}/accept", headers=op, json={"reason": "go"})
    assert acc.status_code == 409 and code(acc) == "NOT_DONE"
    f = tmp_path / "tables.yaml"
    f.write_text("connection_id: contractconn\nschemas:\n  S:\n    tables:\n      LOG:\n        enabled: false\n",
                 encoding="utf-8")
    monkeypatch.setenv("ALELT_IMPORT_FILE", str(f))
    r = client.post("/api/v1/import/runs", headers=op, json={"reason": "import"}).json()
    assert r["status"] == "DONE" and any(e["path"].startswith("tables.S.LOG") for e in r["diff"])
    acc = client.post(f"/api/v1/import/runs/{r['id']}/accept", headers=op, json={"reason": "go"})
    assert acc.status_code == 200 and acc.json()["source"] == "alelt_import"
    runs = client.get("/api/v1/import/runs", headers=op).json()
    assert next(x for x in runs if x["id"] == r["id"])["status"] == "ACCEPTED"


def test_switch_two_person_rule(client: Any, tables: dict[str, str]) -> None:
    from sqlalchemy import select, update

    from alelt_hub.db import get_engine, switch_request, switch_state

    alice, bob = role("DataOperator", "alice@x"), role("DataOperator", "bob@x")
    scope = {"table_id": tables["KEYED"]}
    r = client.post("/api/v1/switch", headers=alice, json={"scope": scope, "target": "ALELT", "reason": "go",
                                                           "confirm_text": "SWITCH S.KEYED"})
    assert r.status_code == 409 and code(r) == "NOT_CERTIFIED"
    r = client.post("/api/v1/switch", headers=alice, json={"scope": scope, "target": "LEGACY", "reason": "go",
                                                           "confirm_text": "SWITCH S.KEYED"})
    assert r.status_code == 400 and code(r) == "CONFIRM_MISMATCH"
    req = client.post("/api/v1/switch", headers=alice, json={"scope": scope, "target": "LEGACY", "reason": "drill",
                                                             "confirm_text": "ROLLBACK S.KEYED"}).json()
    assert req["status"] == "PENDING_APPROVAL" and req["requested_by"] == "alice@x"
    url = f"/api/v1/switch/requests/{req['id']}"
    body = {"reason": "approve", "confirm_text": "ROLLBACK S.KEYED"}
    r = client.post(f"{url}/approve", headers=alice, json=body)
    assert r.status_code == 409 and code(r) == "TWO_PERSON_RULE"
    assert client.post(f"{url}/approve", headers=role("Auditor", "aud@x"), json=body).status_code == 403
    r = client.post(f"{url}/approve", headers=bob, json=body)
    assert r.status_code == 200 and r.json()["status"] == "EXECUTED" and r.json()["approved_by"] == "bob@x"
    with get_engine().connect() as c:
        st = c.execute(select(switch_state.c.active_source).where(switch_state.c.table_id == tables["KEYED"])).scalar()
    assert st == "LEGACY"
    r = client.post(f"{url}/approve", headers=bob, json=body)
    assert r.status_code == 409 and code(r) == "NOT_PENDING"

    # wave scope + expiry
    w = client.post("/api/v1/certification/waves", headers=alice, json={"reason": "plan", "name": "wave-1",
                                                                         "table_ids": [tables["KEYED"], tables["LOG"]],
                                                                         "planned_at": None})
    assert w.status_code == 200 and w.json()["status"] == "PLANNED"
    req = client.post("/api/v1/switch", headers=alice, json={"scope": {"wave_id": w.json()["id"]}, "target": "LEGACY",
                                                             "reason": "drill", "confirm_text": "ROLLBACK wave-1"}).json()
    with get_engine().begin() as c:
        c.execute(update(switch_request).where(switch_request.c.id == req["id"])
                  .values(expires_at=utcnow() - timedelta(hours=1)))   # portable: no SQL interval math
    r = client.post(f"/api/v1/switch/requests/{req['id']}/approve", headers=bob,
                    json={"reason": "late", "confirm_text": "ROLLBACK wave-1"})
    assert r.status_code == 409 and code(r) == "EXPIRED"
    lst = client.get("/api/v1/switch/requests", headers=role("Auditor")).json()
    assert next(x for x in lst if x["id"] == req["id"])["status"] == "EXPIRED"
    rej = client.post("/api/v1/switch", headers=alice, json={"scope": scope, "target": "LEGACY", "reason": "again",
                                                             "confirm_text": "ROLLBACK S.KEYED"}).json()
    r = client.post(f"/api/v1/switch/requests/{rej['id']}/reject", headers=bob, json={"reason": "not now"})
    assert r.status_code == 200 and r.json()["status"] == "REJECTED"


def test_hub_health_and_agents_shape(client: Any, tables: dict[str, str]) -> None:
    adm = role("PlatformAdmin")
    h = client.get("/api/v1/hub/health", headers=adm).json()
    assert {"services", "queues", "spool_used_bytes", "destination", "credentials"} <= set(h)
    assert h["destination"]["credits_used_month"] is None        # XC-28: unknown is null, never a fake 0
    r = client.get("/api/v1/retention", headers=adm).json()
    assert r["postgres_backups"] is None and r["wal_archiving_ok"] in (None, True, False)
    from alelt_hub.db import get_engine

    if get_engine().dialect.name == "sqlite":
        assert r["wal_archiving_ok"] is None
    ag = next(a for a in client.get("/api/v1/agents", headers=adm).json() if a["id"] == "contract-agent")
    assert ag["connection_id"] == CONN and ag["engine"] == "fake" and ag["database"] == "FAKE"
