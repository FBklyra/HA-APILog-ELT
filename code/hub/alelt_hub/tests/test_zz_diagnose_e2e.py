"""Self-diagnosis end-to-end (runs last: it adds connections, quarantine and checkpoints that the L0 tests count).
Inject faults, assert an incident with the right root cause appears; console API
(permissions, reason, audit), explain, AI-assistant admin settings against a fake LLM endpoint."""
from __future__ import annotations

import dataclasses
import json
import socket
import threading
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select, update

from alelt_hub.testing import FakeAgent, ev, fake_catalog

from .conftest import TOKEN, drain, role

API = "/api/v1"


def _incidents(client: Any, **q: str) -> list[dict[str, Any]]:
    r = client.get(f"{API}/incidents", params=q, headers=role("Viewer"))
    assert r.status_code == 200, r.text
    return list(r.json())


def _agent(client: Any, cid: str) -> FakeAgent:
    a = FakeAgent(client, TOKEN, agent_id=f"{cid}-agent", connection_id=cid)
    a.register()
    a.catalog(fake_catalog(cid))
    return a


def _park_jobs(cid: str) -> None:
    """Keep failed jobs of a fault-injected connection from bothering later tests."""
    from alelt_hub.db import get_engine, job

    with get_engine().begin() as c:
        c.execute(update(job).where(job.c.payload["connection_id"].as_string() == cid).values(status="DONE"))  # type: ignore[index]


def test_gap_and_agent_auth_open_incidents(client: Any) -> None:
    a = _agent(client, "diaggap")
    assert a.send([ev(1, "T1", "COMMIT")]).status_code == 200      # no row events: nothing reaches the landing
    hdr, body = a.make_batch([ev(9, "T2", "COMMIT")], from_pos="1:00000000000000F0")
    assert a.post(hdr, body).status_code == 409
    gap = [i for i in _incidents(client, status="ACTIVE") if i["connection_id"] == "diaggap"]
    assert [i["kind"] for i in gap] == ["POSITION_GAP"] and gap[0]["rule_id"] == "position_gap"
    assert gap[0]["position"] == "1:00000000000000F0"

    r = client.post("/agent/v1/heartbeat", headers={"X-ALELT-Protocol": "1", "X-ALELT-Agent-Token": "wrong"}, json={})
    assert r.status_code == 401
    auth = [i for i in _incidents(client) if i["kind"] == "AGENT_AUTH"]
    assert auth and auth[0]["rule_id"] == "agent_auth_rejected"


def test_corrupt_batch_body_incident(client: Any, worker: Any) -> None:
    a = _agent(client, "diagcorrupt")
    hdr, body = a.make_batch([ev(1, "T1", "INSERT", "S.KEYED", after={"ID": 1, "NAME": "Zoé Secret-Value"}),
                              ev(2, "T1", "COMMIT")])
    assert a.post(hdr, body).status_code == 200
    from alelt_hub.db import batch, get_engine

    with get_engine().connect() as c:
        path = Path(c.execute(select(batch.c.spool_path).where(batch.c.id == hdr.batch_id)).scalar_one())
    path.write_bytes(zlib.compress(b'{"pos": "1:0000000000000001"}\n'))        # bit rot / bad restore
    drain(worker)
    _park_jobs("diagcorrupt")
    inc = [i for i in _incidents(client) if i["connection_id"] == "diagcorrupt"]
    assert len(inc) == 1 and inc[0]["kind"] == "JOB_FAILED" and inc[0]["rule_id"] == "spool_corrupt", inc
    assert inc[0]["batch_id"] == str(hdr.batch_id) and inc[0]["severity"] == "critical"

    iid = inc[0]["id"]
    d = client.get(f"{API}/incidents/{iid}", headers=role("Auditor")).json()
    assert d["actions"][0]["kind"] == "RETRY_JOB" and d["findings"][0]["rule_id"] == "spool_corrupt"
    pack = client.get(f"{API}/incidents/{iid}/evidence", headers=role("Viewer"))
    assert pack.status_code == 200 and "Zoé" not in pack.text and pack.json()["batch"]["id"] == str(hdr.batch_id)

    # approval: Viewer/Auditor forbidden, reason required, audited
    aid = d["actions"][0]["id"]
    body_ok = {"decision": "REJECT", "reason": "restore first"}
    assert client.post(f"{API}/incident-actions/{aid}/decision", headers=role("Viewer"), json=body_ok).status_code == 403
    r = client.post(f"{API}/incident-actions/{aid}/decision", headers=role("DataOperator"), json={"decision": "REJECT"})
    assert r.status_code == 400 and r.json()["error"]["code"] == "REASON_REQUIRED"
    assert client.post(f"{API}/incident-actions/{aid}/decision", headers=role("DataOperator", "op@x"),
                       json=body_ok).json()["status"] == "REJECTED"
    r = client.post(f"{API}/incidents/{iid}/resolve", headers=role("PlatformAdmin", "adm@x"),
                    json={"reason": "restored", "confirmed_root_cause": "Spool volume restored from stale snapshot"})
    assert r.status_code == 200 and r.json()["status"] == "RESOLVED"
    audit = client.get(f"{API}/audit?action=incident.resolve", headers=role("Auditor")).json()["items"]
    assert any(x["target"] == iid for x in audit)


def test_destination_down_incident(client: Any, worker: Any) -> None:
    from alelt_hub.worker import Worker

    a = _agent(client, "diagdest")
    drain(worker)
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()                                                              # nothing listens there: refused
    bad = Worker(worker.engine, dataclasses.replace(
        worker.s, dest_url=f"postgresql://loader:pw-xyz@127.0.0.1:{port}/x?connect_timeout=3"), "bad-dest-worker")
    assert a.send([ev(1, "T1", "COMMIT")]).status_code == 200
    drain(bad)
    _park_jobs("diagdest")
    inc = [i for i in _incidents(client) if i["connection_id"] == "diagdest"]
    assert inc and inc[0]["kind"] == "DESTINATION_ERROR" and inc[0]["rule_id"] == "network_tls", inc
    assert "pw-xyz" not in json.dumps(inc)
    d = client.post(f"{API}/incidents/{inc[0]['id']}/diagnose", headers=role("Viewer"), json={}).json()
    assert d["rule_id"] == "network_tls"
    r = client.post(f"{API}/incidents/{inc[0]['id']}/diagnose", headers=role("Viewer"), json={"use_llm": True})
    assert r.status_code == 403


def test_quarantine_incident_and_explain(client: Any, worker: Any) -> None:
    a = _agent(client, "diagq")
    hdr, body = a.make_batch([ev(1, "T1", "COMMIT")])
    raw = zlib.decompress(body) + b"not json at all\n"
    from alelt_common.util import sha256_hex

    hdr.raw_bytes, hdr.sha256 = len(raw), sha256_hex(raw)
    assert a.post(hdr, zlib.compress(raw)).status_code == 200
    drain(worker)
    inc = [i for i in _incidents(client) if i["connection_id"] == "diagq"]
    # unattributed record: quarantine incident + connection integrity SUSPECT (F-01: never silent)
    assert sorted(i["kind"] for i in inc) == ["QUARANTINE", "TABLE_OUT_OF_SYNC"]
    q = client.get(f"{API}/quarantine", headers=role("Viewer")).json()["items"]
    qid = next(x["id"] for x in q if x["raw_ref"] and x["raw_ref"]["batch_id"] == str(hdr.batch_id))
    ex = client.post(f"{API}/diagnose/explain", headers=role("Viewer"), json={"kind": "quarantine", "ref_id": qid})
    assert ex.status_code == 200 and ex.json()["source"] == "rules" and ex.json()["fix_steps"]
    ex = client.post(f"{API}/diagnose/explain", headers=role("Viewer"), json={"kind": "batch", "ref_id": str(hdr.batch_id)})
    assert ex.status_code == 200


@pytest.fixture()
def fake_llm() -> Any:
    seen: list[dict[str, Any]] = []

    class H(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            seen.append(body)
            user = body["messages"][-1]["content"]
            text = '{"ok": true}' if "Reply exactly" in user else json.dumps(
                {"root_cause": "AI says: gap after agent reinstall", "confidence": 0.7,
                 "reasoning_summary": "expected_from differs", "fix_steps": ["START_CAPTURE"], "risk": "medium",
                 "needs_human": True})
            out = json.dumps({"model": body["model"], "stop_reason": "end_turn",
                              "content": [{"type": "text", "text": text}],
                              "usage": {"input_tokens": 10, "output_tokens": 5}}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(out)

        def log_message(self, *_: object) -> None:
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}", seen
    srv.shutdown()


def test_ai_assistant_admin_and_ask_ai(client: Any, fake_llm: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    url, seen = fake_llm
    assert client.get(f"{API}/meta", headers=role("Viewer")).json()["llm"]["configured"] is False
    assert client.get(f"{API}/admin/llm", headers=role("DataOperator")).status_code == 403
    adm = role("PlatformAdmin", "adm@x")
    r = client.put(f"{API}/admin/llm", headers=adm, json={"enabled": True})
    assert r.status_code == 400
    monkeypatch.delenv("ALELT_DIAG_SECRET_KEY", raising=False)
    r = client.put(f"{API}/admin/llm", headers=adm, json={"reason": "r", "api_key": "sk-test-key-0123456789"})
    assert r.status_code == 409 and r.json()["error"]["code"] == "NO_ENCRYPTION_KEY"
    monkeypatch.setenv("ALELT_DIAG_SECRET_KEY", "e2e-only")
    t = client.post(f"{API}/admin/llm/test", headers=adm, json={"provider": "anthropic", "url": url, "model": "m-e2e",
                                                                 "api_key": "sk-test-key-0123456789"})
    assert t.status_code == 200 and t.json()["ok"] and t.json()["model"] == "m-e2e"
    r = client.put(f"{API}/admin/llm", headers=adm, json={
        "reason": "enable AI", "enabled": True, "provider": "anthropic", "url": url, "model": "m-e2e",
        "api_key": "sk-test-key-0123456789", "max_tokens_day": 100000})
    v = r.json()
    assert r.status_code == 200 and v["key_set"] and v["configured"] and "sk-test" not in r.text
    assert client.get(f"{API}/meta", headers=role("Viewer")).json()["llm"]["configured"] is True

    gap = next(i for i in _incidents(client) if i["kind"] == "POSITION_GAP")
    r = client.post(f"{API}/incidents/{gap['id']}/diagnose", headers=role("DataOperator"),
                    json={"use_llm": True, "reason": "second opinion"})
    assert r.status_code == 200 and r.json()["ai"][0]["answer"]["root_cause"].startswith("AI says")
    assert r.json()["ai"][0]["prompt_hash"] and r.json()["actions"] == []        # LLM never creates actions
    ex = client.post(f"{API}/diagnose/explain", headers=role("DataOperator"),
                     json={"kind": "incident", "ref_id": gap["id"]}).json()
    assert ex["source"] == "llm" and ex["llm_configured"]
    ex = client.post(f"{API}/diagnose/explain", headers=role("Viewer"), json={"kind": "incident", "ref_id": gap["id"]})
    assert ex.json()["source"] == "rules"                                         # Viewer: rules only
    assert all("sk-test" not in json.dumps(b) for b in seen)
    audit = client.get(f"{API}/audit?action=llm.settings.update", headers=role("Auditor")).json()["items"]
    assert audit and "sk-test" not in json.dumps(audit)
    client.put(f"{API}/admin/llm", headers=adm, json={"reason": "off", "enabled": False, "api_key": ""})


def test_permissions_and_summary(client: Any) -> None:
    for r in ("Viewer", "DataOperator", "PlatformAdmin", "Auditor"):
        assert client.get(f"{API}/incidents", headers=role(r)).status_code == 200
        assert client.get(f"{API}/incidents/summary", headers=role(r)).status_code == 200
    assert client.post(f"{API}/diagnose/scan", headers=role("Viewer")).status_code == 403
    assert client.post(f"{API}/diagnose/scan", headers=role("DataOperator")).status_code == 200
    assert client.get(f"{API}/incidents/nope", headers=role("Viewer")).status_code == 404


def test_source_data_incident_reveal_and_code_fix_guard(client: Any, worker: Any) -> None:
    a = _agent(client, "diagsrc")
    long_name = "N" * 60 + "-private-value"                                  # NAME is VARCHAR(40) in the catalog
    assert a.send([ev(1, "T1", "INSERT", "S.KEYED", after={"ID": 901, "NAME": long_name}),
                   ev(2, "T1", "COMMIT")]).status_code == 200
    drain(worker)
    inc = [i for i in _incidents(client) if i["connection_id"] == "diagsrc" and i["kind"] == "SOURCE_DATA"]
    assert len(inc) == 1 and inc[0]["classification"] == "SOURCE_DATA", inc
    sd = inc[0]["source_data"]
    assert sd["issue"] == "OUT_OF_RANGE" and sd["column"] == "NAME" and sd["key_columns"] == ["ID"]
    assert sd["dba_sql"].startswith("SELECT") and "WITH UR" in sd["dba_sql"]
    iid = inc[0]["id"]
    detail = client.get(f"{API}/incidents/{iid}", headers=role("Viewer"))
    pack = client.get(f"{API}/incidents/{iid}/evidence", headers=role("Viewer"))
    assert "private-value" not in detail.text + pack.text
    url = f"{API}/incidents/{iid}/source-values"
    assert client.post(url, headers=role("PlatformAdmin"), json={"reason": "r"}).status_code == 403
    assert client.post(url, headers=role("DataOperator"), json={}).status_code == 400
    rows = client.post(url, headers=role("DataOperator"), json={"reason": "data owner"}).json()
    assert rows[0]["value"] == long_name and rows[0]["key"] == {"ID": "901"}
    r = client.post(f"{API}/incidents/{iid}/code-fix", headers=role("DataOperator"), json={"reason": "r"})
    assert r.status_code == 409 and r.json()["error"]["code"] == "CODE_FIX_UNAVAILABLE"
    _park_jobs("diagsrc")
