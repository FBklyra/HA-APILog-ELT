"""Redaction: no row values / PII / secrets in the evidence pack or the LLM prompt."""
from __future__ import annotations

import json
import uuid
from typing import Any

from sqlalchemy import insert

from alelt_diagnose import evidence, llm
from alelt_diagnose.models import IncidentKind, Trigger
from alelt_diagnose.redact import redact, scrub
from alelt_diagnose.rules import evaluate

ROW_VALUE = "Jean Tremblay 514-555-0199"
SECRETS = ["hunter2-pass", "sk-ant-api03-ABCDEFGHIJKLMNOPQRSTUV", "eyJhbGciOi.eyJzdWIiOi.c2lnbmF0dXJl", "s3cr3t-token"]
HEX = "4e00" * 200


def _dirty_error() -> str:
    return (f"IntegrityError: duplicate key value violates unique constraint DETAIL: Key (id, name)=(42, {ROW_VALUE}) "
            f"already exists; dsn postgresql://loader:{SECRETS[0]}@db:5432/land api_key={SECRETS[1]} "
            f"Authorization: Bearer {SECRETS[2]} token='{SECRETS[3]}' value '{ROW_VALUE}' by ops@example.com")


def test_scrub_and_redact_unit() -> None:
    out = scrub(_dirty_error())
    for s in [*SECRETS, ROW_VALUE, "ops@example.com"]:
        assert s not in out, s
    assert "duplicate key" in out and "unique constraint" in out           # error text kept
    d = redact({"before": {"NAME": ROW_VALUE, "ID": 7}, "after": [ROW_VALUE], "password": "x", "dest_url": "pg://u:p@h",
                "raw_hex": HEX, "columns": [{"name": "NAME", "engine_type": "VARCHAR"}]})
    assert d["before"] == {"NAME": "<str:26>", "ID": "<int>"} and d["after"] == ["<str:26>"]
    assert d["password"] == "***" and d["dest_url"] == "***" and len(d["raw_hex"]) <= 33
    assert d["columns"] == [{"name": "NAME", "engine_type": "VARCHAR"}]   # structure + types kept


def test_evidence_pack_and_prompt_have_no_values_or_secrets(engine: Any) -> None:
    from alelt_hub.db import agent, audit_event, batch, connection, quarantine_event

    bid = uuid.uuid4()
    with engine.begin() as c:
        c.execute(insert(connection).values(id="c1", engine="fake"))
        c.execute(insert(agent).values(id="a1", connection_id="c1", status="CAPTURING"))
        c.execute(insert(batch).values(id=bid, seq=1, connection_id="c1", agent_id="a1", engine="fake",
                                       from_lri="1:01", to_lri="1:02", rec_count=1, raw_bytes=1, sha256="0" * 64,
                                       codec="none", spool_path="x", error=_dirty_error()))
        c.execute(insert(quarantine_event).values(id=uuid.uuid4(), connection_id="c1", batch_id=bid, table_fqn="S.T",
                                                  lri="1:01", reason=f"row image: BAD_BCD: AMT nibble F '{ROW_VALUE}'",
                                                  raw_hex=HEX))
        c.execute(insert(audit_event).values(id=uuid.uuid4(), actor="agent:a1", action="agent.event.ERROR",
                                             target="a1", detail={"detail": {"message": _dirty_error()}}))
    t = Trigger(kind=IncidentKind.JOB_FAILED, connection_id="c1", batch_id=str(bid), table_fqn="S.T",
                error=_dirty_error(), detail={"traceback": _dirty_error(), "row": {"NAME": ROW_VALUE}})
    p = evidence.collect(engine, t, "strict")
    blob = p.model_dump_json()
    prompt = llm.build_prompt(p, evaluate(p))
    for text in (blob, prompt):
        for s in [*SECRETS, ROW_VALUE, "ops@example.com", HEX[:80]]:
            assert s not in text, s
    assert p.quarantine and p.quarantine[0]["raw_hex_head"].startswith("4e00") and len(p.quarantine[0]["raw_hex_head"]) <= 33
    assert "duplicate key" in blob                                        # diagnosis still possible
    assert json.loads(prompt)["evidence"]["trigger"]["detail"]["row"] == {"NAME": "<str:26>"}
