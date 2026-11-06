"""Stage A: each signature rule against a fixture incident (top finding = expected root cause)."""
from __future__ import annotations

from typing import Any

import pytest

from alelt_diagnose.models import IncidentKind as K
from alelt_diagnose.rules import RULES, KnownIssue, evaluate
from alelt_diagnose.signature import normalize, signature

from .conftest import pack

Q = K.QUARANTINE


def q(reason: str, pos: str = "1:000000000000000A") -> dict[str, Any]:
    return {"id": "q1", "reason": reason, "position": pos, "table": "S.T", "raw_hex_head": "0011"}


CASES: list[tuple[str, dict[str, Any], str]] = [
    ("codepage_mismatch", dict(kind=Q, error="row image: BAD_CODEPAGE: NAME codepage 37",
                               quarantine=[q("row image: BAD_CODEPAGE: NAME codepage 37")],
                               trigger={"table_fqn": "S.T", "batch_id": "b1"}), "REDECODE_RANGE"),
    ("catalog_drift", dict(kind=Q, error="table tbspaceid=2 tableid=7 not in catalog",
                           catalog={"versions": [{"id": 1}, {"id": 2}],
                                    "table_layouts": {"1": {"columns": [{"name": "A"}]},
                                                      "2": {"columns": [{"name": "A"}, {"name": "B"}]}}},
                           schema_changes=[{"kind": "ADD_COLUMN", "position": "1:00000000000000FF"}],
                           trigger={"table_fqn": "S.T", "position": "1:000000000000000A"}), "REDECODE_RANGE"),
    ("decoder_defect", dict(kind=Q, error="row image: BAD_BCD: AMT sign nibble 7",
                            quarantine=[q("row image: BAD_BCD: AMT sign nibble 7")],
                            trigger={"table_fqn": "S.T", "batch_id": "b1"}), "REDECODE_RANGE"),
    ("unsupported_type", dict(kind=Q, error="row image: UNSUPPORTED_TYPE: PRICE DECFLOAT"), ""),
    ("value_compression", dict(kind=Q, error="value compression (V) row format not supported"), ""),
    ("out_of_row_lob", dict(kind=Q, error="partial image lob_unavailable:NOTES", trigger={"table_fqn": "S.T"}),
     "RESYNC_TABLE"),
    ("spool_corrupt", dict(error="PipelineError: spool corrupt for batch 1b2c", trigger={"job_id": 7}), "RETRY_JOB"),
    ("archive_log_unavailable", dict(kind=K.AGENT_ERROR, error="db2ReadLog SQL1273N log file not found",
                                     trigger={"table_fqn": "S.T"}), "RESYNC_TABLE"),
    ("capture_disabled", dict(kind=K.CAPTURE_LOST, error="data capture changes disabled on an in-scope table",
                              schema_changes=[{"kind": "NEW_TABLE", "at": "x"}], trigger={"table_fqn": "S.T"}),
     "RESYNC_TABLE"),
    ("agent_auth_rejected", dict(kind=K.AGENT_AUTH, error="agent request rejected: client certificate not allow-listed"),
     ""),
    ("db2_auth_expired", dict(kind=K.AGENT_ERROR, error="SQL30082N Security processing failed with reason 1"), ""),
    ("warehouse_suspended", dict(kind=K.DESTINATION_ERROR,
                                 error="ProgrammingError: 000606 (57P03): No active warehouse selected"), ""),
    ("permission_denied", dict(kind=K.DESTINATION_ERROR, error="InsufficientPrivilege: permission denied for schema land"),
     ""),
    ("destination_auth", dict(kind=K.DESTINATION_ERROR,
                              error="OperationalError: FATAL: password authentication failed for user loader"), ""),
    ("network_tls", dict(kind=K.DESTINATION_ERROR, error="OperationalError: connection refused (port 5999)",
                         trigger={"job_id": 3}), "RETRY_JOB"),
    ("merge_failure", dict(kind=K.DESTINATION_ERROR, error="DataError: numeric field overflow", trigger={"job_id": 4}),
     "RETRY_JOB"),
    ("spool_full_backpressure", dict(kind=K.GOVERNOR_PAUSE, error="agent paused_by_governor",
                                     agent={"id": "a1", "limits": {"cpu_pct": 10}}), "RESUME_AGENT"),
    ("position_gap", dict(kind=K.POSITION_GAP, error="position gap/overlap: batch from_pos does not follow",
                          trigger={"detail": {"expected_from": "1:0A", "got": "1:0F"}}), ""),
    ("position_mismatch", dict(kind=K.LAG_ABOVE_TARGET, error="capture lag above target",
                               positions={"destination_vs_mirror": -1, "destination_checkpoint": "1:01",
                                          "mirror_position": "1:09"}), "REDECODE_RANGE"),
    ("clock_skew", dict(kind=K.AGENT_ERROR, error="agent event X",
                        agent_events=[{"type": "X", "clock_skew_s": 900}]), ""),
    ("agent_offline", dict(kind=K.AGENT_OFFLINE, error="agent heartbeat missing", agent={"heartbeat_age_s": 900}), ""),
    ("recon_mismatch", dict(kind=K.RECON_MISMATCH, error="reconciliation mismatch"), ""),
    ("completion_stalled", dict(kind=K.COMPLETION_STALLED, error="out-of-row completion of S.T unanswered"), ""),
    ("completion_oversize", dict(kind=K.COMPLETION_OVERSIZE, error="one row exceeds the edge result cap"),
     ""),                                      # no table on the fixture trigger: no RESYNC action
]


@pytest.mark.parametrize("rule_id,kw,action", CASES, ids=[c[0] for c in CASES])
def test_rule_top_finding(rule_id: str, kw: dict[str, Any], action: str) -> None:
    kw = dict(kw)
    kind = kw.pop("kind", K.JOB_FAILED)
    error = kw.pop("error")
    f = evaluate(pack(kind, error, **kw))[0]
    assert f.rule_id == rule_id, [x.rule_id for x in evaluate(pack(kind, error, **kw))]
    assert f.root_cause and f.fix_steps and f.evidence_refs and 0 < f.confidence <= 1
    assert (f.action.kind if f.action else "") == action


CATEGORY = {"codepage_mismatch": "PLATFORM_DEFECT", "decoder_defect": "PLATFORM_DEFECT", "spool_corrupt": "INFRASTRUCTURE",
            "capture_disabled": "CONFIGURATION", "warehouse_suspended": "DESTINATION", "network_tls": "INFRASTRUCTURE",
            "catalog_drift": "PLATFORM_DEFECT", "recon_mismatch": "UNKNOWN"}


@pytest.mark.parametrize("rule_id,kw,action", [c for c in CASES if c[0] in CATEGORY], ids=lambda x: str(x)[:20])
def test_classification(rule_id: str, kw: dict[str, Any], action: str) -> None:
    kw = dict(kw)
    f = evaluate(pack(kw.pop("kind", K.JOB_FAILED), kw.pop("error"), **kw))[0]
    assert f.category.value == CATEGORY[rule_id]


def test_every_rule_has_a_fixture() -> None:
    covered = {c[0] for c in CASES}
    assert {rid for rid, _ in RULES} - covered <= {"lag_above_target"}   # lag: covered in test_lag_rule


def test_lag_rule() -> None:
    f = evaluate(pack(K.LAG_ABOVE_TARGET, "capture lag above target", metrics={"jobs_failed": 2}))[0]
    assert f.rule_id == "lag_above_target" and "failing jobs" in f.root_cause


def test_unclassified_and_known_issue() -> None:
    p = pack(K.JOB_FAILED, "WeirdError: flux capacitor 88 overloaded")
    assert evaluate(p)[0].rule_id == "unclassified"
    k = KnownIssue("k1", "JOB_FAILED", normalize("WeirdError: flux capacitor 12 overloaded"), "flux", "capacitor", ["x"])
    top = evaluate(p, [k])[0]
    assert top.rule_id == "known:k1" and top.confidence >= 0.95


def test_signature_ignores_volatile_tokens() -> None:
    a = signature("JOB_FAILED", "c", "S.T", "spool corrupt for batch 0b6a4f6e-1111-2222-3333-444455556666 at 1:000000000000000A")
    b = signature("JOB_FAILED", "c", "S.T", "spool corrupt for batch 9c6a4f6e-1111-2222-3333-444455556666 at 1:00000000000000FF")
    assert a == b != signature("JOB_FAILED", "c", "S.U", "spool corrupt for batch x")
