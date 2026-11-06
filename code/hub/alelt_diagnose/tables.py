"""Diagnosis tables, registered on the hub metadata (`alelt_hub.db.md`) so `init_db()` / `python -m alelt_hub.migrate`
create them. Generic SQLAlchemy types only (string ids, JSON, DateTime) - Postgres and SQLite."""
from __future__ import annotations

from sqlalchemy import JSON, BigInteger, Boolean, Column, Float, Index, Integer, String, Table, Text

from alelt_hub.db import md
from alelt_hub.dialect import UTCDateTime

J = JSON()


def _ts(name: str) -> Column:  # type: ignore[type-arg]
    return Column(name, UTCDateTime())


incident = Table(
    "diag_incident", md,
    Column("id", String(36), primary_key=True),
    Column("signature", String(64), nullable=False),
    Column("kind", String(40), nullable=False),
    Column("severity", String(10), nullable=False),
    Column("status", String(20), nullable=False, server_default="OPEN"),   # OPEN/DIAGNOSED/RESOLVED
    Column("title", Text, nullable=False),
    Column("connection_id", String(100)),
    Column("table_fqn", String(300)),
    Column("batch_id", String(64)),
    Column("job_id", BigInteger),
    Column("agent_id", String(100)),
    Column("position", Text),
    Column("error", Text),                        # redacted
    Column("trigger", J),                         # redacted Trigger
    Column("count", Integer, nullable=False, server_default="1"),
    _ts("first_seen"), _ts("last_seen"),
    Column("rule_id", String(80)),
    Column("root_cause", Text),
    Column("explanation", Text),
    Column("confidence", Float),
    Column("fix_steps", J),
    Column("resolved_by", String(320)),
    _ts("resolved_at"),
    Column("confirmed_cause", Text),
    Column("classification", String(30)),         # PLATFORM_DEFECT/SOURCE_DATA/CONFIGURATION/INFRASTRUCTURE/...
    Column("source_data", J),                     # SourceDataIssue (no values)
    Index("ix_diag_incident_sig", "signature", "status"),
    Index("ix_diag_incident_status", "status", "last_seen"),
)

incident_event = Table(
    "diag_incident_event", md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("incident_id", String(36), nullable=False, index=True),
    _ts("at"),
    Column("kind", String(30), nullable=False),   # OPENED/RECURRED/DIAGNOSED/ACTION_*/RESOLVED
    Column("actor", String(320), nullable=False),
    Column("detail", J),
)

diag_run = Table(
    "diag_run", md,
    Column("id", String(36), primary_key=True),
    Column("incident_id", String(36), index=True),
    Column("ref_kind", String(30), nullable=False),
    Column("ref_id", String(100), nullable=False),
    _ts("at"),
    Column("actor", String(320), nullable=False),
    Column("stage", String(1), nullable=False),   # A = rules, B = LLM
    Column("findings", J),
    Column("provider", String(30)),
    Column("model", String(100)),
    Column("prompt_hash", String(64)),
    Column("answer", J),
    Column("tokens", Integer),
    Column("duration_ms", Integer),
    Column("error", Text),
)

action = Table(
    "diag_action", md,
    Column("id", String(36), primary_key=True),
    Column("incident_id", String(36), nullable=False, index=True),
    Column("kind", String(30), nullable=False),
    Column("params", J),
    Column("description", Text),
    Column("safe", Boolean, nullable=False, server_default="0"),
    Column("rule_id", String(80)),
    Column("status", String(20), nullable=False, server_default="PROPOSED"),  # PROPOSED/EXECUTED/REJECTED/FAILED
    _ts("created_at"),
    Column("decided_by", String(320)),
    _ts("decided_at"),
    Column("reason", Text),
    Column("result", J),
)

known_issue = Table(
    "diag_known_issue", md,
    Column("id", String(36), primary_key=True),
    Column("kind", String(40), nullable=False),
    Column("pattern", Text, nullable=False),      # normalized error text (signature.normalize)
    Column("title", Text, nullable=False),
    Column("root_cause", Text, nullable=False),
    Column("fix_steps", J),
    Column("source_incident", String(36)),
    Column("created_by", String(320), nullable=False),
    _ts("created_at"),
    Column("hits", Integer, nullable=False, server_default="0"),
    Column("category", String(30)),
)

code_fix = Table(
    "diag_code_fix", md,
    Column("id", String(36), primary_key=True),
    Column("incident_id", String(36), nullable=False, index=True),
    Column("status", String(20), nullable=False, server_default="PROPOSED"),  # PROPOSED/REVIEWED/REJECTED/EXPORTED
    Column("summary", Text, nullable=False),
    Column("explanation", Text),
    Column("files", J),
    Column("patch", Text, nullable=False),
    Column("test_path", Text),
    Column("validation", J),                      # {paths_ok, disallowed, apply_check, message, has_test}
    Column("regression_case", J),                 # {path, content} golden corpus case skeleton
    Column("provider", String(30)),
    Column("model", String(100)),
    Column("prompt_hash", String(64)),
    Column("confidence", Float),
    _ts("created_at"),
    Column("created_by", String(320), nullable=False),
    Column("reviewed_by", String(320)),
    _ts("reviewed_at"),
    Column("review_reason", Text),
    _ts("exported_at"),
)

# Raw source values for SOURCE_DATA incidents: stay in the hub database, shown only with incident.reveal (audited).
sensitive_sample = Table(
    "diag_sensitive_sample", md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("incident_id", String(36), nullable=False, index=True),
    Column("column_name", String(128)),
    Column("position", Text),
    Column("key", J),
    Column("value", Text),
    _ts("at"),
)
