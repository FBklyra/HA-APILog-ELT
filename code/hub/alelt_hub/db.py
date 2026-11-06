"""Hub metadata schema (docs/ARCHITECTURE.md + job queue, catalog_version, checkpoint_mirror).

SQLAlchemy 2 Core, portable across PostgreSQL and SQLite (ALELT_DB_URL). Rules for new tables/columns: generic types
only (`UUID`, `J`, `TS`/`UTCDateTime`, `BIGID` for autoincrement keys), defaults via `utc_now()` / `false()` /
`true()`; dialect specifics live in `dialect.py`. `init_db()` is idempotent (metadata.create_all).
"""
from __future__ import annotations

import uuid
from functools import lru_cache

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    Engine,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Sequence,
    String,
    Table,
    Text,
    UniqueConstraint,
    false,
    true,
)
from sqlalchemy.dialects.postgresql import JSONB

from .dialect import UTCDateTime, UUIDType, make_engine, next_seq, utc_now
from .settings import get_settings

md = MetaData()
J = JSON().with_variant(JSONB(), "postgresql")
UUID = UUIDType                               # native uuid on Postgres, CHAR(32) on SQLite
TS = UTCDateTime                              # timestamptz on Postgres, UTC ISO text on SQLite
BIGID = BigInteger().with_variant(Integer(), "sqlite")   # SQLite autoincrement needs INTEGER PRIMARY KEY
Sequence("batch_seq_gen", metadata=md)       # Postgres only (SQLite: next_seq falls back to max+1)


def _id() -> Column[uuid.UUID]:
    return Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)


def _ts(name: str = "created_at") -> Column:  # type: ignore[type-arg]
    return Column(name, TS(), nullable=False, server_default=utc_now())


connection = Table(
    "connection", md,
    Column("id", String(100), primary_key=True),             # protocol connection_id
    Column("engine", String(30), nullable=False),
    Column("db_instance", String(200)),
    Column("db_database", String(200)),
    Column("legacy_connection_id", String(200)),            # id in the legacy pipeline, if any
    _ts(),
)

agent = Table(
    "agent", md,
    Column("id", String(100), primary_key=True),             # protocol agent_id
    Column("connection_id", String(100), ForeignKey("connection.id"), nullable=False),
    Column("cert_thumbprint", String(128)),
    Column("version", String(50)),
    Column("hostname", String(200)),
    Column("status", String(30), nullable=False, server_default="REGISTERED"),
    Column("last_lri", Text),                                  # last read position (text)
    Column("durable_pos", Text),
    Column("spool_bytes", BigInteger),
    Column("metrics", J),
    Column("capabilities", J),
    Column("limits", J),
    Column("last_heartbeat", TS()),
    _ts("registered_at"),
)

source_table = Table(
    "source_table", md, _id(),
    Column("connection_id", String(100), ForeignKey("connection.id"), nullable=False),
    Column("src_schema", String(128), nullable=False),
    Column("src_table", String(128), nullable=False),
    Column("dest_name", String(256)),
    Column("state", String(30), nullable=False, server_default="DISCOVERED"),
    Column("capture_enabled", Boolean, nullable=False, server_default=false()),
    Column("compression", String(30)),
    Column("engine_ids", J),
    UniqueConstraint("connection_id", "src_schema", "src_table"),
)

source_column = Table(
    "source_column", md, _id(),
    Column("table_id", UUID(as_uuid=True), ForeignKey("source_table.id", ondelete="CASCADE"), nullable=False),
    Column("name", String(128), nullable=False),
    Column("engine_type", String(64), nullable=False),
    Column("canonical_type", String(30), nullable=False),
    Column("ordinal", Integer, nullable=False),
    Column("length", Integer),
    Column("scale", Integer),
    Column("nullable", Boolean),
    Column("key_seq", Integer),
    Column("codepage", Integer),
    UniqueConstraint("table_id", "name"),
)

config_revision = Table(
    "config_revision", md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("author", String(200), nullable=False),
    _ts(),
    Column("reason", Text),
    Column("body", J),                                         # the change (table/column overrides)
)

table_config = Table(
    "table_config", md, _id(),
    Column("table_id", UUID(as_uuid=True), ForeignKey("source_table.id", ondelete="CASCADE"), nullable=False, unique=True),
    Column("enabled", Boolean, nullable=False, server_default=true()),
    Column("sync_mode", String(30), nullable=False, server_default="SOFT_DELETE"),
    Column("revision", Integer, ForeignKey("config_revision.id")),
)

column_config = Table(
    "column_config", md, _id(),
    Column("column_id", UUID(as_uuid=True), ForeignKey("source_column.id", ondelete="CASCADE"), nullable=False, unique=True),
    Column("enabled", Boolean, nullable=False, server_default=true()),
    Column("hashed", Boolean, nullable=False, server_default=false()),
    Column("revision", Integer, ForeignKey("config_revision.id")),
)

catalog_version = Table(
    "catalog_version", md,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("connection_id", String(100), ForeignKey("connection.id"), nullable=False),
    Column("valid_from_pos", Text),
    Column("taken_at", TS(), nullable=False),
    Column("sha256", String(64), nullable=False),
    Column("body", J, nullable=False),
    _ts("received_at"),
    UniqueConstraint("connection_id", "sha256"),
)

batch = Table(
    "batch", md,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("seq", BigInteger, default=next_seq("batch_seq_gen", "batch", "seq"), nullable=False, unique=True),   # global receive order
    Column("connection_id", String(100), ForeignKey("connection.id"), nullable=False),
    Column("agent_id", String(100), nullable=False),
    Column("engine", String(30), nullable=False),
    Column("from_lri", Text, nullable=False),
    Column("to_lri", Text, nullable=False),
    Column("next_lri", Text),
    Column("rec_count", Integer, nullable=False),
    Column("raw_bytes", BigInteger, nullable=False),
    Column("sha256", String(64), nullable=False),
    Column("codec", String(10), nullable=False),
    Column("spool_path", Text, nullable=False),
    Column("catalog_version_id", Integer, ForeignKey("catalog_version.id")),
    Column("decoded_catalog_vid", Integer),     # XC-22: version the first decode used; replays reuse it
    Column("decoded_policy_rev", String(32)),   # XC-30: source policy revision of the first decode ("" = none)
    Column("status", String(20), nullable=False, server_default="RECEIVED"),  # RECEIVED/DECODED/LOADED/FAILED
    Column("events", Integer),
    Column("error", Text),
    _ts("received_at"),
    Column("updated_at", TS()),
    Index("ix_batch_conn_seq", "connection_id", "seq"),
)

batch_table_stat = Table(
    "batch_table_stat", md,
    Column("batch_id", String(64), primary_key=True),          # loader flush id (covers >=1 source batch)
    Column("table_fqn", String(300), primary_key=True),
    Column("table_id", UUID(as_uuid=True), ForeignKey("source_table.id")),
    Column("inserts", Integer, nullable=False, server_default="0"),
    Column("updates", Integer, nullable=False, server_default="0"),
    Column("deletes", Integer, nullable=False, server_default="0"),
    Column("duration_ms", Integer),
    Column("commit_ts_min", TS()),      # F-19: oldest / newest source commit applied by this flush for the table;
    Column("commit_ts_max", TS()),      # created_at - commit_ts_min = worst commit -> apply latency of the flush
    _ts(),
)

checkpoint_mirror = Table(
    "checkpoint_mirror", md,
    Column("stream", String(200), primary_key=True),
    Column("connection_id", String(100), nullable=False),
    Column("position", Text),
    Column("last_seq", BigInteger, nullable=False, server_default="0"),   # last batch seq applied
    Column("restart_seq", BigInteger, nullable=False, server_default="0"),  # replay low-water mark
    Column("open_txns", Integer, nullable=False, server_default="0"),
    Column("oldest_open_s", Integer, nullable=False, server_default="0"),
    Column("last_commit_ts", TS()),                     # capture lag basis
    Column("updated_at", TS(), server_default=utc_now()),
    # position the destination checkpoint holds (RC4 empty-flush rule: the mirror may run ahead of it when flushes had
    # nothing to apply; a checkpoint-only apply catches it up every ALELT_CHECKPOINT_IDLE_S and at shutdown)
    Column("dest_position", Text),
)

schema_change_event = Table(
    "schema_change_event", md, _id(),
    Column("table_id", UUID(as_uuid=True), ForeignKey("source_table.id")),
    Column("kind", String(50), nullable=False),
    Column("status", String(30), nullable=False, server_default="PENDING"),  # PENDING/APPROVED/REJECTED/APPLIED
    Column("detail", J),
    Column("position", Text),
    Column("policy", String(30), nullable=False, server_default="REQUIRE_APPROVAL"),
    _ts(),
)

recon_run = Table(
    "recon_run", md, _id(),
    Column("check_kind", String(50), nullable=False),
    Column("started_at", TS(), server_default=utc_now()),
    Column("finished_at", TS()),
)

recon_result = Table(
    "recon_result", md, _id(),
    Column("run_id", UUID(as_uuid=True), ForeignKey("recon_run.id")),
    Column("table_id", UUID(as_uuid=True), ForeignKey("source_table.id")),
    Column("outcome", String(20), nullable=False),
    Column("detail", Text),
    Column("drill", J),
    _ts(),
)

quarantine_event = Table(
    "quarantine_event", md, _id(),
    Column("connection_id", String(100)),
    Column("batch_id", UUID(as_uuid=True)),
    Column("table_id", UUID(as_uuid=True), ForeignKey("source_table.id")),
    Column("table_fqn", String(300)),
    Column("lri", Text),
    Column("reason", Text, nullable=False),
    Column("raw_ref", J),
    Column("raw_hex", Text),
    Column("status", String(20), nullable=False, server_default="OPEN"),   # OPEN/RESOLVED/IGNORED
    _ts(),
)

switch_state = Table(
    "switch_state", md,
    Column("table_id", UUID(as_uuid=True), ForeignKey("source_table.id"), primary_key=True),
    Column("active_source", String(20), nullable=False, server_default="LEGACY"),
    Column("switched_at", TS()),
)

app_user = Table(
    "app_user", md, _id(),
    Column("entra_oid", String(100), unique=True),
    Column("email", String(320)),
    _ts(),
)

role_assignment = Table(
    "role_assignment", md,
    Column("user_id", UUID(as_uuid=True), ForeignKey("app_user.id"), primary_key=True),
    Column("role", String(30), primary_key=True),
)

signoff = Table(
    "signoff", md, _id(),
    Column("table_id", UUID(as_uuid=True), ForeignKey("source_table.id")),
    Column("user_id", UUID(as_uuid=True), ForeignKey("app_user.id")),
    Column("criterion", String(50), nullable=False),
    Column("signed_at", TS(), server_default=utc_now()),
    Column("user_name", String(320)),                            # signer identity (console principal)
    Column("comment", Text),
)

audit_event = Table(
    "audit_event", md, _id(),
    Column("actor", String(320), nullable=False),
    Column("role", String(30)),
    Column("action", String(100), nullable=False),
    Column("target", String(300)),
    Column("detail", J),
    _ts("at"),
)

agent_command = Table(
    "agent_command", md,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("agent_id", String(100), ForeignKey("agent.id"), nullable=False),
    Column("name", String(30), nullable=False),
    Column("args", J),
    Column("issued_by", String(320), nullable=False),
    Column("status", String(20), nullable=False, server_default="PENDING"),  # PENDING/SENT/ACKED/FAILED
    Column("result", J),
    _ts("issued_at"),
    Column("acked_at", TS()),
    Column("sent_at", TS()),                  # last delivery (HC-12: unacked SENT commands are re-delivered)
    Column("deliveries", Integer),
    Index("ix_cmd_agent_status", "agent_id", "status"),
)

snapshot_chunk = Table(
    "snapshot_chunk", md,
    Column("snapshot_id", UUID(as_uuid=True), primary_key=True),
    Column("chunk_no", Integer, primary_key=True),
    Column("connection_id", String(100), nullable=False),
    Column("table_fqn", String(300), nullable=False),
    Column("rows", Integer, nullable=False),
    Column("last", Boolean, nullable=False),
    Column("header", J, nullable=False),
    Column("spool_path", Text, nullable=False),
    Column("status", String(20), nullable=False, server_default="RECEIVED"),
    _ts("received_at"),
)

# per-table apply holds shared by every worker (alelt_hub/holds.py): snapshot jobs (alelt_snapshot SnapshotJob, X-22) and
# schedule holds (tables.yaml `schedule`, X-110). While held, the main stream skips the table; release = catch-up.
table_hold = Table(
    "table_hold", md,
    Column("connection_id", String(100), primary_key=True),
    Column("table_fqn", String(300), primary_key=True),
    Column("kind", String(20), primary_key=True),                # snapshot | schedule
    Column("state", String(30), nullable=False),
    Column("held", Boolean, nullable=False, server_default=false()),
    Column("start_pos", Text),
    Column("body", J, nullable=False),
    Column("updated_at", TS(), server_default=utc_now()),
)

job = Table(
    "job", md,
    Column("id", BIGID, primary_key=True, autoincrement=True),
    Column("kind", String(30), nullable=False),              # decode / snapshot_load
    Column("payload", J, nullable=False),
    Column("dedup_key", String(200), unique=True),
    Column("status", String(20), nullable=False, server_default="QUEUED"),   # QUEUED/RUNNING/DONE/FAILED
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("run_after", TS(), nullable=False, server_default=utc_now()),
    Column("locked_by", String(100)),
    Column("locked_at", TS()),
    Column("error", Text),
    _ts(),
    Column("finished_at", TS()),
    Column("priority", Integer, nullable=False, server_default="100"),   # lower first (alelt_hub.jobs.PRIORITY)
    Index("ix_job_ready", "status", "run_after"),
    Index("ix_job_claim", "status", "priority", "run_after"),
)

# ---- out-of-row completion (alelt_hub/completion.py): one entry per (table, key) whose out-of-row columns must be
# fetched by key and written through the main stream. Deleted when done.
table_completion = Table(
    "table_completion", md,
    Column("connection_id", String(100), primary_key=True),
    Column("table_fqn", String(300), primary_key=True),
    Column("key_id", String(32), primary_key=True),             # sha256[:32] of the canonical key JSON
    Column("key", J, nullable=False),                            # tagged JSON (canonical_jsonl.encode_value)
    Column("columns", J),                                        # union of the out-of-row columns signalled
    Column("first_pos", Text),                                   # commit position of the first / newest signal
    Column("last_pos", Text),
    Column("first_seq", BigInteger),                             # batch seq of the first signal (spool floor)
    Column("status", String(20), nullable=False, server_default="PENDING"),   # PENDING/INFLIGHT/CANCELLED
    Column("command_id", String(64)),
    Column("read_pos", Text),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("next_attempt_at", TS(), nullable=False, server_default=utc_now()),
    Column("last_error", Text),
    _ts(),
    Column("updated_at", TS(), server_default=utc_now()),
    Index("ix_completion_due", "connection_id", "status", "next_attempt_at"),
    Index("ix_completion_table", "connection_id", "table_fqn"),
)

# ---- table lifecycle (alelt_hub/lifecycle.py): one row per enabled table, the single owner of its phase
table_lifecycle = Table(
    "table_lifecycle", md,
    Column("connection_id", String(100), primary_key=True),
    Column("table_fqn", String(300), primary_key=True),
    Column("phase", String(20), nullable=False, server_default="NEW"),  # NEW/INITIAL_LOAD/CATCH_UP/STREAMING/RESYNC/DISABLED
    Column("since", TS(), server_default=utc_now()),
    Column("mode", String(200), nullable=False, server_default="auto"),   # auto | none | replay_from:<pos> | legacy
    Column("start_pos", Text),
    Column("snapshot_id", String(36)),
    Column("verified_at", TS()),
    Column("resync_count", Integer, nullable=False, server_default="0"),
    Column("body", J),
    Column("updated_at", TS(), server_default=utc_now()),
)

# ---- cooperative yield (alelt_hub/worker.py): a control step that needs `pipeline:<cid>` asks the decode slice to end
pipeline_request = Table(
    "pipeline_request", md,
    Column("connection_id", String(100), primary_key=True),
    Column("requested_by", String(100)),
    Column("reason", String(200)),
    Column("requested_at", TS(), server_default=utc_now()),
)


wave = Table(
    "wave", md, _id(),
    Column("name", String(200), nullable=False, unique=True),
    Column("table_ids", J, nullable=False),
    Column("planned_at", TS()),
    Column("status", String(30), nullable=False, server_default="PLANNED"),
    Column("created_by", String(320), nullable=False),
    _ts(),
)

switch_request = Table(
    "switch_request", md, _id(),
    Column("scope", J, nullable=False),                          # {"table_id": ...} | {"wave_id": ...}
    Column("scope_label", String(300), nullable=False),
    Column("target", String(20), nullable=False),                # LEGACY | ALELT
    Column("requested_by", String(320), nullable=False),
    Column("requested_at", TS(), nullable=False, server_default=utc_now()),
    Column("reason", Text, nullable=False),
    Column("status", String(30), nullable=False, server_default="PENDING_APPROVAL"),
    Column("approved_by", String(320)),
    Column("executed_at", TS()),
    Column("expires_at", TS(), nullable=False),
)

import_run = Table(
    "import_run", md, _id(),
    Column("started_at", TS(), nullable=False, server_default=utc_now()),
    Column("finished_at", TS()),
    Column("status", String(20), nullable=False, server_default="RUNNING"),   # RUNNING/DONE/FAILED/ACCEPTED
    Column("source_ref", String(300), nullable=False, server_default=""),
    Column("diff", J),
    Column("body", J),                                           # proposed config (tables.yaml document)
    Column("message", Text),
    Column("started_by", String(320), nullable=False),
)

# ---- migration importer + pipeline wizard (hub/alelt_hub/importer.py); secrets are never stored, only references
import_session = Table(
    "import_session", md, _id(),
    Column("plugin", String(100), nullable=False),
    Column("platform", String(200), nullable=False, server_default=""),
    Column("mode", String(20), nullable=False, server_default="live"),          # live | files
    Column("status", String(30), nullable=False, server_default="DISCOVERED"),  # DISCOVERED/FAILED/PENDING_APPROVAL/APPLIED
    Column("started_by", String(320), nullable=False),
    Column("started_at", TS(), nullable=False, server_default=utc_now()),
    Column("finished_at", TS()),
    Column("files", J),                                          # [{name, kind}] of an offline import
    Column("inventory", J),                                      # InventoryModel (secrets masked)
    Column("plan", J),                                           # generated MigrationPlan
    Column("edits", J),                                          # PlanEdits from the review tree
    Column("golden", J),                                         # golden spec from dropped DDL / columns CSV
    Column("message", Text),
    Column("requested_by", String(320)),                         # two-person rule (apply that changes config in force)
    Column("approved_by", String(320)),
    Column("applied_revision", Integer),
    Column("result", J),
)

pipeline_definition = Table(
    "pipeline_definition", md,
    Column("connection_id", String(100), primary_key=True),
    Column("origin", String(100), nullable=False),               # import:<session> | wizard
    Column("environment", String(10), nullable=False, server_default="TEST"),  # TEST | PROD
    Column("legacy_ref", String(300)),
    Column("source_engine", String(30), nullable=False),
    Column("destination_engine", String(30), nullable=False),
    Column("source", J),                                         # non-secret settings + secret references
    Column("destination", J),
    Column("tables_doc", J, nullable=False),                     # config/tables.yaml document
    Column("latency_target_s", Integer, nullable=False, server_default="120"),
    Column("schedule", J),
    Column("status", String(30), nullable=False, server_default="DRAFT"),     # DRAFT/NEEDS_SECRETS/READY/SHADOW
    Column("active_source", String(20), nullable=False, server_default="LEGACY"),
    Column("revision", Integer),
    Column("created_by", String(320), nullable=False),
    _ts(),
    Column("updated_at", TS(), server_default=utc_now()),
)

pipeline_trigger = Table(
    "pipeline_trigger", md, _id(),
    Column("kind", String(20), nullable=False),                  # webhook | dbt_job
    Column("name", String(200), nullable=False),
    Column("target", Text),
    Column("events", J),
    Column("after_connections", J),
    Column("settings", J),
    Column("enabled", Boolean, nullable=False, server_default=true()),
    Column("legacy_ref", String(300)),
    Column("revision", Integer),
    _ts(),
    UniqueConstraint("kind", "name"),
)

golden_capture = Table(
    "golden_capture", md, _id(),
    Column("connection_id", String(100), nullable=False),
    Column("schema_name", String(200)),
    Column("origin", String(20), nullable=False),                # files | destination
    Column("status", String(20), nullable=False),                # OK | FAILED | EMPTY
    Column("body", J),                                           # {tables, ddl, columns_csv, columns}
    Column("message", Text),
    _ts("captured_at"),
)

hub_setting = Table(
    "hub_setting", md,
    Column("key", String(100), primary_key=True),
    Column("value", J, nullable=False),
    Column("updated_at", TS(), server_default=utc_now()),
)

group_mapping = Table(
    "group_mapping", md,
    Column("group_id", String(100), primary_key=True),           # Entra ID group object id
    Column("group_name", String(200), nullable=False),
    Column("role", String(30), nullable=False),
)

# ---- one config source of truth (F-18, ADR-008): every config change is a `config_revision` (a change set); the
# full tables.yaml document of each connection touched by it is stored here. Active config of a connection = the
# config_doc with the highest revision_id. YAML in Git is import/export only (alelt_hub/config_store.py).
config_doc = Table(
    "config_doc", md,
    Column("revision_id", Integer, ForeignKey("config_revision.id"), primary_key=True),
    Column("connection_id", String(100), primary_key=True),
    Column("doc", J, nullable=False),
    Column("sha256", String(64), nullable=False),
    Index("ix_config_doc_conn", "connection_id", "revision_id"),
)

# ---- versioned, forward-only metadata migrations (F-24, alelt_hub/migrations.py)
schema_migration = Table(
    "schema_migration", md,
    Column("version", Integer, primary_key=True),
    Column("name", String(200), nullable=False),
    Column("app_version", String(50), nullable=False),
    _ts("applied_at"),
)


@lru_cache(maxsize=4)
def get_engine(url: str | None = None) -> Engine:
    return make_engine(url or get_settings().db_url)


def create_tables(engine: Engine) -> None:
    """CREATE every missing table (idempotent). On PostgreSQL serialized by an advisory lock, so N replicas
    starting together on an empty database do not race on CREATE (HA)."""
    if engine.dialect.name != "postgresql":
        md.create_all(engine)
        return
    from sqlalchemy import text

    with engine.begin() as c:
        c.execute(text("SELECT pg_advisory_xact_lock(724631001)"))     # fixed key: hub schema DDL
        md.create_all(c)


def init_db(engine: Engine | None = None) -> None:
    """Bring the metadata schema to the version of this code: forward-only versioned migrations (F-24,
    `alelt_hub/migrations.py`). Refuses a database written by a newer release. With ALELT_AUTO_MIGRATE=0 nothing is
    changed and pending migrations raise (run `alelt migrate` first - HA / production upgrade order)."""
    from .migrations import upgrade

    upgrade(engine or get_engine())


def drop_db(engine: Engine | None = None) -> None:
    """Drop every hub table (tests only)."""
    md.drop_all(engine or get_engine())
