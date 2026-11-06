"""Canonical wire and pipeline models (DESIGN.md §4). Every module speaks only these types.

Rules:
- Positions are opaque to the core; only the source connector parses/compares them (Position.text is canonical form).
- Values in ChangeEvent.before/after are already canonical Python values (see CanonicalType).
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=False)


# ---------------------------------------------------------------- positions
class Position(_M):
    """Opaque, ordered source position. Db2: type-1 LRI text '<part1>:<part2 hex16>'."""

    engine: str
    text: str

    def __str__(self) -> str:  # pragma: no cover - convenience
        return self.text


# ---------------------------------------------------------------- canonical types
class CanonicalType(str, Enum):
    BOOLEAN = "BOOLEAN"
    SHORT = "SHORT"
    INT = "INT"
    LONG = "LONG"
    DECIMAL = "DECIMAL"          # precision/scale on ColumnInfo
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    LOCALDATE = "LOCALDATE"
    LOCALTIME = "LOCALTIME"
    LOCALDATETIME = "LOCALDATETIME"
    INSTANT = "INSTANT"
    STRING = "STRING"
    BINARY = "BINARY"
    JSON = "JSON"
    XML = "XML"
    UNKNOWN = "UNKNOWN"          # => quarantine (R7)


# ---------------------------------------------------------------- catalog
class TableRef(_M):
    schema_: str = Field(alias="schema")
    name: str
    model_config = ConfigDict(extra="forbid", populate_by_name=True, frozen=True)

    @property
    def fqn(self) -> str:
        return f"{self.schema_}.{self.name}"


class ColumnInfo(_M):
    name: str
    ordinal: int                      # 0-based, engine order (Db2 COLNO)
    engine_type: str                  # e.g. 'VARCHAR', 'DECIMAL', 'TIMESTAMP'
    canonical_type: CanonicalType
    length: int | None = None
    scale: int | None = None
    nullable: bool = True
    key_seq: int | None = None        # 1..n if part of primary key
    codepage: int | None = None       # character columns; 0 = binary (FOR BIT DATA)
    default: str | None = None


class TableInfo(_M):
    table: TableRef
    engine_ids: dict[str, int] = Field(default_factory=dict)   # Db2: {'tbspaceid':2,'tableid':5}
    capture_enabled: bool = False
    compression: str | None = None
    columns: list[ColumnInfo] = Field(default_factory=list)

    def key_columns(self) -> list[ColumnInfo]:
        return sorted((c for c in self.columns if c.key_seq), key=lambda c: c.key_seq or 0)


class CatalogSnapshot(_M):
    connection_id: str
    engine: str
    taken_at: datetime
    position: Position | None = None          # position at which the snapshot is valid
    database_codepage: int | None = None
    tables: list[TableInfo]


# ---------------------------------------------------------------- change events
class ChangeKind(str, Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    TRUNCATE = "TRUNCATE"
    COMMIT = "COMMIT"
    ROLLBACK = "ROLLBACK"
    COMPENSATION = "COMPENSATION"   # undo record inside a transaction (Db2 CLR)
    DDL = "DDL"
    OTHER = "OTHER"


class RawRef(_M):
    batch_id: UUID
    offset: int
    length: int


class ChangeEvent(_M):
    position: Position
    txid: str
    kind: ChangeKind
    table: TableRef | None = None
    before: dict[str, Any] | None = None
    after: dict[str, Any] | None = None
    commit_ts: datetime | None = None
    raw_ref: RawRef | None = None
    note: str | None = None             # decoder remark (e.g. 'partial image')


class ChangeSet(_M):
    txid: str
    commit_position: Position
    commit_ts: datetime | None
    events: list[ChangeEvent]           # only INSERT/UPDATE/DELETE/TRUNCATE, position order


class QuarantineRecord(_M):
    position: Position | None
    table: TableRef | None
    reason: str
    raw_ref: RawRef | None
    raw_hex: str | None = None          # first bytes, for diagnosis


# ---------------------------------------------------------------- edge <-> hub wire (protocol v1)
class BatchHeader(_M):
    """Sent as JSON in HTTP header X-ALELT-Batch; body = codec(raw engine records)."""

    batch_id: UUID
    connection_id: str
    agent_id: str
    engine: str
    from_pos: Position
    to_pos: Position
    next_pos: Position | None = None     # where the next read must start
    rec_count: int
    raw_bytes: int
    sha256: str                          # hex sha256 of the UNCOMPRESSED body
    codec: Literal["zstd", "zlib", "none"] = "zstd"
    protocol: int = 1


class BatchAck(_M):
    batch_id: UUID
    durable_pos: Position
    duplicate: bool = False


class RegisterRequest(_M):
    agent_id: str
    connection_id: str
    engine: str
    version: str
    hostname: str
    database: str
    capabilities: list[str] = Field(default_factory=list)


class RegisterResponse(_M):
    accepted: bool
    resume_from: Position | None = None
    limits: Limits | None = None
    # per-agent credential (`alelta_...`), returned once when an enrolment-token agent declares capability
    # "credential.v1"; the agent stores it (DPAPI / secret store) and uses it instead of the enrolment token
    credential: str | None = None
    credential_expires_at: datetime | None = None


class Limits(_M):
    cpu_pct: int = 100                # % of ONE core
    mem_mb: int = 512
    batch_kb: int = 1024
    max_rate_kbps: int = 0            # 0 = unlimited
    poll_ms: int = 1000


class CommandName(str, Enum):
    START_CAPTURE = "START_CAPTURE"
    PAUSE = "PAUSE"
    RESUME = "RESUME"
    SET_LIMITS = "SET_LIMITS"
    SNAPSHOT_CATALOG = "SNAPSHOT_CATALOG"
    SNAPSHOT_TABLE = "SNAPSHOT_TABLE"
    PROBE = "PROBE"
    SHUTDOWN = "SHUTDOWN"
    VERIFY_TABLE = "VERIFY_TABLE"


PROBES = ("current_position", "db_health", "log_config")   # whitelisted PROBE names


class Command(_M):
    """Whitelisted hub -> edge command (R5). Unknown names fail validation."""

    command_id: UUID
    name: CommandName
    args: dict[str, Any] = Field(default_factory=dict)
    issued_at: datetime

    @model_validator(mode="after")
    def _typed_args(self) -> "Command":
        if self.name is CommandName.VERIFY_TABLE:
            VerifyTableArgs.model_validate(self.args)      # same rules as the agents (R5)
        return self


class Heartbeat(_M):
    agent_id: str
    state: str                         # IDLE/CAPTURING/PAUSED/PAUSED_BY_GOVERNOR/ERROR
    last_read_pos: Position | None = None
    durable_pos: Position | None = None
    spool_bytes: int = 0
    cpu_pct: float = 0.0
    mem_mb: float = 0.0
    batches_sent: int = 0
    errors: int = 0
    metrics: dict[str, float] = Field(default_factory=dict)   # REQ-11: extra X-16 names (alelt_agent_*), optional
    at: datetime


class AgentEvent(_M):
    """Agent-originated event that is not a command result (POST /agent/v1/events), e.g.
    PAUSED_BY_GOVERNOR, GOVERNOR_RESUMED, COMMAND_REJECTED (command id unreadable), SPOOL_FULL.
    Command results always go to POST /agent/v1/commands/{command_id}/ack."""

    agent_id: str
    type: str = Field(pattern=r"^[A-Z][A-Z0-9_]{0,63}$")
    command_id: UUID | None = None
    detail: dict[str, Any] = Field(default_factory=dict)
    at: datetime


class SnapshotChunkHeader(_M):
    snapshot_id: UUID
    table: TableRef
    chunk_no: int
    key_from: list[Any] | None = None
    key_to: list[Any] | None = None
    last_key: list[Any] | None = None      # key of the chunk's last row: a snapshot resumes after it (key_from)
    rows: int
    last: bool = False
    start_pos: Position | None = None
    codec: Literal["zstd", "zlib", "none"] = "zlib"
    format: Literal["jsonl", "canonical_jsonl", "arrow"] = "jsonl"   # canonical_jsonl: typed values (REQ-23)


# ---------------------------------------------------------------- VERIFY_TABLE (F-03): edge-side verification reads
# Identical contract for every edge (C agent aleltagent.exe + plugins, aleltd, dev edge). The edge runs only compiled-in,
# parameterized SELECTs (Db2: WITH CS, currently committed) on tables of its local allow-list; never SQL text.
VERIFY_CHECKS = ("row_count", "aggregates", "keys", "rows")
VERIFY_MAX_RANGES = 256          # key ranges per command
VERIFY_MAX_KEYS = 10_000         # keys returned per range ("keys")
VERIFY_MAX_KEY_VALUES = 1_000    # rows re-read by key per command ("rows")
VERIFY_MAX_COLUMNS = 1012        # Db2 LUW column limit
VERIFY_MAX_TIMEOUT_S = 600
VERIFY_MAX_ARGS_BYTES = 60 * 1024  # C agent: whole command <= 64 KiB
# Aggregate classes (per column, chosen by the edge from the source column type; the hub mirrors them):
#   int / dec: count, min, max, exact sum   flt / tmp: count, min, max   str: count, sum of RTRIM length (code points)
#   cnt: count only (LOB, XML, bit data, other)
VERIFY_AGG_CLASSES = ("int", "dec", "flt", "tmp", "str", "cnt")
_IDENT_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_$#@")
KeyScalar = str | int | float


def verify_ident_ok(s: str) -> bool:
    """Identifier rule of the agents (alelt_ident_valid): [A-Za-z0-9_$#@], 1..128 characters."""
    return 0 < len(s) < 129 and set(s) <= _IDENT_CHARS


class VerifyKeyRange(_M):
    """Primary-key range: `after` exclusive lower bound, `to` inclusive upper bound (full key each, None = open)."""

    after: list[KeyScalar] | None = Field(default=None, min_length=1, max_length=16)
    to: list[KeyScalar] | None = Field(default=None, min_length=1, max_length=16)


class VerifyTableArgs(_M):
    table: TableRef
    verify_id: UUID
    checks: list[Literal["row_count", "aggregates", "keys", "rows"]] = Field(min_length=1, max_length=4)
    columns: list[str] | None = Field(default=None, max_length=VERIFY_MAX_COLUMNS)   # None = every column
    ranges: list[VerifyKeyRange] | None = Field(default=None, max_length=VERIFY_MAX_RANGES)  # None = whole table
    key_values: list[list[KeyScalar]] | None = Field(default=None, max_length=VERIFY_MAX_KEY_VALUES)
    max_keys: int = Field(default=VERIFY_MAX_KEYS, ge=1, le=VERIFY_MAX_KEYS)
    timeout_s: int = Field(default=300, ge=1, le=VERIFY_MAX_TIMEOUT_S)

    @field_validator("checks")
    @classmethod
    def _uniq(cls, v: list[str]) -> list[str]:
        if len(set(v)) != len(v):
            raise ValueError("checks must be unique")
        return v

    @field_validator("columns")
    @classmethod
    def _cols(cls, v: list[str] | None) -> list[str] | None:
        if v is not None and (not v or not all(verify_ident_ok(c) for c in v)):
            raise ValueError("columns must be non-empty identifiers ([A-Za-z0-9_$#@], max 128)")
        return v

    @field_validator("table")
    @classmethod
    def _tab(cls, v: TableRef) -> TableRef:
        if not (verify_ident_ok(v.schema_) and verify_ident_ok(v.name)):
            raise ValueError("table schema/name must be identifiers")
        return v

    @model_validator(mode="after")
    def _shape(self) -> "VerifyTableArgs":
        if ("rows" in self.checks) != bool(self.key_values):
            raise ValueError("key_values is required with (and only with) the 'rows' check")
        for k in self.key_values or []:
            if not 1 <= len(k) <= 16:
                raise ValueError("each key value tuple has 1..16 elements")
        if len(self.model_dump_json(by_alias=True)) > VERIFY_MAX_ARGS_BYTES:
            raise ValueError(f"VERIFY_TABLE args larger than {VERIFY_MAX_ARGS_BYTES} bytes: split the command")
        return self


class VerifyColumnAgg(_M):
    count: int
    min: str | None = None
    max: str | None = None
    sum: str | None = None
    len: str | None = None


class VerifyRangeResult(_M):
    index: int
    rows: int | None = None
    agg: dict[str, VerifyColumnAgg] | None = None
    keys: list[list[str | None]] | None = None
    keys_truncated: bool = False


class VerifyTableResult(_M):
    """Command result (ack `result`) of VERIFY_TABLE. Values are the source's text forms (as in snapshot chunks)."""

    verify_id: UUID
    table: TableRef
    engine: str
    isolation: str                                  # db2: "CS" (currently committed); generic edges: "connector"
    key_columns: list[str]
    columns: dict[str, str]                         # column -> aggregate class (VERIFY_AGG_CLASSES)
    position_before: str | None = None
    position_after: str | None = None
    ranges: list[VerifyRangeResult] = Field(default_factory=list)
    rows: list[dict[str, str | None]] = Field(default_factory=list)
    missing: list[list[str | None]] = Field(default_factory=list)
    truncated: bool = False                         # size cap hit: fewer rows than asked (hub asks again)
    elapsed_ms: int = 0


RegisterResponse.model_rebuild()
