"""Diagnosis models (pydantic v2). Wire shapes of the incident API live here, not in alelt_common."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid")


class IncidentKind(str, Enum):
    JOB_FAILED = "JOB_FAILED"                  # decode / load / snapshot / verify job failure
    DESTINATION_ERROR = "DESTINATION_ERROR"    # auth, warehouse suspended, permissions, MERGE failure
    QUARANTINE = "QUARANTINE"                  # records quarantined (burst => critical)
    AGENT_OFFLINE = "AGENT_OFFLINE"
    AGENT_ERROR = "AGENT_ERROR"                # agent state ERROR / error event
    AGENT_AUTH = "AGENT_AUTH"                  # agent cert / token rejected at the gateway
    GOVERNOR_PAUSE = "GOVERNOR_PAUSE"
    BACKPRESSURE = "BACKPRESSURE"              # spool full / hub backlog 503
    CAPTURE_LOST = "CAPTURE_LOST"              # DATA CAPTURE CHANGES off on an in-scope table
    POSITION_GAP = "POSITION_GAP"              # gap / overlap / position mismatch
    RECON_MISMATCH = "RECON_MISMATCH"
    LAG_ABOVE_TARGET = "LAG_ABOVE_TARGET"
    ALERT = "ALERT"                            # explain an alert by name
    SOURCE_DATA = "SOURCE_DATA"                # root cause in the source data (not a ALELT defect)
    TABLE_OUT_OF_SYNC = "TABLE_OUT_OF_SYNC"    # table held / suspect after an unexplained quarantine (F-01)
    COMPLETION_STALLED = "COMPLETION_STALLED"  # out-of-row values of a table not fetched for ALELT_COMPLETION_STALL_S
    COMPLETION_OVERSIZE = "COMPLETION_OVERSIZE"  # one row's out-of-row values exceed the edge result cap
    DECODE_LAG = "DECODE_LAG"                  # decoder behind the edge (AleltDecodeLag)


class Classification(str, Enum):
    """Where the root cause lives. Rules decide first, the LLM only fills UNKNOWN."""

    PLATFORM_DEFECT = "PLATFORM_DEFECT"        # ALELT bug / unsupported feature -> code-fix proposal
    SOURCE_DATA = "SOURCE_DATA"                # values written by the application -> data owner
    CONFIGURATION = "CONFIGURATION"            # settings, credentials, capture flags, catalog snapshots
    INFRASTRUCTURE = "INFRASTRUCTURE"          # disk, network, clocks, retention, agent host
    DESTINATION = "DESTINATION"                # warehouse, permissions, landing contract
    UNKNOWN = "UNKNOWN"


SEVERITY: dict[IncidentKind, str] = {
    IncidentKind.JOB_FAILED: "critical", IncidentKind.DESTINATION_ERROR: "critical",
    IncidentKind.QUARANTINE: "warning", IncidentKind.AGENT_OFFLINE: "critical", IncidentKind.AGENT_ERROR: "warning",
    IncidentKind.AGENT_AUTH: "critical", IncidentKind.GOVERNOR_PAUSE: "info", IncidentKind.BACKPRESSURE: "warning",
    IncidentKind.CAPTURE_LOST: "critical", IncidentKind.POSITION_GAP: "critical",
    IncidentKind.RECON_MISMATCH: "critical", IncidentKind.LAG_ABOVE_TARGET: "warning", IncidentKind.ALERT: "warning",
    IncidentKind.SOURCE_DATA: "warning", IncidentKind.TABLE_OUT_OF_SYNC: "critical",
    IncidentKind.COMPLETION_STALLED: "critical", IncidentKind.COMPLETION_OVERSIZE: "warning",
    IncidentKind.DECODE_LAG: "warning",
}

# Prometheus alert name (deploy/hub/observability/alerts.yml) -> incident kind
ALERT_KINDS: dict[str, IncidentKind] = {
    "AleltCaptureLagAboveTarget": IncidentKind.LAG_ABOVE_TARGET, "AleltLatencyP95AboveTarget": IncidentKind.LAG_ABOVE_TARGET,
    "AleltCaptureLost": IncidentKind.CAPTURE_LOST, "AleltAgentOffline": IncidentKind.AGENT_OFFLINE,
    "AleltSpoolAbove70Pct": IncidentKind.BACKPRESSURE, "AleltReconMismatch": IncidentKind.RECON_MISMATCH,
    "AleltQuarantineGrowing": IncidentKind.QUARANTINE, "AleltArchiveRetentionHeadroomLow": IncidentKind.LAG_ABOVE_TARGET,
    "AleltTableOutOfSync": IncidentKind.TABLE_OUT_OF_SYNC,
    "AleltCompletionStalled": IncidentKind.COMPLETION_STALLED, "AleltDecodeLag": IncidentKind.DECODE_LAG,
}


class Trigger(_M):
    """What happened. Everything an incident is linked to (batch / table / position / job)."""

    kind: IncidentKind
    connection_id: str | None = None
    table_fqn: str | None = None
    batch_id: str | None = None
    job_id: int | None = None
    agent_id: str | None = None
    position: str | None = None
    stage: str | None = None             # decode / load / snapshot / verify
    error: str = ""
    detail: dict[str, Any] = Field(default_factory=dict)


ActionKind = Literal["RETRY_JOB", "REDECODE_RANGE", "RESYNC_TABLE", "SNAPSHOT_CATALOG", "RESUME_AGENT",
                     "SOURCE_DATA_POLICY"]
# Actions that only replay hub-side work or ask the agent for a read-only catalog snapshot: idempotent.
SAFE_IDEMPOTENT: frozenset[str] = frozenset({"RETRY_JOB", "REDECODE_RANGE", "SNAPSHOT_CATALOG"})


class SuggestedAction(_M):
    kind: ActionKind
    params: dict[str, Any] = Field(default_factory=dict)
    description: str

    @property
    def safe(self) -> bool:
        return self.kind in SAFE_IDEMPOTENT


class Finding(_M):
    rule_id: str
    title: str
    root_cause: str
    explanation: str                      # plain language, for operators
    confidence: float = Field(ge=0, le=1)
    category: Classification = Classification.UNKNOWN
    evidence_refs: list[str] = Field(default_factory=list)
    fix_steps: list[str] = Field(default_factory=list)
    action: SuggestedAction | None = None
    source_data: SourceDataIssue | None = None


SourceIssue = Literal["INVALID_ENCODING", "INVALID_DATE", "FUTURE_TIMESTAMP", "NULL_IN_NOT_NULL", "OUT_OF_RANGE",
                      "DUPLICATE_KEYLESS", "ORPHAN_KEY"]
HANDLING_OPTIONS = ("QUARANTINE_SKIP", "COERCE", "ACCEPT_AS_IS")


class SourceDataIssue(_M):
    """Source-data diagnosis. Values are never stored here (only masked keys); raw samples need incident.reveal."""

    issue: SourceIssue
    table: str | None = None
    column: str | None = None
    key_columns: list[str] = Field(default_factory=list)
    positions: list[str] = Field(default_factory=list)
    count: int = 0
    explanation: str = ""                 # for the data owner
    handling_options: list[str] = Field(default_factory=lambda: list(HANDLING_OPTIONS))
    recommended: str = "QUARANTINE_SKIP"
    dba_sql: str = ""                     # read-only query, shown never executed (R1)


class EvidencePack(_M):
    """Redacted, read-only evidence. Never contains row values or secrets (see redact.py)."""

    collected_at: datetime
    redaction: Literal["strict", "standard"] = "strict"
    trigger: Trigger
    job: dict[str, Any] | None = None
    batch: dict[str, Any] | None = None
    quarantine: list[dict[str, Any]] = Field(default_factory=list)
    quarantine_reasons: dict[str, int] = Field(default_factory=dict)
    catalog: dict[str, Any] = Field(default_factory=dict)
    schema_changes: list[dict[str, Any]] = Field(default_factory=list)
    agent: dict[str, Any] | None = None
    agent_events: list[dict[str, Any]] = Field(default_factory=list)
    config_revisions: list[dict[str, Any]] = Field(default_factory=list)
    positions: dict[str, Any] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)
    logs: list[dict[str, Any]] = Field(default_factory=list)
    recon: list[dict[str, Any]] = Field(default_factory=list)

    def text(self) -> str:
        """Lower-cased haystack of every error-ish string (rules match on it)."""
        parts = [self.trigger.error, str(self.trigger.detail.get("traceback", ""))]
        if self.job:
            parts.append(str(self.job.get("error") or ""))
        if self.batch:
            parts.append(str(self.batch.get("error") or ""))
        parts += [str(q.get("reason", "")) for q in self.quarantine]
        parts += [f"{e.get('type', '')} {e.get('detail', '')}" for e in self.agent_events]
        parts += [str(lg.get("msg", "")) + " " + str(lg.get("exc", "")) for lg in self.logs]
        parts.append(str(self.positions.get("destination_error") or ""))
        return "\n".join(parts).lower()


class LlmAnswer(_M):
    """Structured Stage-B answer (validated; never auto-executed)."""

    model_config = ConfigDict(extra="ignore")
    root_cause: str
    confidence: float = Field(ge=0, le=1)
    reasoning_summary: str
    fix_steps: list[str] = Field(default_factory=list)
    risk: Literal["low", "medium", "high"] = "medium"
    needs_human: bool = True
    classification: Classification | None = None

    @field_validator("confidence", mode="before")
    @classmethod
    def _pct(cls, v: Any) -> Any:
        return v / 100 if isinstance(v, (int, float)) and 1 < v <= 100 else v


Provider = Literal["anthropic", "openai_compatible", "ollama"]
Redaction = Literal["strict", "standard"]


class LlmSettingsView(_M):
    """GET /admin/llm - the API key is never returned, only `key_set`."""

    enabled: bool
    provider: Provider
    url: str
    model: str
    timeout_s: float
    max_tokens_day: int
    max_output_tokens: int
    redaction: Redaction
    key_set: bool
    configured: bool
    tokens_used_today: int
    source: dict[str, str] = Field(default_factory=dict)   # field -> "env" | "console" | "default"


class LlmSettingsUpdate(BaseModel):
    """PUT /admin/llm. `api_key`: write-only; "" clears the stored key; omitted keeps it."""

    model_config = ConfigDict(extra="forbid")
    reason: str | None = None
    enabled: bool | None = None
    provider: Provider | None = None
    url: str | None = None
    model: str | None = None
    api_key: str | None = None
    timeout_s: float | None = Field(default=None, ge=1, le=300)
    max_tokens_day: int | None = Field(default=None, ge=0, le=100_000_000)
    max_output_tokens: int | None = Field(default=None, ge=128, le=64_000)
    redaction: Redaction | None = None


class ExplainResult(_M):
    kind: str
    ref_id: str
    source: Literal["rules", "llm"]
    rule_id: str | None
    root_cause: str
    explanation: str
    fix_steps: list[str]
    confidence: float
    needs_human: bool
    llm_configured: bool
    llm_error: str | None = None
    classification: Classification = Classification.UNKNOWN
    findings: list[Finding] = Field(default_factory=list)
    run_id: str | None = None


class CodeFixAnswer(_M):
    """Stage-B code-fix proposal (validated; never applied by the hub)."""

    model_config = ConfigDict(extra="ignore")
    summary: str
    explanation: str
    files: list[str] = Field(default_factory=list)
    patch: str                            # unified diff, repo-relative paths (a/..., b/...)
    test_path: str | None = None          # the regression test the patch adds/changes
    confidence: float = Field(default=0.5, ge=0, le=1)


Finding.model_rebuild()
