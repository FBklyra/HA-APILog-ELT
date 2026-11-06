"""Vendor-neutral migration models: what a legacy platform exposes (InventoryModel) and what ALELT will
create from it (MigrationPlan). Secrets never appear in either: values are masked and listed in `secret_fields`."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

MASK = "********"
Severity = Literal["info", "warning", "blocker"]
TriggerKind = Literal["dbt_job", "webhook", "notification", "transformation"]


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CredentialField(_M):
    """A credential the plugin needs for discovery (rendered by the console; `secret` => masked input)."""

    name: str
    label: str
    secret: bool = False
    required: bool = True
    multiline: bool = False
    help: str = ""


class PluginInfo(_M):
    name: str
    display_name: str
    description: str = ""
    credential_fields: list[CredentialField] = Field(default_factory=list)
    read_only: bool = True
    accepts_files: bool = False
    file_hint: str = ""
    live: bool = True


class Warning_(_M):
    code: str
    severity: Severity = "warning"
    message: str
    ref: str | None = None              # e.g. "connection:<id>", "table:<id>/S.T"


# ------------------------------------------------------------------ inventory (legacy platform, read-only view)
class InvSchedule(_M):
    frequency_minutes: int | None = None
    daily_time: str | None = None       # "HH:MM" when the legacy runs once a day
    timezone: str | None = None
    paused: bool = False
    schedule_type: str | None = None    # legacy wording, e.g. auto / manual


class InvColumn(_M):
    name: str
    name_in_destination: str | None = None
    enabled: bool = True                # False = blocked / excluded
    hashed: bool = False
    is_primary_key: bool = False
    source_type: str | None = None


class InvTable(_M):
    name: str
    name_in_destination: str | None = None
    enabled: bool = True
    sync_mode: str = "SOFT_DELETE"      # SOFT_DELETE | HISTORY | LIVE
    columns: list[InvColumn] = Field(default_factory=list)
    note: str | None = None


class InvSchema(_M):
    name: str
    name_in_destination: str | None = None
    enabled: bool = True
    tables: list[InvTable] = Field(default_factory=list)


class InvGroup(_M):
    id: str
    name: str


class InvDestination(_M):
    id: str
    name: str
    service: str                        # legacy destination type
    engine: str | None = None           # ALELT destination engine, None = no equivalent
    settings: dict[str, Any] = Field(default_factory=dict)      # secrets masked
    secret_fields: list[str] = Field(default_factory=list)


class InvConnection(_M):
    id: str
    name: str
    group_id: str | None = None
    service: str                        # legacy source type
    engine: str | None = None           # ALELT source engine, None = no equivalent
    destination_id: str | None = None
    destination_schema: str | None = None   # existing landing schema prefix in the destination
    settings: dict[str, Any] = Field(default_factory=dict)      # secrets masked
    secret_fields: list[str] = Field(default_factory=list)
    schedule: InvSchedule = Field(default_factory=InvSchedule)
    schema_change_policy: str = "ALLOW_COLUMNS"   # ALLOW_ALL | ALLOW_COLUMNS | BLOCK_ALL
    schemas: list[InvSchema] = Field(default_factory=list)
    status: str | None = None


class InvTrigger(_M):
    id: str
    kind: TriggerKind
    name: str
    connection_ids: list[str] = Field(default_factory=list)    # legacy connections it runs after
    target: str | None = None           # URL / job reference (masked if it embeds a secret)
    events: list[str] = Field(default_factory=list)
    schedule: str | None = None
    enabled: bool = True
    settings: dict[str, Any] = Field(default_factory=dict)


class InvUser(_M):
    id: str
    email: str
    name: str = ""
    role: str | None = None             # legacy role name
    teams: list[str] = Field(default_factory=list)
    active: bool = True


class InventoryModel(_M):
    plugin: str
    platform: str                       # display name of the legacy platform
    account: dict[str, Any] = Field(default_factory=dict)
    discovered_at: datetime
    groups: list[InvGroup] = Field(default_factory=list)
    destinations: list[InvDestination] = Field(default_factory=list)
    connections: list[InvConnection] = Field(default_factory=list)
    triggers: list[InvTrigger] = Field(default_factory=list)
    users: list[InvUser] = Field(default_factory=list)
    warnings: list[Warning_] = Field(default_factory=list)


# ------------------------------------------------------------------ plan (what ALELT creates)
class PlannedSecret(_M):
    key: str                            # stable key; the same credential is asked once
    label: str
    owner: str                          # "connection:<alelt id>" | "destination:<legacy id>"
    field: str
    where: Literal["hub", "agent"] = "hub"   # edge sources keep source credentials on the agent host
    used_by: list[str] = Field(default_factory=list)


class PlannedConnection(_M):
    legacy_id: str
    legacy_name: str
    connection_id: str
    include: bool = True
    supported: bool = True
    source_engine: str | None
    destination_engine: str | None
    capture_mode: str | None = None
    source: dict[str, Any] = Field(default_factory=dict)       # non-secret settings + {"secret_ref": key}
    destination: dict[str, Any] = Field(default_factory=dict)
    tables: dict[str, Any] = Field(default_factory=dict)       # config/tables.yaml document
    latency_target_s: int = 120
    schedule: InvSchedule = Field(default_factory=InvSchedule)
    paused: bool = False
    legacy_schema: str | None = None
    enabled_tables: int = 0
    total_tables: int = 0


class PlannedTrigger(_M):
    legacy_id: str
    kind: Literal["webhook", "dbt_job"]
    name: str
    include: bool = True
    target: str | None = None
    events: list[str] = Field(default_factory=list)
    after_connections: list[str] = Field(default_factory=list)  # ALELT connection ids
    settings: dict[str, Any] = Field(default_factory=dict)


class PlannedRole(_M):
    email: str
    name: str = ""
    roles: list[str]
    legacy_role: str | None = None
    include: bool = True


class MigrationPlan(_M):
    plugin: str
    platform: str
    generated_at: datetime
    connections: list[PlannedConnection] = Field(default_factory=list)
    triggers: list[PlannedTrigger] = Field(default_factory=list)
    roles: list[PlannedRole] = Field(default_factory=list)
    secrets: list[PlannedSecret] = Field(default_factory=list)
    warnings: list[Warning_] = Field(default_factory=list)
    summary: dict[str, int] = Field(default_factory=dict)


class PlanEdits(_M):
    """User toggles from the review tree; applied on top of the generated plan (idempotent)."""

    connections: dict[str, ConnectionEdit] = Field(default_factory=dict)    # by legacy_id
    triggers: dict[str, bool] = Field(default_factory=dict)                 # legacy_id -> include
    roles: dict[str, bool] = Field(default_factory=dict)                    # email -> include


class TableEdit(_M):
    enabled: bool | None = None
    columns: dict[str, ColumnEdit] = Field(default_factory=dict)


class ColumnEdit(_M):
    enabled: bool | None = None
    hashed: bool | None = None


class ConnectionEdit(_M):
    include: bool | None = None
    connection_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_]{0,62}$")
    latency_target_s: int | None = Field(default=None, ge=10, le=86400)
    output_profile: str | None = None
    tables: dict[str, TableEdit] = Field(default_factory=dict)              # "S.T"


PlanEdits.model_rebuild()
TableEdit.model_rebuild()
ConnectionEdit.model_rebuild()
