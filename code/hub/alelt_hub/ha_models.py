"""High-availability models (X-47, X-49): capture lease wire models + `capture_lease` / `hub_node` tables.

Tables are declared on the shared hub metadata (`db.md`) with portable types (X-42); importing this module registers
them (done by `alelt_hub.ha`, `alelt_hub.migrate`, `alelt_hub.metamigrate`).
"""
from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import BigInteger, Column, Integer, String, Table, Text

from alelt_common.models import Position

from .db import TS, UUID, J, md, utc_now


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid")


# ------------------------------------------------------------------ wire (agent <-> hub)
class LeaseRequest(_M):
    agent_id: str = Field(min_length=1, max_length=100)
    connection_id: str = Field(min_length=1, max_length=100)
    ttl_s: int | None = Field(default=None, ge=5, le=300)
    lease_id: uuid.UUID | None = None
    epoch: int | None = None


class LeaseGrant(_M):
    granted: bool
    connection_id: str
    holder: str | None = None
    lease_id: uuid.UUID | None = None
    epoch: int = 0
    ttl_s: int = 0
    expires_at: datetime | None = None
    takeover: bool = False           # holder changed: discard the local spool, resume at resume_from
    resume_from: Position | None = None
    retry_after_s: float = 0.0
    # AHS-24: False = the operator paused capture of this connection (last capture-control command is PAUSE):
    # the new holder starts paused until RESUME / START_CAPTURE. True = running, None = no command yet.
    capturing: bool | None = None


# ------------------------------------------------------------------ tables
capture_lease = Table(
    "capture_lease", md,
    Column("connection_id", String(100), primary_key=True),
    Column("holder", String(100)),                     # agent_id of the active agent
    Column("lease_id", UUID(as_uuid=True)),
    Column("epoch", BigInteger, nullable=False, server_default="0"),   # fencing token, +1 on every holder change
    Column("ttl_s", Integer, nullable=False, server_default="30"),
    Column("acquired_at", TS()),
    Column("renewed_at", TS()),
    Column("expires_at", TS()),
    Column("candidates", J),                           # {agent_id: last acquire attempt (ISO)} - standby visibility
    # HA-14: `lease-clear` keeps the row (epoch stays monotonic) and marks it standalone; NULL = active/passive.
    Column("mode", String(20)),
    # HA-01: release clears `holder`; the released agent is remembered so a forced failover prefers a standby.
    Column("last_holder", String(100)),
    Column("released_at", TS()),
)

hub_node = Table(
    "hub_node", md,
    Column("node_id", String(200), primary_key=True),  # host:pid (or ALELT_NODE_ID)
    Column("role", String(20), nullable=False),        # api | worker
    Column("host", String(200)),
    Column("version", String(50)),
    Column("spool_url", Text),                         # store this process writes to (topology resume check)
    Column("db_backend", String(20)),
    Column("started_at", TS(), nullable=False, server_default=utc_now()),
    Column("last_seen", TS(), nullable=False, server_default=utc_now()),
)
