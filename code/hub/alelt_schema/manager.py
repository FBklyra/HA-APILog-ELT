"""Applies drift decisions through the Destination SPI; records pending approvals; regenerates views."""
from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import datetime
from enum import Enum
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from alelt_common.models import CatalogSnapshot, ColumnInfo, TableInfo, TableRef
from alelt_common.spi import DestinationConnector
from alelt_common.util import utcnow

from .config import ConnectionConfig, OutputProfile, SchemaConfig, TableConfig
from .drift import DriftEvent, DriftKind, diff_catalogs
from .policy import Action, decide
from .views import view_columns

AlertSink = Callable[[str, dict[str, Any]], None]
GoldenLookup = Callable[[str], tuple[str | None, list[str] | None]]   # dest_name -> (ddl, column order)


class ApprovalStatus(str, Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class PendingApproval(BaseModel):
    model_config = ConfigDict(extra="forbid")

    approval_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    connection_id: str
    event: DriftEvent
    status: ApprovalStatus = ApprovalStatus.PENDING
    created_at: datetime
    decided_by: str | None = None
    decided_at: datetime | None = None


class ApprovalStore(Protocol):
    def add(self, a: PendingApproval) -> None: ...
    def get(self, approval_id: str) -> PendingApproval: ...
    def update(self, a: PendingApproval) -> None: ...
    def pending(self, connection_id: str) -> list[PendingApproval]: ...


class InMemoryApprovalStore:
    def __init__(self) -> None:
        self.items: dict[str, PendingApproval] = {}

    def add(self, a: PendingApproval) -> None:
        self.items[a.approval_id] = a

    def get(self, approval_id: str) -> PendingApproval:
        return self.items[approval_id]

    def update(self, a: PendingApproval) -> None:
        self.items[a.approval_id] = a

    def pending(self, connection_id: str) -> list[PendingApproval]:
        return [a for a in self.items.values() if a.connection_id == connection_id and a.status == ApprovalStatus.PENDING]


class DriftReport(BaseModel):
    applied: list[DriftEvent] = Field(default_factory=list)
    pending: list[PendingApproval] = Field(default_factory=list)
    ignored: list[DriftEvent] = Field(default_factory=list)
    views_regenerated: list[str] = Field(default_factory=list)


class SchemaManager:
    def __init__(self, dest: DestinationConnector, cfg: ConnectionConfig, profile: OutputProfile,
                 store: ApprovalStore, alert: AlertSink,
                 golden: GoldenLookup = lambda name: (None, None),
                 clock: Callable[[], datetime] = utcnow) -> None:
        self.dest, self.cfg, self.profile, self.store, self.alert = dest, cfg, profile, store, alert
        self.golden, self.clock = golden, clock

    def process(self, old: CatalogSnapshot | None, new: CatalogSnapshot) -> DriftReport:
        report = DriftReport()
        tables = {t.table: t for t in new.tables}
        dropped: dict[TableRef, list[str]] = {}
        touched: set[TableRef] = set()
        for ev in diff_catalogs(old, new):
            if ev.kind == DriftKind.COLUMN_DROPPED and ev.column:
                dropped.setdefault(ev.table, []).append(ev.column)   # never dropped in landing: kept in view
            action = decide(ev, self.cfg.schema_change_handling, self.cfg.is_enabled(ev.table))
            if ev.alert:
                self.alert(ev.describe(), {"connection_id": self.cfg.connection_id, **ev.model_dump(mode="json")})
            if action == Action.APPLY:
                self._apply(ev, tables)
                touched.add(ev.table)
                report.applied.append(ev)
            elif action == Action.PENDING:
                a = PendingApproval(connection_id=self.cfg.connection_id, event=ev, created_at=self.clock())
                self.store.add(a)
                report.pending.append(a)
                self.alert(f"approval required: {ev.describe()}", {"approval_id": a.approval_id})
            else:
                report.ignored.append(ev)
        for ref in sorted(touched, key=lambda r: r.fqn):
            if ref in tables:
                report.views_regenerated.append(self.publish(tables[ref], dropped.get(ref, [])))
        return report

    def approve(self, approval_id: str, by: str, catalog: CatalogSnapshot) -> PendingApproval:
        """Approve and apply. Non-additive changes (alter/drop/key/capture) are acknowledged; view regenerated."""
        a = self.store.get(approval_id)
        if a.status != ApprovalStatus.PENDING:
            raise ValueError(f"approval {approval_id} is {a.status.value}")
        tables = {t.table: t for t in catalog.tables}
        if a.event.kind in (DriftKind.COLUMN_ADDED, DriftKind.TABLE_ADDED):
            self._apply(a.event, tables)
        if a.event.table in tables and (self.cfg.is_enabled(a.event.table) or a.event.kind == DriftKind.TABLE_ADDED):
            retained = [a.event.column] if a.event.kind == DriftKind.COLUMN_DROPPED and a.event.column else []
            self.publish(tables[a.event.table], retained)
        a = a.model_copy(update={"status": ApprovalStatus.APPROVED, "decided_by": by, "decided_at": self.clock()})
        self.store.update(a)
        return a

    def reject(self, approval_id: str, by: str) -> PendingApproval:
        a = self.store.get(approval_id).model_copy(
            update={"status": ApprovalStatus.REJECTED, "decided_by": by, "decided_at": self.clock()})
        self.store.update(a)
        return a

    def publish(self, table: TableInfo, retained: list[str] | None = None) -> str:
        name = self.cfg.dest_name(table.table)
        _, golden_cols = self.golden(name)
        cols = view_columns(table, self.profile, golden_cols, retained or [])
        self.dest.publish_view(self.cfg.views, name, self.cfg.landing, cols)
        return name

    def _apply(self, ev: DriftEvent, tables: dict[TableRef, TableInfo]) -> None:
        if ev.kind == DriftKind.COLUMN_ADDED and ev.new is not None:
            self.dest.add_column(self.cfg.dest_name(ev.table), ColumnInfo.model_validate(ev.new))
        elif ev.kind == DriftKind.TABLE_ADDED:
            info = tables.get(ev.table) or TableInfo.model_validate(ev.new or {})
            sc = self.cfg.schemas.setdefault(ev.table.schema_, SchemaConfig())
            sc.tables.setdefault(ev.table.name, TableConfig())
            name = self.cfg.dest_name(ev.table)
            ddl, _ = self.golden(name)
            self.dest.ensure_table(info, name, ddl)

