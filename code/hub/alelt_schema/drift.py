"""Drift detection between successive CatalogSnapshots (pure)."""
from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict

from alelt_common.models import CatalogSnapshot, ColumnInfo, TableInfo, TableRef


class DriftKind(str, Enum):
    TABLE_ADDED = "TABLE_ADDED"
    TABLE_DROPPED = "TABLE_DROPPED"
    COLUMN_ADDED = "COLUMN_ADDED"
    COLUMN_DROPPED = "COLUMN_DROPPED"
    COLUMN_ALTERED = "COLUMN_ALTERED"
    KEY_CHANGED = "KEY_CHANGED"
    CAPTURE_LOST = "CAPTURE_LOST"            # DATA CAPTURE CHANGES removed (e.g. application upgrade) => alert
    CAPTURE_ENABLED = "CAPTURE_ENABLED"
    COMPRESSION_CHANGED = "COMPRESSION_CHANGED"


ALERT_KINDS = {DriftKind.CAPTURE_LOST, DriftKind.TABLE_DROPPED, DriftKind.KEY_CHANGED, DriftKind.COMPRESSION_CHANGED}
_ALTER_FIELDS = ("engine_type", "canonical_type", "length", "scale", "nullable", "codepage")


class DriftEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: DriftKind
    table: TableRef
    column: str | None = None
    old: dict[str, Any] | None = None
    new: dict[str, Any] | None = None

    @property
    def alert(self) -> bool:
        return self.kind in ALERT_KINDS

    def describe(self) -> str:
        tgt = self.table.fqn + (f".{self.column}" if self.column else "")
        return f"{self.kind.value} {tgt}"


def _cols(t: TableInfo) -> dict[str, ColumnInfo]:
    return {c.name: c for c in t.columns}


def diff_tables(old: TableInfo, new: TableInfo) -> list[DriftEvent]:
    ref = new.table
    out: list[DriftEvent] = []
    if old.capture_enabled and not new.capture_enabled:
        out.append(DriftEvent(kind=DriftKind.CAPTURE_LOST, table=ref))
    elif new.capture_enabled and not old.capture_enabled:
        out.append(DriftEvent(kind=DriftKind.CAPTURE_ENABLED, table=ref))
    if (old.compression or None) != (new.compression or None):
        out.append(DriftEvent(kind=DriftKind.COMPRESSION_CHANGED, table=ref,
                              old={"compression": old.compression}, new={"compression": new.compression}))
    oc, nc = _cols(old), _cols(new)
    for name in sorted(nc.keys() - oc.keys(), key=lambda n: nc[n].ordinal):
        out.append(DriftEvent(kind=DriftKind.COLUMN_ADDED, table=ref, column=name, new=nc[name].model_dump(mode="json")))
    for name in sorted(oc.keys() - nc.keys(), key=lambda n: oc[n].ordinal):
        out.append(DriftEvent(kind=DriftKind.COLUMN_DROPPED, table=ref, column=name, old=oc[name].model_dump(mode="json")))
    for name in sorted(oc.keys() & nc.keys(), key=lambda n: nc[n].ordinal):
        a, b = oc[name], nc[name]
        changed = {f: (getattr(a, f), getattr(b, f)) for f in _ALTER_FIELDS if getattr(a, f) != getattr(b, f)}
        if changed:
            out.append(DriftEvent(kind=DriftKind.COLUMN_ALTERED, table=ref, column=name,
                                  old=a.model_dump(mode="json"), new=b.model_dump(mode="json")))
    if [c.name for c in old.key_columns()] != [c.name for c in new.key_columns()]:
        out.append(DriftEvent(kind=DriftKind.KEY_CHANGED, table=ref, old={"key": [c.name for c in old.key_columns()]},
                              new={"key": [c.name for c in new.key_columns()]}))
    return out


def diff_catalogs(old: CatalogSnapshot | None, new: CatalogSnapshot) -> list[DriftEvent]:
    """All drift events, ordered by table fqn then event kind order above (deterministic)."""
    before = {t.table: t for t in (old.tables if old else [])}
    after = {t.table: t for t in new.tables}
    out: list[DriftEvent] = []
    for ref in sorted(before.keys() | after.keys(), key=lambda r: r.fqn):
        if ref not in before:
            out.append(DriftEvent(kind=DriftKind.TABLE_ADDED, table=ref, new=after[ref].model_dump(mode="json", by_alias=True)))
        elif ref not in after:
            out.append(DriftEvent(kind=DriftKind.TABLE_DROPPED, table=ref))
        else:
            out.extend(diff_tables(before[ref], after[ref]))
    return out
