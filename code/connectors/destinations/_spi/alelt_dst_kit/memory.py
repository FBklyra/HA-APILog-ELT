"""In-memory reference destination (tests, conformance self-check). State is shared per `url` in the process."""
from __future__ import annotations

import copy
from datetime import datetime
from typing import Any

from alelt_common.models import ColumnInfo, Position, TableInfo
from alelt_common.spi import ApplyStats

from .base import ControlState, KitDestination
from .plan import TablePlan

_STORES: dict[str, dict[str, Any]] = {}


class MemoryDestination(KitDestination):
    keeps_timestamp_text = True          # stores the delivered text (XC-31)
    engine = "memory"
    needs_full_rows = True

    def __init__(self, url: str = "memory://default", **kw: Any) -> None:
        super().__init__(url, **kw)
        self.store = _STORES.setdefault(f"{url}|{self.schema}", {"tables": {}, "ckpt": {}, "audit": []})

    def _on_schema(self, schema: str | None) -> "MemoryDestination":
        if not schema or schema == self.schema:
            return self
        c = copy.copy(self)
        c.schema = schema
        c.store = _STORES.setdefault(f"{self.url}|{schema}", {"tables": {}, "ckpt": {}, "audit": []})
        return c

    def bootstrap(self) -> None:
        pass

    def ensure_table(self, table: TableInfo, dest_name: str, golden_ddl: str | None) -> None:
        self.store["tables"].setdefault(self.ident(dest_name), {})
        self.store.setdefault("info", {})[self.ident(dest_name)] = table

    @property
    def _info(self) -> dict[str, TableInfo]:
        return self.store.setdefault("info", {})  # type: ignore[no-any-return]

    def add_column(self, dest_name: str, col: ColumnInfo) -> None:
        pass

    def control_state(self, stream: str) -> ControlState:
        return copy.deepcopy(self.store["ckpt"].get(stream) or ControlState())

    def lookup(self, plan: TablePlan, keys: list[tuple[Any, ...]]) -> dict[tuple[Any, ...], dict[str, Any]]:
        t = self.store["tables"].get(self.ident(plan.dest), {})
        rev = {self.ident(c): c for c in plan.columns}
        return {k: {rev[c]: v for c, v in t[k].items() if c in rev} for k in keys if k in t}

    def commit_batch(self, stream: str | None, plans: list[TablePlan], to_pos: Position | None, batch_id: str,
                     prev: ControlState, ts: datetime, stats: dict[str, ApplyStats]) -> None:
        if stream is not None and to_pos is not None:          # compare-and-set (the store is the "lock")
            self.cas(stream, prev, self.control_state(stream), to_pos)
        work = copy.deepcopy(self.store["tables"])
        for pl in plans:
            t = work.setdefault(self.ident(pl.dest), {})
            if pl.truncate:
                for k in list(t):
                    if self.hard:
                        del t[k]
                    elif not t[k][self.c_deleted]:
                        t[k] = {**t[k], self.c_deleted: True, self.c_synced: ts}
            for op in pl.ops.values():
                if op.op == "D" and self.hard:
                    t.pop(op.key, None)
                else:
                    t[op.key] = self.phys_row(pl, op, ts)
            self.count(pl, stats[pl.dest])
        self._hook("after_data")
        self._hook("before_commit")
        self.store["tables"] = work
        if stream is not None and to_pos is not None:
            self.cas(stream, prev, self.control_state(stream), to_pos)
            self.store["ckpt"][stream] = self.next_state(prev, to_pos, batch_id)
            self.store["audit"].append(self.audit_record(stream, prev, to_pos, batch_id, stats))

    def delete_checkpoint(self, stream: str) -> None:
        self.store["ckpt"].pop(stream, None)

    def fetch_rows(self, dest_name: str) -> list[dict[str, Any]]:
        return [dict(r) for r in self.store["tables"].get(self.ident(dest_name), {}).values()]

    def audit_rows(self, stream: str) -> list[dict[str, Any]]:
        return [a for a in self.store["audit"] if a["stream"] == stream]
