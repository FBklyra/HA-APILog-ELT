"""Canonical change sets -> net row operations per destination table.

The loader already compacts to a final op per key; plan_batch() does it again (idempotent) so a destination is
correct when fed raw change sets too:
- INSERT/UPDATE => upsert of the after image (partial images merge over an earlier upsert of the same key)
- UPDATE whose before key differs from the after key => delete(old key) + upsert(new key)  (PK update)
- DELETE => delete with the before image (soft delete keeps the row values)
- TRUNCATE => drops earlier ops of the batch for that table and marks the table truncated (applied first)
- keyless tables => key = surrogate id column of the output profile (alelt_loader.surrogate_id)
An event without the full key raises ValueError before anything is written (atomicity).
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Literal

from alelt_common.models import ChangeKind, ChangeSet, TableInfo, TableRef
from alelt_loader import data_columns, surrogate_id
from alelt_loader.loader import _canon
from alelt_loader.policy import SystemColumns


@dataclass
class RowOp:
    op: Literal["U", "D"]                # upsert / delete
    key: tuple[Any, ...]                 # canonical key values (hashable)
    row: dict[str, Any]                  # source column -> canonical value (delete: before image)
    kind: ChangeKind                     # last source event kind (stats when existence is unknown)
    position: str
    partial: bool = False                # image lacks some data columns


@dataclass
class TablePlan:
    ref: TableRef
    dest: str
    info: TableInfo
    keys: list[str]                      # key columns (source names); keyless => [sys.row_id]
    columns: list[str]                   # data columns in ordinal order (+ row_id first when keyless)
    keyless: bool = False
    truncate: bool = False
    truncate_position: str | None = None
    ops: OrderedDict[tuple[Any, ...], RowOp] = field(default_factory=OrderedDict)

    def upserts(self) -> list[RowOp]:
        return [o for o in self.ops.values() if o.op == "U"]

    def deletes(self) -> list[RowOp]:
        return [o for o in self.ops.values() if o.op == "D"]


def plan_batch(changesets: list[ChangeSet], table_map: dict[TableRef, str], tables: dict[TableRef, TableInfo],
               sys_cols: SystemColumns) -> dict[str, TablePlan]:
    plans: dict[str, TablePlan] = {}
    for cs in changesets:
        for ev in cs.events:
            ref = ev.table
            if ref is None or ref not in table_map:
                continue
            dest = table_map[ref]
            tp = plans.get(dest)
            if tp is None:
                info = tables[ref]
                keys = [c.name for c in info.key_columns()]
                cols = data_columns(info)
                keyless = not keys
                if keyless:
                    keys, cols = [sys_cols.row_id], [sys_cols.row_id, *cols]
                tp = plans[dest] = TablePlan(ref=ref, dest=dest, info=info, keys=keys, columns=cols, keyless=keyless)
            pos = ev.position.text
            if ev.kind is ChangeKind.TRUNCATE:
                tp.ops.clear()
                tp.truncate, tp.truncate_position = True, pos
                continue
            if ev.kind not in (ChangeKind.INSERT, ChangeKind.UPDATE, ChangeKind.DELETE):
                continue
            known = set(tp.columns)
            before = _img(tp, ev.before, known)
            after = _img(tp, ev.after, known)
            if ev.kind is ChangeKind.DELETE:
                img = before if before is not None else after
                k = _key(tp, img, pos)
                prev = tp.ops.pop(k, None)
                img = {**prev.row, **(img or {})} if prev is not None and prev.op == "U" else (img or {})
                tp.ops[k] = RowOp("D", k, img, ev.kind, pos, _partial(tp, img))
                continue
            if after is None:
                raise ValueError(f"{dest}: {ev.kind.value} without after image at {pos}")
            ka = _key(tp, after, pos)
            if ev.kind is ChangeKind.UPDATE and before is not None and all(k in before for k in tp.keys):
                kb = _key(tp, before, pos)
                if kb != ka:                                  # PK update => delete old key
                    old = tp.ops.pop(kb, None)
                    img = {**old.row, **before} if old is not None and old.op == "U" else before
                    tp.ops[kb] = RowOp("D", kb, img, ev.kind, pos, _partial(tp, img))
            prev = tp.ops.pop(ka, None)
            row = {**prev.row, **after} if prev is not None and prev.op == "U" else after
            tp.ops[ka] = RowOp("U", ka, row, ev.kind, pos, _partial(tp, row))
    return plans


def _img(tp: TablePlan, row: dict[str, Any] | None, known: set[str]) -> dict[str, Any] | None:
    if row is None:
        return None
    out = {k: v for k, v in row.items() if k in known}
    if tp.keyless:
        data = [c for c in tp.columns if c != tp.keys[0]]
        missing = [c for c in data if c not in out]
        if missing:                    # surrogate needs the full image (DST-kit-03): never guess, abort the batch
            raise ValueError(f"{tp.dest}: keyless_partial_image - keyless table needs full row images "
                             f"(missing {missing[:5]}); enable full before images at the source")
        out = {tp.keys[0]: surrogate_id(out, data), **out}
    return out


def _key(tp: TablePlan, row: dict[str, Any] | None, pos: str) -> tuple[Any, ...]:
    if row is None or any(k not in row or row[k] is None for k in tp.keys):
        raise ValueError(f"{tp.dest}: event without full key at {pos}")
    return tuple(_canon(row[k]) for k in tp.keys)


def _partial(tp: TablePlan, row: dict[str, Any] | None) -> bool:
    return row is None or any(c not in row for c in tp.columns)


def complete_rows(plan: TablePlan, existing: dict[tuple[Any, ...], dict[str, Any]]) -> None:
    """Fill partial images from the current destination rows (engines that rewrite whole rows)."""
    for op in plan.ops.values():
        cur = existing.get(op.key)
        if op.partial and op.op != "D" and cur is None and op.kind is not ChangeKind.INSERT:
            # DST-kit-02: an UPDATE image without the current row would NULL every absent column - refuse
            raise ValueError(f"{plan.dest}: partial update image for key {op.key} but no current row found "
                             "(lookup miss) - refusing to NULL-fill absent columns")
        if op.partial or op.op == "D":
            base = {c: None for c in plan.columns}
            if cur:
                base.update({c: cur.get(c) for c in plan.columns if c in cur})
            op.row = {**base, **op.row}
            op.partial = False
