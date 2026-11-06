"""Fold change sets into per-table load rows: final operation per key within a batch (pure)."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from alelt_common.models import ChangeKind, ChangeSet, TableInfo, TableRef

from .profile import OutputProfile


@dataclass
class TableBatch:
    """Rows for one destination table; each row has data columns + op ('I'/'U'/'D') + seq + `_present` (data
    columns carried by the image: a partial image updates only those, never NULLs the others)."""

    ref: TableRef
    rows: list[dict[str, Any]] = field(default_factory=list)
    truncated: bool = False
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    min_commit_ts: datetime | None = None
    max_commit_ts: datetime | None = None


def _jsonable(v: Any) -> Any:
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).hex()
    if isinstance(v, (datetime, date, time)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return format(v.normalize(), "f")
    return v


def surrogate_id(row: dict[str, Any], columns: list[str]) -> str:
    """Surrogate key for keyless tables: sha256 over the canonical JSON of all data columns.
    Contract note: if the golden spec requires another algorithm (OQ-03 family) it replaces this function."""
    payload = json.dumps([_jsonable(row.get(c)) for c in columns], separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def fold(changesets: list[ChangeSet], tables: dict[TableRef, TableInfo], profile: OutputProfile) -> dict[TableRef, TableBatch]:
    """Last operation per key wins; TRUNCATE discards earlier rows of that table in the batch."""
    out: dict[TableRef, TableBatch] = {}
    latest: dict[TableRef, dict[tuple[Any, ...], dict[str, Any]]] = {}
    seq = 0
    for cs in changesets:
        for ev in cs.events:
            if ev.table is None or ev.kind not in (ChangeKind.INSERT, ChangeKind.UPDATE, ChangeKind.DELETE,
                                                   ChangeKind.TRUNCATE):
                continue
            info = tables[ev.table]
            tb = out.setdefault(ev.table, TableBatch(ev.table))
            ts = ev.commit_ts or cs.commit_ts
            if ts is not None:
                tb.min_commit_ts = ts if tb.min_commit_ts is None else min(tb.min_commit_ts, ts)
                tb.max_commit_ts = ts if tb.max_commit_ts is None else max(tb.max_commit_ts, ts)
            rows = latest.setdefault(ev.table, {})
            if ev.kind == ChangeKind.TRUNCATE:
                tb.truncated = True
                rows.clear()
                continue
            data_cols = [c.name for c in sorted(info.columns, key=lambda c: c.ordinal)]
            keys = [c.name for c in info.key_columns()]
            for op, image in _ops(ev.kind, ev.before, ev.after, keys):
                seq += 1
                present = frozenset(c for c in data_cols if c in image)
                if not keys and present != frozenset(data_cols):
                    raise ValueError(f"{ev.table.fqn}: keyless_partial_image - keyless table needs full row images "
                                     f"(missing {sorted(set(data_cols) - present)[:5]}) at {ev.position.text}")
                row = {c: image.get(c) for c in data_cols}
                if keys:
                    if any(row[c] is None for c in keys):
                        raise ValueError(f"{ev.table.fqn}: event without full key at {ev.position.text}")
                    k = tuple(row[c] for c in keys)
                else:
                    row[profile.id_column] = surrogate_id(row, data_cols)
                    k = (row[profile.id_column],)
                prev = rows.get(k)
                if op == "I" and prev is not None and prev["_op"] == "D":
                    op = "U"
                if op != "D" and present != frozenset(data_cols) and prev is not None and prev["_op"] != "D":
                    # partial image over an earlier image of this batch: keep the earlier values (X-29)
                    row = {**prev, **{c: image[c] for c in present}}
                    present = present | prev["_present"]
                row["_op"], row["_seq"], row["_present"] = op, seq, present
                rows[k] = row
    for ref, tb in out.items():
        tb.rows = sorted(latest.get(ref, {}).values(), key=lambda r: r["_seq"])
        tb.inserts = sum(r["_op"] == "I" for r in tb.rows)
        tb.updates = sum(r["_op"] == "U" for r in tb.rows)
        tb.deletes = sum(r["_op"] == "D" for r in tb.rows)
    return out


def _ops(kind: ChangeKind, before: dict[str, Any] | None, after: dict[str, Any] | None,
         keys: list[str]) -> list[tuple[str, dict[str, Any]]]:
    if kind == ChangeKind.INSERT:
        return [("I", after or {})]
    if kind == ChangeKind.DELETE:
        return [("D", before or after or {})]
    # UPDATE: identity change (keyless table, or primary-key update) => delete old image, upsert new image
    if before is not None and (not keys or any(before.get(k) != (after or {}).get(k) for k in keys)):
        return [("D", before), ("I" if not keys else "U", after or {})]
    return [("U", after or {})]
