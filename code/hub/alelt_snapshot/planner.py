"""Snapshot planner: primary-key ranges from catalog cardinality (pure)."""
from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

from pydantic import BaseModel, ConfigDict

from alelt_common.models import CanonicalType, TableInfo

_NUMERIC = {CanonicalType.SHORT, CanonicalType.INT, CanonicalType.LONG, CanonicalType.DECIMAL}


class KeyRange(BaseModel):
    """Half-open key range [lo, hi); None = unbounded. Values are key tuples as lists."""

    model_config = ConfigDict(extra="forbid")
    chunk_no: int
    lo: list[Any] | None = None
    hi: list[Any] | None = None
    est_rows: int = 0


class SnapshotPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")
    table: str
    key_columns: list[str]
    chunk_rows: int
    cardinality: int
    ranges: list[KeyRange]


def plan(table: TableInfo, cardinality: int, chunk_rows: int = 50_000,
         key_min: Any = None, key_max: Any = None, boundaries: Sequence[Sequence[Any]] | None = None) -> SnapshotPlan:
    """Choose ranges:
    - explicit `boundaries` (sorted sampled keys, e.g. from a PROBE) => ranges between them;
    - single numeric key with min/max => equal-width ranges, ceil(cardinality/chunk_rows) of them;
    - otherwise (composite / text keys, keyless) => one open range; the edge pages it by key with chunk_rows."""
    if chunk_rows < 1:
        raise ValueError("chunk_rows must be >= 1")
    keys = table.key_columns()
    names = [k.name for k in keys]
    n = max(1, math.ceil(max(cardinality, 0) / chunk_rows))
    ranges: list[KeyRange]
    if boundaries:
        pts = [list(b) for b in boundaries]
        if pts != sorted(pts):
            raise ValueError("boundaries must be sorted")
        edges: list[list[Any] | None] = [None, *pts, None]
        est = max(1, cardinality // (len(pts) + 1))
        ranges = [KeyRange(chunk_no=i, lo=edges[i], hi=edges[i + 1], est_rows=est) for i in range(len(edges) - 1)]
    elif (len(keys) == 1 and keys[0].canonical_type in _NUMERIC and key_min is not None and key_max is not None
          and n > 1 and key_max > key_min):
        lo, hi = int(key_min), int(key_max) + 1
        width = max(1, math.ceil((hi - lo) / n))
        starts = list(range(lo, hi, width))
        ranges = [KeyRange(chunk_no=i, lo=None if i == 0 else [s],
                           hi=None if i == len(starts) - 1 else [starts[i + 1]],
                           est_rows=max(1, cardinality // len(starts))) for i, s in enumerate(starts)]
    else:
        ranges = [KeyRange(chunk_no=0, est_rows=max(cardinality, 0))]
    return SnapshotPlan(table=table.table.fqn, key_columns=names, chunk_rows=chunk_rows,
                        cardinality=max(cardinality, 0), ranges=ranges)
