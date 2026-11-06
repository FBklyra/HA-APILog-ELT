"""Reconciliation evidence records (ALELT_CONTROL.RECON_RESULT / hub metadata)."""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field


class CheckName(str, Enum):
    ROW_COUNT = "ROW_COUNT"
    KEY_SET = "KEY_SET"
    COLUMN_CHECKSUM = "COLUMN_CHECKSUM"
    COLUMN_AGGREGATES = "COLUMN_AGGREGATES"
    SAMPLED_ROWS = "SAMPLED_ROWS"
    DELETES = "DELETES"
    LATENCY = "LATENCY"
    DBT_SHADOW = "DBT_SHADOW"
    ROW_MULTISET = "ROW_MULTISET"       # keyless tables (F-10): count per identical row, never a key set


class Status(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    ERROR = "ERROR"


class ReconResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recon_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    run_id: str
    table: str
    check: CheckName
    status: Status
    column: str | None = None
    key: str | None = None
    bucket: int | None = None
    pos_from: str | None = None
    pos_to: str | None = None
    raw_ref: str | None = None
    expected: str | None = None
    actual: str | None = None
    detail: dict[str, Any] = Field(default_factory=dict)
    recorded_at: datetime

    def snowflake_row(self) -> tuple[Any, ...]:
        """Values for ALELT_CONTROL.RECON_RESULT (column order of migration 003)."""
        return (self.recon_id, self.table, self.check.value, self.status.value, self.column, self.key, self.bucket,
                self.pos_from, self.pos_to, self.raw_ref, self.expected, self.actual,
                json.dumps({"run_id": self.run_id, **self.detail}, sort_keys=True, default=str), self.recorded_at)


class ReconStore(Protocol):
    def add(self, results: list[ReconResult]) -> None: ...
    def for_table(self, table: str, since: datetime) -> list[ReconResult]: ...


class MemoryReconStore:
    def __init__(self) -> None:
        self.results: list[ReconResult] = []

    def add(self, results: list[ReconResult]) -> None:
        self.results.extend(results)

    def for_table(self, table: str, since: datetime) -> list[ReconResult]:
        return [r for r in self.results if r.table == table and r.recorded_at >= since]
