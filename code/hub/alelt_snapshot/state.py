"""Per-table snapshot state machine and persisted job state."""
from __future__ import annotations

import json
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Protocol
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

from alelt_common.models import Position

from .planner import SnapshotPlan


class TableState(str, Enum):
    NEW = "NEW"
    SNAPSHOTTING = "SNAPSHOTTING"
    CATCHING_UP = "CATCHING_UP"
    IN_SYNC = "IN_SYNC"
    RESYNC = "RESYNC"


TRANSITIONS: dict[TableState, set[TableState]] = {
    TableState.NEW: {TableState.SNAPSHOTTING},
    TableState.SNAPSHOTTING: {TableState.CATCHING_UP, TableState.RESYNC},
    TableState.CATCHING_UP: {TableState.IN_SYNC, TableState.RESYNC},
    TableState.IN_SYNC: {TableState.RESYNC},
    TableState.RESYNC: {TableState.SNAPSHOTTING},
}


class InvalidTransition(RuntimeError):
    pass


class ChunkStatus(str, Enum):
    PENDING = "PENDING"
    DISPATCHED = "DISPATCHED"
    DONE = "DONE"


class SnapshotJob(BaseModel):
    model_config = ConfigDict(extra="forbid")

    table: str                                   # source fqn
    dest_name: str
    state: TableState = TableState.NEW
    snapshot_id: UUID = Field(default_factory=uuid4)
    start_pos: Position | None = None
    # earliest consistent-read position reported by the source over the chunks (SRC-cap-03): the catch-up starts there
    # instead of at the (earlier) position probed when the snapshot was requested
    read_from: Position | None = None
    plan: SnapshotPlan | None = None
    chunks: dict[int, ChunkStatus] = Field(default_factory=dict)
    rows_loaded: int = 0
    paused: bool = False
    last_error: str | None = None
    history: list[tuple[str, str, datetime]] = Field(default_factory=list)

    def move(self, to: TableState, at: datetime) -> None:
        if to not in TRANSITIONS[self.state]:
            raise InvalidTransition(f"{self.table}: {self.state.value} -> {to.value} not allowed")
        self.history.append((self.state.value, to.value, at))
        self.state = to

    @property
    def progress(self) -> float:
        return sum(s == ChunkStatus.DONE for s in self.chunks.values()) / len(self.chunks) if self.chunks else 0.0


class JobStore(Protocol):
    def save(self, job: SnapshotJob) -> None: ...
    def load_all(self) -> list[SnapshotJob]: ...


class MemoryJobStore:
    def __init__(self) -> None:
        self.jobs: dict[str, str] = {}

    def save(self, job: SnapshotJob) -> None:
        self.jobs[job.table] = job.model_dump_json()

    def load_all(self) -> list[SnapshotJob]:
        return [SnapshotJob.model_validate_json(v) for _, v in sorted(self.jobs.items())]


class FileJobStore:
    """One JSON file per table, atomically replaced (survives hub restarts)."""

    def __init__(self, directory: str | Path) -> None:
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)

    def save(self, job: SnapshotJob) -> None:
        p = self.dir / f"{job.table}.json"
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(json.loads(job.model_dump_json()), indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(p)

    def load_all(self) -> list[SnapshotJob]:
        return [SnapshotJob.model_validate_json(p.read_text(encoding="utf-8")) for p in sorted(self.dir.glob("*.json"))]
