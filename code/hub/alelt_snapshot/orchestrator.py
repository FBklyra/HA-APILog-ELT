"""Snapshot orchestrator: NEW -> SNAPSHOTTING -> CATCHING_UP -> IN_SYNC (-> RESYNC), via SPIs only.

Method (prompt 07): record start position, keep capture running, load key-range chunks inside the off-peak window,
then the loader replays the table's changes from start_pos (MERGE is idempotent per key) and alelt_verify reconciles
before IN_SYNC. While a table is SNAPSHOTTING the loader must hold its changes (`holds_changes`).
"""
from __future__ import annotations

import uuid
from collections.abc import Callable, Iterable, Iterator
from datetime import datetime
from typing import Any, Protocol

from alelt_common.models import Command, CommandName, Position, SnapshotChunkHeader, TableInfo
from alelt_common.spi import DestinationConnector, SourceConnector
from alelt_common.util import utcnow

from .planner import KeyRange, plan
from .state import ChunkStatus, JobStore, SnapshotJob, TableState
from .window import OffPeakWindow


class ChunkRunner(Protocol):
    def run(self, job: SnapshotJob, info: TableInfo, rng: KeyRange) -> Iterable[dict[str, Any]] | None:
        """Rows synchronously (remote) or None when rows arrive later via on_chunk (edge)."""


class RemoteChunkRunner:
    """Hub-side reader: SourceConnector.snapshot_rows."""

    def __init__(self, source: SourceConnector) -> None:
        self.source = source

    def run(self, job: SnapshotJob, info: TableInfo, rng: KeyRange) -> Iterator[dict[str, Any]]:
        chunk_rows = job.plan.chunk_rows if job.plan else 50_000
        for page in self.source.snapshot_rows(info, (rng.lo, rng.hi), chunk_rows):
            yield from page


EDGE_SNAPSHOT_ARGS = ("table", "snapshot_id", "chunk_rows", "key_from", "key_to")   # = C agent command.c


class EdgeChunkRunner:
    """Edge agent: whitelisted SNAPSHOT_TABLE command (R5); rows come back through POST /agent/v1/snapshot.

    Sends exactly the agents' argument set (EDGE_SNAPSHOT_ARGS, XC-14): the agent pages the WHOLE table itself
    (keyset paging, WITH CS), so planner ranges are not sent (the planner's half-open [lo, hi) ranges have no exact
    form in the agents' (key_from exclusive, key_to inclusive] contract); the isolation is the agent's (CS, F-04),
    never chosen by the hub. Chunks after the first dispatch nothing new: the agent streams them."""

    def __init__(self, send: Callable[[Command], None], clock: Callable[[], datetime] = utcnow) -> None:
        self.send, self.clock = send, clock

    def run(self, job: SnapshotJob, info: TableInfo, rng: KeyRange) -> None:
        schema, _, name = job.table.partition(".")
        if rng.chunk_no == 0:
            self.send(Command(command_id=uuid.uuid4(), name=CommandName.SNAPSHOT_TABLE, issued_at=self.clock(), args={
                "snapshot_id": str(job.snapshot_id), "table": {"schema": schema, "name": name},
                "chunk_rows": job.plan.chunk_rows if job.plan else 50_000}))
        return None


class SnapshotOrchestrator:
    def __init__(self, dest: DestinationConnector, runner: ChunkRunner, store: JobStore, window: OffPeakWindow,
                 probe: Callable[[], Position], clock: Callable[[], datetime] = utcnow,
                 on_catchup: Callable[[SnapshotJob], None] = lambda job: None,
                 compare: Callable[[Position, Position], int] | None = None) -> None:
        self.dest, self.runner, self.store, self.window = dest, runner, store, window
        self.compare = compare
        self.probe, self.clock, self.on_catchup = probe, clock, on_catchup
        self.jobs: dict[str, SnapshotJob] = {}
        self.infos: dict[str, TableInfo] = {}

    # ------------------------------------------------------------ lifecycle
    def recover(self, infos: Iterable[TableInfo]) -> list[SnapshotJob]:
        """After a hub restart: reload jobs; in-flight chunks are re-dispatched (re-load is idempotent)."""
        self.infos.update({i.table.fqn: i for i in infos})
        for job in self.store.load_all():
            for k, s in job.chunks.items():
                if s == ChunkStatus.DISPATCHED:
                    job.chunks[k] = ChunkStatus.PENDING
            self.jobs[job.table] = job
            self.store.save(job)
        return list(self.jobs.values())

    def start(self, info: TableInfo, dest_name: str, cardinality: int, chunk_rows: int = 50_000,
              **plan_kw: Any) -> SnapshotJob:
        job = self.jobs.get(info.table.fqn) or SnapshotJob(table=info.table.fqn, dest_name=dest_name)
        self.infos[job.table] = info
        self._begin(job, info, cardinality, chunk_rows, **plan_kw)
        return job

    def resync(self, table: str, cardinality: int, chunk_rows: int = 50_000, **plan_kw: Any) -> SnapshotJob:
        job = self.jobs[table]
        job.move(TableState.RESYNC, self.clock())
        job.snapshot_id = uuid.uuid4()
        self._begin(job, self.infos[table], cardinality, chunk_rows, **plan_kw)
        return job

    def _begin(self, job: SnapshotJob, info: TableInfo, cardinality: int, chunk_rows: int, **plan_kw: Any) -> None:
        job.start_pos = self.probe()            # capture keeps running from here
        job.plan = plan(info, cardinality, chunk_rows, **plan_kw)
        job.chunks = {r.chunk_no: ChunkStatus.PENDING for r in job.plan.ranges}
        job.rows_loaded, job.last_error = 0, None
        job.move(TableState.SNAPSHOTTING, self.clock())
        self.jobs[job.table] = job
        self.store.save(job)

    def pause(self, table: str) -> None:
        self.jobs[table].paused = True
        self.store.save(self.jobs[table])

    def resume(self, table: str) -> None:
        self.jobs[table].paused = False
        self.store.save(self.jobs[table])

    def holds_changes(self, table: str) -> bool:
        job = self.jobs.get(table)
        return bool(job and job.state in (TableState.SNAPSHOTTING, TableState.RESYNC))

    # ------------------------------------------------------------ work
    def step(self, table: str) -> str:
        """Advance one chunk. Returns: paused | outside_window | waiting | dispatched | loaded | complete | idle."""
        job = self.jobs[table]
        if job.state != TableState.SNAPSHOTTING:
            return "idle"
        if job.paused:
            return "paused"
        if not self.window.contains(self.clock()):
            return "outside_window"
        if any(s == ChunkStatus.DISPATCHED for s in job.chunks.values()):
            return "waiting"
        pending = sorted(k for k, s in job.chunks.items() if s == ChunkStatus.PENDING)
        if not pending:
            return self._complete(job)
        assert job.plan is not None
        rng = job.plan.ranges[pending[0]]
        rows = self.runner.run(job, self.infos[table], rng)
        if rows is None:
            job.chunks[rng.chunk_no] = ChunkStatus.DISPATCHED
            self.store.save(job)
            return "dispatched"
        try:
            job.rows_loaded += self.dest.load_snapshot(job.dest_name, self.infos[table], rows)
        except Exception as e:
            job.last_error = f"chunk {rng.chunk_no}: {e}"
            self.store.save(job)
            raise
        self._note_read_position(job)
        job.chunks[rng.chunk_no] = ChunkStatus.DONE
        self.store.save(job)
        return "loaded" if len(pending) > 1 else self._complete(job)

    def _note_read_position(self, job: SnapshotJob) -> None:
        """Keep the earliest read position over the chunks (each chunk may be its own consistent read)."""
        rp = getattr(self.runner, "read_position", None)
        p = rp(job) if rp is not None else None
        if p is None or self.compare is None:
            return
        if job.read_from is None or self.compare(p, job.read_from) < 0:
            job.read_from = p

    def on_chunk(self, header: SnapshotChunkHeader, rows: Iterable[dict[str, Any]]) -> None:
        """Edge snapshot rows received; `last` closes the dispatched range."""
        job = self.jobs[header.table.fqn]
        if header.snapshot_id != job.snapshot_id or job.state != TableState.SNAPSHOTTING:
            raise ValueError(f"stale snapshot chunk for {job.table}")
        if job.chunks.get(header.chunk_no) == ChunkStatus.DONE:
            return
        job.rows_loaded += self.dest.load_snapshot(job.dest_name, self.infos[job.table], rows)
        if header.last:
            job.chunks[header.chunk_no] = ChunkStatus.DONE
            if all(s == ChunkStatus.DONE for s in job.chunks.values()):
                self._complete(job)
                return
        self.store.save(job)

    def _complete(self, job: SnapshotJob) -> str:
        if (job.read_from is not None and job.start_pos is not None and self.compare is not None
                and self.compare(job.read_from, job.start_pos) > 0):
            # every change up to the earliest chunk read is in the snapshot rows: replaying them again would apply
            # them twice (keyless duplicates, SRC-cap-03); the source position is <= its consistent read point
            job.start_pos = job.read_from
        job.move(TableState.CATCHING_UP, self.clock())
        self.store.save(job)
        self.on_catchup(job)                    # loader replays from job.start_pos
        return "complete"

    def caught_up(self, table: str, verified: bool, detail: str | None = None) -> TableState:
        """Called after catch-up + alelt_verify reconciliation; IN_SYNC only with a clean recon."""
        job = self.jobs[table]
        if verified:
            job.move(TableState.IN_SYNC, self.clock())
            job.last_error = None
        else:
            job.last_error = detail or "reconciliation failed"
        self.store.save(job)
        return job.state
