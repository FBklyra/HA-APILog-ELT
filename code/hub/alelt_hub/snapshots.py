"""Initial load / resync wired into the worker (alelt_snapshot, X-22 / X-30 / REQ-8 / REQ-14).

State lives in `table_hold` (kind snapshot) through `holds.DbSnapshotStore`, so every worker sees the hold.

Remote sources (hub-side capture): SnapshotOrchestrator + RemoteChunkRunner read `SourceConnector.snapshot_rows` from a
live connector (settings + secret reference of the pipeline / ALELT_CAPTURE_CONNECTIONS); rows keep their Python types
(bytes, Decimal, dates, timestamps) and go through the column policy before `load_snapshot`; values the connector could
not convert are quarantined (R7). The start position is recorded as the runner's first-start position (REQ-21).

Edge sources (agent): one whitelisted SNAPSHOT_TABLE{table, snapshot_id, chunk_rows} command per table (R5); the agent
pages the table and posts chunks to /agent/v1/snapshot; the normal snapshot_load job loads them and
`edge_chunk_loaded` closes the job on the `last` chunk. Start position = hub durable position (<= agent read point).

Window: tables.yaml `snapshot_window: {start, end, tz}` -> OffPeakWindow (REQ-8). After the last chunk the table goes
CATCHING_UP (hold released); the next decode job replays it from start_pos under the pipeline lock (catch-up) and
reconciles it with alelt_verify before IN_SYNC (remote: read live; edge: a POST_SNAPSHOT recon run through the agent's
VERIFY_TABLE is queued, its MATCH moves the table to IN_SYNC - X-123). Edge snapshot chunks are read WITH CS (currently
committed, F-04): no uncommitted row is ever loaded.
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import Engine, select

from alelt_common.canonical_jsonl import Undecodable, canonical_snapshot_row, snapshot_types
from alelt_common.models import CommandName, Position, SnapshotChunkHeader, TableInfo, TableRef
from alelt_common.util import utcnow
from alelt_snapshot import OffPeakWindow, SnapshotOrchestrator, TableState
from alelt_snapshot.state import ChunkStatus, SnapshotJob

from . import holds, jobs, store
from .db import agent, agent_command, batch

log = logging.getLogger("alelt_hub.snapshots")
ACTOR = "system:snapshot"


class PipelineBusy(RuntimeError):
    """The pipeline lock could not be taken to place a snapshot hold synchronously (HC-19): retry later."""


def chunk_rows_default() -> int:
    return int(os.environ.get("ALELT_SNAPSHOT_CHUNK_ROWS", "50000"))


# ------------------------------------------------------------------ adapters
class PolicyDest:
    """orchestrator.dest: column policy + quarantine of unconvertible values, then the real destination."""

    def __init__(self, pipeline: Any) -> None:
        self.p = pipeline

    def load_snapshot(self, dest_name: str, info: TableInfo, rows: Any) -> int:
        """Load in pages of ALELT_SNAPSHOT_CHUNK_ROWS (a remote range may be the whole table)."""
        import itertools

        it = iter(rows)
        n = 0
        while True:
            page = list(itertools.islice(it, chunk_rows_default()))
            if not page:
                return n
            n += self._page(dest_name, info, page)

    def _page(self, dest_name: str, info: TableInfo, rows: list[dict[str, Any]]) -> int:
        types = snapshot_types(info)             # CDC value forms (JSON = text); bad objects -> Undecodable (R7)
        rows = [canonical_snapshot_row(r, types) for r in rows]
        bad = [r for r in rows if any(isinstance(v, Undecodable) for v in r.values())]
        if bad:
            from alelt_capture.snapshot import _quarantine

            keys = [c.name for c in info.key_columns()]
            _quarantine(self.p.engine, self.p.cid, info.table, uuid.uuid4(), None, keys, bad)
            drop = {id(r) for r in bad}
            rows = [r for r in rows if id(r) not in drop]
        dinfo = self.p.policy.project(info)
        out = [self.p.policy.apply_row(info.table, r) or {} for r in rows]
        self.p.dest.ensure_table(dinfo, dest_name, None)
        self.p.dest.load_snapshot(dest_name, dinfo, out)
        return len(out)


class EdgeRunner:
    """SNAPSHOT_TABLE with the argument set the agents accept (table, snapshot_id, chunk_rows; the agent pages the
    whole table). run() returns None: rows arrive later through POST /agent/v1/snapshot."""

    def __init__(self, engine: Engine, agent_id: str) -> None:
        self.engine, self.agent_id = engine, agent_id


class RemoteRunner:
    """Hub-side reader over `SourceConnector.snapshot_rows` (unbounded range => key_range None)."""

    def __init__(self, source: Any) -> None:
        self.source = source

    def run(self, job: SnapshotJob, info: TableInfo, rng: Any) -> Any:
        kr = None if rng.lo is None and rng.hi is None else (rng.lo, rng.hi)
        n = job.plan.chunk_rows if job.plan else chunk_rows_default()
        for page in self.source.snapshot_rows(info, kr, n):
            yield from page

    def read_position(self, job: SnapshotJob) -> Position | None:
        """Position of the consistent read the source just used for this table (MySQL: low watermark taken before
        the snapshot, MariaDB: exact binlog_snapshot position), when the connector reports one (SRC-cap-03)."""
        d = (getattr(self.source, "snapshot_positions", None) or {}).get(job.table)
        if not d or not d.get("position"):
            return None
        try:
            return self.source.parse_position(str(d["position"]))  # type: ignore[no-any-return]
        except Exception:  # noqa: BLE001 - unknown form: keep the probed start position
            return None


# ------------------------------------------------------------------ service
class SnapshotService:
    def __init__(self, engine: Engine, pipeline: Any) -> None:
        self.engine, self.p, self.cid = engine, pipeline, pipeline.cid
        self.store = holds.DbSnapshotStore(engine, self.cid)

    @property
    def remote(self) -> bool:
        return getattr(self.p.source, "capture_mode", "edge") == "remote"

    def window(self) -> OffPeakWindow:
        w = self.p.policy.snapshot_window
        if not w or os.environ.get("ALELT_SNAPSHOT_IGNORE_WINDOW", "").strip().lower() in ("1", "true", "yes"):
            return OffPeakWindow.always()
        return OffPeakWindow.parse(str(w["start"]), str(w["end"]), str(w.get("tz") or "UTC"))

    def _live_source(self) -> Any:
        from alelt_capture.config import connections

        rc = connections(self.engine).get(self.cid)
        if rc is None:
            raise LookupError(f"{self.cid}: no live source settings (pipeline definition or ALELT_CAPTURE_CONNECTIONS)")
        return rc.factory()()

    def _edge_agent(self) -> str | None:
        """Agent that runs SNAPSHOT_TABLE: the live capture-lease holder when the connection is active/passive (a
        standby, e.g. on the HADR standby host, cannot read the table - HA-05), else the most recent heartbeat."""
        from . import ha

        with self.engine.connect() as c:
            h = ha.holder(c, self.cid)
            if h is not None:
                return h
            r = c.execute(select(agent.c.id).where(agent.c.connection_id == self.cid,
                                                   ~agent.c.id.like("capture:%"))
                          .order_by(agent.c.last_heartbeat.desc()).limit(1)).first()
        return str(r[0]) if r else None

    def _hub_position(self) -> Position | None:
        """Snapshot start position of an edge connection: the hub's chain position (last received batch). Before the
        first batch (idle source, XC-13): the position the capturing agent reports in its heartbeat (durable_pos,
        else last read position) - nothing before it was shipped, so the catch-up from there misses nothing."""
        with self.engine.connect() as c:
            r = c.execute(select(batch.c.next_lri, batch.c.to_lri).where(batch.c.connection_id == self.cid)
                          .order_by(batch.c.seq.desc()).limit(1)).first()
            if r:
                return Position(engine=self.p.engine_name, text=r[0] or r[1])
            fresh = utcnow() - timedelta(seconds=float(os.environ.get("ALELT_SNAPSHOT_AGENT_POS_MAX_AGE_S", "300")))
            for dur, last, hb in c.execute(select(agent.c.durable_pos, agent.c.last_lri, agent.c.last_heartbeat)
                                           .where(agent.c.connection_id == self.cid, agent.c.status == "CAPTURING")
                                           .order_by(agent.c.last_heartbeat.desc())):
                hb = hb if hb is None or hb.tzinfo else hb.replace(tzinfo=fresh.tzinfo)
                if hb is not None and hb >= fresh and (dur or last):
                    return Position(engine=self.p.engine_name, text=str(dur or last))
        return None

    def _edge_start_position(self) -> Position | None:
        """HC-19: an edge snapshot hold starts at what the main stream FLUSHED (taken under the pipeline lock, so no
        decode run can apply the table after it); before the first flush: the chain / agent position."""
        flushed = self.p.flushed_position()
        if flushed:
            return Position(engine=self.p.engine_name, text=flushed)
        return self._hub_position()

    def _pipeline_lock(self) -> Any:
        return self._lock_for(self.engine, self.cid)

    @staticmethod
    def _lock_for(engine: Engine, cid: str) -> Any:
        """The pipeline lock for placing holds. Cooperative yield (RC4): a `pipeline_request` row asks the running
        decode slice to end at its next chunk, so the wait is one chunk, not a whole slice."""
        from .dialect import try_lock

        lk = try_lock(engine, f"pipeline:{cid}")
        if lk is not None:
            return lk
        request_pipeline(engine, cid, "snapshot hold")
        try:
            deadline = time.monotonic() + float(os.environ.get("ALELT_SNAPSHOT_HOLD_LOCK_WAIT_S", "30"))
            while True:
                lk = try_lock(engine, f"pipeline:{cid}")
                if lk is not None:
                    return lk
                if time.monotonic() >= deadline:
                    raise PipelineBusy(f"{cid}: pipeline busy, hold not placed (retried)")
                time.sleep(0.1)
        finally:
            release_pipeline_request(engine, cid)

    def _infos(self) -> dict[str, TableInfo]:
        with self.engine.connect() as c:
            vid = store.latest_catalog_id(c, self.cid)
            cat = store.load_catalog(c, vid) if vid else None
        return {t.table.fqn: t for t in (cat.tables if cat else [])}

    def orchestrator(self, src: Any = None) -> SnapshotOrchestrator:
        svc = self

        class _Edge(EdgeRunner):
            def run(self, job: SnapshotJob, info: TableInfo, rng: Any) -> None:
                s, _, n = job.table.partition(".")
                with svc.engine.begin() as c:
                    cmd = store.issue_command(c, self.agent_id, CommandName.SNAPSHOT_TABLE, {
                        "table": {"schema": s, "name": n}, "snapshot_id": str(job.snapshot_id),
                        "chunk_rows": job.plan.chunk_rows if job.plan else 5000}, ACTOR, reason="initial load")
                svc._flags[job.table] = {"agent_command_id": str(cmd.command_id),
                                         "dispatched_at": utcnow().isoformat()}
                return None

        self._flags: dict[str, dict[str, Any]] = {}
        if self.remote:
            runner: Any = RemoteRunner(src) if src is not None else None
            probe = (lambda: src.current_position()) if src is not None else (lambda: None)
        else:
            aid = self._edge_agent()
            runner = _Edge(self.engine, aid) if aid else None
            probe = self._edge_start_position
        orch = SnapshotOrchestrator(PolicyDest(self.p), runner, _FlagStore(self), self.window(), probe,  # type: ignore[arg-type]
                                    compare=getattr(src if src is not None else self.p.source, "compare", None))
        orch.jobs = {j.table: j for j in self.store.load_all()}
        orch.infos.update(self._infos())
        return orch

    # ---------------------------------------------------------------- lifecycle
    def start(self, tables: list[str] | None = None, resync: bool = False, reason: str = "initial load",
              locked: bool = False) -> list[str]:
        """Begin (or restart with `resync`) the snapshot of `tables` (default: every enabled catalog table).
        Returns the tables started. Edge: needs a registered agent; remote: needs live settings.
        `locked`: the caller already holds the pipeline lock (decode job)."""
        infos = self._infos()
        names = tables or [f for f, i in infos.items() if self.p.policy.table(i.table).enabled]
        src = self._live_source() if self.remote else None
        started: list[str] = []
        from .dialect import unlock

        lock = None
        try:
            # HC-19: the hold is placed under the pipeline lock: no decode run applies the table between the start
            # position and the hold becoming effective (the next decode run refreshes holds before it starts)
            lock = None if locked else self._pipeline_lock()
            if not self.remote and self._edge_start_position() is None:
                raise RuntimeError("no start position yet (no batch and no agent heartbeat position): retried")
            orch = self.orchestrator(src)
            for fqn in names:
                info = infos.get(fqn)
                if info is None:
                    raise LookupError(f"{fqn} not in the catalog of {self.cid}")
                job = orch.jobs.get(fqn)
                if job and job.state in (TableState.SNAPSHOTTING, TableState.RESYNC) and not resync                         and job.start_pos is not None:
                    continue
                if job and job.start_pos is None:            # a start that never got a position: start over
                    job = None
                    orch.jobs.pop(fqn, None)
                if job is None or job.state == TableState.NEW:
                    orch.start(info, self.p.policy.dest_name(info.table), 0, chunk_rows_default())
                elif job.state != TableState.RESYNC:
                    orch.resync(fqn, 0, chunk_rows_default())
                job = orch.jobs[fqn]
                if job.start_pos is None:
                    raise RuntimeError(f"{fqn}: no start position (source/hub position unknown yet)")
                from . import integrity, lifecycle
                from .completion import drop_table

                drop_table(self.engine, self.cid, fqn)   # the snapshot carries every value: pending completions void
                rseq = integrity.restart_for_new_hold(self.p, fqn)
                if rseq is not None:                     # catch-up scan start (HC-09)
                    holds.set_body(self.engine, self.cid, fqn, "snapshot", restart_seq=rseq)
                lc = lifecycle.get(self.engine, self.cid, fqn)
                lifecycle.move(self.engine, self.cid, fqn, lifecycle.RESYNC if resync else lifecycle.INITIAL_LOAD,
                               snapshot_id=str(job.snapshot_id), start_pos=job.start_pos.text, verified_at=None,
                               resync_count=int((lc or {}).get("resync_count") or 0) + (1 if resync else 0),
                               **({} if lc else {"mode": "auto"}))
                if self.remote:
                    from alelt_capture.receipt import record_start_pos

                    record_start_pos(self.engine, self.cid, job.start_pos, "snapshot")
                started.append(fqn)
        finally:
            if lock is not None:
                unlock(lock, f"pipeline:{self.cid}")
            if src is not None:
                src.close()
        if started:
            with self.engine.begin() as c:
                jobs.enqueue(c, "snapshot", {"connection_id": self.cid}, dedup_key=f"snapshot:{self.cid}:{uuid.uuid4()}")
                store.audit(c, ACTOR, "snapshot.start", self.cid, {"tables": started, "resync": resync,
                                                                    "reason": reason})
        return started

    def step(self) -> float | None:
        """Advance every SNAPSHOTTING table one chunk. Returns seconds until the next step, None when nothing left."""
        jobs_ = {j.table: j for j in self.store.load_all()}
        active = [t for t, j in jobs_.items() if j.state == TableState.SNAPSHOTTING]
        if not active:
            return None
        from .lifecycle import _class_rank

        active.sort(key=lambda t: (_class_rank(self.p.policy, t), t))   # stream -> deferred -> window (ADR-012)
        win = self.window()
        if not win.contains(utcnow()):
            return max(5.0, (win.next_open(utcnow()) - utcnow()).total_seconds())
        src = self._live_source() if self.remote else None
        delay: float | None = None
        try:
            orch = self.orchestrator(src)
            if not self.remote and orch.runner is None:
                return 30.0                            # no agent registered yet
            for t in active:
                if not self.remote and self._edge_failed(t):
                    continue
                r = orch.step(t)
                if r in ("dispatched", "waiting"):
                    delay = 30.0 if delay is None else min(delay, 30.0)
                elif r == "loaded":
                    delay = 0.0
                elif r == "outside_window":
                    delay = 60.0
        finally:
            if src is not None:
                src.close()
        if any(j.state == TableState.CATCHING_UP for j in self.store.load_all()):
            from . import lifecycle

            lifecycle.wake(self.engine, self.cid)      # CATCH_UP: one replay pass (lifecycle.drive)
        return delay

    def _edge_failed(self, fqn: str) -> bool:
        """An agent that refused SNAPSHOT_TABLE (ack ok:false): job paused with the agent's message."""
        fl = self.store.flags(fqn)
        cid_ = fl.get("agent_command_id")
        if not cid_:
            return False
        with self.engine.connect() as c:
            r = c.execute(select(agent_command.c.status, agent_command.c.result)
                          .where(agent_command.c.id == uuid.UUID(cid_))).first()
        if r is None or r[0] != "FAILED":
            return False
        job = next(j for j in self.store.load_all() if j.table == fqn)
        if not job.paused:
            job.paused = True
            job.last_error = f"agent refused SNAPSHOT_TABLE: {(r[1] or {}).get('message') or (r[1] or {}).get('error') or r[1]}"
            self.store.save(job)
        return True

    def _caught_up(self, fqn: str, verified: bool, detail: str | None) -> None:
        orch = SnapshotOrchestrator(PolicyDest(self.p), None, _FlagStore(self), self.window(),  # type: ignore[arg-type]
                                    lambda: None)  # type: ignore[arg-type,return-value]
        orch._flags = {}  # type: ignore[attr-defined]
        orch.jobs = {j.table: j for j in self.store.load_all()}
        if fqn in orch.jobs and orch.jobs[fqn].state == TableState.CATCHING_UP:
            orch.caught_up(fqn, verified, detail)

    def reconciled(self, fqn: str, outcome: str, detail: str) -> None:
        """Recon result for a table (verify job): MATCH moves a caught-up snapshot to IN_SYNC and sets the lifecycle
        `verified_at` (required by certification)."""
        if outcome == "MATCH" and self.store.flags(fqn).get("catchup_done"):
            self._caught_up(fqn, True, detail)
            from . import lifecycle

            lifecycle.verified(self.engine, self.cid, fqn, detail)


class _FlagStore:
    """JobStore that keeps service flags (agent command id, catch-up marker) next to the job."""

    def __init__(self, svc: SnapshotService) -> None:
        self.svc = svc

    def save(self, job: SnapshotJob) -> None:
        self.svc.store.save(job)
        extra = getattr(self.svc, "_flags", {}).pop(job.table, None)
        if extra:
            holds.set_body(self.svc.engine, self.svc.cid, job.table, "snapshot", **extra)

    def load_all(self) -> list[SnapshotJob]:
        return self.svc.store.load_all()


def request_pipeline(engine: Engine, cid: str, reason: str) -> None:
    from .db import pipeline_request
    from .dialect import insert

    vals = {"requested_by": f"{os.getpid()}", "reason": reason[:200], "requested_at": utcnow()}
    with engine.begin() as c:
        c.execute(insert(pipeline_request).values(connection_id=cid, **vals)
                  .on_conflict_do_update(index_elements=["connection_id"], set_=vals))


def release_pipeline_request(engine: Engine, cid: str) -> None:
    from .db import pipeline_request

    with engine.begin() as c:
        c.execute(pipeline_request.delete().where(pipeline_request.c.connection_id == cid))


# ------------------------------------------------------------------ edge chunks (snapshot_load job)
def edge_chunk_loaded(engine: Engine, connection_id: str, hdr: SnapshotChunkHeader, compare: Any = None) -> bool:
    """A chunk of an orchestrated edge snapshot was loaded. Completion is driven by `snapshot_chunk.status` (HC-03):
    the table goes CATCHING_UP only when the `last` chunk exists AND every chunk_no up to it is LOADED, whatever order
    the snapshot_load jobs finished in; the hold row is updated under a row lock (no lost update between concurrent
    chunk loads). Returns True when the chunk belonged to an orchestrated job."""
    import json

    from sqlalchemy import update as sa_update

    from .db import snapshot_chunk, table_hold

    key = (table_hold.c.connection_id == connection_id, table_hold.c.table_fqn == hdr.table.fqn,
           table_hold.c.kind == "snapshot")
    closed = False
    with engine.begin() as c:
        cur = c.execute(select(table_hold).where(*key).with_for_update()).mappings().first()
        if cur is None:
            return False
        body = dict(cur["body"] or {})
        job = SnapshotJob.model_validate({k: v for k, v in body.items() if k in SnapshotJob.model_fields})
        if job.snapshot_id != hdr.snapshot_id or job.state != TableState.SNAPSHOTTING:
            return False
        chunks = c.execute(select(snapshot_chunk.c.chunk_no, snapshot_chunk.c.rows, snapshot_chunk.c.last,
                                  snapshot_chunk.c.status).where(snapshot_chunk.c.snapshot_id == hdr.snapshot_id)).all()
        loaded = {int(n): int(r) for n, r, _l, st in chunks if st == "LOADED"}
        last_no = next((int(n) for n, _r, la, _st in chunks if la), None)
        job.rows_loaded = sum(loaded.values())
        if last_no is not None and all(n in loaded for n in range(0, last_no + 1)):
            # the agent's log position when its read began (chunk header start_pos) is <= its read point: every
            # change before it is in the rows, so the catch-up starts there (not at the older hub position)
            if (compare is not None and hdr.start_pos is not None and job.start_pos is not None
                    and hdr.start_pos.engine == job.start_pos.engine):
                try:
                    if compare(hdr.start_pos, job.start_pos) > 0:
                        job.start_pos = hdr.start_pos
                except Exception:  # noqa: BLE001 - unparseable agent position: keep the hub start position
                    pass
            for k, st_ in job.chunks.items():
                if st_ != ChunkStatus.DONE:
                    job.chunks[k] = ChunkStatus.DONE
            job.move(TableState.CATCHING_UP, utcnow())
            closed = True
        new = json.loads(job.model_dump_json())
        for k, v in body.items():                  # service flags kept (agent command id, verify note, ...)
            new.setdefault(k, v)
        c.execute(sa_update(table_hold).where(*key).values(
            body=new, state=job.state.value, held=holds.snapshot_held(job.state.value, new),
            start_pos=job.start_pos.text if job.start_pos else None, updated_at=utcnow()))
        if closed:
            from . import lifecycle

            lifecycle.wake(c, connection_id)            # CATCH_UP: one replay pass (lifecycle.drive)
    return True


__all__ = ["PipelineBusy", "SnapshotService", "edge_chunk_loaded", "TableRef"]
