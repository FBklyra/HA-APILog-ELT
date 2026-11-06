"""Worker process: `python -m alelt_hub.worker` - two claim loops (lanes, RC4) over the metadata queue:

data lane (ALELT_WORKER_DATA_THREADS, default 1): decode (the per-connection "stream dirty" marker: completion step at
    the slice start, decode -> txn -> load in a time slice, schedule gates before every chunk, then the short attach
    tails of finished passes and the schedule tick), replay (bulk phase of a replay pass incl. the catch-up of an initial
    load, lock `replay:<cid>`, NOT the pipeline lock), snapshot_load (snapshot chunk files).
control lane (ALELT_WORKER_CONTROL_THREADS, default 1), by priority: integrity, completion (polls fetches, remote keyed
    reads), lifecycle (table lifecycle driver), snapshot (SNAPSHOT_TABLE dispatch / remote reads), schema (approved
    schema changes, destination DDL retries), verify, resync / reprocess, maintain.

Rule: under the pipeline lock nothing waits on a network peer, a destination DDL, or an unbounded replay. A control step
that needs the pipeline lock writes `pipeline_request(cid)`; the running slice ends at its next chunk (cooperative
yield). Each lane thread has its own pipeline objects (the stream state lives in the data lane; control steps rebuild
from metadata). Per-table holds come from `table_hold`, phases from `table_lifecycle`, shared by all workers.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import socket
import tempfile
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, select, update

from alelt_common.canonical_jsonl import Undecodable, canonical_snapshot_row, snapshot_types
from alelt_common.models import SnapshotChunkHeader, TableRef
from alelt_common.util import decompress, setup_logging

from alelt_diagnose import hooks as diag_hooks

from . import holds, integrity, jobs, lifecycle, ops, replay, schedule, snapshots, store
from .blobstore import read_ref
from .db import connection, get_engine, init_db, snapshot_chunk, table_hold
from .metrics import REGISTRY, serve
from .dialect import require_lock
from .pipeline import StreamPipeline, try_lock, unlock
from .settings import Settings, get_settings

log = logging.getLogger("alelt_hub.worker")
KINDS = ["decode", "snapshot_load", "snapshot", "resync", "reprocess", "verify", "replay", "integrity", "maintain",
         "completion", "lifecycle", "schema"]


class Worker:
    def __init__(self, engine: Engine, settings: Settings, worker_id: str | None = None,
                 hold: Callable[[TableRef], bool] | None = None) -> None:
        """`hold`: extra snapshot hold predicate (tests / embedding); `table_hold` rows are always honoured (X-22)."""
        self.hold = hold
        self.engine = engine
        self.s = settings
        self.id = worker_id or f"{socket.gethostname()}:{os.getpid()}"
        self._local = threading.local()
        self.stop = False

    @property
    def pipelines(self) -> dict[str, StreamPipeline]:
        """Pipeline objects of the calling lane thread (each lane has its own; state lives in metadata)."""
        d = getattr(self._local, "pipelines", None)
        if d is None:
            d = self._local.pipelines = {}
        return d

    def _hold_for(self, cid: str) -> Callable[[TableRef], bool] | None:
        return self.hold

    def pipeline(self, cid: str) -> StreamPipeline:
        p = self.pipelines.get(cid)
        if p is None:
            p = self.pipelines[cid] = StreamPipeline(self.engine, cid, self.s, hold=self._hold_for(cid))
            p.refresh_held()                           # p.held: one set object, evaluated by the loader at flush time
            p.on_chunk = lambda: schedule.ensure(p, p.held)   # holds effective from the first batch (X-125)
        return p

    def _refresh_held(self, cid: str) -> set[str]:
        return self.pipeline(cid).refresh_held()

    def run_once(self, lane: str | None = None) -> bool:
        """Claim and run one job of `lane` ('data' | 'control'); without a lane the lanes alternate (single-threaded
        callers: tests, `--once`), so a busy control lane never starves the stream and vice versa.
        Returns False when the queue (of the lane / of both lanes) is empty."""
        if lane is None:
            order = ("data", "control") if not getattr(self, "_flip", False) else ("control", "data")
            self._flip = not getattr(self, "_flip", False)
            j = None
            for ln in order:
                j = jobs.claim(self.engine, self.id, self._kinds(ln))
                if j is not None:
                    break
        else:
            j = jobs.claim(self.engine, self.id, self._kinds(lane))
        if j is None:
            return False
        try:
            with jobs.heartbeat(self.engine, j["id"], self.id):   # F-20 liveness
                self._run_kind(j)
            jobs.finish(self.engine, j["id"], self.id)
        except _Busy:
            jobs.fail(self.engine, j["id"], "pipeline busy (another worker owns the stream)", 0, worker_id=self.id)
        except Exception as e:  # noqa: BLE001 - job retried with backoff; batches stay spooled
            log.exception("job failed", extra={"component": "alelt_hub", "correlation_id": str(j["id"])})
            REGISTRY.inc("alelt_decode_errors_total", connection_id=j["payload"].get("connection_id", ""),
                         reason=type(e).__name__)
            dead = self.pipelines.pop(j["payload"].get("connection_id", ""), None)   # force rebuild on retry
            if dead is not None:
                dead.asm.reset()                                            # no leaked txn spill files (HC-30)
            jobs.fail(self.engine, j["id"], f"{type(e).__name__}: {e}", j["attempts"], worker_id=self.id)
            diag_hooks.job_failed(self.engine, j, e)                        # incident + Stage A (never raises)
        return True

    @staticmethod
    def _kinds(lane: str) -> tuple[str, ...]:
        return tuple(k for k in jobs.LANES[lane] if k in KINDS)

    def _run_kind(self, j: dict[str, Any]) -> None:
        kind = j["kind"]
        if kind == "decode":
            self._decode({**j["payload"], "_job_id": j["id"]})
        elif kind == "snapshot_load":
            self._snapshot(j["payload"])
        elif kind == "snapshot":
            self._snapshot_step(j["payload"])
        elif kind == "resync":
            ops.consume_resync(self.engine, self, j["payload"])
        elif kind == "reprocess":
            self._locked(j["payload"]["connection_id"],
                         lambda: ops.consume_reprocess(self.engine, self, j["payload"], j["id"]))
        elif kind == "verify":
            ops.consume_verify(self.engine, self, j["payload"])
        elif kind == "replay":
            self._replay(j["payload"])
        elif kind == "integrity":
            integrity.recover(self.engine, self, j["payload"])
        elif kind == "completion":
            from . import completion

            completion.control(self.engine, self, j["payload"])
        elif kind == "lifecycle":
            lifecycle.drive(self.engine, self, str(j["payload"]["connection_id"]))
        elif kind == "schema":
            self._schema(str(j["payload"]["connection_id"]))
        elif kind == "maintain":
            from . import maintenance

            maintenance.run(self, j["payload"])                    # lake compaction / expiry, never during an apply

    def _locked(self, cid: str, fn: Callable[[], Any]) -> Any:
        lock = try_lock(self.engine, f"pipeline:{cid}")
        if lock is None:
            raise _Busy()
        try:
            return fn()
        finally:
            unlock(lock, f"pipeline:{cid}")

    def _decode(self, payload: dict[str, Any]) -> None:
        """The stream marker: one bounded slice of the connection's main stream under `pipeline:<cid>`."""
        cid = payload["connection_id"]
        lock = try_lock(self.engine, f"pipeline:{cid}")
        if lock is None:                       # another worker owns this stream: look again shortly (one per 2 s, HC-17)
            with self.engine.begin() as c:
                jobs.enqueue(c, "decode", {"connection_id": cid}, delay_s=2,
                             dedup_key=f"busy:{cid}:{int(time.time() // 2)}")
            return
        try:
            p = self.pipeline(cid)
            held = p.refresh_held()                                     # every hold kind + lifecycle, all workers
            require_lock(lock, f"pipeline:{cid}")                       # HA-08: never run on a lost lock
            pending = p.process_pending(max_s=decode_slice_s())
            require_lock(lock, f"pipeline:{cid}")
            if not pending and not p.more:
                replay.attach_ready(p, held)                            # bounded tails of finished bulk passes
                schedule.tick(p, held)
            if holds.rows_of_kinds(self.engine, cid, ("schema", "ddl"), held_only=True):
                with self.engine.begin() as c:                          # DDL / schema decisions: control lane
                    jobs.enqueue(c, "schema", {"connection_id": cid}, dedup_key=jobs.marker_key("schema", cid))
        finally:
            unlock(lock, f"pipeline:{cid}")
        if p.more:                             # time slice used up / yield requested: continue after the other jobs
            with self.engine.begin() as c:
                jobs.mark_dirty(c, cid)
            return
        if pending:                            # latency window not reached: come back later
            delay = max(1.0, p.loader.max_latency_s / 4 if p.loader.max_latency_s else 1.0)
            with self.engine.begin() as c:     # key per time bucket: never collides with the running job (HC-04)
                jobs.enqueue(c, "decode", {"connection_id": cid},
                             dedup_key=f"flush:{cid}:{p.last_seq}:{int(time.time() // delay)}", delay_s=delay)

    def _schema(self, cid: str) -> None:
        """Control lane: approved / rejected schema changes and destination DDL retries of held tables (the tables are
        held meanwhile, so the stream does not need the pipeline lock for them)."""
        from . import ddl_hold, schema_drift

        p = self.pipeline(cid)
        with self.engine.connect() as c:
            vid = store.latest_catalog_id(c, cid)
            cat = store.load_catalog(c, vid) if vid else None
        if cat is not None:
            p.tables.update(p.table_infos_for(int(vid), cat))    # type: ignore[arg-type]
        held = p.refresh_held()
        schema_drift.apply_decisions(p, held)
        ddl_hold.retry_due(p, held)

    def _replay(self, payload: dict[str, Any]) -> None:
        """Bulk phase of a replay pass: lock `replay:<cid>` (one pass at a time per connection); the main stream
        keeps flowing in other jobs / workers."""
        cid = payload["connection_id"]
        lock = try_lock(self.engine, f"replay:{cid}")
        if lock is None:
            with self.engine.begin() as c:
                jobs.enqueue(c, "replay", payload, delay_s=2,
                             dedup_key=f"replay-busy:{cid}:{payload.get('pass_id')}:{int(time.time() // 2)}")
            return
        try:
            replay.run_pass(self.pipeline(cid), str(payload["pass_id"]))
        finally:
            unlock(lock, f"replay:{cid}")

    def _snapshot_step(self, payload: dict[str, Any]) -> None:
        cid = payload["connection_id"]
        delay = snapshots.SnapshotService(self.engine, self.pipeline(cid)).step()
        if delay is not None:
            with self.engine.begin() as c:
                jobs.enqueue(c, "snapshot", {"connection_id": cid}, dedup_key=f"snapshot:{cid}:{time.time_ns()}",
                             delay_s=delay)

    def _snapshot(self, payload: dict[str, Any]) -> None:
        with self.engine.connect() as c:
            ch = c.execute(select(snapshot_chunk).where(
                snapshot_chunk.c.snapshot_id == payload["snapshot_id"],
                snapshot_chunk.c.chunk_no == payload["chunk_no"])).mappings().first()
            if ch is None:                     # discarded by a resumed snapshot (lifecycle._reissue): read again
                return
            eng = c.execute(select(connection.c.engine).where(connection.c.id == ch["connection_id"])).scalar_one()
            vid = store.latest_catalog_id(c, ch["connection_id"])
            cat = store.load_catalog(c, vid) if vid else None
        hdr = SnapshotChunkHeader.model_validate(ch["header"])
        p = self.pipeline(ch["connection_id"])
        info = store.table_infos(cat).get(hdr.table) if cat else None
        if info is None or eng != p.engine_name:
            raise RuntimeError(f"snapshot table {hdr.table.fqn} not in catalog")
        if ch["status"] == "LOADED":
            return                                                 # loaded by an earlier run of this job
        lines = [json.loads(line) for line in decompress(read_ref(ch["spool_path"]), hdr.codec).splitlines()
                 if line.strip()]
        # typed values (REQ-23: bytes / decimals / dates / timestamps) in the CDC value forms: JSON columns = JSON text
        # (objects / arrays of a plain chunk serialized), tagged values of other columns decoded, anything else for a
        # non-JSON column quarantined with the chunk reference (R7), never written as a guess.
        types = snapshot_types(info)
        if hdr.format == "jsonl":              # edge chunks carry the source text forms (Db2 CLI): typed here
            from alelt_common.verify_edge import parse_text

            lines = [{k: parse_text(v, types.get(k)) for k, v in r.items()} for r in lines]
        lines = [canonical_snapshot_row(r, types) for r in lines]
        bad = [r for r in lines if any(isinstance(v, Undecodable) for v in r.values())]
        if bad:
            from alelt_capture.snapshot import _quarantine

            _quarantine(self.engine, ch["connection_id"], hdr.table, hdr.snapshot_id, hdr.start_pos,
                        [c.name for c in info.key_columns()], bad)
            drop = {id(r) for r in bad}
            lines = [r for r in lines if id(r) not in drop]
        dinfo = p.policy.project(info)
        rows = [p.policy.apply_row(hdr.table, r) or {} for r in lines]
        p.dest.ensure_table(dinfo, p.policy.dest_name(hdr.table), None)
        p.dest.load_snapshot(p.policy.dest_name(hdr.table), dinfo, rows)
        with self.engine.begin() as c:
            c.execute(update(snapshot_chunk).where(snapshot_chunk.c.snapshot_id == ch["snapshot_id"],
                                                   snapshot_chunk.c.chunk_no == ch["chunk_no"]).values(status="LOADED"))
        snapshots.edge_chunk_loaded(self.engine, ch["connection_id"], hdr, compare=p.source.compare)

    def sweep(self) -> None:
        """Periodic: drive table lifecycles; keep schedule-held connections ticking without traffic; completion polls."""
        from .db import table_completion, table_lifecycle

        with self.engine.connect() as c:
            lc = {str(x) for x, in c.execute(select(table_lifecycle.c.connection_id).where(
                table_lifecycle.c.phase != lifecycle.STREAMING).group_by(table_lifecycle.c.connection_id))}
            cp = {str(x) for x, in c.execute(select(table_completion.c.connection_id)
                                             .group_by(table_completion.c.connection_id))}
        with self.engine.begin() as c:
            for cid in lc:
                lifecycle.wake(c, cid)
            for cid in cp:
                jobs.enqueue(c, "completion", {"connection_id": cid}, dedup_key=jobs.marker_key("completion", cid))
        if time.monotonic() - getattr(self, "_last_purge", -1e9) > float(os.environ.get("ALELT_RETENTION_EVERY_S", "3600")):
            self._last_purge = time.monotonic()
            try:
                from . import retention

                retention.purge(self.engine, self)                  # jobs, decoded cache, raw archive (HC-05)
            except Exception:  # noqa: BLE001
                log.exception("retention purge failed", extra={"component": "alelt_hub"})
        try:
            integrity.sweep(self.engine, self)                      # RESYNCING done, recovery after restart
            from . import maintenance

            maintenance.schedule(self)                              # destination maintain() when due (off-peak)
        except Exception:  # noqa: BLE001
            log.exception("integrity sweep failed", extra={"component": "alelt_hub"})
        with self.engine.connect() as c:
            cids = {str(x) for x, in c.execute(select(table_hold.c.connection_id).where(
                table_hold.c.kind.in_([schedule.KIND, integrity.KIND])).group_by(table_hold.c.connection_id))}
        for cid in cids:
            try:
                replay.resume_passes(self.pipeline(cid))            # bulk phases left by a crashed worker
            except Exception:  # noqa: BLE001
                log.exception("replay resume failed", extra={"component": "alelt_hub", "correlation_id": cid})
        with self.engine.begin() as c:
            for cid in cids:
                jobs.mark_dirty(c, cid)                             # schedule tick without traffic
        with self.engine.connect() as c:
            sch = {str(x) for x, in c.execute(select(table_hold.c.connection_id).where(
                table_hold.c.kind.in_(["schema", "ddl"]), table_hold.c.held.is_(True))
                .group_by(table_hold.c.connection_id))}
        with self.engine.begin() as c:
            for cid in sch:
                jobs.enqueue(c, "schema", {"connection_id": cid}, dedup_key=jobs.marker_key("schema", cid))

    def _lane_loop(self, lane: str, idle_sleep: float) -> None:
        while not self.stop:
            try:
                if not self.run_once(lane):
                    time.sleep(idle_sleep)
            except Exception:  # noqa: BLE001 - a lane never dies (job errors are handled in run_once)
                log.exception("worker lane error", extra={"component": "alelt_hub", "correlation_id": lane})
                time.sleep(idle_sleep)
        if lane == "data":                      # shutdown: checkpoint-only apply bounds the restart replay window
            for cid, p in list(self.pipelines.items()):
                try:
                    self._locked(cid, lambda p=p: p.checkpoint_only_apply(force=True))
                except Exception:  # noqa: BLE001
                    log.warning("shutdown checkpoint apply skipped", extra={"component": "alelt_hub",
                                                                           "correlation_id": cid})

    def run(self, idle_sleep: float = 0.5) -> None:
        """Lane threads (data / control) + this thread for liveness, reaping and the periodic sweep."""
        n_data = max(1, int(os.environ.get("ALELT_WORKER_DATA_THREADS", "1")))
        n_ctl = max(1, int(os.environ.get("ALELT_WORKER_CONTROL_THREADS", "1")))
        threads = [threading.Thread(target=self._lane_loop, args=(lane, idle_sleep), daemon=True,
                                    name=f"lane-{lane}-{i}")
                   for lane, n in (("data", n_data), ("control", n_ctl)) for i in range(n)]
        for t in threads:
            t.start()
        last_reap = last_sweep = 0.0
        alive = alive_file(self.s)
        while not self.stop:
            alive.touch()
            if time.monotonic() - last_reap > 60:
                jobs.requeue_stale(self.engine)
                diag_hooks.scan(self.engine, self.s)                        # agent/capture/lag/recon incidents
                last_reap = time.monotonic()
            if time.monotonic() - last_sweep > 30:
                try:
                    self.sweep()
                except Exception:  # noqa: BLE001
                    log.exception("sweep failed", extra={"component": "alelt_hub"})
                last_sweep = time.monotonic()
            time.sleep(idle_sleep)
        for t in threads:
            t.join(timeout=60)


class _Busy(Exception):
    pass


def decode_slice_s() -> float:
    """Longest run of one decode job (ALELT_DECODE_SLICE_S, default 20 s; 0 = unbounded)."""
    return float(os.environ.get("ALELT_DECODE_SLICE_S", "20") or 0)


def alive_file(s: Settings) -> Path:
    """Liveness file touched every poll loop: ALELT_WORKER_ALIVE_FILE, else /tmp/alelt_worker_alive (temp dir on Windows)."""
    if s.worker_alive_file:
        return Path(s.worker_alive_file)
    return Path("/tmp/alelt_worker_alive") if os.name != "nt" else Path(tempfile.gettempdir()) / "alelt_worker_alive"


def sqlite_single_worker(engine: Any) -> Any:
    """F-29: on SQLite only one worker process may run (single writer; workers would only contend on the write lock).
    Holds an OS file lock for the life of the process; a second worker exits 78 (configuration error, no restart
    loop under systemd/Docker `on-failure` policies that exclude 78). Returns the lock handle (None on PostgreSQL)."""
    if engine.dialect.name != "sqlite":
        return None
    lk = try_lock(engine, "worker:sqlite-singleton")
    if lk is None:
        log.error("another worker already runs on this SQLite metadata database; SQLite supports exactly one worker "
                  "(use PostgreSQL for several workers)", extra={"component": "alelt_hub"})
        raise SystemExit(78)
    return lk


def main() -> None:
    ap = argparse.ArgumentParser(description="ALELT hub worker")
    ap.add_argument("--once", action="store_true", help="drain the queue and exit")
    args = ap.parse_args()
    s = get_settings()
    setup_logging(s.log_level)
    from .ha import start_node_heartbeat   # X-49: hub_node row for console Topology & HA (registers HA tables)

    init_db()
    from .config_store import seed_from_files   # F-18: files are imported once, never a runtime override

    seed_from_files(get_engine(), s.tables_config)
    single = sqlite_single_worker(get_engine())         # F-29: SQLite metadata = exactly one worker process
    w = Worker(get_engine(), s)
    start_node_heartbeat(get_engine(), "worker", w.id)
    if s.worker_metrics_port:
        serve(REGISTRY, s.worker_metrics_port)
    signal.signal(signal.SIGINT, lambda *_: setattr(w, "stop", True))
    signal.signal(signal.SIGTERM, lambda *_: setattr(w, "stop", True))
    log.info("worker started", extra={"component": "alelt_hub", "correlation_id": w.id})
    if args.once:
        while w.run_once():
            pass
        return
    w.run()


if __name__ == "__main__":
    main()
