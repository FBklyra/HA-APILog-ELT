"""Per-connection pipeline: spooled raw batch -> SourceConnector.decode -> alelt_txn -> alelt_loader -> destination.

Restart model (no in-memory-only state survives a crash):
- The destination checkpoint (applied atomically with the data, R6) is the source of truth.
- checkpoint_mirror.restart_seq = first batch seq still needed to rebuild open transactions.
- On (re)start the pipeline replays batches seq >= restart_seq and skips change sets whose commit
  position <= destination checkpoint, so replay is idempotent.
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, select, update

from alelt_common.models import CatalogSnapshot, Position, TableInfo, TableRef
from alelt_common.spi import DestinationConnector, SourceConnector
from alelt_common.util import load, utcnow
from alelt_loader import Loader
from alelt_loader.policy import ConnectionPolicy
from alelt_txn import TxnAssembler

from . import completion, integrity, replay, store
from .db import batch, batch_table_stat, catalog_version, checkpoint_mirror, connection, quarantine_event, source_table
from .dialect import insert, try_lock, unlock
from .metrics import REGISTRY
from .settings import Settings
from .source_policy import PolicyCache

log = logging.getLogger("alelt_hub.pipeline")


def reason_label(reason: str) -> str:
    """Bounded metric label for a quarantine reason (XC-27): the text before the first ':' / '(' with digits, hex
    runs and quoted parts removed, at most 40 characters - positions and ids never become label values."""
    import re

    head = re.split(r"[:(\[]", reason or "", maxsplit=1)[0]
    head = re.sub(r"'[^']*'|\"[^\"]*\"|\S*\d\S*|\b[0-9A-Fa-f]{8,}\b", "", head)
    return re.sub(r"\s+", " ", head).strip()[:40] or "other"


class PipelineError(RuntimeError):
    pass


_DEST_ROOT = Path(__file__).resolve().parents[2] / "connectors" / "destinations"


def destination_registry() -> Any:
    """alelt_dst_kit.registry: the single engine name -> 'module:Class' map (worker, wizard catalog, lab)."""
    import sys

    kit = _DEST_ROOT / "_spi"
    if str(kit) not in sys.path and kit.is_dir():
        sys.path.insert(0, str(kit))
    from alelt_dst_kit import registry

    return registry


def destination_class(ref_or_engine: str) -> Any:
    """Destination class from 'module:Class' or an engine name (see alelt_dst_kit.registry; ALELT_DESTINATIONS extras).
    One code path for ALELT_DESTINATION and per-pipeline destinations (wizard)."""
    if ":" in ref_or_engine:
        return load(ref_or_engine)
    return destination_registry().load_destination(ref_or_engine)


def make_destination(s: Settings, policy: ConnectionPolicy, source: SourceConnector,
                     spec: dict[str, Any] | None = None) -> DestinationConnector:
    """Build the destination of one connection.

    Default: ALELT_DESTINATION ('module:Class' or engine name) + ALELT_DEST_URL. A wizard/import pipeline may name its own
    destination (`pipeline_definition.destination = {engine, settings: {url?}, secret_refs?}`): it is used when it
    carries a `url` or names an engine other than the default; the password reference (if any) fills `ALELT_DEST_PASSWORD`
    semantics through the `password=` keyword only for classes that accept it. Classes with `from_hub` (key-pair
    warehouses such as Snowflake, X-111) are built by that factory with the same keywords."""
    ref, url = s.destination, s.dest_url
    extra: dict[str, Any] = {}
    if spec and spec.get("engine"):
        st = dict(spec.get("settings") or {})
        default_cls = destination_class(s.destination)
        cls_spec = destination_class(str(spec["engine"]))
        if st.get("url") or cls_spec is not default_cls:
            ref, url = str(spec["engine"]), str(st.get("url") or s.dest_url)
            pw_ref = (spec.get("secret_refs") or {}).get("password")
            if pw_ref:
                from alelt_import.secrets import SecretStore

                extra["password"] = SecretStore().resolve(pw_ref)
    cls = destination_class(ref)
    kw: dict[str, Any] = dict(url=url, schema=policy.dest_schema or policy.connection_id,
                              output_profile=policy.output_profile, compare=source.compare)
    factory = getattr(cls, "from_hub", None)       # destinations not built from a URL (e.g. key-pair warehouses)
    if extra:
        import inspect

        target = factory or cls
        params = inspect.signature(target).parameters
        if "password" in params or any(p.kind is p.VAR_KEYWORD for p in params.values()):
            kw.update(extra)
    return factory(**kw) if factory else cls(**kw)  # type: ignore[no-any-return]


class StreamPipeline:
    """Owns decode/assembly/load state for one connection (stream = connection id)."""

    READ_CHUNK = 50

    def __init__(self, engine: Engine, connection_id: str, settings: Settings,
                 source: SourceConnector | None = None, dest: DestinationConnector | None = None,
                 hold: Callable[[TableRef], bool] | None = None) -> None:
        """`hold(table)`: extra hold predicate (tests / embedding). `self.held` (fqn set, shared with the worker) holds
        tables of every `table_hold` kind (snapshot, schedule, integrity); evaluated at flush time."""
        self.engine = engine
        self.cid = connection_id
        self.s = settings
        with engine.connect() as c:
            eng = c.execute(select(connection.c.engine).where(connection.c.id == connection_id)).scalar()
            if eng is None:
                raise PipelineError(f"unknown connection {connection_id}")
            policy = store.connection_policy(c, connection_id, settings.tables_config)
            spec = (store.pipeline_doc(c, connection_id) or {}).get("destination")
        self.engine_name = str(eng)
        src = source or store.source_connector(self.engine_name)
        if src is None:
            raise PipelineError(f"no source connector for engine {self.engine_name} (ALELT_SOURCES)")
        self.source = src
        self.policy = policy
        self.dest = dest or make_destination(settings, self.policy, src, spec)
        self.dest.bootstrap()
        self.stream = connection_id
        self.asm = TxnAssembler(settings.txn_byte_budget, settings.spool_dir / "txn-spill" / connection_id,
                                settings.long_txn_seconds)
        self.held: set[str] = set()
        outer = hold

        def held(ref: TableRef) -> bool:
            return ref.fqn in self.held or (outer is not None and outer(ref))
        self.frozen: set[str] = set()             # holds that also stop completion rows (integrity/schema/snapshot)

        def frozen(ref: TableRef) -> bool:
            return ref.fqn in self.frozen or (outer is not None and outer(ref))
        self.loader = Loader(self.dest, self.policy, self.stream, src.compare,
                             max_rows=settings.load_max_rows, max_latency_s=settings.load_max_latency_s, hold=held,
                             frozen=frozen)
        self.cache = replay.DecodedCache(connection_id, src, self.engine_name)
        self.on_chunk: Callable[[], Any] | None = None      # worker: schedule holds before every chunk (F-08)
        self.more = False                         # process_pending stopped at its time slice (batches left)
        self._infos: dict[int, dict[TableRef, TableInfo]] = {}
        self._last_vid: int | None = None
        self.last_seq: int | None = None          # last batch seq fed to the assembler
        self.flushed_seq: int | None = None       # last seq made durable at the destination by us
        self.replay_ckpt: Position | None = None
        self.tables: dict[TableRef, TableInfo] = {}
        self._catalogs: dict[int, CatalogSnapshot] = {}
        self.source_policy = PolicyCache(engine)          # REQ-16: diag.source_policy
        self._policy_rev: str = ""
        # out-of-row completion (alelt_hub.completion): signals of open transactions, results put into the buffer
        self.signals = completion.SignalTracker()
        self.completion_injected: dict[str, completion.Injected] = {}
        self.completion_tables: set[str] = set()
        self._floor_tables: set[str] = set()
        self._last_dest_apply = time.monotonic()      # checkpoint-only apply pacing (ALELT_CHECKPOINT_IDLE_S)

    # ------------------------------------------------------------------ state
    def _mirror(self) -> dict[str, Any] | None:
        with self.engine.connect() as c:
            r = c.execute(select(checkpoint_mirror).where(checkpoint_mirror.c.stream == self.stream)).mappings().first()
        return dict(r) if r else None

    def rebuild(self) -> None:
        """Reset in-memory state from the destination checkpoint + mirror low-water mark."""
        m = self._mirror()
        self.asm.reset()
        self.signals.reset()
        self.completion_injected.clear()              # results re-collected from their commands
        self.loader.buf = type(self.loader.buf)()
        ck = self.dest.read_checkpoint(self.stream)
        self.replay_ckpt = Position(engine=self.engine_name, text=ck.text) if ck and ck.text else None
        restart = int(m["restart_seq"]) if m else 0
        self.last_seq = restart - 1
        self.flushed_seq = int(m["last_seq"]) if m else 0
        with self.engine.connect() as c:
            self.policy = store.connection_policy(c, self.cid, self.s.tables_config)
        self.loader.policy = self.policy
        self.loader.max_latency_s = self.flush_cadence_s()
        if ck is not None and ck.text and m is not None and m.get("dest_position") is None:
            self._set_dest_position(ck.text)
        log.info("pipeline rebuilt", extra={"component": "alelt_hub", "position": self.replay_ckpt.text
                                            if self.replay_ckpt else None, "correlation_id": f"restart_seq={restart}"})

    # ------------------------------------------------------------------ processing
    def process_pending(self, max_s: float | None = None) -> bool:
        """Process every received batch in seq order. Returns True if an unflushed buffer remains.
        A connection stopped by on_quarantine=stop_stream processes nothing until an operator resumes it.
        `max_s`: time slice - after it the buffer is flushed and the call returns with `self.more` = True (the worker
        re-enqueues a continuation, so integrity / snapshot / replay jobs of the shared queue are never starved by a
        stream that receives batches faster than it decodes them)."""
        self.more = False
        if integrity.blocks_replay(self.engine, self.cid, integrity.CONN):
            return False
        t_end = time.monotonic() + max_s if max_s else None
        self.refresh_held()           # XC-23: every caller (decode, reprocess, snapshot steps) sees current holds
        m = self._mirror()
        if self.last_seq is None or (m is not None and int(m["last_seq"]) != (self.flushed_seq or 0)):
            self.rebuild()
        self.loader.max_latency_s = self.flush_cadence_s()   # F-22 (tables known after the first catalog)
        try:
            completion.step(self)                     # collect answered fetches into the buffer, issue new ones
        except Exception:  # noqa: BLE001 - completion never blocks the stream (entries stay, retried next slice)
            log.exception("completion step failed", extra={"component": "alelt_hub", "correlation_id": self.cid})
        while True:
            if self.on_chunk is not None:
                self.on_chunk()
            with self.engine.connect() as c:
                rows = c.execute(select(batch).where(batch.c.connection_id == self.cid, batch.c.seq > self.last_seq)
                                 .order_by(batch.c.seq).limit(self.READ_CHUNK)).mappings().all()
            if not rows:
                break
            try:
                for b in rows:
                    self._process(dict(b))
                    if self.loader.due():
                        self.flush()
            except integrity.StreamStopped:
                self._publish_txn_gauges()
                return False
            if (t_end is not None and time.monotonic() >= t_end) or self.yield_requested():
                self.more = True               # slice over / a control step needs the lock (cooperative yield)
                if self.loader.buf.rows:
                    self.flush()               # durable before the continuation (other jobs run first)
                break
        if self.loader.due():
            self.flush()
        if not self.loader.pending() and self.loader.injected():
            completion.flush_idle(self)               # idle source: completion rows through a transient stream
        if not self.loader.pending():
            self.checkpoint_only_apply()              # destination behind the mirror after empty flushes
        self._publish_txn_gauges()
        return self.loader.pending()

    def yield_requested(self) -> bool:
        """A control step waits for `pipeline:<cid>` (alelt_hub.snapshots / lifecycle): end the slice at this chunk."""
        from .db import pipeline_request

        with self.engine.connect() as c:
            return c.execute(select(pipeline_request.c.connection_id).where(
                pipeline_request.c.connection_id == self.cid)).first() is not None

    def checkpoint_idle_s(self) -> float:
        env = os.environ.get("ALELT_CHECKPOINT_IDLE_S")
        return max(60.0, float(env)) if env else max(60.0, self.flush_cadence_s())

    def checkpoint_only_apply(self, force: bool = False) -> bool:
        """One small destination transaction (checkpoint only) when the mirror is ahead of the destination checkpoint
        (flushes that had nothing to apply), at most every ALELT_CHECKPOINT_IDLE_S and at worker shutdown (`force`)."""
        m = self._mirror()
        if not m or not m.get("position") or m.get("dest_position") == m["position"] or self.loader.pending():
            return False
        if not force and time.monotonic() - self._last_dest_apply < self.checkpoint_idle_s():
            return False
        self.loader.apply_checkpoint(Position(engine=self.engine_name, text=str(m["position"])),
                                     f"{self.cid}:ckpt:{m['last_seq']}")
        self._set_dest_position(str(m["position"]))
        self._last_dest_apply = time.monotonic()
        return True

    def _set_dest_position(self, pos: str) -> None:
        with self.engine.begin() as c:
            c.execute(update(checkpoint_mirror).where(checkpoint_mirror.c.stream == self.stream)
                      .values(dest_position=pos))

    def snapshot_active(self) -> set[str]:
        """Tables whose snapshot hold is effective (SNAPSHOTTING / RESYNC / CATCHING_UP before its catch-up)."""
        from . import holds

        return {r["table_fqn"] for r in holds.rows(self.engine, self.cid, "snapshot")
                if holds.snapshot_held(str(r["state"]), r["body"] or {})}

    def refresh_held(self) -> set[str]:
        """Reload the held-table set (all hold kinds, all workers) in place: the set object is shared with the
        worker and the loader's hold predicate."""
        from . import holds

        from . import lifecycle

        now = holds.held_tables(self.engine, self.cid) | lifecycle.unapplied(self.engine, self.cid)
        self.held.clear()
        self.held.update(now)
        fails = getattr(self.loader, "_ddl_fail", None)
        if fails and not holds.rows_of_kinds(self.engine, self.cid, ("ddl",), held_only=True):
            fails.clear()                             # DDL fixed by the control lane: the backoff state is stale
        self.frozen.clear()
        self.frozen.update(completion.frozen(self))
        return self.held

    def _catalog_for(self, b: dict[str, Any]) -> CatalogSnapshot:
        return self._catalog(self.catalog_vid_for(b))

    def latest_catalog_vid(self) -> int:
        with self.engine.connect() as c:
            vid = store.latest_catalog_id(c, self.cid)
        if vid is None:
            raise PipelineError("connection has no catalog version")
        return int(vid)

    def table_infos_for(self, vid: int, cat: CatalogSnapshot) -> dict[TableRef, TableInfo]:
        got = self._infos.get(vid)
        if got is None:
            got = self._infos[vid] = store.table_infos(cat)
        return got

    def catalog_vid_for(self, b: dict[str, Any]) -> int:
        """Catalog version valid at the batch position: newest valid_from_pos <= from_pos (source order).
        Falls back to the version current at receipt when no version predates the batch. Once a batch was decoded,
        the version that decode used is recorded and reused (XC-22): a catalog ingested later with an earlier
        valid_from never makes a replay decode the batch differently from the main stream (an explicit re-decode
        with the newest catalog passes latest_catalog to alelt_hub.replay instead)."""
        if b.get("decoded_catalog_vid") is not None:
            return int(b["decoded_catalog_vid"])
        with self.engine.connect() as c:
            versions = c.execute(select(catalog_version.c.id, catalog_version.c.valid_from_pos)
                                 .where(catalog_version.c.connection_id == self.cid)).all()
        at = Position(engine=self.engine_name, text=b["from_lri"])
        best: tuple[Position, int] | None = None
        for vid, vpos in versions:
            if not vpos:
                continue
            p = Position(engine=self.engine_name, text=vpos)
            if self.source.compare(p, at) <= 0 and (best is None or self.source.compare(p, best[0]) >= 0):
                best = (p, int(vid))
        vid_ = best[1] if best else b["catalog_version_id"]
        if vid_ is None:
            raise PipelineError("batch has no catalog version (agent must POST /catalog first)")
        return int(vid_)

    def _catalog(self, vid: int | None) -> CatalogSnapshot:
        if vid is None:
            raise PipelineError("batch has no catalog version (agent must POST /catalog first)")
        if vid not in self._catalogs:
            with self.engine.connect() as c:
                self._catalogs[vid] = store.load_catalog(c, vid)
        return self._catalogs[vid]

    def _prev_vid(self, b: dict[str, Any]) -> int | None:
        with self.engine.connect() as c:
            prev = c.execute(select(batch).where(batch.c.connection_id == self.cid, batch.c.seq < b["seq"])
                             .order_by(batch.c.seq.desc()).limit(1)).mappings().first()
        return self.catalog_vid_for(dict(prev)) if prev else None

    def _process(self, b: dict[str, Any]) -> None:
        if self.loader.buf.rows >= self.loader.max_buffer_rows:
            self.flush()                  # HC-18: make room BEFORE feeding, never Backpressure after asm.feed
        vid = self.catalog_vid_for(b)
        cat = self._catalog(vid)
        self.tables.update(self.table_infos_for(vid, cat))
        if b["status"] == "RECEIVED":                 # HC-10: drift between consecutive catalog versions, before apply
            prev = self._last_vid if self._last_vid is not None else self._prev_vid(b)
            if prev is not None and prev != vid:
                from . import schema_drift

                schema_drift.on_catalog_change(self, self._catalog(prev), cat, b, self.held)
        self._last_vid = vid
        persist = replay.cache_mode() == "always" or (replay.cache_mode() == "auto" and bool(self.held))
        res = replay.drift_check(self.cache.decode(b, cat, vid, persist=persist), self.table_infos_for(vid, cat),
                                 self.policy)                 # REQ-26: unknown columns / tables never dropped
        first_time = b["status"] == "RECEIVED"
        events, skipped = self._apply_source_policy(res.events, self.tables, None if first_time else b)
        if first_time and (res.quarantined or skipped):
            self._quarantine(b, [*res.quarantined, *skipped])
        sigs = replay.signal_records(res.events) if first_time else []   # out-of-row signals with keys: completion
        if sigs:                                      # completeness signals (X-132): recorded like quarantine (R7)
            self._quarantine(b, sigs)
        if first_time and (res.quarantined or sigs):  # F-01: unexplained records hold their table (never diverge)
            try:
                integrity.on_quarantine(self, b, [*res.quarantined, *sigs], self.held)
            except integrity.StreamStopped:
                self.flush()                          # earlier batches only; nothing of this batch is applied
                raise
        if first_time and res.events:
            from alelt_diagnose import hooks as diag_hooks   # bounded source-data checks; never raises

            diag_hooks.source_checks(self.engine, self.cid, b, res.events, self.tables)
        self.signals.feed(res.events)
        sets = self.asm.feed(events, int(b["seq"]))
        self._completion_signals(sets, int(b["seq"]))
        self.loader.add(sets, Position(engine=self.engine_name, text=b["to_lri"]), self.replay_ckpt)
        if first_time:
            with self.engine.begin() as c:
                c.execute(update(batch).where(batch.c.id == b["id"]).values(
                    status="DECODED", events=len(res.events), decoded_catalog_vid=vid,
                    decoded_policy_rev=self._policy_rev, updated_at=utcnow()))
        self.last_seq = int(b["seq"])

    def _completion_signals(self, sets: list[Any], seq: int) -> None:
        """Out-of-row signals of the transactions committed in this batch: queued for completion when the main stream
        applies their table (enabled, not held); a held table's signals are queued by the pass that applies its range
        (alelt_hub.replay.run). DELETEs committed after an entry's newest signal cancel it. Change sets the destination
        already holds (restart replay) are skipped: their signals were queued by the first run."""
        cmp, eng = self.source.compare, self.engine_name
        ck = self.replay_ckpt
        new = [cs for cs in sets if ck is None or cmp(cs.commit_position, ck) > 0]
        keep: list[completion.Signal] = []
        deferred = 0
        for sg in self.signals.committed(sets, seq):
            if ck is not None and cmp(Position(engine=eng, text=sg.pos), ck) <= 0:
                continue                              # applied before the restart: queued by the first run
            ref = TableRef(schema=sg.fqn.partition(".")[0], name=sg.fqn.partition(".")[2])
            if not self.policy.table(ref).enabled:
                continue
            if sg.fqn in self.held or self.hold_extra(ref):
                deferred += 1
                continue
            keep.append(sg)
        if deferred:
            REGISTRY.inc("alelt_signals_deferred_total", float(deferred), connection_id=self.cid, reason="held")
        if keep:
            completion.enqueue_signals(self.engine, self, keep)
            self.completion_tables |= {sg.fqn for sg in keep}
        if self.completion_tables and new:
            completion.cancel_deleted(self.engine, self, new)

    def hold_extra(self, ref: TableRef) -> bool:
        """Tables not applied by the main stream besides `self.held` (embedding hold predicate)."""
        return False

    def _apply_source_policy(self, events: list[Any], tables: dict[TableRef, TableInfo],
                             b: dict[str, Any] | None = None) -> tuple[list[Any], list[Any]]:
        """REQ-16: chosen handling (QUARANTINE_SKIP / COERCE / ACCEPT_AS_IS) per table.column + issue. `b` = a batch
        decoded before: the policy revision of its first decode is reused (XC-30), so replays, catch-ups and rebuilds
        produce what the main stream applied even after the policy changed. Without `b` (first decode) the current
        policy is used and its revision recorded on the batch."""
        if b is None:
            pol = self.source_policy.get()
            self.source_policy.remember(pol)
            self._policy_rev = pol.rev
        else:
            pol = self.source_policy.revision(b.get("decoded_policy_rev"))
        if not pol:
            return events, []
        out: list[Any] = []
        skipped: list[Any] = []
        for ev in events:
            o = pol.apply(ev, tables.get(ev.table) if ev.table is not None else None)
            if o.event is None:
                skipped.append(o.quarantine)
            else:
                out.append(o.event)
        return out, skipped

    def _quarantine(self, b: dict[str, Any], recs: list[Any]) -> None:
        with self.engine.begin() as c:
            for q in recs:
                tid = None
                if q.table is not None:
                    tid = c.execute(select(source_table.c.id).where(
                        source_table.c.connection_id == self.cid, source_table.c.src_schema == q.table.schema_,
                        source_table.c.src_table == q.table.name)).scalar()
                c.execute(insert(quarantine_event).values(
                    id=uuid.uuid4(), connection_id=self.cid, batch_id=b["id"], table_id=tid,
                    table_fqn=q.table.fqn if q.table else None, lri=q.position.text if q.position else None,
                    reason=q.reason, raw_ref=q.raw_ref.model_dump(mode="json") if q.raw_ref else None,
                    raw_hex=q.raw_hex))
        for q in recs:
            REGISTRY.inc("alelt_quarantine_total", table=q.table.fqn if q.table else "", reason=reason_label(q.reason))
        log.warning("records quarantined", extra={"component": "alelt_hub", "batch_id": str(b["id"]),
                                                  "correlation_id": f"n={len(recs)}"})
        from alelt_diagnose import hooks as diag_hooks   # incident per (table, reason); never raises

        diag_hooks.quarantined(self.engine, self.cid, b, recs)

    def _flush_isolating_ddl(self, flush_id: str) -> list[Any]:
        """loader.flush (watermarks: SPI default, held tables absent); a table whose destination DDL fails is held
        alone (alelt_hub.ddl_hold) and the flush is retried for the others (DST-loader-01)."""
        from alelt_loader import TableDDLError

        from . import ddl_hold

        extra = self._floor_watermarks()
        for _ in range(max(1, len(self.tables)) + 1):
            try:
                return self.loader.flush(self.tables, batch_id=flush_id, extra_watermarks=extra or None)
            except TableDDLError as e:
                ddl_hold.on_ddl_error(self, e, self.held)
        return self.loader.flush(self.tables, batch_id=flush_id, extra_watermarks=extra or None)

    def _floor_watermarks(self) -> dict[str, str | None]:
        """Completion floor `<cid>#t:<table>` = min(first_pos) of the table's open completion entries (ADR-005
        amendment); a table whose entries drained gets its floor moved to this flush's position (None = to_pos)."""
        if not self.completion_tables and not self._floor_tables:
            return {}
        fl = completion.floors(self.engine, self) if self.completion_tables else {}
        out: dict[str, str | None] = dict(fl)
        for k in self._floor_tables - set(fl):
            out[k] = None
        self._floor_tables = set(fl)
        return out

    def flush(self) -> None:
        if not self.loader.pending() or self.last_seq is None:
            return
        flush_id = f"{self.cid}:{self.last_seq}"
        commit_ts = next((cs.commit_ts for cs in reversed(self.loader.buf.changesets) if cs.commit_ts), None)
        span: dict[str, list[Any]] = {}                 # F-19 latency evidence: table -> [min, max] commit_ts
        for cs in self.loader.buf.changesets:
            if cs.commit_ts is not None:
                for ev in cs.events:
                    if ev.table is not None:
                        mm = span.setdefault(ev.table.fqn, [cs.commit_ts, cs.commit_ts])
                        mm[0], mm[1] = min(mm[0], cs.commit_ts), max(mm[1], cs.commit_ts)
        t0 = time.monotonic()
        carried = {k: v for k, v in self.completion_injected.items()
                   if any(cs.txid == f"{completion.TXID_PREFIX}{k}" for cs in self.loader.injected())}
        frozen_now = set(self.frozen)
        stats = self._flush_isolating_ddl(flush_id)
        dur = time.monotonic() - t0
        for st in stats:
            REGISTRY.observe("alelt_merge_duration_seconds", dur, table=st.table)
        with self.engine.begin() as c:
            to_text = c.execute(select(batch.c.to_lri).where(batch.c.connection_id == self.cid,
                                                            batch.c.seq == self.last_seq)).scalar()
            vals = dict(connection_id=self.cid, position=to_text, last_seq=self.last_seq,
                        restart_seq=self.asm.low_water_seq(self.last_seq + 1), open_txns=len(self.asm.open),
                        oldest_open_s=int(self.asm.oldest_open_seconds()), updated_at=utcnow())
            if commit_ts is not None:
                vals["last_commit_ts"] = commit_ts
            if not self.loader.last_flush_empty:
                vals["dest_position"] = to_text      # the destination committed this position with the data
            c.execute(insert(checkpoint_mirror).values(stream=self.stream, **vals)
                      .on_conflict_do_update(index_elements=["stream"], set_=vals))
            c.execute(update(batch).where(batch.c.connection_id == self.cid, batch.c.seq <= self.last_seq,
                                          batch.c.status.in_(["RECEIVED", "DECODED"]))
                      .values(status="LOADED", updated_at=utcnow()))
            for st in stats:
                c.execute(insert(batch_table_stat).values(
                    batch_id=flush_id, table_fqn=st.table, inserts=st.inserts, updates=st.updates,
                    deletes=st.deletes, duration_ms=st.duration_ms,
                    commit_ts_min=span.get(st.table, [None, None])[0],
                    commit_ts_max=span.get(st.table, [None, None])[1]).on_conflict_do_nothing())
        self.flushed_seq = self.last_seq
        if carried:
            for k in carried:
                self.completion_injected.pop(k, None)
            completion.after_flush(self, carried, frozen_now)
        for t in self.asm.long_open():
            log.warning("long-open transaction", extra={"component": "alelt_txn", "position": t.first_position,
                                                        "correlation_id": t.txid})

    # ------------------------------------------------------------------ snapshot catch-up (X-22)
    def catch_up(self, job: Any) -> int:
        """`SnapshotOrchestrator.on_catchup` hook: replay one table from `job.start_pos` up to what the main stream
        has applied, into its own checkpoint stream `<stream>#catchup:<table>` (exactly-once per catch-up).
        Call under the pipeline lock (worker), after the table left SNAPSHOTTING. Returns change events applied."""
        if job.start_pos is None:
            raise PipelineError(f"catch-up for {job.table}: snapshot has no start_pos")
        s, _, n = str(job.table).partition(".")
        return self.replay_table(TableRef(schema=s, name=n), job.start_pos.text, f"{self.stream}#catchup:{s}.{n}")

    def replay_table(self, ref: TableRef, start_text: str | None, stream: str, inclusive: bool = False,
                     bypass_cache: bool = False) -> int:
        """Re-apply one table's committed changes after `start_text` (commit > start; >= with `inclusive`; None =
        from the first batch) up to the batch the main stream last flushed, into checkpoint stream `stream`
        (alelt_hub.replay.run: decoded-event cache, resumable, exactly-once per stream). Used by snapshot catch-up and
        reprocess jobs; schedule releases and integrity recovery use multi-table passes (alelt_hub.replay)."""
        return int(self.replay_tables([replay.Target(ref, start_text, inclusive)], stream,
                                      bypass_cache=bypass_cache).applied.get(ref.fqn, 0))

    def replay_tables(self, targets: list[Any], stream: str, bypass_cache: bool = False) -> Any:
        """One pass for many tables up to what the main stream flushed (call under the pipeline lock).
        `bypass_cache`: decode the raw spool again (reprocess after a decoder fix, HC-02)."""
        if self.flushed_seq is None:
            self.rebuild()
        return replay.run(self, targets, stream, self.flushed_seq or 0, bypass_cache=bypass_cache)

    def refresh_policy(self) -> None:
        """Re-read the connection policy (config revisions) - replay passes use the current one (HC-20)."""
        with self.engine.connect() as c:
            self.policy = store.connection_policy(c, self.cid, self.s.tables_config)
        self.loader.policy = self.policy
        self.loader.max_latency_s = self.flush_cadence_s()

    def flush_cadence_s(self) -> float:
        """F-22 shared flush cadence: the main stream applies ALL its tables in one destination transaction per
        cadence (rows / buffer caps still flush earlier: bounded memory). Cadence = ALELT_FLUSH_CADENCE_S (else
        ALELT_LOAD_MAX_LATENCY_S), bounded by the smallest `schedule.latency_target_s` of the enabled tables. A connection
        configured with 900 s costs one warehouse transaction per 15 min instead of one per table per 2 min."""
        env = os.environ.get("ALELT_FLUSH_CADENCE_S")
        base = float(env) if env else float(self.s.load_max_latency_s)
        refs = [r for r in self.tables if self.policy.table(r).enabled] or [
            TableRef(schema=f.partition(".")[0], name=f.partition(".")[2])
            for f, tp in self.policy.tables.items() if tp.enabled]
        targets = [self.policy.latency_target_s(r, base) for r in refs] or [base]
        return max(0.0, min(base, *targets))

    def flushed_position(self) -> str | None:
        """to_pos of the last batch made durable by the main stream (hold start position for schedules)."""
        m = self._mirror()
        return str(m["position"]) if m and m.get("position") else None

    def _publish_txn_gauges(self) -> None:
        REGISTRY.set("alelt_txn_open", len(self.asm.open), stream=self.stream)
        REGISTRY.set("alelt_txn_oldest_open_seconds", self.asm.oldest_open_seconds(), stream=self.stream)
        with self.engine.begin() as c:
            c.execute(update(checkpoint_mirror).where(checkpoint_mirror.c.stream == self.stream).values(
                open_txns=len(self.asm.open), oldest_open_s=int(self.asm.oldest_open_seconds())))


__all__ = ["StreamPipeline", "PipelineError", "make_destination", "try_lock", "unlock"]
