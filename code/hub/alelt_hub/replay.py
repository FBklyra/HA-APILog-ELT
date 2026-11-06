"""Replay engine (F-08 / X-125): decode once, replay many tables in one pass, never block the main stream.

Pieces
- `DecodedCache`: decoded canonical events per (batch, catalog version, decoder version). Memory LRU + the blob store
  (`decoded/<connection>/<batch>.v<catalog>.<decoder>.cjl.z`, canonical JSONL, zlib). The main stream writes it when
  the connection has held tables (`ALELT_DECODED_CACHE=auto`, default; `always` / `off`), every replay reads it, so a
  replay never re-decodes raw log records that were already decoded with the same catalog + decoder version.
- `run()`: ONE pass over the spool range for every target table (each with its own start position), into one
  checkpoint stream. Exactly-once per pass: the destination checkpoint of the pass stream is read first and every
  change set at or below it is skipped (intermediate flushes keep memory bounded and stay resumable).
- Pass orchestration over `table_hold` (`mark_pass`, `run_pass`, `attach_ready`):
    bulk phase  (`replay` job, lock `replay:<cid>`, NOT the pipeline lock): replay every table of the pass from its
                hold position up to U1 = what the main stream had applied when the pass started (persisted in the
                hold rows, so a restarted pass replays the same range); afterwards every held row of those tables
                gets start_pos = U1 (the table watermark advanced). Deferred tables stay held (next round later).
    attach phase (decode job, pipeline lock, short): tables that rejoin the main stream (window opened, integrity
                recovered, schedule removed) replay the small tail (U1, P] where P = the main stream position under
                the lock, then the holds are released; the next main flush includes them (exact boundary).
  The main stream keeps flowing during the bulk phase; only the tail runs under the pipeline lock.

Checkpoint streams (what the hub asks the destination to commit, ADR-005): `<cid>` main stream; `<cid>#pass:<id>`
bulk; `<cid>#pass:<id>.attach` tail; `<cid>#catchup:<fqn>` snapshot catch-up; `<cid>#reprocess:<fqn>:<job>`;
`<cid>#redecode:<fqn>:<n>`. Every stream row carries per-table watermarks {dest_table: position} written in the same
commit (SPI default, stream K); applied position of a table = max of its watermarks over all streams (ADR-005).
"""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
import zlib
from pathlib import Path
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select

from alelt_common.canonical_jsonl import decode_batch, encode_event
from alelt_common.completeness import RecordClass, signal_of
from alelt_common.models import (
    CatalogSnapshot,
    ChangeEvent,
    ChangeSet,
    Position,
    QuarantineRecord,
    TableInfo,
    TableRef,
)
from alelt_common.spi import DecodeResult
from alelt_common.util import decompress, sha256_hex, utcnow
from alelt_loader import Loader
from alelt_txn import TxnAssembler

from . import holds, jobs
from .blobstore import get_store, read_ref
from .db import batch, checkpoint_mirror
from .metrics import REGISTRY

log = logging.getLogger("alelt_hub.replay")


class ReplayError(RuntimeError):
    pass


def cache_mode() -> str:
    m = os.environ.get("ALELT_DECODED_CACHE", "auto").strip().lower()
    return m if m in ("auto", "always", "off") else "auto"


def _ref(fqn: str) -> TableRef:
    s, _, n = fqn.partition(".")
    return TableRef(schema=s, name=n)


# ------------------------------------------------------------------ decoded-event cache
_FP: dict[type, str] = {}


def decoder_fingerprint(source: Any) -> str:
    """Identity of the decoder that produced cached events (HC-02): the connector's declared `decoder_version` (if
    any) + a hash of its package sources and native decoder library (name, size, mtime). Any decoder change -> new
    key -> cache miss; nothing stale is ever replayed after a decoder fix."""
    t = type(source)
    if t in _FP:
        return _FP[t]
    import hashlib
    import inspect
    import sys

    h = hashlib.sha256(str(getattr(source, "decoder_version", "") or "").encode())
    try:
        mod = sys.modules.get(t.__module__)
        f = inspect.getsourcefile(mod) if mod else None    # type: ignore[arg-type]
        root = Path(f).parent if f else None
        if root is not None:
            for x in sorted(root.rglob("*")):
                if x.suffix in (".py", ".dll", ".so", ".dylib", ".pyd") and "__pycache__" not in x.parts:
                    h.update(x.name.encode())
                    h.update(x.read_bytes() if x.suffix == ".py" else f"{x.stat().st_size}:{x.stat().st_mtime_ns}".encode())
    except Exception:  # noqa: BLE001 - unknown provenance: unique per process (never reuses a foreign blob)
        h.update(f"{os.getpid()}:{time.time_ns()}".encode())
    for env in ("ALELTDECODE_LIB",):
        lib = os.environ.get(env)
        if lib and Path(lib).is_file():
            st = Path(lib).stat()
            h.update(f"{lib}:{st.st_size}:{st.st_mtime_ns}".encode())
    _FP[t] = h.hexdigest()[:16]
    return _FP[t]


def _res_bytes(res: DecodeResult) -> int:
    from alelt_txn.assembler import _event_size

    return sum(_event_size(e) for e in res.events) + 256 * (len(res.quarantined) + 1)


class DecodedCache:
    """Decode a spooled batch once per (batch, catalog version, decoder fingerprint). Memory bounded in bytes
    (ALELT_DECODED_CACHE_MEM_MB, default 256, HC-22)."""

    def __init__(self, cid: str, source: Any, engine_name: str, mem_bytes: int | None = None) -> None:
        self.cid, self.source, self.engine_name = cid, source, engine_name
        self.mem_bytes = mem_bytes if mem_bytes is not None else int(
            float(os.environ.get("ALELT_DECODED_CACHE_MEM_MB", "256")) * (1 << 20))
        self._mem: OrderedDict[str, tuple[DecodeResult, int]] = OrderedDict()
        self._n = 0
        self.hits = self.misses = 0

    @property
    def decoder_version(self) -> str:
        return decoder_fingerprint(self.source)

    def key(self, b: dict[str, Any], vid: int) -> str:
        safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in self.cid)
        return f"decoded/{safe}/{b['id']}.v{vid}.{self.decoder_version}.cjl.z"

    def decode(self, b: dict[str, Any], cat: CatalogSnapshot, vid: int, persist: bool = False,
               bypass: bool = False) -> DecodeResult:
        """`bypass`: decode the raw spool even when cached (re-decode / reprocess) and refresh the cache."""
        k = self.key(b, vid)
        hit = None if bypass else self._mem.get(k)
        if hit is not None:
            self._mem.move_to_end(k)
            self.hits += 1
            return hit[0]
        res = None if bypass else self._load(k, b["id"])
        if res is None:
            self.misses += 1
            res = self._decode_raw(b, cat)
            if (persist or bypass) and cache_mode() != "off":
                blob = _dump(res)
                if blob is not None:
                    try:
                        get_store().put(k, blob)
                    except Exception:  # noqa: BLE001 - the cache is an optimisation, never a failure
                        log.warning("decoded cache write failed", extra={"component": "alelt_hub", "batch_id": str(b["id"])})
        else:
            self.hits += 1
        self._remember(k, res)
        return res

    def _decode_raw(self, b: dict[str, Any], cat: CatalogSnapshot) -> DecodeResult:
        raw = decompress(read_ref(b["spool_path"]), b["codec"])
        if sha256_hex(raw) != b["sha256"]:
            from .pipeline import PipelineError

            raise PipelineError(f"spool corrupt for batch {b['id']}")
        REGISTRY.inc("alelt_decode_raw_total", connection_id=self.cid)
        return self.source.decode(raw, cat, b["id"])  # type: ignore[no-any-return]

    def _load(self, k: str, batch_id: Any) -> DecodeResult | None:
        if cache_mode() == "off":
            return None
        try:
            data = get_store().get(k)
        except Exception:  # noqa: BLE001 - missing / unreadable: decode from the raw spool
            return None
        return _load(data, self.engine_name, batch_id)

    def _remember(self, k: str, res: DecodeResult) -> None:
        n = _res_bytes(res)
        if n > self.mem_bytes:
            return
        self._mem[k] = (res, n)
        self._n += n
        while self._n > self.mem_bytes and len(self._mem) > 1:
            _, (_old, m) = self._mem.popitem(last=False)
            self._n -= m


_DML = ("INSERT", "UPDATE", "DELETE")


def drift_check(res: DecodeResult, tables: dict[TableRef, TableInfo], policy: Any) -> DecodeResult:
    """Schema drift is never silent (REQ-26): a change with a column the catalog version does not know, or of an
    in-scope table that catalog does not contain, is quarantined against its table (hold -> catalog refresh ->
    re-decode, alelt_hub.integrity) instead of being dropped by the loader."""
    keep: list[ChangeEvent] = []
    q: list[QuarantineRecord] = []
    for e in res.events:
        if e.table is not None and e.kind.value in _DML:
            info = tables.get(e.table)
            if info is None:
                if policy.table(e.table).enabled:
                    q.append(QuarantineRecord(position=e.position, table=e.table, raw_ref=e.raw_ref,
                                              reason="table not in catalog (catalog refresh needed)"))
                    continue
            else:
                known = {c.name for c in info.columns}
                extra = sorted({k for img in (e.after, e.before) for k in (img or {}) if k not in known})
                if extra:
                    q.append(QuarantineRecord(position=e.position, table=e.table, raw_ref=e.raw_ref,
                                              reason=f"column not in catalog: {','.join(extra[:5])} "
                                                     "(schema drift, catalog refresh needed)"))
                    continue
        keep.append(e)
    if not q:
        return res
    return DecodeResult(events=keep, quarantined=[*res.quarantined, *q])


def signal_records(events: list[ChangeEvent], include_keyed: bool = False) -> list[QuarantineRecord]:
    """Source completeness signals (alelt_common.completeness, X-132) as quarantine-equivalent records: a change the
    source could not decode completely. Integrity treats them like a quarantined record of their table.
    `needs_keyed_reread` signals WITH keys are completion work (alelt_hub.completion), not integrity records: left out
    unless `include_keyed`."""
    out: list[QuarantineRecord] = []
    for e in events:
        sig = signal_of(e)
        if sig is None:
            continue
        if not include_keyed and sig.cls is RecordClass.NEEDS_KEYED_REREAD and sig.keys and sig.table is not None:
            continue
        out.append(QuarantineRecord(position=e.position, table=sig.table, raw_ref=e.raw_ref,
                                    reason=f"signal {sig.cls.value}: {sig.reason}"[:500],
                                    raw_hex=(e.note or "")[:256].encode().hex()[:512]))
    return out


def _dump(res: DecodeResult) -> bytes | None:
    try:
        parts = [json.dumps({"v": 1, "n": len(res.events), "q": len(res.quarantined)}).encode() + b"\n"]
        parts += [encode_event(e) for e in res.events]
        parts += [q.model_dump_json().encode() + b"\n" for q in res.quarantined]
        return zlib.compress(b"".join(parts), 1)
    except (TypeError, ValueError):
        return None                     # a value without a canonical form: not cached, decoded again when needed


def _load(data: bytes, engine: str, batch_id: Any) -> DecodeResult | None:
    try:
        raw = zlib.decompress(data)
        head, _, rest = raw.partition(b"\n")
        h = json.loads(head)
        lines = rest.splitlines(keepends=True)
        n = int(h["n"])
        dr = decode_batch(b"".join(lines[:n]), engine, batch_id)
        if dr.quarantined or len(dr.events) != n:
            return None
        q = [QuarantineRecord.model_validate_json(x) for x in lines[n:] if x.strip()]
        return DecodeResult(events=dr.events, quarantined=q)
    except Exception:  # noqa: BLE001 - corrupt cache object: decode from the raw spool instead
        return None


# ------------------------------------------------------------------ one pass over the spool for many tables
@dataclass
class Target:
    ref: TableRef
    start: str | None                   # apply commits > start (>= with inclusive); None = from the first batch
    inclusive: bool = False
    scan_from_seq: int | None = None    # first batch to feed the assembler: the main stream's restart_seq when the
    #                                     table was detached (HC-09: open txns rebuilt completely); None = lookback


@dataclass
class PassResult:
    stream: str
    applied: dict[str, int] = field(default_factory=dict)
    quarantined: dict[str, list[QuarantineRecord]] = field(default_factory=dict)
    to_pos: str | None = None
    upto_seq: int = 0
    batches: int = 0
    decoded_raw: int = 0
    seconds: float = 0.0
    aborted: bool = False
    reread_keys: dict[str, list[Any]] = field(default_factory=dict)   # fqn -> completion.Signal committed in range


def mirror(p: Any) -> tuple[int, str | None]:
    """(last_seq, position) the main stream made durable at the destination (metadata, any process)."""
    seq, pos, _ = mirror3(p)
    return seq, pos


def mirror3(p: Any) -> tuple[int, str | None, int | None]:
    """(last_seq, position, restart_seq): restart_seq = first batch of the oldest transaction open at that point."""
    with p.engine.connect() as c:
        r = c.execute(select(checkpoint_mirror.c.last_seq, checkpoint_mirror.c.position, checkpoint_mirror.c.restart_seq)
                      .where(checkpoint_mirror.c.stream == p.stream)).first()
    return (int(r[0]), r[1], int(r[2]) if r[2] is not None else None) if r else (0, None, 0)  # nothing flushed yet


_BATCH_COLS = (batch.c.id, batch.c.seq, batch.c.from_lri, batch.c.to_lri, batch.c.spool_path, batch.c.codec,
               batch.c.sha256, batch.c.catalog_version_id, batch.c.status)


def _scan_rows(p: Any, targets: list[Target], starts: dict[TableRef, tuple[Position | None, bool]], upto_seq: int
               ) -> list[dict[str, Any]]:
    """Batches to feed: from the smallest per-target scan start up to upto_seq. Targets with a recorded
    restart_seq bound the metadata scan (HC-05); others fall back to `catchup_lookback` batches before the first
    batch covering their start position."""
    cmp, eng = p.source.compare, p.engine_name
    lo: int | None = None
    need_scan = [t for t in targets if t.scan_from_seq is None]
    if need_scan:
        with p.engine.connect() as c:
            light = c.execute(select(batch.c.seq, batch.c.to_lri).where(
                batch.c.connection_id == p.cid, batch.c.seq <= upto_seq).order_by(batch.c.seq)).all()
        for t in need_scan:
            st = starts[t.ref][0]
            idx = 0 if st is None else next((i for i, (_s, to) in enumerate(light)
                                             if cmp(Position(engine=eng, text=to), st) >= 0), None)
            if idx is None:
                continue
            first = int(light[max(0, idx - p.s.catchup_lookback)][0])
            lo = first if lo is None else min(lo, first)
    for t in targets:
        if t.scan_from_seq is not None:
            lo = t.scan_from_seq if lo is None else min(lo, t.scan_from_seq)
    if lo is None:
        return []
    with p.engine.connect() as c:
        return [dict(r) for r in c.execute(select(*_BATCH_COLS).where(
            batch.c.connection_id == p.cid, batch.c.seq >= lo, batch.c.seq <= upto_seq).order_by(batch.c.seq)).mappings()]


def run(p: Any, targets: list[Target], stream: str, upto_seq: int, *, latest_catalog: bool = False,
        strict: bool = False, bypass_cache: bool = False, watermarks: dict[str, str | None] | None = None,
        max_buffer_rows: int | None = None, enqueue_completion: bool = True) -> PassResult:
    """Replay `targets` from their start positions up to batch `upto_seq` (inclusive) in ONE pass into `stream`.

    latest_catalog: decode with the newest catalog version (re-decode after a catalog refresh, F-07).
    strict: nothing is applied when a record of a target table inside its range is quarantined again (decode-only
            pre-scan before anything is flushed, HC-14).
    bypass_cache: decode the raw spool again (decoder fix / reprocess, HC-02) and refresh the cache.
    Out-of-row signals (needs_keyed_reread with keys) of target tables committed inside the range are returned in
    `reread_keys` and, with `enqueue_completion` (default), queued for completion after the pass flushed
    (alelt_hub.completion): catch-up, window release, deferred rounds, re-decode and reprocess all complete them.
    Signals committed before a target's start are not in range and are dropped (the snapshot rows carry the value)."""
    from .completion import Signal, SignalTracker, enqueue_signals

    t0 = time.monotonic()
    reapply = bypass_cache          # reprocess / strict re-decode: the CURRENT source policy (deliberate re-apply)
    out = PassResult(stream=stream, upto_seq=upto_seq)
    if not targets:
        return out
    cmp, eng_name = p.source.compare, p.engine_name
    starts = {t.ref: (Position(engine=eng_name, text=t.start) if t.start else None, t.inclusive) for t in targets}
    with p.engine.connect() as c:
        last = c.execute(select(batch.c.to_lri).where(batch.c.connection_id == p.cid, batch.c.seq <= upto_seq)
                         .order_by(batch.c.seq.desc()).limit(1)).scalar()
    if last is None:
        return out
    out.to_pos = str(last)
    rows = _scan_rows(p, targets, starts, upto_seq)
    if not rows:
        return out

    def in_range(ref: TableRef, pos: Position | None, batch_to: str) -> bool:
        st, inc = starts[ref]
        if st is None:
            return True
        at = pos or Position(engine=eng_name, text=batch_to)
        c_ = cmp(at, st)
        return c_ >= 0 if inc else c_ > 0
    latest = p.latest_catalog_vid() if latest_catalog else None
    refs = set(starts)
    tables: dict[TableRef, TableInfo] = {}
    misses0 = p.cache.misses

    def decoded(b: dict[str, Any], again: bool) -> DecodeResult:
        vid = latest if latest is not None else p.catalog_vid_for(b)
        cat = p._catalog(vid)
        infos = p.table_infos_for(vid, cat)
        tables.update(infos)
        return drift_check(p.cache.decode(b, cat, vid, persist=True, bypass=again), infos, p.policy)
    if strict:                                   # decide before anything is applied
        for b in rows:
            dr = decoded(b, bypass_cache)
            for q in [*dr.quarantined, *signal_records(dr.events)]:
                if q.table is not None and q.table in refs and in_range(q.table, q.position, b["to_lri"]):
                    out.quarantined.setdefault(q.table.fqn, []).append(q)
        if out.quarantined:
            out.aborted = True
            out.decoded_raw = p.cache.misses - misses0
            out.seconds = time.monotonic() - t0
            return out
        bypass_cache = False                     # the pre-scan refreshed the cache
    ck = p.dest.read_checkpoint(stream)
    asm = TxnAssembler(p.s.txn_byte_budget, p.s.spool_dir / "txn-spill" / f"{p.cid}-pass-{uuid.uuid4().hex[:8]}")
    ld = Loader(p.dest, p.policy, stream, cmp, max_rows=10**12, max_latency_s=10**9, only=refs)
    cap = max_buffer_rows or int(os.environ.get("ALELT_REPLAY_FLUSH_ROWS", "200000"))
    tracker = SignalTracker()
    sigs: list[Signal] = []
    try:
        for b in rows:
            res = decoded(b, bypass_cache)
            for q in [*res.quarantined, *signal_records(res.events)]:
                if q.table is not None and q.table in refs and in_range(q.table, q.position, b["to_lri"]):
                    out.quarantined.setdefault(q.table.fqn, []).append(q)
            tracker.feed(res.events)
            # other replays: the policy revision of the batch's first decode (XC-30)
            events, _ = p._apply_source_policy(res.events, tables, None if reapply else b)
            keep: list[ChangeSet] = []
            sets = asm.feed(events, int(b["seq"]))
            for sg in tracker.committed(sets, int(b["seq"])):     # out-of-row values committed inside the range
                ref = _ref(sg.fqn)
                if ref in refs and in_range(ref, Position(engine=eng_name, text=sg.pos), b["to_lri"]):
                    out.reread_keys.setdefault(sg.fqn, []).append(sg)
                    sigs.append(sg)
            for cs in sets:
                if ck is not None and cmp(cs.commit_position, ck) <= 0:
                    continue                    # this pass already applied it (resumed pass)
                evs = [e for e in cs.events if e.table in refs and in_range(e.table, cs.commit_position, b["to_lri"])]
                if evs:
                    keep.append(cs.model_copy(update={"events": evs}))
                    for e in evs:
                        out.applied[e.table.fqn] = out.applied.get(e.table.fqn, 0) + 1  # type: ignore[union-attr]
            ld.add(keep, Position(engine=eng_name, text=b["to_lri"]))
            out.batches += 1
            if ld.buf.rows >= cap:
                ld.flush(tables, batch_id=f"{stream}:{b['seq']}")
        if ld.pending():
            flush_with_watermarks(ld, tables, f"{stream}:{upto_seq}", watermarks, eng_name)
    finally:
        asm.reset()
        out.decoded_raw = p.cache.misses - misses0
        out.seconds = time.monotonic() - t0
    if sigs and enqueue_completion:
        enqueue_signals(p.engine, p, sigs)
    REGISTRY.observe("alelt_replay_pass_seconds", out.seconds, connection_id=p.cid)
    log.info("replay pass applied", extra={"component": "alelt_hub", "correlation_id":
                                           f"stream={stream} tables={len(refs)} batches={out.batches} "
                                           f"events={sum(out.applied.values())} raw_decodes={out.decoded_raw} "
                                           f"completions={len(sigs)} s={out.seconds:.2f}"})
    return out


def flush_with_watermarks(ld: Loader, tables: dict[TableRef, TableInfo], batch_id: str,
                          watermarks: dict[str, str | None] | None, engine_name: str) -> Any:
    """Loader flush. Per-table watermarks follow the SPI default (stream K, ADR-005): every destination table of the
    flush gets watermark = to_pos in the stream's checkpoint row, in the same commit. Held tables are never in a
    main-stream flush, so their main-stream watermark stays at the hold position by construction; a pass stream's
    row carries the watermarks of the tables it replayed. Applied position of a table = max over its streams'
    watermarks. `watermarks` is accepted for call-site compatibility and not sent."""
    return ld.flush(tables, batch_id=batch_id)


def wm_stream(cid: str, fqn: str) -> str:
    return f"{cid}#t:{fqn}"


# ------------------------------------------------------------------ pass orchestration over table_hold
def pass_of(r: dict[str, Any]) -> dict[str, Any] | None:
    return ((r.get("body") or {}).get("pass")) or None


def mark_pass(p: Any, items: list[tuple[str, str]], then: str, enqueue: bool = True) -> str | None:
    """Put (fqn, kind) hold rows into one new pass. `then`: 'attach' (rejoin the main stream after the bulk phase) or
    'rehold' (deferred round: stay held, watermark advanced). Rows already in a pass are left alone."""
    if not items:
        return None
    pid = uuid.uuid4().hex[:12]
    marked = 0
    for fqn, kind in items:
        r = holds.get(p.engine, p.cid, fqn, kind)
        if r is None or pass_of(r):
            continue
        holds.modify(p.engine, p.cid, fqn, kind, **{"pass": {"id": pid, "phase": "bulk", "then": then,
                                                             "marked_at": utcnow().isoformat()}})
        marked += 1
    if not marked:
        return None
    REGISTRY.inc("alelt_replay_passes_total", connection_id=p.cid, then=then)
    if enqueue:
        with p.engine.begin() as c:
            jobs.enqueue(c, "replay", {"connection_id": p.cid, "pass_id": pid}, dedup_key=f"replay:{p.cid}:{pid}")
    return pid


def _pass_rows(p: Any, pid: str) -> list[dict[str, Any]]:
    return [r for r in holds.rows(p.engine, p.cid) if (pass_of(r) or {}).get("id") == pid]


def _table_start(p: Any, fqn: str) -> str | None:
    return _target(p, fqn).start


def _target(p: Any, fqn: str) -> Target:
    """Table watermark: smallest start_pos of every held row of the table (None = from the first batch) and the
    smallest recorded restart_seq (scan start that rebuilds every transaction open at the watermark, HC-09)."""
    rs = [r for r in holds.rows(p.engine, p.cid) if r["table_fqn"] == fqn and r["held"]]
    starts = [r["start_pos"] for r in rs]
    seqs = [(r["body"] or {}).get("restart_seq") for r in rs]
    scan = None if not seqs or any(x is None for x in seqs) else min(int(x) for x in seqs)
    if not starts or any(x is None for x in starts):
        return Target(_ref(fqn), None, scan_from_seq=scan if starts else None)
    cmp = p.source.compare
    best = starts[0]
    for x in starts[1:]:
        if cmp(Position(engine=p.engine_name, text=x), Position(engine=p.engine_name, text=best)) < 0:
            best = x
    return Target(_ref(fqn), best, scan_from_seq=scan)


def _drop_checkpoint(p: Any, stream: str) -> None:
    """Pass streams are transient (HC-21): once the hub recorded the pass, destinations that can delete a checkpoint
    row (`delete_checkpoint`, stream K) drop it; others keep it (harmless, one row per pass)."""
    fn = getattr(p.dest, "delete_checkpoint", None)
    if callable(fn):
        try:
            fn(stream)
        except Exception:  # noqa: BLE001
            log.warning("pass checkpoint not deleted", extra={"component": "alelt_hub", "correlation_id": stream})


def run_pass(p: Any, pid: str) -> PassResult | None:
    """Bulk phase of pass `pid` (replay job). Resumable: the range is persisted before anything is applied."""
    from . import integrity

    rows = [r for r in _pass_rows(p, pid) if pass_of(r)["phase"] == "bulk"]   # type: ignore[index]
    if not rows:
        return None
    p.refresh_policy()                          # same column policy as the main stream (HC-20)
    pinfo = pass_of(rows[0]) or {}
    if pinfo.get("upto_seq") is None:
        seq, pos, rseq = mirror3(p)
        for r in rows:
            holds.modify(p.engine, p.cid, r["table_fqn"], r["kind"],
                         **{"pass": {**(pass_of(r) or {}), "upto_seq": seq, "upto_pos": pos, "upto_restart": rseq}})
        pinfo = {**pinfo, "upto_seq": seq, "upto_pos": pos, "upto_restart": rseq}
    seq, pos, rseq = int(pinfo["upto_seq"]), pinfo.get("upto_pos"), pinfo.get("upto_restart")
    fqns = sorted({r["table_fqn"] for r in rows})
    blocked = {f for f in fqns if integrity.blocks_replay(
        p.engine, p.cid, f, tuple(r["kind"] for r in rows if r["table_fqn"] == f))}
    targets = [_target(p, f) for f in fqns if f not in blocked]
    wm = {wm_stream(p.cid, f): pos for f in fqns if f not in blocked}
    res = run(p, targets, f"{p.cid}#pass:{pid}", seq, watermarks=wm,
              latest_catalog=bool(pinfo.get("latest_catalog")), strict=bool(pinfo.get("strict")))
    if res.aborted:
        for r in rows:
            holds.modify(p.engine, p.cid, r["table_fqn"], r["kind"], drop=("pass",))
        integrity.pass_aborted(p, rows, res)
        return res
    for r in rows:
        f = r["table_fqn"]
        if f in blocked:                           # integrity hold: table stays held at its watermark
            holds.modify(p.engine, p.cid, f, r["kind"], drop=("pass",))
            continue
        advance_watermark(p, f, pos, rseq)
        info = pass_of(holds.get(p.engine, p.cid, f, r["kind"]) or r) or {}
        if info.get("then") == "attach":
            holds.modify(p.engine, p.cid, f, r["kind"], **{"pass": {**info, "phase": "attach"}})
        else:
            holds.modify(p.engine, p.cid, f, r["kind"], drop=("pass",), last_round={
                "at": utcnow().isoformat(), "upto_pos": pos, "events": res.applied.get(f, 0),
                "seconds": round(res.seconds, 3)})
    _drop_checkpoint(p, f"{p.cid}#pass:{pid}")
    if any((pass_of(r) or {}).get("then") == "attach" for r in rows):
        with p.engine.begin() as c:             # the attach tail runs in the next decode slice (pipeline lock)
            jobs.mark_dirty(c, p.cid)
    return res


def advance_watermark(p: Any, fqn: str, pos: str | None, restart_seq: int | None = None) -> None:
    """Every held row of the table now starts at `pos` (everything up to it is applied for the table); the scan start
    moves to the main stream's restart_seq at that point (atomic row updates, HC-11)."""
    if pos is None:
        return
    for r in holds.rows(p.engine, p.cid):
        if r["table_fqn"] == fqn and r["held"]:
            if restart_seq is None:
                holds.modify(p.engine, p.cid, fqn, r["kind"], start_pos=pos, drop=("restart_seq",))
            else:
                holds.modify(p.engine, p.cid, fqn, r["kind"], start_pos=pos, restart_seq=int(restart_seq))


def attach_ready(p: Any, held: set[str]) -> list[str]:
    """Attach phase (decode job, pipeline lock held, main stream idle): replay the tail of every table whose bulk
    phase is done and release its holds. Returns the tables re-attached."""
    from . import integrity

    rows = [r for r in holds.rows(p.engine, p.cid) if (pass_of(r) or {}).get("phase") == "attach"]
    if not rows:
        return []
    upto_seq, upto_pos, upto_restart = mirror3(p)    # under the pipeline lock: = what the main stream flushed
    upto = p.flushed_seq if p.flushed_seq is not None else upto_seq
    by_pass: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by_pass.setdefault(pass_of(r)["id"], []).append(r)   # type: ignore[index]
    done: list[str] = []
    cap = int(os.environ.get("ALELT_ATTACH_TAIL_MAX_BATCHES", "200"))
    for pid, rs in by_pass.items():
        fqns = sorted({r["table_fqn"] for r in rs})
        info = pass_of(rs[0]) or {}
        if info.get("upto_seq") is not None and int(upto) - int(info["upto_seq"]) > cap:
            # a long tail would hold the pipeline lock too long: another bulk round first (replay lane), then attach
            for r in rs:
                holds.modify(p.engine, p.cid, r["table_fqn"], r["kind"], **{"pass": {
                    **(pass_of(r) or {}), "phase": "bulk", "upto_seq": None, "upto_pos": None, "upto_restart": None}})
            with p.engine.begin() as c:
                jobs.enqueue(c, "replay", {"connection_id": p.cid, "pass_id": pid},
                             dedup_key=f"replay:{p.cid}:{pid}:{int(upto)}")
            continue
        blocked = {f for f in fqns if integrity.blocks_replay(
            p.engine, p.cid, f, tuple(r["kind"] for r in rs if r["table_fqn"] == f))}
        go = [f for f in fqns if f not in blocked]
        targets = [_target(p, f) for f in go]
        res = run(p, targets, f"{p.cid}#pass:{pid}.attach", int(upto), latest_catalog=bool(info.get("latest_catalog")),
                  strict=bool(info.get("strict")), watermarks={wm_stream(p.cid, f): None for f in go})
        if res.aborted:
            for r in rs:
                holds.modify(p.engine, p.cid, r["table_fqn"], r["kind"], drop=("pass",))
            integrity.pass_aborted(p, rs, res)
            continue
        for r in rs:
            f = r["table_fqn"]
            if f in blocked:
                holds.modify(p.engine, p.cid, f, r["kind"], drop=("pass",))
                continue
            release(p, f, r["kind"], held, upto_pos, upto_restart)
            done.append(f)
        integrity.attached(p, rs, res)
        _drop_checkpoint(p, f"{p.cid}#pass:{pid}.attach")
    return sorted(set(done))


def release(p: Any, fqn: str, kind: str, held: set[str], upto_pos: str | None,
            upto_restart: int | None = None) -> None:
    """Release one hold row (the table was replayed through `upto_pos` under the pipeline lock)."""
    from . import integrity, schedule

    r = holds.get(p.engine, p.cid, fqn, kind)
    if r is None:
        return
    state = {schedule.KIND: "OPEN", integrity.KIND: integrity.IN_SYNC, "schema": "APPLIED"}.get(kind, r["state"])
    extra: dict[str, Any] = {}
    if kind == "snapshot":                         # catch-up done: the table rejoins the main stream here
        extra["catchup_done"] = (r["body"] or {}).get("snapshot_id")
    holds.modify(p.engine, p.cid, fqn, kind, state=state, held=False, start_pos=None,
                 drop=("pass", "reread", "restart_seq"), released_at=utcnow().isoformat(), **extra)
    others = [x for x in holds.rows(p.engine, p.cid) if x["table_fqn"] == fqn and x["held"]]
    if others:
        advance_watermark(p, fqn, upto_pos, upto_restart)
    else:
        held.discard(fqn)
    if kind == "snapshot":
        from . import lifecycle

        lifecycle.caught_up(p, fqn, held)


def resume_passes(p: Any) -> list[str]:
    """Re-enqueue bulk phases left by a crashed / restarted worker (idempotent: dedup per pass id)."""
    pids = sorted({pass_of(r)["id"] for r in holds.rows(p.engine, p.cid)   # type: ignore[index]
                   if (pass_of(r) or {}).get("phase") == "bulk"})
    if pids:
        with p.engine.begin() as c:
            for pid in pids:
                jobs.enqueue(c, "replay", {"connection_id": p.cid, "pass_id": pid},
                             dedup_key=f"replay:{p.cid}:{pid}:{int(time.time() // 60)}")
    return pids


__all__ = ["DecodedCache", "PassResult", "ReplayError", "Target", "attach_ready", "decoder_fingerprint", "mark_pass",
           "mirror", "mirror3", "run", "run_pass", "release", "resume_passes"]
