"""Decoder output (PlainEvent / PlainQuarantine) -> canonical ChangeEvents for the remote-capture runner.

Implements connectors/sources/_spi/REMOTE.md S2-S7 on top of decode.BinlogDecoder:
- one transaction at a time (the binlog never interleaves): DML / TRUNCATE / DDL buffered until the commit, then
  emitted contiguously followed by COMMIT;
- 'ROLLBACK TO SAVEPOINT' (decoder COMPENSATION) removes the newest buffered DML here, so no COMPENSATION and no
  rolled-back row ever leaves the connector;
- explicit ROLLBACK (non-transactional tables, XA) -> ROLLBACK event only (DML dropped);
- a transaction cut by a server crash (decoder ROLLBACK with length 0) is dropped silently: its position is not a
  safe resume token;
- transactions that touch only non-captured tables (nothing buffered) are not emitted, except one empty COMMIT
  every `empty_commit_s` so the durable position keeps following the source;
- decoder quarantine records and values without canonical form (rows.Unconvertible) become lines carrying
  alelt_common.canonical_jsonl.Undecodable values => the hub quarantines them with their raw bytes (R7);
- decoder signals (alelt_common/completeness.py) become signal events inside their transaction (kept on ROLLBACK);
- row images without every catalog column (binlog_row_image=MINIMAL / NOBLOB): an UPDATE that keeps its key is
  decoded with the documented "unchanged" semantics (missing key columns of the after image are taken from the
  before image); an INSERT, or an UPDATE that changes the key, is emitted AND re-read by key (needs_keyed_reread):
  the missing values are never guessed.
"""
from __future__ import annotations

import itertools

import logging
import time
from collections.abc import Callable, Iterator
from typing import Any

from alelt_common import completeness as FC
from alelt_common.canonical_jsonl import Undecodable
from alelt_common.models import ChangeEvent, ChangeKind, Position, TableRef

from .decode import DecodeOut, PlainEvent, PlainQuarantine, PlainSignal
from .rows import Unconvertible

log = logging.getLogger("alelt_src_mysql")
DML = frozenset({"INSERT", "UPDATE", "DELETE"})


def _vals(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {k: Undecodable(str(v)) if isinstance(v, Unconvertible) else v for k, v in row.items()}


from alelt_common.spill import SpillBuffer, SpillLimitExceeded  # noqa: E402

# HC-08: an open transaction is no longer limited by memory: it spills to disk (alelt_common.spill) and is delivered
# lazily; only the disk cap (ALELT_CAPTURE_MAX_SPILL_MB) stops capture (SpillLimitExceeded -> runner stop + incident).
TransactionTooLarge = SpillLimitExceeded


class TxnAssembler:
    def __init__(self, engine: str, *, empty_commit_s: float = 30.0,
                 captured_schemas: set[str] | None = None,
                 columns_of: Callable[[str, str], list[str] | None] | None = None,
                 clock: Callable[[], float] = time.monotonic,
                 keys_of: Callable[[str, str], list[str] | None] | None = None,
                 census: FC.Census | None = None) -> None:
        self.engine = engine
        self.keys_of = keys_of
        self.census = census
        self.empty_commit_s = empty_commit_s
        self.captured_schemas = {s.lower() for s in captured_schemas} if captured_schemas else None
        self.columns_of = columns_of
        self.clock = clock
        self.buf = SpillBuffer(label="mysql")
        self.orphans: list[ChangeEvent] = []      # quarantine lines seen outside a transaction
        self.last_emit = clock()
        self.dropped_txns = 0
        self.compensated = 0
        self._reread: FC.SourceSignal | None = None

    # ------------------------------------------------------------------ conversion
    def _pos(self, text: str | None) -> Position:
        return Position(engine=self.engine, text=text or "")

    def _event(self, e: PlainEvent, kind: ChangeKind) -> ChangeEvent:
        before, after, note = _vals(e.before), _vals(e.after), e.note
        self._reread = None
        ref = TableRef(schema=e.schema, name=e.table) if e.schema and e.table else None
        if kind in (ChangeKind.INSERT, ChangeKind.UPDATE) and after is not None and self.columns_of and ref:
            cols = self.columns_of(ref.schema_, ref.name) or []
            keys = (self.keys_of(ref.schema_, ref.name) if self.keys_of else None) or []
            if kind is ChangeKind.UPDATE and before is not None:
                for k in keys:                            # MINIMAL: an unchanged key is only in the before image
                    if k not in after and k in before:
                        after[k] = before[k]
            undoc = list(dict.fromkeys(n[len("reread:"):] for n in (note or "").split("; ") if n.startswith("reread:")))
            if self.census is not None and "json_decoder_failed:" in (note or ""):
                self.census.count("plugin:json_binary_decoder", FC.RecordClass.NEEDS_KEYED_REREAD,
                                  "JSON decoder plugin failed: row re-read")
            missing = [c for c in cols if c not in after and c not in undoc]
            if undoc:                                     # value form not documented: never guessed, re-read
                key = {k: after[k] for k in keys if k in after}
                why = f"{kind.value}: value form of column(s) {','.join(undoc)} not documented for row images"
                self._reread = (FC.keyed_reread(ref, [key], why, f"binlog:{kind.value.lower()}", columns=undoc)
                                if keys and len(key) == len(keys) else FC.unsupported(ref, why + "; key not available"))
            if missing:
                note = ";".join(filter(None, [note] + [f"unchanged:{c}" for c in missing]))
                moved = kind is ChangeKind.UPDATE and before is not None and any(
                    k in before and before.get(k) != after.get(k) for k in keys)
                if kind is ChangeKind.INSERT or moved:    # the destination has no row to keep the values from
                    key = {k: after[k] for k in keys if k in after}
                    why = (f"{kind.value} row image without column(s) {','.join(missing)} "
                           "(binlog_row_image MINIMAL/NOBLOB)")
                    self._reread = (FC.keyed_reread(ref, [key], why, f"binlog:{kind.value.lower()}", columns=missing)
                                    if keys and len(key) == len(keys) and not any(
                                        isinstance(v, Undecodable) for v in key.values())
                                    else FC.unsupported(ref, why + "; key not available"))
        return ChangeEvent(position=self._pos(e.position), txid=e.txid, kind=kind,
                           table=ref, before=before, after=after, commit_ts=e.commit_ts, note=note)

    def _quarantine(self, q: PlainQuarantine, txid: str) -> ChangeEvent:
        reason = f"{q.reason} | event bytes {q.raw_hex}"
        return ChangeEvent(position=self._pos(q.position), txid=txid, kind=ChangeKind.OTHER,
                           table=TableRef(schema=q.schema, name=q.table) if q.schema and q.table else None,
                           after={"_alelt_undecodable": Undecodable(reason[:4000])}, note=q.reason[:500])

    def _captured_ddl(self, e: PlainEvent) -> bool:
        return self.captured_schemas is None or (e.schema or "").lower() in self.captured_schemas

    # ------------------------------------------------------------------ feeding
    def feed(self, out: DecodeOut, open_txid: str | None = None) -> list[ChangeEvent]:
        """Events of one decoder feed() call -> ChangeEvents ready to yield (possibly none).
        `open_txid` = decoder's open transaction id (used to attach quarantine lines)."""
        res: list[ChangeEvent] = []
        txid = open_txid or (out.events[0].txid if out.events else None)
        for q in out.quarantined:
            if txid:
                self.buf.append(self._quarantine(q, txid))
            else:
                self.orphans.append(self._quarantine(q, "orphan"))
        for s in out.signals:
            sev = s.signal.event(self._pos(s.position), txid or "orphan")
            (self.buf if txid else self.orphans).append(sev)
        for e in out.events:
            k = e.kind
            if k in DML:
                self.buf.append(self._event(e, ChangeKind(k)))
                if self.census is not None and any(isinstance(v, Unconvertible) for r in (e.before, e.after) if r
                                                   for v in r.values()):
                    self.census.count(f"row:{k.lower()}", FC.RecordClass.UNSUPPORTED,
                                      "value without canonical form (quarantined)")
                if self._reread is not None:
                    self.buf.append(self._reread.event(self._pos(e.position), e.txid))
                    if self.census is not None:
                        self.census.count(f"row:{k.lower()}", self._reread.cls, "row image without every column")
            elif k == "TRUNCATE":
                self.buf.append(self._event(e, ChangeKind.TRUNCATE))
            elif k == "DDL":
                if self._captured_ddl(e):
                    self.buf.append(self._event(e, ChangeKind.DDL))
            elif k == "COMPENSATION":
                self._compensate(e)
            elif k == "COMMIT":
                got = self._commit(e)
                if isinstance(got, list):
                    res.extend(got)
                else:                              # spilled transaction: delivered lazily (bounded memory)
                    return _chain(res, got, self, out, open_txid, e)
            elif k == "ROLLBACK":
                res.extend(self._rollback(e))
        return res

    def _compensate(self, e: PlainEvent) -> None:
        tail = self.buf.tail
        for i in range(len(tail) - 1, -1, -1):
            ev = tail[i]
            if ev.kind.value in DML:
                if ev.table is not None and (ev.table.schema_, ev.table.name) != (e.schema, e.table):
                    log.warning("savepoint rollback table mismatch (%s.%s vs %s)", e.schema, e.table, ev.table.fqn,
                                extra={"component": "alelt_src_mysql", "position": e.position})
                self.buf.pop_tail(i)
                self.compensated += 1
                return
        if self.buf.spilled:                       # the undone row is in the spilled part: never guessed
            ref = TableRef(schema=e.schema, name=e.table) if e.schema and e.table else None
            self.buf.append(FC.unsupported(ref, "savepoint rollback of a row in the spilled part of a large "
                                                "transaction: hold + resync", "binlog:rollback_to").event(
                self._pos(e.position), e.txid))

    def _take(self, txid: str) -> list[ChangeEvent] | Iterator[ChangeEvent]:
        orph = [o.model_copy(update={"txid": txid}) for o in self.orphans]
        buf, self.orphans = self.buf, []
        self.buf = SpillBuffer(label="mysql")
        if not buf.spilled:
            evs = orph + list(buf.tail)
            buf.close()
            return evs
        return itertools.chain(orph, buf.drain())

    def _commit(self, e: PlainEvent) -> list[ChangeEvent] | Iterator[ChangeEvent]:
        empty = not self.orphans and not self.buf
        evs = self._take(e.txid)
        now = self.clock()
        if empty and now - self.last_emit < self.empty_commit_s:
            return []                              # nothing captured in this txn: skip (position catches up later)
        self.last_emit = now
        commit = ChangeEvent(position=self._pos(e.position), txid=e.txid, kind=ChangeKind.COMMIT, commit_ts=e.commit_ts)
        if isinstance(evs, list):
            return evs + [commit]
        return itertools.chain(evs, [commit])

    def _rollback(self, e: PlainEvent) -> list[ChangeEvent]:
        evs = self._take(e.txid)
        keep = [x for x in evs if x.kind is ChangeKind.OTHER]       # quarantine lines / signals are never dropped
        if e.length == 0:                          # cut by a server crash: no resume token, nothing emitted
            self.dropped_txns += 1
            if self.census is not None:
                self.census.count("txn", FC.RecordClass.IGNORED, "transaction cut by a source restart (not committed)")
            if keep:
                self.orphans.extend(keep)
            log.info("transaction cut by a source restart discarded", extra={"component": "alelt_src_mysql",
                                                                             "correlation_id": e.txid})
            return []
        self.last_emit = self.clock()
        return keep + [ChangeEvent(position=self._pos(e.position), txid=e.txid, kind=ChangeKind.ROLLBACK)]

    @property
    def pending(self) -> bool:
        return bool(self.buf)


def _chain(head: list[ChangeEvent], lazy: Iterator[ChangeEvent], asm: "TxnAssembler", out: DecodeOut,
           open_txid: str | None, after: PlainEvent) -> Iterator[ChangeEvent]:
    """Lazy result of feed() when a spilled transaction commits: head, the spilled transaction, then the rest of
    this decoder output (events after the COMMIT, rare: the next transaction's first events)."""
    yield from head
    yield from lazy
    i = next(n for n, x in enumerate(out.events) if x is after)
    rest = DecodeOut(events=out.events[i + 1:])
    if rest.events:
        yield from asm.feed(rest, open_txid)


def replay_frames(raw: bytes, flavor: str, catalog: dict[str, Any], *, empty_commit_s: float = float("inf"),
                  captured_schemas: set[str] | None = None, json_decoder: Any = None
                  ) -> tuple[list[ChangeEvent], TxnAssembler]:
    """Decode a framed raw capture (framing.py, golden corpus) exactly as stream() does, without a server.
    `catalog` = CatalogSnapshot dict (only capture_enabled tables are decoded)."""
    from .decode import BinlogDecoder, specs_from_catalog
    from .framing import CONTEXT, EVENT, iter_frames, read_context

    specs = specs_from_catalog({"tables": [t for t in catalog.get("tables", []) if t.get("capture_enabled", True)]})
    cols = {(s.lower(), n.lower()): [c.name for c in sp.columns] for (s, n), sp in specs.items()}
    keys = {(s.lower(), n.lower()): [c.name for c in sorted(sp.columns, key=lambda c: c.key_seq or 0) if c.key_seq]
            for (s, n), sp in specs.items()}
    dec = BinlogDecoder(flavor, specs, json_decoder=json_decoder)
    dec.restore(read_context(raw))
    asm = TxnAssembler(flavor, empty_commit_s=empty_commit_s, captured_schemas=captured_schemas,
                       columns_of=lambda s, n: cols.get((s.lower(), n.lower())),
                       keys_of=lambda s, n: keys.get((s.lower(), n.lower())), census=dec.census)
    out: list[ChangeEvent] = []
    for kind, off, payload in iter_frames(raw):
        if kind == CONTEXT:
            continue
        if kind != EVENT:
            raise ValueError(f"unknown frame kind {kind} at {off}")
        d = dec.feed(payload, off)
        if d.events or d.quarantined or d.signals:
            out.extend(asm.feed(d, dec.txn.txid if dec.txn is not None else None))
    return out, asm
