"""pgoutput messages -> canonical ChangeEvents (transport independent, golden-testable).

- Non-streamed transactions arrive whole (Begin .. Commit) in commit order: events are emitted as they come.
- Streamed transactions (protocol v2 `streaming=on`) are buffered per top-level xid until Stream Commit and then
  emitted contiguously (REMOTE.md S2); Stream Abort discards the whole txn or one subtransaction.
- COMMIT position = Commit.end_lsn (resume token, REMOTE.md S3); DML positions = WAL position of the message.
- REPLICA IDENTITY: DEFAULT -> `before` = key columns (only when the key changed / on delete); FULL -> full old row.
  Unchanged TOAST values ('u') are filled from a FULL old row when present, else omitted + note `unchanged:<col>`.
- A Relation message whose columns differ from the last known shape of the table yields a DDL event first, so the
  runner refreshes the catalog before the batch is shipped.

Completeness (alelt_common/completeness.py, X-132): every message is counted in `census`:
- decoded: Begin/Commit/Relation/Insert/Update/Delete/Truncate and the stream control messages;
- ignored (documented, no row data): Type (types are resolved through pg_type), Origin (replication origin of the
  txn), logical decoding Message (pg_logical_emit_message: application payload, not table data), Stream Abort of a
  txn never seen;
- needs_keyed_reread: an unchanged-TOAST value ('u') that cannot be taken from the destination - in an INSERT (not
  documented) or in an UPDATE that changes the key with REPLICA IDENTITY DEFAULT (the loader writes a new row: the
  value would be lost) -> the change is still emitted (without the column) AND the new key is re-read;
- unsupported: DML/TRUNCATE for a relation without a Relation message in this session (cannot be decoded without its
  column list; table resolved through pg_class when possible) + needs_catalog_refresh; a streamed txn whose first
  segment was not seen (earlier changes unknown) -> connection-wide; two-phase messages (never requested).
"""
from __future__ import annotations

import itertools

import time

from collections.abc import Callable
from typing import Any

from alelt_common import completeness as FC
from alelt_common.canonical_jsonl import Undecodable
from alelt_common.spill import SpillBuffer, SpillLimitExceeded
from alelt_common.models import ChangeEvent, ChangeKind, Position, TableRef

from . import pgoutput as P
from .position import format_lsn
from .types import name_of, parse_text

ENGINE = "postgres"
TypeResolver = Callable[[list[int]], dict[int, str]]
RelationResolver = Callable[[int], "tuple[str, str] | None"]


# HC-08: streamed transactions spill to disk (alelt_common.spill) instead of stopping capture; only the disk cap
# (ALELT_CAPTURE_MAX_SPILL_MB) stops it (SpillLimitExceeded -> runner stop + incident).
TransactionTooLarge = SpillLimitExceeded


class ChangeBuilder:
    def __init__(self, resolve_types: TypeResolver | None = None,
                 shapes: dict[tuple[str, str], tuple[tuple[str, str], ...]] | None = None,
                 census: FC.Census | None = None, resolve_relation: RelationResolver | None = None,
                 fallback_codec: str | None = None, empty_commit_s: float = 0.0) -> None:
        """`shapes`: {(schema, table): ((column, engine_type), ...)} of the current catalog (DDL detection).
        `resolve_relation(oid)` -> (schema, table) from pg_class (unknown relation in this session)."""
        self.resolve_types = resolve_types
        # values arrive in UTF-8 (client_encoding=UTF8: the walsender converts). Only SQL_ASCII databases can send
        # bytes that are not UTF-8; `fallback_codec` (settings.sql_ascii_codec) decodes them, else the row is
        # quarantined (never the stream stopped, SRC-pg-01)
        self.fallback_codec = fallback_codec
        self.empty_commit_s = empty_commit_s
        self._txn_has_events = False
        self._last_empty_emit = float("-inf")
        self.census = census if census is not None else FC.Census(ENGINE)
        self.resolve_relation = resolve_relation
        self.incomplete: set[int] = set()                       # streamed txns whose first segment was not seen
        self.types: dict[int, str] = {}
        self.rel: dict[int, P.Relation] = {}
        self.shapes = dict(shapes or {})
        self.xid: int | None = None
        self.in_stream: int | None = None                       # top xid of the open stream block
        self.streams: dict[int, SpillBuffer] = {}   # top xid -> events tagged with their subxid (HC-08: spills)

    # ------------------------------------------------------------------ helpers
    def _type(self, oid: int) -> str:
        t = self.types.get(oid) or name_of(oid)
        if t is None and self.resolve_types is not None:
            self.types.update(self.resolve_types([oid]))
            t = self.types.get(oid)
        return t or f"oid:{oid}"

    def _row(self, rel: P.Relation, cells: tuple[P.Cell, ...], key_only: bool = False,
             old: dict[str, Any] | None = None, notes: list[str] | None = None) -> dict[str, Any]:
        if len(cells) != len(rel.columns):
            raise P.ProtocolError(f"{rel.namespace}.{rel.name}: {len(cells)} values for {len(rel.columns)} columns")
        out: dict[str, Any] = {}
        for col, cell in zip(rel.columns, cells):
            if key_only and not col.key:
                continue
            if cell.kind == "n":
                out[col.name] = None
            elif cell.kind == "u":
                if old is not None and col.name in old:
                    out[col.name] = old[col.name]
                elif notes is not None:
                    notes.append(f"unchanged:{col.name}")
            elif cell.kind == "t":
                raw = cell.data or b""
                try:
                    text = raw.decode("utf-8")
                except UnicodeDecodeError as e:
                    if self.fallback_codec is None:      # SQL_ASCII bytes that are not UTF-8: this row only (R7)
                        out[col.name] = Undecodable(f"invalid UTF-8 ({e.reason} at {e.start}):{raw.hex()}")
                        continue
                    text = raw.decode(self.fallback_codec)
                out[col.name] = parse_text(self._type(col.type_oid), text)
            else:
                out[col.name] = Undecodable(f"binary:{(cell.data or b'').hex()}")
        return out

    def _ref(self, relid: int) -> tuple[P.Relation, TableRef]:
        rel = self.rel.get(relid)
        if rel is None:
            raise P.ProtocolError(f"change for relation {relid} before its Relation message")
        return rel, TableRef(schema=rel.namespace or "pg_catalog", name=rel.name)

    def _unknown_relation(self, relid: int, tag: str, pos: Position, xid: int | None) -> list[ChangeEvent]:
        """A change for a relation whose Relation message was not received in this session: its column list is
        unknown, the tuple cannot be decoded (never guessed from the current catalog)."""
        name = None
        if self.resolve_relation is not None:
            try:
                name = self.resolve_relation(relid)
            except Exception:  # noqa: BLE001 - resolution is best effort; the signal is emitted either way
                name = None
        ref = TableRef(schema=name[0], name=name[1]) if name else None
        why = f"{tag} for relation oid {relid} before its Relation message"
        self.census.count(f"pgoutput:{tag}", FC.RecordClass.UNSUPPORTED, "unknown relation")
        sigs = [FC.catalog_refresh(ref, why, f"pgoutput:{tag}", relid=relid),
                FC.unsupported(ref, why + " (column list unknown: tuple not decodable)", f"pgoutput:{tag}",
                               relid=relid)]
        return [s.event(pos, self._txid(xid)) for s in sigs]

    def _count_row(self, rec: str, row: dict[str, Any]) -> None:
        if any(isinstance(v, Undecodable) for v in row.values()):   # the hub quarantines the line (R7) -> hold
            self.census.count(rec, FC.RecordClass.UNSUPPORTED, "value without canonical form (quarantined)")
        else:
            self.census.count(rec, FC.RecordClass.DECODED)

    def _key_of(self, rel: P.Relation, row: dict[str, Any]) -> dict[str, Any] | None:
        keys = [c.name for c in rel.columns if c.key]
        if not keys or any(k not in row or isinstance(row[k], Undecodable) for k in keys):
            return None
        return {k: row[k] for k in keys}

    def _toast_reread(self, rel: P.Relation, ref: TableRef, after: dict[str, Any], notes: list[str], why: str,
                      tag: str, pos: Position, xid: int | None) -> list[ChangeEvent]:
        cols = [n[len("unchanged:"):] for n in notes if n.startswith("unchanged:")]
        key = self._key_of(rel, after)
        self.census.count(f"pgoutput:{tag}", FC.RecordClass.NEEDS_KEYED_REREAD, "unchanged TOAST not recoverable")
        if key is None:
            sig = FC.unsupported(ref, f"{why}; no usable key to re-read", f"pgoutput:{tag}", columns=cols)
        else:
            sig = FC.keyed_reread(ref, [key], why, f"pgoutput:{tag}", columns=cols)
        return [sig.event(pos, self._txid(xid))]

    def _txid(self, xid: int | None) -> str:
        x = xid if xid is not None else self.xid
        return str(x) if x is not None else "0"

    # ------------------------------------------------------------------ main
    def feed(self, payload: bytes, data_start: int) -> list[ChangeEvent]:
        """One pgoutput message -> events. A transaction without any captured event (PG < 15 sends Begin/Commit for
        every transaction of the database) yields its COMMIT token at most every `empty_commit_s` (SRC-pg-07): the
        position still follows the source, without one COMMIT line per foreign transaction."""
        evs = self._feed(payload, data_start)
        if not isinstance(evs, list):        # HC-08: a spilled streamed transaction, delivered lazily
            self._txn_has_events = False
            return evs
        if not evs:
            return evs
        if any(e.kind is not ChangeKind.COMMIT for e in evs):
            self._txn_has_events = True
            if evs[-1].kind is not ChangeKind.COMMIT:
                return evs
        if evs[-1].kind is ChangeKind.COMMIT and len(evs) == 1 and not self._txn_has_events:
            now = time.monotonic()
            if now - self._last_empty_emit < self.empty_commit_s:
                self.census.count("pgoutput:C", FC.RecordClass.IGNORED, "empty transaction (COMMIT throttled)")
                return []
            self._last_empty_emit = now
        self._txn_has_events = False
        return evs

    def _feed(self, payload: bytes, data_start: int) -> list[ChangeEvent]:
        m = P.decode(payload, in_stream=self.in_stream is not None)
        pos = Position(engine=ENGINE, text=format_lsn(data_start))
        out: list[ChangeEvent] = []
        tag = chr(payload[0])
        rec = f"pgoutput:{tag}"
        if isinstance(m, (P.Insert, P.Update, P.Delete)) and m.relid not in self.rel:
            return self._stream_or_out(m, self._unknown_relation(m.relid, tag, pos, m.xid))
        if isinstance(m, P.Truncate) and any(r not in self.rel for r in m.relids):
            for r in m.relids:
                if r not in self.rel:
                    out.extend(self._unknown_relation(r, tag, pos, m.xid))
                else:
                    _, ref = self._ref(r)
                    out.append(ChangeEvent(position=pos, txid=self._txid(m.xid), kind=ChangeKind.TRUNCATE,
                                           table=ref, note="cascade" if m.options & 1 else None))
            return self._stream_or_out(m, out)
        if isinstance(m, (P.Begin, P.Commit, P.Relation, P.Delete, P.Truncate, P.StreamStart, P.StreamStop,
                          P.StreamCommit)):
            self.census.count(rec, FC.RecordClass.DECODED)
        elif isinstance(m, P.TypeMsg):
            self.census.count(rec, FC.RecordClass.IGNORED, "type message (types resolved through pg_type)")
        elif isinstance(m, P.Origin):
            self.census.count(rec, FC.RecordClass.IGNORED, "origin message (replication origin of the txn)")
        elif isinstance(m, P.LogicalMessage):
            self.census.count(rec, FC.RecordClass.IGNORED,
                              "logical decoding message (pg_logical_emit_message: not table data)")
        if isinstance(m, P.Begin):
            self.xid = m.xid
            self._txn_has_events = False
        elif isinstance(m, P.Commit):
            out.append(ChangeEvent(position=Position(engine=ENGINE, text=format_lsn(m.end_lsn)),
                                   txid=self._txid(None), kind=ChangeKind.COMMIT, commit_ts=m.commit_ts))
            self.xid = None
        elif isinstance(m, P.Relation):
            out.extend(self._relation(m, pos))
        elif isinstance(m, P.TypeMsg):
            pass                                   # resolved through pg_type (typtype is not in the message)
        elif isinstance(m, P.Insert):
            rel, ref = self._ref(m.relid)
            notes: list[str] = []
            after = self._row(rel, m.new, notes=notes)
            out.append(ChangeEvent(position=pos, txid=self._txid(m.xid), kind=ChangeKind.INSERT, table=ref,
                                   after=after, note=";".join(notes) or None))
            if notes:          # 'u' in a new row is not documented: the value is unknown
                out.extend(self._toast_reread(rel, ref, after, notes, "unchanged TOAST value in an INSERT",
                                              tag, pos, m.xid))
            else:
                self._count_row(rec, after)
        elif isinstance(m, P.Update):
            rel, ref = self._ref(m.relid)
            before = self._row(rel, m.old, key_only=m.old_kind == "K") if m.old is not None else None
            notes = []
            after = self._row(rel, m.new, old=before if m.old_kind == "O" else None, notes=notes)
            out.append(ChangeEvent(position=pos, txid=self._txid(m.xid), kind=ChangeKind.UPDATE, table=ref,
                                   before=before, after=after, note=";".join(notes) or None))
            if notes and m.old_kind == "K":     # key changed: the loader writes a NEW row, the old value is lost
                out.extend(self._toast_reread(rel, ref, after, notes, "unchanged TOAST value in a key-changing "
                                              "UPDATE (REPLICA IDENTITY DEFAULT)", tag, pos, m.xid))
            else:                               # documented: the value is unchanged, the destination keeps it
                self._count_row(rec, after)
        elif isinstance(m, P.Delete):
            rel, ref = self._ref(m.relid)
            out.append(ChangeEvent(position=pos, txid=self._txid(m.xid), kind=ChangeKind.DELETE, table=ref,
                                   before=self._row(rel, m.old, key_only=m.old_kind == "K")))
        elif isinstance(m, P.Truncate):
            for relid in m.relids:
                _, ref = self._ref(relid)
                out.append(ChangeEvent(position=pos, txid=self._txid(m.xid), kind=ChangeKind.TRUNCATE, table=ref,
                                       note="cascade" if m.options & 1 else None))
        elif isinstance(m, P.StreamStart):
            self.in_stream = m.xid
            if not m.first_segment and m.xid not in self.streams:
                self.incomplete.add(m.xid)      # earlier segments of this txn were not received
            if m.xid not in self.streams:
                self.streams[m.xid] = SpillBuffer(label=f"postgres:{m.xid}")
            return []
        elif isinstance(m, P.StreamStop):
            self.in_stream = None
            return []
        elif isinstance(m, P.StreamCommit):
            buf = self.streams.pop(m.xid, None) or SpillBuffer(label=f"postgres:{m.xid}")
            lazy = bool(buf.spilled)
            sx = str(m.xid)
            body: Any = ((e.model_copy(update={"txid": sx}) for e in buf.drain()) if lazy
                         else [e.model_copy(update={"txid": sx}) for e in buf])
            if not lazy:
                buf.close()
            evs: list[ChangeEvent] = []
            if m.xid in self.incomplete:
                self.incomplete.discard(m.xid)
                self.census.count(rec, FC.RecordClass.UNSUPPORTED, "streamed txn without its first segment")
                evs.append(FC.unsupported(None, f"streamed transaction {m.xid} committed but its first segment was "
                                                "not received: earlier changes unknown", rec, xid=m.xid)
                           .event(Position(engine=ENGINE, text=format_lsn(m.commit_lsn)), str(m.xid)))
            tail = evs + [ChangeEvent(position=Position(engine=ENGINE, text=format_lsn(m.end_lsn)), txid=sx,
                                      kind=ChangeKind.COMMIT, commit_ts=m.commit_ts)]
            if lazy:
                return itertools.chain(body, tail)  # type: ignore[return-value]
            return body + tail
        elif isinstance(m, P.StreamAbort):
            if m.xid not in self.streams:
                self.census.count(rec, FC.RecordClass.IGNORED, "stream abort of a txn not seen")
                return []
            self.census.count(rec, FC.RecordClass.DECODED)
            if m.subxid == m.xid:
                self.incomplete.discard(m.xid)
                b = self.streams.pop(m.xid, None)
                if b is not None:
                    b.close()
            else:
                self.streams[m.xid].drop_tag(int(m.subxid))
            return []
        elif isinstance(m, P.Other):              # two-phase messages: never requested (two_phase is off)
            self.census.count(rec, FC.RecordClass.UNSUPPORTED, "two-phase message")
            out.append(FC.unsupported(None, f"pgoutput two-phase message {tag!r} (two_phase is not requested)", rec)
                       .event(pos, self._txid(None)))
        return self._stream_or_out(m, out)

    def _stream_or_out(self, m: P.Message, out: list[ChangeEvent]) -> list[ChangeEvent]:
        if self.in_stream is not None and out:
            sub = getattr(m, "xid", None)
            buf = self.streams.setdefault(self.in_stream, SpillBuffer(label=f"postgres:{self.in_stream}"))
            for e in out:
                buf.append(e, tag=int(sub if sub is not None else self.in_stream))
            return []
        return out

    def _relation(self, m: P.Relation, pos: Position) -> list[ChangeEvent]:
        prev = self.rel.get(m.relid)
        self.rel[m.relid] = m
        key = (m.namespace or "pg_catalog", m.name)
        shape = tuple((c.name, self._type(c.type_oid)) for c in m.columns)
        known = self.shapes.get(key)
        self.shapes[key] = shape
        changed = (known is not None and known != shape) or known is None or (
            prev is not None and (prev.namespace, prev.name) != (m.namespace, m.name))
        if not changed:
            return []
        return [ChangeEvent(position=pos, txid=self._txid(m.xid), kind=ChangeKind.DDL,
                            table=TableRef(schema=key[0], name=key[1]),
                            note="relation shape changed" if known is not None else "new relation")]

