"""Transaction assembler: buffer ChangeEvents per txid, emit ChangeSets on COMMIT, drop ROLLBACK.

Semantics (engine-agnostic):
- INSERT/UPDATE/DELETE/TRUNCATE are buffered under their txid in arrival (= log) order.
- COMPENSATION undoes one still-live DML of the same txid (F-11, one rule for every source). Match order:
  (1) `target_ref:<id>` on the compensation == `row_ref:<id>` on the DML (same table), newest first;
  (2) positional: the newest live event of the named table (any table when none is named) - undo records
  are written in strict reverse order (Db2 CLRs, savepoint rollback). Spilled events are matched the same
  way (table + ref kept in memory per spilled record), never by a bare counter. Sources that resolve
  savepoints themselves (MySQL, Oracle, SQL Server) emit no COMPENSATION; the result must be identical.
- COMMIT emits a ChangeSet {txid, commit_position, commit_ts, events}; ROLLBACK discards the txn.
- DDL/OTHER are counted and ignored (schema drift is alelt_schema's job).
- Large transactions spill to disk once the total in-memory byte budget is exceeded
  (length-prefixed canonical-JSON event records with CRC32, verified on read-back; process-private files;
  no pickle - F-15).
- Values may be any canonical Python value, including bytes (BINARY/BLOB), Decimal and datetimes.

Restart: the assembler is in-memory only by design; callers rebuild it by replaying spooled batches
from `low_water_seq()` (first batch of the oldest open txn) and skipping change sets whose commit
position is <= the destination checkpoint.
"""
from __future__ import annotations

import logging
import os
import json
import struct
import tempfile
import time
import zlib
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from alelt_common.models import ChangeEvent, ChangeKind, ChangeSet

log = logging.getLogger("alelt_txn")

DML = {ChangeKind.INSERT, ChangeKind.UPDATE, ChangeKind.DELETE, ChangeKind.TRUNCATE}



# ------------------------------------------------------------------ spill codec (F-15: canonical JSON, never pickle)
def _spill_value(v: Any) -> Any:
    from alelt_common.canonical_jsonl import Undecodable, encode_value

    if isinstance(v, Undecodable):
        return {"$undecodable": str(v)}
    return encode_value(v)


def _unspill_value(v: Any) -> Any:
    from alelt_common.canonical_jsonl import Undecodable, decode_value

    if isinstance(v, dict) and len(v) == 1 and "$undecodable" in v:
        return Undecodable(v["$undecodable"])
    return decode_value(v)


def _spill_encode(e: ChangeEvent) -> bytes:
    d = e.model_dump(mode="json", exclude={"before", "after", "commit_ts"})
    for k in ("before", "after"):
        row = getattr(e, k)
        d[k] = None if row is None else {c: _spill_value(v) for c, v in row.items()}
    d["commit_ts"] = _spill_value(e.commit_ts) if e.commit_ts is not None else None
    return json.dumps(d, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _spill_decode(blob: bytes) -> ChangeEvent:
    d = json.loads(blob)
    for k in ("before", "after"):
        if d.get(k) is not None:
            d[k] = {c: _unspill_value(v) for c, v in d[k].items()}
    if d.get("commit_ts") is not None:
        d["commit_ts"] = _unspill_value(d["commit_ts"])
    return ChangeEvent.model_validate(d)

@dataclass
class OpenTxn:
    txid: str
    first_seq: int
    first_position: str
    opened_at: float
    events: list[ChangeEvent] = field(default_factory=list)
    nbytes: int = 0
    spill_path: Path | None = None
    spilled_count: int = 0
    undo_pending: int = 0            # number of spilled events cancelled by compensations
    spilled_keys: list[tuple[str | None, str | None]] = field(default_factory=list)  # (table fqn, row ref)
    spilled_cancelled: set[int] = field(default_factory=set)                       # indexes into the spill


def note_ref(note: str | None, key: str) -> str | None:
    """Value of a `key:<ref>` token in a decoder note (tokens separated by ';').

    F-11 rule: a DML event may carry `row_ref:<id>` (Db2 RID, Oracle ROWID, ...) and a COMPENSATION may carry
    `target_ref:<id>` naming the row it undoes. The assembler matches by ref first, then positionally."""
    if not note:
        return None
    prefix = key + ":"
    for tok in note.split(";"):
        tok = tok.strip()
        if tok.startswith(prefix):
            return tok[len(prefix):].strip() or None
    return None


@dataclass
class AssemblerStats:
    committed: int = 0
    rolled_back: int = 0
    events_emitted: int = 0
    events_dropped: int = 0
    compensations: int = 0
    ignored: int = 0
    spills: int = 0


_REC = struct.Struct("<II")       # payload length, crc32


def _value_size(v: object) -> int:
    if isinstance(v, (bytes, bytearray, str)):
        return len(v) + 8
    if isinstance(v, dict):
        return sum(len(str(k)) + _value_size(x) for k, x in v.items()) + 16
    return 16


def _event_size(ev: ChangeEvent) -> int:
    """Approximate in-memory footprint; safe for every canonical value type (no JSON round-trip)."""
    return 128 + _value_size(ev.before or {}) + _value_size(ev.after or {})


class TxnAssembler:
    """Feed events in log order; collect emitted ChangeSets in commit order."""

    def __init__(self, byte_budget: int = 64 << 20, spill_dir: str | Path | None = None,
                 long_txn_seconds: float = 300.0) -> None:
        self.byte_budget = byte_budget
        self.spill_dir = Path(spill_dir) if spill_dir else Path(tempfile.gettempdir()) / "alelt_txn_spill"
        self.long_txn_seconds = long_txn_seconds
        self.open: dict[str, OpenTxn] = {}
        self.mem_bytes = 0
        self.stats = AssemblerStats()

    # ------------------------------------------------------------------ feeding
    def feed(self, events: Iterable[ChangeEvent], seq: int = 0) -> list[ChangeSet]:
        """Process events of one batch (batch sequence `seq`); return change sets committed in it."""
        out: list[ChangeSet] = []
        for ev in events:
            cs = self._one(ev, seq)
            if cs is not None:
                out.append(cs)
        return out

    def _one(self, ev: ChangeEvent, seq: int) -> ChangeSet | None:
        k = ev.kind
        if k in DML:
            t = self.open.get(ev.txid)
            if t is None:
                t = self.open[ev.txid] = OpenTxn(ev.txid, seq, ev.position.text, time.monotonic())
            n = _event_size(ev)
            t.events.append(ev)
            t.nbytes += n
            self.mem_bytes += n
            if self.mem_bytes > self.byte_budget:
                self._spill_largest()
            return None
        if k is ChangeKind.COMPENSATION:
            self.stats.compensations += 1
            self._compensate(ev)
            return None
        if k is ChangeKind.COMMIT:
            return self._commit(ev)
        if k is ChangeKind.ROLLBACK:
            self._discard(ev.txid)
            return None
        self.stats.ignored += 1
        return None

    def _compensate(self, ev: ChangeEvent) -> None:
        t = self.open.get(ev.txid)
        if t is None:
            log.warning("compensation for unknown txn", extra={"component": "alelt_txn", "position": ev.position.text})
            return
        target = note_ref(ev.note, "target_ref")
        fqn = ev.table.fqn if ev.table is not None else None

        def live_match(by_ref: bool) -> int | None:
            for i in range(len(t.events) - 1, -1, -1):
                e = t.events[i]
                if fqn is not None and (e.table is None or e.table.fqn != fqn):
                    continue
                if by_ref and note_ref(e.note, "row_ref") != target:
                    continue
                return i
            return None

        def spilled_match(by_ref: bool) -> int | None:
            for i in range(len(t.spilled_keys) - 1, -1, -1):
                if i in t.spilled_cancelled:
                    continue
                tf, ref = t.spilled_keys[i]
                if fqn is not None and tf != fqn:
                    continue
                if by_ref and ref != target:
                    continue
                return i
            return None

        # undo records are written newest-first, so the newest matching event is the one undone; the in-memory
        # events are always newer than the spilled ones. Ref match first (both tiers), positional fallback.
        for by_ref in ((True, False) if target else (False,)):
            i = live_match(by_ref)
            if i is not None:
                gone = t.events.pop(i)
                n = _event_size(gone)
                t.nbytes -= n
                self.mem_bytes -= n
                self.stats.events_dropped += 1
                return
            j = spilled_match(by_ref)
            if j is not None:
                t.spilled_cancelled.add(j)
                t.undo_pending += 1
                self.stats.events_dropped += 1
                return
        log.warning("compensation without live event", extra={"component": "alelt_txn", "position": ev.position.text})

    def _commit(self, ev: ChangeEvent) -> ChangeSet | None:
        t = self.open.pop(ev.txid, None)
        if t is None:
            return None                     # read-only / non-captured txn
        events = list(self._drain(t))
        self.stats.committed += 1
        if not events:
            return None
        self.stats.events_emitted += len(events)
        return ChangeSet(txid=ev.txid, commit_position=ev.position, commit_ts=ev.commit_ts, events=events)

    def _discard(self, txid: str) -> None:
        t = self.open.pop(txid, None)
        self.stats.rolled_back += 1
        if t is None:
            return
        self.stats.events_dropped += len(t.events) + t.spilled_count - t.undo_pending
        self.mem_bytes -= t.nbytes
        if t.spill_path:
            t.spill_path.unlink(missing_ok=True)

    # ------------------------------------------------------------------ spill
    def _spill_largest(self) -> None:
        t = max(self.open.values(), key=lambda x: x.nbytes)
        if not t.events:
            return
        self.spill_dir.mkdir(parents=True, exist_ok=True)
        if t.spill_path is None:
            safe = "".join(c if c.isalnum() else "_" for c in t.txid)
            t.spill_path = self.spill_dir / f"txn_{safe}_{os.getpid()}_{id(t):x}.spill"
        with t.spill_path.open("ab") as f:
            for e in t.events:
                blob = _spill_encode(e)
                f.write(_REC.pack(len(blob), zlib.crc32(blob)) + blob)
                t.spilled_keys.append((e.table.fqn if e.table is not None else None, note_ref(e.note, "row_ref")))
        self.stats.spills += 1
        t.spilled_count += len(t.events)
        self.mem_bytes -= t.nbytes
        t.events, t.nbytes = [], 0

    def _drain(self, t: OpenTxn) -> Iterator[ChangeEvent]:
        self.mem_bytes -= t.nbytes
        if t.spill_path:
            with t.spill_path.open("rb") as f:
                for i in range(t.spilled_count):
                    hdr = f.read(_REC.size)
                    if len(hdr) != _REC.size:
                        raise OSError(f"spill file truncated: {t.spill_path} record {i}")
                    n, crc = _REC.unpack(hdr)
                    blob = f.read(n)
                    if len(blob) != n or zlib.crc32(blob) != crc:
                        raise OSError(f"spill file corrupt: {t.spill_path} record {i}")
                    if i not in t.spilled_cancelled:
                        yield _spill_decode(blob)
            t.spill_path.unlink(missing_ok=True)
        yield from t.events

    # ------------------------------------------------------------------ state
    def low_water_seq(self, default: int) -> int:
        """Smallest batch seq still needed to rebuild open transactions (default when none open)."""
        return min((t.first_seq for t in self.open.values()), default=default)

    def oldest_open_seconds(self) -> float:
        now = time.monotonic()
        return max((now - t.opened_at for t in self.open.values()), default=0.0)

    def long_open(self) -> list[OpenTxn]:
        now = time.monotonic()
        return [t for t in self.open.values() if now - t.opened_at > self.long_txn_seconds]

    def reset(self) -> None:
        for t in self.open.values():
            if t.spill_path:
                t.spill_path.unlink(missing_ok=True)
        self.open.clear()
        self.mem_bytes = 0
