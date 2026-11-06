"""Bounded-memory buffer of an open source transaction (HC-08).

Connectors that must hold a transaction until its commit (MySQL binlog assembler, Oracle LogMiner assembler,
PostgreSQL streamed transactions) keep it in a `SpillBuffer`: the newest events stay in memory (the tail, where
savepoint rollbacks and LOB pieces look), older ones move to a process-private temp file once the memory budget
is exceeded. Iteration replays the file then the tail, one event at a time, so a multi-GB transaction is delivered
with bounded memory and never stops capture.

- Memory budget: `ALELT_CAPTURE_TXN_MEM_MB` (default 64) per buffer.
- Disk cap: `ALELT_CAPTURE_MAX_SPILL_MB` (default 65536) per buffer; above it `SpillLimitExceeded` is raised (the
  capture runner stops the connection with an incident - never a silent loss). The deprecated
  `ALELT_CAPTURE_MAX_TXN_MB` is no longer a stop threshold.
- Spilled records: length-prefixed canonical JSON with CRC32 (no pickle, F-15), checked on read-back.
- Events in the spilled prefix are immutable except for tag-based removal (`drop_tag`, e.g. an aborted
  PostgreSQL subtransaction): per-record tags are kept in a compact in-memory array and filtered on replay.
  Searches for older events (`spilled` > 0) report a miss; the connector then emits a keyed re-read / hold
  signal instead of guessing.
"""
from __future__ import annotations

import json
import os
import struct
import tempfile
import zlib
from array import array
from collections.abc import Iterator
from typing import Any

from .models import ChangeEvent, ChangeKind, Position, TableRef

_HDR = struct.Struct("<II")       # length, crc32


class SpillLimitExceeded(RuntimeError):
    """The open transaction exceeds ALELT_CAPTURE_MAX_SPILL_MB on disk: stop + incident (never dropped)."""


def mem_budget_bytes() -> int:
    try:
        return max(1, int(os.environ.get("ALELT_CAPTURE_TXN_MEM_MB", "64"))) << 20
    except ValueError:
        return 64 << 20


def disk_cap_bytes() -> int:
    try:
        return max(1, int(os.environ.get("ALELT_CAPTURE_MAX_SPILL_MB", "65536"))) << 20
    except ValueError:
        return 65536 << 20


def _enc_value(v: Any) -> Any:
    from .canonical_jsonl import Undecodable, encode_value

    if isinstance(v, Undecodable):
        return {"$undecodable": str(v)}
    return encode_value(v)


def _dec_value(v: Any) -> Any:
    from .canonical_jsonl import Undecodable, decode_value

    if isinstance(v, dict) and len(v) == 1 and "$undecodable" in v:
        return Undecodable(v["$undecodable"])
    return decode_value(v)


def encode_spill(ev: ChangeEvent, extra: Any = None) -> bytes:
    def row(r: dict[str, Any] | None) -> dict[str, Any] | None:
        return None if r is None else {k: _enc_value(x) for k, x in r.items()}

    d = {"e": ev.position.engine, "p": ev.position.text, "x": ev.txid, "k": ev.kind.value,
         "s": ev.table.schema_ if ev.table else None, "t": ev.table.name if ev.table else None,
         "b": row(ev.before), "a": row(ev.after), "c": _enc_value(ev.commit_ts) if ev.commit_ts else None,
         "n": ev.note, "z": extra}
    return json.dumps(d, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def decode_spill(raw: bytes) -> tuple[ChangeEvent, Any]:
    d = json.loads(raw)

    def row(r: dict[str, Any] | None) -> dict[str, Any] | None:
        return None if r is None else {k: _dec_value(x) for k, x in r.items()}

    ev = ChangeEvent(position=Position(engine=d["e"], text=d["p"]), txid=d["x"], kind=ChangeKind(d["k"]),
                     table=TableRef(schema=d["s"], name=d["t"]) if d.get("t") else None,
                     before=row(d.get("b")), after=row(d.get("a")),
                     commit_ts=_dec_value(d["c"]) if d.get("c") else None, note=d.get("n"))
    return ev, d.get("z")


def event_size(ev: ChangeEvent) -> int:
    n = 96 + len(ev.note or "")
    for r in (ev.before, ev.after):
        if r:
            for k, v in r.items():
                n += len(k) + (len(v) if isinstance(v, (str, bytes)) else 16)
    return n


class SpillBuffer:
    """Ordered events of one open transaction; memory-bounded, iterable once or many times."""

    def __init__(self, mem_budget: int | None = None, disk_cap: int | None = None, label: str = "") -> None:
        self.mem_budget = mem_budget if mem_budget is not None else mem_budget_bytes()
        self.disk_cap = disk_cap if disk_cap is not None else disk_cap_bytes()
        self.label = label
        self.tail: list[ChangeEvent] = []
        self.tail_extra: list[Any] = []
        self.tail_tags: list[int] = []
        self.tail_bytes = 0
        self._f: Any = None
        self._path: str | None = None
        self.disk_bytes = 0
        self.spilled = 0                      # records in the file
        self._tags = array("q")               # tag per spilled record (0 = none)
        self._dropped: set[int] = set()       # tags removed after spilling
        self.max_tail_bytes = 0

    # ------------------------------------------------------------------ list-like (tail)
    def __len__(self) -> int:
        return self.spilled + len(self.tail)

    def __bool__(self) -> bool:
        return bool(self.spilled or self.tail)

    def append(self, ev: ChangeEvent, extra: Any = None, tag: int = 0) -> None:
        self.tail.append(ev)
        self.tail_extra.append(extra)
        self.tail_tags.append(tag)
        self.tail_bytes += event_size(ev)
        if self.tail_bytes > self.mem_budget:
            self._spill_half()
        self.max_tail_bytes = max(self.max_tail_bytes, self.tail_bytes)

    def pop_tail(self, i: int) -> ChangeEvent:
        ev = self.tail.pop(i)
        self.tail_extra.pop(i)
        self.tail_tags.pop(i)
        self.tail_bytes -= event_size(ev)
        return ev

    def drop_tag(self, tag: int) -> int:
        """Remove every event with this tag (aborted subtransaction); returns how many were in the tail."""
        keep = [i for i, t in enumerate(self.tail_tags) if t != tag]
        n = len(self.tail) - len(keep)
        if n:
            self.tail = [self.tail[i] for i in keep]
            self.tail_extra = [self.tail_extra[i] for i in keep]
            self.tail_tags = [self.tail_tags[i] for i in keep]
            self.tail_bytes = sum(event_size(e) for e in self.tail)
        if self.spilled and tag in set(self._tags):
            self._dropped.add(tag)
        return n

    # ------------------------------------------------------------------ spill
    def write_cold(self, ev: ChangeEvent, extra: Any = None, tag: int = 0) -> None:
        """Write one event straight to the file (callers that keep their own mutable tail: the file always
        precedes the tail, so only use this while this buffer's own tail is empty)."""
        assert not self.tail, "write_cold with a non-empty tail would reorder events"
        self._open()
        self._f.seek(0, os.SEEK_END)
        body = encode_spill(ev, extra)
        self._f.write(_HDR.pack(len(body), zlib.crc32(body) & 0xFFFFFFFF) + body)
        self.disk_bytes += _HDR.size + len(body)
        self._tags.append(tag)
        self.spilled += 1
        if self.disk_bytes > self.disk_cap:
            raise SpillLimitExceeded(f"open transaction {self.label} spilled {self.disk_bytes >> 20} MiB > "
                                     f"ALELT_CAPTURE_MAX_SPILL_MB ({self.disk_cap >> 20})")

    def _open(self) -> None:
        if self._f is None:
            fd, self._path = tempfile.mkstemp(prefix="alelt-txn-", suffix=".spill")
            self._f = os.fdopen(fd, "w+b")
            try:
                os.chmod(self._path, 0o600)
            except OSError:
                pass

    def _spill_half(self) -> None:
        """Move the oldest half (by bytes) of the tail to the file; the newest events stay searchable."""
        self._open()
        target = self.tail_bytes // 2
        moved = 0
        n = 0
        self._f.seek(0, os.SEEK_END)
        while n < len(self.tail) - 1 and moved < target:
            ev = self.tail[n]
            body = encode_spill(ev, self.tail_extra[n])
            self._f.write(_HDR.pack(len(body), zlib.crc32(body) & 0xFFFFFFFF) + body)
            self.disk_bytes += _HDR.size + len(body)
            self._tags.append(self.tail_tags[n])
            moved += event_size(ev)
            n += 1
        self.spilled += n
        del self.tail[:n], self.tail_extra[:n], self.tail_tags[:n]
        self.tail_bytes -= moved
        if self.disk_bytes > self.disk_cap:
            raise SpillLimitExceeded(f"open transaction {self.label} spilled {self.disk_bytes >> 20} MiB > "
                                     f"ALELT_CAPTURE_MAX_SPILL_MB ({self.disk_cap >> 20})")

    # ------------------------------------------------------------------ replay
    def items(self) -> Iterator[tuple[ChangeEvent, Any]]:
        if self._f is not None and self.spilled:
            self._f.flush()
            with open(self._path, "rb") as f:          # type: ignore[arg-type]
                for i in range(self.spilled):
                    hdr = f.read(_HDR.size)
                    ln, crc = _HDR.unpack(hdr)
                    body = f.read(ln)
                    if zlib.crc32(body) & 0xFFFFFFFF != crc:
                        raise OSError(f"spill file of transaction {self.label} corrupted at record {i}")
                    if self._tags[i] and self._tags[i] in self._dropped:
                        continue
                    yield decode_spill(body)
        yield from zip(list(self.tail), list(self.tail_extra))

    def __iter__(self) -> Iterator[ChangeEvent]:
        for ev, _ in self.items():
            yield ev

    def close(self) -> None:
        if self._f is not None:
            try:
                self._f.close()
            finally:
                try:
                    os.unlink(self._path)              # type: ignore[arg-type]
                except OSError:
                    pass
        self._f, self._path = None, None
        self.tail, self.tail_extra, self.tail_tags = [], [], []
        self.tail_bytes = self.disk_bytes = self.spilled = 0
        self._tags = array("q")
        self._dropped = set()

    def drain(self) -> Iterator[ChangeEvent]:
        """Iterate every event once, then release memory and the file."""
        try:
            yield from self
        finally:
            self.close()

    def __del__(self) -> None:  # pragma: no cover - safety net for abandoned transactions
        try:
            self.close()
        except Exception:  # noqa: BLE001
            pass
