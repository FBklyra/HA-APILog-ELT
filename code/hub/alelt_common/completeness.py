"""Source completeness contract (generic, every engine): classify EVERY record a source reads.

DESIGN-REVIEW F-01 / F-02 / F-07, X-120 / X-121. A source never drops a record silently and never assumes a value
is "unchanged" unless the vendor's public documentation says so. Every record / message / event / row read from
the source log is put in exactly one class:

| Class | Meaning | Travels to the hub as |
|---|---|---|
| `decoded` | turned into canonical ChangeEvents (documented semantics only) | the DML / COMMIT events |
| `ignored` | carries no row change BY DOCUMENTED DESIGN (heartbeat, origin, savepoint marker, ...) - reason given | census count only |
| `needs_keyed_reread` | a row change that cannot be decoded completely: re-read the row(s) by key | `OTHER` signal event |
| `needs_catalog_refresh` | table shape / dictionary changed or unknown: refresh the catalog, re-decode | `DDL` signal event |
| `unsupported` | cannot be decoded and no key is known: hold the table (or the connection) | `OTHER` signal event |

Signals are ordinary ChangeEvents (so the batch format, the runner and the assembler are unchanged):
`kind = OTHER` (`DDL` for needs_catalog_refresh, so the capture runner refreshes the catalog), `table` = the
affected table (None = the whole connection), `note = "alelt-signal:" + JSON` (`SourceSignal.to_note`). The integrity
state machine (stream P) reads them with `signal_of(event)`:

- `needs_keyed_reread(table, keys | key_range)`: table SUSPECT at the signal position; call
  `SourceConnector.keyed_reread(table, keys)` and apply the rows (missing key = row deleted); a keyed re-read that is
  impossible (no key, no `keyed_reread`) escalates to hold + resync.
- `needs_catalog_refresh(table)`: refresh the catalog (the runner does it for DDL events), re-decode from the signal
  position; a table that is still not decodable afterwards becomes `unsupported`.
- `unsupported(table | None)`: hold the table (None: every table of the connection), its watermark stops at the
  signal position, certification/switch blocked, resync.

A signal is part of the transaction it was found in (same txid, before its COMMIT) or of the next transaction when it
was found between transactions; its position is the position of the record that caused it.

`Census` counts every record a connector reads per (record kind, class, reason); `alelt census` prints it.
"Unexplained" = a record kind the connector has no rule for; it is ALSO emitted as an `unsupported` signal (never
silent), and a census with unexplained > 0 fails.
"""
from __future__ import annotations

import json
import threading
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .models import ChangeEvent, ChangeKind, Position, TableRef

SIGNAL_PREFIX = "alelt-signal:"


class RecordClass(str, Enum):
    DECODED = "decoded"
    IGNORED = "ignored"
    NEEDS_KEYED_REREAD = "needs_keyed_reread"
    NEEDS_CATALOG_REFRESH = "needs_catalog_refresh"
    UNSUPPORTED = "unsupported"


SIGNAL_CLASSES = frozenset({RecordClass.NEEDS_KEYED_REREAD, RecordClass.NEEDS_CATALOG_REFRESH,
                            RecordClass.UNSUPPORTED})


@dataclass
class SourceSignal:
    """One classification that the hub must act on (never a decoded change)."""

    cls: RecordClass
    reason: str
    table: TableRef | None = None                     # None = the whole connection
    keys: list[dict[str, Any]] | None = None          # canonical key values (needs_keyed_reread)
    key_range: tuple[list[Any] | None, list[Any] | None] | None = None   # (from exclusive, to inclusive)
    record: str = ""                                  # engine record kind, e.g. 'pgoutput:U', 'binlog:39'
    detail: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.cls = RecordClass(self.cls)
        if self.cls not in SIGNAL_CLASSES:
            raise ValueError(f"{self.cls.value} is not a signal class")

    # ---- wire (ChangeEvent.note)
    def to_note(self) -> str:
        from .canonical_jsonl import encode_value

        d: dict[str, Any] = {"class": self.cls.value, "reason": self.reason[:2000], "record": self.record}
        if self.keys is not None:
            d["keys"] = [{k: encode_value(v) for k, v in row.items()} for row in self.keys]
        if self.key_range is not None:
            d["key_range"] = [None if b is None else [encode_value(v) for v in b] for b in self.key_range]
        if self.detail:
            d["detail"] = self.detail
        return SIGNAL_PREFIX + json.dumps(d, ensure_ascii=False, separators=(",", ":"), default=str)

    @classmethod
    def from_note(cls, note: str, table: TableRef | None) -> SourceSignal:
        from .canonical_jsonl import decode_value

        d = json.loads(note[len(SIGNAL_PREFIX):])
        keys = [{k: decode_value(v) for k, v in row.items()} for row in d["keys"]] if d.get("keys") is not None else None
        kr = d.get("key_range")
        rng = (tuple(None if b is None else [decode_value(v) for v in b] for b in kr)  # type: ignore[assignment]
               if kr is not None else None)
        return cls(RecordClass(d["class"]), d.get("reason", ""), table, keys, rng, d.get("record", ""),
                   d.get("detail") or {})

    def event(self, position: Position, txid: str) -> ChangeEvent:
        kind = ChangeKind.DDL if self.cls is RecordClass.NEEDS_CATALOG_REFRESH else ChangeKind.OTHER
        return ChangeEvent(position=position, txid=txid, kind=kind, table=self.table, note=self.to_note())


def signal_of(ev: ChangeEvent) -> SourceSignal | None:
    """The SourceSignal carried by an event, else None (ordinary change / DDL / quarantine line)."""
    if ev.note and ev.note.startswith(SIGNAL_PREFIX) and ev.kind in (ChangeKind.OTHER, ChangeKind.DDL):
        return SourceSignal.from_note(ev.note, ev.table)
    return None


def keyed_reread(table: TableRef, keys: list[dict[str, Any]] | None, reason: str, record: str = "",
                 key_range: tuple[list[Any] | None, list[Any] | None] | None = None,
                 **detail: Any) -> SourceSignal:
    return SourceSignal(RecordClass.NEEDS_KEYED_REREAD, reason, table, keys, key_range, record, dict(detail))


def catalog_refresh(table: TableRef | None, reason: str, record: str = "", **detail: Any) -> SourceSignal:
    return SourceSignal(RecordClass.NEEDS_CATALOG_REFRESH, reason, table, None, None, record, dict(detail))


def unsupported(table: TableRef | None, reason: str, record: str = "", **detail: Any) -> SourceSignal:
    return SourceSignal(RecordClass.UNSUPPORTED, reason, table, None, None, record, dict(detail))


# ------------------------------------------------------------------ keyed re-read result (SourceConnector SPI)
@dataclass
class KeyedReread:
    """Result of `SourceConnector.keyed_reread(table, keys)`.

    `rows`: current row per requested key that exists (canonical values, same forms as snapshot_rows);
    `missing`: requested keys with no row (the row was deleted -> apply a delete);
    `read_from`: source position taken BEFORE the consistent read: every change committed up to it is in `rows`,
    changes committed after it may or may not be - the caller applies the stream after `read_from` on top (full-image
    replays converge). `isolation`: what the engine used (documentation / audit)."""

    rows: list[dict[str, Any]]
    missing: list[dict[str, Any]]
    read_from: Position | None
    isolation: str


# ------------------------------------------------------------------ census
class Census:
    """Thread-safe counters: (record kind, class, reason) -> count. One per connector instance."""

    def __init__(self, engine: str) -> None:
        self.engine = engine
        self._c: Counter[tuple[str, str, str]] = Counter()
        self._lock = threading.Lock()

    def count(self, record: str, cls: RecordClass | str, reason: str = "", n: int = 1) -> None:
        c = RecordClass(cls).value
        with self._lock:
            self._c[(record, c, reason if c != RecordClass.DECODED.value else "")] += n

    def unexplained(self, record: str, reason: str = "no rule for this record kind") -> None:
        with self._lock:
            self._c[(record, "unexplained", reason)] += 1

    def signal(self, sig: SourceSignal) -> SourceSignal:
        self.count(sig.record or "signal", sig.cls, sig.reason.split(":", 1)[0][:120])
        return sig

    def merge(self, other: Census) -> None:
        with self._lock, other._lock:
            self._c.update(other._c)

    def reset(self) -> None:
        with self._lock:
            self._c.clear()

    @property
    def total(self) -> int:
        return sum(self._c.values())

    @property
    def unexplained_total(self) -> int:
        return sum(n for (_, c, _), n in self._c.items() if c == "unexplained")

    def by_class(self) -> dict[str, int]:
        out: Counter[str] = Counter()
        for (_, c, _), n in self._c.items():
            out[c] += n
        return dict(out)

    def rows(self) -> list[dict[str, Any]]:
        return [{"record": r, "class": c, "reason": why, "count": n}
                for (r, c, why), n in sorted(self._c.items())]

    def report(self) -> dict[str, Any]:
        return {"engine": self.engine, "total": self.total, "by_class": self.by_class(),
                "unexplained": self.unexplained_total, "rows": self.rows()}
