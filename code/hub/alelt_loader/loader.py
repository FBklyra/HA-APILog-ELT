"""Engine-agnostic loader: micro-batching, column policy, final-op-per-key, atomic apply via the SPI.

The loader buffers committed ChangeSets until a row/latency threshold, then calls
`DestinationConnector.apply()` ONCE with a single compacted ChangeSet and the batch to_pos.
Exactly-once comes from the destination (apply + checkpoint in one transaction, no-op if
checkpoint >= to_pos); the loader additionally skips change sets already at/below the checkpoint
so a replay from an earlier low-water mark is idempotent.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime
from decimal import Decimal
from typing import Any

from alelt_common.models import (
    ChangeEvent,
    ChangeKind,
    ChangeSet,
    Position,
    TableInfo,
    TableRef,
)
from alelt_common.spi import ApplyStats, DestinationConnector

from .policy import ConnectionPolicy

log = logging.getLogger("alelt_loader")

Compare = Callable[[Position, Position], int]
COMPLETION_TXID = "completion:"            # alelt_hub.completion change sets (out-of-row values fetched by key)


class Backpressure(Exception):
    """Loader buffer full - caller must stop feeding until a flush succeeds."""


def _canon(v: Any) -> Any:
    """Canonical JSON form of a value (same rules as alelt_dst_snowflake.rows._jsonable)."""
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).hex()
    if isinstance(v, (datetime, date, dtime)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return format(v.normalize(), "f")
    return v


def surrogate_id(row: dict[str, Any], columns: list[str]) -> str:
    """Surrogate key for keyless tables: first 32 hex of sha256 over the canonical JSON of all data columns
    (identical to alelt_dst_snowflake.rows.surrogate_id; replace both if the golden spec shows another algorithm)."""
    payload = json.dumps([_canon(row.get(c)) for c in columns], separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def data_columns(info: TableInfo) -> list[str]:
    return [c.name for c in sorted(info.columns, key=lambda c: c.ordinal)]


@dataclass
class _Buffer:
    changesets: list[ChangeSet] = field(default_factory=list)
    to_pos: Position | None = None
    rows: int = 0
    since: float = 0.0


class TableDDLError(RuntimeError):
    """ensure_table (DDL) of one destination table failed; the flush applied nothing (DST-loader-01)."""

    def __init__(self, table: TableRef, dest: str, error: str, attempts: int, retry_in_s: float) -> None:
        self.table, self.dest, self.error, self.attempts, self.retry_in_s = table, dest, error, attempts, retry_in_s
        super().__init__(f"destination DDL for {dest} ({table.fqn}) failed {attempts}x, retry in {retry_in_s:.0f}s: "
                         f"{error}")


class Loader:
    """One loader per (connection, destination) stream."""

    def __init__(self, dest: DestinationConnector, policy: ConnectionPolicy, stream: str,
                 compare: Compare, max_rows: int = 50_000, max_latency_s: float = 120.0,
                 max_buffer_rows: int = 500_000, hold: Callable[[TableRef], bool] | None = None,
                 only: set[TableRef] | None = None, frozen: Callable[[TableRef], bool] | None = None) -> None:
        """`hold(table)` True => table is held (snapshot / schedule / integrity): its changes are not applied
        (replayed by the release). `frozen(table)`: the subset of holds that also stops out-of-row completion rows
        (`completion:` change sets, alelt_hub.completion); without it completion rows follow `hold`.
        `only` restricts the loader to a set of tables (catch-up replay)."""
        self.dest = dest
        self.policy = policy
        self.stream = stream
        self.compare = compare
        self.max_rows = max_rows
        self.max_latency_s = max_latency_s
        self.max_buffer_rows = max_buffer_rows
        self.hold = hold
        self.frozen = frozen
        self.only = only
        self.buf = _Buffer()
        self.last_flush_empty = False             # the last flush had nothing to apply (no destination transaction)
        self._ensured: set[tuple[str, int]] = set()
        self._ddl_fail: dict[str, tuple[int, float, str]] = {}    # dest -> (attempts, retry_at monotonic, error)

    # ------------------------------------------------------------------ buffering
    def add(self, changesets: list[ChangeSet], to_pos: Position, checkpoint: Position | None = None) -> None:
        """Buffer committed change sets of one source batch; advance the pending to_pos."""
        if self.buf.rows >= self.max_buffer_rows:
            raise Backpressure(f"loader buffer full ({self.buf.rows} rows)")
        for cs in changesets:
            if checkpoint is not None and self.compare(cs.commit_position, checkpoint) <= 0:
                continue                                # already applied (replay)
            self.buf.changesets.append(cs)
            self.buf.rows += len(cs.events)
        if not self.buf.since:
            self.buf.since = time.monotonic()
        self.buf.to_pos = to_pos

    def inject(self, cs: ChangeSet) -> None:
        """Out-of-row completion rows read at `cs.commit_position` (alelt_hub.completion): placed before the first buffered
        change set committed after it, so every later buffered change of the same key is applied on top. Does not
        advance to_pos (the rows ride the next flush of the stream)."""
        i = len(self.buf.changesets)
        for j, x in enumerate(self.buf.changesets):
            if not x.txid.startswith(COMPLETION_TXID) and self.compare(x.commit_position, cs.commit_position) > 0:
                i = j
                break
        self.buf.changesets.insert(i, cs)
        self.buf.rows += len(cs.events)
        if not self.buf.since:
            self.buf.since = time.monotonic()

    def injected(self) -> list[ChangeSet]:
        return [cs for cs in self.buf.changesets if cs.txid.startswith(COMPLETION_TXID)]

    def take_injected(self) -> list[ChangeSet]:
        """Remove and return the completion change sets of the buffer (idle flush through a transient stream)."""
        out = self.injected()
        if out:
            self.buf.changesets = [cs for cs in self.buf.changesets if not cs.txid.startswith(COMPLETION_TXID)]
            self.buf.rows -= sum(len(cs.events) for cs in out)
        return out

    def pending(self) -> bool:
        return self.buf.to_pos is not None

    def due(self) -> bool:
        return self.pending() and (self.buf.rows >= self.max_rows
                                   or time.monotonic() - self.buf.since >= self.max_latency_s)

    # ------------------------------------------------------------------ flush
    def flush(self, tables: dict[TableRef, TableInfo], batch_id: str | None = None,
              watermarks: dict[Any, Position | str | None] | None = None,
              extra_watermarks: dict[str, str | None] | None = None) -> list[ApplyStats]:
        """Apply the buffer atomically. Buffer is kept if the destination raises (backpressure/retry).
        `watermarks`: explicit per-table watermarks {dest_table: position} for the SPI (default when omitted: every
        table of the flush -> to_pos, committed with the data; ADR-005). `extra_watermarks`: keys added to the default
        set (e.g. the completion floor `<cid>#t:<table>`, alelt_hub.completion; None value = to_pos)."""
        if self.buf.to_pos is None:
            return []
        to_pos = self.buf.to_pos
        events, dtables = self.compact(self.buf.changesets, tables)
        self.last_flush_empty = not events
        if not events:                              # RC4: nothing to apply -> no destination transaction
            self.buf = _Buffer()
            return []
        table_map = {ref: self.policy.dest_name(ref) for ref in dtables}
        if extra_watermarks and not watermarks:
            watermarks = {**{ref: None for ref in dtables}, **extra_watermarks}
        for ref, info in dtables.items():
            # DST-loader-02: length / scale / nullability are part of the shape (type widening on drift fires)
            sig = (table_map[ref], hash(tuple((c.name, c.canonical_type, c.length, c.scale, c.nullable, c.key_seq)
                                              for c in info.columns)))
            if sig not in self._ensured:
                self._ensure(ref, info, table_map[ref], sig)
        last = self.buf.changesets[-1] if self.buf.changesets else None
        sets = [] if not events else [ChangeSet(
            txid=f"aleltbatch:{batch_id or ''}",
            commit_position=last.commit_position if last else to_pos,
            commit_ts=last.commit_ts if last else None, events=events)]
        if watermarks:
            stats = self.dest.apply(self.stream, sets, to_pos, batch_id or str(uuid.uuid4()), table_map, dtables,
                                    watermarks=watermarks)  # type: ignore[call-arg]
        else:
            stats = self.dest.apply(self.stream, sets, to_pos, batch_id or str(uuid.uuid4()), table_map, dtables)
        log.info("applied", extra={"component": "alelt_loader", "position": to_pos.text, "batch_id": batch_id,
                                   "table": ",".join(s.table for s in stats)})
        self.buf = _Buffer()
        return stats

    def apply_checkpoint(self, to_pos: Position, batch_id: str) -> None:
        """Checkpoint-only destination transaction (no rows): the destination catches up with the stream position
        after flushes that had nothing to apply (bounded restart replay window, honest lag / settled)."""
        self.dest.apply(self.stream, [], to_pos, batch_id, {}, {})

    def _ensure(self, ref: TableRef, info: TableInfo, dest: str, sig: tuple[str, int]) -> None:
        """ensure_table with bounded retries (DST-loader-01): a failing DDL is retried with exponential backoff
        (5 s .. 15 min) instead of hitting the destination on every flush; while backing off the flush raises
        TableDDLError naming the table (buffer kept, nothing applied) so the caller can hold / alert on it."""
        f = self._ddl_fail.get(dest)
        now = time.monotonic()
        if f is not None and now < f[1]:
            raise TableDDLError(ref, dest, f[2], f[0], f[1] - now)
        try:
            self.dest.ensure_table(info, dest, None)
        except Exception as e:  # noqa: BLE001 - typed and rate-limited below
            n = (f[0] if f else 0) + 1
            delay = min(900.0, 5.0 * 2 ** (n - 1))
            self._ddl_fail[dest] = (n, now + delay, f"{type(e).__name__}: {e}"[:500])
            log.error("ensure_table failed", extra={"component": "alelt_loader", "table": dest,
                                                    "correlation_id": f"attempt {n}, next in {delay:.0f}s"})
            raise TableDDLError(ref, dest, self._ddl_fail[dest][2], n, delay) from e
        self._ddl_fail.pop(dest, None)
        self._ensured.add(sig)

    # ------------------------------------------------------------------ compaction
    def compact(self, changesets: list[ChangeSet], tables: dict[TableRef, TableInfo]
                ) -> tuple[list[ChangeEvent], dict[TableRef, TableInfo]]:
        """Column policy + final op per key. Returns ordered events and destination TableInfos."""
        dtables: dict[TableRef, TableInfo] = {}
        per_table: dict[TableRef, OrderedDict[Any, ChangeEvent]] = OrderedDict()
        truncates: dict[TableRef, ChangeEvent] = {}

        for cs in changesets:
            completion = cs.txid.startswith(COMPLETION_TXID)
            for ev in cs.events:
                ref = ev.table
                if ref is None or ref not in tables or not self.policy.table(ref).enabled:
                    continue
                if self.only is not None and ref not in self.only:
                    continue
                if completion and self.frozen is not None:
                    if self.frozen(ref):
                        continue
                elif self.hold is not None and self.hold(ref):
                    continue
                info = dtables.get(ref)
                if info is None:
                    info = dtables[ref] = self.policy.project(tables[ref])
                slot = per_table.setdefault(ref, OrderedDict())
                if ev.kind is ChangeKind.TRUNCATE:
                    slot.clear()
                    truncates[ref] = ev
                    continue
                before = self.policy.apply_row(ref, ev.before)
                after = self.policy.apply_row(ref, ev.after)
                keys = info.key_columns()
                cols = data_columns(info)

                def key(row: dict[str, Any] | None) -> Any:
                    """Business key; keyless tables use the surrogate (the destination writes the id column)."""
                    if row is None:
                        return None
                    if not keys:
                        return surrogate_id(row, cols)
                    return tuple(_canon(row.get(c.name)) for c in keys)

                if not keys and before is not None and after is not None and any(c not in after for c in before):
                    after = {**before, **after}   # keyless partial image: surrogate of the MERGED row (HC-27)
                kb, ka = key(before), key(after)
                if ev.kind is ChangeKind.DELETE:
                    img = before or after
                    k = kb if before is not None else ka
                    if k is None:
                        continue
                    slot.pop(k, None)
                    slot[k] = ev.model_copy(update={"before": img, "after": None})
                    continue
                if ka is None:
                    continue
                if ev.kind is ChangeKind.UPDATE and kb is not None and kb != ka:
                    slot.pop(kb, None)       # key change => delete old key, upsert new key
                    slot[kb] = ev.model_copy(update={"kind": ChangeKind.DELETE, "before": before, "after": None})
                prev = slot.pop(ka, None)
                upd: dict[str, Any] = {}
                if prev is not None and prev.kind is not ChangeKind.DELETE and prev.after:
                    after = {**prev.after, **after}   # partial image (e.g. lob_unavailable): keep earlier values
                    if completion and prev.kind is ChangeKind.INSERT:
                        upd["kind"] = ChangeKind.INSERT   # completed insert stays an insert
                slot[ka] = ev.model_copy(update={"before": before, "after": after, **upd})

        out: list[ChangeEvent] = []
        for ref, slot in per_table.items():
            if ref in truncates:
                out.append(truncates[ref])
            out.extend(slot.values())
        return out, dtables
