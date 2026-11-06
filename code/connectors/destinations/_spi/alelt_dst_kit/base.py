"""Destination base classes - every destination plugin derives from `DestinationBase` (F-16).

`DestinationBase` (all engines, including the hand-written SQL / Postgres / Snowflake ones):
- position ordering, compare-and-set rules for the stream checkpoint (F-05), per-table watermarks (ADR-005);
- the verify helpers of the SPI (row_count, column_aggregates per key range, sample_rows, key_set / key_set_diff,
  deleted_keys, key_bucket_hashes, column_checksums) implemented generically over `fetch_rows()` - SQL engines
  override them with server-side queries;
- quarantine records and raw edge batches kept in the control area (`alelt_quarantine`, `alelt_raw_archive`), written
  through the engine's own ensure_table + load_snapshot (idempotent upserts by key);
- describe_schema over `list_tables()` / `describe_table()`.

`KitDestination` (lake / warehouse / streaming plugins) adds the apply template:
read control state -> no-op when checkpoint >= to_pos (replay) -> expected check -> plan_batch -> (optional) complete
partial images from current rows -> engine `commit_batch()` which MUST make data + checkpoint (+ watermarks + audit)
visible atomically (or, for engines without multi-object transactions, idempotently per object - documented per
plugin) AND compare-and-set the checkpoint against `prev` inside that commit (`self.cas(...)`).

Fault injection for the conformance suite: engines call `self._hook(point)` at 'after_read' (control state read,
nothing written), 'after_data' (data written, not committed) and 'before_commit'; a test replaces `_hook` with a
raiser to simulate a crash mid-apply, or runs a second writer from it (concurrent writers).
"""
from __future__ import annotations

import copy
import hashlib
import json
import time
from abc import abstractmethod
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time as dtime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from alelt_common.canonical_jsonl import canonical_snapshot_row, snapshot_types
from alelt_common.models import (CanonicalType, ChangeEvent, ChangeKind, ChangeSet, ColumnInfo, Position,
                              QuarantineRecord, TableInfo, TableRef)
from alelt_common.spi import UNSET, ApplyStats, CheckpointConflictError, DestinationConnector
from alelt_loader.loader import _canon
from alelt_loader.policy import SystemColumns, system_columns

from .plan import RowOp, TablePlan, complete_rows, plan_batch
from .urls import DestUrl, parse_url

Compare = Callable[[Position, Position], int]
QUARANTINE_TABLE = "alelt_quarantine"
RAW_ARCHIVE_TABLE = "alelt_raw_archive"


def default_compare(a: Position, b: Position) -> int:
    """Generic ordering for 'n:n:..' positions (decimal or zero-padded hex parts)."""
    def k(p: Position) -> tuple[tuple[int, str], ...]:
        return tuple((len(s.lstrip("0")), s.lstrip("0").upper()) for s in p.text.split(":"))
    ka, kb = k(a), k(b)
    return (ka > kb) - (ka < kb)


@dataclass
class ControlState:
    position: str | None = None
    seq: int = 0                  # number of batches applied for the stream (monotonic, used as write version)
    batch_id: str | None = None
    watermarks: dict[str, str] = field(default_factory=dict)   # {dest_table: position} of this stream


class AlreadyApplied(Exception):
    """Internal: the checkpoint found under the lock is already >= to_pos (same batch committed by another writer)."""


def wm_dumps(wm: Mapping[str, str]) -> str:
    return json.dumps(dict(sorted(wm.items())), separators=(",", ":"))


def wm_loads(v: Any) -> dict[str, str]:
    if v is None or v == "":
        return {}
    if hasattr(v, "read"):
        v = v.read()
    if isinstance(v, (bytes, bytearray, memoryview)):
        v = bytes(v).decode("utf-8")
    d = json.loads(v) if isinstance(v, str) else v
    return {str(k): str(x) for k, x in (d or {}).items()}


# ---------------------------------------------------------------------------- canonical key / value forms
def canon_value(v: Any) -> Any:
    """Engine-neutral comparable form of a value read back from any driver / file format."""
    if v is None:
        return None
    if isinstance(v, memoryview):
        v = bytes(v)
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).hex()
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float, Decimal)):
        try:
            return format(Decimal(str(v)).normalize(), "f")
        except InvalidOperation:  # pragma: no cover - nan / inf
            return str(v)
    if isinstance(v, datetime):
        if v.tzinfo is not None:
            v = v.astimezone(timezone.utc).replace(tzinfo=None)
        return v.isoformat()
    if isinstance(v, (date, dtime)):
        return v.isoformat()
    if isinstance(v, str):
        return v.rstrip()
    return str(v)


def canon_key(values: Iterable[Any]) -> tuple[Any, ...]:
    return tuple(canon_value(v) for v in values)


def _order(v: Any) -> tuple[int, Any]:
    """Sort/range key: numbers numerically, everything else as canonical text."""
    if isinstance(v, bool):
        return (0, Decimal(int(v)))
    if isinstance(v, (int, float, Decimal)):
        return (0, Decimal(str(v)))
    if isinstance(v, str):
        try:
            return (0, Decimal(v.strip()))
        except InvalidOperation:
            return (1, v.rstrip())
    return (1, str(canon_value(v)))


def in_range(v: Any, key_range: tuple[Any, Any] | None) -> bool:
    if not key_range:
        return True
    lo, hi = key_range
    o = _order(v)
    if lo is not None and o < _order(lo):
        return False
    return not (hi is not None and o > _order(hi))


def _ctl_col(name: str, ordinal: int, ct: CanonicalType, length: int | None = None, key: int | None = None
             ) -> ColumnInfo:
    return ColumnInfo(name=name, ordinal=ordinal, engine_type="CONTROL", canonical_type=ct, length=length,
                      key_seq=key, nullable=key is None)


QUARANTINE_INFO = TableInfo(table=TableRef(schema="ALELT_CONTROL", name="ALELT_QUARANTINE"), columns=[
    _ctl_col("QUARANTINE_ID", 0, CanonicalType.STRING, 64, 1),
    _ctl_col("STREAM", 1, CanonicalType.STRING, 200), _ctl_col("TABLE_NAME", 2, CanonicalType.STRING, 400),
    _ctl_col("POSITION_TEXT", 3, CanonicalType.STRING, 400), _ctl_col("REASON", 4, CanonicalType.STRING, 4000),
    _ctl_col("RAW_BATCH_ID", 5, CanonicalType.STRING, 64), _ctl_col("RAW_OFFSET", 6, CanonicalType.LONG),
    _ctl_col("RAW_LENGTH", 7, CanonicalType.LONG), _ctl_col("RAW_HEX", 8, CanonicalType.STRING, 4000),
    _ctl_col("QUARANTINED_AT", 9, CanonicalType.INSTANT)])
RAW_ARCHIVE_INFO = TableInfo(table=TableRef(schema="ALELT_CONTROL", name="ALELT_RAW_ARCHIVE"), columns=[
    _ctl_col("CONNECTION_ID", 0, CanonicalType.STRING, 200, 1), _ctl_col("BATCH_ID", 1, CanonicalType.STRING, 200, 2),
    _ctl_col("DAY", 2, CanonicalType.LOCALDATE), _ctl_col("HEADER", 3, CanonicalType.STRING, 4000),
    _ctl_col("BODY", 4, CanonicalType.BINARY), _ctl_col("BODY_BYTES", 5, CanonicalType.LONG),
    _ctl_col("SHA256", 6, CanonicalType.STRING, 64), _ctl_col("ARCHIVED_AT", 7, CanonicalType.INSTANT)])


def quarantine_id(stream: str, r: QuarantineRecord) -> str:
    raw = r.raw_ref
    ident = [stream, r.table.fqn if r.table else None, r.position.text if r.position else None, r.reason,
             str(raw.batch_id) if raw else None, raw.offset if raw else None, raw.length if raw else None]
    return hashlib.sha256(json.dumps(ident, default=str).encode()).hexdigest()[:32]


# ---------------------------------------------------------------------------- shared base
class DestinationBase(DestinationConnector):
    """Shared behaviour of every destination (see module doc). Subclasses set compare / schema / control / sys
    (output-profile system columns) and implement `fetch_rows` (or override the verify helpers)."""

    engine = "base"
    compare: Compare = staticmethod(default_compare)  # type: ignore[assignment]
    schema: str = "landing"
    control: str = "alelt_control"
    sys: SystemColumns = system_columns(None)
    mirror = False                                    # True: no system columns (hard deletes)
    keeps_timestamp_text = False                      # True: timestamp text is stored as delivered (Kafka, memory)

    def refuse_lossy_values(self, changesets: list[ChangeSet], tables: Mapping[TableRef, TableInfo]) -> None:
        """XC-31 / XC-36: values the engine would silently change are refused before anything is written. Today:
        timestamp text with non-zero digits below the microsecond (Db2 TIMESTAMP(7..12), SQL Server datetime2(7),
        Oracle FF9) - every engine here stores microseconds and would round."""
        if self.keeps_timestamp_text:
            return
        from .arrowutil import parse_timestamp_text

        ts_types = (CanonicalType.LOCALDATETIME, CanonicalType.INSTANT)
        for c in changesets:
            for ev in c.events:
                info = tables.get(ev.table) if ev.table is not None else None
                if info is None:
                    continue
                cols = {col.name: col.canonical_type for col in info.columns if col.canonical_type in ts_types}
                if not cols:
                    continue
                for img in (ev.before, ev.after):
                    for k, v in (img or {}).items():
                        if isinstance(v, str) and k in cols:
                            parse_timestamp_text(v)          # raises SubMicrosecondError on lossy text

    # ------------------------------------------------------------------ naming
    # DST-loader-05: characters a schema / dataset / topic-part name may not contain on this engine (None = any:
    # the engine quotes identifiers). The landing schema defaults to the connection id (e.g. 'erp-prod').
    container_forbidden: str | None = None

    def safe_container(self, name: str) -> str:
        if not self.container_forbidden:
            return name
        import re as _re

        out = _re.sub(self.container_forbidden, "_", name)
        if out != name:
            import logging

            logging.getLogger("alelt_dst_kit").warning("%s: schema %r is not a valid name here, using %r", self.engine, name, out)
        return out

    def ident(self, name: str) -> str:
        return name

    @property
    def c_synced(self) -> str:
        return self.ident(self.sys.synced)

    @property
    def c_deleted(self) -> str:
        return self.ident(self.sys.deleted)

    @property
    def c_row_id(self) -> str:
        return self.ident(self.sys.row_id)

    def _hook(self, point: str) -> None:
        """Fault-injection point (no-op in production)."""

    # ------------------------------------------------------------------ checkpoint CAS (F-05)
    def pos_ge(self, a: str | None, b: Position) -> bool:
        """a (checkpoint text) >= b."""
        return bool(a) and self.compare(Position(engine=b.engine, text=str(a)), b) >= 0

    @staticmethod
    def same_state(a: ControlState, b: ControlState) -> bool:
        return (a.position or None) == (b.position or None) and a.seq == b.seq and \
            (a.batch_id or None) == (b.batch_id or None)

    def check_expected(self, stream: str, found: ControlState, expected: Any, to_pos: Position) -> None:
        """apply(expected=...) check against the checkpoint found. Raises AlreadyApplied / CheckpointConflictError."""
        if expected is UNSET:
            return
        exp = expected.text if isinstance(expected, Position) else (expected or None)
        if (found.position or None) == exp:
            return
        if self.pos_ge(found.position, to_pos):
            raise AlreadyApplied
        raise CheckpointConflictError(stream, exp, found.position, "checkpoint is not the expected base of the batch")

    def cas(self, stream: str, prev: ControlState, found: ControlState, to_pos: Position) -> None:
        """Compare-and-set rule, called by engines INSIDE their commit with the checkpoint as found there (under the
        row/table lock, at the expected snapshot, by the fenced producer...)."""
        if self.same_state(prev, found):
            return
        if self.pos_ge(found.position, to_pos):
            raise AlreadyApplied
        raise CheckpointConflictError(stream, prev.position, found.position,
                                      f"concurrent writer (seq {prev.seq} -> {found.seq}, batch {found.batch_id})")

    # ------------------------------------------------------------------ watermarks (ADR-005)
    def batch_watermarks(self, to_pos: Position, table_map: Mapping[TableRef, str],
                         watermarks: Mapping[Any, Position | str] | None) -> dict[str, str]:
        if watermarks is None:
            return {self.ident(d): to_pos.text for d in dict.fromkeys(table_map.values())}
        out: dict[str, str] = {}
        for k, v in watermarks.items():         # explicit keys are kept verbatim (e.g. `<cid>#t:<table>`)
            name = self.ident(table_map[k]) if isinstance(k, TableRef) and k in table_map else \
                (k.fqn if isinstance(k, TableRef) else str(k))
            out[name] = v.text if isinstance(v, Position) else (to_pos.text if v is None else str(v))
        return out

    def merge_watermarks(self, old: Mapping[str, str], new: Mapping[str, str], engine: str = "") -> dict[str, str]:
        """Watermarks only move forward."""
        out = dict(old)
        for t, pos in new.items():
            cur = out.get(t)
            if cur is None or self.compare(Position(engine=engine, text=pos), Position(engine=engine, text=cur)) > 0:
                out[t] = pos
        return out

    def delete_checkpoint(self, stream: str) -> None:
        """Drop the checkpoint row of a transient stream (replay pass `<cid>#pass:<id>`, HC-21). Engines override."""
        raise NotImplementedError

    # ------------------------------------------------------------------ verify helpers (generic, over fetch_rows)
    def fetch_rows(self, dest_name: str) -> list[dict[str, Any]]:  # pragma: no cover - per engine
        """All current rows (physical, lower-case column names), soft-deleted included; hard-deleted rows absent."""
        raise NotImplementedError

    def _col(self, name: str) -> str:
        return self.ident(name).lower()

    def _is_deleted(self, r: Mapping[str, Any]) -> bool:
        if self.mirror:
            return False
        return r.get(self._col(self.sys.deleted)) in (True, 1, "1", "true", "True", "t")

    def scan_rows(self, dest_name: str, columns: list[str] | None, include_deleted: bool, key_col: str | None,
                  key_range: tuple[Any, Any] | None) -> list[dict[str, Any]]:
        """Rows for the verify helpers. Engines override with projection (`columns`, physical names; None = all) and
        filter pushdown (live rows, key range on `key_col`) - lakes prune files by statistics instead of reading the
        table. Default: fetch_rows (the caller filters again)."""
        return self.fetch_rows(dest_name)

    def _rows_for(self, dest_name: str, include_deleted: bool = False, key_cols: Sequence[str] | None = None,
                  key_range: tuple[Any, Any] | None = None, schema: str | None = None,
                  columns: Sequence[str] | None = None) -> list[dict[str, Any]]:
        phys = None
        if columns is not None:
            phys = list(dict.fromkeys([*(self.ident(c) for c in [*(key_cols or []), *columns]),
                                       *([] if self.mirror else [self.c_deleted])]))
        k0 = self.ident(key_cols[0]) if key_cols else None
        rows = self._on_schema(schema).scan_rows(dest_name, phys, include_deleted, k0, key_range)
        out = [{k.lower(): v for k, v in r.items()} for r in rows]
        if not include_deleted:
            out = [r for r in out if not self._is_deleted(r)]
        if key_cols and key_range:
            k0 = self._col(key_cols[0])
            out = [r for r in out if in_range(r.get(k0), key_range)]
        return out

    def fetch_rows_in(self, schema: str, dest_name: str) -> list[dict[str, Any]]:
        """Schema-qualified row access (X-124 reference-landing parity): rows of `schema.dest_name` in this same
        destination (lower-case column names)."""
        return [{k.lower(): v for k, v in r.items()} for r in self._on_schema(schema).fetch_rows(dest_name)]

    def _on_schema(self, schema: str | None) -> Any:
        if not schema or schema == self.schema:
            return self
        c = copy.copy(self)
        c.schema = schema
        return c

    def row_count(self, dest_name: str, include_deleted: bool = False, **kw: Any) -> int:
        return len(self._rows_for(dest_name, include_deleted, schema=kw.get("schema"), columns=[]))

    def column_aggregates(self, dest_name: str, columns: list[str] | None = None, key_col: str | None = None,
                          key_range: tuple[Any, Any] | None = None, include_deleted: bool = False,
                          **kw: Any) -> dict[str, Any]:
        rows = self._rows_for(dest_name, include_deleted, [key_col] if key_col else None, key_range,
                              kw.get("schema"), columns=columns)
        system = {self._col(self.sys.synced), self._col(self.sys.deleted)}
        cols = [self._col(c) for c in columns] if columns else \
            [c for c in (rows[0] if rows else {}) if c not in system]
        out: dict[str, Any] = {"_rows": len(rows)}
        for c in cols:
            vals = [r.get(c) for r in rows if r.get(c) is not None]
            agg: dict[str, Any] = {"count": len(vals)}
            plain = [v for v in vals if not isinstance(v, (bytes, bytearray, memoryview, dict, list, bool))]
            if plain and len(plain) == len(vals):
                agg["min"] = min(plain, key=_order)
                agg["max"] = max(plain, key=_order)
                if all(_order(v)[0] == 0 for v in plain):          # numbers (or numeric text: JSON decimals)
                    agg["sum"] = sum(_order(v)[1] for v in plain)
            out[c] = agg
        return out

    def aggregates(self, dest_name: str, columns: list[str]) -> dict[str, Any]:
        """count of live rows + sum/min/max of the given numeric columns (live rows) - older form of
        column_aggregates()."""
        ag = {k.lower(): v for k, v in self.column_aggregates(dest_name, columns).items()}
        out: dict[str, Any] = {"count": ag["_rows"]}
        for c in columns:
            a = ag.get(c.lower(), {})
            out[f"sum:{c}"], out[f"min:{c}"], out[f"max:{c}"] = a.get("sum"), a.get("min"), a.get("max")
        return out

    def sample_rows(self, dest_name: str, key_cols: list[str], columns: list[str] | None = None,
                    keys: list[tuple[Any, ...]] | None = None, **kw: Any) -> Any:
        rows = self._rows_for(dest_name, bool(kw.get("include_deleted")) or keys is not None, key_cols,
                              kw.get("key_range"), kw.get("schema"))
        kc = [self._col(k) for k in key_cols]
        pick = [self._col(c) for c in dict.fromkeys([*key_cols, *(columns or [])])] if columns else None
        shape = (lambda r: {c: r.get(c) for c in pick}) if pick else dict
        if keys is not None:
            want = {canon_key(k): k for k in keys}
            out: dict[tuple[Any, ...], dict[str, Any]] = {}
            for r in rows:
                ck = canon_key(r.get(c) for c in kc)
                if ck in want:
                    out[tuple(want[ck])] = shape(r)
            return out
        rows.sort(key=lambda r: tuple(_order(r.get(c)) for c in kc))
        return [shape(r) for r in rows[: int(kw.get("n") or 10)]]

    def sample(self, dest_name: str, key_cols: list[str], n: int = 10) -> list[dict[str, Any]]:
        """First n live rows in key order (kept for older callers)."""
        return self.sample_rows(dest_name, key_cols, n=n)  # type: ignore[no-any-return]

    def key_set(self, dest_name: str, key_cols: list[str], key_range: tuple[Any, Any] | None = None,
                include_deleted: bool = False, **kw: Any) -> list[tuple[Any, ...]]:
        kc = [self._col(k) for k in key_cols]
        rows = self._rows_for(dest_name, include_deleted, key_cols, key_range, kw.get("schema"), columns=[])
        return sorted({canon_key(r.get(c) for c in kc) for r in rows}, key=lambda t: tuple(map(_order, t)))

    def key_set_diff(self, dest_name: str, key_cols: list[str], reference: Iterable[tuple[Any, ...]],
                     key_range: tuple[Any, Any] | None = None, **kw: Any) -> dict[str, list[tuple[Any, ...]]]:
        mine = set(self.key_set(dest_name, key_cols, key_range, **kw))
        ref = {canon_key(k) for k in reference if not key_range or in_range(k[0], key_range)}
        srt = lambda s: sorted(s, key=lambda t: tuple(map(_order, t)))  # noqa: E731
        return {"missing": srt(ref - mine), "extra": srt(mine - ref)}

    def deleted_keys(self, dest_name: str, key_cols: list[str], **kw: Any) -> list[tuple[Any, ...]]:
        kc = [self._col(k) for k in key_cols]
        rows = self._rows_for(dest_name, True, key_cols, schema=kw.get("schema"), columns=[])
        return sorted({canon_key(r.get(c) for c in kc) for r in rows if self._is_deleted(r)},
                      key=lambda t: tuple(map(_order, t)))

    def _bucket(self, key: tuple[Any, ...], buckets: int) -> int:
        return int(hashlib.sha256(json.dumps(list(key), default=str).encode()).hexdigest(), 16) % buckets

    def key_bucket_hashes(self, dest_name: str, key_cols: list[str], buckets: int, **kw: Any) -> dict[int, str]:
        acc: dict[int, list[str]] = {}
        system = {self._col(self.sys.synced), self._col(self.sys.deleted)}
        kc = [self._col(c) for c in key_cols]
        for r in self._rows_for(dest_name, schema=kw.get("schema")):
            k = json.dumps([_canon(r.get(c)) for c in kc], default=str)
            b = int(hashlib.sha256(k.encode()).hexdigest(), 16) % buckets
            data = {c: _canon(v) for c, v in r.items() if c not in system}
            acc.setdefault(b, []).append(json.dumps(data, sort_keys=True, default=str))
        return {b: hashlib.sha256("\n".join(sorted(v)).encode()).hexdigest() for b, v in acc.items()}

    def column_checksums(self, dest_name: str, key_cols: list[str], columns: list[str], buckets: int,
                         **kw: Any) -> dict[str, dict[int, str]]:
        kc = [self._col(c) for c in key_cols]
        acc: dict[str, dict[int, list[str]]] = {c: {} for c in columns}
        for r in self._rows_for(dest_name, False, key_cols, schema=kw.get("schema"), columns=columns):
            k = canon_key(r.get(c) for c in kc)
            b = self._bucket(k, buckets)
            for c in columns:
                acc[c].setdefault(b, []).append(json.dumps([list(k), canon_value(r.get(self._col(c)))], default=str))
        return {c: {b: hashlib.sha256("\n".join(sorted(v)).encode()).hexdigest() for b, v in per.items()}
                for c, per in acc.items()}

    # ------------------------------------------------------------------ describe_schema
    def list_tables(self, schema: str | None = None) -> list[str]:  # pragma: no cover - per engine
        raise NotImplementedError

    def describe_table(self, dest_name: str, schema: str | None = None) -> list[dict[str, Any]]:  # pragma: no cover
        raise NotImplementedError

    def describe_schema(self, tables: list[str] | None = None, schema: str | None = None
                        ) -> dict[str, list[dict[str, Any]]]:
        names = tables if tables is not None else self.list_tables(schema)
        return {n: self.describe_table(n, schema) for n in names}

    # ------------------------------------------------------------------ quarantine / raw archive (control area)
    def control_area(self) -> Any:
        """This destination re-targeted at the control schema (for the generic control tables)."""
        return self._on_schema(self.control)

    def quarantine(self, stream: str, records: Sequence[QuarantineRecord]) -> int:
        if not records:
            return 0
        now = datetime.now(timezone.utc)
        rows = [{"QUARANTINE_ID": quarantine_id(stream, r), "STREAM": stream,
                 "TABLE_NAME": r.table.fqn if r.table else None,
                 "POSITION_TEXT": r.position.text if r.position else None, "REASON": r.reason[:4000],
                 "RAW_BATCH_ID": str(r.raw_ref.batch_id) if r.raw_ref else None,
                 "RAW_OFFSET": r.raw_ref.offset if r.raw_ref else None,
                 "RAW_LENGTH": r.raw_ref.length if r.raw_ref else None,
                 "RAW_HEX": (r.raw_hex or None) and r.raw_hex[:4000], "QUARANTINED_AT": now} for r in records]
        ca = self.control_area()
        ca.ensure_table(QUARANTINE_INFO, QUARANTINE_TABLE, None)
        return int(ca.load_snapshot(QUARANTINE_TABLE, QUARANTINE_INFO, rows))

    def archive_raw(self, connection_id: str, batch_id: str, body: bytes, header: dict[str, Any],
                    day: date | None = None) -> str:
        d = day or datetime.now(timezone.utc).date()
        ca = self.control_area()
        ca.ensure_table(RAW_ARCHIVE_INFO, RAW_ARCHIVE_TABLE, None)
        ca.load_snapshot(RAW_ARCHIVE_TABLE, RAW_ARCHIVE_INFO, [{
            "CONNECTION_ID": connection_id, "BATCH_ID": str(batch_id), "DAY": d,
            "HEADER": json.dumps(header, sort_keys=True, default=str), "BODY": bytes(body),
            "BODY_BYTES": len(body), "SHA256": hashlib.sha256(body).hexdigest(),
            "ARCHIVED_AT": datetime.now(timezone.utc)}])
        return f"{self.engine}:{self.control}.{RAW_ARCHIVE_TABLE}/{connection_id}/{d:%Y/%m/%d}/{batch_id}"

    def quarantined(self, stream: str | None = None) -> list[dict[str, Any]]:
        """Quarantine rows kept by `quarantine()` (tests / diagnosis)."""
        ca = self.control_area()
        try:
            rows = ca.fetch_rows(QUARANTINE_TABLE)
        except Exception:  # noqa: BLE001 - table not created yet
            return []
        rows = [{k.lower(): v for k, v in r.items()} for r in rows]
        return [r for r in rows if stream is None or r.get("stream") == stream]

    def raw_archived(self, connection_id: str, batch_id: str) -> tuple[bytes, dict[str, Any]] | None:
        """(body, header) archived for a batch, None if absent."""
        for r in self.control_area().fetch_rows(RAW_ARCHIVE_TABLE):
            r = {k.lower(): v for k, v in r.items()}
            if r.get("connection_id") == connection_id and r.get("batch_id") == str(batch_id):
                body = r.get("body")
                if isinstance(body, str):
                    body = bytes.fromhex(body)
                return bytes(body or b""), json.loads(r.get("header") or "{}")
        return None


class KitDestination(DestinationBase):
    engine = "kit"
    needs_full_rows = False       # engine rewrites whole rows => partial images completed from current rows

    def __init__(self, url: str = "", schema: str = "landing", control_schema: str = "alelt_control",
                 output_profile: str | None = None, compare: Compare | None = None,
                 delete_mode: str | None = None, case: str | None = None, **opts: Any) -> None:
        self.url = url
        self.u: DestUrl = parse_url(url) if url else DestUrl(scheme="")
        self.case = (case or self.u.get("case") or "lower").lower()
        self.schema = self.safe_container(self.ident(self.u.get("schema") or schema))
        self.control = self.safe_container(self.ident(self.u.get("control_schema") or control_schema))
        self.sys: SystemColumns = system_columns(output_profile or self.u.get("output_profile"))
        self.compare: Compare = compare or default_compare  # type: ignore[misc]
        self.delete_mode = (delete_mode or self.u.get("delete_mode") or "soft").lower()
        if self.delete_mode not in ("soft", "hard"):
            raise ValueError(f"delete_mode must be soft|hard, got {self.delete_mode!r}")
        self.opts = opts
        self.batch_wm: dict[str, str] = {}      # watermarks to commit with the current batch (set by apply)

    # ------------------------------------------------------------------ naming
    def ident(self, name: str) -> str:
        case = getattr(self, "case", "lower")
        return name.lower() if case == "lower" else name.upper() if case == "upper" else name

    def _col(self, name: str) -> str:
        return self.ident(name).lower()

    @property
    def hard(self) -> bool:
        return self.delete_mode == "hard"

    def _is_deleted(self, r: Mapping[str, Any]) -> bool:
        return r.get(self._col(self.sys.deleted)) in (True, 1, "1", "true", "True", "t")

    # ------------------------------------------------------------------ engine API
    @abstractmethod
    def control_state(self, stream: str) -> ControlState: ...

    @abstractmethod
    def commit_batch(self, stream: str | None, plans: list[TablePlan], to_pos: Position | None, batch_id: str,
                     prev: ControlState, ts: datetime, stats: dict[str, ApplyStats]) -> None:
        """Write plans + checkpoint(stream)=to_pos (seq=prev.seq+1, watermarks=self.batch_wm) + audit atomically,
        compare-and-set against `prev` inside the commit (`self.cas(stream, prev, found, to_pos)`).
        stream None = snapshot load (no checkpoint, no audit)."""

    def lookup(self, plan: TablePlan, keys: list[tuple[Any, ...]]) -> dict[tuple[Any, ...], dict[str, Any]]:
        """Current rows by canonical key (source column names). Engines with needs_full_rows override."""
        return {}

    def wm_text(self) -> str:
        return wm_dumps(self.batch_wm)

    def next_state(self, prev: ControlState, to_pos: Position, batch_id: str) -> ControlState:
        return ControlState(to_pos.text, prev.seq + 1, batch_id, dict(self.batch_wm))

    # ------------------------------------------------------------------ SPI
    def read_checkpoint(self, stream: str) -> Position | None:
        st = self.control_state(stream)
        return Position(engine="", text=st.position) if st.position else None

    def read_watermarks(self, stream: str) -> dict[str, Position]:
        return {t: Position(engine="", text=p) for t, p in self.control_state(stream).watermarks.items()}

    def apply(self, stream: str, changesets: list[ChangeSet], to_pos: Position, batch_id: str,
              table_map: dict[TableRef, str], tables: dict[TableRef, TableInfo], *, expected: Any = UNSET,
              watermarks: Mapping[Any, Position | str] | None = None) -> list[ApplyStats]:
        t0 = time.monotonic()
        self.refuse_lossy_values(changesets, tables)
        prev = self.control_state(stream)
        try:
            self.check_expected(stream, prev, expected, to_pos)
        except AlreadyApplied:
            return []
        if self.pos_ge(prev.position, to_pos):
            return []                                   # idempotent replay
        self._hook("after_read")
        plans = self._plans(changesets, table_map, tables)
        stats = {p.dest: ApplyStats(table=p.dest) for p in plans}
        self.batch_wm = self.merge_watermarks(prev.watermarks, self.batch_watermarks(to_pos, table_map, watermarks),
                                              to_pos.engine)
        try:
            self.commit_batch(stream, plans, to_pos, batch_id, prev, datetime.now(timezone.utc), stats)
        except AlreadyApplied:
            return []                                   # committed by another writer meanwhile
        finally:
            self.batch_wm = {}
        dur = int((time.monotonic() - t0) * 1000)
        for s in stats.values():
            s.duration_ms = dur
        return list(stats.values())

    def load_snapshot(self, dest_name: str, table: TableInfo, rows: Iterable[dict[str, Any]]) -> int:
        types = snapshot_types(table)          # snapshot values in the CDC forms (JSON = text; dict/list accepted)
        evs = [ChangeEvent(position=Position(engine="snapshot", text="0"), txid="snapshot", kind=ChangeKind.INSERT,
                           table=table.table, after=canonical_snapshot_row(r, types, strict=True)) for r in rows]
        if not evs:
            return 0
        cs = ChangeSet(txid="snapshot", commit_position=evs[0].position, commit_ts=None, events=evs)
        plans = self._plans([cs], {table.table: dest_name}, {table.table: table}, full_images=True)
        stats = {p.dest: ApplyStats(table=p.dest) for p in plans}
        self.commit_batch(None, plans, None, "snapshot", ControlState(), datetime.now(timezone.utc), stats)
        return len(evs)

    def _plans(self, changesets: list[ChangeSet], table_map: dict[TableRef, str],
               tables: dict[TableRef, TableInfo], full_images: bool = False) -> list[TablePlan]:
        """`full_images`: every op is a complete row image (snapshot chunk) - no lookup of current rows (DST-kit-06)."""
        plans = list(plan_batch(changesets, table_map, tables, self.sys).values())
        for p in plans:
            existing: dict[tuple[Any, ...], dict[str, Any]] | None = None
            if full_images and not any(op.partial or op.op == "D" for op in p.ops.values()):
                p.existing = None  # type: ignore[attr-defined]
                continue
            if self.needs_full_rows and p.ops:
                existing = self.lookup(p, list(p.ops))
                complete_rows(p, existing)
            p.existing = set(existing) if existing is not None else None  # type: ignore[attr-defined]
        return plans

    # ------------------------------------------------------------------ helpers for engines
    def table_decision(self, stream: str, prev: ControlState, to_pos: Position, batch_id: str,
                       found_seq: int, found_pos: str | None, found_batch: str | None) -> str:
        """Per-table commit marker check for per-table atomic engines (lakes): 'write' or 'skip', else conflict.
        The table marker (seq, position, batch) is written in the same table commit as the data."""
        seq = prev.seq + 1
        if found_seq <= prev.seq:
            return "write"
        if found_seq == seq and (found_pos or None) == to_pos.text and (found_batch is None or found_batch == batch_id):
            return "skip"                               # this batch already committed this table (crash after it)
        if found_seq == seq and found_pos and self.compare(Position(engine=to_pos.engine, text=found_pos), to_pos) <= 0:
            return "write"                              # left by a crashed attempt of a smaller batch: idempotent
        raise CheckpointConflictError(stream, prev.position, found_pos,
                                      f"table already written by another writer (seq {found_seq})")

    @staticmethod
    def count(plan: TablePlan, st: ApplyStats) -> None:
        known: set[Any] | None = getattr(plan, "existing", None)
        for op in plan.ops.values():
            if op.op == "D":
                st.deletes += 1
            elif known is not None:
                if op.key in known:
                    st.updates += 1
                else:
                    st.inserts += 1
            elif op.kind is ChangeKind.INSERT:
                st.inserts += 1
            else:
                st.updates += 1

    def phys_row(self, plan: TablePlan, op: RowOp, ts: datetime, full: bool = True) -> dict[str, Any]:
        """Destination row: data columns (physical names) + synced + deleted. full=False keeps only present cols."""
        cols = plan.columns if full else [c for c in plan.columns if c in op.row]
        out = {self.ident(c): op.row.get(c) for c in cols}
        out[self.c_synced] = ts
        out[self.c_deleted] = op.op == "D"
        return out

    def data_cols(self, info: TableInfo, keyless: bool) -> list[ColumnInfo]:
        cols = sorted(info.columns, key=lambda c: c.ordinal)
        if keyless:
            cols = [ColumnInfo(name=self.sys.row_id, ordinal=-1, engine_type="SURROGATE",
                               canonical_type=CanonicalType.STRING, length=32, nullable=False, key_seq=1), *cols]
        return cols

    def key_names(self, info: TableInfo) -> list[str]:
        keys = [c.name for c in info.key_columns()]
        return keys or [self.sys.row_id]

    def audit_record(self, stream: str, prev: ControlState, to_pos: Position, batch_id: str,
                     stats: dict[str, ApplyStats], dur_ms: int = 0) -> dict[str, Any]:
        by = {s.table: {"inserts": s.inserts, "updates": s.updates, "deletes": s.deletes} for s in stats.values()}
        return {"batch_id": batch_id, "stream": stream, "from_position": prev.position, "to_position": to_pos.text,
                "seq": prev.seq + 1, "rows_by_table": json.dumps(by, sort_keys=True),
                "total_rows": sum(s.inserts + s.updates + s.deletes for s in stats.values()),
                "duration_ms": dur_ms, "status": "APPLIED"}

    # ------------------------------------------------------------------ describe_schema default
    def list_tables(self, schema: str | None = None) -> list[str]:
        return sorted(getattr(self, "_info", {}) or {})

    def describe_table(self, dest_name: str, schema: str | None = None) -> list[dict[str, Any]]:
        """Default: the remembered TableInfo + system columns (canonical type names)."""
        info = (getattr(self, "_info", {}) or {}).get(self.ident(dest_name))
        if info is None:
            raise KeyError(dest_name)
        keyless = not info.key_columns()
        out = [{"name": self.ident(c.name), "type": c.canonical_type.value.lower(), "nullable": not c.key_seq}
               for c in self.data_cols(info, keyless)]
        out += [{"name": self.c_synced, "type": "instant", "nullable": False},
                {"name": self.c_deleted, "type": "boolean", "nullable": False}]
        return out
