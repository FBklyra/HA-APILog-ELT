"""Source and Destination SPIs (DESIGN.md §5-§6). Engine plugins implement these; the core uses nothing else.

Plugins are registered by engine name in config (`connectors.sources.<engine>` / `connectors.destinations.<engine>`)
as 'module:Class' and loaded with alelt_common.registry.load().
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Literal

from .completeness import Census, KeyedReread
from .models import (
    CanonicalType,
    CatalogSnapshot,
    ChangeEvent,
    ChangeSet,
    ColumnInfo,
    Position,
    QuarantineRecord,
    TableInfo,
    TableRef,
)


class SourceGapError(RuntimeError):
    """The source no longer holds the changes after the resume position (log/CDC retention passed it). Retrying cannot
    help: the capture runner stops the connection (state ERROR) and opens a "resync needed" incident (REQ-20).
    Connectors raise it (or a subclass); engine-specific names are also recognised by `is_gap_error`."""


GAP_ERROR_NAMES = frozenset({"SourceGapError", "RetentionGapError", "LogNotAvailable", "LogMinerGap",
                             "LogGapError", "BinlogPurgedError", "SlotLostError"})


def is_gap_error(e: BaseException) -> bool:
    """True for SourceGapError and the engine gap errors named in GAP_ERROR_NAMES (anywhere in the MRO)."""
    return isinstance(e, SourceGapError) or any(c.__name__ in GAP_ERROR_NAMES for c in type(e).__mro__)


@dataclass
class DecodeResult:
    events: list[ChangeEvent]
    quarantined: list[QuarantineRecord]
    skipped: int = 0


class SourceConnector(ABC):
    """Hub-side part of a source engine."""

    engine: str
    capture_mode: Literal["edge", "remote"]

    # --- positions
    @abstractmethod
    def parse_position(self, text: str) -> Position: ...
    @abstractmethod
    def compare(self, a: Position, b: Position) -> int:
        """<0 if a<b, 0 if equal, >0 if a>b."""

    # --- types
    @abstractmethod
    def canonical_type(self, col: ColumnInfo) -> CanonicalType: ...

    # --- decoding (edge mode: raw batch shipped by agent)
    @abstractmethod
    def decode(self, raw: bytes, catalog: CatalogSnapshot, batch_id: Any) -> DecodeResult: ...

    # --- remote mode (optional)
    def stream(self, from_pos: Position | None) -> Iterator[ChangeEvent | None]:  # pragma: no cover
        """Remote mode: whole committed txns in commit order; None = idle tick (REMOTE.md §3)."""
        raise NotImplementedError(f"{self.engine} is edge-captured")

    def read_catalog(self) -> CatalogSnapshot:  # pragma: no cover
        raise NotImplementedError

    def snapshot_rows(self, table: TableInfo, key_range: tuple[Any, Any] | None,
                      chunk_rows: int) -> Iterator[list[dict[str, Any]]]:  # pragma: no cover
        raise NotImplementedError

    # remote capture runner hooks (connectors/sources/_spi/REMOTE.md); no-op defaults
    def ack(self, pos: Position) -> None:
        """`pos` (a batch next_pos) is durable at the hub: the source may release log retention up to it."""

    def current_position(self) -> Position | None:
        """Current end of the source log (snapshot start position), if the engine can tell."""
        return None

    def close(self) -> None:
        """Release connections; idempotent."""

    # --- source completeness (alelt_common/completeness.py; DESIGN-REVIEW F-01/F-02/F-07)
    @property
    def census(self) -> Census:
        """Per-instance record census: every record read is counted as decoded / ignored / needs_keyed_reread /
        needs_catalog_refresh / unsupported (or 'unexplained' = no rule, also emitted as an unsupported signal)."""
        c = self.__dict__.get("_alelt_census")
        if c is None:
            c = self.__dict__["_alelt_census"] = Census(getattr(self, "engine", "?"))
        return c

    def keyed_reread(self, table: TableInfo, keys: list[dict[str, Any]]) -> KeyedReread:
        """Current rows for `keys` (primary-key values) in ONE consistent read; `read_from` = source position taken
        before the read. Remote engines: hub-side SELECT by primary key (engine isolation documented per connector);
        edge engines: the agent's whitelisted keyed re-read / VERIFY_TABLE command. NotImplementedError = the hub
        escalates the table to hold + resync."""
        raise NotImplementedError(f"{getattr(self, 'engine', '?')}: keyed re-read not available")

    # --- prerequisites shown in console
    def prerequisites(self) -> list[str]:
        return []

    # --- pipeline wizard (optional)
    connection_fields: list[dict[str, Any]] = []   # [{name, label, secret?, required?, default?}] for the source form
    display_name: str = ""

    def prerequisite_checks(self, catalog: CatalogSnapshot | None, probes: dict[str, Any],
                            tables: list[TableRef] | None = None, settings: dict[str, Any] | None = None
                            ) -> list[dict[str, Any]]:
        """[{id, label, status PASS|FAIL|UNKNOWN|INFO, detail, fix_sql?}] - fix_sql is shown to the DBA, never run (R1)."""
        return [{"id": f"p{i}", "label": p, "status": "UNKNOWN", "detail": "", "fix_sql": None}
                for i, p in enumerate(self.prerequisites(), 1)]

    def read_catalog_remote(self, settings: dict[str, Any], password: str | None, schemas: list[str],
                            connection_id: str) -> CatalogSnapshot:
        """Read-only catalog from the hub (test sources only); NotImplementedError when only the agent can."""
        raise NotImplementedError


@dataclass
class ApplyStats:
    table: str
    inserts: int = 0
    updates: int = 0
    deletes: int = 0
    duration_ms: int = 0


class CheckpointConflictError(RuntimeError):
    """Another writer moved checkpoint(stream) between the read this batch was planned from and the commit (F-05).

    Raised by `DestinationConnector.apply` from INSIDE the destination transaction (compare-and-set on the
    checkpoint row / expected snapshot / fenced producer): the batch was not committed. Engines with per-table
    atomicity only (Delta, Iceberg, Databricks SQL, ClickHouse) may have committed idempotent table writes, never the
    checkpoint. The caller must not retry blindly: rebuild from `read_checkpoint()` (the other writer's batch is
    durable) and report the incident (two appliers for one stream = lock/lease problem)."""

    def __init__(self, stream: str, expected: str | None, found: str | None, detail: str = "") -> None:
        self.stream, self.expected, self.found = stream, expected, found
        super().__init__(f"checkpoint conflict on stream {stream!r}: expected {expected!r}, found {found!r}"
                         + (f" ({detail})" if detail else ""))


class WriterFenceError(RuntimeError):
    """A single-writer destination (ClickHouse: no transactions, no conditional INSERT) refused a batch because the
    caller does not prove that it holds the hub lock of the stream (DST-clickhouse-01). Nothing was written."""


# Writer fences: the hub registers every stream lock it holds (dialect.try_lock / unlock); destinations that cannot
# compare-and-set atomically require one before writing a stream. Per thread / task (ContextVar).
_FENCES: ContextVar[dict[str, Callable[[], bool]] | None] = ContextVar("alelt_writer_fences", default=None)


def hold_writer(key: str, alive: Callable[[], bool]) -> None:
    """Register a held lock: `key` = 'pipeline:<cid>' / 'replay:<cid>' (hub locks) or 'stream:<stream>' /
    'stream:*' (tools and tests that own the destination exclusively). `alive()` re-checks the lock."""
    d = dict(_FENCES.get() or {})
    d[key] = alive
    _FENCES.set(d)


def release_writer(key: str) -> None:
    d = dict(_FENCES.get() or {})
    d.pop(key, None)
    _FENCES.set(d)


def writer_fence(stream: str) -> Callable[[], bool] | None:
    """The fence that covers `stream`: the main stream `<cid>` needs `pipeline:<cid>`; replay / pass streams
    `<cid>#...` are written under `pipeline:<cid>` or `replay:<cid>`."""
    d = _FENCES.get() or {}
    cid = stream.split("#", 1)[0]
    keys = [f"stream:{stream}", "stream:*", f"pipeline:{cid}"] + ([f"replay:{cid}"] if "#" in stream else [])
    return next((d[k] for k in keys if k in d), None)


def require_writer(stream: str) -> None:
    """Raise WriterFenceError unless a live fence covers `stream`."""
    alive = writer_fence(stream)
    if alive is None:
        raise WriterFenceError(f"stream {stream!r}: no hub stream lock held by this writer (single-writer "
                               "destination: apply only under the hub pipeline/replay lock)")
    try:
        ok = bool(alive())
    except Exception:  # noqa: BLE001 - a failing check = lock not provably held
        ok = False
    if not ok:
        raise WriterFenceError(f"stream {stream!r}: the hub stream lock was lost (failover / session killed)")


class _Unset:
    def __repr__(self) -> str:
        return "UNSET"


UNSET: Any = _Unset()
"""Default of `apply(expected=...)`: compare-and-set against the checkpoint read at the start of apply()."""


class DestinationConnector(ABC):
    """Destination engine. apply() MUST be atomic with the checkpoint (R6) and MUST compare-and-set the checkpoint
    inside its own transaction (F-05): exactly-once never depends on the hub lock alone.

    Checkpoint model (ADR-005, stream + per-table watermarks): one checkpoint row per stream (`<cid>` main stream,
    `<cid>#catchup:<t>` / `#sched:<t>` / `#reprocess:<t>:<job>` replay streams) holding the stream position plus
    the per-table watermarks {dest_table: position} of that stream, all written in the SAME commit as the data.
    """

    engine: str

    @abstractmethod
    def bootstrap(self) -> None:
        """Create control schema/tables/stages; idempotent."""

    @abstractmethod
    def ensure_table(self, table: TableInfo, dest_name: str, golden_ddl: str | None) -> None: ...

    @abstractmethod
    def add_column(self, dest_name: str, col: ColumnInfo) -> None: ...

    @abstractmethod
    def read_checkpoint(self, stream: str) -> Position | None: ...

    def read_watermarks(self, stream: str) -> dict[str, Position]:
        """Per-table watermarks of `stream` ({dest_table: position}), committed atomically with the data."""
        raise NotImplementedError

    @abstractmethod
    def apply(self, stream: str, changesets: list[ChangeSet], to_pos: Position,
              batch_id: str, table_map: dict[TableRef, str],
              tables: dict[TableRef, TableInfo], *, expected: Position | None = UNSET,
              watermarks: Mapping[str, Position | str] | None = None) -> list[ApplyStats]:
        """Apply change sets + set checkpoint(stream)=to_pos + per-table watermarks + write audit row, in ONE
        transaction. Must be a no-op (return []) if checkpoint >= to_pos.

        Compare-and-set (F-05), evaluated inside the destination transaction: the checkpoint found there must equal
        `expected` (None = the stream has no checkpoint yet; UNSET = the value read at the start of apply()), else
        the batch is rejected with CheckpointConflictError - unless the checkpoint found is already >= to_pos (the
        same batch committed by another writer: no-op, []).
        `watermarks` {dest_table: position}: tables whose watermark this batch sets (default: every table of
        `table_map` -> to_pos). Watermarks only move forward and are merged into the stream's existing ones."""

    @abstractmethod
    def load_snapshot(self, dest_name: str, table: TableInfo, rows: Iterable[dict[str, Any]]) -> int: ...

    # ------------------------------------------------------------------ verification helpers (alelt_verify, F-16)
    # Common keyword arguments: schema (default landing schema), include_deleted (soft-deleted rows),
    # key_range (lo, hi) inclusive on the first key column, None = open end.
    def row_count(self, dest_name: str, include_deleted: bool = False, **kw: Any) -> int:  # pragma: no cover
        raise NotImplementedError

    def column_aggregates(self, dest_name: str, columns: list[str] | None = None, key_col: str | None = None,
                          key_range: tuple[Any, Any] | None = None, include_deleted: bool = False,
                          **kw: Any) -> dict[str, Any]:  # pragma: no cover
        """{'_rows': n, <col>: {count, min, max[, sum]}} over live rows (optionally a key range)."""
        raise NotImplementedError

    def sample_rows(self, dest_name: str, key_cols: list[str], columns: list[str] | None = None,
                    keys: list[tuple[Any, ...]] | None = None, **kw: Any) -> Any:  # pragma: no cover
        """keys given: {key tuple: row} for those keys; else the first `n` (kw, default 10) rows in key order
        (optionally `key_range`) as a list of rows."""
        raise NotImplementedError

    def key_set(self, dest_name: str, key_cols: list[str], key_range: tuple[Any, Any] | None = None,
                include_deleted: bool = False, **kw: Any) -> list[tuple[Any, ...]]:  # pragma: no cover
        """Sorted canonical key tuples of the (live) rows."""
        raise NotImplementedError

    def key_set_diff(self, dest_name: str, key_cols: list[str], reference: Iterable[tuple[Any, ...]],
                     key_range: tuple[Any, Any] | None = None, **kw: Any
                     ) -> dict[str, list[tuple[Any, ...]]]:  # pragma: no cover
        """{'missing': keys in `reference` absent here, 'extra': keys here absent from `reference`} (canonical)."""
        raise NotImplementedError

    def deleted_keys(self, dest_name: str, key_cols: list[str], **kw: Any
                     ) -> list[tuple[Any, ...]]:  # pragma: no cover
        raise NotImplementedError

    def key_bucket_hashes(self, dest_name: str, key_cols: list[str], buckets: int, **kw: Any
                          ) -> dict[int, str]:  # pragma: no cover
        raise NotImplementedError

    def column_checksums(self, dest_name: str, key_cols: list[str], columns: list[str], buckets: int,
                         **kw: Any) -> dict[str, dict[int, str]]:  # pragma: no cover
        raise NotImplementedError

    def describe_schema(self, tables: list[str] | None = None, schema: str | None = None
                        ) -> dict[str, list[dict[str, Any]]]:  # pragma: no cover
        """{table: [{name, type, nullable}]} as the destination reports it (landing schema by default)."""
        raise NotImplementedError

    # ------------------------------------------------------------------ quarantine / raw archive (R7, ADR-007)
    def quarantine(self, stream: str, records: Sequence[QuarantineRecord]) -> int:  # pragma: no cover
        """Persist quarantine records in the destination's control area (idempotent per record). Returns count."""
        raise NotImplementedError

    def archive_raw(self, connection_id: str, batch_id: str, body: bytes, header: dict[str, Any],
                    day: Any = None) -> str:  # pragma: no cover
        """Keep a raw edge batch (as shipped) + header; returns a reference. Idempotent per (connection, batch)."""
        raise NotImplementedError

    def publish_view(self, view_schema: str, table: str, source_schema: str,
                     columns: list[str]) -> None:  # pragma: no cover
        raise NotImplementedError
