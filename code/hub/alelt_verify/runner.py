"""Verifier: runs checks between two sides and records evidence. Sides are thin adapters over SPI helpers."""
from __future__ import annotations

import uuid
from collections.abc import Callable, Sequence
from datetime import datetime
from functools import partial
from typing import Any, Protocol

from alelt_common.util import utcnow

from . import checks
from .models import CheckName, ReconResult, ReconStore, Status


class VerifySide(Protocol):
    """One side of a comparison (ALELT landing, existing landing schema of the legacy pipeline, or source probe)."""

    def row_count(self, table: str) -> int: ...
    def key_buckets(self, table: str, keys: list[str], buckets: int) -> dict[int, str]: ...
    def column_checksums(self, table: str, keys: list[str], columns: list[str], buckets: int) -> dict[str, dict[int, str]]: ...
    def deleted_keys(self, table: str, keys: list[str]) -> list[tuple[Any, ...]]: ...
    def sample_rows(self, table: str, keys: list[str], columns: list[str],
                    key_values: list[tuple[Any, ...]]) -> dict[tuple[Any, ...], dict[str, Any]]: ...


class DestinationSide:
    """Adapter over a destination's verify helpers, bound to a schema + output profile."""

    def __init__(self, dest: Any, schema: str | None = None, profile: Any = None,
                 synced_before: datetime | None = None) -> None:
        self.dest, self.kw = dest, {"schema": schema, "profile": profile}
        self.synced_before = synced_before

    def row_count(self, table: str) -> int:
        return int(self.dest.row_count(table, False, synced_before=self.synced_before, **self.kw))

    def key_buckets(self, table: str, keys: list[str], buckets: int) -> dict[int, str]:
        return dict(self.dest.key_bucket_hashes(table, keys, buckets, synced_before=self.synced_before, **self.kw))

    def column_checksums(self, table: str, keys: list[str], columns: list[str], buckets: int) -> dict[str, dict[int, str]]:
        return dict(self.dest.column_checksums(table, keys, columns, buckets, synced_before=self.synced_before, **self.kw))

    def deleted_keys(self, table: str, keys: list[str]) -> list[tuple[Any, ...]]:
        return list(self.dest.deleted_keys(table, keys, **self.kw))

    def sample_rows(self, table: str, keys: list[str], columns: list[str],
                    key_values: list[tuple[Any, ...]]) -> dict[tuple[Any, ...], dict[str, Any]]:
        return dict(self.dest.sample_rows(table, keys, columns, key_values, schema=self.kw["schema"]))


class Verifier:
    """`reference` is the trusted side (legacy landing schema or source); `candidate` is ALELT."""

    def __init__(self, reference: VerifySide, candidate: VerifySide, store: ReconStore,
                 clock: Callable[[], datetime] = utcnow, pos_window: tuple[str | None, str | None] = (None, None)) -> None:
        self.ref, self.cand, self.store, self.clock = reference, candidate, store, clock
        self.pos_window = pos_window

    def _mk(self, run_id: str, table: str, check: CheckName, status: Status, **kw: Any) -> ReconResult:
        return ReconResult(run_id=run_id, table=table, check=check, status=status, recorded_at=self.clock(),
                           pos_from=self.pos_window[0], pos_to=self.pos_window[1], **kw)

    def _run(self, table: str, fn: Callable[[checks.Factory], list[ReconResult]], check: CheckName) -> list[ReconResult]:
        run_id = uuid.uuid4().hex
        mk = partial(self._mk, run_id, table)
        try:
            res = fn(mk)
        except Exception as e:  # evidence of a failed run is evidence too
            res = [mk(check, Status.ERROR, detail={"error": f"{type(e).__name__}: {e}"})]
        self.store.add(res)
        return res

    def row_count(self, table: str) -> list[ReconResult]:
        return self._run(table, lambda mk: checks.row_count(
            mk, {"reference": self.ref.row_count(table), "candidate": self.cand.row_count(table)}), CheckName.ROW_COUNT)

    def key_set(self, table: str, keys: list[str], buckets: int = 1024) -> list[ReconResult]:
        return self._run(table, lambda mk: checks.buckets(
            mk, CheckName.KEY_SET, self.ref.key_buckets(table, keys, buckets),
            self.cand.key_buckets(table, keys, buckets)), CheckName.KEY_SET)

    def column_checksums(self, table: str, keys: list[str], columns: list[str], buckets: int = 1024) -> list[ReconResult]:
        return self._run(table, lambda mk: checks.column_checksums(
            mk, self.ref.column_checksums(table, keys, columns, buckets),
            self.cand.column_checksums(table, keys, columns, buckets)), CheckName.COLUMN_CHECKSUM)

    def deletes(self, table: str, keys: list[str]) -> list[ReconResult]:
        return self._run(table, lambda mk: checks.deletes(
            mk, self.ref.deleted_keys(table, keys), self.cand.deleted_keys(table, keys)), CheckName.DELETES)

    def sampled_rows(self, table: str, keys: list[str], columns: list[str],
                     key_values: list[tuple[Any, ...]]) -> list[ReconResult]:
        return self._run(table, lambda mk: checks.sampled_rows(
            mk, self.ref.sample_rows(table, keys, columns, key_values),
            self.cand.sample_rows(table, keys, columns, key_values), columns), CheckName.SAMPLED_ROWS)

    def default_suite(self, table: str, ref_rows: list[dict[str, Any]], cand_rows: list[dict[str, Any]],
                      keys: list[str], columns: list[str], hash_buckets: int = 0, **kw: Any) -> Any:
        """Default reconciliation (alelt_verify.defaults): counts + aggregates per key range + sampled rows, key-set diff
        on mismatch; hash buckets only when `hash_buckets` > 0. Evidence stored; returns a SuiteResult."""
        from .defaults import SuiteResult, run_default_suite

        run_id = uuid.uuid4().hex
        mk = partial(self._mk, run_id, table)
        try:
            out = run_default_suite(mk, ref_rows, cand_rows, keys, columns, hash_buckets=hash_buckets, **kw)
        except Exception as e:  # noqa: BLE001 - evidence of a failed run is evidence too
            out = SuiteResult(Status.ERROR, [mk(CheckName.ROW_COUNT, Status.ERROR,
                                                detail={"error": f"{type(e).__name__}: {e}"})])
        self.store.add(out.results)
        return out

    def latency(self, table: str, pairs: Sequence[tuple[datetime, datetime]], p95_target_s: float) -> list[ReconResult]:
        return self._run(table, lambda mk: checks.latency(mk, pairs, p95_target_s), CheckName.LATENCY)

    def dbt_shadow(self, table: str, model_diffs: dict[str, int]) -> list[ReconResult]:
        return self._run(table, lambda mk: checks.dbt_shadow(mk, model_diffs), CheckName.DBT_SHADOW)
