"""Reference-landing parity (X-124): destination-side compare of two schemas in the SAME destination - the existing
landing of the pipeline being replaced (reference) vs this platform's landing (candidate). Vendor-neutral: the
mapping (system columns, renamed columns, columns that cannot compare, settled window) is data; vendor-specific
mappings come from extensions (`config/extensions.yaml: reference_mappings: {profile: 'module:function'}`).

Settled-row window: rows whose synced timestamp on EITHER side is at/after `settled_before` are still moving and are
excluded from both sides by key (a lagging legacy sync must not count as a mismatch). Deleted rows (soft-delete column
of each side) are excluded. Then the default suite runs: counts + aggregates per key range + sampled rows, keyed diff
on mismatch (alelt_verify.defaults).
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from . import checks
from .defaults import SuiteResult, canon, run_default_suite


@dataclass
class ReferenceMapping:
    """How a reference landing table maps onto the candidate landing table (column names upper case)."""

    name: str = "generic"
    exclude: set[str] = field(default_factory=set)         # system / non-comparable columns, either side
    rename: dict[str, str] = field(default_factory=dict)   # reference column -> candidate column
    ref_deleted: str | None = None
    cand_deleted: str | None = None
    ref_synced: str | None = None
    cand_synced: str | None = None
    key_override: list[str] | None = None                  # e.g. a surrogate id column for keyless tables


def mapping_from_profiles(ref_profile: Any = None, cand_profile: Any = None, name: str = "profiles") -> ReferenceMapping:
    """Generic mapping from the two output profiles' system columns (synced / deleted / id are never compared)."""
    def cols(p: Any) -> tuple[str | None, str | None, str | None]:
        if p is None:
            return None, None, None
        g = (lambda k: (getattr(p, k, None) or (p.get(k) if isinstance(p, Mapping) else None)))
        return tuple((str(v).upper() if v else None) for v in (g("synced_column"), g("deleted_column"),
                                                               g("id_column")))  # type: ignore[return-value]
    rs, rd, ri = cols(ref_profile)
    cs, cd, ci = cols(cand_profile)
    return ReferenceMapping(name=name, exclude={c for c in (rs, rd, ri, cs, cd, ci) if c}, ref_deleted=rd,
                            cand_deleted=cd, ref_synced=rs, cand_synced=cs)


def _is_true(v: Any) -> bool:
    return v in (True, 1, "true", "TRUE", "t", "T", "1")


def _ts(v: Any) -> datetime | None:
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, str):
        try:
            d = datetime.fromisoformat(v)
        except ValueError:
            return None
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    return None


def _norm(rows: Iterable[Mapping[str, Any]], rename: Mapping[str, str] | None = None) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        u = {str(k).upper(): v for k, v in r.items()}
        if rename:
            u = {rename.get(k, k): v for k, v in u.items()}
        out.append(u)
    return out


def prepare(ref_rows: Iterable[Mapping[str, Any]], cand_rows: Iterable[Mapping[str, Any]], keys: Sequence[str],
            m: ReferenceMapping, settled_before: datetime | None) -> tuple[list[dict[str, Any]], list[dict[str, Any]],
                                                                          dict[str, int]]:
    """Normalise both sides; drop deleted rows and (by key, both sides) rows not settled yet."""
    rename = {k.upper(): v.upper() for k, v in m.rename.items()}
    ref = _norm(ref_rows, rename)
    cand = _norm(cand_rows)
    ks = [k.upper() for k in (m.key_override or keys)]
    cut = settled_before if settled_before is None or settled_before.tzinfo else settled_before.replace(tzinfo=timezone.utc)
    moving: set[tuple[Any, ...]] = set()
    if cut is not None and ks:
        for rows, col in ((ref, m.ref_synced), (cand, m.cand_synced)):
            if col:
                for r in rows:
                    t = _ts(r.get(col))
                    if t is not None and t >= cut:
                        moving.add(tuple(canon(r.get(k)) for k in ks))
    stats = {"unsettled_keys": len(moving), "ref_deleted": 0, "cand_deleted": 0}

    def keep(rows: list[dict[str, Any]], deleted: str | None, tag: str) -> list[dict[str, Any]]:
        out = []
        for r in rows:
            if deleted and _is_true(r.get(deleted)):
                stats[tag] += 1
                continue
            if moving and tuple(canon(r.get(k)) for k in ks) in moving:
                continue
            out.append(r)
        return out
    return keep(ref, m.ref_deleted, "ref_deleted"), keep(cand, m.cand_deleted, "cand_deleted"), stats


def compared_columns(ref_rows: Sequence[Mapping[str, Any]], cand_rows: Sequence[Mapping[str, Any]],
                     columns: Sequence[str] | None, m: ReferenceMapping) -> list[str]:
    """Columns present on both sides minus the mapping's exclusions (or the given list minus exclusions)."""
    if columns is not None:
        return [c.upper() for c in columns if c.upper() not in m.exclude]
    rc = set(ref_rows[0]) if ref_rows else set()
    cc = set(cand_rows[0]) if cand_rows else set()
    return sorted((rc & cc) - m.exclude) if rc and cc else sorted((rc | cc) - m.exclude)


def run_reference_suite(mk: checks.Factory, ref_rows: Iterable[Mapping[str, Any]],
                        cand_rows: Iterable[Mapping[str, Any]], keys: Sequence[str], m: ReferenceMapping,
                        columns: Sequence[str] | None = None, settled_before: datetime | None = None,
                        **kw: Any) -> SuiteResult:
    ref, cand, stats = prepare(ref_rows, cand_rows, keys, m, settled_before)
    cols = compared_columns(ref, cand, columns, m)
    ks = [k.upper() for k in (m.key_override or keys)]
    out = run_default_suite(mk, ref, cand, ks, cols, **kw)
    out.summary.update({"mapping": m.name, "columns": len(cols), "excluded": sorted(m.exclude),
                        "settled_before": settled_before.isoformat() if settled_before else None, **stats})
    return out


RowsFn = Callable[[str, str], list[dict[str, Any]]]


def schema_rows(dest: Any) -> RowsFn:
    """(schema, table) -> rows through the destination's schema-qualified row access (destination SPI verify helper).
    Accepts `fetch_rows(table, schema=...)` or `fetch_rows_in(schema, table)`."""
    def rows(schema: str, table: str) -> list[dict[str, Any]]:
        f = getattr(dest, "fetch_rows_in", None)
        if callable(f):
            return list(f(schema, table))
        f = getattr(dest, "fetch_rows", None)
        if callable(f):
            try:
                return list(f(table, schema=schema))
            except TypeError:
                pass
        raise RuntimeError(f"destination {type(dest).__name__} has no schema-qualified row access "
                           "(fetch_rows(table, schema=) / fetch_rows_in(schema, table))")
    return rows
