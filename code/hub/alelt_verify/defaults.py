"""Default reconciliation suite (cheap checks first, exact detail only where something differs).

1. ROW_COUNT          live rows on both sides;
2. COLUMN_AGGREGATES  per key range: rows, and per column non-null count, min, max (canonical text) and exact sum
                      (integers / decimals);
3. SAMPLED_ROWS       evenly spread keys compared column by column;
4. KEY_SET            on any mismatch above: exact key diff (missing / extra keys) inside the ranges that differ;
5. hash buckets       KEY_SET + COLUMN_CHECKSUM bucket hashes - optional, OFF by default (`hash_buckets=0`;
                      ALELT_VERIFY_HASH_BUCKETS=n to enable from the worker).

Sides only need `rows(table) -> list[dict]` (live rows, deleted excluded). Column names are matched
case-insensitively; values are compared in a canonical text form (CHAR padding, Decimal scale, tz-aware vs UTC naive,
bytes as hex), so a source and a destination of different engines compare equal when the data is equal.
"""
from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, Protocol

from . import checks
from .models import CheckName, ReconResult, Status

RowsFn = Callable[[str], list[dict[str, Any]]]


class RowsSide(Protocol):
    def rows(self, table: str) -> list[dict[str, Any]]: ...


@dataclass
class ListSide:
    """Side over a callable (e.g. source snapshot_rows, destination fetch_rows); `deleted` = soft-delete column."""

    fetch: RowsFn
    deleted: str | None = None

    def rows(self, table: str) -> list[dict[str, Any]]:
        out = []
        dl = self.deleted.upper() if self.deleted else None
        for r in self.fetch(table):
            u = {str(k).upper(): v for k, v in r.items()}
            if dl and u.get(dl) in (True, 1, "true", "TRUE"):
                continue
            out.append(u)
        return out


def canon(v: Any) -> str | None:
    """Engine-neutral text form used for every comparison."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, Decimal):
        if not v.is_finite():
            return str(v)
        s = format(v.normalize(), "f")
        return s if s not in ("-0",) else "0"
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return str(v)
        if v == int(v) and abs(v) < 1e15:
            return str(int(v))
        return format(v, ".15g")    # HC-15: DBL_DIG - any 15-digit decimal round-trips; 13th-digit drift is seen
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    if isinstance(v, datetime):
        if v.tzinfo is not None:
            v = v.astimezone(timezone.utc).replace(tzinfo=None)
        return v.isoformat(sep=" ")
    if isinstance(v, (date, time)):
        return v.isoformat()
    if isinstance(v, str):
        return v.rstrip(" ")
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True, separators=(",", ":"), default=str)
    return str(v)


def _num(v: Any) -> Decimal | None:
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, int):
        return Decimal(v)
    if isinstance(v, Decimal) and v.is_finite():
        return v
    return None


def aggregate(rows: Iterable[Mapping[str, Any]], columns: Sequence[str]) -> dict[str, Any]:
    """{'_rows': n, COL: {count, min, max, sum?}} - min/max over canonical text, exact sum for int/decimal."""
    out: dict[str, Any] = {"_rows": 0}
    acc = {c: {"count": 0, "min": None, "max": None, "sum": Decimal(0), "numeric": True} for c in columns}
    for r in rows:
        out["_rows"] += 1
        for c in columns:
            v = r.get(c)
            if v is None:
                continue
            a = acc[c]
            a["count"] += 1
            t = canon(v)
            a["min"] = t if a["min"] is None or t < a["min"] else a["min"]
            a["max"] = t if a["max"] is None or t > a["max"] else a["max"]
            n = _num(v)
            if n is None:
                a["numeric"] = False
            elif a["numeric"]:
                a["sum"] += n
    for c, a in acc.items():
        d = {"count": a["count"], "min": a["min"], "max": a["max"]}
        if a["numeric"] and a["count"]:
            d["sum"] = canon(a["sum"])
        out[c] = d
    return out


def _key(r: Mapping[str, Any], keys: Sequence[str]) -> tuple[str | None, ...]:
    return tuple(canon(r.get(k)) for k in keys)


def _sort_key(k: tuple[str | None, ...]) -> tuple[tuple[int, Any], ...]:
    """Numeric-aware ordering of canonical key tuples (ranges are cut on the reference order)."""
    out = []
    for x in k:
        if x is None:
            out.append((0, ""))
            continue
        try:
            out.append((1, Decimal(x)))
        except ArithmeticError:
            out.append((2, x))
        except ValueError:
            out.append((2, x))
    return tuple(out)


@dataclass
class SuiteResult:
    status: Status
    results: list[ReconResult] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def outcome(self) -> str:
        return {Status.PASS: "MATCH", Status.FAIL: "MISMATCH"}.get(self.status, "ERROR")


def run_default_suite(mk: checks.Factory, ref_rows: list[dict[str, Any]], cand_rows: list[dict[str, Any]],
                      keys: Sequence[str], columns: Sequence[str], range_rows: int = 10_000, sample: int = 50,
                      hash_buckets: int = 0, max_key_diff: int = 100, key_diff: str = "on_mismatch") -> SuiteResult:
    """Compare reference (source / legacy landing) with candidate (our landing). Keys/columns upper-case."""
    keys = [k.upper() for k in keys]
    columns = [c.upper() for c in columns]
    res: list[ReconResult] = []
    res += checks.row_count(mk, {"reference": len(ref_rows), "candidate": len(cand_rows)})
    eff_keys = keys or columns                       # keyless: the full row is the identity
    if not keys:
        res += row_multiset(mk, ref_rows, cand_rows, columns, max_key_diff)
    ref_by = {_key(r, eff_keys): r for r in ref_rows}
    cand_by = {_key(r, eff_keys): r for r in cand_rows}
    order = sorted(ref_by, key=_sort_key)
    n_ranges = max(1, math.ceil(len(order) / max(1, range_rows)))
    bounds = [order[min(len(order) - 1, i * range_rows)] for i in range(n_ranges)] if order else []

    def range_of(k: tuple[str | None, ...]) -> int:
        sk = _sort_key(k)
        lo, hi = 0, len(bounds) - 1
        idx = 0
        while lo <= hi:                               # last bound <= k
            mid = (lo + hi) // 2
            if _sort_key(bounds[mid]) <= sk:
                idx, lo = mid, mid + 1
            else:
                hi = mid - 1
        return idx

    ref_parts: dict[int, list[dict[str, Any]]] = {i: [] for i in range(n_ranges)}
    cand_parts: dict[int, list[dict[str, Any]]] = {i: [] for i in range(n_ranges)}
    for k, r in ref_by.items():
        ref_parts[range_of(k)].append(r)
    for k, r in cand_by.items():
        cand_parts[range_of(k) if bounds else 0].append(r)
    bad_ranges: list[int] = []
    for i in range(n_ranges):
        ea, aa = aggregate(ref_parts[i], columns), aggregate(cand_parts[i], columns)
        lo = list(bounds[i]) if bounds else None
        hi = list(bounds[i + 1]) if bounds and i + 1 < len(bounds) else None
        diff = sorted(c for c in ["_rows", *columns] if ea.get(c) != aa.get(c))
        if diff:
            bad_ranges.append(i)
            for c in diff:
                res.append(mk(CheckName.COLUMN_AGGREGATES, Status.FAIL, column=None if c == "_rows" else c, bucket=i,
                              expected=json.dumps(ea.get(c), default=str), actual=json.dumps(aa.get(c), default=str),
                              detail={"range": {"from": lo, "to_exclusive": hi}}))
    if not bad_ranges:
        res.append(mk(CheckName.COLUMN_AGGREGATES, Status.PASS, detail={"ranges": n_ranges, "columns": len(columns)}))

    if order and sample > 0:
        step = max(1, len(order) // sample)
        picked = order[::step][:sample]
        if order[-1] not in picked:
            picked.append(order[-1])
        exp = {k: {c: canon(ref_by[k].get(c)) for c in columns} for k in picked}
        got = {k: {c: canon(cand_by[k].get(c)) for c in columns} for k in picked if k in cand_by}
        res += checks.sampled_rows(mk, exp, got, columns)

    mismatch = any(r.status is not Status.PASS for r in res)
    if mismatch or key_diff == "always":
        scope = set(bad_ranges) if bad_ranges else set(range(n_ranges))
        missing = sorted((k for k in ref_by if k not in cand_by and range_of(k) in scope), key=_sort_key)
        extra = sorted((k for k in cand_by if k not in ref_by and (not bounds or range_of(k) in scope)), key=_sort_key)
        for k in missing[:max_key_diff]:
            res.append(mk(CheckName.KEY_SET, Status.FAIL, key=json.dumps(list(k)), expected="row", actual="missing"))
        for k in extra[:max_key_diff]:
            res.append(mk(CheckName.KEY_SET, Status.FAIL, key=json.dumps(list(k)), expected="missing", actual="row"))
        if not missing and not extra:
            res.append(mk(CheckName.KEY_SET, Status.PASS, detail={"scope_ranges": len(scope)}))
        else:
            res.append(mk(CheckName.KEY_SET, Status.FAIL, detail={"missing": len(missing), "extra": len(extra),
                                                                  "shown": min(len(missing), max_key_diff)
                                                                  + min(len(extra), max_key_diff)}))
    if hash_buckets > 0:
        res += checks.buckets(mk, CheckName.KEY_SET, bucket_hashes(ref_by, hash_buckets),
                              bucket_hashes(cand_by, hash_buckets))
        res += checks.column_checksums(mk, column_bucket_hashes(ref_by, columns, hash_buckets),
                                       column_bucket_hashes(cand_by, columns, hash_buckets))
    status = Status.FAIL if any(r.status is Status.FAIL for r in res) else (
        Status.ERROR if any(r.status is Status.ERROR for r in res) else Status.PASS)
    fails = [r for r in res if r.status is Status.FAIL]
    return SuiteResult(status, res, {"reference_rows": len(ref_rows), "candidate_rows": len(cand_rows),
                                     "ranges": n_ranges, "bad_ranges": len(bad_ranges), "failures": len(fails),
                                     "checks": sorted({r.check.value for r in fails})})


def row_multiset(mk: checks.Factory, ref_rows: Iterable[Mapping[str, Any]], cand_rows: Iterable[Mapping[str, Any]],
                 columns: Sequence[str], max_diff: int = 100) -> list[ReconResult]:
    """F-10: keyless tables compare as a multiset (count per identical canonical row). The key-set view above collapses
    identical rows (as does the surrogate id), so [X, X, Y] vs [X, Y, Y] would otherwise MATCH."""
    from collections import Counter

    ref = Counter(_key(r, columns) for r in ref_rows)
    cand = Counter(_key(r, columns) for r in cand_rows)
    diff = sorted((k for k in ref.keys() | cand.keys() if ref[k] != cand[k]), key=_sort_key)
    if not diff:
        return [mk(CheckName.ROW_MULTISET, Status.PASS, detail={"distinct_rows": len(ref),
                                                               "duplicated_rows": sum(1 for v in ref.values() if v > 1)})]
    out = [mk(CheckName.ROW_MULTISET, Status.FAIL, key=json.dumps(list(k)), expected=str(ref[k]), actual=str(cand[k]))
           for k in diff[:max_diff]]
    out.append(mk(CheckName.ROW_MULTISET, Status.FAIL, detail={"differing_rows": len(diff)}))
    return out


def _bucket(k: tuple[str | None, ...], n: int) -> int:
    return int(hashlib.sha256(json.dumps(list(k)).encode()).hexdigest()[:8], 16) % n


def bucket_hashes(by_key: Mapping[tuple[str | None, ...], Any], n: int) -> dict[int, str]:
    acc: dict[int, list[str]] = {}
    for k in by_key:
        acc.setdefault(_bucket(k, n), []).append(json.dumps(list(k)))
    return {b: f"{len(v)}:" + hashlib.sha256("\n".join(sorted(v)).encode()).hexdigest()[:16] for b, v in acc.items()}


def column_bucket_hashes(by_key: Mapping[tuple[str | None, ...], Mapping[str, Any]], columns: Sequence[str],
                         n: int) -> dict[str, dict[int, str]]:
    out: dict[str, dict[int, str]] = {}
    for c in columns:
        acc: dict[int, list[str]] = {}
        for k, r in by_key.items():
            acc.setdefault(_bucket(k, n), []).append(json.dumps([list(k), canon(r.get(c))]))
        out[c] = {b: hashlib.sha256("\n".join(sorted(v)).encode()).hexdigest()[:16] for b, v in acc.items()}
    return out
