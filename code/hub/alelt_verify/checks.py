"""Pure comparators: every mismatch becomes a located ReconResult (table, column, key, bucket)."""
from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from typing import Any

from .models import CheckName, ReconResult, Status

Factory = Callable[..., ReconResult]


def _key_text(key: Sequence[Any]) -> str:
    return json.dumps([str(k) for k in key])


def _norm(v: Any) -> Any:
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    if isinstance(v, Decimal):
        return v.normalize()
    if isinstance(v, str):
        return v.rstrip(" ")            # CHAR padding is not a difference
    return v


def row_count(mk: Factory, counts: Mapping[str, int]) -> list[ReconResult]:
    """counts per side, e.g. {'source': n, 'candidate': n, 'reference': n}; all must be equal."""
    vals = set(counts.values())
    status = Status.PASS if len(vals) <= 1 else Status.FAIL
    sides = sorted(counts)
    return [mk(CheckName.ROW_COUNT, status, expected=str(counts[sides[0]]), actual=json.dumps(dict(sorted(counts.items()))),
               detail={"counts": dict(sorted(counts.items()))})]


def buckets(mk: Factory, check: CheckName, expected: Mapping[int, str], actual: Mapping[int, str],
            column: str | None = None) -> list[ReconResult]:
    """Bucket hash maps (KEY_SET or COLUMN_CHECKSUM); one FAIL per differing bucket, else one PASS."""
    bad = sorted(b for b in expected.keys() | actual.keys() if expected.get(b) != actual.get(b))
    if not bad:
        return [mk(check, Status.PASS, column=column, detail={"buckets": len(expected)})]
    return [mk(check, Status.FAIL, column=column, bucket=b, expected=expected.get(b), actual=actual.get(b)) for b in bad]


def column_checksums(mk: Factory, expected: Mapping[str, Mapping[int, str]],
                     actual: Mapping[str, Mapping[int, str]]) -> list[ReconResult]:
    out: list[ReconResult] = []
    for col in sorted(expected.keys() | actual.keys()):
        out += buckets(mk, CheckName.COLUMN_CHECKSUM, expected.get(col, {}), actual.get(col, {}), column=col)
    return out


def sampled_rows(mk: Factory, expected: Mapping[tuple[Any, ...], Mapping[str, Any]],
                 actual: Mapping[tuple[Any, ...], Mapping[str, Any]], columns: Sequence[str]) -> list[ReconResult]:
    """Column-by-column comparison of sampled keys (source vs destination)."""
    out: list[ReconResult] = []
    for key in sorted(expected, key=_key_text):
        got = actual.get(key)
        if got is None:
            out.append(mk(CheckName.SAMPLED_ROWS, Status.FAIL, key=_key_text(key), expected="row", actual="missing"))
            continue
        for c in columns:
            if _norm(expected[key].get(c)) != _norm(got.get(c)):
                out.append(mk(CheckName.SAMPLED_ROWS, Status.FAIL, key=_key_text(key), column=c,
                              expected=str(expected[key].get(c)), actual=str(got.get(c))))
    extra = sorted((k for k in actual if k not in expected), key=_key_text)
    out += [mk(CheckName.SAMPLED_ROWS, Status.FAIL, key=_key_text(k), expected="missing", actual="row") for k in extra]
    return out or [mk(CheckName.SAMPLED_ROWS, Status.PASS, detail={"keys": len(expected)})]


def deletes(mk: Factory, expected: Sequence[tuple[Any, ...]], actual: Sequence[tuple[Any, ...]]) -> list[ReconResult]:
    e, a = set(map(tuple, expected)), set(map(tuple, actual))
    out = [mk(CheckName.DELETES, Status.FAIL, key=_key_text(k), expected="deleted", actual="live")
           for k in sorted(e - a, key=_key_text)]
    out += [mk(CheckName.DELETES, Status.FAIL, key=_key_text(k), expected="live", actual="deleted")
            for k in sorted(a - e, key=_key_text)]
    return out or [mk(CheckName.DELETES, Status.PASS, detail={"deleted": len(e)})]


def percentiles(values: Sequence[float], ps: Sequence[float] = (50, 95, 99)) -> dict[str, float]:
    """Nearest-rank percentiles."""
    if not values:
        return {}
    s = sorted(values)
    return {f"p{int(p)}": s[max(0, math.ceil(p / 100 * len(s)) - 1)] for p in ps}


def latency(mk: Factory, pairs: Sequence[tuple[datetime, datetime]], p95_target_s: float) -> list[ReconResult]:
    """pairs = (source commit_ts, destination synced_ts)."""
    lat = [(synced - commit).total_seconds() for commit, synced in pairs]
    if not lat:
        return [mk(CheckName.LATENCY, Status.ERROR, detail={"reason": "no samples"})]
    pc = percentiles(lat)
    status = Status.PASS if pc["p95"] <= p95_target_s else Status.FAIL
    return [mk(CheckName.LATENCY, status, expected=f"p95<={p95_target_s}", actual=str(pc["p95"]),
               detail={**pc, "samples": len(lat)})]


def dbt_shadow(mk: Factory, model_diffs: Mapping[str, int]) -> list[ReconResult]:
    """model -> differing row count from dbt audit_helper compare (0 = identical)."""
    out = [mk(CheckName.DBT_SHADOW, Status.FAIL, column=m, expected="0", actual=str(n))
           for m, n in sorted(model_diffs.items()) if n]
    return out or [mk(CheckName.DBT_SHADOW, Status.PASS, detail={"models": len(model_diffs)})]
