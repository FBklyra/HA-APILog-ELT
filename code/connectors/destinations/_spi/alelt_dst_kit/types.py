"""Canonical -> physical type from config/type_maps/canonical_to_<engine>.yaml (built-in fallback per plugin).

YAML format (same as canonical_to_postgres.yaml): `types: {CANONICAL: physical with {p} {s} {n}}`, plus optional
`defaults: {STRING_without_length, DECIMAL_without_precision, STRING_over_max}` and `limits: {STRING_max_length,
DECIMAL_max_precision}`. ALELT_TYPE_MAP_DIR overrides the directory.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

from alelt_common.models import ColumnInfo

_DIR = Path(__file__).resolve().parents[4] / "config" / "type_maps"


@lru_cache(maxsize=32)
def _yaml(engine: str) -> dict[str, Any]:
    p = Path(os.environ.get("ALELT_TYPE_MAP_DIR") or _DIR) / f"canonical_to_{engine}.yaml"
    if not p.exists():
        return {}
    import yaml

    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


def type_map(engine: str, fallback: dict[str, Any]) -> dict[str, Any]:
    """{'types': {...}, 'defaults': {...}, 'limits': {...}} = fallback overlaid with the YAML file."""
    doc = _yaml(engine)
    return {k: {**(fallback.get(k) or {}), **(doc.get(k) or {})} for k in ("types", "defaults", "limits")}


def cap_decimal(p: int, s: int, max_p: int) -> tuple[int, int]:
    """DECIMAL(p, s) capped at max_p digits keeping the INTEGER digits (DST-kit-07 / DST-sql-02): the scale gives
    way first (fraction rounded by the engine); integer digits never shrink unless they alone exceed max_p."""
    s = max(0, min(s, p))
    if p <= max_p:
        return p, s
    ints = p - s
    return max_p, max(0, min(s, max_p - ints))


def physical_type(col: ColumnInfo, tmap: dict[str, Any], overrides: dict[str, str] | None = None) -> str:
    """Physical type for one column; UNKNOWN (or unmapped) raises - such columns are never created (R7)."""
    ct = col.canonical_type.value
    t = (overrides or {}).get(ct) or tmap["types"].get(ct)
    if not t:
        raise ValueError(f"column {col.name}: canonical type {ct} has no mapping")
    d, lim = tmap.get("defaults") or {}, tmap.get("limits") or {}
    if "{p}" in t:
        if not col.length:
            return str(d.get("DECIMAL_without_precision") or t.format(p=38, s=0))
        p, sc = cap_decimal(col.length, col.scale or 0, int(lim.get("DECIMAL_max_precision") or 38))
        return t.format(p=p, s=sc)
    if "{n}" in t:
        mx = int(lim.get("STRING_max_length") or 0)
        if not col.length:
            return str(d.get("STRING_without_length") or t.format(n=mx or 65535))
        # engines that size VARCHAR in BYTES (Redshift, Fabric UTF-8 varchar) while sources count characters:
        # n characters need up to n * STRING_bytes_per_char bytes (UTF-8: 4) - DST-redshift-03 / DST-fabric-04
        n = col.length * int(lim.get("STRING_bytes_per_char") or 1)
        if mx and n > mx:
            return str(d.get("STRING_over_max") or d.get("STRING_without_length") or t.format(n=mx))
        return t.format(n=n)
    return str(t)
