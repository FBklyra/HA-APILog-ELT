"""Canonical -> physical type maps per dialect, from `config/type_maps/canonical_to_<dialect>.yaml`.

File format (same as canonical_to_postgres.yaml / canonical_to_snowflake.yaml):
  types:    {CANONICAL: physical with {p} {s} {n} placeholders | null}
  defaults: {STRING_without_length, STRING_over_max, BINARY_without_length, BINARY_over_max,
             DECIMAL_without_precision, DECIMAL_over_max}
  limits:   {max_varchar, max_varbinary, max_decimal_precision}
`ALELT_SQL_TYPE_MAP_DIR` points at another directory (same file names). UNKNOWN has no mapping (R7: never created).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from alelt_common.models import ColumnInfo

CONFIG_DIR = Path(__file__).resolve().parents[4] / "config" / "type_maps"


@dataclass(frozen=True)
class TypeMap:
    dialect: str
    types: dict[str, str | None]
    defaults: dict[str, str] = field(default_factory=dict)
    limits: dict[str, int] = field(default_factory=dict)

    def physical(self, c: ColumnInfo) -> str:
        """Physical type of one column; raises for UNKNOWN / unmapped (R7)."""
        ct = c.canonical_type.value
        t = self.types.get(ct)
        if t is None:
            raise ValueError(f"column {c.name}: canonical type {ct} has no {self.dialect} mapping")
        if "{p}" in t:
            p = c.length
            if not p:
                return self.defaults.get("DECIMAL_without_precision", t.replace("({p},{s})", ""))
            mx = self.limits.get("max_decimal_precision")
            if mx and p > mx:
                over = self.defaults.get("DECIMAL_over_max")
                if over and "(" not in over:          # an unbounded type (Oracle NUMBER): keeps every digit
                    return over
                from alelt_dst_kit.types import cap_decimal

                pp, ss = cap_decimal(p, c.scale or 0, mx)   # keep integer digits; scale gives way (DST-sql-02)
                return t.format(p=pp, s=ss)
            return t.format(p=p, s=c.scale or 0)
        if "{n}" in t:
            kind = "BINARY" if ct == "BINARY" else "STRING"
            mx = self.limits.get("max_varbinary" if kind == "BINARY" else "max_varchar")
            if not c.length:
                return self.defaults.get(f"{kind}_without_length", t.replace("({n})", ""))
            if mx and c.length > mx:
                return self.defaults.get(f"{kind}_over_max", self.defaults.get(f"{kind}_without_length", t))
            return t.format(n=c.length)
        return t


def _read(path: Path) -> dict[str, Any]:
    import yaml

    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


@lru_cache(maxsize=32)
def load_type_map(dialect: str, builtin_limits: tuple[tuple[str, int], ...] = ()) -> TypeMap:
    d = Path(os.environ.get("ALELT_SQL_TYPE_MAP_DIR") or CONFIG_DIR)
    p = d / f"canonical_to_{dialect}.yaml"
    if not p.exists():
        p = CONFIG_DIR / f"canonical_to_{dialect}.yaml"
    doc = _read(p)
    limits = {**dict(builtin_limits), **(doc.get("limits") or {})}
    return TypeMap(dialect, dict(doc.get("types") or {}), dict(doc.get("defaults") or {}), limits)
