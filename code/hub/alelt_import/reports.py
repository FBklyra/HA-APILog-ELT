"""type-map, system-columns, diff and golden-coverage reports (pure, deterministic)."""
from __future__ import annotations

import csv
import io
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from alelt_common.models import CatalogSnapshot

from .ddl import GoldenColumn


def base_type(t: str) -> str:
    return re.sub(r"\(.*\)$", "", t.strip().upper())


def type_map(catalog: CatalogSnapshot, golden: Iterable[GoldenColumn],
             dest_names: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Source engine type -> destination type as observed in golden DDL. Columns without a golden match => UNRESOLVED.
    dest_names maps source fqn -> destination table name (default: source table name)."""
    by_key = {(g.table.upper(), g.column.upper()): g for g in golden}
    observed: dict[str, Counter[str]] = {}
    columns: dict[str, dict[str, str]] = {}
    unresolved: list[str] = []
    for t in sorted(catalog.tables, key=lambda t: t.table.fqn):
        dest = (dest_names or {}).get(t.table.fqn, t.table.name).upper()
        for c in sorted(t.columns, key=lambda c: c.ordinal):
            g = by_key.get((dest, c.name.upper()))
            key = f"{dest}.{c.name}"
            if g is None:
                unresolved.append(key)
                continue
            observed.setdefault(base_type(c.engine_type), Counter())[g.data_type] += 1
            columns[key] = {"source_type": c.engine_type, "destination_type": g.data_type}
    mappings = {st: sorted(cnt.items(), key=lambda kv: (-kv[1], kv[0]))[0][0] for st, cnt in sorted(observed.items())}
    ambiguous = {st: dict(sorted(cnt.items())) for st, cnt in sorted(observed.items()) if len(cnt) > 1}
    return {"engine": catalog.engine, "mappings": mappings, "ambiguous": ambiguous,
            "columns": dict(sorted(columns.items())), "UNRESOLVED": unresolved}


def system_columns(golden: Iterable[GoldenColumn], names: Iterable[str] = (), prefixes: Iterable[str] = ()) -> str:
    """CSV of system columns: exact `names` (e.g. from an output profile) or any of `prefixes`."""
    want = {n.upper() for n in names}
    pre = tuple(p.upper() for p in prefixes)
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(("table", "column", "data_type"))
    for g in sorted(golden, key=lambda g: (g.table, g.ordinal)):
        u = g.column.upper()
        if u in want or (pre and u.startswith(pre)):
            w.writerow((g.table, g.column, g.data_type))
    return buf.getvalue()


def diff(committed: Any, current: Any, path: str = "") -> list[tuple[str, Any, Any]]:
    """Recursive structural diff -> [(path, committed, current)], sorted by path."""
    if isinstance(committed, dict) and isinstance(current, dict):
        out: list[tuple[str, Any, Any]] = []
        for k in sorted(committed.keys() | current.keys(), key=str):
            p = f"{path}.{k}" if path else str(k)
            if k not in committed:
                out.append((p, None, current[k]))
            elif k not in current:
                out.append((p, committed[k], None))
            else:
                out += diff(committed[k], current[k], p)
        return out
    return [] if committed == current else [(path, committed, current)]


def missing_golden(enabled_dest_tables: Iterable[str], ddl_dir: str | Path) -> list[str]:
    """Enabled destination tables with no golden DDL file (contract test, R8)."""
    have = {p.stem.upper() for p in Path(ddl_dir).glob("*.sql")}
    return sorted(t for t in {t.upper() for t in enabled_dest_tables} if t not in have)
