"""Pipeline-wizard smart defaults (pure, engine-agnostic): table preselection, ordering, latency, output profile,
and the tables.yaml document of a new pipeline."""
from __future__ import annotations

import re
import socket
import time
from collections.abc import Iterable, Mapping
from typing import Any

from alelt_common.models import CatalogSnapshot, TableInfo

HIGH_CHURN_ROWS_PER_DAY = 100_000
_NOISE = re.compile(r"(^|_)(TMP|TEMP|BAK|BACKUP|OLD|COPY|ARCHIVE|ARC|HIST_OLD|DELETED|TEST)(_|$|\d)|^(EXPLAIN_|ADVISE_|SYS)",
                    re.I)


def tcp_check(host: str, port: int, timeout_s: float = 4.0) -> dict[str, Any]:
    """Reachability only (no protocol, no login)."""
    t0 = time.monotonic()
    try:
        with socket.create_connection((host, int(port)), timeout=timeout_s):
            ok, detail = True, "reachable"
    except OSError as e:
        ok, detail = False, f"{type(e).__name__}: {e.strerror or e}"
    return {"step": "network", "ok": ok, "detail": f"{host}:{port} {detail}",
            "elapsed_ms": int((time.monotonic() - t0) * 1000)}


def is_noise(name: str) -> bool:
    return bool(_NOISE.search(name))


def table_defaults(catalog: CatalogSnapshot | None, imported: Mapping[str, Mapping[str, Any]] | None = None,
                   churn: Mapping[str, float] | None = None, golden_tables: Iterable[str] = ()) -> list[dict[str, Any]]:
    """One row per source table with a preselection and the reason. Order: selected first, keyed first,
    high churn first, then name. `imported` = {"S.T": tables.yaml table doc} from an import (wins when present)."""
    golden = {g.upper() for g in golden_tables}
    rows: list[dict[str, Any]] = []
    tables: list[TableInfo] = list(catalog.tables) if catalog else []
    seen = {t.table.fqn for t in tables}
    for fqn in sorted(set(imported or {}) - seen):          # imported but not (yet) in the catalog
        s, _, n = fqn.partition(".")
        rows.append({"fqn": fqn, "schema": s, "name": n, "has_pk": None, "capture_enabled": None, "columns": 0,
                     "selected": bool((imported or {})[fqn].get("enabled", True)), "reason": "imported",
                     "churn_per_day": None, "high_churn": False, "in_golden": n.upper() in golden})
    for t in tables:
        fqn = t.table.fqn
        has_pk = bool(t.key_columns())
        cpd = (churn or {}).get(fqn)
        imp = (imported or {}).get(fqn)
        if imp is not None:
            sel, why = bool(imp.get("enabled", True)) and t.capture_enabled, "imported"
            if imp.get("enabled", True) and not t.capture_enabled:
                why = "capture_disabled"
        elif not t.capture_enabled:
            sel, why = False, "capture_disabled"
        elif is_noise(t.table.name):
            sel, why = False, "noise_name"
        elif golden and t.table.name.upper() not in golden:
            sel, why = False, "not_in_golden"
        else:
            sel, why = True, "keyed" if has_pk else "keyless_surrogate"
        rows.append({"fqn": fqn, "schema": t.table.schema_, "name": t.table.name, "has_pk": has_pk,
                     "capture_enabled": t.capture_enabled, "columns": len(t.columns), "selected": sel, "reason": why,
                     "churn_per_day": cpd, "high_churn": bool(cpd and cpd >= HIGH_CHURN_ROWS_PER_DAY),
                     "in_golden": t.table.name.upper() in golden})
    rows.sort(key=lambda r: (not r["selected"], r["has_pk"] is False, -(r["churn_per_day"] or 0), r["fqn"]))
    return rows


def recommend_latency(rows: Iterable[Mapping[str, Any]], legacy_s: int | None = None) -> dict[str, Any]:
    hot = [r["fqn"] for r in rows if r.get("selected") and r.get("high_churn")]
    if hot:
        return {"latency_target_s": 60, "why": "high_churn", "tables": hot}
    if legacy_s:
        return {"latency_target_s": int(legacy_s), "why": "legacy_schedule", "tables": []}
    return {"latency_target_s": 120, "why": "default", "tables": []}


def recommend_profile(imported_profile: str | None, has_golden: bool) -> str:
    return imported_profile or ("golden_spec" if has_golden else "alelt")


def tables_doc(connection_id: str, source_engine: str, destination_engine: str, source_name: str,
               selected: Iterable[str], catalog: CatalogSnapshot | None, output_profile: str,
               imported: Mapping[str, Mapping[str, Any]] | None = None, legacy_schema: str | None = None,
               landing_schema: str | None = None) -> dict[str, Any]:
    """config/tables.yaml document for a new pipeline: selected tables enabled, imported column policy kept."""
    by = {t.table.fqn: t for t in (catalog.tables if catalog else [])}
    sel = set(selected)
    schemas: dict[str, Any] = {}
    for fqn in sorted(sel | set(imported or {}) | set(by)):
        s, _, n = fqn.partition(".")
        imp = dict((imported or {}).get(fqn) or {})
        cols = dict(imp.get("columns") or {})
        info = by.get(fqn)
        for c in info.key_columns() if info else []:
            cols.setdefault(c.name, {"enabled": True, "hashed": False, "is_primary_key": True})
        sd = schemas.setdefault(s, {"enabled": True, "name_in_destination": s, "tables": {}})
        sd["tables"][n] = {"enabled": fqn in sel, "name_in_destination": str(imp.get("name_in_destination") or n).upper(),
                           "sync_mode": imp.get("sync_mode") or "SOFT_DELETE", "columns": dict(sorted(cols.items()))}
    return {"connection_id": connection_id, "source_engine": source_engine, "destination_engine": destination_engine,
            "output_profile": output_profile, "schema_change_handling": "ALLOW_COLUMNS",
            "source_name": source_name.upper(), "landing_schema": landing_schema or f"ALELT_{source_name.upper()}",
            "legacy_schema": legacy_schema, "schemas": schemas}
