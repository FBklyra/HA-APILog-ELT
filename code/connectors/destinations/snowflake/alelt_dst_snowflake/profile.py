"""Output profile: names/types of the system columns written by the destination (config/output_profiles.yaml)."""
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class OutputProfile:
    name: str
    synced_column: str
    deleted_column: str
    id_column: str
    synced_type: str = "TIMESTAMP_TZ"
    deleted_type: str = "BOOLEAN"
    id_type: str = "VARCHAR(16777216)"

    def system_columns(self, keyless: bool) -> list[tuple[str, str]]:
        cols = [(self.synced_column, self.synced_type), (self.deleted_column, self.deleted_type)]
        if keyless:
            cols.insert(0, (self.id_column, self.id_type))
        return cols


ALELT = OutputProfile("alelt", "_ALELT_SYNCED", "_ALELT_DELETED", "_ALELT_ID")


def golden_system_columns(rows: Iterable[tuple[str, str, str]]) -> dict[str, str]:
    """Detect system-column names/types from golden (table, column, data_type) rows: a `_*_SYNCED` and `_*_DELETED`
    column present in every table, and the most common `_*_ID` column (else derived from the synced prefix)."""
    rows = list(rows)
    tables = {t for t, _, _ in rows}
    types = {c: dt for _, c, dt in rows}
    per_col: dict[str, set[str]] = {}
    for t, c, _ in rows:
        per_col.setdefault(c, set()).add(t)

    def common(suffix: str) -> str:
        found = sorted(c for c, ts in per_col.items() if c.startswith("_") and c.endswith(suffix) and ts == tables)
        if len(found) != 1:
            raise ValueError(f"golden spec: expected one common '*{suffix}' system column, found {found}")
        return found[0]

    synced, deleted = common("_SYNCED"), common("_DELETED")
    ids = sorted((c for c in per_col if c.startswith("_") and c.endswith("_ID")), key=lambda c: (-len(per_col[c]), c))
    id_col = ids[0] if ids else synced[: -len("_SYNCED")] + "_ID"
    return {"synced_column": synced, "deleted_column": deleted, "id_column": id_col,
            "synced_type": types[synced], "deleted_type": types[deleted],
            "id_type": types.get(id_col, "VARCHAR(16777216)")}


def read_golden_rows(path: str | Path) -> list[tuple[str, str, str]]:
    import csv

    with open(path, encoding="utf-8", newline="") as f:
        return [(r["table"], r["column"], r["data_type"]) for r in csv.DictReader(f)]


def profiles_from_dict(doc: dict[str, Any], golden_csv: str | Path | None = None) -> dict[str, OutputProfile]:
    out: dict[str, OutputProfile] = {}
    for name, spec in sorted(doc.get("profiles", {}).items()):
        if spec.get("from_golden"):
            if golden_csv is None:
                continue          # resolvable only with the golden columns.csv
            spec = golden_system_columns(read_golden_rows(golden_csv))
        out[name] = OutputProfile(name=name, **spec)
    return out


def load_profile(paths: str | Path | Sequence[str | Path], name: str | None = None,
                 golden_csv: str | Path | None = None) -> OutputProfile:
    """Load profile `name` (or the first file's default) from one or more output-profile YAML files (merged in order).
    `from_golden` profiles need `golden_csv` (config/golden/columns.csv)."""
    import yaml

    files = [paths] if isinstance(paths, (str, Path)) else list(paths)
    profiles: dict[str, OutputProfile] = {}
    default = "alelt"
    for i, f in enumerate(files):
        doc = yaml.safe_load(Path(f).read_text(encoding="utf-8")) or {}
        profiles.update(profiles_from_dict(doc, golden_csv))
        if i == 0:
            default = doc.get("default", default)
    key = name or default
    if key not in profiles:
        raise KeyError(f"unknown output profile {key!r}; known: {sorted(profiles)}")
    return profiles[key]
