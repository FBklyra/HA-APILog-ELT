"""Connection config (config/tables.yaml) and output profiles (config/output_profiles.yaml), hub side."""
from __future__ import annotations

from enum import Enum
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from alelt_common.models import TableRef


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Policy(str, Enum):
    ALLOW_ALL = "ALLOW_ALL"
    ALLOW_COLUMNS = "ALLOW_COLUMNS"
    BLOCK_ALL = "BLOCK_ALL"


class ColumnConfig(_M):
    enabled: bool = True
    hashed: bool = False
    is_primary_key: bool = False


class TableConfig(_M):
    enabled: bool = True
    name_in_destination: str | None = None
    sync_mode: str = "SOFT_DELETE"
    legacy_sync_mode: str | None = None       # importer: unsupported legacy mode kept for the record (X-127)
    columns: dict[str, ColumnConfig] = Field(default_factory=dict)
    on_quarantine: Literal["hold_table", "stop_stream", "continue"] | None = None   # F-01 (alelt_hub.integrity)


class SchemaConfig(_M):
    enabled: bool = True
    name_in_destination: str | None = None
    tables: dict[str, TableConfig] = Field(default_factory=dict)


class WindowConfig(_M):
    start: str
    end: str
    tz: str = "UTC"


class ConnectionConfig(_M):
    config_version: int = 1                   # document format version (alelt_hub.config_store.migrate_doc)
    connection_id: str
    source_engine: str = "db2"
    destination_engine: str = "snowflake"
    output_profile: str = "alelt"
    schema_change_handling: Policy = Policy.ALLOW_COLUMNS
    source_name: str
    landing_schema: str | None = None
    view_schema: str | None = None
    legacy_schema: str | None = None          # existing landing schema of the legacy pipeline (read-only)
    snapshot_window: WindowConfig | None = None
    on_quarantine: Literal["hold_table", "stop_stream", "continue"] = "hold_table"  # F-01 default per table
    maintenance: dict[str, Any] | None = None   # destination maintain() cadence {every_s, window, enabled} (hub)
    schemas: dict[str, SchemaConfig] = Field(default_factory=dict)

    @property
    def landing(self) -> str:
        return self.landing_schema or f"ALELT_{self.source_name.upper()}"

    @property
    def views(self) -> str:
        return self.view_schema or f"{self.source_name.upper()}_RAW"

    def table(self, ref: TableRef) -> TableConfig | None:
        s = self.schemas.get(ref.schema_)
        return s.tables.get(ref.name) if s else None

    def is_enabled(self, ref: TableRef) -> bool:
        s = self.schemas.get(ref.schema_)
        t = s.tables.get(ref.name) if s else None
        return bool(s and s.enabled and t and t.enabled)

    def dest_name(self, ref: TableRef) -> str:
        t = self.table(ref)
        return ((t.name_in_destination if t else None) or ref.name).upper()

    def enabled_tables(self) -> list[TableRef]:
        return [TableRef(schema=s, name=t) for s, sc in sorted(self.schemas.items()) if sc.enabled
                for t, tc in sorted(sc.tables.items()) if tc.enabled]

    def to_yaml(self) -> str:
        import yaml

        return yaml.safe_dump(self.model_dump(mode="json"), sort_keys=True, default_flow_style=False, allow_unicode=True)


def load_connection(path: str | Path) -> ConnectionConfig:
    import yaml

    return ConnectionConfig.model_validate(yaml.safe_load(Path(path).read_text(encoding="utf-8")))


class OutputProfile(_M):
    name: str
    synced_column: str
    deleted_column: str
    id_column: str
    synced_type: str = "TIMESTAMP_TZ"
    deleted_type: str = "BOOLEAN"
    id_type: str = "VARCHAR(16777216)"

    @property
    def prefixes(self) -> tuple[str, ...]:
        return tuple({c.rsplit("_", 1)[0] + "_" for c in (self.synced_column, self.deleted_column, self.id_column)})


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


def load_profiles(paths: str | Path | Sequence[str | Path],
                  golden_csv: str | Path | None = None) -> tuple[dict[str, OutputProfile], str]:
    """Merge profile files in order; the default comes from the first file. `from_golden` profiles resolve
    from golden columns.csv when given (skipped otherwise)."""
    import yaml

    files = [paths] if isinstance(paths, (str, Path)) else list(paths)
    profiles: dict[str, OutputProfile] = {}
    default = "alelt"
    for i, f in enumerate(files):
        doc: dict[str, Any] = yaml.safe_load(Path(f).read_text(encoding="utf-8")) or {}
        for n, spec in sorted(doc.get("profiles", {}).items()):
            if spec.get("from_golden"):
                if golden_csv is None:
                    continue
                spec = golden_system_columns(read_golden_rows(golden_csv))
            profiles[n] = OutputProfile(name=n, **spec)
        if i == 0:
            default = str(doc.get("default", default))
    return profiles, default


def load_profile(paths: str | Path | Sequence[str | Path], name: str | None = None,
                 golden_csv: str | Path | None = None) -> OutputProfile:
    profiles, default = load_profiles(paths, golden_csv)
    key = name or default
    if key not in profiles:
        raise KeyError(f"unknown output profile {key!r}; known: {sorted(profiles)}")
    return profiles[key]


class Extension(_M):
    path: str | None = None
    import_commands: dict[str, str] = Field(default_factory=dict)
    import_plugins: dict[str, str] = Field(default_factory=dict)     # name -> 'module:Class' (alelt_import.plugin)
    profile_files: list[str] = Field(default_factory=list)
    reference_mappings: dict[str, str] = Field(default_factory=dict)  # output profile -> 'module:function' (X-124)


def load_extensions(repo_root: str | Path) -> list[Extension]:
    """config/extensions.yaml; adds each extension path to sys.path. Missing file => no extensions."""
    import sys

    import yaml

    root = Path(repo_root)
    f = root / "config" / "extensions.yaml"
    if not f.exists():
        return []
    exts = [Extension.model_validate(e) for e in (yaml.safe_load(f.read_text(encoding="utf-8")) or {}).get("extensions") or []]
    for e in exts:
        if e.path and str(root / e.path) not in sys.path:
            sys.path.insert(0, str(root / e.path))
    return exts


def profile_files(repo_root: str | Path) -> list[Path]:
    """Core profile file followed by every extension profile file."""
    root = Path(repo_root)
    return [root / "config" / "output_profiles.yaml",
            *(root / p for e in load_extensions(root) for p in e.profile_files)]
