"""Column/table policy and output profile, from `config/tables.yaml` + `config/output_profiles.yaml`.

Format: config/DESIGN.md. `ALELT_TABLES_CONFIG` may name one tables.yaml or a directory of them
(one document per connection, matched by `connection_id`). System column names always come from the
output profile, by name, from config/output_profiles.yaml; only the neutral `alelt` default is built in.
Extra profiles (e.g. from a private extension) are merged from ALELT_OUTPUT_PROFILES (os.pathsep-separated files).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from alelt_common.models import CanonicalType, ColumnInfo, TableInfo, TableRef

log = logging.getLogger("alelt_loader")
_CONFIG = Path(__file__).resolve().parents[2] / "config"


@dataclass(frozen=True)
class SystemColumns:
    """System column names written by the destination (per output profile)."""

    name: str
    synced: str
    deleted: str
    row_id: str          # surrogate key for keyless tables


_BUILTIN = {
    "alelt": SystemColumns("alelt", "_ALELT_SYNCED", "_ALELT_DELETED", "_ALELT_ID"),
}
DEFAULT_PROFILE = "alelt"


def _read(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".json":
        return json.loads(text)  # type: ignore[no-any-return]
    import yaml  # pyyaml (hub dependency)

    return yaml.safe_load(text) or {}


def _profile_files(extra: str) -> list[Path]:
    """config/output_profiles.yaml, then `config/extensions.yaml: profile_files`, then ALELT_OUTPUT_PROFILES."""
    files = [_CONFIG / "output_profiles.yaml"]
    try:
        from alelt_schema.config import profile_files   # same resolution as alelt_schema / destinations (X-23, X-24)

        files = list(profile_files(_CONFIG.parent))
    except ImportError:
        pass
    return [*files, *(Path(p) for p in extra.split(os.pathsep) if p)]


def _golden(spec: dict[str, Any]) -> dict[str, Any] | None:
    """Resolve a `from_golden: true` profile from config/golden/columns.csv; None when no golden spec exists."""
    csv_path = Path(os.environ.get("ALELT_GOLDEN_COLUMNS") or _CONFIG / "golden" / "columns.csv")
    if not csv_path.exists():
        return None
    from alelt_schema.config import golden_system_columns, read_golden_rows

    return golden_system_columns(read_golden_rows(csv_path))


def output_profiles(paths: str | None = None) -> dict[str, SystemColumns]:
    """Built-in `alelt` + every profile file (core, extensions, ALELT_OUTPUT_PROFILES), later files win.
    `from_golden` profiles exist only when the golden spec is present (never assumed, R8)."""
    out = dict(_BUILTIN)
    for p in _profile_files(paths if paths is not None else os.environ.get("ALELT_OUTPUT_PROFILES", "")):
        if not p.exists():
            continue
        for n, s in (_read(p).get("profiles") or {}).items():
            spec = _golden(s) if (s or {}).get("from_golden") else s
            if not spec or not all(k in spec for k in ("synced_column", "deleted_column", "id_column")):
                out.pop(n, None)
                continue
            out[n] = SystemColumns(n, spec["synced_column"], spec["deleted_column"], spec["id_column"])
    return out


OUTPUT_PROFILES = output_profiles()


def system_columns(profile: str | None) -> SystemColumns:
    """Resolve an output profile name; unknown names are a configuration error."""
    name = profile or DEFAULT_PROFILE
    try:
        return OUTPUT_PROFILES[name]
    except KeyError:
        raise ValueError(f"unknown output_profile {name!r}; known: {sorted(OUTPUT_PROFILES)}") from None


def hash_value(value: Any) -> str | None:
    """Column hashing placeholder: SHA-256 hex of the UTF-8 text form (bytes hashed as-is).

    TODO OQ-03: must reproduce the existing landing schema's hashing bit-for-bit or be flagged as a contract break.
    """
    if value is None:
        return None
    raw = value if isinstance(value, bytes) else str(value).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


HASHING_IS_PLACEHOLDER = True   # OQ-03 open


@dataclass
class ColumnPolicy:
    enabled: bool = True
    hashed: bool = False


@dataclass(frozen=True)
class TableSchedule:
    """Per-table schedule from tables.yaml `schedule: {class, latency_target_s, window: {start, end, tz}}`.
    Acted on by `alelt_hub.schedule` (X-110): a `window` table is applied only inside its daily window (changes held
    outside, replayed from the hold position when the window opens); a table whose latency target is at least
    ALELT_SCHEDULE_DEFER_MIN_S is applied in deferred rounds every latency_target_s. Others stream normally."""

    schedule_class: str | None = None
    latency_target_s: int | None = None
    window: dict[str, str] | None = None

    @classmethod
    def parse(cls, doc: Any) -> "TableSchedule | None":
        if not doc:
            return None
        if not isinstance(doc, dict):
            raise ValueError(f"schedule must be a mapping, got {doc!r}")
        lat = doc.get("latency_target_s")
        if lat is not None and (not isinstance(lat, int) or lat <= 0):
            raise ValueError(f"schedule.latency_target_s must be a positive integer, got {lat!r}")
        win = doc.get("window")
        if win is not None and not (isinstance(win, dict) and "start" in win and "end" in win):
            raise ValueError(f"schedule.window needs start and end, got {win!r}")
        return cls(doc.get("class"), lat, dict(win) if win else None)


@dataclass
class TablePolicy:
    enabled: bool = True
    dest_name: str | None = None
    sync_mode: str = "SOFT_DELETE"
    columns: dict[str, ColumnPolicy] = field(default_factory=dict)
    schedule: TableSchedule | None = None
    on_quarantine: str | None = None     # hold_table | stop_stream | continue (None = connection default)
    initial_load: str | None = None      # auto | none (None = connection default, alelt_hub.lifecycle)
    replay_from: str | None = None       # test / ops: replay the table's history from this source position


@dataclass
class ConnectionPolicy:
    connection_id: str
    output_profile: str = DEFAULT_PROFILE
    dest_schema: str | None = None
    schemas: dict[str, tuple[bool, str | None]] = field(default_factory=dict)   # S -> (enabled, dest name)
    allow_new_tables: bool = True
    tables: dict[str, TablePolicy] = field(default_factory=dict)                 # 'S.T' -> policy
    snapshot_window: dict[str, str] | None = None        # {start, end, tz}: off-peak window for snapshots (REQ-8)
    on_quarantine: str = "hold_table"                    # connection default (F-01 / X-120), per table overridable
    schema_change_handling: str = "ALLOW_COLUMNS"        # ALLOW_ALL | ALLOW_COLUMNS | BLOCK_ALL (alelt_hub.schema_drift)
    initial_load: str | None = None                      # auto (default: snapshot of new tables) | none
    replay_from: str | None = None                       # test / ops only: history from a position (alelt_hub.lifecycle)

    def quarantine_policy(self, ref: TableRef) -> str:
        """`on_quarantine` of a table: hold_table (default) | stop_stream | continue (explicit opt-in, loud)."""
        return self.table(ref).on_quarantine or self.on_quarantine

    @property
    def system(self) -> SystemColumns:
        return system_columns(self.output_profile)

    def table(self, ref: TableRef) -> TablePolicy:
        tp = self.tables.get(ref.fqn)
        if tp is not None:
            return tp
        if self.schemas:
            en, _ = self.schemas.get(ref.schema_, (False, None))
            return TablePolicy(enabled=en and self.allow_new_tables)
        return TablePolicy(enabled=self.allow_new_tables)

    def dest_name(self, ref: TableRef) -> str:
        return self.table(ref).dest_name or ref.name

    def latency_target_s(self, ref: TableRef, default: float) -> float:
        """Per-table latency target (tables.yaml `schedule.latency_target_s`), else `default`."""
        sch = self.table(ref).schedule
        return float(sch.latency_target_s) if sch and sch.latency_target_s else default

    def project(self, info: TableInfo) -> TableInfo:
        """Destination view of a table: disabled and UNKNOWN columns removed, hashed columns -> STRING(64)."""
        tp = self.table(info.table)
        cols: list[ColumnInfo] = []
        for c in info.columns:
            cp = tp.columns.get(c.name, ColumnPolicy())
            if not cp.enabled or c.canonical_type is CanonicalType.UNKNOWN:
                continue                  # UNKNOWN: values are quarantined by the decoder (R7)
            if cp.hashed:
                c = c.model_copy(update={"canonical_type": CanonicalType.STRING, "engine_type": "HASH",
                                         "length": 64, "scale": None})
            cols.append(c)
        return info.model_copy(update={"columns": cols})

    def apply_row(self, ref: TableRef, row: dict[str, Any] | None) -> dict[str, Any] | None:
        """Filter/hash one row image according to the column policy."""
        if row is None:
            return None
        cols = self.table(ref).columns
        out: dict[str, Any] = {}
        for k, v in row.items():
            cp = cols.get(k)
            if cp is None:
                out[k] = v
            elif cp.enabled:
                out[k] = hash_value(v) if cp.hashed else v
        return out


ON_QUARANTINE = ("hold_table", "stop_stream", "continue")


def _on_quarantine(v: Any, where: str) -> str | None:
    """Validate `on_quarantine` (F-01): unknown values are a configuration error, never a silent default."""
    if v is None:
        return None
    if v not in ON_QUARANTINE:
        raise ValueError(f"{where}: on_quarantine must be one of {ON_QUARANTINE}, got {v!r}")
    return str(v)


def _docs(path: Path) -> list[dict[str, Any]]:
    if path.is_dir():
        return [_read(p) for p in sorted(path.glob("*.y*ml")) + sorted(path.glob("*.json"))]
    return [_read(path)] if path.exists() else []


def default_config_path() -> Path:
    return Path(os.environ.get("ALELT_TABLES_CONFIG") or _CONFIG / "tables.yaml")


def _initial_load(v: Any, where: str) -> str | None:
    """tables.yaml `initial_load`: auto | none (connection default, per table override)."""
    if v is None:
        return None
    x = str(v).strip().lower()
    if x in ("auto", "none"):
        return x
    if x in ("off", "false", "no"):
        return "none"
    raise ValueError(f"initial_load of {where} must be auto or none, got {v!r}")


def load_policy(connection_id: str, path: str | Path | None = None,
                overrides: dict[str, dict[str, Any]] | None = None,
                doc: dict[str, Any] | None = None) -> ConnectionPolicy:
    """Policy for one connection. No matching document => defaults (all enabled, profile 'alelt').

    `overrides` = console edits from the metadata DB: {'S.T': {"enabled": bool, "columns": {col: {...}}}}.
    `doc` = the tables.yaml document itself (e.g. `pipeline_definition.tables_doc`, REQ-14); the file is not read.
    """
    if doc is None:
        p = Path(path) if path else default_config_path()
        doc = next((d for d in _docs(p) if d.get("connection_id") == connection_id), None)
    if doc is None:
        log.warning("no tables.yaml document for connection %s - defaults", connection_id,
                    extra={"component": "alelt_loader"})
        doc = {}
    pol = ConnectionPolicy(
        connection_id=connection_id,
        output_profile=doc.get("output_profile") or DEFAULT_PROFILE,
        dest_schema=doc.get("landing_schema"),
        allow_new_tables=doc.get("schema_change_handling", "ALLOW_COLUMNS") != "BLOCK_ALL",
        schema_change_handling=str(doc.get("schema_change_handling") or "ALLOW_COLUMNS"),
        snapshot_window=dict(doc["snapshot_window"]) if isinstance(doc.get("snapshot_window"), dict) else None,
        on_quarantine=_on_quarantine(doc.get("on_quarantine"), "connection") or "hold_table",
        initial_load=_initial_load(doc.get("initial_load"), "connection"),
        replay_from=str(doc["replay_from"]) if doc.get("replay_from") else None,
    )
    system_columns(pol.output_profile)  # validate early
    merged: dict[str, dict[str, Any]] = {}
    for s, sd in (doc.get("schemas") or {}).items():
        sd = sd or {}
        pol.schemas[s] = (bool(sd.get("enabled", True)), sd.get("name_in_destination"))
        for t, td in (sd.get("tables") or {}).items():
            td = dict(td or {})
            td.setdefault("enabled", bool(sd.get("enabled", True)))
            merged[f"{s}.{t}"] = td
    for fqn, ov in (overrides or {}).items():
        base = merged.setdefault(fqn, {})
        if "enabled" in ov:
            base["enabled"] = ov["enabled"]
        for c, cp in (ov.get("columns") or {}).items():
            base.setdefault("columns", {})[c] = {**((base.get("columns") or {}).get(c) or {}), **cp}
    for fqn, t in merged.items():
        pol.tables[fqn] = TablePolicy(
            enabled=bool(t.get("enabled", True)),
            dest_name=t.get("name_in_destination"),
            sync_mode=t.get("sync_mode", "SOFT_DELETE"),
            columns={c: ColumnPolicy(bool((cp or {}).get("enabled", True)), bool((cp or {}).get("hashed", False)))
                     for c, cp in (t.get("columns") or {}).items()},
            schedule=TableSchedule.parse(t.get("schedule")),
            on_quarantine=_on_quarantine(t.get("on_quarantine"), fqn),
            initial_load=_initial_load(t.get("initial_load"), fqn),
            replay_from=str(t["replay_from"]) if t.get("replay_from") else None,
        )
    return pol
