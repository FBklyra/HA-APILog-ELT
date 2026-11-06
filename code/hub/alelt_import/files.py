"""Offline import: classify dropped files (or a ZIP of them) so no live connection to the legacy platform is needed.

Kinds: `json` (API exports - interpreted by the plugin), `yaml` (manifest), `ddl` (CREATE TABLE / GET_DDL output),
`columns` (INFORMATION_SCHEMA.COLUMNS CSV, any header case), `unknown`. Size and entry limits guard against zip bombs.
"""
from __future__ import annotations

import csv
import io
import json
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any

from .ddl import GoldenColumn, full_type
from .golden import parse_ddl

MAX_FILE = 25 << 20
MAX_TOTAL = 100 << 20
MAX_ENTRIES = 2000


class BundleError(ValueError):
    pass


@dataclass
class Bundle:
    json_docs: dict[str, Any] = field(default_factory=dict)       # file name -> parsed JSON
    yaml_docs: dict[str, Any] = field(default_factory=dict)
    ddl: dict[str, str] = field(default_factory=dict)            # TABLE -> DDL
    columns: list[GoldenColumn] = field(default_factory=list)
    ddl_columns: list[GoldenColumn] = field(default_factory=list)
    files: list[dict[str, str]] = field(default_factory=list)     # [{name, kind}]

    @property
    def has_golden(self) -> bool:
        return bool(self.ddl or self.columns)


def _columns_csv(text: str) -> list[GoldenColumn] | None:
    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows:
        return None
    keys = {k.strip().lower(): k for k in rows[0] if k}
    tk = keys.get("table_name") or keys.get("table")
    ck = keys.get("column_name") or keys.get("column")
    if not tk or not ck:
        return None
    dk = keys.get("data_type")
    ok = keys.get("ordinal_position") or keys.get("ordinal")
    nk = keys.get("is_nullable")

    def num(r: dict[str, str], k: str | None) -> int | None:
        v = (r.get(k) or "").strip() if k else ""
        return int(float(v)) if v.replace(".", "", 1).isdigit() else None
    out = []
    for i, r in enumerate(rows):
        dt = (r.get(dk) or "") if dk else ""
        dt = full_type(dt, num(r, keys.get("character_maximum_length")), num(r, keys.get("numeric_precision")),
                       num(r, keys.get("numeric_scale"))) if dt else "UNKNOWN"
        out.append(GoldenColumn(r[tk].strip().upper(), r[ck].strip().upper(), num(r, ok) or i + 1, dt.upper(),
                                (r.get(nk) or "YES").strip().upper() in ("YES", "Y", "TRUE") if nk else True))
    return sorted(out, key=lambda c: (c.table, c.ordinal))


def classify(name: str, data: bytes, bundle: Bundle) -> None:
    if len(data) > MAX_FILE:
        raise BundleError(f"{name}: file larger than {MAX_FILE >> 20} MB")
    suffix = PurePosixPath(name.lower()).suffix
    if suffix == ".zip" or data[:4] == b"PK\x03\x04":
        _unzip(name, data, bundle)
        return
    text = data.decode("utf-8-sig", errors="replace")
    stripped = text.lstrip()
    kind = "unknown"
    if suffix == ".json" or stripped[:1] in ("{", "["):
        try:
            bundle.json_docs[name] = json.loads(text)
            kind = "json"
        except ValueError:
            kind = "unknown"
    elif suffix in (".yaml", ".yml"):
        import yaml

        try:
            bundle.yaml_docs[name] = yaml.safe_load(text)
            kind = "yaml"
        except yaml.YAMLError:
            kind = "unknown"
    elif suffix == ".csv":
        cols = _columns_csv(text)
        if cols:
            bundle.columns += cols
            kind = "columns"
    if kind == "unknown" and "create" in text.lower() and "table" in text.lower():
        ddls, cols = parse_ddl(text)
        if ddls:
            bundle.ddl.update(ddls)
            bundle.ddl_columns += cols
            kind = "ddl"
    bundle.files.append({"name": name, "kind": kind})


def _unzip(name: str, data: bytes, bundle: Bundle) -> None:
    try:
        z = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise BundleError(f"{name}: not a valid ZIP") from None
    infos = [i for i in z.infolist() if not i.is_dir()]
    if len(infos) > MAX_ENTRIES or sum(i.file_size for i in infos) > MAX_TOTAL:
        raise BundleError(f"{name}: ZIP too large")
    for i in infos:
        if i.file_size > MAX_FILE:
            raise BundleError(f"{name}/{i.filename}: entry too large")
        base = PurePosixPath(i.filename)
        if base.name.startswith(".") or "__MACOSX" in base.parts:
            continue
        classify(f"{name}/{i.filename}", z.read(i), bundle)


def load_files(files: Mapping[str, bytes]) -> Bundle:
    if sum(len(v) for v in files.values()) > MAX_TOTAL:
        raise BundleError("upload too large")
    b = Bundle()
    for name, data in sorted(files.items()):
        classify(name, data, b)
    # a columns CSV is more precise than DDL-derived columns for the same table
    csv_tables = {c.table for c in b.columns}
    merged = {(c.table, c.column): c for c in b.columns}
    merged.update({(c.table, c.column): c for c in b.ddl_columns if c.table not in csv_tables})
    b.columns = sorted(merged.values(), key=lambda c: (c.table, c.ordinal))
    b.ddl_columns = []
    return b
