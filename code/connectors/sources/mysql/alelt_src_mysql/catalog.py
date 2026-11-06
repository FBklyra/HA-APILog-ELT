"""Catalog snapshot from information_schema (read-only). Returns the CatalogSnapshot dict form (no pydantic here);
connector.py validates it into alelt_common.models.CatalogSnapshot.

ColumnInfo mapping:
- engine_type = information_schema.COLUMNS.COLUMN_TYPE as reported ("int unsigned", "enum('A','b')", "json");
  MariaDB JSON (LONGTEXT + json_valid CHECK) is reported as "json"
- codepage    = character set as CCSID number (1208 UTF-8, 1252 latin1, 0 binary; types.CHARSET_CODEPAGE)
- length      = CHARACTER_MAXIMUM_LENGTH (characters) / NUMERIC_PRECISION / bit width; scale = NUMERIC_SCALE or
  DATETIME_PRECISION (fractional seconds)
- key_seq     = position in the PRIMARY KEY
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from . import types as T

_COLS = """
SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.ORDINAL_POSITION, c.DATA_TYPE, c.COLUMN_TYPE,
       c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.DATETIME_PRECISION, c.IS_NULLABLE,
       c.CHARACTER_SET_NAME, c.COLLATION_NAME, c.COLUMN_DEFAULT, c.EXTRA
FROM information_schema.COLUMNS c
JOIN information_schema.TABLES t ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
WHERE t.TABLE_TYPE = 'BASE TABLE' AND c.TABLE_SCHEMA IN ({ph})
ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
"""
_TABLES = """
SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE, ROW_FORMAT, CREATE_OPTIONS FROM information_schema.TABLES
WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ({ph})
"""
_PK = """
SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION FROM information_schema.KEY_COLUMN_USAGE
WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA IN ({ph})
"""
_CHECKS = """
SELECT CONSTRAINT_SCHEMA, TABLE_NAME, CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS
WHERE CONSTRAINT_SCHEMA IN ({ph})
"""


def _s(v: Any) -> str:
    return v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else ("" if v is None else str(v))


def read_catalog(conn: Any, schemas: list[str], connection_id: str, flavor: str,
                 tables: list[tuple[str, str]] | None = None, position: dict[str, Any] | None = None
                 ) -> dict[str, Any]:
    if not schemas:
        raise ValueError("at least one schema (database) is required")
    ph = ", ".join(["%s"] * len(schemas))
    want = {(s.lower(), t.lower()) for s, t in tables or []}
    with conn.cursor() as c:
        c.execute(_TABLES.format(ph=ph), schemas)
        tabs = {(_s(r[0]), _s(r[1])): {"engine": _s(r[2]), "row_format": _s(r[3]), "opts": _s(r[4])}
                for r in c.fetchall()}
        c.execute(_PK.format(ph=ph), schemas)
        pk = {(_s(r[0]), _s(r[1]), _s(r[2])): int(r[3]) for r in c.fetchall()}
        json_cols: set[tuple[str, str, str]] = set()
        if flavor == "mariadb":
            try:
                c.execute(_CHECKS.format(ph=ph), schemas)
                for s, t, clause in c.fetchall():
                    m = re.match(r"^\s*json_valid\(`?([^`)]+)`?\)\s*$", _s(clause), re.I)
                    if m:
                        json_cols.add((_s(s), _s(t), m.group(1)))
            except Exception:  # noqa: BLE001 - CHECK_CONSTRAINTS missing on very old servers
                pass
        c.execute(_COLS.format(ph=ph), schemas)
        rows = c.fetchall()
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        schema, name, col = _s(r[0]), _s(r[1]), _s(r[2])
        if want and (schema.lower(), name.lower()) not in want:
            continue
        data_type, col_type = _s(r[4]).lower(), _s(r[5])
        if (schema, name, col) in json_cols and data_type == "longtext":
            col_type = data_type = "json"
        charset = _s(r[11]) or None
        base = T.base_type(col_type)
        if base in T.BINARY_TYPES or base in ("bit", "geometry", "point", "linestring", "polygon", "multipoint",
                                              "multilinestring", "multipolygon", "geometrycollection",
                                              "geomcollection", "vector"):
            codepage: int | None = 0
        elif charset:
            codepage = T.codepage_for(charset)
        elif base == "json":
            codepage = 1208
        else:
            codepage = None
        length: int | None
        scale: int | None = None
        if r[6] is not None and base not in ("enum", "set"):
            length = int(r[6])
        elif r[7] is not None:
            length = int(r[7])
            scale = int(r[8]) if r[8] is not None else None
        else:
            length = None
        if base == "bit":
            a = T.type_args(col_type)
            length = int(a) if a.isdigit() else 1
        if base in ("time", "datetime", "timestamp") and r[9] is not None:
            scale = int(r[9])
        if base == "json":
            length = None                                  # MariaDB JSON = LONGTEXT (4 GiB): no meaningful length
        elif length is not None and length > 2_147_483_647:
            length = 2_147_483_647                         # LONGTEXT / LONGBLOB: keep within a 32-bit int column
        canon = T.canonical_name(col_type, length if base == "bit" else None)
        if canon == "DECIMAL" and base == "bigint":
            length, scale = 20, 0
        tinfo = tabs.get((schema, name), {})
        t = out.setdefault((schema, name), {
            "table": {"schema": schema, "name": name}, "engine_ids": {},
            "capture_enabled": str(tinfo.get("engine", "")).lower() in ("innodb", "rocksdb", "aria", "myisam", ""),
            "compression": ("COMPRESSED" if "compress" in (tinfo.get("row_format", "") + tinfo.get("opts", "")).lower()
                            else None),
            "columns": []})
        t["columns"].append({
            "name": col, "ordinal": int(r[3]) - 1, "engine_type": col_type, "canonical_type": canon,
            "length": length, "scale": scale, "nullable": _s(r[10]).upper() == "YES",
            "key_seq": pk.get((schema, name, col)), "codepage": codepage,
            "default": None if r[13] is None else _s(r[13])})
    return {"connection_id": connection_id, "engine": flavor, "taken_at": datetime.now(timezone.utc).isoformat(),
            "position": position, "database_codepage": 1208, "tables": list(out.values())}
