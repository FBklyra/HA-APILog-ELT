"""alelt_dst_sql - generic SQL destination (engine `sql`, `dialect` in the destination config).

Register: ALELT_DESTINATION=alelt_dst_sql:SqlDestination, ALELT_DEST_URL=<dialect URL> (see README.md).
"""
from __future__ import annotations
import importlib.util as _ilu
import sys as _sys
from pathlib import Path as _Path

for _dep, _dir in (("alelt_dst_kit", "_spi"),):                     # repo checkout without PYTHONPATH: shared destination base
    if _ilu.find_spec(_dep) is None:
        _p = str(_Path(__file__).resolve().parents[2] / _dir)
        if _p not in _sys.path:
            _sys.path.append(_p)

from typing import Any
from urllib.parse import quote

from .destination import SqlDestination, default_compare, split_sql
from .dialects import DIALECTS, Dialect, dialect_name

__all__ = ["DIALECTS", "Dialect", "SqlDestination", "catalog", "default_compare", "dialect_name", "split_sql",
           "url_from_fields"]

_NET = [{"name": "host", "label": "Host", "required": True},
        {"name": "port", "label": "Port", "required": True},
        {"name": "database", "label": "Database", "required": True},
        {"name": "user", "label": "User", "required": True},
        {"name": "password", "label": "Password", "secret": True},
        {"name": "schema", "label": "Landing schema", "required": True},
        {"name": "system_columns", "label": "System columns (off = pure mirror, hard deletes)",
         "default": "on", "choices": ["on", "off"]}]
_FILE = [{"name": "path", "label": "Database file", "required": True},
         {"name": "schema", "label": "Landing schema", "required": True},
         {"name": "system_columns", "label": "System columns (off = pure mirror, hard deletes)",
          "default": "on", "choices": ["on", "off"]}]

_ENTRIES = [
    ("postgres", "PostgreSQL (also Aurora, AlloyDB, CockroachDB, YugabyteDB)", 5432, "psycopg"),
    ("mysql", "MySQL 8/9 (also Aurora MySQL, TiDB)", 3306, "PyMySQL"),
    ("mariadb", "MariaDB", 3306, "PyMySQL"),
    ("sqlserver", "SQL Server / Azure SQL", 1433, "python-tds"),
    ("oracle", "Oracle Database", 1521, "python-oracledb"),
    ("db2", "IBM Db2 LUW", 50000, "ibm_db"),
    ("sqlite", "SQLite file", None, "sqlite3"),
    ("duckdb", "DuckDB file", None, "duckdb"),
]


def catalog() -> list[dict[str, Any]]:
    """Destination wizard entries: {engine:'sql', dialect, display_name, driver, fields[], test, bootstrap}."""
    out = []
    for d, label, port, drv in _ENTRIES:
        fields = [dict(f) for f in (_FILE if port is None else _NET)]
        for f in fields:
            if f["name"] == "port":
                f["default"] = port
        out.append({"engine": "sql", "dialect": d, "display_name": label, "driver": drv, "fields": fields,
                    "test": "SqlDestination.test_connection", "bootstrap": "SqlDestination.bootstrap"})
    return out


def url_from_fields(dialect: str, fields: dict[str, Any]) -> str:
    """Build the destination URL from wizard fields (the password is passed separately, never stored in it)."""
    d = dialect_name(dialect)
    opts = "" if str(fields.get("system_columns", "on")).lower() != "off" else "?system_columns=off"
    if d in ("sqlite", "duckdb"):
        return f"{d}:///{fields['path']}{opts}"
    user = quote(str(fields.get("user") or ""), safe="")
    host = fields.get("host") or "localhost"
    port = f":{fields['port']}" if fields.get("port") else ""
    return f"{d}://{user}@{host}{port}/{quote(str(fields.get('database') or ''), safe='')}{opts}"
