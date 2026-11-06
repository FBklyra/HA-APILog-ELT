"""Dialect adapter contract: everything engine-specific the generic SQL destination needs.

A dialect turns neutral requests (schema, table, columns, keys) into SQL text for one engine, adapts canonical
Python values to what its DB-API driver binds, and opens connections. No dialect holds state across calls
except the connection settings it was built from.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit

from alelt_common.models import CanonicalType

from ..types import TypeMap, load_type_map

_BAD_IDENT = re.compile(r"[\x00]")


@dataclass
class ConnSettings:
    """Parsed destination URL: `<scheme>://user:password@host:port/database?k=v` or `<scheme>:///path`."""

    scheme: str
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None
    database: str | None = None
    path: str | None = None
    options: dict[str, str] = field(default_factory=dict)
    raw: str = ""

    @classmethod
    def parse(cls, url: str) -> ConnSettings:
        u = urlsplit(url)
        opts = dict(parse_qsl(u.query))
        db = unquote(u.path[1:]) if u.path.startswith("/") else unquote(u.path)
        return cls(scheme=u.scheme.lower(), host=u.hostname, port=u.port,
                   user=unquote(u.username) if u.username else None,
                   password=unquote(u.password) if u.password else None,
                   database=db or None, path=db or None, options=opts, raw=url)


class Dialect:
    """Base adapter. Subclasses override what differs; defaults follow standard SQL."""

    name = ""
    driver = ""                      # DB-API module, for messages
    paramstyle = "qmark"             # qmark '?', format '%s', numeric ':1'
    fold = "lower"                   # identifier folding: lower | upper | none
    upsert = "on_conflict"           # on_conflict | on_duplicate | merge
    multirow = False                 # build multi-row VALUES statements instead of executemany
    max_params = 30_000
    max_rows_per_stmt = 1_000
    bulk_rows = 1_000
    merge_cast = False               # CAST(? AS type) in MERGE ... USING (VALUES ...)
    builtin_limits: tuple[tuple[str, int], ...] = ()
    lob_prefixes: tuple[str, ...] = ()   # physical types that cannot be MIN/MAX'ed / compared

    def __init__(self, settings: ConnSettings | None = None) -> None:
        self.settings = settings or ConnSettings(scheme=self.name)
        self.types: TypeMap = load_type_map(self.name, self.builtin_limits)

    # ------------------------------------------------------------ connections
    def connect(self, schemas: list[str]) -> Any:  # pragma: no cover - per dialect
        raise NotImplementedError

    def begin(self, conn: Any) -> None:
        """Start a transaction (drivers with implicit transactions need nothing)."""

    def cursor(self, conn: Any) -> Any:
        return conn.cursor()

    def version(self, cur: Any) -> str:
        cur.execute("SELECT version()")
        return str(cur.fetchone()[0])

    # ------------------------------------------------------------ identifiers
    def norm(self, name: str) -> str:
        if not name or _BAD_IDENT.search(name):
            raise ValueError(f"invalid identifier {name!r}")
        return name.lower() if self.fold == "lower" else name.upper() if self.fold == "upper" else name

    def q(self, name: str) -> str:
        return '"' + self.norm(name).replace('"', '""') + '"'

    def fq(self, schema: str, name: str) -> str:
        return f"{self.q(schema)}.{self.q(name)}"

    def ph(self, i: int) -> str:
        """Placeholder for the i-th (1-based) parameter of a statement."""
        return {"qmark": "?", "format": "%s", "numeric": f":{i}"}[self.paramstyle]

    def phs(self, n: int, start: int = 1) -> list[str]:
        return [self.ph(start + k) for k in range(n)]

    # ------------------------------------------------------------ DDL
    def create_schema(self, cur: Any, schema: str) -> None:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.q(schema)}")

    def drop_schema(self, cur: Any, schema: str) -> None:
        """Cleanup helper (tests / teardown of objects ALELT created)."""
        cur.execute(f"DROP SCHEMA IF EXISTS {self.q(schema)} CASCADE")

    def columns_sql(self) -> str:
        """(schema, table) -> rows (column_name, data_type, is_nullable YES/NO) in ordinal order."""
        p = self.phs(2)
        return ("SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                f"WHERE table_schema = {p[0]} AND table_name = {p[1]} ORDER BY ordinal_position")

    def tables_sql(self) -> str:
        return ("SELECT table_name FROM information_schema.tables WHERE table_schema = "
                f"{self.ph(1)} AND table_type = 'BASE TABLE' ORDER BY table_name")

    def describe(self, cur: Any, schema: str, table: str) -> list[dict[str, Any]]:
        cur.execute(self.columns_sql(), (self.norm(schema), self.norm(table)))
        return [{"name": r[0], "type": str(r[1]).lower(), "nullable": str(r[2]).upper() in ("YES", "Y", "1", "TRUE")}
                for r in cur.fetchall()]

    def list_tables(self, cur: Any, schema: str) -> list[str]:
        cur.execute(self.tables_sql(), (self.norm(schema),))
        return [r[0] for r in cur.fetchall()]

    def create_table_sql(self, fqn: str, cols: list[tuple[str, str, bool]], keys: list[str]) -> str:
        body = [f"{self.q(n)} {t}{'' if nullable else ' NOT NULL'}" for n, t, nullable in cols]
        body.append(f"PRIMARY KEY ({', '.join(map(self.q, keys))})")
        return f"CREATE TABLE {fqn} ({', '.join(body)})"

    def add_column_sql(self, fqn: str, name: str, ptype: str) -> str:
        return f"ALTER TABLE {fqn} ADD COLUMN {self.q(name)} {ptype}"

    def alter_type_sql(self, fqn: str, name: str, ptype: str, nullable: bool) -> list[str]:
        """Widen a column's type in place (drift); [] = not needed / not supported."""
        return [f"ALTER TABLE {fqn} ALTER COLUMN {self.q(name)} TYPE {ptype}"]

    def view_sql(self, vfqn: str, select: str) -> list[str]:
        return [f"CREATE OR REPLACE VIEW {vfqn} AS {select}"]

    # ------------------------------------------------------------ DML
    def upsert_sql(self, fqn: str, cols: list[str], keys: list[str], update: list[str],
                   ptypes: dict[str, str], rows: int = 1) -> str:
        """Upsert statement for `rows` rows (rows > 1 only when `multirow`); params = row-major values of `cols`."""
        qc = ", ".join(map(self.q, cols))
        qk = ", ".join(map(self.q, keys))
        vals = self.phs(len(cols))
        if update:
            sets = ", ".join(f"{self.q(c)} = EXCLUDED.{self.q(c)}" for c in update)
            tail = f"ON CONFLICT ({qk}) DO UPDATE SET {sets}"
        else:
            tail = f"ON CONFLICT ({qk}) DO NOTHING"
        return f"INSERT INTO {fqn} ({qc}) VALUES ({', '.join(vals)}) {tail}"

    def merge_sql(self, fqn: str, cols: list[str], keys: list[str], update: list[str],
                  ptypes: dict[str, str], rows: int, hint: str = "") -> str:
        """Standard MERGE with a VALUES source (SQL Server, Db2)."""
        n = len(cols)
        tuples = []
        for r in range(rows):
            p = self.phs(n, 1 + r * n)
            if self.merge_cast:
                p = [f"CAST({x} AS {ptypes[c]})" for x, c in zip(p, cols)]
            tuples.append("(" + ", ".join(p) + ")")
        on = " AND ".join(f"T.{self.q(k)} = S.{self.q(k)}" for k in keys)
        qc = ", ".join(map(self.q, cols))
        sql = (f"MERGE INTO {fqn}{hint} AS T USING (VALUES {', '.join(tuples)}) AS S ({qc}) ON {on}")
        if update:
            sql += " WHEN MATCHED THEN UPDATE SET " + ", ".join(f"{self.q(c)} = S.{self.q(c)}" for c in update)
        sql += f" WHEN NOT MATCHED THEN INSERT ({qc}) VALUES ({', '.join('S.' + self.q(c) for c in cols)})"
        return sql

    def limit(self, n: int) -> str:
        """Row limit appended after ORDER BY."""
        return f" LIMIT {int(n)}"

    def prepare(self, cur: Any, cols: list[str], ptypes: dict[str, str], rows: int = 1) -> None:
        """Hook before execute/executemany (e.g. LOB input sizes)."""

    def executemany(self, cur: Any, sql: str, seq: list[tuple[Any, ...]]) -> None:
        cur.executemany(sql, seq)

    # ------------------------------------------------------------ values
    def adapt(self, v: Any, ct: CanonicalType) -> Any:
        """Canonical Python value -> value the driver binds for this dialect."""
        if v is None:
            return None
        if ct is CanonicalType.JSON and not isinstance(v, (str, bytes)):
            return json.dumps(v, separators=(",", ":"), ensure_ascii=False, default=str)
        return v

    @staticmethod
    def utc_naive(v: Any) -> Any:
        if isinstance(v, datetime) and v.tzinfo is not None:
            return v.astimezone(timezone.utc).replace(tzinfo=None)
        return v

    def is_lob(self, ptype: str) -> bool:
        return ptype.lower().startswith(self.lob_prefixes) if self.lob_prefixes else False


def norm_value(v: Any) -> Any:
    """Comparable form of a key value read back from any driver (for insert/update statistics only)."""
    if v is None:
        return None
    if isinstance(v, memoryview):
        v = bytes(v)
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).hex()
    if isinstance(v, bool):
        return str(int(v))
    if isinstance(v, (int, float, Decimal)):
        try:
            d = Decimal(str(v)).normalize()
            return format(d, "f")
        except Exception:  # noqa: BLE001
            return str(v)
    if isinstance(v, datetime):
        if v.tzinfo is not None:
            v = v.astimezone(timezone.utc).replace(tzinfo=None)
        return v.isoformat(sep="T")
    if isinstance(v, (date, time)):
        return v.isoformat()
    if isinstance(v, str):
        s = v.rstrip()
        if len(s) >= 19 and s[10:11] in (" ", "T") and s[4:5] == "-":   # ISO text (SQLite) -> same form
            try:
                return norm_value(datetime.fromisoformat(s))
            except ValueError:
                return s
        return s
    return str(v)
