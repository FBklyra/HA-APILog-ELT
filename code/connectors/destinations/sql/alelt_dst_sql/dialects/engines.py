"""Dialect adapters: postgres, mysql, mariadb, sqlserver, oracle, db2, sqlite, duckdb.

Compatibility notes (same wire protocol / SQL, use the listed dialect):
- postgres: PostgreSQL, Aurora PostgreSQL, AlloyDB, Cloud SQL, Azure Database for PostgreSQL, YugabyteDB (YSQL),
  CockroachDB (ON CONFLICT supported; `version()` differs; no `CREATE SCHEMA ... CASCADE` drop needed).
- mysql: MySQL 8.0.19+ / 9, Aurora MySQL, TiDB (`?upsert=values` selects the VALUES() form TiDB supports).
- mariadb: MariaDB 10.5+ (VALUES() upsert form, JSON as LONGTEXT).
- sqlserver: SQL Server 2017+, Azure SQL Database / Managed Instance, Azure SQL Edge. Fabric Warehouse lacks
  PRIMARY KEY enforcement and MERGE on some tiers - not supported by this adapter yet.
- oracle: Oracle 19c+ (thin python-oracledb); schema = user, created only with `?create_schema=1`.
- db2: Db2 LUW 11.5 (ibm_db, Apache-2.0); local catalog alias `db2:///<ALIAS>` or `db2://user:pw@host:25000/DB`.
"""
from __future__ import annotations

import os
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from alelt_common.models import CanonicalType

from .base import Dialect

_T = 10  # connect / login timeout, seconds


def _opt(d: Dialect, key: str, default: str | None = None) -> str | None:
    return d.settings.options.get(key, default)


# ====================================================================== PostgreSQL
# libpq connection parameters kept from the destination URL query (DST-sql-09); plugin options (schema, case, ...)
# are not libpq's and are dropped
_LIBPQ = {"sslmode", "sslrootcert", "sslcert", "sslkey", "sslpassword", "sslcrl", "sslnegotiation", "sslcompression",
          "requiressl", "application_name", "target_session_attrs", "options", "keepalives", "keepalives_idle",
          "keepalives_interval", "keepalives_count", "tcp_user_timeout", "gssencmode", "krbsrvname", "service",
          "channel_binding", "load_balance_hosts", "hostaddr", "passfile", "require_auth", "client_encoding"}


def pg_conninfo_url(raw: str) -> str:
    from urllib.parse import parse_qsl, urlencode

    rest = raw.split("://", 1)[1]
    base, _, query = rest.partition("?")
    keep = [(k, v) for k, v in parse_qsl(query, keep_blank_values=True) if k in _LIBPQ]
    return "postgresql://" + base + ("?" + urlencode(keep) if keep else "")


class PostgresDialect(Dialect):
    name = "postgres"
    driver = "psycopg"
    paramstyle = "format"
    builtin_limits = (("max_varchar", 10_485_760),)

    def connect(self, schemas: list[str]) -> Any:
        import psycopg

        s = self.settings
        url = pg_conninfo_url(s.raw)
        return psycopg.connect(url, autocommit=False, connect_timeout=int(_opt(self, "connect_timeout", str(_T))))

    def describe(self, cur: Any, schema: str, table: str) -> list[dict[str, Any]]:
        cur.execute("SELECT a.attname, format_type(a.atttypid, a.atttypmod), NOT a.attnotnull FROM pg_attribute a "
                    "JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
                    (self.norm(schema), self.norm(table)))
        return [{"name": r[0], "type": str(r[1]).lower(), "nullable": bool(r[2])} for r in cur.fetchall()]

    def adapt(self, v: Any, ct: CanonicalType) -> Any:
        if v is None:
            return None
        if ct is CanonicalType.JSON:
            import json

            from psycopg.types.json import Jsonb

            if isinstance(v, (str, bytes)):           # canonical JSON is text (REMOTE.md S7)
                try:
                    return Jsonb(json.loads(v))
                except ValueError:
                    return Jsonb(v)
            return Jsonb(v)
        return v


# ====================================================================== MySQL / MariaDB / TiDB
class MySQLDialect(Dialect):
    name = "mysql"
    driver = "pymysql"
    paramstyle = "format"
    upsert = "on_duplicate"
    multirow = True
    max_params = 60_000
    lob_prefixes = ("json",)

    def q(self, name: str) -> str:
        return "`" + self.norm(name).replace("`", "``") + "`"

    def connect(self, schemas: list[str]) -> Any:
        import pymysql

        s = self.settings
        return pymysql.connect(host=s.host or "localhost", port=s.port or 3306, user=s.user, password=s.password or "",
                               database=s.database, charset="utf8mb4", autocommit=False, binary_prefix=True,
                               connect_timeout=_T, read_timeout=int(_opt(self, "read_timeout", "300")),
                               write_timeout=int(_opt(self, "write_timeout", "300")),
                               init_command="SET time_zone = '+00:00'")

    def version(self, cur: Any) -> str:
        cur.execute("SELECT version()")
        return str(cur.fetchone()[0])

    # schema = database
    def create_schema(self, cur: Any, schema: str) -> None:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {self.q(schema)} CHARACTER SET utf8mb4")

    def drop_schema(self, cur: Any, schema: str) -> None:
        cur.execute(f"DROP DATABASE IF EXISTS {self.q(schema)}")

    def describe(self, cur: Any, schema: str, table: str) -> list[dict[str, Any]]:
        cur.execute("SELECT column_name, column_type, is_nullable FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                    (self.norm(schema), self.norm(table)))
        return [{"name": r[0], "type": str(r[1]).lower(), "nullable": r[2] == "YES"} for r in cur.fetchall()]

    def alter_type_sql(self, fqn: str, name: str, ptype: str, nullable: bool) -> list[str]:
        return [f"ALTER TABLE {fqn} MODIFY COLUMN {self.q(name)} {ptype}{'' if nullable else ' NOT NULL'}"]

    def values_form(self) -> bool:
        return _opt(self, "upsert", "alias") == "values"

    def upsert_sql(self, fqn: str, cols: list[str], keys: list[str], update: list[str],
                   ptypes: dict[str, str], rows: int = 1) -> str:
        n = len(cols)
        tuples = ", ".join("(" + ", ".join(self.phs(n, 1 + r * n)) + ")" for r in range(rows))
        sets = update or keys[:1]                     # no updatable column: no-op assignment keeps the row
        if self.values_form():
            tail = ", ".join(f"{self.q(c)} = VALUES({self.q(c)})" for c in sets)
            return f"INSERT INTO {fqn} ({', '.join(map(self.q, cols))}) VALUES {tuples} ON DUPLICATE KEY UPDATE {tail}"
        tail = ", ".join(f"{self.q(c)} = alelt_new.{self.q(c)}" for c in sets)
        return (f"INSERT INTO {fqn} ({', '.join(map(self.q, cols))}) VALUES {tuples} AS alelt_new "
                f"ON DUPLICATE KEY UPDATE {tail}")

    def view_sql(self, vfqn: str, select: str) -> list[str]:
        return [f"CREATE OR REPLACE VIEW {vfqn} AS {select}"]

    def adapt(self, v: Any, ct: CanonicalType) -> Any:
        v = super().adapt(v, ct)
        if v is None:
            return None
        if ct is CanonicalType.INSTANT:
            return self.utc_naive(v)
        if isinstance(v, bool):
            return int(v)
        return v


class MariaDBDialect(MySQLDialect):
    name = "mariadb"

    def values_form(self) -> bool:
        return True


# ====================================================================== SQL Server / Azure SQL
class SqlServerDialect(Dialect):
    name = "sqlserver"
    driver = "pytds"
    paramstyle = "format"
    fold = "none"
    upsert = "merge"
    multirow = True
    merge_cast = True
    max_params = 2_000          # protocol limit 2100
    max_rows_per_stmt = 1_000
    lob_prefixes = ("nvarchar(max)", "varbinary(max)", "varchar(max)", "xml", "text", "ntext", "image")

    def q(self, name: str) -> str:
        return "[" + self.norm(name).replace("]", "]]") + "]"

    def connect(self, schemas: list[str]) -> Any:
        import pytds

        s = self.settings
        return pytds.connect(server=s.host or "localhost", port=s.port or 1433, database=s.database or "master",
                             user=s.user, password=s.password, autocommit=False, login_timeout=_T,
                             timeout=float(_opt(self, "timeout", "300")), appname="alelt")

    def version(self, cur: Any) -> str:
        cur.execute("SELECT @@VERSION")
        return str(cur.fetchone()[0]).splitlines()[0]

    def create_schema(self, cur: Any, schema: str) -> None:
        s = self.norm(schema).replace("'", "''")
        cur.execute(f"IF SCHEMA_ID(N'{s}') IS NULL EXEC(N'CREATE SCHEMA {self.q(schema).replace(chr(39), chr(39) * 2)}')")

    def drop_schema(self, cur: Any, schema: str) -> None:
        s = self.norm(schema).replace("'", "''")
        cur.execute("SELECT o.name, o.type FROM sys.objects o JOIN sys.schemas sc ON sc.schema_id = o.schema_id "
                    f"WHERE sc.name = N'{s}' AND o.type IN ('U', 'V')")
        for name, typ in cur.fetchall():
            cur.execute(f"DROP {'VIEW' if typ.strip() == 'V' else 'TABLE'} {self.fq(schema, name)}")
        cur.execute(f"IF SCHEMA_ID(N'{s}') IS NOT NULL EXEC(N'DROP SCHEMA {self.q(schema).replace(chr(39), chr(39) * 2)}')")

    def describe(self, cur: Any, schema: str, table: str) -> list[dict[str, Any]]:
        cur.execute("SELECT c.name, t.name, c.max_length, c.precision, c.scale, c.is_nullable FROM sys.columns c "
                    "JOIN sys.types t ON t.user_type_id = c.user_type_id WHERE c.object_id = "
                    "OBJECT_ID(QUOTENAME(%s) + '.' + QUOTENAME(%s)) ORDER BY c.column_id", (schema, table))
        out = []
        for n, t, ml, p, sc, nl in cur.fetchall():
            t = t.lower()
            if t in ("nvarchar", "nchar"):
                t += "(max)" if ml == -1 else f"({ml // 2})"
            elif t in ("varchar", "char", "varbinary", "binary"):
                t += "(max)" if ml == -1 else f"({ml})"
            elif t in ("decimal", "numeric"):
                t += f"({p},{sc})"
            elif t in ("datetime2", "datetimeoffset", "time"):
                t += f"({sc})"
            out.append({"name": n, "type": t, "nullable": bool(nl)})
        return out

    def tables_sql(self) -> str:
        return ("SELECT table_name FROM information_schema.tables WHERE table_schema = %s "
                "AND table_type = 'BASE TABLE' ORDER BY table_name")

    def add_column_sql(self, fqn: str, name: str, ptype: str) -> str:
        return f"ALTER TABLE {fqn} ADD {self.q(name)} {ptype}"

    def alter_type_sql(self, fqn: str, name: str, ptype: str, nullable: bool) -> list[str]:
        return [f"ALTER TABLE {fqn} ALTER COLUMN {self.q(name)} {ptype} {'NULL' if nullable else 'NOT NULL'}"]

    def view_sql(self, vfqn: str, select: str) -> list[str]:
        return [f"CREATE OR ALTER VIEW {vfqn} AS {select}"]

    def upsert_sql(self, fqn: str, cols: list[str], keys: list[str], update: list[str],
                   ptypes: dict[str, str], rows: int = 1) -> str:
        return self.merge_sql(fqn, cols, keys, update, ptypes, rows, hint=" WITH (HOLDLOCK)") + ";"

    def limit(self, n: int) -> str:
        return f" OFFSET 0 ROWS FETCH NEXT {int(n)} ROWS ONLY"

    def adapt(self, v: Any, ct: CanonicalType) -> Any:
        v = super().adapt(v, ct)
        if isinstance(v, (bytes, bytearray, memoryview)):
            import pytds

            return pytds.Binary(bytes(v))         # plain bytes are sent as text by python-tds
        return v


# ====================================================================== Oracle
class OracleDialect(Dialect):
    name = "oracle"
    driver = "oracledb"
    paramstyle = "numeric"
    fold = "upper"
    upsert = "merge"
    max_params = 30_000
    lob_prefixes = ("clob", "blob", "nclob")

    def connect(self, schemas: list[str]) -> Any:
        import oracledb

        s = self.settings
        dsn = f"{s.host or 'localhost'}:{s.port or 1521}/{s.database or 'FREEPDB1'}"
        conn = oracledb.connect(user=s.user, password=s.password, dsn=dsn, tcp_connect_timeout=_T)
        conn.call_timeout = int(_opt(self, "call_timeout_ms", "300000"))

        def _decimals(cur: Any, meta: Any) -> Any:
            # NUMBER with a fraction (or unconstrained) is fetched as Decimal, not float (verify helpers compare
            # exact values; the default float fetch loses digits beyond 15-17 significant)
            if meta.type_code is oracledb.DB_TYPE_NUMBER and (meta.scale or 0) != 0 or (
                    meta.type_code is oracledb.DB_TYPE_NUMBER and not meta.precision):
                return cur.var(Decimal, arraysize=cur.arraysize)
            return None
        conn.outputtypehandler = _decimals
        return conn

    def version(self, cur: Any) -> str:
        cur.execute("SELECT banner FROM v$version WHERE ROWNUM = 1")
        return str(cur.fetchone()[0])

    def create_schema(self, cur: Any, schema: str) -> None:
        cur.execute("SELECT count(*) FROM all_users WHERE username = :1", (self.norm(schema),))
        if cur.fetchone()[0]:
            return
        if _opt(self, "create_schema", "0") not in ("1", "true", "yes"):
            raise RuntimeError(f"Oracle schema (user) {self.norm(schema)} does not exist; create it or pass "
                               "?create_schema=1 with a user allowed to CREATE USER")
        cur.execute(f"CREATE USER {self.q(schema)} NO AUTHENTICATION QUOTA UNLIMITED ON USERS")

    def drop_schema(self, cur: Any, schema: str) -> None:
        cur.execute(f"DROP USER {self.q(schema)} CASCADE")

    def describe(self, cur: Any, schema: str, table: str) -> list[dict[str, Any]]:
        cur.execute("SELECT column_name, data_type, nullable, char_length, char_used, data_precision, data_scale "
                    "FROM all_tab_columns WHERE owner = :1 AND table_name = :2 ORDER BY column_id",
                    (self.norm(schema), self.norm(table)))
        out = []
        for name, dt, nullable, clen, cused, prec, scale in cur.fetchall():
            t = str(dt).lower()
            if t in ("varchar2", "nvarchar2", "char", "nchar") and clen:
                t += f"({clen}{' char' if cused == 'C' and not t.startswith('n') else ''})"
            elif t == "raw" and clen:
                t += f"({clen})"
            elif t == "number" and prec is not None:
                t += f"({prec},{scale or 0})"
            out.append({"name": name, "type": t, "nullable": nullable == "Y"})
        return out

    def tables_sql(self) -> str:
        return "SELECT table_name FROM all_tables WHERE owner = :1 ORDER BY table_name"

    def create_table_sql(self, fqn: str, cols: list[tuple[str, str, bool]], keys: list[str]) -> str:
        return super().create_table_sql(fqn, cols, keys)

    def add_column_sql(self, fqn: str, name: str, ptype: str) -> str:
        return f"ALTER TABLE {fqn} ADD ({self.q(name)} {ptype})"

    def alter_type_sql(self, fqn: str, name: str, ptype: str, nullable: bool) -> list[str]:
        if ptype.lower().startswith(("clob", "blob", "nclob")):
            return []                                  # VARCHAR2 -> CLOB is not an in-place MODIFY
        return [f"ALTER TABLE {fqn} MODIFY ({self.q(name)} {ptype})"]

    def upsert_sql(self, fqn: str, cols: list[str], keys: list[str], update: list[str],
                   ptypes: dict[str, str], rows: int = 1) -> str:
        src = ", ".join(f"{p} AS {self.q(c)}" for p, c in zip(self.phs(len(cols)), cols))
        on = " AND ".join(f"T.{self.q(k)} = S.{self.q(k)}" for k in keys)
        qc = ", ".join(map(self.q, cols))
        sql = f"MERGE INTO {fqn} T USING (SELECT {src} FROM dual) S ON ({on})"
        if update:
            sql += " WHEN MATCHED THEN UPDATE SET " + ", ".join(f"T.{self.q(c)} = S.{self.q(c)}" for c in update)
        return sql + f" WHEN NOT MATCHED THEN INSERT ({qc}) VALUES ({', '.join('S.' + self.q(c) for c in cols)})"

    def prepare(self, cur: Any, cols: list[str], ptypes: dict[str, str], rows: int = 1) -> None:
        import oracledb

        def size(pt: str) -> Any:
            pt = pt.lower()
            if pt.startswith(("clob", "nclob")):
                return oracledb.DB_TYPE_CLOB
            if pt.startswith("blob"):
                return oracledb.DB_TYPE_BLOB
            if pt.startswith("timestamp"):
                # python-oracledb binds datetime as DATE by default => fractional seconds would be lost
                return oracledb.DB_TYPE_TIMESTAMP
            return None
        sizes = [size(ptypes[c]) for c in cols]
        if any(s is not None for s in sizes):
            cur.setinputsizes(*sizes)

    def limit(self, n: int) -> str:
        return f" FETCH FIRST {int(n)} ROWS ONLY"

    def view_sql(self, vfqn: str, select: str) -> list[str]:
        return [f"CREATE OR REPLACE VIEW {vfqn} AS {select}"]

    def adapt(self, v: Any, ct: CanonicalType) -> Any:
        v = super().adapt(v, ct)
        if v is None:
            return None
        if ct is CanonicalType.LOCALTIME and isinstance(v, time):
            return v.isoformat()
        if ct is CanonicalType.INSTANT:
            return self.utc_naive(v)
        if isinstance(v, bool):
            return int(v)
        return v


# ====================================================================== Db2 LUW
class Db2Dialect(Dialect):
    def alter_type_sql(self, fqn: str, name: str, ptype: str, nullable: bool) -> list[str]:
        if ptype.lower().startswith(("clob", "blob", "dbclob")):
            return []
        return [f"ALTER TABLE {fqn} ALTER COLUMN {self.q(name)} SET DATA TYPE {ptype}",
                f"CALL SYSPROC.ADMIN_CMD('REORG TABLE {fqn.replace(chr(39), chr(39) * 2)}')"]

    name = "db2"
    driver = "ibm_db"
    paramstyle = "qmark"
    fold = "upper"
    upsert = "merge"
    merge_cast = True
    multirow = True              # multi-row MERGE ... USING (VALUES ...): ibm_db execute_many is avoided
    max_rows_per_stmt = 200
    max_params = 30_000
    lob_prefixes = ("clob", "blob", "dbclob", "xml")

    def executemany(self, cur: Any, sql: str, seq: list[tuple[Any, ...]]) -> None:
        for p in seq:                   # ibm_db execute_many crashes on some type mixes (seen on 3.3.0 / Windows)
            cur.execute(sql, p)

    def connect(self, schemas: list[str]) -> Any:
        """Local catalogued alias (`db2:///<ALIAS>`) -> Db2 CLI via ctypes (alelt_dst_sql.db2cli, SQLConnect);
        `db2://user:pw@host:port/DB` or `?driver=ibm_db` -> ibm_db (Apache-2.0) with its own CLI driver.
        The two must not be mixed in one process (ibm_db crashes when the local SQLLIB DLLs are loaded)."""
        from .. import db2cli

        s = self.settings
        if _opt(self, "driver", "") != "ibm_db" and not s.host and db2cli.available():
            return db2cli.connect(s.database or "", s.user, s.password)
        import ibm_db_dbi

        if s.host:
            dsn = (f"DATABASE={s.database};HOSTNAME={s.host};PORT={s.port or 50000};PROTOCOL=TCPIP;"
                   f"UID={s.user or ''};PWD={s.password or ''};CONNECTTIMEOUT={_T};")
            conn = ibm_db_dbi.connect(dsn, "", "")
        else:
            conn = ibm_db_dbi.connect(s.database or "", s.user or "", s.password or "")
        conn.set_autocommit(False)
        return conn

    def version(self, cur: Any) -> str:
        cur.execute("SELECT service_level FROM TABLE(sysproc.env_get_inst_info()) AS t")
        return "Db2 " + str(cur.fetchone()[0])

    def create_schema(self, cur: Any, schema: str) -> None:
        cur.execute("SELECT count(*) FROM syscat.schemata WHERE schemaname = ?", (self.norm(schema),))
        if not cur.fetchone()[0]:
            cur.execute(f"CREATE SCHEMA {self.q(schema)}")

    def drop_schema(self, cur: Any, schema: str) -> None:
        s = self.norm(schema)
        cur.execute("SELECT tabname, type FROM syscat.tables WHERE tabschema = ?", (s,))
        for name, typ in cur.fetchall():
            cur.execute(f"DROP {'VIEW' if typ == 'V' else 'TABLE'} {self.fq(schema, name)}")
        cur.execute("SELECT count(*) FROM syscat.schemata WHERE schemaname = ?", (s,))
        if cur.fetchone()[0]:
            cur.execute(f"DROP SCHEMA {self.q(schema)} RESTRICT")

    def describe(self, cur: Any, schema: str, table: str) -> list[dict[str, Any]]:
        cur.execute("SELECT colname, typename, length, scale, nulls FROM syscat.columns WHERE tabschema = ? AND "
                    "tabname = ? ORDER BY colno", (self.norm(schema), self.norm(table)))
        out = []
        for n, t, ln, sc, nl in cur.fetchall():
            t = t.strip().lower()
            if t in ("varchar", "character", "varbinary", "binary"):
                t = f"{'char' if t == 'character' else t}({ln})"
            elif t == "decimal":
                t = f"decimal({ln},{sc})"
            elif t == "timestamp":
                t = f"timestamp({sc})"
            out.append({"name": n.strip(), "type": t, "nullable": nl == "Y"})
        return out

    def tables_sql(self) -> str:
        return "SELECT tabname FROM syscat.tables WHERE tabschema = ? AND type = 'T' ORDER BY tabname"

    def upsert_sql(self, fqn: str, cols: list[str], keys: list[str], update: list[str],
                   ptypes: dict[str, str], rows: int = 1) -> str:
        return self.merge_sql(fqn, cols, keys, update, ptypes, rows)

    def limit(self, n: int) -> str:
        return f" FETCH FIRST {int(n)} ROWS ONLY"

    def adapt(self, v: Any, ct: CanonicalType) -> Any:
        v = super().adapt(v, ct)
        if v is None:
            return None
        if ct is CanonicalType.INSTANT:
            return self.utc_naive(v)
        if ct is CanonicalType.BOOLEAN:
            return bool(v)
        if ct is CanonicalType.LOCALTIME and isinstance(v, time):
            return v.replace(microsecond=0)               # Db2 TIME has whole seconds
        if ct is CanonicalType.DECIMAL and isinstance(v, (int, float)):
            return Decimal(str(v))
        return v


# ====================================================================== SQLite
class SqliteDialect(Dialect):
    def alter_type_sql(self, fqn: str, name: str, ptype: str, nullable: bool) -> list[str]:
        return []                                      # dynamic typing: declared lengths are not enforced

    """One file per schema: `<dir>/<stem>.<schema>.db` ATTACHed under the schema name (atomic multi-file commit
    with the default rollback journal; WAL mode is not used for that reason)."""

    name = "sqlite"
    driver = "sqlite3"
    paramstyle = "qmark"
    max_params = 30_000

    def _path(self) -> Path:
        p = self.settings.path or "alelt.db"
        if len(p) > 2 and p[0] == "/" and p[2] == ":":   # sqlite:///C:/x.db
            p = p[1:]
        return Path(p)

    def connect(self, schemas: list[str]) -> Any:
        import sqlite3

        main = self._path()
        main.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(main), isolation_level=None, timeout=float(_opt(self, "timeout", "30")))
        conn.execute("PRAGMA journal_mode = DELETE")
        for s in dict.fromkeys(map(self.norm, schemas)):
            conn.execute("ATTACH DATABASE ? AS " + self.q(s), (str(main.with_name(f"{main.stem}.{s}.db")),))
        return conn

    def begin(self, conn: Any) -> None:
        conn.execute("BEGIN IMMEDIATE")

    def version(self, cur: Any) -> str:
        cur.execute("SELECT 'SQLite ' || sqlite_version()")
        return str(cur.fetchone()[0])

    def create_schema(self, cur: Any, schema: str) -> None:
        """Schemas are attached files (connect())."""

    def drop_schema(self, cur: Any, schema: str) -> None:
        cur.execute(f"SELECT name, type FROM {self.q(schema)}.sqlite_master WHERE type IN ('table', 'view')")
        for name, typ in cur.fetchall():
            cur.execute(f"DROP {typ.upper()} IF EXISTS {self.fq(schema, name)}")

    def describe(self, cur: Any, schema: str, table: str) -> list[dict[str, Any]]:
        cur.execute(f"PRAGMA {self.q(schema)}.table_info({self.q(table)})")
        return [{"name": r[1], "type": str(r[2]).lower(), "nullable": not r[3] and not r[5]} for r in cur.fetchall()]

    def list_tables(self, cur: Any, schema: str) -> list[str]:
        cur.execute(f"SELECT name FROM {self.q(schema)}.sqlite_master WHERE type = 'table' ORDER BY name")
        return [r[0] for r in cur.fetchall()]

    def view_sql(self, vfqn: str, select: str) -> list[str]:
        return [f"DROP VIEW IF EXISTS {vfqn}", f"CREATE VIEW {vfqn} AS {select}"]

    def adapt(self, v: Any, ct: CanonicalType) -> Any:
        v = super().adapt(v, ct)
        if v is None:
            return None
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, float) and v != v:
            # SQLite turns a bound NaN into NULL: refuse loudly instead of storing something else (XC-31)
            raise ValueError("SQLite cannot store a floating-point NaN (it would become NULL)")
        if isinstance(v, Decimal):
            return format(v, "f")
        if isinstance(v, datetime):
            if v.tzinfo is not None:
                v = v.astimezone(timezone.utc)
            return v.isoformat(sep=" ")
        if isinstance(v, (date, time)):
            return v.isoformat()
        return v


# ====================================================================== DuckDB
class DuckDBDialect(Dialect):
    """DuckDB file database. The connection object is used as cursor: `con.cursor()` opens a second connection
    with its own transaction context, which would break apply+checkpoint atomicity."""

    name = "duckdb"
    driver = "duckdb"
    paramstyle = "qmark"
    bulk_rows = 2_000

    def connect(self, schemas: list[str]) -> Any:
        import duckdb

        p = self.settings.path or "alelt.duckdb"
        if len(p) > 2 and p[0] == "/" and p[2] == ":":
            p = p[1:]
        Path(p).parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(p)
        try:        # TIMESTAMPTZ values come back in UTC (a local zone overflows near 9999-12-31, XC-31)
            con.execute("SET TimeZone = 'UTC'")
        except Exception:  # noqa: BLE001 - DuckDB build without ICU: values are already UTC
            pass
        return con

    def cursor(self, conn: Any) -> Any:
        return conn

    def begin(self, conn: Any) -> None:
        conn.execute("BEGIN TRANSACTION")

    def tables_sql(self) -> str:
        return ("SELECT table_name FROM information_schema.tables WHERE table_schema = ? "
                "AND table_type = 'BASE TABLE' ORDER BY table_name")


DIALECTS: dict[str, type[Dialect]] = {
    d.name: d for d in (PostgresDialect, MySQLDialect, MariaDBDialect, SqlServerDialect, OracleDialect, Db2Dialect,
                        SqliteDialect, DuckDBDialect)
}

ALIASES = {
    "postgresql": "postgres", "postgres": "postgres", "pg": "postgres", "psql": "postgres", "cockroachdb": "postgres",
    "cockroach": "postgres", "yugabyte": "postgres", "yugabytedb": "postgres", "alloydb": "postgres",
    "aurora-postgresql": "postgres", "mysql": "mysql", "tidb": "mysql", "aurora-mysql": "mysql", "mariadb": "mariadb",
    "mssql": "sqlserver", "sqlserver": "sqlserver", "azuresql": "sqlserver", "tds": "sqlserver", "oracle": "oracle",
    "db2": "db2", "ibm_db": "db2", "ibmdb2": "db2", "sqlite": "sqlite", "duckdb": "duckdb",
}


def dialect_name(scheme: str) -> str:
    s = scheme.lower().split("+", 1)
    if s[0] == "sql" and len(s) > 1:              # sql+mysql://
        s = [s[1]]
    try:
        return ALIASES[s[0]]
    except KeyError:
        raise ValueError(f"unknown SQL dialect {scheme!r}; known: {sorted(DIALECTS)}") from None
