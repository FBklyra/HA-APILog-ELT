"""MERGE + checkpoint for SQL warehouses (Redshift, Fabric/Synapse, BigQuery, Databricks SQL).

One transaction per batch (dialect permitting):
  [lock checkpoint row] -> per table: truncate? -> per op group: stage rows -> MERGE (soft delete / upsert) or
  DELETE (hard delete) -> checkpoint (delete + insert) -> audit row -> COMMIT.
Op groups = (upsert|delete, set of columns present): a partial image updates only the columns it carries, so no
read-back is needed. Staging: `temp` = temp table + parameterised executemany (Redshift, T-SQL); `inline` =
literal SELECT ... UNION ALL source (BigQuery, Databricks: one script / no driver-side parameters needed).

Executors are injected (tests use RecordingExecutor); the generator is pure and unit-tested per dialect.
"""
from __future__ import annotations

import contextlib
import json
from collections.abc import Callable, Iterator
from datetime import date, datetime, time as dtime, timezone
from decimal import Decimal
from typing import Any

from alelt_common.models import CanonicalType, ColumnInfo, Position, TableInfo
from alelt_common.spi import ApplyStats, CheckpointConflictError

from .base import AlreadyApplied, ControlState, KitDestination, wm_loads
from .plan import RowOp, TablePlan
from .types import physical_type, type_map

import logging

log = logging.getLogger("alelt_dst_kit.sqlmerge")


# ---------------------------------------------------------------------------- executors
CAS_MARK = "alelt checkpoint conflict"     # message of the in-script assertion (engines without rowcount)


class Tx:
    def execute(self, sql: str, params: Any = None) -> int | None:
        """Returns the affected row count when the driver reports one, else None."""
    def executemany(self, sql: str, rows: list[Any]) -> None: ...
    def query(self, sql: str, params: Any = None) -> tuple[list[str], list[tuple[Any, ...]]]: ...


class Executor:
    """transaction() yields a Tx; query()/execute() run outside a transaction (autocommit)."""

    supports_query_in_tx = True

    @contextlib.contextmanager
    def transaction(self) -> Iterator[Tx]:  # pragma: no cover
        raise NotImplementedError
        yield

    def query(self, sql: str, params: Any = None) -> tuple[list[str], list[tuple[Any, ...]]]:  # pragma: no cover
        raise NotImplementedError

    def execute(self, sql: str, params: Any = None) -> None:  # pragma: no cover
        raise NotImplementedError

    def close(self) -> None:
        pass


def _json_strict(o: Any) -> Any:
    """JSON for canonical values only (DST-kit-14): Decimal / temporal as text, anything else is an error."""
    if isinstance(o, Decimal):
        return format(o, "f")
    if isinstance(o, (datetime, date, dtime)):
        return o.isoformat()
    if isinstance(o, (bytes, bytearray)):
        return bytes(o).hex()
    raise TypeError(f"value of type {type(o).__name__} has no JSON form")


def _is_disconnect(e: BaseException) -> bool:
    """DB-API OperationalError / InterfaceError or a socket-level error: the cached connection is unusable."""
    names = {c.__name__ for c in type(e).__mro__}
    return bool(names & {"OperationalError", "InterfaceError", "ConnectionError", "BrokenPipeError", "OSError"})


class DbapiExecutor(Executor):
    """DB-API 2 connection factory (psycopg, redshift_connector, pyodbc)."""

    def __init__(self, connect: Callable[[], Any]) -> None:
        self._connect = connect
        self._conn: Any = None

    @property
    def conn(self) -> Any:
        if self._conn is None:
            self._conn = self._connect()
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            with contextlib.suppress(Exception):
                self._conn.close()
            self._conn = None

    _drop = close

    class _T(Tx):
        def __init__(self, cur: Any) -> None:
            self.cur = cur

        def execute(self, sql: str, params: Any = None) -> int | None:
            self.cur.execute(sql, params) if params is not None else self.cur.execute(sql)
            rc = getattr(self.cur, "rowcount", None)
            return int(rc) if isinstance(rc, int) and rc >= 0 else None

        def executemany(self, sql: str, rows: list[Any]) -> None:
            if not rows:
                return
            fast = getattr(self.cur, "fast_executemany", None)
            if fast and any(isinstance(v, (bytes, bytearray, str)) and len(v) > 255 for r in rows for v in r):
                # pyodbc fast_executemany sizes (max) parameters at 255 (DST-fabric-04): row-wise binding instead
                self.cur.fast_executemany = False
                try:
                    self.cur.executemany(sql, rows)
                finally:
                    self.cur.fast_executemany = True
                return
            if fast and hasattr(self.cur, "setinputsizes"):
                # pyodbc fast_executemany drops fractional seconds of datetime2 unless sized explicitly
                ncol = len(rows[0])
                ts = [any(isinstance(r[i], datetime) for r in rows) for i in range(ncol)]
                if any(ts):
                    import pyodbc

                    self.cur.setinputsizes([(pyodbc.SQL_TYPE_TIMESTAMP, 26, 6) if t else None for t in ts])
                    try:
                        self.cur.executemany(sql, rows)
                    finally:
                        self.cur.setinputsizes(None)          # sizes stick to the cursor otherwise
                    return
            self.cur.executemany(sql, rows)

        def query(self, sql: str, params: Any = None) -> tuple[list[str], list[tuple[Any, ...]]]:
            self.execute(sql, params)
            names = [d[0] for d in self.cur.description or []]
            return names, [tuple(r) for r in self.cur.fetchall()]

    @contextlib.contextmanager
    def transaction(self) -> Iterator[Tx]:
        conn = self.conn
        cur = conn.cursor()
        if hasattr(cur, "fast_executemany"):
            cur.fast_executemany = True
        try:
            yield self._T(cur)
            conn.commit()
        except BaseException as e:
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001 - rollback failed: the connection is broken (DST-kit-11)
                self._drop()
            else:
                if _is_disconnect(e) or getattr(conn, "closed", 0):
                    self._drop()                      # reconnect on the next transaction, not on worker restart
            raise
        finally:
            with contextlib.suppress(Exception):
                cur.close()

    def query(self, sql: str, params: Any = None) -> tuple[list[str], list[tuple[Any, ...]]]:
        with self.transaction() as t:
            return t.query(sql, params)

    def execute(self, sql: str, params: Any = None) -> None:
        with self.transaction() as t:
            t.execute(sql, params)


class RecordingExecutor(Executor):
    """Captures statements (SQL generation tests). Query results come from `answers` (substring -> rows)."""

    def __init__(self, answers: dict[str, tuple[list[str], list[tuple[Any, ...]]]] | None = None,
                 rowcount: int | None = 1) -> None:
        self.log: list[tuple[str, Any]] = []
        self.committed: list[list[tuple[str, Any]]] = []
        self.answers = answers or {}
        self.rowcount = rowcount            # reported for every execute() inside a transaction

    def _answer(self, sql: str) -> tuple[list[str], list[tuple[Any, ...]]]:
        for k, v in self.answers.items():
            if k in sql:
                return v
        return [], []

    @contextlib.contextmanager
    def transaction(self) -> Iterator[Tx]:
        buf: list[tuple[str, Any]] = []
        rec = self

        class T(Tx):
            def execute(self, sql: str, params: Any = None) -> int | None:
                buf.append((sql, params))
                return rec.rowcount

            def executemany(self, sql: str, rows: list[Any]) -> None:
                buf.append((sql, rows))

            def query(self, sql: str, params: Any = None) -> tuple[list[str], list[tuple[Any, ...]]]:
                buf.append((sql, params))
                return rec._answer(sql)
        yield T()
        self.committed.append(buf)
        self.log.extend(buf)

    def query(self, sql: str, params: Any = None) -> tuple[list[str], list[tuple[Any, ...]]]:
        self.log.append((sql, params))
        return self._answer(sql)

    def execute(self, sql: str, params: Any = None) -> None:
        self.log.append((sql, params))


# ---------------------------------------------------------------------------- dialect
class Dialect:
    name = "ansi"
    type_engine = "ansi"
    fallback: dict[str, Any] = {"types": {}}
    stage_mode = "temp"                 # temp | inline
    param = "%s"
    true, false = "TRUE", "FALSE"
    end = ""                            # statement terminator required by the dialect (T-SQL MERGE: ';')
    lock_checkpoint = True
    # checkpoint compare-and-set evidence: 'rowcount' (conditional UPDATE, driver row count), 'assert' (in-script
    # ASSERT, scripts without client round trips), 'readback' (autocommit engines: re-read after the write)
    cas_mode = "rowcount"

    def __init__(self, overrides: dict[str, str] | None = None) -> None:
        self.overrides = overrides or {}

    def q(self, ident: str) -> str:
        return '"' + ident.replace('"', '""') + '"'

    def table(self, schema: str, name: str) -> str:
        return f"{self.q(schema)}.{self.q(name)}"

    def ptype(self, col: ColumnInfo) -> str:
        return physical_type(col, type_map(self.type_engine, self.fallback), self.overrides)

    # DDL
    def create_schema(self, schema: str) -> list[str]:
        return [f"CREATE SCHEMA IF NOT EXISTS {self.q(schema)}"]

    def create_table(self, fqn: str, coldefs: list[str], keys: list[str]) -> str:
        pk = f", PRIMARY KEY ({', '.join(map(self.q, keys))})" if keys else ""
        return f"CREATE TABLE IF NOT EXISTS {fqn} ({', '.join(coldefs)}{pk})"

    def columns_sql(self, schema: str, table: str) -> tuple[str, Any]:
        p = self.param
        return (f"SELECT column_name FROM information_schema.columns WHERE table_schema = {p} AND table_name = {p}",
                (schema, table))

    def add_column(self, fqn: str, col: str, typ: str) -> str:
        return f"ALTER TABLE {fqn} ADD COLUMN {self.q(col)} {typ}"

    def describe_sql(self, schema: str, table: str) -> tuple[str, Any]:
        """(sql, params) -> rows (column_name, data_type, is_nullable) in ordinal order."""
        p = self.param
        return ("SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                f"WHERE table_schema = {p} AND table_name = {p} ORDER BY ordinal_position", (schema, table))

    def tables_sql(self, schema: str) -> tuple[str, Any]:
        return (f"SELECT table_name FROM information_schema.tables WHERE table_schema = {self.param} "
                "ORDER BY table_name", (schema,))

    def widen_sql(self, fqn: str, col: str, typ: str, nullable: bool = True) -> list[str]:
        """Statements widening a column in place (drift, control columns of an older release); [] = unsupported."""
        return [f"ALTER TABLE {fqn} ALTER COLUMN {self.q(col)} TYPE {typ}"]

    def types_sql(self, schema: str, table: str) -> tuple[str, Any] | None:
        """(sql, params) -> rows (column_name, data_type, char_max_length, numeric_precision, numeric_scale);
        None = no type introspection (no widening)."""
        p = self.param
        return ("SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale FROM "
                f"information_schema.columns WHERE table_schema = {p} AND table_name = {p}", (schema, table))

    def assert_sql(self, cond: str, message: str) -> str:
        return f"ASSERT {cond} AS {self.string_lit(message)}"

    def param_for(self, typ: str) -> str:
        """Bind marker for a stage column of physical type `typ` (Redshift: JSON_PARSE(%s) into SUPER)."""
        return self.param

    def temp_table(self, name: str, coldefs: list[str]) -> tuple[list[str], str]:
        """(statements creating it, reference)."""
        return [f"DROP TABLE IF EXISTS {self.q(name)}", f"CREATE TEMP TABLE {self.q(name)} ({', '.join(coldefs)})"], \
            self.q(name)

    def drop_temp(self, ref: str) -> list[str]:
        return [f"DROP TABLE IF EXISTS {ref}"]

    def lock(self, ckpt: str) -> list[str]:
        return [f"LOCK TABLE {ckpt}"] if self.lock_checkpoint else []

    def bool_cmp(self, col: str, value: bool) -> str:
        return f"{col} = {self.true if value else self.false}"

    # literals (inline staging)
    def string_lit(self, s: str) -> str:
        return "'" + s.replace("'", "''") + "'"

    def bytes_lit(self, b: bytes) -> str:
        return f"X'{b.hex()}'"

    def cast_type(self, typ: str) -> str:
        """Type used inside CAST() (some engines reject parameterised types there)."""
        return typ

    def lit(self, v: Any, typ: str, ct: CanonicalType | None = None) -> str:
        typ = self.cast_type(typ)
        if v is None:
            return f"CAST(NULL AS {typ})"
        if isinstance(v, bool):
            return f"CAST({self.true if v else self.false} AS {typ})"
        if isinstance(v, (bytes, bytearray)):
            return self.bytes_lit(bytes(v))
        if isinstance(v, int):
            return f"CAST({v} AS {typ})"
        if isinstance(v, Decimal):
            return f"CAST({self.string_lit(format(v, 'f'))} AS {typ})"
        if isinstance(v, datetime):
            if v.tzinfo is not None:
                v = v.astimezone(timezone.utc)
                return f"CAST({self.string_lit(v.strftime('%Y-%m-%d %H:%M:%S.%f+00:00'))} AS {typ})"
            return f"CAST({self.string_lit(v.strftime('%Y-%m-%d %H:%M:%S.%f'))} AS {typ})"
        if isinstance(v, (date, dtime)):
            return f"CAST({self.string_lit(v.isoformat())} AS {typ})"
        if isinstance(v, float):
            if v != v or v in (float("inf"), float("-inf")):      # DST-kit-14: never invalid SQL
                return self.float_special(v, typ)
            return f"CAST({v!r} AS {typ})"
        if not isinstance(v, str):
            v = json.dumps(v, separators=(",", ":"), default=_json_strict)
        return f"CAST({self.string_lit(v)} AS {typ})"

    def float_special(self, v: float, typ: str) -> str:
        """NaN / +-Infinity as a typed literal (engines accept the text form in CAST); dialects override."""
        txt = "NaN" if v != v else ("Infinity" if v > 0 else "-Infinity")
        return f"CAST({self.string_lit(txt)} AS {typ})"

    def param_value(self, v: Any, ct: CanonicalType | None) -> Any:
        if ct is CanonicalType.JSON and v is not None and not isinstance(v, str):
            return json.dumps(v, separators=(",", ":"), default=_json_strict)
        return v

    def merge(self, target: str, source: str, keys: list[str], cols: list[str], synced: str, deleted: str,
              soft_delete: bool) -> str:
        q = self.q
        on = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in keys)
        if soft_delete:
            upd = f"{q(deleted)} = {self.true}, {q(synced)} = s.{q(synced)}"
            flag = self.true
        else:
            upd = ", ".join([*(f"{q(c)} = s.{q(c)}" for c in cols if c not in keys),
                             f"{q(synced)} = s.{q(synced)}", f"{q(deleted)} = {self.false}"])
            flag = self.false
        ins = ", ".join(map(q, [*cols, synced, deleted]))
        vals = ", ".join([*(f"s.{q(c)}" for c in cols), f"s.{q(synced)}", flag])
        return (f"MERGE INTO {target} AS t USING {source} AS s ON {on} "
                f"WHEN MATCHED THEN UPDATE SET {upd} "
                f"WHEN NOT MATCHED THEN INSERT ({ins}) VALUES ({vals}){self.end}")

    def hard_delete(self, target: str, source: str, keys: list[str]) -> str:
        on = " AND ".join(f"s.{self.q(k)} = {target}.{self.q(k)}" for k in keys)
        return f"DELETE FROM {target} WHERE EXISTS (SELECT 1 FROM {source} AS s WHERE {on}){self.end}"


# ---------------------------------------------------------------------------- destination
class SqlMergeDestination(KitDestination):
    """KitDestination for SQL engines with MERGE. Subclasses set `dialect` and build `self.ex`."""

    engine = "sqlmerge"
    chunk_rows = 500                     # rows per inline source / executemany chunk
    chunk_chars = 0                      # >0: also cap the rendered size of one statement
    atomic_tables = True
    verify_after = False                 # read the checkpoint back after commit (script engines w/o ASSERT)                 # False: engine commits per statement (Databricks) - per-table idempotence

    def __init__(self, url: str = "", schema: str = "landing", control_schema: str = "alelt_control",
                 output_profile: str | None = None, compare: Any = None, executor: Executor | None = None,
                 dialect: Dialect | None = None, **kw: Any) -> None:
        super().__init__(url, schema, control_schema, output_profile, compare, **kw)
        self.d: Dialect = dialect or self.make_dialect()
        self._ex = executor
        self._cols: dict[str, set[str]] = {}
        self._types: dict[str, dict[str, str]] = {}

    # subclasses
    def make_dialect(self) -> Dialect:
        return Dialect()

    def make_executor(self) -> Executor:  # pragma: no cover
        raise NotImplementedError

    @property
    def ex(self) -> Executor:
        if self._ex is None:
            self._ex = self.make_executor()
        return self._ex

    def close(self) -> None:
        if self._ex is not None:
            self._ex.close()

    def t(self, dest: str) -> str:
        return self.d.table(self.schema, self.ident(dest))

    def c(self, name: str) -> str:
        return self.d.table(self.control, self.ident(name))

    # ------------------------------------------------------------------ DDL
    def control_ddl(self) -> list[str]:
        d = self.d
        # rows_by_table is unbounded (DST-redshift-01 / DST-fabric-01: > ~80 tables per batch overflowed 4000)
        s, i, b, ts = (d.ptype(_col(CanonicalType.STRING, 1000)), d.ptype(_col(CanonicalType.LONG)),
                       self._wm_type(), d.ptype(_col(CanonicalType.INSTANT)))
        return [
            d.create_table(self.c("checkpoint"), [f"{d.q('stream')} {s} NOT NULL", f"{d.q('seq')} {i} NOT NULL",
                                                  f"{d.q('position')} {s} NOT NULL", f"{d.q('batch_id')} {s}",
                                                  f"{d.q('updated_at')} {ts}", f"{d.q('watermarks')} {self._wm_type()}"],
                           ["stream"]),
            d.create_table(self.c("audit_batch"), [
                f"{d.q('batch_id')} {s} NOT NULL", f"{d.q('stream')} {s} NOT NULL", f"{d.q('seq')} {i}",
                f"{d.q('from_position')} {s}", f"{d.q('to_position')} {s}", f"{d.q('rows_by_table')} {b}",
                f"{d.q('total_rows')} {i}", f"{d.q('status')} {s}", f"{d.q('applied_at')} {ts}"], []),
        ]

    def _wm_type(self) -> str:
        return self.d.ptype(_col(CanonicalType.STRING))

    def bootstrap(self) -> None:
        for st in [*self.d.create_schema(self.schema), *self.d.create_schema(self.control), *self.control_ddl()]:
            self.ex.execute(st)
        ctl = self._on_schema(self.control)
        if "watermarks" not in ctl._existing("checkpoint"):          # control tables of an older release
            self.ex.execute(self.d.add_column(self.c("checkpoint"), "watermarks", self._wm_type()))
        for st in self.d.widen_sql(self.c("audit_batch"), "rows_by_table", self._wm_type()):
            with contextlib.suppress(Exception):                      # older release: VARCHAR(4000)
                self.ex.execute(st)

    def table_ddl(self, table: TableInfo, dest: str) -> list[str]:
        d = self.d
        keyless = not table.key_columns()
        cols = self.data_cols(table, keyless)
        defs = [f"{d.q(self.ident(c.name))} {d.ptype(c)}{' NOT NULL' if c.key_seq else ''}" for c in cols]
        defs += [f"{d.q(self.c_synced)} {d.ptype(_col(CanonicalType.INSTANT))} NOT NULL",
                 f"{d.q(self.c_deleted)} {d.ptype(_col(CanonicalType.BOOLEAN))} NOT NULL"]
        keys = [self.ident(k) for k in self.key_names(table)]
        return [*d.create_schema(self.schema), d.create_table(self.t(dest), defs, keys)]

    def ensure_table(self, table: TableInfo, dest_name: str, golden_ddl: str | None) -> None:
        if golden_ddl:
            self.ex.execute(golden_ddl)
        for st in self.table_ddl(table, dest_name):
            self.ex.execute(st)
        have = self._existing(dest_name)
        for c in table.columns:
            if self.ident(c.name).lower() not in have:
                self.add_column(dest_name, c)
        self._widen(table, dest_name)
        self._remember(table, dest_name)

    def _column_types(self, dest: str) -> dict[str, str]:
        q = self.d.types_sql(self.schema, self.ident(dest))
        if q is None:
            return {}
        out: dict[str, str] = {}
        for name, dt, ln, prec, sc in self.ex.query(*q)[1]:
            t = str(dt).lower()
            if "(" not in t and ln not in (None, -1) and any(x in t for x in ("char", "binary", "varbyte")):
                t += f"({ln})"
            elif "(" not in t and prec is not None and any(x in t for x in ("numeric", "decimal")):
                t += f"({prec},{sc or 0})"
            elif "(" not in t and ln == -1:
                t += "(max)"
            out[str(name).lower()] = t
        return out

    def _widen(self, table: TableInfo, dest: str) -> None:
        """Type widening on drift (DST-kit-04): ALTER columns narrower than the current catalog's type."""
        from .widen import widen_to

        try:
            have = self._column_types(dest)
        except Exception as e:  # noqa: BLE001 - introspection unavailable: additive drift only
            log.warning("no type introspection for %s: %s", dest, e)
            return
        for c in table.columns:
            cur = have.get(self.ident(c.name).lower())
            to = widen_to(cur, self.d.ptype(c)) if cur else None
            if not to:
                continue
            for st in self.d.widen_sql(self.t(dest), self.ident(c.name), to, nullable=not c.key_seq):
                try:
                    self.ex.execute(st)
                    log.info("widened %s.%s to %s", dest, c.name, to)
                except Exception as e:  # noqa: BLE001 - reported; longer values then fail loudly
                    log.warning("cannot widen %s.%s to %s: %s", dest, c.name, to, e)

    def _remember(self, table: TableInfo, dest: str) -> None:
        keyless = not table.key_columns()
        self._types[self.ident(dest)] = {c.name: self.d.ptype(c) for c in self.data_cols(table, keyless)}

    def _existing(self, dest: str) -> set[str]:
        sql, params = self.d.columns_sql(self.schema, self.ident(dest))
        _, rows = self.ex.query(sql, params)
        return {str(r[0]).lower() for r in rows}

    def add_column(self, dest_name: str, col: ColumnInfo) -> None:
        if self.ident(col.name).lower() in self._existing(dest_name):     # DST-kit-12: case=upper too
            return
        self.ex.execute(self.d.add_column(self.t(dest_name), self.ident(col.name), self.d.ptype(col)))

    # ------------------------------------------------------------------ control
    def _state_sql(self) -> str:
        q = self.d.q
        return (f"SELECT {q('seq')}, {q('position')}, {q('batch_id')}, {q('watermarks')} "
                f"FROM {self.c('checkpoint')} WHERE {q('stream')} = {self.d.param}")

    @staticmethod
    def _state(rows: list[tuple[Any, ...]]) -> ControlState:
        if not rows:
            return ControlState()
        r = max(rows, key=lambda x: int(x[0] or 0))
        return ControlState(r[1] or None, int(r[0] or 0), r[2] or None, wm_loads(r[3]))

    def control_state(self, stream: str) -> ControlState:
        _, rows = self.ex.query(self._state_sql(), (stream,))
        return self._state(rows)

    def checkpoint_sql(self, stream: str, seq: int, to_pos: Position, batch_id: str, ts: datetime,
                       prev: ControlState | None = None) -> list[tuple[str, Any]]:
        """Conditional checkpoint write (compare-and-set on seq): UPDATE ... WHERE stream AND seq = prev.seq, or the
        first INSERT. Exactly one row must be affected (row count / ASSERT / read-back per `cas_mode`)."""
        d, p, q = self.d, self.d.param, self.d.q
        prev = prev or ControlState(seq=seq - 1)
        cols = ", ".join(map(q, ["stream", "seq", "position", "batch_id", "updated_at", "watermarks"]))
        wm = self.wm_text()
        if d.stage_mode == "inline":
            tsl = d.lit(ts, d.ptype(_col(CanonicalType.INSTANT)))
            if prev.seq == 0:
                return [(f"INSERT INTO {self.c('checkpoint')} ({cols}) VALUES ({d.string_lit(stream)}, {seq}, "
                         f"{d.string_lit(to_pos.text)}, {d.string_lit(batch_id)}, {tsl}, {d.string_lit(wm)})", None)]
            return [(f"UPDATE {self.c('checkpoint')} SET {q('seq')} = {seq}, {q('position')} = "
                     f"{d.string_lit(to_pos.text)}, {q('batch_id')} = {d.string_lit(batch_id)}, {q('updated_at')} = "
                     f"{tsl}, {q('watermarks')} = {d.string_lit(wm)} WHERE {q('stream')} = {d.string_lit(stream)} "
                     f"AND {q('seq')} = {prev.seq}", None)]
        if prev.seq == 0:
            return [(f"INSERT INTO {self.c('checkpoint')} ({cols}) VALUES ({p}, {p}, {p}, {p}, {p}, {p})",
                     (stream, seq, to_pos.text, batch_id, ts, wm))]
        return [(f"UPDATE {self.c('checkpoint')} SET {q('seq')} = {p}, {q('position')} = {p}, {q('batch_id')} = {p}, "
                 f"{q('updated_at')} = {p}, {q('watermarks')} = {p} WHERE {q('stream')} = {p} AND {q('seq')} = {p}",
                 (seq, to_pos.text, batch_id, ts, wm, stream, prev.seq))]

    def delete_checkpoint(self, stream: str) -> None:
        d = self.d
        p = d.string_lit(stream) if d.stage_mode == "inline" else d.param
        self.ex.execute(f"DELETE FROM {self.c('checkpoint')} WHERE {d.q('stream')} = {p}",
                        None if d.stage_mode == "inline" else (stream,))

    def cas_assert_sql(self, stream: str, prev: ControlState) -> str:
        """In-script guard (cas_mode 'assert'): the checkpoint row is still the one the batch was planned from."""
        d, q = self.d, self.d.q
        cond = (f"(SELECT COUNT(*) FROM {self.c('checkpoint')} WHERE {q('stream')} = {d.string_lit(stream)}"
                + (f" AND {q('seq')} = {prev.seq}) = 1" if prev.seq else ") = 0"))
        return d.assert_sql(cond, CAS_MARK)

    def _conflict(self, stream: str, prev: ControlState, to_pos: Position, detail: str, force: bool = False
                  ) -> Exception | None:
        """After a failed guard / driver conflict: the same batch committed meanwhile (AlreadyApplied), a real
        conflict, or None (checkpoint unchanged and not a guard failure: the original error stands)."""
        found = self.control_state(stream)
        if self.pos_ge(found.position, to_pos):
            return AlreadyApplied()
        if not force and self.same_state(prev, found):
            return None
        return CheckpointConflictError(stream, prev.position, found.position, detail)

    def audit_sql(self, a: dict[str, Any], ts: datetime) -> tuple[str, Any]:
        d, p = self.d, self.d.param
        names = ["batch_id", "stream", "seq", "from_position", "to_position", "rows_by_table", "total_rows",
                 "status", "applied_at"]
        vals = [a["batch_id"], a["stream"], a["seq"], a["from_position"], a["to_position"], a["rows_by_table"],
                a["total_rows"], a["status"], ts]
        cols = ", ".join(map(d.q, names))
        if d.stage_mode == "inline":
            s, i = d.ptype(_col(CanonicalType.STRING, 4000)), d.ptype(_col(CanonicalType.LONG))
            lits = [d.lit(v, i if isinstance(v, int) else (d.ptype(_col(CanonicalType.INSTANT))
                                                         if isinstance(v, datetime) else s)) for v in vals]
            return f"INSERT INTO {self.c('audit_batch')} ({cols}) VALUES ({', '.join(lits)})", None
        return f"INSERT INTO {self.c('audit_batch')} ({cols}) VALUES ({', '.join([p] * len(vals))})", tuple(vals)

    # ------------------------------------------------------------------ apply
    def commit_batch(self, stream: str | None, plans: list[TablePlan], to_pos: Position | None, batch_id: str,
                     prev: ControlState, ts: datetime, stats: dict[str, ApplyStats]) -> None:
        """ONE transaction: [lock checkpoint + re-read + CAS] -> data -> conditional checkpoint write (exactly one
        row, evidence per `cas_mode`) -> audit -> COMMIT. A failed guard rolls everything back."""
        seq = prev.seq + 1
        if stream is not None and to_pos is not None and not self.ex.supports_query_in_tx:
            self.cas(stream, prev, self.control_state(stream), to_pos)      # pre-check (script engines)
        try:
            with self.ex.transaction() as tx:
                if stream is not None and to_pos is not None:
                    for st in (self.d.lock(self.c("checkpoint")) if self.d.lock_checkpoint else []):
                        tx.execute(st)
                    if self.ex.supports_query_in_tx:             # re-read under the lock / in the transaction
                        _, rows = tx.query(self._state_sql(), (stream,))
                        self.cas(stream, prev, self._state(rows), to_pos)
                    elif self.d.cas_mode == "assert":
                        tx.execute(self.cas_assert_sql(stream, prev))
                for i, pl in enumerate(plans):
                    self.count(pl, stats[pl.dest])
                    for sql, params, many in self.plan_sql(pl, ts, stream, seq, i):
                        tx.executemany(sql, params) if many else tx.execute(sql, params)
                self._hook("after_data")
                if stream is not None and to_pos is not None:
                    for sql, params in self.checkpoint_sql(stream, seq, to_pos, batch_id, ts, prev):
                        n = tx.execute(sql, params)
                        if self.d.cas_mode == "rowcount" and n is not None and n < 1:
                            raise CheckpointConflictError(stream, prev.position, None, "checkpoint row changed")
                    sql, params = self.audit_sql(self.audit_record(stream, prev, to_pos, batch_id, stats), ts)
                    tx.execute(sql, params)
                self._hook("before_commit")
            if stream is not None and to_pos is not None and self.d.cas_mode == "assert" and self.verify_after:
                got = self.control_state(stream)                # engines without enforced ASSERT (emulators)
                if (got.position, got.batch_id) != (to_pos.text, batch_id):
                    raise CheckpointConflictError(stream, prev.position, got.position,
                                                  "checkpoint not ours after commit (guard not enforced)")
        except (AlreadyApplied, CheckpointConflictError):
            raise
        except Exception as e:
            if stream is not None and to_pos is not None and (CAS_MARK in str(e) or self.is_conflict_error(e)):
                c = self._conflict(stream, prev, to_pos, f"{type(e).__name__}: {str(e)[:200]}",
                                   force=CAS_MARK in str(e))
                if c is not None:
                    raise c from e
            raise

    @staticmethod
    def is_conflict_error(e: BaseException) -> bool:
        """Driver errors meaning 'a concurrent transaction won' (serialization / update conflict / duplicate)."""
        s = str(e).lower()
        return any(m in s for m in ("could not serialize", "serialization failure", "update conflict",
                                    "snapshot isolation", "deadlock", "concurrent update", "concurrent transaction",
                                    "duplicate key", "unique constraint", "violation of primary key"))

    def plan_sql(self, pl: TablePlan, ts: datetime, stream: str | None, seq: int, idx: int
                 ) -> list[tuple[str, Any, bool]]:
        """Statements for one table: (sql, params, executemany?)."""
        d = self.d
        target = self.t(pl.dest)
        out: list[tuple[str, Any, bool]] = []
        q = d.q
        if pl.truncate:
            if self.hard:
                out.append((f"DELETE FROM {target}{d.end}", None, False))
            else:
                tsv = d.lit(ts, d.ptype(_col(CanonicalType.INSTANT))) if d.stage_mode == "inline" else d.param
                out.append((f"UPDATE {target} SET {q(self.c_deleted)} = {d.true}, {q(self.c_synced)} = {tsv} "
                            f"WHERE {d.bool_cmp(q(self.c_deleted), False)}{d.end}",
                            None if d.stage_mode == "inline" else (ts,), False))
        types = self._types.get(self.ident(pl.dest)) or {c.name: d.ptype(c) for c in
                                                          self.data_cols(pl.info, pl.keyless)}
        cts = {c.name: c.canonical_type for c in pl.info.columns}
        groups: dict[tuple[str, tuple[str, ...]], list[RowOp]] = {}
        for op in pl.ops.values():
            cols = tuple(c for c in pl.columns if c in op.row)
            groups.setdefault((op.op, cols), []).append(op)
        keys = [self.ident(k) for k in pl.keys]
        for g, ((kind, cols), ops) in enumerate(groups.items()):
            pcols = [self.ident(c) for c in cols]
            for start, chunk in self.chunk_ops(ops, cols):
                src, pre, post = self.source(pl, cols, chunk, types, cts, ts, f"alelt_stage_{idx}_{g}_{start}")
                out.extend(pre)
                if kind == "D" and self.hard:
                    out.append((d.hard_delete(target, src, keys), None, False))
                else:
                    out.append((d.merge(target, src, keys, pcols, self.c_synced, self.c_deleted,
                                        soft_delete=kind == "D"), None, False))
                out.extend(post)
        return out

    def chunk_ops(self, ops: list[RowOp], cols: tuple[str, ...]) -> list[tuple[int, list[RowOp]]]:
        """Row chunks per statement: `chunk_rows` rows, and at most `chunk_chars` rendered characters when set
        (inline-literal engines with a statement size limit, e.g. BigQuery 1 MB)."""
        out: list[tuple[int, list[RowOp]]] = []
        cur: list[RowOp] = []
        size, start = 0, 0
        for i, op in enumerate(ops):
            est = 40 * (len(cols) + 1) + sum(len(str(op.row.get(c))) * 2 for c in cols) if self.chunk_chars else 0
            if cur and (len(cur) >= self.chunk_rows or (self.chunk_chars and size + est > self.chunk_chars)):
                out.append((start, cur))
                cur, size, start = [], 0, i
            cur.append(op)
            size += est
        if cur:
            out.append((start, cur))
        return out

    def source(self, pl: TablePlan, cols: tuple[str, ...], ops: list[RowOp], types: dict[str, str],
               cts: dict[str, CanonicalType], ts: datetime, name: str
               ) -> tuple[str, list[tuple[str, Any, bool]], list[tuple[str, Any, bool]]]:
        d = self.d
        tsty = d.ptype(_col(CanonicalType.INSTANT))
        names = [*(self.ident(c) for c in cols), self.c_synced]
        if d.stage_mode == "inline":
            rows = []
            for op in ops:
                lits = [d.lit(op.row.get(c), types[c], cts.get(c)) for c in cols] + [d.lit(ts, tsty)]
                rows.append("SELECT " + ", ".join(f"{v} AS {d.q(n)}" for v, n in zip(lits, names)))
            return "(" + " UNION ALL ".join(rows) + ")", [], []
        defs = [f"{d.q(self.ident(c))} {types[c]}" for c in cols] + [f"{d.q(self.c_synced)} {tsty}"]
        create, ref = d.temp_table(name, defs)
        marks = [d.param_for(types[c]) for c in cols] + [d.param_for(tsty)]
        ins = f"INSERT INTO {ref} ({', '.join(map(d.q, names))}) VALUES ({', '.join(marks)})"
        data = [tuple(d.param_value(op.row.get(c), cts.get(c)) for c in cols) + (ts,) for op in ops]
        return ref, [*((s, None, False) for s in create), (ins, data, True)], \
            [(s, None, False) for s in d.drop_temp(ref)]

    # ------------------------------------------------------------------ verify
    def list_tables(self, schema: str | None = None) -> list[str]:
        sql, params = self.d.tables_sql(schema or self.schema)
        return [str(r[0]) for r in self.ex.query(sql, params)[1]]

    def describe_table(self, dest_name: str, schema: str | None = None) -> list[dict[str, Any]]:
        sql, params = self.d.describe_sql(schema or self.schema, self.ident(dest_name))
        return [{"name": str(r[0]), "type": str(r[1]).lower(),
                 "nullable": str(r[2]).upper() in ("YES", "Y", "1", "TRUE")} for r in self.ex.query(sql, params)[1]]

    def fetch_rows(self, dest_name: str) -> list[dict[str, Any]]:
        names, rows = self.ex.query(f"SELECT * FROM {self.t(dest_name)}")
        return [dict(zip([n.lower() for n in names], r)) for r in rows]

    def row_count(self, dest_name: str, include_deleted: bool = False, **kw: Any) -> int:
        w = "" if include_deleted else f" WHERE {self.d.bool_cmp(self.d.q(self.c_deleted), False)}"
        fqn = self.d.table(kw.get("schema") or self.schema, self.ident(dest_name))
        _, rows = self.ex.query(f"SELECT COUNT(*) FROM {fqn}{w}")
        return int(rows[0][0])

    def aggregates(self, dest_name: str, columns: list[str]) -> dict[str, Any]:
        q = self.d.q
        parts = ["COUNT(*)"] + [f"{f}({q(self.ident(c))})" for c in columns for f in ("SUM", "MIN", "MAX")]
        _, rows = self.ex.query(f"SELECT {', '.join(parts)} FROM {self.t(dest_name)} "
                                f"WHERE {self.d.bool_cmp(q(self.c_deleted), False)}")
        r = rows[0]
        out: dict[str, Any] = {"count": int(r[0])}
        for i, c in enumerate(columns):
            out[f"sum:{c}"], out[f"min:{c}"], out[f"max:{c}"] = r[1 + 3 * i: 4 + 3 * i]
        return out

    def audit_rows(self, stream: str) -> list[dict[str, Any]]:
        q = self.d.q
        names, rows = self.ex.query(f"SELECT {q('batch_id')}, {q('to_position')}, {q('total_rows')} FROM "
                                    f"{self.c('audit_batch')} WHERE {q('stream')} = {self.d.param} "
                                    f"ORDER BY {q('seq')}", (stream,))
        return [dict(zip(names, r)) for r in rows]


def bind_literals(sql: str, params: Any, dialect: Dialect) -> str:
    """Replace '?' placeholders by literals (engines driven by literal SQL scripts: BigQuery, Databricks)."""
    if not params:
        return sql
    vals = iter(params)
    out: list[str] = []
    quote = None
    for ch in sql:
        if quote:
            out.append(ch)
            if ch == quote:
                quote = None
        elif ch in ("'", "`"):
            quote = ch
            out.append(ch)
        elif ch == "?":
            v = next(vals)
            out.append(_auto_lit(v, dialect))
        else:
            out.append(ch)
    return "".join(out)


def _auto_lit(v: Any, d: Dialect) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return d.true if v else d.false
    if isinstance(v, (int, float, Decimal)):
        return str(v)
    if isinstance(v, datetime):
        return d.lit(v, d.ptype(_col(CanonicalType.INSTANT if v.tzinfo else CanonicalType.LOCALDATETIME)))
    return d.string_lit(str(v))


def _col(ct: CanonicalType, length: int | None = None) -> ColumnInfo:
    return ColumnInfo(name="_", ordinal=0, engine_type="CONTROL", canonical_type=ct, length=length)
