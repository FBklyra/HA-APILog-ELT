"""Generic SQL destination: any transactional database as target, through a dialect adapter.

`SqlDestination(url, schema, control_schema="alelt_control", output_profile, compare, dialect=None, ...)`

- Dialect from `dialect=` or the URL scheme (`postgresql://`, `mysql://`, `tidb://`, `mariadb://`, `mssql://`,
  `oracle://`, `db2://`, `sqlite:///file`, `duckdb:///file`, or `sql+<dialect>://`).
- Landing tables `<schema>.<dest_name>`: source columns (canonical -> physical type map per dialect, golden DDL
  override) + output-profile system columns (synced timestamp, soft-delete flag; keyless tables: surrogate id PK).
- System columns OFF (`system_columns=False`, URL `?system_columns=off`, or output_profile `none`) = pure mirror:
  no synced/deleted columns, hard deletes, TRUNCATE deletes every row.
- apply() = ONE transaction: lock checkpoint row -> compare-and-set against the checkpoint the batch was planned
  from (`expected`, default: read at the start of apply; F-05) -> no-op if checkpoint >= to_pos -> bulk
  upserts/deletes -> checkpoint + per-table watermarks -> audit row -> COMMIT (R6). Any error rolls back.
- PK change: the loader emits DELETE(old key) + upsert(new key). Partial images (LOB unavailable / unchanged
  columns absent from the image) update only the columns present, so existing values are kept.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from alelt_common.canonical_jsonl import canonical_snapshot_row, snapshot_types
from alelt_common.models import CanonicalType, ChangeEvent, ChangeKind, ChangeSet, ColumnInfo, Position, TableInfo, TableRef
from alelt_common.spi import UNSET, ApplyStats, CheckpointConflictError
from alelt_dst_kit.base import AlreadyApplied, ControlState, DestinationBase, canon_key, wm_dumps, wm_loads
from alelt_loader import data_columns, surrogate_id
from alelt_loader.policy import SystemColumns, system_columns

from alelt_dst_kit.widen import widen_to

from .dialects import DIALECTS, ConnSettings, Dialect, dialect_name, norm_value

log = logging.getLogger("alelt_dst_sql")

_OFF = {"0", "off", "false", "no", "none"}
MIRROR_PROFILES = {"none", "mirror"}


from alelt_dst_kit.base import default_compare  # noqa: E402  (single copy, DST-kit-09)


def split_sql(script: str) -> list[str]:
    """Split a DDL script on ';' outside quotes and comments."""
    out, buf, i, n, quote = [], [], 0, len(script), ""
    while i < n:
        c = script[i]
        if quote:
            buf.append(c)
            if c == quote:
                quote = ""
        elif c in ("'", '"', "`"):
            quote = c
            buf.append(c)
        elif script.startswith("--", i):
            j = script.find("\n", i)
            i = n if j < 0 else j
            continue
        elif script.startswith("/*", i):
            j = script.find("*/", i + 2)
            i = n if j < 0 else j + 2
            continue
        elif c == ";":
            out.append("".join(buf).strip())
            buf = []
        else:
            buf.append(c)
        i += 1
    out.append("".join(buf).strip())
    return [s for s in out if s]


class _Group:
    """Consecutive rows of one table with the same operation and column set (one bulk statement family)."""

    def __init__(self, dest: str, info: TableInfo, op: str, cols: tuple[str, ...], keys: list[str],
                 st: ApplyStats) -> None:
        self.dest, self.info, self.op, self.cols, self.keys, self.st = dest, info, op, cols, keys, st
        self.rows: list[dict[str, Any]] = []
        self.seen: set[tuple[Any, ...]] = set()


class SqlDestination(DestinationBase):
    engine = "sql"
    display_name = "SQL database"

    def __init__(self, url: str | None = None, schema: str = "landing", control_schema: str = "alelt_control",
                 output_profile: str | None = None, compare: Callable[[Position, Position], int] | None = None,
                 dialect: str | None = None, password: str | None = None, system_columns_on: bool | None = None,
                 golden_dir: str | Path | None = None, enforce_nullability: bool = False, **_: Any) -> None:
        url = url or os.environ.get("ALELT_DEST_URL", "")
        cs = ConnSettings.parse(url) if url else ConnSettings(scheme=dialect or "")
        if password is not None:
            cs.password = password
        elif cs.password is None and os.environ.get("ALELT_DEST_PASSWORD"):
            cs.password = os.environ["ALELT_DEST_PASSWORD"]          # secret from env / Docker secret (R9)
        name = dialect_name(dialect or cs.scheme)
        if cs.scheme.split("+")[-1] == "tidb":
            cs.options.setdefault("upsert", "values")
        self.d: Dialect = DIALECTS[name](cs)
        self.dialect = name
        self.url = url
        self.schema = cs.options.get("schema") or schema              # URL ?schema= / ?control_schema= win
        self.control = cs.options.get("control_schema") or control_schema
        flag = cs.options.get("system_columns", cs.options.get("alelt_system_columns"))
        if system_columns_on is None:
            system_columns_on = not ((flag is not None and flag.lower() in _OFF)
                                     or (output_profile or "").lower() in MIRROR_PROFILES)
        self.mirror = not system_columns_on
        prof = None if (output_profile or "").lower() in MIRROR_PROFILES else output_profile
        self.sys: SystemColumns = system_columns(prof)
        self.compare = compare or default_compare
        self.golden_dir = Path(golden_dir or os.environ.get("ALELT_SQL_GOLDEN_DIR") or "") if (
            golden_dir or os.environ.get("ALELT_SQL_GOLDEN_DIR")) else None
        self.enforce_nullability = enforce_nullability
        self._conn: Any = None
        self.fault: Callable[[str], None] | None = None     # test hook: called at named points inside apply()

    def ident(self, name: str) -> str:
        return self.d.norm(name)

    def _on_schema(self, schema: str | None) -> "SqlDestination":
        """Another schema of the same database (reference landing, control area): own connection, so dialects that
        attach schemas per connection (SQLite files) see it."""
        if not schema or schema == self.schema:
            return self
        import copy

        c = copy.copy(self)
        c.schema, c._conn = schema, None
        return c

    # row dicts returned by fetch_rows / sample_rows use lower-case column names on every dialect
    @property
    def c_synced(self) -> str:
        return self.sys.synced.lower()

    @property
    def c_deleted(self) -> str:
        return self.sys.deleted.lower()

    @property
    def c_row_id(self) -> str:
        return self.sys.row_id.lower()

    # ------------------------------------------------------------------ plumbing
    @property
    def conn(self) -> Any:
        if self._conn is None:
            self._conn = self.d.connect([self.schema, self.control])
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def _reset(self) -> None:
        """Drop a connection after an error (it may be broken); the next call reconnects."""
        try:
            self.close()
        except Exception:  # noqa: BLE001
            self._conn = None

    def _run(self, fn: Callable[[Any], Any]) -> Any:
        """Run fn(cursor) in one transaction; commit on success, rollback + reset on error."""
        conn = self.conn
        try:
            self.d.begin(conn)
            out = fn(self.d.cursor(conn))
            conn.commit()
            return out
        except BaseException:
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            self._reset()
            raise

    def _query(self, sql: str, params: Iterable[Any] = ()) -> list[tuple[Any, ...]]:
        def go(cur: Any) -> list[tuple[Any, ...]]:
            cur.execute(sql, tuple(params)) if params else cur.execute(sql)
            return [tuple(r) for r in cur.fetchall()]
        return self._run(go)  # type: ignore[no-any-return]

    def t(self, name: str) -> str:
        return self.d.fq(self.schema, name)

    def c(self, name: str) -> str:
        return self.d.fq(self.control, name)

    def _ptype(self, ct: CanonicalType, length: int | None = None, scale: int | None = None) -> str:
        return self.d.types.physical(ColumnInfo(name="_", ordinal=0, engine_type="SYSTEM", canonical_type=ct,
                                                length=length, scale=scale))

    def test_connection(self) -> str:
        return self._run(self.d.version)  # type: ignore[no-any-return]

    # ------------------------------------------------------------------ SPI: DDL
    def bootstrap(self) -> None:
        s = lambda n: self._ptype(CanonicalType.STRING, n)  # noqa: E731
        ts, big = self._ptype(CanonicalType.INSTANT), self._ptype(CanonicalType.LONG)
        txt = self._ptype(CanonicalType.STRING)
        # DST-sql-03: positions grow (MySQL GTID sets: one interval list per source server UUID) - 4000 chars
        # (Db2: 1000, its default 4 KiB page cannot hold a 4000-byte checkpoint row)
        pl = 1000 if self.dialect == "db2" else 4000
        ck = [("stream", s(400), False), ("position", s(pl), True), ("batch_id", s(200), True), ("updated_at", ts, True),
              ("watermarks", txt, True)]
        au = [("id", s(32), False), ("batch_id", s(64), True), ("stream", s(200), True), ("from_position", s(pl), True),
              ("to_position", s(pl), True), ("rows_by_table", txt, True), ("total_rows", big, True),
              ("duration_ms", big, True), ("status", s(16), True), ("applied_at", ts, True)]

        def go(cur: Any) -> None:
            for sc in dict.fromkeys((self.schema, self.control)):
                self.d.create_schema(cur, sc)
            for name, cols, key in (("checkpoint", ck, "stream"), ("audit_batch", au, "id")):
                have = {c["name"].lower(): c for c in self.d.describe(cur, self.control, name)}
                if not have:
                    cur.execute(self.d.create_table_sql(self.c(name), cols, [key]))
                elif name == "checkpoint" and "watermarks" not in have:      # control table of an older release
                    cur.execute(self.d.add_column_sql(self.c(name), "watermarks", txt))
                elif name == "audit_batch":                                   # legacy alelt_dst_postgres: bigserial id
                    t = str(have.get("id", {}).get("type", ""))
                    self._audit_serial = any(x in t for x in ("int", "serial", "number"))
                if have:                                                      # older release: widen positions
                    for col, typ, nul in cols:
                        if col.endswith("position") and _shorter(str(have.get(col, {}).get("type", "")), pl):
                            for st in self.d.alter_type_sql(self.c(name), col, typ, nul):
                                cur.execute(st)
        self._run(go)

    def _table_cols(self, table: TableInfo) -> tuple[list[tuple[str, str, bool]], list[str]]:
        keys = [c.name for c in table.key_columns()]
        cols: list[tuple[str, str, bool]] = []
        if not keys:                                    # keyless: surrogate id column from the output profile
            keys = [self.sys.row_id]
            cols.append((self.sys.row_id, self._ptype(CanonicalType.STRING, 32), False))
        for c in sorted(table.columns, key=lambda c: c.ordinal):
            nullable = not c.key_seq and (c.nullable or not self.enforce_nullability)
            cols.append((c.name, self.d.types.physical(c), nullable))
        if not self.mirror:
            cols += [(self.sys.synced, self._ptype(CanonicalType.INSTANT), False),
                     (self.sys.deleted, self._ptype(CanonicalType.BOOLEAN), False)]
        if self.dialect in ("mysql", "mariadb"):
            cols = _fit_mysql_row(cols, set(keys))
        return cols, keys

    def _golden(self, dest_name: str) -> str | None:
        if self.golden_dir is None:
            return None
        for p in (self.golden_dir / self.dialect / f"{dest_name}.sql", self.golden_dir / f"{dest_name}.sql"):
            if p.exists():
                return p.read_text(encoding="utf-8")
        return None

    def ensure_table(self, table: TableInfo, dest_name: str, golden_ddl: str | None) -> None:
        """Create the landing table if absent (golden DDL first, R8), then add missing columns (additive drift)."""
        golden = golden_ddl or self._golden(dest_name)
        cols, keys = self._table_cols(table)

        widen: list[tuple[str, str, bool]] = []

        def go(cur: Any) -> None:
            self.d.create_schema(cur, self.schema)
            desc = {c["name"].lower(): c for c in self.d.describe(cur, self.schema, dest_name)}
            have = set(desc)
            if not have:
                if golden:
                    script = golden.replace("{{SCHEMA}}", self.d.q(self.schema)).replace("{{TABLE}}", self.t(dest_name))
                    for stmt in split_sql(script):
                        cur.execute(stmt)
                    have = {c["name"].lower() for c in self.d.describe(cur, self.schema, dest_name)}
                else:
                    cur.execute(self.d.create_table_sql(self.t(dest_name), cols, keys))
                    return
            for n, ptype, nullable in cols:
                if self.d.norm(n).lower() not in have:
                    cur.execute(self.d.add_column_sql(self.t(dest_name), n, ptype))
                elif self.d.norm(n).lower() in desc:
                    to = widen_to(str(desc[self.d.norm(n).lower()]["type"]), ptype)
                    if to:
                        widen.append((n, to, nullable))
        self._run(go)
        for n, to, nullable in widen:                  # type widening on drift (DST-kit-04), one txn per column
            stmts = self.d.alter_type_sql(self.t(dest_name), n, to, nullable)
            try:
                self._run(lambda cur, s=stmts: [cur.execute(x) for x in s])
                log.info("widened %s.%s to %s", dest_name, n, to)
            except Exception as e:  # noqa: BLE001 - reported; inserts of longer values fail loudly
                log.warning("cannot widen %s.%s to %s: %s", dest_name, n, to, e)

    def add_column(self, dest_name: str, col: ColumnInfo) -> None:
        ptype = self.d.types.physical(col)

        def go(cur: Any) -> None:
            have = {c["name"].lower() for c in self.d.describe(cur, self.schema, dest_name)}
            if self.d.norm(col.name).lower() not in have:
                cur.execute(self.d.add_column_sql(self.t(dest_name), col.name, ptype))
        self._run(go)

    def publish_view(self, view_schema: str, table: str, source_schema: str, columns: list[str]) -> None:
        cols = ", ".join(map(self.d.q, columns)) if columns else "*"
        sel = f"SELECT {cols} FROM {self.d.fq(source_schema, table)}"

        def go(cur: Any) -> None:
            self.d.create_schema(cur, view_schema)
            for stmt in self.d.view_sql(self.d.fq(view_schema, table), sel):
                cur.execute(stmt)
        self._run(go)

    def describe_schema(self, tables: list[str] | None = None, schema: str | None = None
                        ) -> dict[str, list[dict[str, Any]]]:
        """{table: [{name, type, nullable}]} as the database reports it (landing schema by default)."""
        sc = schema or self.schema

        def go(cur: Any) -> dict[str, list[dict[str, Any]]]:
            names = tables if tables is not None else self.d.list_tables(cur, sc)
            return {n: self.d.describe(cur, sc, n) for n in names}
        return self._run(go)  # type: ignore[no-any-return]

    # ------------------------------------------------------------------ SPI: checkpoint / apply
    def read_checkpoint(self, stream: str) -> Position | None:
        rows = self._query(f"SELECT {self.d.q('position')} FROM {self.c('checkpoint')} WHERE "
                           f"{self.d.q('stream')} = {self.d.ph(1)}", (stream,))
        return Position(engine="", text=rows[0][0]) if rows and rows[0][0] else None

    def read_watermarks(self, stream: str) -> dict[str, Position]:
        rows = self._query(f"SELECT {self.d.q('watermarks')} FROM {self.c('checkpoint')} WHERE "
                           f"{self.d.q('stream')} = {self.d.ph(1)}", (stream,))
        return {t: Position(engine="", text=x) for t, x in (wm_loads(rows[0][0]) if rows else {}).items()}

    def delete_checkpoint(self, stream: str) -> None:
        """Drop the checkpoint row of a transient stream (replay pass, HC-21)."""
        self._run(lambda cur: cur.execute(f"DELETE FROM {self.c('checkpoint')} WHERE {self.d.q('stream')} = "
                                          f"{self.d.ph(1)}", (stream,)))

    def _state(self, stream: str) -> ControlState:
        q = self.d.q
        rows = self._query(f"SELECT {q('position')}, {q('batch_id')} FROM {self.c('checkpoint')} WHERE "
                           f"{q('stream')} = {self.d.ph(1)}", (stream,))
        return ControlState(rows[0][0] or None, 0, rows[0][1] or None) if rows else ControlState()

    def _lock_checkpoint(self, cur: Any, stream: str) -> ControlState:
        """Row-lock the checkpoint of `stream` for the rest of the transaction; returns it as found under the lock."""
        q, p = self.d.q, self.d.ph(1)
        ck = self.c("checkpoint")
        cur.execute(f"UPDATE {ck} SET {q('batch_id')} = {q('batch_id')} WHERE {q('stream')} = {p}", (stream,))
        cur.execute(f"SELECT {q('position')}, {q('batch_id')}, {q('watermarks')} FROM {ck} WHERE {q('stream')} = {p}",
                    (stream,))
        row = cur.fetchone()
        if row is None:              # first batch of the stream: a concurrent first insert fails on the PK
            p2 = self.d.phs(2)
            cur.execute(f"INSERT INTO {ck} ({q('stream')}, {q('position')}) VALUES ({p2[0]}, {p2[1]})", (stream, ""))
            return ControlState()
        wm = row[2].read() if hasattr(row[2], "read") else row[2]
        return ControlState(row[0] or None, 0, row[1] or None, wm_loads(wm))

    def apply(self, stream: str, changesets: list[ChangeSet], to_pos: Position, batch_id: str,
              table_map: dict[TableRef, str], tables: dict[TableRef, TableInfo], *, expected: Any = UNSET,
              watermarks: Any = None) -> list[ApplyStats]:
        t0 = time.monotonic()
        self.refuse_lossy_values(changesets, tables)
        stats: dict[TableRef, ApplyStats] = {}
        start = self._state(stream)                     # what this batch is planned from (CAS base, F-05)
        if expected is UNSET and self.pos_ge(start.position, to_pos):
            return []                                   # idempotent replay
        self._hook("after_read")
        base = start if expected is UNSET else ControlState(
            expected.text if isinstance(expected, Position) else (expected or None))

        def go(cur: Any) -> list[ApplyStats] | None:
            found = self._lock_checkpoint(cur, stream)
            if (found.position or None) != (base.position or None):
                if self.pos_ge(found.position, to_pos):
                    return None                                   # same batch committed by another writer
                raise CheckpointConflictError(stream, base.position, found.position,
                                              "checkpoint moved since the batch was planned (concurrent writer)")
            if self.pos_ge(found.position, to_pos):
                return None                                       # idempotent replay
            prev = found.position
            wm = wm_dumps(self.merge_watermarks(found.watermarks, self.batch_watermarks(to_pos, table_map, watermarks),
                                                to_pos.engine))
            w = _Writer(self, cur, datetime.now(timezone.utc))
            for cs in changesets:
                for ev in cs.events:
                    if ev.table is None or ev.table not in table_map:
                        continue
                    st = stats.setdefault(ev.table, ApplyStats(table=table_map[ev.table]))
                    w.add(table_map[ev.table], tables[ev.table], ev, st)
            w.flush()
            self._hook("after_rows")
            self._hook("after_data")
            q, ph = self.d.q, self.d.phs
            p = ph(5)
            cur.execute(f"UPDATE {self.c('checkpoint')} SET {q('position')} = {p[0]}, {q('batch_id')} = {p[1]}, "
                        f"{q('updated_at')} = {p[2]}, {q('watermarks')} = {p[3]} WHERE {q('stream')} = {p[4]}",
                        (to_pos.text, batch_id, self.d.adapt(w.ts, CanonicalType.INSTANT), wm, stream))
            rc = getattr(cur, "rowcount", -1)
            if isinstance(rc, int) and rc == 0:
                raise CheckpointConflictError(stream, prev, None, "checkpoint row vanished under the lock")
            self._hook("after_checkpoint")
            dur = int((time.monotonic() - t0) * 1000)
            for s in stats.values():
                s.duration_ms = dur
            by_table = {s.table: {"inserts": s.inserts, "updates": s.updates, "deletes": s.deletes}
                        for s in stats.values()}
            cols = ["id", "batch_id", "stream", "from_position", "to_position", "rows_by_table", "total_rows",
                    "duration_ms", "status", "applied_at"]
            vals: tuple[Any, ...] = (uuid.uuid4().hex, batch_id, stream, prev, to_pos.text,
                                     json.dumps(by_table, sort_keys=True),
                                     sum(s.inserts + s.updates + s.deletes for s in stats.values()), dur, "APPLIED",
                                     self.d.adapt(w.ts, CanonicalType.INSTANT))
            if getattr(self, "_audit_serial", False):          # legacy audit table: id generated by the database
                cols, vals = cols[1:], vals[1:]
            cur.execute(f"INSERT INTO {self.c('audit_batch')} ({', '.join(map(q, cols))}) VALUES "
                        f"({', '.join(ph(len(cols)))})", vals)
            self._hook("before_commit")
            return list(stats.values())

        try:
            out = self._run(go)
        except AlreadyApplied:
            return []
        except CheckpointConflictError:
            raise
        except Exception as e:                          # e.g. two first inserts of a new stream: PK violation
            found = self._state(stream)
            if (found.position or None) != (base.position or None):
                if self.pos_ge(found.position, to_pos):
                    return []
                raise CheckpointConflictError(stream, base.position, found.position, f"{type(e).__name__}") from e
            raise
        return out or []

    def _hook(self, point: str) -> None:
        if self.fault is not None:
            self.fault(point)

    # ------------------------------------------------------------------ snapshot
    def load_snapshot(self, dest_name: str, table: TableInfo, rows: Iterable[dict[str, Any]]) -> int:
        n = 0
        st = ApplyStats(table=dest_name)

        def go(cur: Any) -> int:
            nonlocal n
            w = _Writer(self, cur, datetime.now(timezone.utc))
            types = snapshot_types(table)
            for r in rows:                     # snapshot values in the CDC forms (JSON = text; dict/list accepted)
                w.add(dest_name, table, ChangeEvent(position=Position(engine="snapshot", text="0"), txid="snapshot",
                                                    kind=ChangeKind.INSERT, table=table.table,
                                                    after=canonical_snapshot_row(r, types, strict=True)), st)
                n += 1
            w.flush()
            return n
        return self._run(go)  # type: ignore[no-any-return]

    # ------------------------------------------------------------------ verification helpers (alelt_verify)
    def _live(self, include_deleted: bool) -> tuple[str, list[Any]]:
        if self.mirror or include_deleted:
            return "", []
        return f"{self.d.q(self.sys.deleted)} = {{p}}", [self.d.adapt(False, CanonicalType.BOOLEAN)]

    def _where(self, include_deleted: bool, key_col: str | None, key_range: tuple[Any, Any] | None
               ) -> tuple[str, list[Any]]:
        conds, params = [], []
        live, lp = self._live(include_deleted)
        if live:
            conds.append(live)
            params += lp
        if key_col and key_range:
            lo, hi = key_range
            if lo is not None:
                conds.append(f"{self.d.q(key_col)} >= {{p}}")
                params.append(lo)
            if hi is not None:
                conds.append(f"{self.d.q(key_col)} <= {{p}}")
                params.append(hi)
        if not conds:
            return "", []
        sql = " WHERE " + " AND ".join(conds)
        i = 0

        def sub(_: re.Match[str]) -> str:
            nonlocal i
            i += 1
            return self.d.ph(i)
        return re.sub(r"\{p\}", sub, sql), params

    def row_count(self, dest_name: str, include_deleted: bool = False, **kw: Any) -> int:
        if kw.get("schema") not in (None, self.schema):
            return self._on_schema(kw.pop("schema")).row_count(dest_name, include_deleted, **kw)
        w, p = self._where(include_deleted, None, None)
        fqn = self.d.fq(kw.get("schema") or self.schema, dest_name)
        return int(self._query(f"SELECT COUNT(*) FROM {fqn}{w}", p)[0][0])

    def column_aggregates(self, dest_name: str, columns: list[str] | None = None, key_col: str | None = None,
                          key_range: tuple[Any, Any] | None = None, include_deleted: bool = False,
                          **kw: Any) -> dict[str, Any]:
        """{'_rows': n, col: {count, min, max[, sum]}} over live rows, optionally a key range (inclusive)."""
        if kw.get("schema") not in (None, self.schema):
            return self._on_schema(kw.pop("schema")).column_aggregates(dest_name, columns, key_col, key_range,
                                                                        include_deleted, **kw)
        sc = kw.get("schema") or self.schema
        desc = {c["name"].lower(): c for c in self.describe_schema([dest_name], sc)[dest_name]}
        cols = [self.d.norm(c) for c in columns] if columns else [
            c["name"] for c in desc.values() if c["name"].lower() not in
            {self.sys.synced.lower(), self.sys.deleted.lower()}]
        parts, plan = ["COUNT(*)"], []
        for c in cols:
            ptype = desc[c.lower()]["type"]
            qc = self.d.q(c)
            parts.append(f"COUNT({qc})")
            plan.append((c, "count"))
            if not self.d.is_lob(ptype) and not ptype.startswith(("bool", "bit", "blob", "bytea", "varbinary", "raw",
                                                                    "binary", "json", "xml", "longblob")):
                parts += [f"MIN({qc})", f"MAX({qc})"]
                plan += [(c, "min"), (c, "max")]
                if re.match(r"(small|big|tiny)?int|integer|decimal|numeric|number|real|double|float", ptype):
                    parts.append(f"SUM({qc})")
                    plan.append((c, "sum"))
        w, p = self._where(include_deleted, key_col and self.d.norm(key_col), key_range)
        row = self._query(f"SELECT {', '.join(parts)} FROM {self.d.fq(sc, dest_name)}{w}", p)[0]
        out: dict[str, Any] = {"_rows": int(row[0])}
        for (c, agg), v in zip(plan, row[1:]):
            out.setdefault(c, {})[agg] = v
        return out

    def sample_rows(self, dest_name: str, key_cols: list[str], columns: list[str] | None = None,
                    keys: list[tuple[Any, ...]] | None = None, *, n: int = 10,
                    key_range: tuple[Any, Any] | None = None, include_deleted: bool = False, **kw: Any) -> Any:
        """SPI: keys given -> {key: row} for those keys (soft-deleted included); else the first n rows in key order."""
        if kw.get("schema") not in (None, self.schema):
            return self._on_schema(kw.pop("schema")).sample_rows(dest_name, key_cols, columns, keys, n=n,
                                                                  key_range=key_range, include_deleted=include_deleted)
        fqn = self.d.fq(kw.get("schema") or self.schema, dest_name)
        pick = [self.d.norm(c).lower() for c in dict.fromkeys([*key_cols, *(columns or [])])] if columns else None

        def shape(r: dict[str, Any]) -> dict[str, Any]:
            return {c: r.get(c) for c in pick} if pick else r
        if keys is not None:
            out: dict[tuple[Any, ...], dict[str, Any]] = {}
            want = {canon_key(k): k for k in keys}
            kl = [self.d.norm(k).lower() for k in key_cols]
            step = max(1, min(200, (self.d.max_params - 10) // max(1, len(key_cols))))
            for i in range(0, len(keys), step):
                chunk = keys[i:i + step]
                cond = " OR ".join("(" + " AND ".join(f"{self.d.q(k)} = {{p}}" for k in key_cols) + ")" for _ in chunk)
                j = 0

                def sub(_: re.Match[str]) -> str:
                    nonlocal j
                    j += 1
                    return self.d.ph(j)
                for r in self._rows(f"SELECT * FROM {fqn} WHERE " + re.sub(r"\{p\}", sub, cond),
                                    [v for k in chunk for v in k]):
                    ck = canon_key(r.get(c) for c in kl)
                    if ck in want:
                        out[tuple(want[ck])] = shape(r)
            return out
        w, p = self._where(include_deleted, key_cols[0] if key_cols else None, key_range)
        order = ", ".join(self.d.q(k) for k in key_cols) or "1"
        return [shape(r) for r in self._rows(f"SELECT * FROM {fqn}{w} ORDER BY {order}{self.d.limit(n)}", p)]

    def key_set(self, dest_name: str, key_cols: list[str], key_range: tuple[Any, Any] | None = None,
                include_deleted: bool = False, **kw: Any) -> list[tuple[Any, ...]]:
        if kw.get("schema") not in (None, self.schema):
            return self._on_schema(kw.pop("schema")).key_set(dest_name, key_cols, key_range, include_deleted)
        w, p = self._where(include_deleted, key_cols[0] if key_cols else None, key_range)
        cols = ", ".join(self.d.q(k) for k in key_cols)
        fqn = self.d.fq(kw.get("schema") or self.schema, dest_name)
        rows = self._query(f"SELECT {cols} FROM {fqn}{w} ORDER BY {cols}", p)
        from alelt_dst_kit.base import _order
        return sorted({canon_key(r) for r in rows}, key=lambda t: tuple(map(_order, t)))

    def deleted_keys(self, dest_name: str, key_cols: list[str], **kw: Any) -> list[tuple[Any, ...]]:
        if kw.get("schema") not in (None, self.schema):
            return self._on_schema(kw.pop("schema")).deleted_keys(dest_name, key_cols)
        if self.mirror:
            return []
        cols = ", ".join(self.d.q(k) for k in key_cols)
        fqn = self.d.fq(kw.get("schema") or self.schema, dest_name)
        rows = self._query(f"SELECT {cols} FROM {fqn} WHERE {self.d.q(self.sys.deleted)} = {self.d.ph(1)}",
                           [self.d.adapt(True, CanonicalType.BOOLEAN)])
        from alelt_dst_kit.base import _order
        return sorted({canon_key(r) for r in rows}, key=lambda t: tuple(map(_order, t)))

    def list_tables(self, schema: str | None = None) -> list[str]:
        return self._run(lambda cur: self.d.list_tables(cur, schema or self.schema))  # type: ignore[no-any-return]

    def describe_table(self, dest_name: str, schema: str | None = None) -> list[dict[str, Any]]:
        return self._run(lambda cur: self.d.describe(cur, schema or self.schema, dest_name))  # type: ignore[no-any-return]


    def _rows(self, sql: str, params: list[Any] | tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        def go(cur: Any) -> list[dict[str, Any]]:
            cur.execute(sql, tuple(params)) if params else cur.execute(sql)
            names = [str(d[0]).lower() for d in cur.description or []]
            return [dict(zip(names, r)) for r in cur.fetchall()]
        return self._run(go)  # type: ignore[no-any-return]

    def fetch_rows(self, dest_name: str) -> list[dict[str, Any]]:
        """Test helper: all rows as dicts (column names lower case)."""
        return self._rows(f"SELECT * FROM {self.t(dest_name)}")

    def audit_rows(self, stream: str) -> list[dict[str, Any]]:
        q = self.d.q
        rows = self._rows(f"SELECT {q('batch_id')}, {q('to_position')}, {q('rows_by_table')}, {q('total_rows')} FROM "
                          f"{self.c('audit_batch')} WHERE {q('stream')} = {self.d.ph(1)} ORDER BY {q('applied_at')}",
                          (stream,))
        for r in rows:
            v = r["rows_by_table"]
            r["rows_by_table"] = v if isinstance(v, (dict, list)) else json.loads(v.read() if hasattr(v, "read") else v)
        return rows


class _Writer:
    """Turns ordered change events into bulk statements; flushes whenever the (table, op, columns) group changes
    or a key repeats, so the order of operations on one key is preserved."""

    def __init__(self, dst: SqlDestination, cur: Any, ts: datetime) -> None:
        self.dst, self.d, self.cur, self.ts = dst, dst.d, cur, ts
        self.g: _Group | None = None

    def add(self, dest: str, info: TableInfo, ev: ChangeEvent, st: ApplyStats) -> None:
        dst = self.dst
        keys = [c.name for c in info.key_columns()]
        types = {c.name: c.canonical_type for c in info.columns}
        if ev.kind is ChangeKind.TRUNCATE:
            self.flush()
            self._truncate(dest, st)
            return
        deleting = ev.kind is ChangeKind.DELETE
        if (ev.kind is ChangeKind.UPDATE and keys and ev.before and ev.after
                and all(k in ev.before for k in keys) and all(k in ev.after for k in keys)
                and any(norm_value(ev.before[k]) != norm_value(ev.after[k]) for k in keys)):
            # primary-key update without loader compaction (DST-postgres-02): delete(old key) + upsert(new key)
            self.add(dest, info, ev.model_copy(update={"kind": ChangeKind.DELETE, "after": None}), st)
        row = {k: v for k, v in ((ev.before if deleting else ev.after) or ev.before or {}).items() if k in types}
        if not keys:
            keys = [dst.sys.row_id]
            row = {dst.sys.row_id: surrogate_id(row, data_columns(info)), **row}
        if any(k not in row for k in keys):
            raise ValueError(f"{dest}: event without full key at {ev.position.text}")
        if deleting and dst.mirror:
            op, row = "delete", {k: row[k] for k in keys}
        else:
            op = "softdel" if deleting else "upsert"
        cols = tuple(row)
        kv = tuple(norm_value(self.d.adapt(row[k], types.get(k, CanonicalType.STRING))) for k in keys)
        g = self.g
        if g is None or (g.dest, g.op, g.cols) != (dest, op, cols) or kv in g.seen:
            self.flush()
            g = self.g = _Group(dest, info, op, cols, keys, st)
        g.rows.append(row)
        g.seen.add(kv)

    # ---------------------------------------------------------------- statements
    def _ptypes(self, info: TableInfo) -> dict[str, str]:
        dst = self.dst
        pt = {c.name: self.d.types.physical(c) for c in info.columns}
        pt[dst.sys.row_id] = dst._ptype(CanonicalType.STRING, 32)
        pt[dst.sys.synced] = dst._ptype(CanonicalType.INSTANT)
        pt[dst.sys.deleted] = dst._ptype(CanonicalType.BOOLEAN)
        return pt

    def _truncate(self, dest: str, st: ApplyStats) -> None:
        dst, d, cur = self.dst, self.d, self.cur
        w, p = dst._where(False, None, None)
        cur.execute(f"SELECT COUNT(*) FROM {dst.t(dest)}{w}", tuple(p)) if p else cur.execute(
            f"SELECT COUNT(*) FROM {dst.t(dest)}")
        st.deletes += int(cur.fetchone()[0])
        if dst.mirror:
            cur.execute(f"DELETE FROM {dst.t(dest)}")
            return
        ph = d.phs(3)
        cur.execute(f"UPDATE {dst.t(dest)} SET {d.q(dst.sys.deleted)} = {ph[0]}, {d.q(dst.sys.synced)} = {ph[1]} "
                    f"WHERE {d.q(dst.sys.deleted)} = {ph[2]}",
                    (d.adapt(True, CanonicalType.BOOLEAN), d.adapt(self.ts, CanonicalType.INSTANT),
                     d.adapt(False, CanonicalType.BOOLEAN)))

    def _existing(self, fqn: str, keys: list[str], ktypes: list[CanonicalType], rows: list[dict[str, Any]]
                  ) -> set[tuple[Any, ...]]:
        """Keys of `rows` already present (insert/update statistics)."""
        d, cur, out = self.d, self.cur, set()
        step = max(1, min(500, (d.max_params - 10) // len(keys)))
        for i in range(0, len(rows), step):
            chunk = rows[i:i + step]
            params = [d.adapt(r[k], t) for r in chunk for k, t in zip(keys, ktypes)]
            if len(keys) == 1:
                cond = f"{d.q(keys[0])} IN ({', '.join(d.phs(len(chunk)))})"
            else:
                n = len(keys)
                cond = " OR ".join("(" + " AND ".join(f"{d.q(k)} = {p}" for k, p in zip(keys, d.phs(n, 1 + j * n)))
                                   + ")" for j in range(len(chunk)))
            cur.execute(f"SELECT {', '.join(map(d.q, keys))} FROM {fqn} WHERE {cond}", tuple(params))
            out |= {tuple(norm_value(v) for v in r) for r in cur.fetchall()}
        return out

    def flush(self) -> None:
        g, self.g = self.g, None
        if g is None or not g.rows:
            return
        dst, d, cur = self.dst, self.d, self.cur
        fqn = dst.t(g.dest)
        types = {c.name: c.canonical_type for c in g.info.columns}
        types[dst.sys.row_id] = CanonicalType.STRING
        ktypes = [types[k] for k in g.keys]
        existing = self._existing(fqn, g.keys, ktypes, g.rows)
        if g.op == "delete":
            where = " AND ".join(f"{d.q(k)} = {p}" for k, p in zip(g.keys, d.phs(len(g.keys))))
            params = [tuple(d.adapt(r[k], t) for k, t in zip(g.keys, ktypes)) for r in g.rows]
            for i in range(0, len(params), d.bulk_rows):
                d.executemany(cur, f"DELETE FROM {fqn} WHERE {where}", params[i:i + d.bulk_rows])
            g.st.deletes += sum(1 for r in g.rows if self._k(r, g.keys, ktypes) in existing)
            return
        cols = list(g.cols)
        vals = [[d.adapt(r[c], types[c]) for c in cols] for r in g.rows]
        if g.op == "softdel":
            update = [] if dst.mirror else [dst.sys.synced, dst.sys.deleted]
        else:
            update = [c for c in cols if c not in g.keys]
            if not dst.mirror:
                update += [dst.sys.synced, dst.sys.deleted]
        if not dst.mirror:
            cols += [dst.sys.synced, dst.sys.deleted]
            extra = [d.adapt(self.ts, CanonicalType.INSTANT), d.adapt(g.op == "softdel", CanonicalType.BOOLEAN)]
            vals = [v + extra for v in vals]
        ptypes = self._ptypes(g.info)
        if d.multirow:
            per = max(1, min(d.max_rows_per_stmt, d.max_params // len(cols)))
            for i in range(0, len(vals), per):
                chunk = vals[i:i + per]
                d.prepare(cur, cols, ptypes, len(chunk))
                cur.execute(d.upsert_sql(fqn, cols, g.keys, update, ptypes, rows=len(chunk)),
                            tuple(v for row in chunk for v in row))
        else:
            sql = d.upsert_sql(fqn, cols, g.keys, update, ptypes)
            for i in range(0, len(vals), d.bulk_rows):
                d.prepare(cur, cols, ptypes)
                d.executemany(cur, sql, [tuple(v) for v in vals[i:i + d.bulk_rows]])
        for r in g.rows:
            hit = self._k(r, g.keys, ktypes) in existing
            if g.op == "softdel":
                g.st.deletes += 1
            elif hit:
                g.st.updates += 1
            else:
                g.st.inserts += 1

    def _k(self, r: dict[str, Any], keys: list[str], ktypes: list[CanonicalType]) -> tuple[Any, ...]:
        return tuple(norm_value(self.d.adapt(r[k], t)) for k, t in zip(keys, ktypes))


def _shorter(described: str, n: int) -> bool:
    """True if a described character type (e.g. 'varchar(400)', 'character varying(1000)') holds fewer than n."""
    import re

    m = re.search(r"char[^(]*\((\d+)", described.lower())
    return bool(m) and int(m.group(1)) < n


def _fit_mysql_row(cols: list[tuple[str, str, bool]], keys: set[str]) -> list[tuple[str, str, bool]]:
    """DST-sql-04: InnoDB counts every VARCHAR at its declared size (utf8mb4: 4 bytes/char) toward the 65 535-byte
    row limit. Non-key VARCHARs are turned into TEXT (stored off-row), largest first, until the row fits."""
    import re

    def size(t: str) -> int:
        m = re.match(r"\s*varchar\((\d+)\)", t.lower())
        return int(m.group(1)) * 4 + 2 if m else (8 if "text" in t.lower() or "blob" in t.lower() else 16)

    out = list(cols)
    while sum(size(t) for _, t, _ in out) > 65_000:
        cand = [(size(t), i) for i, (n, t, _) in enumerate(out) if n not in keys and t.lower().startswith("varchar")]
        if not cand:
            break
        _, i = max(cand)
        n, t, nul = out[i]
        out[i] = (n, "mediumtext" if size(t) <= 16_777_215 else "longtext", nul)
    return out
