"""Metadata-database dialect layer: the only module that knows whether ALELT_DB_URL is PostgreSQL or SQLite.

Shared code uses the portable pieces exported here:
- `make_engine(url)`        engine; SQLite gets WAL, busy_timeout, foreign_keys, synchronous=NORMAL, BEGIN IMMEDIATE
- `UUIDType`                uuid (native on Postgres, CHAR(32) on SQLite)
- `UTCDateTime`             tz-aware timestamp, stored as UTC (timestamptz on Postgres, ISO text on SQLite)
- `utc_now()`               server-side "now" default (UTC on both)
- `next_seq(...)`           global monotonic counter default (Postgres sequence, SQLite max+1 under the write lock)
- `insert(table)`           INSERT with ON CONFLICT DO NOTHING / DO UPDATE (same syntax on both)
- `claim_one(...)`          queue claim: FOR UPDATE SKIP LOCKED (Postgres) / atomic UPDATE ... RETURNING (SQLite)
- `try_lock / unlock`       cross-process advisory lock: pg_try_advisory_lock (Postgres) / OS file lock (SQLite)
- `add_column_sql(...)`     additive migration DDL

SQLite is single-host only: one API + a few workers sharing one local volume (no network filesystems).
"""
from __future__ import annotations

import hashlib
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import (
    BigInteger,
    ColumnElement,
    Connection,
    DateTime,
    Engine,
    Table,
    Uuid,
    create_engine,
    event,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import insert as _pg_insert
from sqlalchemy.dialects.postgresql.dml import Insert
from sqlalchemy.engine import Dialect, make_url
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.types import TypeDecorator

SQLITE_BUSY_TIMEOUT_MS = 15000


def is_sqlite(url: str) -> bool:
    return url.startswith("sqlite")


# ------------------------------------------------------------------ engine
def make_engine(url: str) -> Engine:
    if not is_sqlite(url):
        # never block forever on a connect (libpq default = no timeout): e.g. `localhost` resolving to ::1 while the
        # server is published on 127.0.0.1 only makes the first address hang - with a timeout libpq moves on and a
        # dead server is a clear error at startup instead of a silent hang
        q = make_url(url).query
        args: dict[str, Any] = {} if "connect_timeout" in q else {
            "connect_timeout": int(os.environ.get("ALELT_DB_CONNECT_TIMEOUT_S", "10"))}
        return create_engine(url, pool_pre_ping=True, future=True, connect_args=args)
    db = make_url(url).database
    if db and db != ":memory:":
        Path(db).parent.mkdir(parents=True, exist_ok=True)
    eng = create_engine(url, future=True, pool_pre_ping=True,
                        connect_args={"timeout": SQLITE_BUSY_TIMEOUT_MS / 1000, "check_same_thread": False})

    @event.listens_for(eng, "connect")
    def _on_connect(dbapi_conn: Any, _: Any) -> None:
        dbapi_conn.isolation_level = None           # we issue BEGIN ourselves (below)
        cur = dbapi_conn.cursor()
        # HA-11: synchronous=FULL - an ACKed batch row survives a power loss (WAL + NORMAL may lose the last commits)
        for p in ("journal_mode=WAL", f"busy_timeout={SQLITE_BUSY_TIMEOUT_MS}", "foreign_keys=ON",
                  f"synchronous={os.environ.get('ALELT_SQLITE_SYNCHRONOUS', 'FULL')}"):
            cur.execute(f"PRAGMA {p}")
        cur.close()

    @event.listens_for(eng, "begin")
    def _on_begin(conn: Connection) -> None:
        # take the write lock up front: no SQLITE_BUSY on read->write upgrade, writers queue on busy_timeout
        conn.exec_driver_sql("BEGIN IMMEDIATE")

    return eng


# ------------------------------------------------------------------ types / defaults
class UTCDateTime(TypeDecorator[datetime]):
    """Timezone-aware timestamp; naive input is taken as UTC; always returned as an aware UTC datetime."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        v = (value if value.tzinfo else value.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)
        return v.replace(tzinfo=None) if dialect.name == "sqlite" else v

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


class UUIDType(TypeDecorator[uuid.UUID]):
    """UUID column: native uuid on Postgres, CHAR(32) on SQLite; also accepts str values on bind."""

    impl = Uuid
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: Dialect) -> uuid.UUID | None:
        return value if value is None or isinstance(value, uuid.UUID) else uuid.UUID(str(value))


class utc_now(FunctionElement[datetime]):  # noqa: N801 - used like func.now()
    """Server-side current timestamp (UTC)."""

    type = UTCDateTime()
    inherit_cache = True


@compiles(utc_now)
def _utc_now_default(_: utc_now, compiler: SQLCompiler, **kw: Any) -> str:
    return "now()"


@compiles(utc_now, "sqlite")
def _utc_now_sqlite(_: utc_now, compiler: SQLCompiler, **kw: Any) -> str:
    # bound UTCDateTime values are 'YYYY-MM-DD HH:MM:SS.ffffff' (6 digits); %f gives 3 -> pad to the same layout
    # so text ORDER BY / comparisons agree across server-default and bound rows (HC-29)
    return "(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000')"


class next_seq(FunctionElement[int]):  # noqa: N801
    """Next value of a global monotonic counter used as an INSERT default for `table.column`.
    Postgres: nextval(<sequence>) (declare the Sequence on the MetaData); SQLite: max+1, safe because every
    SQLite write transaction holds the database write lock (BEGIN IMMEDIATE)."""

    type = BigInteger()
    inherit_cache = False          # carries plain attributes; statements using it are simply not cached

    def __init__(self, sequence: str, table: str, column: str) -> None:
        self.sequence, self.table_name, self.column_name = sequence, table, column
        super().__init__()


@compiles(next_seq)
def _next_seq_pg(e: next_seq, compiler: SQLCompiler, **kw: Any) -> str:
    return f"nextval('{e.sequence}')"


@compiles(next_seq, "sqlite")
def _next_seq_sqlite(e: next_seq, compiler: SQLCompiler, **kw: Any) -> str:
    return f'(SELECT coalesce(max("{e.column_name}"), 0) + 1 FROM "{e.table_name}")'


# ------------------------------------------------------------------ DML
def insert(table: Table) -> Insert:
    """INSERT supporting .on_conflict_do_nothing / .on_conflict_do_update(index_elements=, set_=) / .excluded /
    .returning. Renders the identical `ON CONFLICT (...) DO ...` clause on PostgreSQL and SQLite (>= 3.35)."""
    return _pg_insert(table)


def claim_one(c: Connection, t: Table, where: list[ColumnElement[bool]], order_by: Any,
              values: dict[str, Any]) -> dict[str, Any] | None:
    """Atomically pick the first row matching `where` (by `order_by`: one column or a tuple), apply `values`
    (expressions over the row allowed, e.g. a CASE clearing a dedup key), return the updated row.
    Concurrent callers never get the same row."""
    order = tuple(order_by) if isinstance(order_by, (tuple, list)) else (order_by,)
    if c.dialect.name == "sqlite":
        pick = select(t.c.id).where(*where).order_by(*order).limit(1).scalar_subquery()
        row = c.execute(update(t).where(t.c.id == pick).values(**values).returning(*t.c)).mappings().first()
        return dict(row) if row else None
    rid = c.execute(select(t.c.id).where(*where).order_by(*order).limit(1)
                    .with_for_update(skip_locked=True)).scalar()
    if rid is None:
        return None
    return dict(c.execute(update(t).where(t.c.id == rid).values(**values).returning(*t.c)).mappings().one())


# ------------------------------------------------------------------ advisory locks
class _FileLock:
    """Exclusive non-blocking OS lock on a file; released by the OS if the process dies."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)

    def acquire(self) -> bool:
        try:
            if os.name == "nt":
                import msvcrt

                os.lseek(self.fd, 0, os.SEEK_SET)
                msvcrt.locking(self.fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            os.close(self.fd)
            return False

    def release(self) -> None:
        try:
            if os.name == "nt":
                import msvcrt

                os.lseek(self.fd, 0, os.SEEK_SET)
                msvcrt.locking(self.fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.fd, fcntl.LOCK_UN)
        finally:
            os.close(self.fd)


def _lock_dir(engine: Engine) -> Path:
    db = engine.url.database
    if db and db != ":memory:":
        p = Path(db)
        return p.parent / f"{p.name}.locks"
    return Path(tempfile.gettempdir()) / "alelt_hub_locks"


class LockLost(RuntimeError):
    """The session holding an advisory lock is gone (PostgreSQL failover, killed backend, pooler)."""


class PoolerError(RuntimeError):
    """Session advisory locks are meaningless behind transaction / statement pooling (HA-08)."""


def try_lock(engine: Engine, key: str) -> Any:
    """Non-blocking cross-process lock for `key` (e.g. one worker per stream). Returns a handle or None.

    PostgreSQL: a *session* advisory lock on a dedicated connection that is held for the lock's lifetime. It needs a
    direct connection or session pooling (PgBouncer `pool_mode = session`); with transaction / statement pooling the
    backend changes between transactions and the lock protects nothing - detected here (backend pid changes between
    two transactions on the same client connection) and refused (PoolerError). `lock_alive()` re-checks the lock
    before work that must not run twice (a failover drops the session and the lock with it, HA-08)."""
    h = _try_lock(engine, key)
    if h is not None:                     # writer fence for single-writer destinations (DST-clickhouse-01)
        from alelt_common.spi import hold_writer
        hold_writer(key, lambda: lock_alive(h))
    return h


def _try_lock(engine: Engine, key: str) -> Any:
    if engine.dialect.name == "sqlite":
        lk = _FileLock(_lock_dir(engine) / f"{hashlib.sha256(key.encode()).hexdigest()[:32]}.lock")
        return lk if lk.acquire() else None
    c = engine.connect()
    pid = c.execute(text("SELECT pg_backend_pid()")).scalar()
    c.commit()
    got = c.execute(text("SELECT pg_try_advisory_lock(hashtext(:k))"), {"k": key}).scalar()
    c.commit()
    if not got:
        c.close()
        return None
    pid2 = c.execute(text("SELECT pg_backend_pid()")).scalar()
    c.commit()
    if pid2 != pid:
        try:
            c.execute(text("SELECT pg_advisory_unlock(hashtext(:k))"), {"k": key})
            c.commit()
        finally:
            c.close()
        raise PoolerError("the metadata connection changes backend between transactions (transaction pooling?): "
                          "per-stream advisory locks need a direct connection or PgBouncer pool_mode=session")
    c.info["alelt_lock"] = (key, pid)
    return c


def lock_alive(handle: Any) -> bool:
    """True while the lock is still held by this process (always True for the SQLite file lock: the OS keeps it)."""
    if handle is None:
        return False
    if isinstance(handle, _FileLock):
        return True
    try:
        key, pid = handle.info.get("alelt_lock", (None, None))      # raises too once the connection is invalidated
        row = handle.execute(text(
            "SELECT pg_backend_pid(), EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND granted "
            "AND pid = pg_backend_pid() AND objid = (hashtext(:k)::bigint & 4294967295)::oid)"), {"k": key}).first()
        handle.commit()
    except Exception:  # noqa: BLE001 - connection gone = lock gone
        try:
            handle.invalidate()
        except Exception:  # noqa: BLE001
            pass
        return False
    return bool(row and row[0] == pid and row[1])


def require_lock(handle: Any, key: str) -> None:
    if not lock_alive(handle):
        raise LockLost(f"advisory lock {key!r} lost (metadata failover or session killed): aborting this run")


def unlock(handle: Any, key: str) -> None:
    from alelt_common.spi import release_writer
    release_writer(key)
    if isinstance(handle, _FileLock):
        handle.release()
        return
    try:
        handle.execute(text("SELECT pg_advisory_unlock(hashtext(:k))"), {"k": key})
        handle.commit()
    except Exception:  # noqa: BLE001 - session already gone (failover / LockLost): the lock went with it
        try:
            handle.invalidate()
        except Exception:  # noqa: BLE001
            pass
    finally:
        handle.close()


# ------------------------------------------------------------------ DDL
def add_column_sql(dialect: Dialect, table: str, column_ddl: str) -> str:
    """Additive column DDL. SQLite: no IF NOT EXISTS (callers add only missing columns), and the new column must
    be nullable or have a constant default (no utc_now(), no UNIQUE/PRIMARY KEY)."""
    if dialect.name == "sqlite":
        return f'ALTER TABLE "{table}" ADD COLUMN {column_ddl}'
    return f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS {column_ddl}'


def seconds_since(ts: datetime | None, now: datetime | None = None) -> float | None:
    """Age of an aware timestamp in seconds (portable replacement for extract(epoch from now() - ts))."""
    if ts is None:
        return None
    return ((now or datetime.now(timezone.utc)) - ts).total_seconds()


def reset_sequences(c: Connection, tables: list[Table]) -> int:
    """After a bulk copy with explicit keys (metadata migration): move Postgres serial / `next_seq` sequences past the
    copied maxima. SQLite derives the next value from the data (nothing to do)."""
    if c.dialect.name == "sqlite":
        return 0
    n = 0
    for t in tables:
        col = t.autoincrement_column
        if col is not None:
            c.execute(text(f"SELECT setval(pg_get_serial_sequence('\"{t.name}\"', '{col.name}'), "
                           f"coalesce((SELECT max(\"{col.name}\") FROM \"{t.name}\"), 0) + 1, false)"))
            n += 1
        for col in t.columns:
            arg = getattr(col.default, "arg", None)
            if isinstance(arg, next_seq):
                c.execute(text(f"SELECT setval('{arg.sequence}', coalesce((SELECT max(\"{col.name}\") "
                               f"FROM \"{t.name}\"), 0) + 1, false)"))
                n += 1
    return n
