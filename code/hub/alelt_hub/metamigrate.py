"""Hub metadata migration between backends: `alelt migrate-metadata --from URL --to URL` (X-50).

Typical: SQLite (single host) -> PostgreSQL (HA), or back. Every table of the hub schema (incl. diagnosis and HA
tables) is copied in foreign-key order with the portable column types, so UUIDs, UTC timestamps and JSON round-trip
identically. Verification per table: row count + digest of the primary keys + digest of the full row content
(HA-23; order-independent sums of per-row sha256, streamed - no key list in memory). The source is read in ONE
snapshot (PostgreSQL REPEATABLE READ READ ONLY, SQLite one read transaction), so rows written by a still running
hub during the copy can never make the verification flap (HA-09); the topology switch freezes agent writes and drains
the workers before this step, and stand-alone use should stop the source hub first. Postgres sequences (serial keys,
`batch_seq_gen`) are moved past the copied maxima. Idempotent: a target that already verifies equal is skipped;
a partially filled target is refused unless `reset_target=True` (then the hub tables in the target are emptied first).
The source is never modified.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import JSON, Connection, Engine, Table, create_engine, delete, func, inspect, null, select
from sqlalchemy.engine import make_url

from .db import init_db, md
from .dialect import insert, make_engine, reset_sequences


def hub_tables() -> list[Table]:
    import alelt_diagnose.tables  # noqa: F401 - registers diag_* tables
    import alelt_hub.ha_models  # noqa: F401 - registers capture_lease / hub_node

    return list(md.sorted_tables)


def redact(url: str) -> str:
    try:
        return make_url(url).render_as_string(hide_password=True)
    except Exception:  # noqa: BLE001
        return url.split("@")[-1]


@dataclass
class TableReport:
    name: str
    source_rows: int
    target_rows: int
    copied: int = 0
    action: str = ""             # copied | skipped (already equal) | empty
    pk_match: bool = True
    content_match: bool = True

    @property
    def ok(self) -> bool:
        return self.source_rows == self.target_rows and self.pk_match and self.content_match


@dataclass
class Report:
    source: str
    target: str
    tables: list[TableReport] = field(default_factory=list)
    extra_source_tables: list[str] = field(default_factory=list)
    sequences_reset: int = 0

    @property
    def ok(self) -> bool:
        return all(t.ok for t in self.tables)

    def lines(self) -> list[str]:
        out = [f"{'table':32} {'source':>9} {'target':>9}  result"]
        for t in self.tables:
            out.append(f"{t.name:32} {t.source_rows:>9} {t.target_rows:>9}  "
                       f"{'OK' if t.ok else 'MISMATCH'} {t.action}")
        if self.extra_source_tables:
            out.append(f"not copied (not part of the hub schema): {', '.join(self.extra_source_tables)}")
        return out


def _count(c: Connection, t: Table) -> int:
    return int(c.execute(select(func.count()).select_from(t)).scalar_one())


def _norm(v: Any) -> Any:
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True, separators=(",", ":"), default=str)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v) if v is not None else None


_MOD = 1 << 256
# written by init_db() on both sides (applied_at / app_version differ by design): keys compared, content not
PK_ONLY = frozenset({"schema_migration"})


def _digests(c: Connection, t: Table) -> tuple[str, str]:
    """(pk digest, content digest) of a table: order-independent sums of per-row sha256 (collation-proof across
    backends), streamed (HA-23)."""
    pk = [x.name for x in (list(t.primary_key.columns) or list(t.columns))]
    cols = pk if t.name in PK_ONLY else [x.name for x in t.columns]
    kd = cd = 0
    res = c.execution_options(stream_results=True, yield_per=2000).execute(select(t))
    for r in res.mappings():
        k = repr(tuple(_norm(r[x]) for x in pk)).encode()
        v = json.dumps([_norm(r[x]) for x in cols], default=str).encode()
        kd = (kd + int.from_bytes(hashlib.sha256(k).digest(), "big")) % _MOD
        cd = (cd + int.from_bytes(hashlib.sha256(k + b"\0" + v).digest(), "big")) % _MOD
    return f"{kd:064x}", f"{cd:064x}"


def _snapshot(src: Engine) -> Connection:
    """One consistent read view of the source for counts, copy and verification (HA-09)."""
    if src.dialect.name == "postgresql":
        c = src.connect().execution_options(isolation_level="REPEATABLE READ", postgresql_readonly=True)
        c.begin()
        return c
    c = src.connect()
    c.exec_driver_sql("BEGIN")          # SQLite WAL: a read transaction sees one snapshot until it ends
    c.exec_driver_sql("SELECT count(*) FROM sqlite_master")
    return c


def _copy(sc: Connection, dst: Engine, t: Table, chunk: int) -> int:
    json_cols = [c.name for c in t.columns if isinstance(c.type, JSON) or isinstance(getattr(c.type, "impl", None), JSON)]
    pk = list(t.primary_key.columns) or list(t.columns)
    n = 0
    res = sc.execution_options(stream_results=True).execute(select(t).order_by(*pk))
    while True:
        rows = [dict(r) for r in res.mappings().fetchmany(chunk)]
        if not rows:
            break
        groups: dict[tuple[str, ...], list[dict[str, Any]]] = {}
        for r in rows:                     # keep SQL NULL in JSON columns (not the JSON literal null)
            nulls = tuple(k for k in json_cols if r.get(k) is None)
            groups.setdefault(nulls, []).append({k: v for k, v in r.items() if k not in nulls})
        with dst.begin() as dc:
            for nulls, params in groups.items():
                stmt = insert(t).values({k: null() for k in nulls}) if nulls else insert(t)
                dc.execute(stmt, params)
        n += len(rows)
    return n


def migrate(src_url: str, dst_url: str, reset_target: bool = False, verify_only: bool = False, chunk: int = 1000,
            log: Callable[[str], None] = print) -> Report:
    if src_url == dst_url:
        raise ValueError("source and target metadata URLs are the same")
    # source is only read: a plain engine (no BEGIN IMMEDIATE on SQLite, so a running hub keeps its write lock)
    src = create_engine(src_url, future=True) if src_url.startswith("sqlite") else make_engine(src_url)
    dst = make_engine(dst_url)
    sc: Connection | None = None
    try:
        rep = Report(redact(src_url), redact(dst_url))
        have = set(inspect(src).get_table_names())
        tables = [t for t in hub_tables() if t.name in have]
        rep.extra_source_tables = sorted(have - {t.name for t in hub_tables()})
        if not verify_only:
            init_db(dst)
            from .migrate import add_missing_columns

            add_missing_columns(dst)
        dst_have = set(inspect(dst).get_table_names())
        sc = _snapshot(src)
        sdig: dict[str, tuple[str, str]] = {}

        def src_dig(t: Table) -> tuple[str, str]:
            if t.name not in sdig:
                sdig[t.name] = _digests(sc, t)  # type: ignore[arg-type]
            return sdig[t.name]

        def dst_dig(t: Table) -> tuple[str, str]:
            with dst.connect() as dc:
                return _digests(dc, t)

        plan = []
        with dst.connect() as dc:
            for t in tables:
                s_n = _count(sc, t)
                d_n = _count(dc, t) if t.name in dst_have else 0
                plan.append((t, s_n, d_n))
        same_keys: set[str] = set()
        equal = set()
        for t, s_n, d_n in plan:
            if s_n == d_n:
                if s_n == 0:
                    equal.add(t.name)
                    continue
                (sk, sv), (dk, dv) = src_dig(t), dst_dig(t)
                if sk == dk:
                    (equal if sv == dv else same_keys).add(t.name)
        # same keys, other content (rows written by init_db on the target, e.g. config bootstrap, or an older copy):
        # refreshed from the source together with every table that references them - not a conflict
        refresh = set(same_keys)
        grew = True
        while grew:
            grew = False
            for t in tables:
                if t.name not in refresh and any(fk.column.table.name in refresh for fk in t.foreign_keys):
                    refresh.add(t.name)
                    grew = True
        partial = [t.name for t, s_n, d_n in plan if d_n and t.name not in equal and t.name not in refresh]
        if verify_only:
            rep.tables = [TableReport(t.name, s_n, d_n, action="verify",
                                      pk_match=t.name in equal or t.name in same_keys or (s_n == d_n == 0),
                                      content_match=t.name in equal or (s_n == d_n == 0))
                          for t, s_n, d_n in plan]
            return rep
        if partial and not reset_target:
            raise RuntimeError("target metadata already holds different data in: " + ", ".join(partial)
                               + " (use --reset-target to empty the hub tables in the target first)")
        if partial:
            log(f"resetting target tables: {', '.join(partial)}")
            with dst.begin() as dc:
                for t in reversed(tables):
                    dc.execute(delete(t))
            equal = {t.name for t, s_n, _ in plan if s_n == 0}
        elif refresh:
            log(f"refreshing target tables from the source: {', '.join(sorted(refresh))}")
            with dst.begin() as dc:
                for t in reversed(tables):
                    if t.name in refresh and t.name in dst_have:
                        dc.execute(delete(t))
            equal -= refresh
        for t, s_n, d_n in plan:
            tr = TableReport(t.name, s_n, d_n)
            if t.name in equal:
                tr.action = "skipped (already equal)" if s_n else "empty"
            else:
                tr.copied = _copy(sc, dst, t, chunk)
                tr.action = f"copied {tr.copied}"
                log(f"  {t.name}: {tr.copied} rows")
            rep.tables.append(tr)
        with dst.begin() as dc:
            rep.sequences_reset = reset_sequences(dc, tables)
        for tr, (t, _, _) in zip(rep.tables, plan):    # verify against the same source snapshot
            with dst.connect() as dc:
                tr.target_rows = _count(dc, t)
            if tr.source_rows:
                (sk, sv), (dk, dv) = src_dig(t), dst_dig(t)
                tr.pk_match, tr.content_match = sk == dk, sv == dv
        return rep
    finally:
        if sc is not None:
            try:
                sc.rollback()
            finally:
                sc.close()
        src.dispose()
        dst.dispose()


def rewrite_refs(engine: Engine, src_store: Any, dst_store: Any, chunk: int = 2000) -> int:
    """Point batch / snapshot spool refs that belong to `src_store` at the same key in `dst_store` (after copy)."""
    from .db import batch, snapshot_chunk

    n = 0
    for t, key in ((batch, [batch.c.id]), (snapshot_chunk, [snapshot_chunk.c.snapshot_id, snapshot_chunk.c.chunk_no])):
        with engine.connect() as c:
            rows = c.execute(select(*key, t.c.spool_path)).all()
        todo = []
        for r in rows:
            k = src_store.key_of(r[-1])
            if k is not None:
                new = dst_store.ref(k)
                if new != r[-1]:
                    todo.append((tuple(r[:-1]), new))
        for i in range(0, len(todo), chunk):
            with engine.begin() as c:
                for ids, new in todo[i:i + chunk]:
                    c.execute(t.update().where(*[col == v for col, v in zip(key, ids)]).values(spool_path=new))
        n += len(todo)
    return n
