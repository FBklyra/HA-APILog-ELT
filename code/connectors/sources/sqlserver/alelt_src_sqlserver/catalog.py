"""SQL Server catalog (sys.* + cdc.change_tables / cdc.captured_columns) and SNAPSHOT-isolation reads. Read-only."""
from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

from alelt_common.models import CanonicalType, CatalogSnapshot, ColumnInfo, Position, TableInfo, TableRef

from . import types as T
from .assemble import CaptureInstance

ENGINE = "sqlserver"


def qi(ident: str) -> str:
    return "[" + ident.replace("]", "]]") + "]"


def _in(vals: list[str]) -> tuple[str, tuple[Any, ...]]:
    return ", ".join(["%s"] * len(vals)) or "NULL", tuple(vals)


COLUMNS_SQL = """
SELECT s.name, t.name, t.object_id, t.is_tracked_by_cdc, c.name, c.column_id,
       CASE WHEN ty.is_user_defined = 0 OR ty.is_assembly_type = 1 THEN ty.name ELSE TYPE_NAME(c.system_type_id) END,
       c.max_length, c.precision, c.scale, c.is_nullable, c.is_computed
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.columns c ON c.object_id = t.object_id
JOIN sys.types ty ON ty.user_type_id = c.user_type_id
WHERE t.is_ms_shipped = 0 AND s.name IN ({schemas})
ORDER BY s.name, t.name, c.column_id
"""

PK_SQL = """
SELECT s.name, t.name, c.name, ic.key_ordinal
FROM sys.indexes i
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
JOIN sys.tables t ON t.object_id = i.object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE i.is_primary_key = 1 AND ic.key_ordinal > 0 AND s.name IN ({schemas})
"""

INSTANCES_SQL = """
SELECT ct.capture_instance, OBJECT_SCHEMA_NAME(ct.source_object_id), OBJECT_NAME(ct.source_object_id),
       ct.start_lsn, ct.create_date, cc.column_name, cc.column_ordinal
FROM cdc.change_tables ct
JOIN cdc.captured_columns cc ON cc.object_id = ct.object_id
ORDER BY ct.create_date, ct.capture_instance, cc.column_ordinal
"""


def table_filter(tables: list[str], present: set[tuple[str, str]]) -> set[tuple[str, str]]:
    """settings.tables -> the (schema, table) pairs to capture. `schema.table` matches exactly; a bare name matches
    only when ONE captured schema has that table - an ambiguous bare name is refused (SRC-ms-06: `ORDERS` must not
    also capture `archive.ORDERS`)."""
    want = {t.upper() for t in tables}
    out = {(s, t) for s, t in present if f"{s}.{t}".upper() in want}
    for bare in (w for w in want if "." not in w):
        hits = sorted((s, t) for s, t in present if t.upper() == bare)
        if len(hits) > 1:
            raise ValueError(f"settings.tables entry {bare!r} exists in several schemas "
                             f"({', '.join(s for s, _ in hits)}): write schema.table")
        out.update(hits)
    return out


def read_catalog(conn: Any, schemas: list[str], connection_id: str, position: Position | None = None,
                 tables: list[str] | None = None) -> tuple[CatalogSnapshot, dict[str, CaptureInstance]]:
    ph, args = _in(schemas)
    cur = conn.cursor()
    cur.execute(COLUMNS_SQL.format(schemas=ph), args)
    by: dict[tuple[str, str], TableInfo] = {}
    tracked: dict[tuple[str, str], bool] = {}
    computed: dict[tuple[str, str], set[str]] = {}
    rows = cur.fetchall()
    allow = table_filter(tables, {(r[0], r[1]) for r in rows}) if tables else None
    for sch, tab, oid, trk, col, colid, typ, maxlen, prec, scale, nullable, is_computed in rows:
        if is_computed:
            computed.setdefault((sch, tab), set()).add(col)
        if allow is not None and (sch, tab) not in allow:
            continue
        ti = by.get((sch, tab))
        if ti is None:
            ti = by[(sch, tab)] = TableInfo(table=TableRef(schema=sch, name=tab), engine_ids={"object_id": int(oid)})
            tracked[(sch, tab)] = bool(trk)
        canon = T.canonical_name(typ, prec, scale)
        ml = int(maxlen)
        if canon == "DECIMAL":
            length, sc = T.decimal_shape(typ, prec, scale)
        elif canon in ("STRING", "BINARY"):
            length = None if ml == -1 else (ml // 2 if typ.lower() in ("nchar", "nvarchar") else ml)
            if typ.lower() == "uniqueidentifier":
                length = 36
            sc = None
        elif canon in ("LOCALTIME", "LOCALDATETIME", "INSTANT"):
            length, sc = None, int(scale) if scale is not None else None
        else:
            length, sc = (int(prec) if prec else None), None
        etype = f"{typ}(max)" if (ml == -1 and typ.lower() in ("varchar", "nvarchar", "varbinary")) else typ
        ti.columns.append(ColumnInfo(name=col, ordinal=int(colid) - 1, engine_type=etype,
                                     canonical_type=CanonicalType(canon), length=length, scale=sc,
                                     nullable=bool(nullable)))
    cur.execute(PK_SQL.format(schemas=ph), args)
    for sch, tab, col, ko in cur.fetchall():
        ti = by.get((sch, tab))
        if ti:
            for c in ti.columns:
                if c.name == col:
                    c.key_seq = int(ko)
    instances: dict[str, CaptureInstance] = {}
    try:
        cur.execute(INSTANCES_SQL)
        rows = cur.fetchall()
    except Exception:  # noqa: BLE001 - database not CDC-enabled: no cdc schema
        rows = []
    newest: dict[tuple[str, str], str] = {}
    per_table: dict[tuple[str, str], list[str]] = {}
    for name, sch, tab, start_lsn, created, col, ordinal in rows:
        ti = by.get((sch, tab))
        if ti is None:
            continue
        ci = instances.get(name)
        if ci is None:
            ci = instances[name] = CaptureInstance(name=name, table=ti,
                                                   start_lsn=bytes(start_lsn) if start_lsn else None,
                                                   create_date=created, computed=set(computed.get((sch, tab), ())))
            newest[(sch, tab)] = name        # ordered by create_date: the newest instance of a table wins
            per_table.setdefault((sch, tab), []).append(name)
        ci.captured[col] = int(ordinal)
        c = next((x for x in ti.columns if x.name == col), None)
        if c is not None and is_lob_col(c):
            ci.lob_cols.add(col)
    # A table has at most two capture instances (schema change: the DBA creates the new one, then drops the old).
    # Both are read: the older one up to (excluding) the newer one's start_lsn, the newer one from its start_lsn -
    # no change is lost in the switch and none is applied twice (XC-32).
    for names in per_table.values():
        ordered = sorted(names, key=lambda n: (instances[n].start_lsn or b"", n))
        for older, newer in zip(ordered, ordered[1:]):
            instances[older].valid_to = instances[newer].start_lsn
    active = {n: ci for n, ci in instances.items() if newest.get((ci.table.table.schema_, ci.table.table.name)) == n
              or ci.valid_to is not None}
    for k, ti in by.items():
        ti.capture_enabled = tracked.get(k, False) and k in newest
    cat = CatalogSnapshot(connection_id=connection_id, engine=ENGINE, taken_at=datetime.now(timezone.utc),
                          position=position, tables=list(by.values()))
    return cat, active


def is_lob_col(c: ColumnInfo) -> bool:
    t = c.engine_type.lower()
    return t in T.LOB_TYPES or t.endswith("(max)")


# --------------------------------------------------------------------------- snapshot
def _orderable(c: ColumnInfo) -> bool:
    return not (is_lob_col(c) or c.canonical_type.value == "UNKNOWN" or c.engine_type.lower() in T.TEXT_FORM_TYPES)


def _after(keys: list[str]) -> str:
    ors = []
    for i in range(len(keys)):
        parts = [f"{qi(keys[j])} = %s" for j in range(i)] + [f"{qi(keys[i])} > %s"]
        ors.append("(" + " AND ".join(parts) + ")")
    return "(" + " OR ".join(ors) + ")"


def _after_args(vals: list[Any]) -> list[Any]:
    out: list[Any] = []
    for i in range(len(vals)):
        out.extend(vals[:i])
        out.append(vals[i])
    return out


def _sel(c: ColumnInfo) -> str:
    if c.engine_type.lower() in T.TEXT_FORM_TYPES:
        return f"CAST({qi(c.name)} AS nvarchar(max)) AS {qi(c.name)}"
    if c.canonical_type.value == "UNKNOWN":
        return f"CAST({qi(c.name)} AS nvarchar(4000)) AS {qi(c.name)}"
    if c.engine_type.lower() == "xml":
        return f"CAST({qi(c.name)} AS nvarchar(max)) AS {qi(c.name)}"
    return qi(c.name)


def snapshot_rows(conn: Any, table: TableInfo, key_range: tuple[Any, Any] | None, chunk_rows: int,
                  snapshot_isolation: bool = True) -> Iterator[list[dict[str, Any]]]:
    """Chunks ordered by primary key inside ONE transaction (SNAPSHOT isolation when the database allows it)."""
    keys = [c.name for c in table.key_columns()]
    cols = sorted(table.columns, key=lambda c: c.ordinal)
    names = [c.name for c in cols]
    src = f"{qi(table.table.schema_)}.{qi(table.table.name)}"
    sel = ", ".join(_sel(c) for c in cols)
    cur = conn.cursor()
    cur.execute("SET TRANSACTION ISOLATION LEVEL " + ("SNAPSHOT" if snapshot_isolation else "READ COMMITTED"))
    cur.execute("BEGIN TRANSACTION")
    try:
        lo = list(key_range[0]) if key_range and key_range[0] is not None else None
        hi = list(key_range[1]) if key_range and key_range[1] is not None else None
        offset = 0
        while True:
            args: list[Any] = []
            if keys:
                where = []
                if lo is not None:
                    where.append(_after(keys))
                    args += _after_args(lo)
                if hi is not None:
                    where.append("NOT " + _after(keys))
                    args += _after_args(hi)
                sql = (f"SELECT TOP ({int(chunk_rows)}) {sel} FROM {src}"
                       + (" WHERE " + " AND ".join(where) if where else "")
                       + " ORDER BY " + ", ".join(qi(k) for k in keys))
            else:
                order = ", ".join(qi(c.name) for c in cols if _orderable(c)) or "(SELECT NULL)"
                sql = (f"SELECT {sel} FROM {src} ORDER BY {order} OFFSET {offset} ROWS "
                       f"FETCH NEXT {int(chunk_rows)} ROWS ONLY")
            cur.execute(sql, tuple(args))
            rows = cur.fetchall()
            if not rows:
                break
            chunk = [{c.name: (T.convert(r[i], c.canonical_type.value, c.scale)
                               if c.canonical_type.value != "UNKNOWN" else r[i]) for i, c in enumerate(cols)}
                     for r in rows]
            if keys:
                lo = [rows[-1][names.index(k)] for k in keys]   # driver values: exact bind for the next chunk
            offset += len(rows)
            yield chunk
            if len(rows) < chunk_rows:
                break
    finally:
        try:
            cur.execute("IF @@TRANCOUNT > 0 COMMIT")
            cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        except Exception:  # noqa: BLE001
            pass
