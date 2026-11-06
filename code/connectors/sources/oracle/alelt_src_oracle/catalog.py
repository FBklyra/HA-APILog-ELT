"""Oracle catalog (ALL_TAB_COLUMNS / ALL_CONSTRAINTS / ALL_LOG_GROUPS) and AS OF SCN snapshot reads. Read-only."""
from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

from alelt_common.models import CanonicalType, CatalogSnapshot, ColumnInfo, Position, TableInfo, TableRef

from . import types as T

ENGINE = "oracle"


def q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _binds(prefix: str, vals: list[str]) -> tuple[str, dict[str, str]]:
    names = [f"{prefix}{i}" for i in range(len(vals))]
    return ", ".join(":" + n for n in names) or "NULL", dict(zip(names, vals))


def read_catalog(conn: Any, schemas: list[str], connection_id: str, position: Position | None = None,
                 tables: list[str] | None = None) -> CatalogSnapshot:
    owners = [s.upper() for s in schemas]
    ob, binds = _binds("o", owners)
    cur = conn.cursor()
    cur.execute(
        "SELECT C.OWNER, C.TABLE_NAME, C.COLUMN_NAME, C.COLUMN_ID, C.DATA_TYPE, C.DATA_LENGTH, C.CHAR_LENGTH, "
        "C.DATA_PRECISION, C.DATA_SCALE, C.NULLABLE, C.CHAR_USED "
        "FROM ALL_TAB_COLUMNS C JOIN ALL_TABLES T ON T.OWNER = C.OWNER AND T.TABLE_NAME = C.TABLE_NAME "
        f"WHERE C.OWNER IN ({ob}) AND T.TEMPORARY = 'N' AND T.NESTED = 'NO' AND T.DROPPED = 'NO' "
        "AND T.SECONDARY = 'N' AND (T.IOT_TYPE IS NULL OR T.IOT_TYPE = 'IOT') "
        "ORDER BY C.OWNER, C.TABLE_NAME, C.COLUMN_ID", binds)
    by: dict[tuple[str, str], TableInfo] = {}
    for owner, tab, col, colid, dtype, dlen, clen, prec, scale, nullable, char_used in cur:
        if tables and f"{owner}.{tab}" not in tables and tab not in tables:
            continue
        ti = by.get((owner, tab))
        if ti is None:
            ti = by[(owner, tab)] = TableInfo(table=TableRef(schema=owner, name=tab))
        canon = T.canonical_name(dtype, prec, scale)
        if canon == "DECIMAL":
            length, sc = T.decimal_shape(dtype, prec, scale)
        elif canon in ("STRING",) and T.base_type(dtype) not in T.LOB_TYPES:
            length, sc = (int(clen) if clen else (int(dlen) if dlen else None)), None
        elif canon == "BINARY" and T.base_type(dtype) not in T.LOB_TYPES:
            length, sc = (int(dlen) if dlen else None), None
        else:
            length, sc = (int(prec) if prec is not None else None), (int(scale) if scale is not None else None)
        ti.columns.append(ColumnInfo(name=col, ordinal=int(colid) - 1, engine_type=dtype,
                                     canonical_type=CanonicalType(canon), length=length, scale=sc,
                                     nullable=nullable == "Y"))
    # primary keys
    cur.execute(
        "SELECT CC.OWNER, CC.TABLE_NAME, CC.COLUMN_NAME, CC.POSITION FROM ALL_CONSTRAINTS K "
        "JOIN ALL_CONS_COLUMNS CC ON CC.OWNER = K.OWNER AND CC.CONSTRAINT_NAME = K.CONSTRAINT_NAME "
        f"WHERE K.CONSTRAINT_TYPE = 'P' AND K.OWNER IN ({ob})", binds)
    for owner, tab, col, pos in cur:
        ti = by.get((owner, tab))
        if ti:
            for c in ti.columns:
                if c.name == col:
                    c.key_seq = int(pos)
    # object ids
    cur.execute(f"SELECT OWNER, OBJECT_NAME, OBJECT_ID FROM ALL_OBJECTS WHERE OBJECT_TYPE = 'TABLE' "
                f"AND OWNER IN ({ob})", binds)
    for owner, tab, oid in cur:
        ti = by.get((owner, tab))
        if ti:
            ti.engine_ids = {"object_id": int(oid)}
    # supplemental logging: (ALL) COLUMNS at table level or database level
    db_all = False
    try:
        cur.execute("SELECT SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_ALL FROM V$DATABASE")
        mn, al = cur.fetchone()
        db_all = str(al) == "YES" and str(mn) in ("YES", "IMPLICIT")
        db_min = str(mn) in ("YES", "IMPLICIT")
    except Exception:  # noqa: BLE001 - no access to V$DATABASE: rely on table log groups only
        db_min = True
    cur.execute(f"SELECT OWNER, TABLE_NAME, LOG_GROUP_TYPE FROM ALL_LOG_GROUPS WHERE OWNER IN ({ob})", binds)
    groups: dict[tuple[str, str], set[str]] = {}
    for owner, tab, gtype in cur:
        groups.setdefault((owner, tab), set()).add(str(gtype))
    for k, ti in by.items():
        g = groups.get(k, set())
        ti.capture_enabled = db_min and (db_all or "ALL COLUMN LOGGING" in g)
        ti.compression = ",".join(sorted(g)) or None   # informational: supplemental log groups present
    return CatalogSnapshot(connection_id=connection_id, engine=ENGINE, taken_at=datetime.now(timezone.utc),
                           position=position, tables=list(by.values()))


# --------------------------------------------------------------------------- snapshot
def _select_expr(c: ColumnInfo) -> str:
    bt = T.base_type(c.engine_type)
    if bt.startswith("INTERVAL"):
        return f"TO_CHAR({q(c.name)})"
    if bt in ("TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"):
        return f"SYS_EXTRACT_UTC(CAST({q(c.name)} AS TIMESTAMP WITH TIME ZONE))"
    if bt in ("XMLTYPE",):
        return f"XMLSERIALIZE(CONTENT {q(c.name)} AS CLOB)"
    if bt == "JSON":
        return f"JSON_SERIALIZE({q(c.name)} RETURNING CLOB)"
    return q(c.name)


def output_type_handler(cursor: Any, metadata: Any) -> Any:
    """NUMBER as Decimal (exact), LOBs as str/bytes."""
    import decimal

    import oracledb

    if metadata.type_code is oracledb.DB_TYPE_NUMBER:
        return cursor.var(decimal.Decimal, arraysize=cursor.arraysize)
    if metadata.type_code is oracledb.DB_TYPE_CLOB or metadata.type_code is oracledb.DB_TYPE_NCLOB:
        return cursor.var(oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize)
    if metadata.type_code is oracledb.DB_TYPE_BLOB:
        return cursor.var(oracledb.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)
    return None


def _after(keys: list[str], last: list[Any], prefix: str) -> tuple[str, dict[str, Any]]:
    """Row-value comparison (k1,k2,..) > (v1,v2,..) expanded (Oracle has no tuple '>')."""
    ors, binds = [], {}
    for i in range(len(keys)):
        parts = [f"{q(keys[j])} = :{prefix}{j}" for j in range(i)] + [f"{q(keys[i])} > :{prefix}{i}"]
        ors.append("(" + " AND ".join(parts) + ")")
    for j, v in enumerate(last):
        binds[f"{prefix}{j}"] = v
    return "(" + " OR ".join(ors) + ")", binds


def _upto(keys: list[str], to: list[Any], prefix: str) -> tuple[str, dict[str, Any]]:
    """(k...) <= (v...)  ==  NOT ((k...) > (v...))"""
    cond, binds = _after(keys, to, prefix)
    return f"NOT {cond}", binds


def snapshot_rows(conn: Any, table: TableInfo, key_range: tuple[Any, Any] | None, chunk_rows: int,
                  as_of_scn: int) -> Iterator[list[dict[str, Any]]]:
    """Chunks ordered by primary key (ROWID for keyless tables), all chunks read AS OF the same SCN (consistent)."""
    keys = [c.name for c in table.key_columns()]
    cols = sorted(table.columns, key=lambda c: c.ordinal)
    sel = ", ".join(f"{_select_expr(c)} AS {q(c.name)}" for c in cols)
    src = f"{q(table.table.schema_)}.{q(table.table.name)} AS OF SCN :scn"
    keyless = not keys
    order = "ROWID" if keyless else ", ".join(q(k) for k in keys)
    if keyless:
        sel += ", ROWIDTOCHAR(ROWID) AS ALELT_ROWID__"
    lo = list(key_range[0]) if key_range and key_range[0] is not None else None
    hi = list(key_range[1]) if key_range and key_range[1] is not None else None
    cur = conn.cursor()
    cur.outputtypehandler = output_type_handler
    cur.arraysize = min(max(chunk_rows, 100), 10000)
    while True:
        where, binds = [], {"scn": as_of_scn, "n": chunk_rows}
        if lo is not None:
            if keyless:
                where.append("ROWID > CHARTOROWID(:r0)")
                binds["r0"] = lo[0]
            else:
                c, b = _after(keys, lo, "a")
                where.append(c)
                binds.update(b)
        if hi is not None and not keyless:
            c, b = _upto(keys, hi, "b")
            where.append(c)
            binds.update(b)
        sql = (f"SELECT {sel} FROM {src}" + (" WHERE " + " AND ".join(where) if where else "")
               + f" ORDER BY {order} FETCH FIRST :n ROWS ONLY")
        cur.execute(sql, binds)
        names = [d[0] for d in cur.description]
        rows = cur.fetchall()
        if not rows:
            return
        chunk: list[dict[str, Any]] = []
        for r in rows:
            raw = dict(zip(names, r))
            out = {c.name: T.convert_driver(raw[c.name], c.canonical_type.value, c.scale) for c in cols}
            chunk.append(out)
        last = rows[-1]
        lo = [dict(zip(names, last))["ALELT_ROWID__"]] if keyless else [dict(zip(names, last))[k] for k in keys]
        yield chunk
        if len(rows) < chunk_rows:
            return
