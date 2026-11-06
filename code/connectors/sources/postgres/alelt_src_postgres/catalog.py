"""Catalog from pg_catalog (read-only). Works with any DB-API connection (psycopg2 / psycopg 3)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from alelt_common.models import CanonicalType, CatalogSnapshot, ColumnInfo, Position, TableInfo, TableRef

from .types import canonical_name, typmod_info

# generated columns are excluded (pgoutput does not send them)
# engine type of a column: base type name (domains resolved), arrays '<elem>[]', enums 'enum:<name>', ...
TYPE_SQL = """
WITH RECURSIVE dom(oid, base, typmod) AS (
    SELECT t.oid, t.oid, -1 FROM pg_type t
  UNION ALL
    SELECT d.oid, t.typbasetype, t.typtypmod FROM dom d JOIN pg_type t ON t.oid = d.base WHERE t.typtype = 'd'
)
SELECT d.oid, b.typname, b.typtype, b.typcategory, e.typname AS elem, d.typmod
  FROM dom d JOIN pg_type b ON b.oid = d.base LEFT JOIN pg_type e ON e.oid = b.typelem AND b.typcategory = 'A'
 WHERE b.typtype <> 'd' AND d.oid = ANY(%s)
"""

TABLES_SQL = """
SELECT c.oid, n.nspname, c.relname, c.relreplident, c.relkind
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relkind IN ('r', 'p') AND n.nspname = ANY(%s) AND NOT c.relispartition
 ORDER BY n.nspname, c.relname
"""

COLUMNS_SQL = """
SELECT a.attrelid, a.attnum, a.attname, a.atttypid, a.atttypmod, NOT a.attnotnull,
       pg_get_expr(ad.adbin, ad.adrelid)
  FROM pg_attribute a LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
 WHERE a.attrelid = ANY(%s) AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
 ORDER BY a.attrelid, a.attnum
"""

PK_SQL = """
SELECT i.indrelid, k.attnum, k.ord
  FROM pg_index i, unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
 WHERE i.indisprimary AND i.indrelid = ANY(%s)
"""

PUB_SQL = """
SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = %s
"""


def engine_type(row: tuple[Any, ...]) -> str:
    """row = (oid, typname, typtype, typcategory, elem typname, domain typmod)."""
    _, name, typtype, cat, elem, _ = row
    if cat == "A" and elem:
        return f"{elem}[]"
    if typtype == "e":
        return f"enum:{name}"
    if typtype == "c":
        return f"composite:{name}"
    if typtype == "r":
        return f"range:{name}"
    if typtype == "m":
        return f"multirange:{name}"
    if typtype == "b" and cat not in ("A", "P", "X") and canonical_name(str(name)) == "UNKNOWN":
        # extension / user-defined base type (citext, hstore, ltree, geometry, vector, ...): its documented text
        # output form is lossless and is what the type's own input function accepts (SRC-pg-03)
        return f"ext:{name}"
    return str(name)


def type_names(cur: Any, oids: list[int]) -> dict[int, tuple[str, int]]:
    """{oid: (engine_type, domain typmod or -1)}."""
    if not oids:
        return {}
    cur.execute(TYPE_SQL, (list(set(oids)),))
    return {int(r[0]): (engine_type(r), int(r[5])) for r in cur.fetchall()}


def publication_tables(cur: Any, publication: str) -> set[tuple[str, str]] | None:
    cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (publication,))
    if cur.fetchone() is None:
        return None
    cur.execute(PUB_SQL, (publication,))
    return {(str(s), str(t)) for s, t in cur.fetchall()}


def read_catalog(conn: Any, schemas: list[str], connection_id: str, publication: str | None,
                 position: str | None = None) -> CatalogSnapshot:
    cur = conn.cursor()
    cur.execute(TABLES_SQL, (schemas,))
    tabs = cur.fetchall()
    oids = [int(t[0]) for t in tabs]
    cols: dict[int, list[tuple[Any, ...]]] = {}
    pk: dict[int, dict[int, int]] = {}
    if oids:
        cur.execute(COLUMNS_SQL, (oids,))
        for r in cur.fetchall():
            cols.setdefault(int(r[0]), []).append(r)
        cur.execute(PK_SQL, (oids,))
        for rel, attnum, ord_ in cur.fetchall():
            pk.setdefault(int(rel), {})[int(attnum)] = int(ord_)
    types = type_names(cur, [int(r[3]) for rs in cols.values() for r in rs])
    pub = publication_tables(cur, publication) if publication else None
    tables = []
    for oid, ns, name, ident, _kind in tabs:
        cinfo = []
        for i, (_, attnum, an, tid, tmod, nullable, default) in enumerate(cols.get(int(oid), [])):
            et, dmod = types.get(int(tid), (f"oid:{tid}", -1))
            ln, sc = typmod_info(et, int(tmod) if int(tmod) >= 0 else dmod)
            cinfo.append(ColumnInfo(name=str(an), ordinal=i, engine_type=et,
                                    canonical_type=CanonicalType(canonical_name(et)), length=ln, scale=sc,
                                    nullable=bool(nullable), key_seq=pk.get(int(oid), {}).get(int(attnum)),
                                    default=str(default) if default is not None else None))
        tables.append(TableInfo(table=TableRef(schema=str(ns), name=str(name)), engine_ids={"oid": int(oid)},
                                capture_enabled=bool(pub is not None and (str(ns), str(name)) in pub),
                                compression=None, columns=cinfo))
        tables[-1].engine_ids["replident"] = ord(str(ident))
    cur.close()
    return CatalogSnapshot(connection_id=connection_id, engine="postgres", taken_at=datetime.now(timezone.utc),
                           position=Position(engine="postgres", text=position) if position else None,
                           database_codepage=1208, tables=tables)
