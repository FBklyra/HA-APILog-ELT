"""Db2 catalog snapshot (SYSCAT, read-only, WITH UR) -> CatalogSnapshot-shaped dict / model.

read_catalog_dict() is stdlib only (runs on the x64 Python next to the Db2 client);
read_catalog() returns the alelt_common.models.CatalogSnapshot (needs pydantic + hub/ on sys.path).
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from . import types as T
from .logdecode import TableSpec
from .rowimage import ColumnSpec

if TYPE_CHECKING:
    from alelt_common.models import CatalogSnapshot

    from .db2api import Db2Connection


def _in(schemas: Sequence[str]) -> str:
    for s in schemas:
        if not s.replace("_", "").replace("#", "").replace("$", "").replace("@", "").isalnum():
            raise ValueError(f"invalid schema name {s!r}")
    return ",".join(f"'{s.upper()}'" for s in schemas)


def read_catalog_dict(conn: Db2Connection, schemas: Sequence[str], connection_id: str = "db2") -> dict[str, Any]:
    """Snapshot tables of the given schemas. Commits, then QUERYs the LRI the snapshot is valid at."""
    flt = _in(schemas)
    tabs = conn.execute(
        "SELECT RTRIM(TABSCHEMA), TABNAME, TBSPACEID, TABLEID, DATACAPTURE, COMPRESSION, ROWCOMPMODE "
        f"FROM SYSCAT.TABLES WHERE TYPE='T' AND TABSCHEMA IN ({flt}) ORDER BY 1,2 WITH UR",
        ctypes=["str", "str", "int", "int", "str", "str", "str"])
    cols = conn.execute(
        "SELECT RTRIM(c.TABSCHEMA), c.TABNAME, c.COLNAME, c.COLNO, "
        "COALESCE(d.SOURCENAME, c.TYPENAME), c.LENGTH, c.SCALE, c.NULLS, c.CODEPAGE, c.DEFAULT "
        "FROM SYSCAT.COLUMNS c LEFT JOIN SYSCAT.DATATYPES d ON d.TYPESCHEMA = c.TYPESCHEMA "
        "AND d.TYPENAME = c.TYPENAME AND d.METATYPE = 'T' "
        f"WHERE c.TABSCHEMA IN ({flt}) ORDER BY 1,2,4 WITH UR",
        ctypes=["str", "str", "str", "int", "str", "int", "int", "str", "int", "str"])
    pk = conn.execute(
        "SELECT RTRIM(k.TABSCHEMA), k.TABNAME, k.COLNAME, k.COLSEQ FROM SYSCAT.KEYCOLUSE k "
        "JOIN SYSCAT.TABCONST t ON t.CONSTNAME = k.CONSTNAME AND t.TABSCHEMA = k.TABSCHEMA "
        f"AND t.TABNAME = k.TABNAME WHERE t.TYPE = 'P' AND k.TABSCHEMA IN ({flt}) WITH UR",
        ctypes=["str", "str", "str", "int"])
    uq = conn.execute(   # fallback key: first unique index (by name) when no primary key
        "SELECT RTRIM(i.TABSCHEMA), i.TABNAME, i.INDNAME, u.COLNAME, u.COLSEQ FROM SYSCAT.INDEXES i "
        "JOIN SYSCAT.INDEXCOLUSE u ON u.INDSCHEMA = i.INDSCHEMA AND u.INDNAME = i.INDNAME "
        f"WHERE i.UNIQUERULE = 'U' AND i.INDEXTYPE IN ('REG', 'CLUS') AND i.TABSCHEMA IN ({flt}) "
        "ORDER BY 1,2,3,5 WITH UR",
        ctypes=["str", "str", "str", "str", "int"])
    parts = conn.execute(   # range-partitioned tables: DML records carry the data partition's object id
        "SELECT RTRIM(TABSCHEMA), TABNAME, DATAPARTITIONID, TBSPACEID, PARTITIONOBJECTID FROM SYSCAT.DATAPARTITIONS "
        f"WHERE TABSCHEMA IN ({flt}) ORDER BY 1,2,3 WITH UR", ctypes=["str", "str", "int", "int", "int"])
    cp = conn.execute("SELECT VALUE FROM SYSIBMADM.DBCFG WHERE NAME = 'codepage' WITH UR")
    conn.commit()
    _, pos = conn.query_lri()

    keys: dict[tuple[str, str], dict[str, int]] = {}
    for s, t, c, seq in pk:
        keys.setdefault((s, t), {})[c] = seq
    has_pk = set(keys)
    first_uq: dict[tuple[str, str], str] = {}
    for s, t, ind, c, seq in uq:
        if (s, t) in has_pk or first_uq.setdefault((s, t), ind) != ind:
            continue
        keys.setdefault((s, t), {})[c] = seq
    by_tab: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for s, t, name, colno, typ, ln, sc, nulls, cpg, dflt in cols:
        typ = T.norm(typ)
        by_tab.setdefault((s, t), []).append({
            "name": name, "ordinal": colno, "engine_type": typ,
            "canonical_type": T.canonical_name(typ, cpg), "length": ln, "scale": sc,
            "nullable": nulls == "Y", "key_seq": keys.get((s, t), {}).get(name),
            "codepage": cpg, "default": dflt})
    part_ids: dict[tuple[str, str], dict[str, int]] = {}
    for s, t, dpid, ptbsp, pobj in parts:
        part_ids.setdefault((s, t), {})[f"{PART_PREFIX}{dpid}"] = ptbsp * 65536 + pobj
    tables = []
    for s, t, tbsp, tid, dcc, comp, rowcomp in tabs:
        ids = {"tbspaceid": tbsp, "tableid": tid}
        if tbsp < 0 or tid < 0:             # partitioned: SYSCAT.TABLES ids are placeholders
            ids.update(part_ids.get((s, t), {}))
        tables.append({
            "table": {"schema": s, "name": t},
            "engine_ids": ids,
            "capture_enabled": dcc in ("Y", "L"),
            "compression": compression_text(comp, rowcomp),
            "columns": by_tab.get((s, t), [])})
    return {"connection_id": connection_id, "engine": "db2",
            "taken_at": datetime.now(timezone.utc).isoformat(),
            "position": {"engine": "db2", "text": pos},
            "database_codepage": int(cp[0][0]) if cp else None,
            "tables": tables}


def read_catalog(conn: Db2Connection, schemas: Sequence[str], connection_id: str = "db2") -> CatalogSnapshot:
    from alelt_common.models import CatalogSnapshot

    return CatalogSnapshot.model_validate(read_catalog_dict(conn, schemas, connection_id))


def compression_text(comp: str | None, rowcomp: str | None) -> str | None:
    """SYSCAT.TABLES COMPRESSION / ROWCOMPMODE -> '<COMPRESSION>[/<ROWCOMPMODE>]' (None when empty). Every edge
    catalog produces the same text (XC-24: edge/tests/catalog_golden.tsv)."""
    c, m = (comp or "").strip(), (rowcomp or "").strip()
    return (c + ("/" + m if m else "")) if c else None


PART_PREFIX = "part."   # engine_ids["part.<DATAPARTITIONID>"] = TBSPACEID * 65536 + PARTITIONOBJECTID


def specs_from_dict(cat: Mapping[str, Any]) -> dict[tuple[int, int], TableSpec]:
    """CatalogSnapshot (as dict, aliases) -> decoder table specs keyed by (tbspaceid, tableid)."""
    out: dict[tuple[int, int], TableSpec] = {}
    for t in cat["tables"]:
        ids = t.get("engine_ids") or {}
        if "tbspaceid" not in ids or "tableid" not in ids:
            continue
        cols = tuple(ColumnSpec(c["name"], T.norm(c["engine_type"]), int(c.get("length") or 0),
                                int(c.get("scale") or 0), bool(c.get("nullable", True)),
                                int(c.get("codepage") or 0))
                     for c in sorted(t["columns"], key=lambda c: c["ordinal"]))
        comp = (t.get("compression") or "").split("/")[0] or None
        key = tuple(c["name"] for c in sorted((c for c in t["columns"] if c.get("key_seq")), key=lambda c: c["key_seq"]))
        spec = TableSpec(t["table"]["schema"], t["table"]["name"], int(ids["tbspaceid"]), int(ids["tableid"]),
                         cols, comp, key)
        parts = [int(v) for k, v in ids.items() if k.startswith(PART_PREFIX)]
        if spec.tbspaceid >= 0 and spec.tableid >= 0 or not parts:
            out[(spec.tbspaceid, spec.tableid)] = spec    # negative ids without partitions: flagged by the decoder
        for v in parts:
            out[(v // 65536, v % 65536)] = spec
    return out
