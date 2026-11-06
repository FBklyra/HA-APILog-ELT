"""SYSCAT catalog snapshot (WITH UR) -> CatalogSnapshot-shaped dict (same shape as alelt_src_db2's catalog)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .db2 import Db2, Db2Error

_CANON = {"SMALLINT": "SHORT", "INTEGER": "INT", "BIGINT": "LONG", "DECIMAL": "DECIMAL", "REAL": "FLOAT",
          "DOUBLE": "DOUBLE", "DATE": "LOCALDATE", "TIME": "LOCALTIME", "TIMESTAMP": "LOCALDATETIME",
          "BOOLEAN": "BOOLEAN", "CHARACTER": "STRING", "VARCHAR": "STRING", "GRAPHIC": "STRING",
          "VARGRAPHIC": "STRING", "CLOB": "STRING", "DBCLOB": "STRING", "BINARY": "BINARY", "VARBINARY": "BINARY",
          "BLOB": "BINARY", "XML": "XML", "LONG VARCHAR": "STRING", "LONG VARGRAPHIC": "STRING"}
_ALIASES = {"CHAR": "CHARACTER", "INT": "INTEGER", "FLOAT": "DOUBLE", "NUMERIC": "DECIMAL", "DEC": "DECIMAL",
            "CHARACTER VARYING": "VARCHAR"}


def compression_text(comp: Optional[str], rowcomp: Optional[str]) -> Optional[str]:
    """Same text as the hub catalog and the C plugin (XC-24: edge/tests/catalog_golden.tsv)."""
    c, m = (comp or "").strip(), (rowcomp or "").strip()
    return (c + ("/" + m if m else "")) if c else None


def _canon(t: str, cp: Optional[int]) -> str:
    c = _CANON.get(t, "UNKNOWN")      # authoritative mapping is re-applied on the hub (Db2Source)
    return "BINARY" if c == "STRING" and cp == 0 and t in ("CHARACTER", "VARCHAR", "CLOB") else c


def check_schemas(schemas: List[str]) -> List[str]:
    for s in schemas:
        if not s.replace("_", "").replace("#", "").replace("$", "").replace("@", "").isalnum():
            raise ValueError(f"invalid schema name {s!r}")
    return [s.upper() for s in schemas]


def read_catalog(db: Db2, schemas: List[str], connection_id: str) -> Dict[str, Any]:
    flt = ",".join(f"'{s}'" for s in check_schemas(schemas))
    tabs = db.query("SELECT RTRIM(TABSCHEMA), TABNAME, TBSPACEID, TABLEID, DATACAPTURE, COMPRESSION, ROWCOMPMODE "
                    f"FROM SYSCAT.TABLES WHERE TYPE='T' AND TABSCHEMA IN ({flt}) ORDER BY 1,2 WITH UR")
    cols = db.query("SELECT RTRIM(c.TABSCHEMA), c.TABNAME, c.COLNAME, c.COLNO, COALESCE(d.SOURCENAME, c.TYPENAME), "
                    "c.LENGTH, c.SCALE, c.NULLS, c.CODEPAGE, c.DEFAULT FROM SYSCAT.COLUMNS c "
                    "LEFT JOIN SYSCAT.DATATYPES d ON d.TYPESCHEMA = c.TYPESCHEMA AND d.TYPENAME = c.TYPENAME "
                    f"AND d.METATYPE = 'T' WHERE c.TABSCHEMA IN ({flt}) ORDER BY 1,2,4 WITH UR")
    pk = db.query("SELECT RTRIM(k.TABSCHEMA), k.TABNAME, k.COLNAME, k.COLSEQ FROM SYSCAT.KEYCOLUSE k "
                  "JOIN SYSCAT.TABCONST t ON t.CONSTNAME = k.CONSTNAME AND t.TABSCHEMA = k.TABSCHEMA "
                  f"AND t.TABNAME = k.TABNAME WHERE t.TYPE = 'P' AND k.TABSCHEMA IN ({flt}) WITH UR")
    uq = db.query("SELECT RTRIM(i.TABSCHEMA), i.TABNAME, i.INDNAME, u.COLNAME, u.COLSEQ FROM SYSCAT.INDEXES i "
                  "JOIN SYSCAT.INDEXCOLUSE u ON u.INDSCHEMA = i.INDSCHEMA AND u.INDNAME = i.INDNAME "
                  f"WHERE i.UNIQUERULE = 'U' AND i.INDEXTYPE IN ('REG', 'CLUS') AND i.TABSCHEMA IN ({flt}) "
                  "ORDER BY 1,2,3,5 WITH UR")
    # range-partitioned tables: DML records carry the data partition's object id (= alelt_src_db2 / C plugin)
    parts = db.query("SELECT RTRIM(TABSCHEMA), TABNAME, DATAPARTITIONID, TBSPACEID, PARTITIONOBJECTID "
                     f"FROM SYSCAT.DATAPARTITIONS WHERE TABSCHEMA IN ({flt}) ORDER BY 1,2,3 WITH UR")
    try:
        cp = db.query("SELECT VALUE FROM SYSIBMADM.DBCFG WHERE NAME = 'codepage' WITH UR")
    except Db2Error:
        cp = []
    db.commit()                                     # V2: COMMIT before db2ReadLog
    _, pos = db.query_lri()
    keys: Dict[Tuple[str, str], Dict[str, int]] = {}
    for s, t, c, seq in pk:
        keys.setdefault((s or "", t or ""), {})[c or ""] = int(seq or 0)
    has_pk, first_uq = set(keys), {}  # type: ignore[var-annotated]
    for s, t, ind, c, seq in uq:
        k = (s or "", t or "")
        if k in has_pk or first_uq.setdefault(k, ind) != ind:
            continue
        keys.setdefault(k, {})[c or ""] = int(seq or 0)
    by_tab: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for s, t, name, colno, typ, ln, sc, nulls, cpg, dflt in cols:
        tn = (typ or "").strip().upper()
        tn = _ALIASES.get(tn, tn)
        cpi = int(cpg) if cpg is not None else None
        by_tab.setdefault((s or "", t or ""), []).append({
            "name": name, "ordinal": int(colno or 0), "engine_type": tn, "canonical_type": _canon(tn, cpi),
            "length": int(ln) if ln is not None else None, "scale": int(sc) if sc is not None else None,
            "nullable": nulls == "Y", "key_seq": keys.get((s or "", t or ""), {}).get(name or ""),
            "codepage": cpi, "default": dflt})
    part_ids: Dict[Tuple[str, str], Dict[str, int]] = {}
    for s, t, dpid, ptbsp, pobj in parts:
        part_ids.setdefault((s or "", t or ""), {})[f"part.{int(dpid)}"] = int(ptbsp) * 65536 + int(pobj)
    tables = []
    for s, t, tbsp, tid, dcc, comp, rowcomp in tabs:
        comp_s = (comp or "").strip()
        rc = (rowcomp or "").strip()
        tables.append({"table": {"schema": s, "name": t},
                       "engine_ids": {"tbspaceid": int(tbsp or 0), "tableid": int(tid or 0),
                                      **(part_ids.get((s or "", t or ""), {})
                                         if int(tbsp or 0) < 0 or int(tid or 0) < 0 else {})},
                       "capture_enabled": dcc in ("Y", "L"),
                       "compression": compression_text(comp_s, rc),
                       "columns": by_tab.get((s or "", t or ""), [])})
    return {"connection_id": connection_id, "engine": "db2", "taken_at": datetime.now(timezone.utc).isoformat(),
            "position": {"engine": "db2", "text": pos},
            "database_codepage": int(cp[0][0]) if cp and cp[0][0] else None, "tables": tables}
