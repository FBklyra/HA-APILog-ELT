"""VERIFY_TABLE for Python Db2 edges (dev edge, aleltd) - the same statements as the C plugin
(connectors/sources/db2/edge/src/db2_build.c; tests compare the text), read-only (R1):

- catalog lookups: SYSCAT ... WITH UR with parameter markers;
- table data: SELECT ... WITH CS only (cursor stability; with CUR_COMMIT=ON readers see the currently committed
  version and never wait on, or return, uncommitted rows - F-04); identifiers come from SYSCAT and are quoted;
  every value is a parameter marker; statements are built from fixed fragments, never from command text (R5).
"""
from __future__ import annotations

import os
import sys
import time
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

Q_TABLE = "SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = ? AND TYPE = 'T' WITH UR"
Q_META = ("SELECT COLNAME, KEYSEQ, TYPENAME, CODEPAGE FROM SYSCAT.COLUMNS WHERE TABSCHEMA = ? AND TABNAME = ? "
          "ORDER BY COLNO WITH UR")
Q_UNIQUE_TAB = ("SELECT I.INDNAME, U.COLNAME, U.COLSEQ FROM SYSCAT.INDEXES I JOIN SYSCAT.INDEXCOLUSE U "
                "ON U.INDSCHEMA = I.INDSCHEMA AND U.INDNAME = I.INDNAME WHERE I.UNIQUERULE = 'U' "
                "AND I.INDEXTYPE IN ('REG', 'CLUS') AND I.TABSCHEMA = ? AND I.TABNAME = ? "
                "ORDER BY I.INDNAME, U.COLSEQ WITH UR")
ISO_CS = " WITH CS"
AGG_ITEMS_MAX = 900
MAX_RESULT_BYTES = 8 * 1024 * 1024
# conversion errors of a key value (too long / not a number / not a date): such a key cannot exist
KEY_CANNOT_EXIST = ("22001", "22003", "22005", "22007", "22008", "22018")


def _validator() -> Any:
    try:
        from alelt_common import verify_edge
    except ImportError:                      # dev edge / daemon next to the hub tree
        here = os.path.dirname(os.path.abspath(__file__))
        hub = os.path.normpath(os.path.join(here, "..", "..", "..", "..", "..", "hub"))
        if os.path.isdir(os.path.join(hub, "alelt_common")) and hub not in sys.path:
            sys.path.append(hub)
        from alelt_common import verify_edge
    return verify_edge


# ------------------------------------------------------------------ statement builders (= db2_build.c)
def agg_class(typename: str, codepage: Optional[int]) -> str:
    t = typename.strip().upper()
    if t in ("SMALLINT", "INTEGER", "BIGINT"):
        return "int"
    if t in ("DECIMAL", "NUMERIC"):
        return "dec"
    if t in ("REAL", "DOUBLE", "FLOAT", "DECFLOAT"):
        return "flt"
    if t in ("DATE", "TIME", "TIMESTAMP"):
        return "tmp"
    if t in ("CHARACTER", "VARCHAR", "GRAPHIC", "VARGRAPHIC") and codepage not in (0, None):
        return "str"
    return "cnt"


ITEMS = {"int": 4, "dec": 4, "flt": 3, "tmp": 3, "str": 2, "cnt": 1}


def q(ident: str) -> str:
    if not ident or any(ord(ch) < 0x20 or ch == ";" for ch in ident):
        raise ValueError("refused identifier")
    return '"' + ident.replace('"', '""') + '"'


def built_ok(sql: str) -> bool:
    return sql.startswith("SELECT ") and sql.endswith(ISO_CS) and ";" not in sql


def _from(schema: str, table: str) -> str:
    return f" FROM {q(schema)}.{q(table)}"


def key_where(keys: Sequence[str], has_after: bool, has_to: bool) -> Tuple[str, List[int]]:
    """WHERE <after exclusive> AND <to inclusive>; marker table: k = after[k], 100 + k = to[k]."""
    sql, pt = "", []
    if has_after or has_to:
        sql += " WHERE "
    if has_after:
        parts = []
        for i in range(len(keys)):
            p = "("
            for j in range(i):
                p += f"{q(keys[j])} = ? AND "
                pt.append(j)
            p += f"{q(keys[i])} > ?)"
            pt.append(i)
            parts.append(p)
        sql += "(" + " OR ".join(parts) + ")"
    if has_to:
        if has_after:
            sql += " AND "
        parts = []
        for i in range(len(keys) + 1):
            last = i == len(keys)
            eqs = []
            for j in range(len(keys) if last else i):
                eqs.append(f"{q(keys[j])} = ?")
                pt.append(100 + j)
            p = " AND ".join(eqs)
            if not last:
                p = (p + " AND " if p else "") + f"{q(keys[i])} < ?"
                pt.append(100 + i)
            parts.append("(" + p + ")")
        sql += "(" + " OR ".join(parts) + ")"
    return sql, pt


def agg_sql(schema: str, table: str, keys: Sequence[str], cols: Sequence[Tuple[str, str]], has_after: bool,
            has_to: bool) -> Tuple[str, List[int]]:
    items = ["COUNT(*)"]
    for c, cls in cols:
        if cls in ("int", "dec", "flt", "tmp"):
            s = f"COUNT({q(c)}),MIN({q(c)}),MAX({q(c)})"
            if cls == "int":
                s += f",SUM(CAST({q(c)} AS DECIMAL(31,0)))"
            elif cls == "dec":
                s += f",SUM({q(c)})"
        elif cls == "str":
            s = f"COUNT({q(c)}),SUM(CAST(CHARACTER_LENGTH(RTRIM({q(c)}),CODEUNITS32) AS DECIMAL(31,0)))"
        else:
            s = f"SUM(CASE WHEN {q(c)} IS NULL THEN 0 ELSE 1 END)"
        items.append(s)
    w, pt = key_where(keys, has_after, has_to)
    sql = "SELECT " + ",".join(items) + _from(schema, table) + w + ISO_CS
    if not built_ok(sql):
        raise ValueError("refused statement")
    return sql, pt


def keys_sql(schema: str, table: str, keys: Sequence[str], has_after: bool, has_to: bool,
             limit: int) -> Tuple[str, List[int]]:
    w, pt = key_where(keys, has_after, has_to)
    sql = ("SELECT " + ",".join(q(k) for k in keys) + _from(schema, table) + w + " ORDER BY "
           + ",".join(q(k) for k in keys) + f" FETCH FIRST {int(limit)} ROWS ONLY" + ISO_CS)
    if not built_ok(sql):
        raise ValueError("refused statement")
    return sql, pt


def rows_sql(schema: str, table: str, keys: Sequence[str], cols: Sequence[str]) -> Tuple[str, List[int]]:
    sql = ("SELECT " + ",".join(q(c) for c in cols) + _from(schema, table) + " WHERE "
           + " AND ".join(f"{q(k)} = ?" for k in keys) + ISO_CS)
    if not built_ok(sql):
        raise ValueError("refused statement")
    return sql, list(range(len(keys)))


def snapshot_sql(schema: str, table: str, cols: Sequence[str], keys: Sequence[str], has_after: bool, has_to: bool,
                 chunk_rows: int) -> Tuple[str, List[int]]:
    w, pt = key_where(keys, has_after, has_to)
    sql = ("SELECT " + ",".join(q(c) for c in cols) + _from(schema, table) + w + " ORDER BY "
           + ",".join(q(k) for k in keys) + f" FETCH FIRST {int(chunk_rows)} ROWS ONLY" + ISO_CS)
    if not built_ok(sql):
        raise ValueError("refused statement")
    return sql, pt


SNAPSHOT_CHUNK_ROWS_MAX = 100_000
SNAPSHOT_CHUNK_BYTES = 8 * 1024 * 1024        # = the C plugin byte cap: next chunk continues after the last key


def valid_allow_entry(entry: str) -> bool:
    """Allow-list entry syntax (all agents, AGENT-COMMANDS.md §allow-list): SCHEMA.TABLE or SCHEMA.* ; a schema
    wildcard (`*.*`, `*.T`) is refused at config load."""
    s, dot, t = str(entry).strip().partition(".")
    return bool(dot) and bool(s) and bool(t) and "*" not in s and (t == "*" or "*" not in t)


def table_allowed(allowed: Sequence[str], schema: str, table: str) -> bool:
    """Local allow-list (`allowed_tables`): exact "SCHEMA.TABLE" entries (case-insensitive) or "SCHEMA.*" for every
    base table of a schema. Identical matcher in the three agents (shared test vectors)."""
    fq = f"{schema}.{table}".upper()
    for a in allowed:
        a = str(a).strip().upper()
        if not valid_allow_entry(a):
            continue
        if a == fq or (a.endswith(".*") and a[:-2] == schema.upper()):
            return True
    return False


def snapshot_chunks(db: Any, schema: str, table: str, chunk_rows: int, max_bytes: int = SNAPSHOT_CHUNK_BYTES,
                    key_from: Optional[Sequence[Any]] = None
                    ) -> Iterator[Tuple[List[Dict[str, Any]], List[str], bool, Optional[List[str]]]]:
    """SNAPSHOT_TABLE reads (= C plugin p_snapshot_chunk): keyset paging on the primary key, SELECT ... WITH CS,
    values in their CLI text form. Yields (rows, key_from of the chunk, last, last_key = key of the chunk's last row).
    `key_from` resumes after that key (agent command contract: a re-issued snapshot continues where the last loaded
    chunk ended). A keyless table is read by one statement and cut into chunks (no key to page on: no resume)."""
    if not 0 < int(chunk_rows) <= SNAPSHOT_CHUNK_ROWS_MAX:
        chunk_rows = SNAPSHOT_CHUNK_ROWS_MAX
    meta, keys = table_meta(db, schema, table)
    cols = [c for c, _, _ in meta]
    if not keys:
        if key_from:
            raise ValueError("key_from given for a table without a primary key")
        sql = "SELECT " + ",".join(q(c) for c in cols) + _from(schema, table) + ISO_CS
        if not built_ok(sql):
            raise ValueError("refused statement")
        rows = [dict(zip(cols, r)) for r in db.query_params(sql, [])]
        db.commit()
        for i in range(0, max(1, len(rows)), chunk_rows):
            yield rows[i:i + chunk_rows], [], i + chunk_rows >= len(rows), None
        return
    after: List[str] = ["" if x is None else str(x) for x in (key_from or [])]
    if after and len(after) != len(keys):
        raise ValueError(f"key_from has {len(after)} value(s), the key has {len(keys)}")
    while True:
        sql, pt = snapshot_sql(schema, table, cols, keys, bool(after), False, chunk_rows)
        got = db.query_params(sql, _bind(pt, after, []))
        db.commit()
        out: List[Dict[str, Any]] = []
        size = 0
        for r in got:
            out.append(dict(zip(cols, r)))
            size += sum(len(v or "") for v in r)
            if size >= max_bytes:
                break
        last = len(got) < chunk_rows and len(out) == len(got)
        last_key = ["" if out[-1][k] is None else str(out[-1][k]) for k in keys] if out else (list(after) or None)
        yield out, list(after), last, last_key
        if last or not out:
            return
        after = last_key or []


def _bind(pt: Sequence[int], lo: Sequence[str], hi: Sequence[str]) -> List[str]:
    return [hi[p - 100] if p >= 100 else lo[p] for p in pt]


# ------------------------------------------------------------------ execution
def table_meta(db: Any, schema: str, table: str) -> Tuple[List[Tuple[str, str, Optional[int]]], List[str]]:
    """([(column, typename, codepage)], key columns in key order); raises LookupError if not a base table."""
    n = db.query_params(Q_TABLE, [schema, table])
    if not n or str(n[0][0]) != "1":
        db.commit()
        raise LookupError(f"table {schema}.{table} not found (or not a base table)")
    cols, keys = [], {}
    for name, ks, tn, cp in db.query_params(Q_META, [schema, table]):
        cols.append((str(name), str(tn).strip(), int(cp) if cp not in (None, "") else None))
        if ks not in (None, "", "0"):
            keys[int(ks)] = str(name)
    if not keys:                             # SRC-db2-07: first unique index (by name), as the catalog does
        first = None
        for ind, col, seq in db.query_params(Q_UNIQUE_TAB, [schema, table]):
            first = first or ind
            if ind != first:
                break
            keys[int(seq)] = str(col)
    db.commit()
    kl = [keys[i] for i in sorted(keys)]
    if kl and sorted(keys) != list(range(1, len(kl) + 1)):
        raise RuntimeError("primary key sequence has a gap")
    return cols, kl


def verify_table(db: Any, raw_args: Dict[str, Any], position: Any = None,
                 max_bytes: int = MAX_RESULT_BYTES) -> Dict[str, Any]:
    """Run one VERIFY_TABLE on a alelt_db2edge.Db2 connection. `position()` -> current LRI text (or None)."""
    ve = _validator()
    a = ve.validate_args(raw_args)
    t0 = time.monotonic()
    schema, table = a["table"]["schema"], a["table"]["name"]
    checks = set(a["checks"])
    timeout = int(a["timeout_s"])
    meta, keys = table_meta(db, schema, table)
    cls = {c: agg_class(t, cp) for c, t, cp in meta}
    names = [c for c, _, _ in meta]
    cols = a.get("columns") or names
    for c in cols:
        if c not in cls:
            raise ve.VerifyArgsError(f"column {c} is not a column of {schema}.{table}")
    if (checks & {"keys", "rows"} or a.get("ranges")) and not keys:
        raise ve.VerifyArgsError(f"table {schema}.{table} has no primary key; keyed reads impossible")
    pos = position or (lambda: None)
    res: Dict[str, Any] = {"verify_id": a["verify_id"], "table": {"schema": schema, "name": table}, "engine": "db2",
                           "isolation": "CS", "key_columns": keys, "columns": {c: cls[c] for c in cols},
                           "position_before": pos(), "ranges": [], "rows": [], "missing": [], "truncated": False}
    size = 0
    try:
        if checks & {"row_count", "aggregates", "keys"}:
            ranges = a.get("ranges")
            for i, r in enumerate(ranges if ranges is not None else [{"after": None, "to": None}]):
                lo, hi = r.get("after") or [], r.get("to") or []
                if (lo and len(lo) != len(keys)) or (hi and len(hi) != len(keys)):
                    raise ve.VerifyArgsError(f"range {i}: key arity must match the primary key ({len(keys)})")
                out: Dict[str, Any] = {"index": i}
                if checks & {"row_count", "aggregates"}:
                    groups: List[List[str]] = [[]]
                    if "aggregates" in checks:
                        groups, n = [], AGG_ITEMS_MAX
                        for c in cols:
                            k = ITEMS[cls[c]]
                            if not groups or n + k > AGG_ITEMS_MAX:
                                groups.append([])
                                n = 1
                            groups[-1].append(c)
                            n += k
                        groups = groups or [[]]
                    agg: Dict[str, Any] = {}
                    rows_n = None
                    for g in groups:
                        sql, pt = agg_sql(schema, table, keys, [(c, cls[c]) for c in g], bool(lo), bool(hi))
                        row = db.query_params(sql, _bind(pt, lo, hi), timeout)[0]
                        db.commit()
                        rows_n = int(row[0]) if rows_n is None else rows_n
                        j = 1
                        for c in g:
                            k = cls[c]
                            if k == "cnt":
                                agg[c] = {"count": int(row[j] or 0)}
                                j += 1
                                continue
                            d: Dict[str, Any] = {"count": int(row[j] or 0)}
                            j += 1
                            if k in ("int", "dec", "flt", "tmp"):
                                d["min"], d["max"] = row[j], row[j + 1]
                                j += 2
                            if k in ("int", "dec", "str"):
                                d["len" if k == "str" else "sum"] = row[j]
                                j += 1
                            agg[c] = d
                    out["rows"] = rows_n
                    if "aggregates" in checks:
                        out["agg"] = agg
                if "keys" in checks:
                    sql, pt = keys_sql(schema, table, keys, bool(lo), bool(hi), int(a["max_keys"]) + 1)
                    ks = db.query_params(sql, _bind(pt, lo, hi), timeout)
                    db.commit()
                    out["keys"] = [list(k) for k in ks[:a["max_keys"]]]
                    out["keys_truncated"] = len(ks) > a["max_keys"]
                    size += sum(len(str(k)) for k in out["keys"])
                res["ranges"].append(out)
        if "rows" in checks:
            sql, pt = rows_sql(schema, table, keys, cols)
            for kt in a["key_values"]:
                if len(kt) != len(keys):
                    raise ve.VerifyArgsError(f"key_values: arity must match the primary key ({len(keys)})")
                if size >= max_bytes:
                    res["truncated"] = True
                    break
                try:
                    got = db.query_params(sql, _bind(pt, kt, []), timeout)
                except Exception as e:  # noqa: BLE001 - Db2Error
                    if not any(f"{x}" in str(e) for x in KEY_CANNOT_EXIST):
                        raise
                    got = []                     # the key value does not fit the key column: no such row
                if got:
                    res["rows"].append(dict(zip(cols, got[0])))
                    size += sum(len(v or "") for v in got[0])
                else:
                    res["missing"].append(list(kt))
            db.commit()
    finally:
        db.commit()
    res["position_after"] = pos()
    res["elapsed_ms"] = int((time.monotonic() - t0) * 1000)
    return res
