"""Pure SQL generation for the Snowflake destination. No I/O: every function returns SQL text (and bind params).

Bind style is the connector default `pyformat` (`%s`); parameterized statements never contain a literal '%'.
"""
from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import datetime

from alelt_common.models import TableInfo

from .profile import OutputProfile
from .types import DEFAULT_MAP, snowflake_type

OP_COLUMN = "_ALELT_OP"      # transient-table only: I/U/D
SEQ_COLUMN = "_ALELT_SEQ"    # transient-table only: order within the batch

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_PLACEHOLDER = re.compile(r"\{\{([A-Z0-9_]+)\}\}")


# ---------------------------------------------------------------- identifiers / templating
def q(name: str) -> str:
    """Quote an identifier exactly as given (case preserved)."""
    if not name or "\x00" in name:
        raise ValueError(f"invalid identifier {name!r}")
    return '"' + name.replace('"', '""') + '"'


def fq(*parts: str) -> str:
    """Fully qualified name; plain identifiers stay unquoted (upper-case semantics), others are quoted."""
    return ".".join(p if _IDENT.match(p) and p.upper() == p else q(p) for p in parts)


def check_ident(name: str) -> str:
    if not _IDENT.match(name):
        raise ValueError(f"unsafe identifier {name!r}")
    return name


def lit(value: str) -> str:
    """SQL string literal (single quotes doubled)."""
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def render(template: str, params: Mapping[str, object]) -> str:
    """Replace {{KEY}} placeholders; every placeholder must be supplied."""
    def sub(m: re.Match[str]) -> str:
        key = m.group(1)
        if key not in params:
            raise KeyError(f"migration placeholder {{{{{key}}}}} not supplied")
        return str(params[key])

    return _PLACEHOLDER.sub(sub, template)


def split_statements(sql: str) -> list[str]:
    """Split a script on ';' outside quotes, comments and $$ blocks. Comment-only fragments are dropped."""
    out: list[str] = []
    buf: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        two = sql[i:i + 2]
        if two == "--":
            j = sql.find("\n", i)
            j = n if j < 0 else j
            buf.append(sql[i:j])
            i = j
        elif two == "/*":
            j = sql.find("*/", i + 2)
            j = n if j < 0 else j + 2
            buf.append(sql[i:j])
            i = j
        elif two == "$$":
            j = sql.find("$$", i + 2)
            if j < 0:
                raise ValueError("unterminated $$ block")
            buf.append(sql[i:j + 2])
            i = j + 2
        elif c in ("'", '"'):
            j = i + 1
            while j < n:
                if sql[j] == "\\" and c == "'":
                    j += 2
                    continue
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:
                        j += 2
                        continue
                    break
                j += 1
            buf.append(sql[i:j + 1])
            i = j + 1
        elif c == ";":
            out.append("".join(buf))
            buf = []
            i += 1
        else:
            buf.append(c)
            i += 1
    out.append("".join(buf))
    return [_strip_leading_comments(s) for s in out if _has_code(s)]


def _strip_leading_comments(stmt: str) -> str:
    return re.sub(r"\A(?:\s*--[^\n]*\n)+", "", stmt).strip()


def _has_code(stmt: str) -> bool:
    no_line = re.sub(r"--[^\n]*", "", stmt)
    return bool(re.sub(r"/\*.*?\*/", "", no_line, flags=re.S).strip())


# ---------------------------------------------------------------- DDL
def column_defs(table: TableInfo, profile: OutputProfile, type_map: Mapping[str, object] = DEFAULT_MAP) -> list[tuple[str, str, bool]]:
    """(name, snowflake type, nullable) for data columns in ordinal order, then system columns."""
    cols = [(c.name, snowflake_type(c, dict(type_map)), c.nullable)
            for c in sorted(table.columns, key=lambda c: c.ordinal)]
    keyless = not table.key_columns()
    cols += [(n, t, n != profile.id_column) for n, t in profile.system_columns(keyless)]
    return cols


def key_names(table: TableInfo, profile: OutputProfile) -> list[str]:
    keys = [c.name for c in table.key_columns()]
    return keys or [profile.id_column]


def create_table_sql(fqn: str, table: TableInfo, profile: OutputProfile,
                     type_map: Mapping[str, object] = DEFAULT_MAP) -> str:
    lines = [f"  {q(n)} {t}{'' if null else ' NOT NULL'}" for n, t, null in column_defs(table, profile, type_map)]
    lines.append(f"  PRIMARY KEY ({', '.join(q(k) for k in key_names(table, profile))})")
    return f"CREATE TABLE IF NOT EXISTS {fqn} (\n" + ",\n".join(lines) + "\n)"


_DDL_HEAD = re.compile(
    r"^\s*create\s+(?:or\s+replace\s+)?(?:transient\s+)?table\s+(?:if\s+not\s+exists\s+)?(\"(?:[^\"]|\"\")+\"|[^\s(]+)",
    re.I)


def golden_table_sql(golden_ddl: str, fqn: str) -> str:
    """Rewrite a GET_DDL output to a non-destructive CREATE TABLE IF NOT EXISTS on the landing table."""
    m = _DDL_HEAD.match(golden_ddl)
    if not m:
        raise ValueError("golden DDL does not start with CREATE TABLE")
    body = golden_ddl[m.end():].strip().rstrip(";").strip()
    return f"CREATE TABLE IF NOT EXISTS {fqn} {body}"


def add_column_sql(fqn: str, name: str, sf_type: str) -> str:
    return f"ALTER TABLE {fqn} ADD COLUMN IF NOT EXISTS {q(name)} {sf_type}"


# ---------------------------------------------------------------- staging
def stage_path(database: str, stage: str, *parts: str) -> str:
    return "@" + fq(database, "ALELT_STAGE", stage) + "/" + "/".join(p.strip("/") for p in parts)


def put_sql(local_path: str, stage_uri: str) -> str:
    p = local_path.replace("\\", "/")
    return f"PUT 'file://{p}' '{stage_uri}' AUTO_COMPRESS = FALSE OVERWRITE = TRUE PARALLEL = 4"


def create_transient_sql(tmp_fqn: str, landing_fqn: str) -> str:
    return f"CREATE OR REPLACE TRANSIENT TABLE {tmp_fqn} LIKE {landing_fqn}"


def create_stage_table_sql(tmp_fqn: str, landing_fqn: str) -> str:
    """Transient stage table = landing columns + op/seq columns in ONE statement (DST-snowflake-06; was CREATE
    ... LIKE + ALTER)."""
    return (f"CREATE OR REPLACE TRANSIENT TABLE {tmp_fqn} AS SELECT *, CAST(NULL AS VARCHAR(1)) AS {q(OP_COLUMN)}, "
            f"CAST(NULL AS NUMBER(18,0)) AS {q(SEQ_COLUMN)} FROM {landing_fqn} WHERE FALSE")


def add_op_columns_sql(tmp_fqn: str) -> str:
    return f"ALTER TABLE {tmp_fqn} ADD COLUMN {q(OP_COLUMN)} VARCHAR(1), {q(SEQ_COLUMN)} NUMBER(18,0)"


def copy_into_sql(tmp_fqn: str, stage_uri: str, case_sensitive: bool = False) -> str:
    """Parquet columns carry the source names; a golden-DDL landing table may have them upper-case (unquoted DDL):
    match case-insensitively (DST-snowflake-07), case-sensitively only when two columns differ by case alone."""
    mode = "CASE_SENSITIVE" if case_sensitive else "CASE_INSENSITIVE"
    return (f"COPY INTO {tmp_fqn} FROM '{stage_uri}' FILE_FORMAT = (TYPE = PARQUET) "
            f"MATCH_BY_COLUMN_NAME = {mode} ON_ERROR = ABORT_STATEMENT PURGE = FALSE")


def landing_schema_name(name: str) -> str:
    """A safe Snowflake schema identifier from a landing schema / connection id (DST-snowflake-10): characters
    outside [A-Za-z0-9_$] become '_', a leading digit gets a '_' prefix, upper-case (unquoted semantics)."""
    out = re.sub(r"[^A-Za-z0-9_$]", "_", name.strip()).upper()
    if not out:
        raise ValueError(f"invalid landing schema {name!r}")
    return out if re.match(r"[A-Z_]", out) else "_" + out


def drop_table_sql(fqn: str) -> str:
    return f"DROP TABLE IF EXISTS {fqn}"


def remove_sql(stage_uri: str) -> str:
    return f"REMOVE '{stage_uri}'"


# ---------------------------------------------------------------- merge (inside the apply transaction)
def merge_sql(landing_fqn: str, tmp_fqn: str, data_cols: Sequence[str], keys: Sequence[str],
              profile: OutputProfile) -> str:
    """Upsert from the transient table; 'D' rows soft-delete; synced = CURRENT_TIMESTAMP(). A 'D' row for a key the
    landing table does not hold inserts a tombstone (key + delivered before-image columns, deleted = TRUE): the one
    DELETE rule of every destination (XC-07, DESIGN.md 5.2)."""
    syn, dele = q(profile.synced_column), q(profile.deleted_column)
    src_cols = list(dict.fromkeys([*data_cols, *keys]))
    sel = ", ".join(q(c) for c in [*src_cols, OP_COLUMN])
    part = ", ".join(q(k) for k in keys)
    on = " AND ".join(f"t.{q(k)} = s.{q(k)}" for k in keys)
    sets = ", ".join(f"t.{q(c)} = s.{q(c)}" for c in src_cols if c not in keys)
    ins_cols = ", ".join([*(q(c) for c in src_cols), dele, syn])
    ins_vals = ", ".join([*(f"s.{q(c)}" for c in src_cols), "FALSE", "CURRENT_TIMESTAMP()"])
    tomb_vals = ", ".join([*(f"s.{q(c)}" for c in src_cols), "TRUE", "CURRENT_TIMESTAMP()"])
    upd = (sets + ", " if sets else "") + f"t.{dele} = FALSE, t.{syn} = CURRENT_TIMESTAMP()"
    return (
        f"MERGE INTO {landing_fqn} AS t\n"
        f"USING (SELECT {sel} FROM {tmp_fqn}\n"
        f"       QUALIFY ROW_NUMBER() OVER (PARTITION BY {part} ORDER BY {q(SEQ_COLUMN)} DESC) = 1) AS s\n"
        f"ON {on}\n"
        f"WHEN MATCHED AND s.{q(OP_COLUMN)} = 'D' THEN UPDATE SET t.{dele} = TRUE, t.{syn} = CURRENT_TIMESTAMP()\n"
        f"WHEN MATCHED THEN UPDATE SET {upd}\n"
        f"WHEN NOT MATCHED AND s.{q(OP_COLUMN)} = 'D' THEN INSERT ({ins_cols}) VALUES ({tomb_vals})\n"
        f"WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})"
    )


def truncate_sql(landing_fqn: str, profile: OutputProfile) -> str:
    """Source TRUNCATE => soft-delete every live row (rows are retained)."""
    dele, syn = q(profile.deleted_column), q(profile.synced_column)
    return f"UPDATE {landing_fqn} SET {dele} = TRUE, {syn} = CURRENT_TIMESTAMP() WHERE NOT {dele}"


# ---------------------------------------------------------------- control tables
def control(database: str, table: str) -> str:
    return fq(database, "ALELT_CONTROL", table)


def read_checkpoint_sql(database: str) -> str:
    return (f"SELECT ENGINE, POSITION_TEXT, BATCH_ID, WATERMARKS FROM {control(database, 'CHECKPOINT')} "
            f"WHERE STREAM = %s")


def checkpoint_cas_sql(database: str) -> str:
    """Compare-and-set of the stream checkpoint, FIRST statement of the apply transaction (F-05).
    Params: stream, engine, position_text, batch_id, watermarks, prev_position (None = no row yet), prev_batch.
    The DML takes the CHECKPOINT table lock for the rest of the transaction (a concurrent applier waits, then
    matches nothing); the result row (inserted, updated) must total 1, else the batch is rolled back."""
    return (
        f"MERGE INTO {control(database, 'CHECKPOINT')} AS t\n"
        f"USING (SELECT %s AS STREAM, %s AS ENGINE, %s AS POSITION_TEXT, %s AS BATCH_ID, %s AS WATERMARKS, "
        f"%s AS PREV_POSITION, %s AS PREV_BATCH) AS s\n"
        f"ON t.STREAM = s.STREAM\n"
        f"WHEN MATCHED AND t.POSITION_TEXT = s.PREV_POSITION AND EQUAL_NULL(t.BATCH_ID, s.PREV_BATCH) THEN UPDATE SET "
        f"ENGINE = s.ENGINE, POSITION_TEXT = s.POSITION_TEXT, BATCH_ID = s.BATCH_ID, WATERMARKS = s.WATERMARKS, "
        f"UPDATED_AT = CURRENT_TIMESTAMP()\n"
        f"WHEN NOT MATCHED AND s.PREV_POSITION IS NULL THEN INSERT (STREAM, ENGINE, POSITION_TEXT, BATCH_ID, "
        f"WATERMARKS, UPDATED_AT) VALUES (s.STREAM, s.ENGINE, s.POSITION_TEXT, s.BATCH_ID, s.WATERMARKS, "
        f"CURRENT_TIMESTAMP())"
    )


def columns_sql(database: str) -> str:
    """Existing columns of a landing table (name, type, char length, precision, scale). Params: schema, table."""
    return (f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE FROM "
            f"{q(database)}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s")


def alter_type_sql(fqn: str, name: str, sf_type: str) -> str:
    """Widen VARCHAR length / NUMBER precision in place (Snowflake cannot change a NUMBER scale)."""
    return f"ALTER TABLE {fqn} ALTER COLUMN {q(name)} SET DATA TYPE {sf_type}"


def checkpoint_merge_sql(database: str) -> str:
    """Params: stream, engine, position_text, batch_id."""
    return (
        f"MERGE INTO {control(database, 'CHECKPOINT')} AS t\n"
        f"USING (SELECT %s AS STREAM, %s AS ENGINE, %s AS POSITION_TEXT, %s AS BATCH_ID) AS s\n"
        f"ON t.STREAM = s.STREAM\n"
        f"WHEN MATCHED THEN UPDATE SET ENGINE = s.ENGINE, POSITION_TEXT = s.POSITION_TEXT, "
        f"BATCH_ID = s.BATCH_ID, UPDATED_AT = CURRENT_TIMESTAMP()\n"
        f"WHEN NOT MATCHED THEN INSERT (STREAM, ENGINE, POSITION_TEXT, BATCH_ID, UPDATED_AT) "
        f"VALUES (s.STREAM, s.ENGINE, s.POSITION_TEXT, s.BATCH_ID, CURRENT_TIMESTAMP())"
    )


AUDIT_COLUMNS = ("BATCH_ID", "STREAM", "TABLE_NAME", "FROM_POS", "TO_POS", "INSERTS", "UPDATES", "DELETES",
                 "TRUNCATED", "DURATION_MS", "STATUS", "MIN_COMMIT_TS", "MAX_COMMIT_TS")


def audit_insert_sql(database: str) -> str:
    cols = ", ".join(AUDIT_COLUMNS)
    vals = ", ".join(["%s"] * len(AUDIT_COLUMNS))
    return f"INSERT INTO {control(database, 'AUDIT_BATCH')} ({cols}) VALUES ({vals})"


def view_columns_sql(database: str, table: str, columns: Sequence[str]) -> list[tuple[str, tuple[object, ...]]]:
    """Statements replacing the registered explicit column list of a view."""
    vc = control(database, "VIEW_COLUMNS")
    stmts: list[tuple[str, tuple[object, ...]]] = [(f"DELETE FROM {vc} WHERE TABLE_NAME = %s", (table,))]
    if columns:
        rows = ", ".join(["(%s, %s, %s)"] * len(columns))
        params: list[object] = []
        for i, c in enumerate(columns, 1):
            params += [table, c, i]
        stmts.append((f"INSERT INTO {vc} (TABLE_NAME, COLUMN_NAME, ORDINAL) VALUES {rows}", tuple(params)))
    return stmts


def view_sql(view_fqn: str, source_fqn: str, columns: Sequence[str]) -> str:
    if not columns:
        raise ValueError("view needs an explicit column list")
    return f"CREATE OR REPLACE VIEW {view_fqn} COPY GRANTS AS SELECT {', '.join(q(c) for c in columns)} FROM {source_fqn}"


# ---------------------------------------------------------------- verification queries
def _live(profile: OutputProfile, include_deleted: bool, synced_before: datetime | None) -> tuple[str, tuple[object, ...]]:
    conds: list[str] = []
    params: tuple[object, ...] = ()
    if not include_deleted:
        conds.append(f"NOT COALESCE({q(profile.deleted_column)}, FALSE)")
    if synced_before is not None:
        conds.append(f"{q(profile.synced_column)} <= %s")
        params = (synced_before,)
    return (" WHERE " + " AND ".join(conds)) if conds else "", params


def row_count_sql(fqn: str, profile: OutputProfile, include_deleted: bool = False,
                  synced_before: datetime | None = None) -> tuple[str, tuple[object, ...]]:
    where, params = _live(profile, include_deleted, synced_before)
    return f"SELECT COUNT(*) FROM {fqn}{where}", params


def _bucket(keys: Sequence[str], buckets: int) -> str:
    if buckets < 1:
        raise ValueError("buckets must be >= 1")
    return f"MOD(ABS(HASH({', '.join(q(k) for k in keys)})), {int(buckets)})"


def key_bucket_sql(fqn: str, keys: Sequence[str], buckets: int, profile: OutputProfile,
                   synced_before: datetime | None = None) -> tuple[str, tuple[object, ...]]:
    where, params = _live(profile, False, synced_before)
    k = ", ".join(q(c) for c in keys)
    b = _bucket(keys, buckets)
    return (f"SELECT {b} AS BUCKET, COUNT(*) AS N, HASH_AGG({k}) AS H FROM {fqn}{where} "
            f"GROUP BY BUCKET ORDER BY BUCKET", params)


def column_checksum_sql(fqn: str, keys: Sequence[str], columns: Sequence[str], buckets: int,
                        profile: OutputProfile, synced_before: datetime | None = None) -> tuple[str, tuple[object, ...]]:
    where, params = _live(profile, False, synced_before)
    aggs = ", ".join(f"HASH_AGG({q(c)}) AS {q('H_' + c)}" for c in columns)
    return (f"SELECT {_bucket(keys, buckets)} AS BUCKET, {aggs} FROM {fqn}{where} GROUP BY BUCKET ORDER BY BUCKET",
            params)


def deleted_keys_sql(fqn: str, keys: Sequence[str], profile: OutputProfile) -> str:
    k = ", ".join(q(c) for c in keys)
    return f"SELECT {k} FROM {fqn} WHERE {q(profile.deleted_column)} ORDER BY {k}"


def sample_rows_sql(fqn: str, keys: Sequence[str], columns: Sequence[str], n_keys: int) -> str:
    """Fetch rows for `n_keys` key tuples (bind params: flattened key values)."""
    if n_keys < 1:
        raise ValueError("n_keys must be >= 1")
    cols = ", ".join(q(c) for c in dict.fromkeys([*keys, *columns]))
    tup = "(" + ", ".join(["%s"] * len(keys)) + ")"
    kk = "(" + ", ".join(q(k) for k in keys) + ")"
    return f"SELECT {cols} FROM {fqn} WHERE {kk} IN ({', '.join([tup] * n_keys)})"
