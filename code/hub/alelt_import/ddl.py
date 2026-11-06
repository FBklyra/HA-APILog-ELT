"""`alelt import ddl`: golden DDL + columns.csv from an existing destination schema (read-only)."""
from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from pathlib import Path

from .readonly import Query, ReadOnly

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
COLUMNS_HEADER = ("table", "column", "ordinal", "data_type", "is_nullable")


@dataclass(frozen=True)
class GoldenColumn:
    table: str
    column: str
    ordinal: int
    data_type: str
    is_nullable: bool


def _ident(x: str) -> str:
    if not _IDENT.match(x):
        raise ValueError(f"unsafe identifier {x!r}")
    return x


def full_type(data_type: str, char_len: int | None, precision: int | None, scale: int | None) -> str:
    """INFORMATION_SCHEMA parts -> DDL-style type (TEXT -> VARCHAR(n), NUMBER -> NUMBER(p,s))."""
    t = data_type.upper()
    if t == "TEXT":
        return f"VARCHAR({char_len})" if char_len else "VARCHAR"
    if t == "NUMBER" and precision is not None:
        return f"NUMBER({precision},{scale or 0})"
    return t


def normalize_ddl(ddl: str) -> str:
    text = ddl.replace("\r\n", "\n").strip()
    return (text if text.endswith(";") else text + ";") + "\n"


def export(q: Query, database: str, schema: str) -> tuple[dict[str, str], list[GoldenColumn]]:
    """Returns ({table: ddl}, columns) sorted by table name / ordinal."""
    q = q if isinstance(q, ReadOnly) else ReadOnly(q)
    db, sc = _ident(database), _ident(schema)
    tables = [str(r[0]) for r in q.execute(
        f"SELECT TABLE_NAME FROM {db}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s "
        f"AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME", (sc,))]
    ddls = {t: normalize_ddl(str(q.execute("SELECT GET_DDL('TABLE', %s)", (f'{db}.{sc}."{t}"',))[0][0]))
            for t in sorted(tables)}
    rows = q.execute(
        f"SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, "
        f"NUMERIC_SCALE, IS_NULLABLE FROM {db}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s "
        f"ORDER BY TABLE_NAME, ORDINAL_POSITION", (sc,))
    cols = [GoldenColumn(str(t), str(c), int(o), full_type(str(dt), cl, p, s), str(n).upper() == "YES")
            for t, c, o, dt, cl, p, s, n in rows if str(t) in ddls]
    cols.sort(key=lambda c: (c.table, c.ordinal))
    return ddls, cols


def columns_csv(cols: list[GoldenColumn]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(COLUMNS_HEADER)
    for c in cols:
        w.writerow([c.table, c.column, c.ordinal, c.data_type, "YES" if c.is_nullable else "NO"])
    return buf.getvalue()


def read_columns_csv(path: str | Path) -> list[GoldenColumn]:
    with open(path, encoding="utf-8", newline="") as f:
        return [GoldenColumn(r["table"], r["column"], int(r["ordinal"]), r["data_type"], r["is_nullable"] == "YES")
                for r in csv.DictReader(f)]


def write(golden_dir: str | Path, ddls: dict[str, str], cols: list[GoldenColumn]) -> list[Path]:
    """Write ddl/<TABLE>.sql and columns.csv; LF line endings for stable diffs."""
    root = Path(golden_dir)
    (root / "ddl").mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for t, ddl in sorted(ddls.items()):
        p = root / "ddl" / f"{t}.sql"
        p.write_bytes(ddl.encode("utf-8"))
        written.append(p)
    p = root / "columns.csv"
    p.write_bytes(columns_csv(cols).encode("utf-8"))
    return [*written, p]
