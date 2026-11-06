"""Parquet (pyarrow, zstd) writer for micro-batch and snapshot load files. pyarrow is imported lazily."""
from __future__ import annotations

from pathlib import Path
from typing import Any

from alelt_common.canonical_jsonl import json_text
from alelt_common.models import CanonicalType, ColumnInfo, TableInfo

from .profile import OutputProfile
from .sqlgen import OP_COLUMN, SEQ_COLUMN


def arrow_type(col: ColumnInfo) -> Any:
    import pyarrow as pa

    t = col.canonical_type
    simple = {
        CanonicalType.BOOLEAN: pa.bool_(), CanonicalType.SHORT: pa.int16(), CanonicalType.INT: pa.int32(),
        CanonicalType.LONG: pa.int64(), CanonicalType.FLOAT: pa.float32(), CanonicalType.DOUBLE: pa.float64(),
        CanonicalType.LOCALDATE: pa.date32(), CanonicalType.LOCALTIME: pa.time64("us"),
        CanonicalType.LOCALDATETIME: pa.timestamp("us"), CanonicalType.INSTANT: pa.timestamp("us", tz="UTC"),
        CanonicalType.STRING: pa.string(), CanonicalType.BINARY: pa.binary(), CanonicalType.JSON: pa.string(),
        CanonicalType.XML: pa.string(),
    }
    if t == CanonicalType.DECIMAL:
        return pa.decimal128(min(col.length or 38, 38), col.scale or 0)
    if t not in simple:
        raise ValueError(f"{col.name}: canonical type {t.value} cannot be loaded (quarantine, R7)")
    return simple[t]


def load_schema(table: TableInfo, profile: OutputProfile, with_ops: bool = True) -> Any:
    import pyarrow as pa

    fields = [pa.field(c.name, arrow_type(c), nullable=True) for c in sorted(table.columns, key=lambda c: c.ordinal)]
    if not table.key_columns():
        fields.append(pa.field(profile.id_column, pa.string()))
    if with_ops:
        fields += [pa.field(OP_COLUMN, pa.string()), pa.field(SEQ_COLUMN, pa.int64())]
    return pa.schema(fields)


def write_parquet(path: str | Path, table: TableInfo, profile: OutputProfile, rows: list[dict[str, Any]]) -> int:
    """Write rows (data columns + '_op'/'_seq') to a zstd Parquet file; returns row count."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = load_schema(table, profile)
    text = {c.name for c in table.columns if c.canonical_type in (CanonicalType.JSON, CanonicalType.XML)}
    cols: dict[str, list[Any]] = {f.name: [] for f in schema}
    for r in rows:
        for f in schema:
            key = "_op" if f.name == OP_COLUMN else "_seq" if f.name == SEQ_COLUMN else f.name
            v = r.get(key)
            cols[f.name].append(json_text(v) if f.name in text else v)   # JSON = text (VARIANT via PARSE_JSON)
    arrays = [pa.array(cols[f.name], type=f.type) for f in schema]
    pq.write_table(pa.Table.from_arrays(arrays, schema=schema), str(path), compression="zstd")
    return len(rows)
