"""Remote snapshot: SourceConnector.snapshot_rows() -> blob store chunks + snapshot_chunk rows + snapshot_load jobs
(same storage and worker path as POST /agent/v1/snapshot).

Chunk rows are canonical JSON lines (`format: canonical_jsonl`, alelt_common.canonical_jsonl value tags): bytes, decimals,
dates, times and timestamps reach the destination with their type (REQ-23), not as base64 / text.

A row holding a value the connector could not convert (`Undecodable`) is never written with a guessed value: it is
left out of the chunk and stored as a `quarantine_event` (table, key, column, source text as raw bytes; R7)."""
from __future__ import annotations

import json
import uuid
from typing import Any

from sqlalchemy import Engine, select

from alelt_common.canonical_jsonl import Undecodable, canonical_snapshot_row, encode_value, snapshot_types
from alelt_common.models import SnapshotChunkHeader, TableInfo, TableRef
from alelt_common.spi import SourceConnector
from alelt_common.util import compress
from alelt_hub import blobstore, jobs
from alelt_hub.db import quarantine_event, snapshot_chunk, source_table
from alelt_hub.dialect import insert


def _jsonable(v: Any) -> Any:
    """Snapshot chunk rows use plain JSON (worker json.loads + destination coercion by column type)."""
    t = encode_value(v)
    return next(iter(t.values())) if isinstance(t, dict) else t


def _quarantine(engine: Engine, connection_id: str, ref: TableRef, sid: uuid.UUID, start: Any,
                keys: list[str], bad: list[dict[str, Any]]) -> None:
    with engine.begin() as c:
        tid = c.execute(select(source_table.c.id).where(
            source_table.c.connection_id == connection_id, source_table.c.src_schema == ref.schema_,
            source_table.c.src_table == ref.name)).scalar()
        for r in bad:
            cols = sorted(k for k, v in r.items() if isinstance(v, Undecodable))
            key = {k: _jsonable(r.get(k)) for k in keys}
            raw = json.dumps({k: str(r[k]) for k in cols}, ensure_ascii=False).encode()
            c.execute(insert(quarantine_event).values(
                id=uuid.uuid4(), connection_id=connection_id, table_id=tid, table_fqn=ref.fqn,
                lri=start.text if start is not None else None,
                reason=f"snapshot {sid}: unconvertible value in {', '.join(cols)} key={json.dumps(key)}"[:500],
                raw_ref={"snapshot_id": str(sid), "key": key}, raw_hex=raw[:256].hex()))


def run_snapshot(engine: Engine, connection_id: str, src: SourceConnector, tables: list[TableRef],
                 chunk_rows: int = 10_000, codec: str = "zlib") -> dict[str, dict[str, Any]]:
    """Snapshot `tables` (catalog from src.read_catalog()). Returns {fqn: {snapshot_id, chunks, rows}}."""
    cat = {t.table: t for t in src.read_catalog().tables}
    start = src.current_position()
    from .receipt import record_start_pos

    record_start_pos(engine, connection_id, start, "snapshot")          # REQ-21: first-start position for the runner
    out: dict[str, dict[str, Any]] = {}
    for ref in tables:
        info: TableInfo | None = cat.get(ref)
        if info is None:
            raise KeyError(f"{ref.fqn} not in source catalog")
        sid = uuid.uuid4()
        keys = [c.name for c in info.key_columns()]
        types = snapshot_types(info)
        n = rows_total = quarantined = 0
        prev_key: list[Any] | None = None
        it = iter(src.snapshot_rows(info, None, chunk_rows))
        cur: list[dict[str, Any]] | None = next(it, [])
        i = -1
        while cur is not None:
            rows, cur = cur, next(it, None)          # one-chunk lookahead: `last` flag without buffering a table
            i += 1
            rows = [canonical_snapshot_row(r, types, decode_tags=False) for r in rows]   # JSON columns = text
            bad = [r for r in rows if any(isinstance(v, Undecodable) for v in r.values())]
            if bad:
                _quarantine(engine, connection_id, ref, sid, start, keys, bad)
                quarantined += len(bad)
                key_to = [_jsonable(rows[-1].get(k)) for k in keys] if rows and keys else None
                drop = {id(r) for r in bad}
                rows = [r for r in rows if id(r) not in drop]
            else:
                key_to = None
            last_key = key_to or ([_jsonable(rows[-1].get(k)) for k in keys] if rows and keys else None)
            body = b"".join(json.dumps({k: encode_value(v) for k, v in r.items()}, ensure_ascii=False,
                                       allow_nan=False).encode() + b"\n" for r in rows)
            hdr = SnapshotChunkHeader(snapshot_id=sid, table=ref, chunk_no=i, key_from=prev_key, key_to=last_key,
                                      rows=len(rows), last=cur is None, start_pos=start,
                                      codec=codec, format="canonical_jsonl")  # type: ignore[arg-type]
            data = compress(body, codec)
            key = blobstore.snapshot_key(str(sid), i, codec)
            ref_s = blobstore.get_store().put(key, data)
            with engine.begin() as c:
                r = c.execute(insert(snapshot_chunk).values(
                    snapshot_id=sid, chunk_no=i, connection_id=connection_id, table_fqn=ref.fqn, rows=len(rows),
                    last=hdr.last, header=json.loads(hdr.model_dump_json(by_alias=True)), spool_path=ref_s)
                    .on_conflict_do_nothing())
                if r.rowcount:
                    jobs.enqueue(c, "snapshot_load", {"snapshot_id": str(sid), "chunk_no": i},
                                 dedup_key=f"snap:{sid}:{i}")
            prev_key = last_key
            n += 1
            rows_total += len(rows)
        out[ref.fqn] = {"snapshot_id": str(sid), "chunks": n, "rows": rows_total, "quarantined": quarantined,
                        "start_pos": start.text if start else None}
    return out
