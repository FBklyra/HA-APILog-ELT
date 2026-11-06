"""Hub-side durable receipt for remote-captured batches: exactly the agent path (gateway._receive_batch).

blob store write (durable) -> `batch` row + `decode` job in one metadata transaction -> BatchAck.
"""
from __future__ import annotations

import socket
from typing import Any

from fastapi import HTTPException
from sqlalchemy import Engine, select

from alelt_common.models import BatchAck, BatchHeader, CatalogSnapshot, Position
from alelt_hub import store
from alelt_hub.db import agent, batch, connection
from alelt_hub.dialect import insert
from alelt_hub.gateway import _receive_batch

from . import __version__


class Backpressure(Exception):
    """Hub backlog full (503): keep the batch and retry later."""


class GapError(Exception):
    """from_pos does not continue the durable chain (409). Never re-chained silently."""


class ReceiptError(Exception):
    pass


def agent_id_for(connection_id: str) -> str:
    return f"capture:{connection_id}"


def ensure_registered(engine: Engine, connection_id: str, src_engine: str, database: str | None = None) -> str:
    """Upsert the connection row and the runner's pseudo-agent (capability 'remote'). Returns the agent id."""
    aid = agent_id_for(connection_id)
    with engine.begin() as c:
        c.execute(insert(connection).values(id=connection_id, engine=src_engine, db_database=database)
                  .on_conflict_do_nothing(index_elements=["id"]))
        have = c.execute(select(connection.c.engine).where(connection.c.id == connection_id)).scalar()
        if have != src_engine:
            raise ReceiptError(f"connection {connection_id} is engine {have!r}, not {src_engine!r}")
        vals: dict[str, Any] = dict(connection_id=connection_id, version=f"alelt_capture/{__version__}",
                                    hostname=socket.gethostname(), capabilities=["remote"], status="CAPTURING")
        c.execute(insert(agent).values(id=aid, **vals).on_conflict_do_update(index_elements=["id"], set_=vals))
    return aid


def durable_resume(engine: Engine, connection_id: str, src_engine: str) -> Position | None:
    """Last durable batch next_pos (else to_pos) of the connection."""
    with engine.connect() as c:
        last = c.execute(select(batch.c.next_lri, batch.c.to_lri).where(batch.c.connection_id == connection_id)
                         .order_by(batch.c.seq.desc()).limit(1)).first()
    return Position(engine=src_engine, text=last[0] or last[1]) if last else None


def open_chunks(engine: Engine, connection_id: str, resume: Position | None) -> tuple[int, str | None, int]:
    """HC-08: trailing chunk batches of a large open transaction (from == to == next == resume): (chunks, txid,
    lines already durable). The source re-delivers that transaction from `resume`; the runner skips those lines."""
    if resume is None:
        return 0, None, 0
    from alelt_common.util import decompress
    from alelt_hub import blobstore

    n = lines = 0
    last: dict[str, Any] | None = None
    with engine.connect() as c:
        for r in c.execute(select(batch.c.from_lri, batch.c.to_lri, batch.c.next_lri, batch.c.rec_count,
                                  batch.c.spool_path, batch.c.codec).where(batch.c.connection_id == connection_id)
                           .order_by(batch.c.seq.desc()).limit(100_000)).mappings():
            if not (r["from_lri"] == r["to_lri"] == (r["next_lri"] or r["to_lri"]) == resume.text):
                break
            n += 1
            lines += int(r["rec_count"] or 0)
            last = last or dict(r)
    if not n or last is None:
        return 0, None, 0
    raw = decompress(blobstore.read_ref(last["spool_path"]), last["codec"])
    first = raw.split(b"\n", 1)[0]
    import json

    txid = str(json.loads(first)["txid"]) if first.strip() else None
    return n, txid, lines


START_KEY = "capture.start:"


def first_start_pos(engine: Engine, connection_id: str) -> str | None:
    """REQ-21: first-start position recorded by the initial snapshot (`hub_setting` capture.start:<cid>), used only
    while the connection has no durable batch (CDC sources without a per-consumer retained position)."""
    from alelt_hub.db import hub_setting

    with engine.connect() as c:
        v = c.execute(select(hub_setting.c.value).where(hub_setting.c.key == START_KEY + connection_id)).scalar()
    return str(v.get("position")) if isinstance(v, dict) and v.get("position") else None


def record_start_pos(engine: Engine, connection_id: str, pos: Position | None, origin: str) -> bool:
    """Keep the EARLIEST snapshot start as first-start position while no batch exists (never moves forward)."""
    if pos is None:
        return False
    from alelt_hub.db import hub_setting

    with engine.begin() as c:
        if c.execute(select(batch.c.id).where(batch.c.connection_id == connection_id).limit(1)).first() is not None:
            return False
        have = c.execute(select(hub_setting.c.value).where(hub_setting.c.key == START_KEY + connection_id)).scalar()
        if have:
            return False
        c.execute(insert(hub_setting).values(key=START_KEY + connection_id,
                                             value={"position": pos.text, "origin": origin})
                  .on_conflict_do_nothing(index_elements=["key"]))
    return True


def ingest_catalog(engine: Engine, snap: CatalogSnapshot) -> tuple[int, bool]:
    with engine.begin() as c:
        return store.ingest_catalog(c, snap)


def receive(hdr: BatchHeader, body: bytes) -> BatchAck:
    """Same code path as POST /agent/v1/blocks (sha256, gap check, blob store, batch row, decode job)."""
    try:
        return _receive_batch(hdr, body, None, None)
    except HTTPException as e:
        if e.status_code == 503:
            raise Backpressure(str(e.detail)) from None
        if e.status_code == 409:
            raise GapError(str(e.detail)) from None
        raise ReceiptError(f"{e.status_code}: {e.detail}") from None
