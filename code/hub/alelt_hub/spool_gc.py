"""Orphan spool object GC (F-21).

Receipt state machine (`gateway._receive_batch`, X-26/X-46/X-47):

    S0 received   -> body decompressed + sha256 checked (422 on mismatch, nothing stored)
    S1 fenced?    -> `fence_precheck` (423 if not the lease holder, nothing stored)
    S2 stored     -> blob `put` confirmed by the store (fsync+rename / S3 PUT 200 + Content-MD5)
    S3 committed  -> ONE metadata txn: lock connection row, re-check fence, re-check duplicate, chain check,
                     insert `batch` row + `decode` job
    S4 acked      -> HTTP 200 `BatchAck{durable_pos}`; only now may the agent delete its spool

    crash / error in S2..S3  -> orphan object (no `batch` row references it); the agent re-sends (same batch_id ->
                                same key, overwritten with identical bytes) -> S3 succeeds -> no orphan left
    rejected in S3 (fence / gap) -> object deleted unless a committed row references it (duplicate race)
    crash after S3, ACK lost -> agent re-sends -> duplicate (200 `duplicate:true`), nothing re-stored
    a lease change between S1 and S3 is caught by the in-txn fence re-check (423, object deleted)

Orphans are therefore benign but accumulate (e.g. an agent that never re-sends after a takeover). This GC removes
batch / snapshot-chunk objects that no metadata row references, with mark-and-sweep: an object must be seen
unreferenced in two passes at least `grace_s` apart (default 1 h), so an object between S2 and S3 is never removed.
Only keys with the batch / snapshot layouts are considered; other prefixes (e.g. the decoded-event cache) are ignored.
"""
from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import Engine, select

from .blobstore import BlobStore, get_store
from .db import batch, hub_setting, snapshot_chunk
from .dialect import insert

_BATCH = re.compile(r"^[A-Za-z0-9_-]+/[0-9a-fA-F]{2}/(?P<id>[0-9a-fA-F-]{36})\.[a-z0-9]+$")
_SNAP = re.compile(r"^snapshots/[0-9a-fA-F-]{36}/\d{8}\.[a-z0-9]+$")
STATE_KEY = "spool_gc.candidates"
GRACE_S = 3600.0


@dataclass
class GcReport:
    scanned: int = 0
    candidates: int = 0
    deleted: list[str] = field(default_factory=list)
    bytes_freed: int = 0


def _referenced(engine: Engine, st: BlobStore) -> tuple[set[uuid.UUID], set[str]]:
    with engine.connect() as c:
        ids = {r[0] if isinstance(r[0], uuid.UUID) else uuid.UUID(str(r[0])) for r in c.execute(select(batch.c.id))}
        paths = [r[0] for r in c.execute(select(snapshot_chunk.c.spool_path))]
    keys = {k for k in (st.key_of(p) if p else None for p in paths) if k}
    return ids, keys


def _is_orphan(key: str, ids: set[uuid.UUID], snap_keys: set[str]) -> bool | None:
    m = _BATCH.match(key)
    if m:
        try:
            return uuid.UUID(m.group("id")) not in ids
        except ValueError:
            return None
    if _SNAP.match(key):
        return key not in snap_keys
    return None                                       # not ours: never touched


def collect(engine: Engine, store: BlobStore | None = None, grace_s: float = GRACE_S, dry_run: bool = False,
            now: float | None = None) -> GcReport:
    st = store or get_store()
    now = time.time() if now is None else now
    rep = GcReport()
    with engine.connect() as c:
        prev: dict[str, Any] = c.execute(select(hub_setting.c.value).where(hub_setting.c.key == STATE_KEY)).scalar() or {}
    objects = list(st.list(""))
    ids, snap_keys = _referenced(engine, st)          # read AFTER listing: a row committed meanwhile is seen
    nxt: dict[str, float] = {}
    for key, size in objects:
        rep.scanned += 1
        if not _is_orphan(key, ids, snap_keys):
            continue
        first = float(prev.get(key, now))
        if now - first >= grace_s:
            if not dry_run:
                st.delete(key)
            rep.deleted.append(key)
            rep.bytes_freed += int(size or 0)
        else:
            nxt[key] = first
            rep.candidates += 1
    if not dry_run:
        with engine.begin() as c:
            c.execute(insert(hub_setting).values(key=STATE_KEY, value=nxt)
                      .on_conflict_do_update(index_elements=["key"], set_={"value": nxt}))
    return rep
