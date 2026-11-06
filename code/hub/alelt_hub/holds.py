"""Per-table apply holds in the metadata database (`table_hold`), shared by every worker and process.

kind `snapshot`: alelt_snapshot SnapshotJob (X-22): held while SNAPSHOTTING / RESYNC; released (CATCHING_UP) when the last
                 chunk is loaded, then the pipeline replays the table from `start_pos` (catch-up).
kind `schedule`: tables.yaml `schedule` (X-110, alelt_hub.schedule): held outside the daily window / between deferred
                 rounds; released by a catch-up from `start_pos` (= checkpoint when the hold began).
kind `integrity`: per-table integrity state (F-01, alelt_hub.integrity): held from the first unexplained quarantined
                 record until re-decode / keyed re-read / resync restored the table.

The main stream skips a held table (Loader `hold`, evaluated at flush time); nothing is lost because every release
replays from start_pos. start_pos of every held row of a table = the table watermark (ADR-005): everything up to it is
applied for that table; replays advance it on all held rows of the table (alelt_hub.replay.advance_watermark).
"""
from __future__ import annotations

import json
from typing import Any

from sqlalchemy import Engine, select, update

from alelt_common.util import utcnow
from alelt_snapshot.state import SnapshotJob, TableState

from .db import table_hold
from .dialect import insert

HELD_STATES = (TableState.SNAPSHOTTING.value, TableState.RESYNC.value)


def snapshot_held(state: str, body: dict[str, Any]) -> bool:
    """A snapshot hold is effective while SNAPSHOTTING / RESYNC and while CATCHING_UP until its catch-up replay ran
    (HC-19): the main stream never applies the table between the snapshot and the catch-up (partial images would not
    converge). The catch-up (pipeline lock) releases it in the same step."""
    if state in HELD_STATES:
        return True
    return state == TableState.CATCHING_UP.value and body.get("catchup_done") != body.get("snapshot_id")


def put(engine: Engine, cid: str, fqn: str, kind: str, state: str, held: bool, start_pos: str | None,
        body: dict[str, Any]) -> None:
    vals = dict(state=state, held=held, start_pos=start_pos, body=body, updated_at=utcnow())
    with engine.begin() as c:
        c.execute(insert(table_hold).values(connection_id=cid, table_fqn=fqn, kind=kind, **vals)
                  .on_conflict_do_update(index_elements=["connection_id", "table_fqn", "kind"], set_=vals))


def rows(engine: Engine, cid: str | None = None, kind: str | None = None) -> list[dict[str, Any]]:
    q = select(table_hold)
    if cid is not None:
        q = q.where(table_hold.c.connection_id == cid)
    if kind is not None:
        q = q.where(table_hold.c.kind == kind)
    with engine.connect() as c:
        return [dict(r) for r in c.execute(q.order_by(table_hold.c.table_fqn)).mappings()]


def rows_of_kinds(engine: Engine, cid: str, kinds: tuple[str, ...], held_only: bool = False) -> list[dict[str, Any]]:
    q = select(table_hold).where(table_hold.c.connection_id == cid, table_hold.c.kind.in_(kinds))
    if held_only:
        q = q.where(table_hold.c.held.is_(True))
    with engine.connect() as c:
        return [dict(r) for r in c.execute(q).mappings()]


def get(engine: Engine, cid: str, fqn: str, kind: str) -> dict[str, Any] | None:
    with engine.connect() as c:
        r = c.execute(select(table_hold).where(table_hold.c.connection_id == cid, table_hold.c.table_fqn == fqn,
                                               table_hold.c.kind == kind)).mappings().first()
    return dict(r) if r else None


def held_tables(engine: Engine, cid: str) -> set[str]:
    """fqn of every table of `cid` currently held (any kind)."""
    with engine.connect() as c:
        return {str(f) for f, in c.execute(select(table_hold.c.table_fqn).where(
            table_hold.c.connection_id == cid, table_hold.c.held.is_(True)))}


_KEEP: Any = object()


def modify(engine: Engine, cid: str, fqn: str, kind: str, *, state: Any = _KEEP, held: Any = _KEEP,
           start_pos: Any = _KEEP, drop: tuple[str, ...] = (), **changes: Any) -> dict[str, Any] | None:
    """Atomic read-modify-write of one hold row (row lock on Postgres, HC-11): merges `changes` into the body, removes
    `drop` keys, sets the given columns. Returns the new row, None when the row does not exist."""
    key = (table_hold.c.connection_id == cid, table_hold.c.table_fqn == fqn, table_hold.c.kind == kind)
    with engine.begin() as c:
        cur = c.execute(select(table_hold).where(*key).with_for_update()).mappings().first()
        if cur is None:
            return None
        body = {k: v for k, v in {**(cur["body"] or {}), **changes}.items() if k not in drop}
        vals: dict[str, Any] = {"body": body, "updated_at": utcnow()}
        for col, v in (("state", state), ("held", held), ("start_pos", start_pos)):
            if v is not _KEEP:
                vals[col] = v
        c.execute(update(table_hold).where(*key).values(**vals))
        return {**dict(cur), **vals}


def set_body(engine: Engine, cid: str, fqn: str, kind: str, **changes: Any) -> None:
    modify(engine, cid, fqn, kind, **changes)


class DbSnapshotStore:
    """alelt_snapshot JobStore over `table_hold` (kind snapshot) for one connection; survives restarts and is visible
    to every worker (FileJobStore is per host)."""

    def __init__(self, engine: Engine, connection_id: str) -> None:
        self.engine, self.cid = engine, connection_id

    def save(self, job: SnapshotJob) -> None:
        body = json.loads(job.model_dump_json())
        old = next((r["body"] for r in rows(self.engine, self.cid, "snapshot") if r["table_fqn"] == job.table), None)
        if old and old.get("snapshot_id") == body["snapshot_id"]:
            for k, v in old.items():                   # service flags (pass, restart_seq, agent command, catch-up
                if k not in SnapshotJob.model_fields:  # marker, verify note, reissues ...) kept across saves
                    body.setdefault(k, v)
        put(self.engine, self.cid, job.table, "snapshot", job.state.value, snapshot_held(job.state.value, body),
            job.start_pos.text if job.start_pos else None, body)

    def load_all(self) -> list[SnapshotJob]:
        out = []
        for r in rows(self.engine, self.cid, "snapshot"):
            b = {k: v for k, v in r["body"].items() if k in SnapshotJob.model_fields}
            out.append(SnapshotJob.model_validate(b))
        return out

    def flags(self, fqn: str) -> dict[str, Any]:
        r = next((r for r in rows(self.engine, self.cid, "snapshot") if r["table_fqn"] == fqn), None)
        return dict(r["body"]) if r else {}
