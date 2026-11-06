"""Test-strategy 4.4 #8: crash point in `flush` AFTER the destination COMMIT and BEFORE the hub `checkpoint_mirror` /
batch-status update. The destination checkpoint is the truth (ADR-005): a fresh worker replays the same batches and
must apply nothing twice (landing, audit and checkpoint unchanged), then continue normally.
Named `zz` so it runs after the shared-connection modules (it only asserts relative counts)."""
from __future__ import annotations

import os
from typing import Any

import psycopg
import pytest

from alelt_hub.testing import FakeAgent, ev, fake_catalog

from .conftest import TOKEN, drain, need_landing


def landing(sql: str) -> list[tuple[Any, ...]]:
    need_landing()
    with psycopg.connect(os.environ["ALELT_DEST_URL"]) as c:
        return c.execute(sql).fetchall()  # type: ignore[arg-type]


def count(sql: str) -> int:
    try:
        return int(landing(sql)[0][0])
    except psycopg.errors.UndefinedTable:
        return 0


def test_crash_between_destination_commit_and_mirror(client: Any, worker: Any, monkeypatch: Any) -> None:
    from alelt_hub import pipeline
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    a = FakeAgent(client, TOKEN)
    a.register()
    a.catalog(fake_catalog(a.connection_id))
    drain(worker)                                         # settle whatever earlier modules left
    from sqlalchemy import select

    from alelt_hub.db import batch
    with get_engine().connect() as c:                     # continue the fakeconn chain
        a.next_from = c.execute(select(batch.c.next_lri).where(batch.c.connection_id == a.connection_id)
                                .order_by(batch.c.seq.desc()).limit(1)).scalar() or a.next_from
    base = 0x7000000
    assert a.send([ev(base, "Z1", "INSERT", "S.KEYED", after={"ID": 501, "NAME": "crash"}),
                   ev(base + 1, "Z1", "COMMIT")]).status_code == 200
    before = count("SELECT count(*) FROM land.keyed")
    audits = count("SELECT count(*) FROM alelt_control.audit_batch")

    real_insert = pipeline.insert
    state = {"armed": True}

    def crashing_insert(table: Any) -> Any:
        if state["armed"] and getattr(table, "name", "") == "checkpoint_mirror":
            state["armed"] = False
            raise RuntimeError("crash after destination COMMIT")
        return real_insert(table)

    monkeypatch.setattr(pipeline, "insert", crashing_insert)
    crashed = Worker(get_engine(), get_settings(), "crash-w")
    p = crashed.pipeline(a.connection_id)
    recorded: list[tuple[Any, Any]] = []
    real_apply = p.dest.apply

    def recording_apply(*args: Any, **kw: Any) -> Any:
        recorded.append((args, kw))
        return real_apply(*args, **kw)

    p.dest.apply = recording_apply                         # type: ignore[method-assign]
    drain(crashed)                                        # job fails (retried later), destination already committed
    monkeypatch.setattr(pipeline, "insert", real_insert)
    assert landing("SELECT count(*) FROM land.keyed WHERE id = 501") == [(1,)]
    applied = landing("SELECT count(*) FROM alelt_control.audit_batch")[0][0]
    assert applied == audits + 1

    fresh = Worker(get_engine(), get_settings(), "after-crash")
    fresh.pipeline(a.connection_id).rebuild()
    fresh.pipelines[a.connection_id].process_pending()
    drain(fresh)
    assert landing("SELECT count(*) FROM land.keyed")[0][0] == before + 1
    assert landing("SELECT count(*) FROM alelt_control.audit_batch")[0][0] == applied      # nothing applied twice
    assert landing("SELECT count(*) FROM land.keyed WHERE id = 501") == [(1,)]

    # destination-level replay x3 of the REAL flushed call (the e2e "replay x3" only exercises the loader skip):
    # the destination must see checkpoint >= to_pos and apply nothing.
    assert recorded, "the crashed worker applied at least one flush"
    synced = landing("SELECT max(_alelt_synced) FROM land.keyed")
    for _ in range(3):
        args, kw = recorded[-1]
        assert real_apply(*args, **kw) == []
    assert landing("SELECT max(_alelt_synced) FROM land.keyed") == synced
    assert count("SELECT count(*) FROM alelt_control.audit_batch") == applied
