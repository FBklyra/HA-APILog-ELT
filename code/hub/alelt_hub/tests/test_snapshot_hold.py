"""X-22: while a table is snapshotting the loader holds its changes; catch-up replays them from start_pos."""
from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any

import psycopg

from alelt_common.models import Position, TableRef
from alelt_hub.testing import FakeAgent, ev, fake_catalog, lri

from .conftest import TOKEN, drain, need_landing


def landing(sql: str) -> list[tuple[Any, ...]]:
    need_landing()
    with psycopg.connect(os.environ["ALELT_DEST_URL"]) as c:
        return c.execute(sql).fetchall()  # type: ignore[arg-type]


def test_hold_and_catch_up(client: Any) -> None:
    need_landing()
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    a = FakeAgent(client, TOKEN, agent_id="snap-agent", connection_id="snapconn")
    a.register()
    a.catalog(fake_catalog("snapconn"))
    held = {"S.KEYED"}
    w = Worker(get_engine(), get_settings(), "snap-worker", hold=lambda r: r.fqn in held)

    assert a.send([ev(1, "A", "INSERT", "S.KEYED", after={"ID": 1, "NAME": "one"}),
                   ev(2, "A", "INSERT", "S.LOG", after={"MSG": "m", "N": 1}), ev(3, "A", "COMMIT")]).status_code == 200
    drain(w)
    assert landing("SELECT msg FROM snapconn.log") == [("m",)]
    assert landing("SELECT to_regclass('snapconn.keyed') IS NULL") == [(True,)]      # held: not applied

    a.send([ev(4, "B", "UPDATE", "S.KEYED", before={"ID": 1}, after={"ID": 1, "NAME": "two"}), ev(5, "B", "COMMIT")])
    drain(w)
    p = w.pipelines["snapconn"]
    info = p.policy.project(p.tables[TableRef(schema="S", name="KEYED")])
    p.dest.ensure_table(info, "KEYED", None)
    p.dest.load_snapshot("KEYED", info, [{"ID": 1, "NAME": "snap"}])                  # snapshot taken at lri(2)

    held.clear()                                                                       # -> CATCHING_UP
    job = SimpleNamespace(table="S.KEYED", start_pos=Position(engine="fake", text=lri(2)))
    assert w.pipelines["snapconn"].catch_up(job) == 2
    assert landing("SELECT name FROM snapconn.keyed") == [("two",)]
    assert w.pipelines["snapconn"].catch_up(job) == 0                                  # replay: nothing re-applied
    assert landing("SELECT count(*) FROM alelt_control.checkpoint WHERE stream = 'snapconn#catchup:S.KEYED'") == [(1,)]

    a.send([ev(6, "C", "UPDATE", "S.KEYED", before={"ID": 1}, after={"ID": 1, "NAME": "three"}), ev(7, "C", "COMMIT")])
    drain(w)
    assert landing("SELECT name FROM snapconn.keyed") == [("three",)]
