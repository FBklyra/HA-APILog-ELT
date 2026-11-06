"""Test-strategy 4.4 #8 / F-21: crash-point injection in the hub receipt (after blob put, after DB commit before ACK),
duplicate race on the reject path, and the orphan spool GC (mark-and-sweep, never touches referenced or foreign keys)."""
from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import delete, select

from alelt_hub.testing import FakeAgent, ev

from .conftest import TOKEN

CONN = "crashconn"


class Boom(RuntimeError):
    pass


@pytest.fixture(scope="module")
def agent(client: Any) -> Iterator[FakeAgent]:
    a = FakeAgent(client, TOKEN, agent_id="crash-a", connection_id=CONN)
    a.register()
    yield a
    from alelt_hub.db import batch, get_engine, job

    with get_engine().begin() as c:
        ids = [str(r[0]) for r in c.execute(select(batch.c.id).where(batch.c.connection_id == CONN))]
        if ids:
            c.execute(delete(job).where(job.c.dedup_key.in_([f"decode:{i}" for i in ids])))
            c.execute(delete(batch).where(batch.c.connection_id == CONN))


def _make(ag: FakeAgent, n: int) -> tuple[Any, bytes]:
    return ag.make_batch([ev(n, f"T{n}", "INSERT", "S.KEYED", after={"ID": n}), ev(n + 1, f"T{n}", "COMMIT")])


def _post(client: Any, ag: FakeAgent, hdr: Any, body: bytes) -> Any:
    return client.post("/agent/v1/blocks", headers={**ag.h, "X-ALELT-Batch": hdr.model_dump_json()}, content=body)


def _row(bid: Any) -> Any:
    from alelt_hub.db import batch, get_engine

    with get_engine().connect() as c:
        return c.execute(select(batch.c.id, batch.c.spool_path).where(batch.c.id == bid)).first()


def _crash(point: str) -> Iterator[None]:
    from alelt_hub import gateway

    def boom() -> None:
        raise Boom(point)

    gateway.FAULTS[point] = boom
    try:
        yield
    finally:
        gateway.FAULTS.pop(point, None)


def test_crash_after_put_then_resend(client: Any, agent: FakeAgent) -> None:
    """Blob stored, metadata txn never commits: no batch row, agent keeps its spool, re-send succeeds once."""
    from fastapi.testclient import TestClient

    hdr, body = _make(agent, 1000)
    gen = _crash("after_put")
    next(gen)
    with pytest.raises(Boom):
        TestClient(client.app, raise_server_exceptions=True).post(
            "/agent/v1/blocks", headers={**agent.h, "X-ALELT-Batch": hdr.model_dump_json()}, content=body)
    gen.close()
    assert _row(hdr.batch_id) is None                       # never ACKed, never recorded
    r = _post(client, agent, hdr, body)
    assert r.status_code == 200 and r.json()["duplicate"] is False
    assert _row(hdr.batch_id) is not None
    agent.next_from = hdr.next_pos.text


def test_crash_after_commit_ack_lost_resend_is_duplicate(client: Any, agent: FakeAgent) -> None:
    from fastapi.testclient import TestClient

    hdr, body = _make(agent, 1010)
    gen = _crash("after_commit")
    next(gen)
    with pytest.raises(Boom):
        TestClient(client.app, raise_server_exceptions=True).post(
            "/agent/v1/blocks", headers={**agent.h, "X-ALELT-Batch": hdr.model_dump_json()}, content=body)
    gen.close()
    assert _row(hdr.batch_id) is not None                   # durable although the ACK was lost
    r = _post(client, agent, hdr, body)
    assert r.status_code == 200 and r.json()["duplicate"] is True
    agent.next_from = hdr.next_pos.text


def test_duplicate_race_never_deletes_committed_object(client: Any, agent: FakeAgent, monkeypatch: Any) -> None:
    """A re-send that passed the pre-check before the first copy committed must answer duplicate (not 409 gap) and
    must not delete the object the committed row references."""
    from alelt_hub import blobstore, gateway

    hdr, body = _make(agent, 1020)
    assert _post(client, agent, hdr, body).status_code == 200
    agent.next_from = hdr.next_pos.text
    real_connect = gateway.get_engine().connect
    calls = {"n": 0}

    class _NoDupView:                                      # first (pre-check) connection does not see the row yet
        def __init__(self, c: Any) -> None:
            self.c = c

        def __enter__(self) -> Any:
            inner = self.c.__enter__()

            class _Wrap:
                def execute(self, stmt: Any, *a: Any, **k: Any) -> Any:
                    r = inner.execute(stmt, *a, **k)
                    if "batch.sha256" in str(stmt) and calls["n"] == 0:
                        calls["n"] += 1

                        class _Empty:
                            def first(self) -> None:
                                return None
                        return _Empty()
                    return r

                def __getattr__(self, name: str) -> Any:
                    return getattr(inner, name)
            return _Wrap()

        def __exit__(self, *a: Any) -> Any:
            return self.c.__exit__(*a)

    eng = gateway.get_engine()
    monkeypatch.setattr(type(eng), "connect", lambda self: _NoDupView(real_connect()), raising=True)
    try:
        r = _post(client, agent, hdr, body)
    finally:
        monkeypatch.undo()
    assert r.status_code == 200 and r.json()["duplicate"] is True
    row = _row(hdr.batch_id)
    assert blobstore.read_ref(row.spool_path) == body       # object still there


def test_orphan_gc_mark_and_sweep(client: Any, agent: FakeAgent) -> None:
    from alelt_hub import blobstore, spool_gc
    from alelt_hub.db import get_engine

    st = blobstore.get_store()
    hdr, body = _make(agent, 1030)
    assert _post(client, agent, hdr, body).status_code == 200
    agent.next_from = hdr.next_pos.text
    orphan = blobstore.batch_key(CONN, str(uuid.uuid4()), "zlib")
    foreign = f"decoded-cache/{uuid.uuid4()}.jsonl"
    st.put(orphan, b"orphan")
    st.put(foreign, b"not ours")
    eng = get_engine()
    r1 = spool_gc.collect(eng, st, grace_s=3600, now=1_000_000.0)
    assert orphan not in r1.deleted and r1.candidates >= 1    # first sighting only marks
    r2 = spool_gc.collect(eng, st, grace_s=3600, now=1_000_000.0 + 3601)
    assert orphan in r2.deleted
    assert st.size(orphan) is None
    assert st.size(foreign) is not None                       # other layouts are never touched
    assert blobstore.read_ref(_row(hdr.batch_id).spool_path) == body
    st.delete(foreign)
