"""HC-08 through the hub: a remote transaction far above the chunk size is shipped in chunk batches by the capture
runner (memory bounded, capture never stops), decoded + assembled across batches by the worker and applied once."""
from __future__ import annotations

import json
import os
import uuid
from typing import Any

import pytest

from alelt_hub.tests import fakes
from alelt_hub.tests.conftest import drain, need_landing


def _landing(sql: str) -> list[tuple[Any, ...]]:
    import psycopg

    with psycopg.connect(os.environ["ALELT_DEST_URL"]) as c:
        return c.execute(sql).fetchall()  # type: ignore[arg-type]


@pytest.fixture()
def remote(client: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    from alelt_capture import receipt
    from alelt_hub import store
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings

    cid = f"lt{uuid.uuid4().hex[:6]}"
    get_settings().sources["fakeremote"] = "alelt_hub.tests.fakes:FakeRemote"
    store.source_connector.cache_clear()
    fakes.WORLDS[cid] = fakes.World()
    monkeypatch.setenv("FAKE_REMOTE_PW", fakes.WORLDS[cid].password)
    monkeypatch.setenv("ALELT_CAPTURE_CONNECTIONS", json.dumps(
        {cid: {"engine": "fakeremote", "settings": {"world": cid}, "password_env": "FAKE_REMOTE_PW"}}))
    receipt.ensure_registered(get_engine(), cid, "fakeremote")
    receipt.ingest_catalog(get_engine(), fakes.catalog(cid))
    return cid


@pytest.mark.parametrize("crash", [False, True])
def test_large_remote_txn_chunked_and_applied_once(client: Any, remote: str, crash: bool,
                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    need_landing()
    from sqlalchemy import select

    from alelt_capture.config import connections
    from alelt_capture.runner import CaptureConfig, CaptureRunner
    from alelt_hub.db import batch, get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    from alelt_hub.snapshots import SnapshotService

    cid, w_ = remote, fakes.WORLDS[remote]
    w_.txn([("INSERT", fakes.item(1, "first"))])
    w = Worker(get_engine(), get_settings(), "lt-worker")
    assert SnapshotService(get_engine(), w.pipeline(cid)).start() == ["S.ITEMS"]
    w_.txn([("INSERT", fakes.item(i, f"n{i}")) for i in range(2, 3002)])          # one ~450 KB transaction
    w_.txn([("INSERT", fakes.item(5000, "after"))])
    if crash:                                            # runner dies after the 4th durable chunk, restarts
        from alelt_capture import receipt

        real, calls = receipt.receive, {"n": 0}

        def flaky(hdr: Any, body: bytes) -> Any:
            calls["n"] += 1
            if calls["n"] == 6:
                raise OSError("runner killed")
            return real(hdr, body)
        monkeypatch.setattr(receipt, "receive", flaky)
    r = CaptureRunner(get_engine(), cid, connections(get_engine())[cid].factory(),
                      CaptureConfig(batch_ms=0, retry_s=0.1, txn_chunk_bytes=30_000))
    assert r.run(idle_exit_s=0.3, max_restarts=3) == "idle"
    with get_engine().connect() as c:
        bs = c.execute(select(batch.c.from_lri, batch.c.to_lri, batch.c.raw_bytes)
                       .where(batch.c.connection_id == cid).order_by(batch.c.seq)).all()
    assert sum(1 for f, t, _ in bs if f == t) >= 5 and max(n for _, _, n in bs) < 60_000
    drain(w)
    rows = _landing(f"SELECT id, count(*) FROM {cid}.items GROUP BY id ORDER BY id")
    assert [i for i, _ in rows] == [1, *range(2, 3002), 5000] and all(n == 1 for _, n in rows)
