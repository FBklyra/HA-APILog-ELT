"""Worker lanes, stream marker, cooperative yield and flush discipline (RC4, FIX-DESIGN WP3), both metadata backends,
in-process rig with a counting in-memory destination (no landing needed)."""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import func, select, update

from alelt_common.util import utcnow
from alelt_hub.testing import FakeAgent, SimEdge, sim_catalog

from .conftest import TOKEN
from .test_completion import DEST, WmDest


class CountingDest(WmDest):
    """Counts destination transactions: data applies vs checkpoint-only applies."""

    def __init__(self) -> None:
        super().__init__()
        self.data_txns = 0
        self.ckpt_txns = 0
        self.fail_ddl: dict[str, int] = {}

    def apply(self, stream: str, changesets: Any, to_pos: Any, batch_id: str, table_map: Any, tables: Any,
              **kw: Any) -> Any:
        if any(cs.events for cs in changesets):
            self.data_txns += 1
        else:
            self.ckpt_txns += 1
        return super().apply(stream, changesets, to_pos, batch_id, table_map, tables, **kw)

    def ensure_table(self, table: Any, dest_name: str, golden_ddl: Any) -> None:
        if self.fail_ddl.get(dest_name, 0) > 0:
            self.fail_ddl[dest_name] -= 1
            raise RuntimeError(f"injected DDL failure on {dest_name}")
        super().ensure_table(table, dest_name, golden_ddl)


@pytest.fixture()
def env(client: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    from alelt_hub import pipeline

    monkeypatch.setattr(pipeline, "make_destination",
                        lambda s, policy, source, spec=None: DEST.setdefault(policy.connection_id, CountingDest()))
    monkeypatch.setenv("ALELT_INITIAL_LOAD", "none")
    return client


def _conn(client: Any, n: int = 2, doc: dict[str, Any] | None = None) -> tuple[str, FakeAgent, Any]:
    from alelt_hub import config_store as cs
    from alelt_hub.db import get_engine, pipeline_definition

    cid = f"wl{uuid.uuid4().hex[:6]}"
    a = FakeAgent(client, TOKEN, agent_id=f"{cid}-agent", connection_id=cid)
    a.register()
    if doc is not None:
        base = {"connection_id": cid, "output_profile": "alelt"}
        with get_engine().begin() as c:
            c.execute(pipeline_definition.insert().values(
                connection_id=cid, origin="wizard", environment="TEST", source_engine="fake",
                destination_engine="postgres", source={}, destination={}, tables_doc=base, status="SHADOW",
                created_by="t"))
            cs.commit(c, cid, {**base, **doc}, "t", "test", "test")
    cat = sim_catalog(cid, n, lob_every=0)
    a.catalog(cat)
    return cid, a, cat


def _worker(name: str) -> Any:
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    return Worker(get_engine(), get_settings(), name)


def _received(cid: str) -> int:
    from alelt_hub.db import batch, get_engine

    with get_engine().connect() as c:
        return int(c.execute(select(func.count()).select_from(batch).where(
            batch.c.connection_id == cid, batch.c.status == "RECEIVED")).scalar_one())


def _flood(e: SimEdge, fqn: str, n: int, base: int = 0) -> None:
    for i in range(n):
        e.tx([("INSERT", fqn, base + i, f"n{i}", None)])


def _park_others(keep: set[str]) -> None:
    from alelt_hub.db import get_engine, job

    with get_engine().begin() as c:                         # other suites' leftovers are not part of the scenario
        for jid, pl in c.execute(select(job.c.id, job.c.payload).where(job.c.status == "QUEUED")).all():
            if str((pl or {}).get("connection_id")) not in keep:
                c.execute(update(job).where(job.c.id == jid).values(status="DONE", error="parked by lane test"))


# ------------------------------------------------------------------ 1. control lane runs while batches are RECEIVED
def test_control_lane_runs_while_stream_is_busy(env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from alelt_hub import jobs
    from alelt_hub.db import get_engine

    monkeypatch.setenv("ALELT_DECODE_SLICE_S", "0.001")
    cid, a, cat = _conn(env)
    e = SimEdge(env, a, cat)
    _flood(e, "SIM.T001", 160)
    with get_engine().begin() as c:
        jobs.enqueue(c, "integrity", {"connection_id": cid, "table": "SIM.T001"}, dedup_key=f"probe:{uuid.uuid4()}")
    _park_others({cid})
    w = _worker(f"{cid}-w")
    assert w.run_once("data")                                # one bounded slice of the stream
    left = _received(cid)
    assert w.run_once("control")                             # the control lane does not wait for the stream
    assert left > 0 and _received(cid) == left               # it ran while batches were still undecoded
    while w.run_once("data"):
        pass
    assert _received(cid) == 0 and len(DEST[cid].live("T001")) == 160


# ------------------------------------------------------------------ 2. fairness across connections
def test_second_connection_runs_between_slices_of_a_flooding_one(env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALELT_DECODE_SLICE_S", "0.001")
    ca, aa, cata = _conn(env)
    cb, ab, catb = _conn(env)
    ea, eb = SimEdge(env, aa, cata), SimEdge(env, ab, catb)
    _flood(ea, "SIM.T000", 250)
    _flood(eb, "SIM.T000", 1)
    _park_others({ca, cb})
    w = _worker(f"{ca}-w")
    order: list[str] = []
    orig = w._run_kind

    def spy(j: dict[str, Any]) -> None:
        if j["kind"] == "decode":
            order.append(str(j["payload"]["connection_id"]))
        orig(j)
    w._run_kind = spy  # type: ignore[method-assign]
    while w.run_once("data"):
        pass
    first_b = order.index(cb)
    assert first_b <= 2, order[:6]                            # B waits at most two slices of A
    assert len(DEST[ca].live("T000")) == 250 and len(DEST[cb].live("T000")) == 1


# ------------------------------------------------------------------ 3. cooperative yield
def test_pipeline_request_ends_the_slice_within_one_chunk(env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from alelt_hub.snapshots import release_pipeline_request, request_pipeline
    from alelt_hub.db import get_engine

    monkeypatch.setenv("ALELT_DECODE_SLICE_S", "600")
    cid, a, cat = _conn(env)
    e = SimEdge(env, a, cat)
    _flood(e, "SIM.T000", 200)
    _park_others({cid})
    w = _worker(f"{cid}-w")
    request_pipeline(get_engine(), cid, "test: snapshot hold")
    try:
        assert w.run_once("data")
        p = w.pipeline(cid)
        assert p.more and _received(cid) >= 200 - p.READ_CHUNK      # one chunk, then yield
    finally:
        release_pipeline_request(get_engine(), cid)
    while w.run_once("data"):
        pass
    assert _received(cid) == 0


# ------------------------------------------------------------------ 4./6. flush discipline
def _closed_window() -> dict[str, str]:
    now = datetime.now(timezone.utc)
    return {"start": (now + timedelta(hours=3)).strftime("%H:%M"), "end": (now + timedelta(hours=4)).strftime("%H:%M"),
            "tz": "UTC"}


def test_empty_buffer_no_destination_transaction_then_checkpoint_only(env: Any) -> None:
    from alelt_hub.db import checkpoint_mirror, get_engine

    cid, a, cat = _conn(env, doc={"schemas": {"SIM": {"enabled": True, "tables": {
        "T000": {"schedule": {"window": _closed_window()}}}}}})
    e = SimEdge(env, a, cat)
    w = _worker(f"{cid}-w")
    _flood(e, "SIM.T000", 30)                                 # every change belongs to a held table
    _park_others({cid})
    while w.run_once():
        pass
    d = DEST[cid]
    assert d.data_txns == 0 and d.ckpt_txns == 0              # nothing to apply -> no destination transaction
    with get_engine().connect() as c:
        m = c.execute(select(checkpoint_mirror).where(checkpoint_mirror.c.stream == cid)).mappings().one()
    assert m["position"] and m["dest_position"] != m["position"]   # mirror ahead of the destination
    p = w.pipeline(cid)
    p._last_dest_apply -= 3600                               # ALELT_CHECKPOINT_IDLE_S elapsed
    _flood(e, "SIM.T000", 1, base=100)
    while w.run_once():
        pass
    assert d.ckpt_txns == 1 and d.data_txns == 0
    with get_engine().connect() as c:
        m = c.execute(select(checkpoint_mirror).where(checkpoint_mirror.c.stream == cid)).mappings().one()
    assert m["dest_position"] == m["position"] and d.ck[cid].text == m["position"]
    _flood(e, "SIM.T001", 3, base=200)                        # a streaming table: data applied normally
    while w.run_once():
        pass
    assert d.data_txns >= 1 and len(d.live("T001")) == 3
    for i in range(3):                                       # restart x3: replay from restart_seq, nothing doubled
        w = _worker(f"{cid}-w{i}")
        _flood(e, "SIM.T001", 1, base=300 + i)
        while w.run_once():
            pass
    assert len(d.live("T001")) == 6
    assert sum(1 for s, t, k, key in d.applied if t == "SIM.T001") == 6


def test_zero_row_destination_transactions_bounded_at_latency_zero(env: Any) -> None:
    cid, a, cat = _conn(env, n=3, doc={"schemas": {"SIM": {"enabled": True, "tables": {
        "T000": {"schedule": {"window": _closed_window()}}}}}})
    e = SimEdge(env, a, cat)
    w = _worker(f"{cid}-w")
    t0 = time.monotonic()
    for i in range(40):                                      # mixed churn: held + streaming tables, latency 0
        e.tx([("INSERT", "SIM.T000", i, "h", None)] + ([("INSERT", "SIM.T001", i, "s", None)] if i % 4 == 0 else []))
        w.run_once()
    while w.run_once():
        pass
    d = DEST[cid]
    run_s = time.monotonic() - t0
    assert d.ckpt_txns <= run_s / 60 + 1, (d.ckpt_txns, run_s)   # ALELT_CHECKPOINT_IDLE_S bound
    assert d.data_txns <= 10 and len(d.live("T001")) == 10


# ------------------------------------------------------------------ 5. DDL failure: control lane retries
def test_ddl_failure_held_alone_control_lane_retry(env: Any) -> None:
    from alelt_hub import holds
    from alelt_hub.db import get_engine, job

    cid, a, cat = _conn(env)
    DEST[cid] = CountingDest()
    DEST[cid].fail_ddl = {"T000": 1}
    e = SimEdge(env, a, cat)
    w = _worker(f"{cid}-w")
    e.tx([("INSERT", "SIM.T000", 1, "a", None), ("INSERT", "SIM.T001", 1, "b", None)])
    while w.run_once("data"):
        pass
    h = holds.get(get_engine(), cid, "SIM.T000", "ddl")
    assert h is not None and h["held"]
    assert len(DEST[cid].live("T001")) == 1 and not DEST[cid].live("T000")    # main stream unblocked
    with get_engine().begin() as c:
        c.execute(update(job).where(job.c.status == "QUEUED").values(run_after=utcnow() - timedelta(seconds=1)))
    holds.set_body(get_engine(), cid, "SIM.T000", "ddl", retry_at=(utcnow() - timedelta(seconds=1)).isoformat())
    for _ in range(30):
        with get_engine().begin() as c:
            c.execute(update(job).where(job.c.status == "QUEUED").values(run_after=utcnow() - timedelta(seconds=1)))
        if not w.run_once():
            w.sweep()
        if DEST[cid].live("T000") and not holds.get(get_engine(), cid, "SIM.T000", "ddl")["held"]:
            break
    assert DEST[cid].live("T000"), holds.get(get_engine(), cid, "SIM.T000", "ddl")
    assert not holds.get(get_engine(), cid, "SIM.T000", "ddl")["held"]
