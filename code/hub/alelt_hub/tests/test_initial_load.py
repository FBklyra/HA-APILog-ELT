"""Initial load of edge tables + out-of-row completion + queue fairness, both metadata backends, landing Postgres.

- rows that existed before capture started land through the initial snapshot (SNAPSHOT_TABLE answered by the
  simulated edge, text values, catch-up from the agent's snapshot position, post-snapshot VERIFY_TABLE -> verified);
- a catalog shaped like the test schema (ALELT_TEST_SIM_TABLES tables, default 222, a share with an out-of-row column)
  converges while out-of-row values arrive as needs_keyed_reread signals: completed by key in batches through
  VERIFY_TABLE{rows} (at most ceil(keys/1000) commands per table), no integrity row, no hold, no incident;
- a completion read at R never overwrites a change committed after R;
- the job queue never stalls behind the stream: one decode marker per connection, control lane runs meanwhile.
"""
from __future__ import annotations

import os
import time
import uuid
from typing import Any

import psycopg
import pytest

from alelt_hub.testing import FakeAgent, SimEdge, sim_catalog

from .conftest import TOKEN, need_landing

N_TABLES = int(os.environ.get("ALELT_TEST_SIM_TABLES", "222"))
LOB_EVERY = 6                                     # every 6th table has an out-of-row column (~37 of 222)


def landing(sql: str, params: Any = None) -> list[tuple[Any, ...]]:
    with psycopg.connect(os.environ["ALELT_DEST_URL"]) as c:
        return c.execute(sql, params).fetchall()  # type: ignore[arg-type]


def converge(w: Any, done: Any, timeout_s: float = 240) -> float:
    """Run the worker (jobs + periodic sweep) until done() or the bound; returns seconds."""
    t0 = time.monotonic()
    last_sweep = 0.0
    while time.monotonic() - t0 < timeout_s:
        busy = w.run_once()
        if time.monotonic() - last_sweep > 2:
            w.sweep()
            last_sweep = time.monotonic()
            if done():
                return time.monotonic() - t0
        if not busy:
            time.sleep(0.05)
    raise AssertionError(f"not converged within {timeout_s:.0f}s: {diag(w)}")


def diag(w: Any) -> str:
    from alelt_hub import holds
    from alelt_hub.db import job
    from sqlalchemy import func, select

    rows = [(r["table_fqn"], r["kind"], r["state"], r["held"], (r["body"] or {}).get("step"))
            for r in holds.rows(w.engine) if r["held"] or r["state"] not in ("IN_SYNC", "OPEN")]
    with w.engine.connect() as c:
        q = c.execute(select(job.c.kind, job.c.status, func.count()).group_by(job.c.kind, job.c.status)).all()
        errs = c.execute(select(job.c.kind, job.c.error).where(job.c.error.is_not(None),
                                                               job.c.status != "DONE").limit(5)).all()
        from alelt_hub.db import recon_result

        rec = [(o, (d or "")[:300], str(dr)[:300]) for o, d, dr in c.execute(
            select(recon_result.c.outcome, recon_result.c.detail, recon_result.c.drill).where(
                recon_result.c.outcome != "MATCH").limit(3))]
    return f"holds {rows[:12]} (+{max(0, len(rows) - 12)}); jobs {q}; errors {errs}; recon {rec}"


def dest_rows(cid: str, fqn: str, lob: bool) -> dict[int, tuple[Any, ...]]:
    t = fqn.partition(".")[2].lower()
    if landing("SELECT to_regclass(%s) IS NULL", (f"{cid}.{t}",))[0][0]:
        return {}
    from alelt_loader import load_policy

    dele = load_policy(cid).system.deleted.lower()
    cols = "id, name" + (", doc" if lob else "")
    return {r[0]: tuple(r[1:]) for r in landing(
        f"SELECT {cols} FROM {cid}.{t} WHERE NOT coalesce({dele}, false)")}


def truth_rows(edge: SimEdge, fqn: str) -> dict[int, tuple[Any, ...]]:
    lob = edge.lob(fqn)
    with edge.lock:
        return {k: (r["NAME"],) + ((r["DOC"],) if lob else ()) for k, r in edge.truth[fqn].items()}


@pytest.fixture()
def initial_auto(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALELT_INITIAL_LOAD", "auto")
    monkeypatch.setenv("ALELT_COMPLETION_POLL_S", "0")
    monkeypatch.setenv("ALELT_VERIFY_EDGE_TIMEOUT_S", "20")
    from alelt_hub import edge_verify, integrity

    monkeypatch.setitem(integrity._HOOKS, "fake", edge_verify.EdgeHooks())   # the fake engine runs behind an agent


def _no_integrity(cid: str) -> None:
    from sqlalchemy import func, select

    from alelt_diagnose.tables import incident
    from alelt_hub.db import get_engine, quarantine_event, table_completion, table_hold

    with get_engine().connect() as c:
        integ = c.execute(select(func.count()).select_from(table_hold).where(
            table_hold.c.connection_id == cid, table_hold.c.kind == "integrity")).scalar_one()
        pend = c.execute(select(func.count()).select_from(table_completion).where(
            table_completion.c.connection_id == cid)).scalar_one()
        q = c.execute(select(func.count()).select_from(quarantine_event).where(
            quarantine_event.c.connection_id == cid, quarantine_event.c.reason.like("signal needs_keyed_reread%"))
        ).scalar_one()
        inc = c.execute(select(func.count()).select_from(incident).where(
            incident.c.connection_id == cid, incident.c.kind.in_(("TABLE_OUT_OF_SYNC", "COMPLETION_STALLED",
                                                                  "COMPLETION_OVERSIZE")))).scalar_one()
    assert (integ, pend, q, inc) == (0, 0, 0, 0), (integ, pend, q, inc)


def _streaming(cid: str, n: int) -> bool:
    from alelt_hub import holds
    from alelt_hub.db import get_engine

    try:
        from alelt_hub import lifecycle

        rows = lifecycle.rows(get_engine(), cid)
        return len(rows) == n and all(r["phase"] == "STREAMING" and r["verified_at"] for r in rows)
    except ImportError:                                      # before the lifecycle module: snapshot holds
        snaps = holds.rows(get_engine(), cid, "snapshot")
        return len(snaps) == n and all(r["state"] == "IN_SYNC" for r in snaps)


def test_initial_snapshot_lands_rows_that_existed_before_capture(client: Any, initial_auto: None) -> None:
    """e2e_local parity gap: rows present before capture (C0002..C0004) land through the initial snapshot; the
    out-of-row value changed after capture start is completed by key (no integrity row)."""
    need_landing()
    from alelt_hub import holds
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    cid = "initconn"
    a = FakeAgent(client, TOKEN, agent_id="init-agent", connection_id=cid)
    a.register()
    cat = sim_catalog(cid, 2)                         # T000 (with out-of-row DOC) + T001
    a.catalog(cat)
    w = Worker(get_engine(), get_settings(), "init-worker")
    with SimEdge(client, a, cat) as e:
        for i in (2, 3, 4):
            e.seed("SIM.T000", i, f"C000{i}", doc=f"note {i}")
            e.seed("SIM.T001", i, f"D{i}")
        e.tx([("INSERT", "SIM.T001", 9, "new", None)])          # first change
        e.tx([("UPDATE", "SIM.T000", 3, "C0003-upd", None)])    # after capture start, before the snapshot read

        def done() -> bool:
            return _streaming(cid, 2) and all(dest_rows(cid, f, e.lob(f)) == truth_rows(e, f) for f in e.infos)
        converge(w, done, 120)
        assert e.seen.get("SNAPSHOT_TABLE") == 2 and e.seen.get("VERIFY_TABLE", 0) >= 2
        e.tx([("UPDATE", "SIM.T000", 2, "after", "doc v2")])     # CDC after the initial load (out-of-row change)
        converge(w, lambda: dest_rows(cid, "SIM.T000", True) == truth_rows(e, "SIM.T000")
                 and not holds.held_tables(get_engine(), cid) and _pending(cid) == 0, 60)
    assert dest_rows(cid, "SIM.T000", True)[2] == ("after", "doc v2")
    _no_integrity(cid)


def _pending(cid: str) -> int:
    from sqlalchemy import func, select

    from alelt_hub.db import get_engine, table_completion

    with get_engine().connect() as c:
        return int(c.execute(select(func.count()).select_from(table_completion).where(
            table_completion.c.connection_id == cid)).scalar_one())


def test_catalog_shaped_schema_with_out_of_row_values_converges(client: Any, initial_auto: None,
                                                                monkeypatch: pytest.MonkeyPatch) -> None:
    """222-table-shaped catalog: initial snapshot of every table, then churn with out-of-row values on ~1/6 of the
    tables (needs_keyed_reread): completed by key in batches (VERIFY_TABLE{rows} <= ceil(keys/1000) per table), no
    integrity row, no hold, no incident; every table converges and the queue drains."""
    need_landing()
    import math

    from sqlalchemy import func, select

    from alelt_hub import holds
    from alelt_hub.db import get_engine, job
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    monkeypatch.setenv("ALELT_DECODE_SLICE_S", "60")
    monkeypatch.setenv("ALELT_COMPLETION_POLL_S", "0")
    cid = "simconn"
    a = FakeAgent(client, TOKEN, agent_id="sim-agent", connection_id=cid)
    a.register()
    cat = sim_catalog(cid, N_TABLES, lob_every=LOB_EVERY)
    a.catalog(cat)
    fqns = sorted(t.table.fqn for t in cat.tables)
    w = Worker(get_engine(), get_settings(), "sim-worker")
    t0 = time.monotonic()
    with SimEdge(client, a, cat) as e:
        lobs = [f for f in fqns if e.lob(f)]
        for f in fqns:                                   # rows before capture (the initial load)
            for i in range(1, 4):
                e.seed(f, i, f"{f}-{i}", doc=f"doc {f} {i}" if e.lob(f) else None)
        e.tx([("INSERT", fqns[1], 100, "first", None)])
        converge(w, lambda: _streaming(cid, len(fqns)), 600)
        t_init = time.monotonic() - t0
        rows_cmds0 = sum(1 for n, x in e.args if n == "VERIFY_TABLE" and x.get("checks") == ["rows"])
        # churn (sent while the worker does not run: decoded in one slice): inserts / updates / deletes; LOB values
        # change (signals) on every LOB table
        keys: dict[str, set[int]] = {f: set() for f in lobs}
        for k in range(3):
            ops: list[tuple[str, str, int, str | None, str | None]] = []
            for j, f in enumerate(fqns):
                ops.append(("INSERT", f, 10 + k, f"ins{k}", f"lob {f} {k}" if e.lob(f) else None))
                if e.lob(f):
                    keys[f].add(10 + k)
                ops.append(("UPDATE", f, 1, f"upd{k}", f"lob-upd {k}" if e.lob(f) and j % 2 == 0 else None))
                if e.lob(f) and j % 2 == 0:
                    keys[f].add(1)
                if k == 2:
                    ops.append(("DELETE", f, 2, None, None))
            for x in range(0, len(ops), 120):
                e.tx(ops[x:x + 120])
        e.tx([("UPDATE", lobs[0], 3, "late name", None)])     # non-LOB change after the LOB changes

        def all_equal() -> bool:
            if holds.held_tables(get_engine(), cid) or _pending(cid):
                return False
            return all(dest_rows(cid, f, e.lob(f)) == truth_rows(e, f) for f in fqns)
        try:
            converge(w, all_equal, float(os.environ.get("ALELT_TEST_SIM_TIMEOUT_S", "600")))
        except AssertionError as ex:
            diffs = {f: (dest_rows(cid, f, e.lob(f)), truth_rows(e, f)) for f in fqns
                     if dest_rows(cid, f, e.lob(f)) != truth_rows(e, f)}
            raise AssertionError(f"{ex} diffs {str(diffs)[:1500]}") from None
        per_table: dict[str, int] = {}
        for n, x in e.args[:]:
            if n == "VERIFY_TABLE" and x.get("checks") == ["rows"]:
                f = f"{x['table']['schema']}.{x['table']['name']}"
                per_table[f] = per_table.get(f, 0) + 1
    with get_engine().connect() as c:
        st = dict(c.execute(select(job.c.status, func.count()).where(job.c.kind != "maintain")
                            .group_by(job.c.status)).all())
    assert "FAILED" not in st, st
    _no_integrity(cid)
    churn_cmds = sum(per_table.values()) - rows_cmds0
    assert churn_cmds <= sum(math.ceil(len(v) / 1000) for v in keys.values()) + rows_cmds0, (per_table, keys)
    assert dest_rows(cid, lobs[0], True)[3][0] == "late name"
    print(f"\n{len(fqns)} tables ({len(lobs)} with out-of-row values): initial load {t_init:.0f}s, "
          f"total {time.monotonic() - t0:.0f}s, commands {e.seen}")


def test_completion_read_never_overwrites_a_newer_change(client: Any, initial_auto: None,
                                                         monkeypatch: pytest.MonkeyPatch) -> None:
    """A completion read at R never overwrites a change committed after R (only the out-of-row columns are written,
    placed before every later change of the key)."""
    need_landing()
    from alelt_hub import holds
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    monkeypatch.setenv("ALELT_INITIAL_LOAD", "none")
    cid = "rrposconn"
    a = FakeAgent(client, TOKEN, agent_id="rrpos-agent", connection_id=cid)
    a.register()
    cat = sim_catalog(cid, 1)
    a.catalog(cat)
    w = Worker(get_engine(), get_settings(), "rrpos-worker")
    with SimEdge(client, a, cat) as e:
        import copy

        e.tx([("INSERT", "SIM.T000", 1, "v1", "doc1")])          # signal -> completion entry
        with e.lock:                                             # the edge read happens HERE (sees NAME v1) ...
            e.frozen = (e.pos, copy.deepcopy(e.truth))
        e.tx([("UPDATE", "SIM.T000", 1, "v2", None)])            # ... this change commits after it
        converge(w, lambda: bool(dest_rows(cid, "SIM.T000", True)) and _pending(cid) == 0
                 and not holds.held_tables(get_engine(), cid), 60)
    assert dest_rows(cid, "SIM.T000", True)[1] == ("v2", "doc1")
    _no_integrity(cid)


def test_queue_never_stalls_behind_the_stream(client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Hundreds of received batches = ONE decode marker; the control lane (integrity / snapshot / verify) runs while
    batches are still undecoded; the stream decodes everything in time slices."""
    need_landing()
    from sqlalchemy import func, select

    from alelt_hub import jobs
    from alelt_hub.db import batch, get_engine, job
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    monkeypatch.setenv("ALELT_DECODE_SLICE_S", "0.001")
    cid = "stallconn"
    a = FakeAgent(client, TOKEN, agent_id="stall-agent", connection_id=cid)
    a.register()
    cat = sim_catalog(cid, 3)
    a.catalog(cat)
    e = SimEdge(client, a, cat)
    for i in range(160):
        e.tx([("INSERT", "SIM.T001", i, f"n{i}", None)])
    eng = get_engine()
    with eng.connect() as c:
        markers = c.execute(select(func.count()).select_from(job).where(
            job.c.kind == "decode", job.c.status == "QUEUED", job.c.payload["connection_id"].as_string() == cid)
        ).scalar_one()
    assert markers == 1                                  # one marker, not one job per batch
    with eng.begin() as c:
        jobs.enqueue(c, "integrity", {"connection_id": cid, "table": "SIM.T001"}, dedup_key=f"probe:{uuid.uuid4()}")
    w = Worker(eng, get_settings(), "stall-worker")
    order: list[tuple[str, int]] = []
    orig = w._run_kind

    def spy(j: dict[str, Any]) -> None:
        with eng.connect() as c:
            left = c.execute(select(func.count()).select_from(batch).where(
                batch.c.connection_id == cid, batch.c.status == "RECEIVED")).scalar_one()
        order.append((j["kind"], int(left)))
        orig(j)
    w._run_kind = spy  # type: ignore[method-assign]
    for _ in range(400):
        if not w.run_once():
            break
    with eng.connect() as c:
        left = c.execute(select(func.count()).select_from(batch).where(
            batch.c.connection_id == cid, batch.c.status != "LOADED")).scalar_one()
    integ = next(i for i, (k, _) in enumerate(order) if k == "integrity")
    assert order[integ][1] > 0, order[:10]           # ran while batches were still undecoded (no starvation)
    assert integ <= 2, order[:5]
    assert left == 0, order[-5:]
    assert landing("SELECT count(*) FROM stallconn.t001")[0][0] == 160
