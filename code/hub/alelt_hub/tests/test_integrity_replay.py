"""F-01 / F-06 / F-08 (+ HC-02/03/04/09): table integrity holds, the replay engine, schedule holds from the start.

Runs on every metadata backend WITHOUT a landing database: the destination is an in-memory SPI implementation that
keeps checkpoints with compare-and-set and records every applied change, so exactly-once is asserted directly."""
from __future__ import annotations

import dataclasses
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import select, update

from alelt_common.models import (
    CanonicalType,
    CatalogSnapshot,
    ColumnInfo,
    Position,
    QuarantineRecord,
    TableInfo,
    TableRef,
)
from alelt_common.spi import ApplyStats, DecodeResult, DestinationConnector
from alelt_hub.testing import FakeAgent, FakeSource, ev, fake_catalog, lri

from .conftest import TOKEN

ENGINE = "fakeq"
MEM: dict[str, "MemDest"] = {}


class AttrSource(FakeSource):
    """FakeSource whose decoder quarantines (attributed to the table) any change with a column the catalog does not
    know - the F-07 shape: records between a DDL and the catalog refresh."""

    engine = ENGINE
    decoder_version = "t1"

    def decode(self, raw: bytes, catalog: CatalogSnapshot, batch_id: Any) -> DecodeResult:
        res = super().decode(raw, catalog, batch_id)
        cols = {t.table: {c.name for c in t.columns} for t in catalog.tables}
        keep, q = [], list(res.quarantined)
        for e in res.events:
            if e.after and e.after.get("SIG"):                    # completeness signal (X-132): keyed re-read
                from alelt_common.completeness import keyed_reread

                keep.append(keyed_reread(e.table, [{"ID": e.after["ID"]}], "overflow record").event(  # type: ignore[arg-type]
                    Position(engine=ENGINE, text=e.position.text), e.txid))
                continue
            known = cols.get(e.table) if e.table is not None else None
            if known is not None and any(k not in known for k in (e.after or {})):
                q.append(QuarantineRecord(position=e.position, table=e.table, reason="column not in catalog: layout",
                                          raw_ref=e.raw_ref, raw_hex="00"))
            else:
                keep.append(e.model_copy(update={"position": Position(engine=ENGINE, text=e.position.text)}))
        return DecodeResult(events=keep, quarantined=q)


class MemDest(DestinationConnector):
    """In-memory destination: checkpoint CAS per stream, rows per table, every applied change recorded."""

    engine = "mem"

    def __init__(self) -> None:
        self.ck: dict[str, Position] = {}
        self.rows: dict[str, dict[Any, dict[str, Any]]] = {}
        self.applied: list[tuple[str, str, str, str]] = []      # stream, table, kind, row key
        self.fail_after: int | None = None                      # raise on the n-th apply (crash injection)
        self.ddl: list[tuple[str, str, str]] = []
        self.calls = 0
        self.cmp = FakeSource().compare

    def bootstrap(self) -> None:
        pass

    def ensure_table(self, table: TableInfo, dest_name: str, golden_ddl: str | None) -> None:
        self.rows.setdefault(dest_name, {})
        self.ddl.append(("ensure", dest_name, ",".join(f"{c.name}:{c.length}" for c in table.columns)))

    def add_column(self, dest_name: str, col: ColumnInfo) -> None:
        self.ddl.append(("add_column", dest_name, col.name))

    def publish_view(self, view_schema: str, table: str, source_schema: str, columns: list[str]) -> None:
        self.ddl.append(("view", f"{view_schema}.{table}", ",".join(columns)))

    def read_checkpoint(self, stream: str) -> Position | None:
        return self.ck.get(stream)

    def apply(self, stream: str, changesets: Any, to_pos: Position, batch_id: str, table_map: Any, tables: Any,
              **_: Any) -> list[ApplyStats]:
        self.calls += 1
        if self.fail_after is not None and self.calls >= self.fail_after:
            raise RuntimeError("injected destination crash before COMMIT")
        ck = self.ck.get(stream)
        if ck is not None and self.cmp(to_pos, ck) <= 0:
            return []
        stats: dict[str, ApplyStats] = {}
        for cs in changesets:
            for e in cs.events:
                name = table_map[e.table]
                keys = [c.name for c in tables[e.table].key_columns()]
                img = e.after or e.before or {}
                k = tuple(str(img.get(c)) for c in keys) if keys else json.dumps(img, sort_keys=True, default=str)
                t = self.rows.setdefault(name, {})
                if e.kind.value == "DELETE":
                    t[k] = {**t.get(k, img), "_deleted": True}
                else:
                    t[k] = {**t.get(k, {}), **(e.after or {}), "_deleted": False}
                self.applied.append((stream, e.table.fqn, e.kind.value, str(k)))
                stats.setdefault(name, ApplyStats(table=name)).inserts += 1
        self.ck[stream] = to_pos
        return list(stats.values())

    def load_snapshot(self, dest_name: str, table: TableInfo, rows: Any) -> int:
        n = 0
        t = self.rows.setdefault(dest_name, {})
        keys = [c.name for c in table.key_columns()]
        for r in rows:
            t[tuple(str(r.get(c)) for c in keys)] = {**r, "_deleted": False}
            n += 1
        return n

    def live(self, name: str) -> dict[Any, dict[str, Any]]:
        return {k: v for k, v in self.rows.get(name, {}).items() if not v.get("_deleted")}


# ------------------------------------------------------------------ fixtures / helpers
@pytest.fixture()
def env(client: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    from alelt_hub import pipeline, store
    from alelt_hub.settings import get_settings

    get_settings().sources[ENGINE] = "alelt_hub.tests.test_integrity_replay:AttrSource"
    store.source_connector.cache_clear()
    monkeypatch.setattr(pipeline, "make_destination",
                        lambda s, policy, source, spec=None: MEM.setdefault(policy.connection_id, MemDest()))
    return client


def _conn(client: Any, doc: dict[str, Any] | None = None, tables: int | None = None) -> tuple[str, FakeAgent]:
    from alelt_hub.db import get_engine, pipeline_definition

    cid = f"iq{uuid.uuid4().hex[:6]}"
    a = FakeAgent(client, TOKEN, agent_id=f"{cid}-agent", connection_id=cid, engine=ENGINE)
    a.register()
    a.catalog(_catalog(cid, tables=tables))
    if doc is not None:
        with get_engine().begin() as c:
            c.execute(pipeline_definition.insert().values(
                connection_id=cid, origin="wizard", environment="TEST", source_engine=ENGINE,
                destination_engine="postgres", source={}, destination={},
                tables_doc={"connection_id": cid, "output_profile": "alelt", **doc}, status="SHADOW",
                created_by="t"))
    return cid, a


def _catalog(cid: str, extra: bool = False, tables: int | None = None, at: int | None = None,
             name_len: int | None = None) -> CatalogSnapshot:
    cat = fake_catalog(cid, engine=ENGINE)
    if at is not None:
        cat = cat.model_copy(update={"position": Position(engine=ENGINE, text=lri(at))})
    if name_len is not None:
        k = cat.tables[0]
        k.columns[1] = k.columns[1].model_copy(update={"length": name_len})
    if extra:                                                   # catalog refresh after the "DDL"
        k = cat.tables[0]
        k.columns.append(ColumnInfo(name="NEWCOL", ordinal=4, engine_type="STRING",
                                    canonical_type=CanonicalType.STRING, length=40))
    if tables:
        cat.tables += [TableInfo(table=TableRef(schema="M", name=f"T{i:03d}"), capture_enabled=True, columns=[
            ColumnInfo(name="ID", ordinal=0, engine_type="INT", canonical_type=CanonicalType.INT, nullable=False,
                       key_seq=1),
            ColumnInfo(name="V", ordinal=1, engine_type="STRING", canonical_type=CanonicalType.STRING, length=40)])
            for i in range(tables)]
    return cat.model_copy(update={"taken_at": datetime.now(timezone.utc)})


def _worker(name: str, **settings: Any) -> Any:
    from alelt_hub.db import get_engine
    from alelt_hub.settings import get_settings
    from alelt_hub.worker import Worker

    s = dataclasses.replace(get_settings(), **settings) if settings else get_settings()
    return Worker(get_engine(), s, name)


def _drain(w: Any, ready: bool = False, rounds: int = 3) -> None:
    """Run jobs until the queue is empty; `ready` also makes delayed jobs due (several rounds)."""
    from alelt_hub.db import get_engine, job

    for _ in range(rounds if ready else 1):
        if ready:
            with get_engine().begin() as c:
                c.execute(update(job).where(job.c.status == "QUEUED").values(run_after=datetime.now(timezone.utc)
                                                                             - timedelta(seconds=1)))
        n = 0
        while w.run_once():
            n += 1
            if n > 300:                                   # bounded: a job loop is a bug, never a hang
                with get_engine().connect() as c:
                    last = [(k, st, (pl or {}).get("connection_id"), (e or "")[:80]) for k, st, pl, e in c.execute(
                        select(job.c.kind, job.c.status, job.c.payload, job.c.error).order_by(job.c.id.desc())
                        .limit(12))]
                raise AssertionError(f"job loop: {last}")


def _set_doc(cid: str, doc: dict[str, Any]) -> None:
    from alelt_hub.db import get_engine, pipeline_definition

    with get_engine().begin() as c:
        c.execute(pipeline_definition.update().where(pipeline_definition.c.connection_id == cid).values(
            tables_doc={"connection_id": cid, "output_profile": "alelt", **doc}))


def _hold(cid: str, fqn: str, kind: str) -> dict[str, Any] | None:
    from alelt_hub import holds
    from alelt_hub.db import get_engine

    return holds.get(get_engine(), cid, fqn, kind)


# ------------------------------------------------------------------ F-01: quarantine -> table held, no divergence
def test_quarantine_holds_table_then_redecode_recovers(env: Any) -> None:
    from alelt_diagnose.tables import incident
    from alelt_hub import integrity
    from alelt_hub.db import get_engine, quarantine_event

    cid, a = _conn(env)
    w = _worker("iq-w1")
    a.send([ev(1, "T1", "INSERT", "S.KEYED", after={"ID": 1, "NAME": "one"}),
            ev(2, "T1", "INSERT", "S.LOG", after={"MSG": "m1", "N": 1}), ev(3, "T1", "COMMIT")])
    _drain(w)
    a.send([ev(4, "T2", "INSERT", "S.KEYED", after={"ID": 2, "NAME": "two", "NEWCOL": "x"}),   # undecodable
            ev(5, "T2", "INSERT", "S.LOG", after={"MSG": "m2", "N": 2}), ev(6, "T2", "COMMIT")])
    a.send([ev(7, "T3", "DELETE", "S.KEYED", before={"ID": 1, "NAME": "one"}),
            ev(8, "T3", "INSERT", "S.LOG", after={"MSG": "m3", "N": 3}), ev(9, "T3", "COMMIT")])
    _drain(w)
    d = MEM[cid]
    assert sorted(r["MSG"] for r in d.live("LOG").values()) == ["m1", "m2", "m3"]      # other tables keep flowing
    assert list(d.live("KEYED")) == [("1",)]                     # held at the record before: DELETE not applied
    r = _hold(cid, "S.KEYED", integrity.KIND)
    assert r is not None and r["held"] and r["state"] in (integrity.SUSPECT, integrity.REDECODING)
    assert r["start_pos"] == lri(3)                               # watermark = what the main stream applied
    assert r["body"]["first_pos"] == lri(4) and r["body"]["incident_id"]
    with get_engine().connect() as c:
        assert integrity.blocking(c, cid, "S.KEYED")              # certification / switch refused
        kinds = {k for k, in c.execute(select(incident.c.kind).where(incident.c.connection_id == cid))}
    assert "TABLE_OUT_OF_SYNC" in kinds
    cmd = next(x for x in env.get(f"/agent/v1/commands?agent_id={a.agent_id}&wait=0", headers=a.h).json()
               if x["name"] == "SNAPSHOT_CATALOG")                 # recovery step 1: catalog refresh requested
    assert cmd
    a.catalog(_catalog(cid, extra=True))                          # agent answers with the post-DDL catalog
    _drain(w, ready=True)
    r = _hold(cid, "S.KEYED", integrity.KIND)
    assert r["state"] == integrity.IN_SYNC and not r["held"] and r["body"]["recovered_by"] == "redecode", r
    assert {k: v.get("NEWCOL") for k, v in d.live("KEYED").items()} == {("2",): "x"}     # re-decoded + DELETE applied
    with get_engine().connect() as c:
        st = {s for s, in c.execute(select(quarantine_event.c.status).where(quarantine_event.c.connection_id == cid))}
        assert integrity.blocking(c, cid, "S.KEYED") is None
    assert st == {"RESOLVED"}
    a.send([ev(10, "T4", "INSERT", "S.KEYED", after={"ID": 3, "NAME": "three"}), ev(11, "T4", "COMMIT")])
    _drain(w)
    assert ("3",) in d.live("KEYED")                              # streaming again
    per: dict[tuple[str, str], int] = {}
    for _s, t, kind, key in d.applied:                            # exactly once per change across all streams
        if t == "S.KEYED":
            per[(kind, key)] = per.get((kind, key), 0) + 1
    assert per == {("INSERT", "('1',)"): 1, ("INSERT", "('2',)"): 1, ("DELETE", "('1',)"): 1,
                   ("INSERT", "('3',)"): 1}, per


def test_redecode_failure_falls_back_to_resync_or_manual(env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from alelt_hub import integrity

    cid, a = _conn(env)
    monkeypatch.setenv("ALELT_INTEGRITY_CATALOG_WAIT_S", "0")        # same catalog content: no new version will come
    w = _worker("iq-w2")
    a.send([ev(1, "T1", "INSERT", "S.KEYED", after={"ID": 1, "NAME": "x", "NEWCOL": "y"}), ev(2, "T1", "COMMIT")])
    _drain(w)
    a.catalog(_catalog(cid))                                      # refresh does not know the column either
    _drain(w, ready=True, rounds=5)
    r = _hold(cid, "S.KEYED", integrity.KIND)
    steps = [x["step"] for x in r["body"]["attempts"]]
    assert steps[:3] == ["catalog", "redecode", "resync"], steps
    # an unexplained record that a re-decode cannot fix -> resync = SNAPSHOT_TABLE through the agent (RESYNCING)
    assert r["state"] in (integrity.RESYNCING, integrity.OUT_OF_SYNC)
    assert "S.KEYED" in __import__("alelt_hub.holds", fromlist=["x"]).held_tables(w.engine, cid)
    assert ("1",) not in MEM[cid].live("KEYED")


def test_stop_stream_and_resume(env: Any) -> None:
    from alelt_hub import integrity
    from alelt_hub.db import get_engine

    cid, a = _conn(env, {"on_quarantine": "stop_stream"})
    w = _worker("iq-w3")
    a.send([ev(1, "T1", "INSERT", "S.LOG", after={"MSG": "a", "N": 1}), ev(2, "T1", "COMMIT")])
    a.send([ev(3, "T2", "INSERT", "S.KEYED", after={"ID": 1, "NEWCOL": "bad"}),
            ev(4, "T2", "INSERT", "S.LOG", after={"MSG": "b", "N": 2}), ev(5, "T2", "COMMIT")])
    a.send([ev(6, "T3", "INSERT", "S.LOG", after={"MSG": "c", "N": 3}), ev(7, "T3", "COMMIT")])
    _drain(w)
    d = MEM[cid]
    assert sorted(r["MSG"] for r in d.live("LOG").values()) == ["a"]      # nothing of / after the batch applied
    assert _hold(cid, "*", integrity.KIND)["held"]
    assert integrity.resume_stream(get_engine(), cid, "op") == ["S.KEYED"]
    _drain(w)
    assert sorted(r["MSG"] for r in d.live("LOG").values()) == ["a", "b", "c"]
    assert _hold(cid, "S.KEYED", integrity.KIND)["held"]                    # offending table: hold_table now


def test_continue_policy_is_loud_and_blocks_certification(env: Any) -> None:
    from alelt_hub import integrity
    from alelt_hub.db import get_engine

    cid, a = _conn(env, {"schemas": {"S": {"enabled": True, "tables": {"KEYED": {"on_quarantine": "continue"}}}}})
    w = _worker("iq-w4")
    a.send([ev(1, "T1", "INSERT", "S.KEYED", after={"ID": 1, "NEWCOL": "bad"}),
            ev(2, "T1", "INSERT", "S.KEYED", after={"ID": 2, "NAME": "ok"}), ev(3, "T1", "COMMIT")])
    _drain(w)
    assert list(MEM[cid].live("KEYED")) == [("2",)]                       # still applying (explicit opt-in)
    r = _hold(cid, "S.KEYED", integrity.KIND)
    assert r["state"] == integrity.SUSPECT and not r["held"] and r["body"]["policy"] == "continue"
    with get_engine().connect() as c:
        assert "SUSPECT" in (integrity.blocking(c, cid, "S.KEYED") or "")
    assert integrity.acknowledge(get_engine(), cid, "S.KEYED", "op", "verified by hand")
    with get_engine().connect() as c:
        assert integrity.blocking(c, cid, "S.KEYED") is None


def test_policy_rejects_unknown_on_quarantine() -> None:
    from alelt_loader import load_policy

    with pytest.raises(ValueError, match="on_quarantine"):
        load_policy("x", doc={"connection_id": "x", "on_quarantine": "ignore"})
    pol = load_policy("x", doc={"connection_id": "x", "schemas": {"S": {"tables": {"T": {"on_quarantine": "continue"}}}}})
    assert pol.quarantine_policy(TableRef(schema="S", name="T")) == "continue"
    assert pol.quarantine_policy(TableRef(schema="S", name="U")) == "hold_table"


# ------------------------------------------------------------------ F-08: holds from the start, one pass for many tables
def _window(closed: bool) -> dict[str, str]:
    now = datetime.now(timezone.utc)
    if closed:
        a = (now + timedelta(hours=2)).strftime("%H:%M")
        return {"start": a, "end": (now + timedelta(hours=3)).strftime("%H:%M"), "tz": "UTC"}
    return {"start": "00:00", "end": "00:00", "tz": "UTC"}


def test_daily_window_held_from_the_first_batch(env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from alelt_hub import schedule
    from alelt_hub.pipeline import StreamPipeline
    monkeypatch.setattr(StreamPipeline, "READ_CHUNK", 2)                    # a long initial catch-up in chunks
    cid, a = _conn(env, {"schemas": {"S": {"enabled": True, "tables": {
        "KEYED": {"schedule": {"class": "daily", "window": _window(closed=True)}}, "LOG": {}}}}})
    for i in range(1, 9):                                                  # backlog BEFORE the first decode job
        a.send([ev(3 * i, f"T{i}", "INSERT", "S.KEYED", after={"ID": i, "NAME": f"n{i}"}),
                ev(3 * i + 1, f"T{i}", "INSERT", "S.LOG", after={"MSG": f"m{i}", "N": i}), ev(3 * i + 2, f"T{i}", "COMMIT")])
    w = _worker("iq-w5")
    _drain(w)
    d = MEM[cid]
    assert len(d.live("LOG")) == 8 and not d.live("KEYED")                 # never applied outside its window
    assert not [x for x in d.applied if x[1] == "S.KEYED"]
    p = w.pipelines[cid]
    _set_doc(cid, {"schemas": {"S": {"enabled": True, "tables": {                     # window opens
        "KEYED": {"schedule": {"class": "daily", "window": _window(closed=False)}}, "LOG": {}}}}})
    p.refresh_policy()
    schedule.tick(p, p.held)
    _drain(w, ready=True)
    assert len(d.live("KEYED")) == 8 and "S.KEYED" not in p.held
    assert all(s.startswith(f"{cid}#pass:") for s, t, *_ in d.applied if t == "S.KEYED")   # released by a pass


def test_replay_pass_releases_160_tables_in_one_pass(env: Any) -> None:
    """169 daily tables in the first use case: one pass, decoded once, without the pipeline lock."""
    from alelt_hub import holds, replay, schedule
    from alelt_hub.db import get_engine, job
    from alelt_hub.dialect import try_lock, unlock

    n_tables, n_batches = 160, 120
    tabs = {f"T{i:03d}": {"schedule": {"latency_target_s": 3600}} for i in range(n_tables)}
    cid, a = _conn(env, {"schemas": {"M": {"enabled": True, "tables": tabs}, "S": {"enabled": True}}}, tables=n_tables)
    pos = 0
    for b in range(n_batches):
        evs = []
        for k in range(8):
            pos += 1
            t = (b * 8 + k) % n_tables
            evs.append(ev(pos, f"X{b}", "INSERT", f"M.T{t:03d}", after={"ID": b * 8 + k, "V": f"v{b}"}))
        pos += 1
        evs.append(ev(pos, f"X{b}", "COMMIT"))
        a.send(evs)
    w = _worker("iq-w6")
    _drain(w)
    assert len(holds.held_tables(get_engine(), cid)) == n_tables            # deferred tables held from the start
    d = MEM[cid]
    assert not [x for x in d.applied if x[1].startswith("M.")]
    p = w.pipelines[cid]
    for r in holds.rows(get_engine(), cid, schedule.KIND):                  # every round due now
        holds.modify(get_engine(), cid, r["table_fqn"], schedule.KIND, next_due="2000-01-01T00:00:00+00:00")
    raw0 = p.cache.misses
    schedule.tick(p, p.held)
    lock = try_lock(get_engine(), f"pipeline:{cid}")                        # main stream lock held by "someone"
    assert lock is not None
    try:
        t0 = time.monotonic()
        _drain(w)                                                           # replay job does not need the lock
        took = time.monotonic() - t0
    finally:
        unlock(lock, f"pipeline:{cid}")
    with get_engine().connect() as c:
        rj = [s for s, pl in c.execute(select(job.c.status, job.c.payload).where(job.c.kind == "replay"))
              if (pl or {}).get("connection_id") == cid]
    assert rj == ["DONE"]                                     # ONE pass for all 160 tables
    assert sum(len(d.live(f"T{i:03d}")) for i in range(n_tables)) == n_batches * 8
    assert p.cache.misses == raw0                                           # decoded once (cache), never again
    print(f"160-table replay job: {took:.2f}s")
    assert took < 60, took
    assert len(holds.held_tables(get_engine(), cid)) == n_tables            # deferred: still held, watermark moved
    r = _hold(cid, "M.T000", schedule.KIND)
    assert r["start_pos"] == replay.mirror(p)[1] and r["body"]["last_round"]["events"] > 0


def test_restart_mid_replay_applies_exactly_once(env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from alelt_hub import holds, schedule
    from alelt_hub.db import get_engine

    monkeypatch.setenv("ALELT_REPLAY_FLUSH_ROWS", "4")                         # intermediate flushes inside the pass
    cid, a = _conn(env, {"schemas": {"S": {"enabled": True, "tables": {
        "KEYED": {"schedule": {"latency_target_s": 3600}}}}}})
    for i in range(1, 11):
        a.send([ev(2 * i, f"T{i}", "INSERT", "S.KEYED", after={"ID": i, "NAME": f"n{i}"}), ev(2 * i + 1, f"T{i}", "COMMIT")])
    w = _worker("iq-w7")
    _drain(w)
    d = MEM[cid]
    holds.modify(get_engine(), cid, "S.KEYED", schedule.KIND, next_due="2000-01-01T00:00:00+00:00")
    p = w.pipelines[cid]
    schedule.tick(p, p.held)
    d.calls, d.fail_after = 0, 2                                             # crash in the 2nd destination apply
    _drain(w)
    got = len(d.live("KEYED"))
    assert 0 < got < 10                                                      # partially applied, then crashed
    d.fail_after = None
    w2 = _worker("iq-w7b")                                                   # fresh worker: resumes the pass
    w2.sweep()
    _drain(w2, ready=True)
    assert len(d.live("KEYED")) == 10
    keyed = [(k, key) for s, t, k, key in d.applied if t == "S.KEYED"]
    assert len(keyed) == len(set(keyed)) == 10, keyed                       # every change applied exactly once                              # every change applied exactly once
    assert not _hold(cid, "S.KEYED", schedule.KIND)["body"].get("pass")


def test_long_transaction_older_than_lookback(env: Any) -> None:
    """HC-09: the replay scan starts at the main stream's restart_seq recorded with the hold, not N batches back."""
    from alelt_hub import holds, schedule
    from alelt_hub.db import get_engine
    from alelt_loader.policy import TableSchedule

    cid, a = _conn(env)
    w = _worker("iq-w8", catchup_lookback=1)
    a.send([ev(1, "TL", "INSERT", "S.KEYED", after={"ID": 9, "NAME": "early"}),   # long txn opens
            ev(2, "TA", "INSERT", "S.LOG", after={"MSG": "a", "N": 1}), ev(3, "TA", "COMMIT")])
    _drain(w)
    for i in range(4):                                                          # several flushed batches later
        a.send([ev(10 + 2 * i, f"TB{i}", "INSERT", "S.LOG", after={"MSG": f"b{i}", "N": i}),
                ev(11 + 2 * i, f"TB{i}", "COMMIT")])
        _drain(w)
    p = w.pipelines[cid]
    p.policy.tables["S.KEYED"] = dataclasses.replace(p.policy.table(TableRef(schema="S", name="KEYED")),
                                                     schedule=TableSchedule(None, 3600, None))
    schedule.ensure(p, p.held)                                                  # held now; TL still open
    a.send([ev(30, "TL", "INSERT", "S.KEYED", after={"ID": 10, "NAME": "late"}), ev(31, "TL", "COMMIT")])
    _drain(w)
    assert not MEM[cid].live("KEYED")
    holds.modify(get_engine(), cid, "S.KEYED", schedule.KIND, next_due="2000-01-01T00:00:00+00:00")
    schedule.tick(p, p.held)
    _drain(w, ready=True)
    assert sorted(MEM[cid].live("KEYED")) == [("10",), ("9",)]                  # the early INSERT was not lost


# ------------------------------------------------------------------ HC-02 decoded cache invalidation
def test_decoded_cache_key_follows_the_decoder() -> None:
    from alelt_hub.replay import DecodedCache, decoder_fingerprint

    class D1(AttrSource):
        decoder_version = "a"

    class D2(AttrSource):
        decoder_version = "b"
    assert decoder_fingerprint(D1()) != decoder_fingerprint(D2())
    b = {"id": uuid.uuid4()}
    assert DecodedCache("c", D1(), ENGINE).key(b, 1) != DecodedCache("c", D2(), ENGINE).key(b, 1)


# ------------------------------------------------------------------ HC-03 edge snapshot: out-of-order chunks
def test_edge_snapshot_closes_only_when_every_chunk_loaded(env: Any) -> None:
    from alelt_common.models import SnapshotChunkHeader
    from alelt_common.util import compress
    from alelt_hub.db import get_engine, job
    from alelt_hub.snapshots import SnapshotService

    cid, a = _conn(env)
    w = _worker("iq-w9")
    a.send([ev(1, "A", "INSERT", "S.LOG", after={"MSG": "m", "N": 1}), ev(2, "A", "COMMIT")])
    _drain(w)
    assert SnapshotService(get_engine(), w.pipeline(cid)).start(["S.KEYED"]) == ["S.KEYED"]
    _drain(w)
    snap = next(c for c in env.get(f"/agent/v1/commands?agent_id={a.agent_id}&wait=0", headers=a.h).json()
                if c["name"] == "SNAPSHOT_TABLE")

    def post(n: int, rows: list[dict[str, Any]], last: bool) -> None:
        body = b"".join(json.dumps(r).encode() + b"\n" for r in rows)
        hdr = SnapshotChunkHeader(snapshot_id=uuid.UUID(snap["args"]["snapshot_id"]),
                                  table=TableRef(schema="S", name="KEYED"), chunk_no=n, rows=len(rows), last=last,
                                  codec="zlib")
        assert env.post("/agent/v1/snapshot", headers={**a.h, "X-ALELT-Snapshot": hdr.model_dump_json(by_alias=True),
                                                       "X-ALELT-Agent-Id": a.agent_id},
                        content=compress(body, "zlib")).status_code == 200
    post(1, [{"ID": 2, "NAME": "two"}], True)                                 # last chunk loads first
    _drain(w)
    assert _hold(cid, "S.KEYED", "snapshot")["state"] == "SNAPSHOTTING"       # not closed: chunk 0 missing
    post(0, [{"ID": 1, "NAME": "one"}], False)
    _drain(w)
    h = _hold(cid, "S.KEYED", "snapshot")
    assert h["state"] == "CATCHING_UP" and h["body"]["rows_loaded"] == 2, h
    with get_engine().connect() as c:
        assert not [1 for st, pl in c.execute(select(job.c.status, job.c.payload))
                    if st == "FAILED" and (pl or {}).get("connection_id") == cid]


# ------------------------------------------------------------------ HC-04 deferred flush with latency > 0
def test_latency_flush_follow_up_job_is_never_lost(env: Any) -> None:
    cid, a = _conn(env)
    w = _worker("iq-w10", load_max_latency_s=2.0)
    a.send([ev(1, "A", "INSERT", "S.LOG", after={"MSG": "late", "N": 1}), ev(2, "A", "COMMIT")])
    _drain(w)
    d = MEM[cid]
    assert not d.live("LOG")                                                  # latency window not reached
    deadline = time.monotonic() + 15
    while not d.live("LOG") and time.monotonic() < deadline:
        time.sleep(0.5)
        _drain(w)                                                            # only due jobs: real follow-ups
    assert d.live("LOG"), "trailing buffer never flushed (follow-up job lost)"


# ------------------------------------------------------------------ X-132 / ADR-013: out-of-row signal = completion
def test_keyed_reread_signal_is_completion_work_not_integrity(env: Any) -> None:
    """A needs_keyed_reread signal WITH keys: the partial image is applied at once, the key is queued for completion,
    no hold, no integrity row, no quarantine record (alelt_hub.completion; the fetch itself: test_completion.py)."""
    from sqlalchemy import select as sel

    from alelt_hub import integrity
    from alelt_hub.db import get_engine, quarantine_event, table_completion

    cid, a = _conn(env)
    w = _worker("iq-w11")
    a.send([ev(1, "T1", "INSERT", "S.KEYED", after={"ID": 4, "NAME": "four"}),
            ev(2, "T1", "UPDATE", "S.KEYED", after={"ID": 5, "SIG": 1}),          # out-of-row: signal with key
            ev(3, "T1", "INSERT", "S.LOG", after={"MSG": "m", "N": 1}), ev(4, "T1", "COMMIT")])
    _drain(w)
    d = MEM[cid]
    assert _hold(cid, "S.KEYED", integrity.KIND) is None
    assert ("4",) in d.live("KEYED") and d.live("LOG")
    with get_engine().connect() as c:
        ent = c.execute(sel(table_completion.c.table_fqn, table_completion.c.key, table_completion.c.last_pos)
                        .where(table_completion.c.connection_id == cid)).all()
        q = c.execute(sel(quarantine_event.c.reason).where(quarantine_event.c.connection_id == cid)).all()
    assert [(e[0], e[1]) for e in ent] == [("S.KEYED", {"ID": 5})] and ent[0][2] == lri(4)   # commit position
    assert q == []


# ------------------------------------------------------------------ lake destination maintain() scheduling
def test_destination_maintenance_scheduled_and_never_during_apply(env: Any) -> None:
    from alelt_hub import holds, maintenance
    from alelt_hub.db import get_engine, job
    from alelt_hub.dialect import try_lock, unlock

    cid, a = _conn(env, {"maintenance": {"every_s": 3600}})
    w = _worker("iq-w12")
    a.send([ev(1, "T1", "INSERT", "S.KEYED", after={"ID": 1, "NAME": "x"}),
            ev(2, "T1", "INSERT", "S.LOG", after={"MSG": "m", "N": 1}), ev(3, "T1", "COMMIT")])
    _drain(w)
    d = MEM[cid]
    calls: list[list[str]] = []
    d.maintain = lambda names: (calls.append(list(names)), {n: "ok" for n in names})[1]  # type: ignore[attr-defined]
    holds.put(get_engine(), cid, "S.LOG", "snapshot", "SNAPSHOTTING", True, None, {})      # being snapshotted
    lock = try_lock(get_engine(), f"pipeline:{cid}")                                       # an apply in progress
    try:
        assert maintenance.schedule(w) == [cid]
        _drain(w)
        assert calls == []                                                                  # waited, re-queued
    finally:
        unlock(lock, f"pipeline:{cid}")
    _drain(w, ready=True)
    assert calls == [["KEYED"]]                                                             # LOG left out
    assert maintenance.last_run(get_engine(), cid) is not None
    assert maintenance.schedule(w) == []                                                    # cadence respected
    with get_engine().connect() as c:
        assert not [1 for st, pl in c.execute(select(job.c.status, job.c.payload))
                    if st == "FAILED" and (pl or {}).get("connection_id") == cid]


def test_destination_maintenance_respects_the_window(env: Any) -> None:
    from alelt_hub import maintenance

    cid, a = _conn(env, {"maintenance": {"window": _window(closed=True)}})
    w = _worker("iq-w13")
    a.send([ev(1, "T1", "INSERT", "S.LOG", after={"MSG": "m", "N": 1}), ev(2, "T1", "COMMIT")])
    _drain(w)
    MEM[cid].maintain = lambda names: {}  # type: ignore[attr-defined]
    assert maintenance.schedule(w) == []                                                    # outside off-peak


# ------------------------------------------------------------------ HC-10: schema drift on the data path
def _schema_events(cid: str) -> list[dict[str, Any]]:
    from alelt_hub.db import get_engine, schema_change_event

    with get_engine().connect() as c:
        return [dict(r) for r in c.execute(select(schema_change_event)).mappings()
                if (r["detail"] or {}).get("connection_id") == cid]


def test_schema_drift_added_column_applied_and_view_regenerated(env: Any) -> None:
    cid, a = _conn(env, {"view_schema": "RAWV"})
    w = _worker("iq-w14")
    a.send([ev(1, "T1", "INSERT", "S.KEYED", after={"ID": 1, "NAME": "a"}), ev(2, "T1", "COMMIT")])
    _drain(w)
    a.catalog(_catalog(cid, extra=True, at=2))                            # ALTER ADD COLUMN at the source
    a.send([ev(4, "T2", "INSERT", "S.KEYED", after={"ID": 2, "NAME": "b", "NEWCOL": "n"}), ev(5, "T2", "COMMIT")])
    _drain(w)
    d = MEM[cid]
    assert ("add_column", "KEYED", "NEWCOL") in d.ddl
    assert any(k == "view" and t == "RAWV.KEYED" and "NEWCOL" in cols for k, t, cols in d.ddl)
    assert d.live("KEYED")[("2",)]["NEWCOL"] == "n"                      # applied, nothing held
    assert [(e["kind"], e["status"]) for e in _schema_events(cid)] == [("COLUMN_ADDED", "APPLIED")]


def test_schema_drift_alter_holds_table_until_approved(env: Any) -> None:
    from alelt_hub import holds

    cid, a = _conn(env)
    w = _worker("iq-w15")
    a.send([ev(1, "T1", "INSERT", "S.KEYED", after={"ID": 1, "NAME": "a"}), ev(2, "T1", "COMMIT")])
    _drain(w)
    a.catalog(_catalog(cid, at=2, name_len=2000))                         # ALTER ... SET DATA TYPE VARCHAR(2000)
    a.send([ev(4, "T2", "UPDATE", "S.KEYED", before={"ID": 1}, after={"ID": 1, "NAME": "x" * 500}),
            ev(5, "T2", "INSERT", "S.LOG", after={"MSG": "flows", "N": 1}), ev(6, "T2", "COMMIT")])
    _drain(w)
    d = MEM[cid]
    assert [r["MSG"] for r in d.live("LOG").values()] == ["flows"]       # the stream does not stall
    assert d.live("KEYED")[("1",)]["NAME"] == "a"                         # held before the wide value
    assert holds.get(w.engine, cid, "S.KEYED", "schema")["held"]
    ev_ = _schema_events(cid)
    assert [(e["kind"], e["status"]) for e in ev_] == [("COLUMN_ALTERED", "PENDING")]
    r = env.post(f"/api/v1/schema-changes/{ev_[0]['id']}/decision", headers={"X-ALELT-Dev-Role": "DataOperator",
                 "X-ALELT-Dev-User": "op"}, json={"decision": "APPROVE", "reason": "widen"})
    assert r.status_code == 200, r.text
    _drain(w, ready=True)
    assert any(k == "ensure" and t == "KEYED" and "NAME:2000" in cols for k, t, cols in d.ddl)   # applied
    assert d.live("KEYED")[("1",)]["NAME"] == "x" * 500                   # replayed after approval
    assert [e["status"] for e in _schema_events(cid)] == ["APPLIED"]
    assert not holds.get(w.engine, cid, "S.KEYED", "schema")["held"]


def test_schema_drift_block_all_rejected_column_is_disabled(env: Any) -> None:
    from alelt_hub import holds

    cid, a = _conn(env, {"schema_change_handling": "BLOCK_ALL", "schemas": {"S": {"enabled": True, "tables": {
        "KEYED": {"enabled": True}, "LOG": {"enabled": True}}}}})
    w = _worker("iq-w16")
    a.send([ev(1, "T1", "INSERT", "S.KEYED", after={"ID": 1, "NAME": "a"}), ev(2, "T1", "COMMIT")])
    _drain(w)
    a.catalog(_catalog(cid, extra=True, at=2))
    a.send([ev(4, "T2", "INSERT", "S.KEYED", after={"ID": 2, "NAME": "b", "NEWCOL": "n"}), ev(5, "T2", "COMMIT")])
    _drain(w)
    d = MEM[cid]
    assert ("2",) not in d.live("KEYED") and not [x for x in d.ddl if x[0] == "add_column"]
    sid = _schema_events(cid)[0]["id"]
    r = env.post(f"/api/v1/schema-changes/{sid}/decision", headers={"X-ALELT-Dev-Role": "DataOperator",
                 "X-ALELT-Dev-User": "op"}, json={"decision": "REJECT", "reason": "not wanted"})
    assert r.status_code == 200, r.text
    _drain(w, ready=True)
    row = d.live("KEYED")[("2",)]
    assert row["NAME"] == "b" and "NEWCOL" not in row                     # column disabled, row applied
    assert not holds.get(w.engine, cid, "S.KEYED", "schema")["held"]


# ------------------------------------------------------------------ DST-loader-01 (hub side): failed DDL holds one table
def test_destination_ddl_failure_holds_only_that_table_then_rejoins(env: Any) -> None:
    from alelt_hub import ddl_hold

    cid, a = _conn(env)
    w = _worker("iq-ddl")
    d = MEM.setdefault(cid, MemDest())
    real = d.ensure_table
    broken = {"KEYED"}

    def ensure(table: TableInfo, dest_name: str, golden_ddl: str | None) -> None:
        if dest_name in broken:
            raise RuntimeError("Row size too large")
        real(table, dest_name, golden_ddl)

    d.ensure_table = ensure  # type: ignore[method-assign]
    a.send([ev(1, "T1", "INSERT", "S.KEYED", after={"ID": 1, "NAME": "one"}),
            ev(2, "T1", "INSERT", "S.LOG", after={"MSG": "m1", "N": 1}), ev(3, "T1", "COMMIT")])
    a.send([ev(4, "T2", "INSERT", "S.KEYED", after={"ID": 2, "NAME": "two"}),
            ev(5, "T2", "INSERT", "S.LOG", after={"MSG": "m2", "N": 2}), ev(6, "T2", "COMMIT")])
    _drain(w)
    assert sorted(r["MSG"] for r in d.live("LOG").values()) == ["m1", "m2"]     # the other table keeps flowing
    assert not d.live("KEYED")
    r = _hold(cid, "S.KEYED", ddl_hold.KIND)
    assert r is not None and r["held"] and r["state"] == ddl_hold.STATE and "Row size" in r["body"]["reason"]
    from alelt_hub import holds
    from alelt_hub.db import get_engine

    def due() -> None:
        holds.modify(get_engine(), cid, "S.KEYED", ddl_hold.KIND, retry_at="2000-01-01T00:00:00+00:00")
        from alelt_hub import jobs                                # = the backoff timer job firing

        with get_engine().begin() as c:
            jobs.enqueue(c, "decode", {"connection_id": cid}, dedup_key=f"t:{uuid.uuid4().hex}")

    due()
    _drain(w, ready=True)                                     # retry due, still failing: stays held, backoff grows
    r = _hold(cid, "S.KEYED", ddl_hold.KIND)
    assert r["held"] and int(r["body"]["attempts"]) >= 2
    broken.clear()                                             # operator fixed the destination
    due()
    _drain(w, ready=True, rounds=4)
    r = _hold(cid, "S.KEYED", ddl_hold.KIND)
    assert not r["held"], (r["body"], r["state"])
    assert sorted(k for k in d.live("KEYED")) == [("1",), ("2",)]
    a.send([ev(7, "T3", "INSERT", "S.KEYED", after={"ID": 3, "NAME": "three"}), ev(8, "T3", "COMMIT")])
    _drain(w)
    assert ("3",) in d.live("KEYED")
    per: dict[tuple[str, str], int] = {}
    for _s, t, kind, key in d.applied:
        if t == "S.KEYED":
            per[(kind, key)] = per.get((kind, key), 0) + 1
    assert per == {("INSERT", "('1',)"): 1, ("INSERT", "('2',)"): 1, ("INSERT", "('3',)"): 1}, per


# ------------------------------------------------------------------ XC-30: source policy revision per batch
def test_replays_reuse_the_source_policy_of_the_first_decode(env: Any) -> None:
    from alelt_hub.db import batch, get_engine, hub_setting
    from alelt_hub.dialect import insert

    def set_policy(doc: dict[str, Any]) -> None:
        with get_engine().begin() as c:
            c.execute(insert(hub_setting).values(key="diag.source_policy", value=doc)
                      .on_conflict_do_update(index_elements=["key"], set_={"value": doc}))

    cid, a = _conn(env)
    a.catalog(_catalog(cid, name_len=3))
    skip = {"S.KEYED.NAME": {"OUT_OF_RANGE": {"option": "QUARANTINE_SKIP"}}}
    set_policy(skip)
    try:
        w = _worker("iq-pol")
        a.send([ev(1, "T1", "INSERT", "S.KEYED", after={"ID": 1, "NAME": "much too long"}),
                ev(2, "T1", "INSERT", "S.KEYED", after={"ID": 2, "NAME": "ok"}), ev(3, "T1", "COMMIT")])
        _drain(w)
        d = MEM[cid]
        assert list(d.live("KEYED")) == [("2",)]                       # skipped by the policy of the first decode
        with get_engine().connect() as c:
            b = dict(c.execute(select(batch).where(batch.c.connection_id == cid)).mappings().one())
        assert b["decoded_policy_rev"]
        set_policy({"S.KEYED.NAME": {"OUT_OF_RANGE": {"option": "ACCEPT_AS_IS"}}})   # changed afterwards
        p = w.pipelines[cid]
        p.source_policy._at = 0
        res = p.cache.decode(b, p._catalog(p.catalog_vid_for(b)), p.catalog_vid_for(b))
        again, skipped = p._apply_source_policy(res.events, p.tables, b)       # replay / rebuild path
        assert [e.after["ID"] for e in again if e.after] == [2] and len(skipped) == 1
        now, skipped_now = p._apply_source_policy(res.events, p.tables)        # first decode / reprocess path
        assert sorted(e.after["ID"] for e in now if e.after) == [1, 2] and not skipped_now
    finally:
        with get_engine().begin() as c:
            c.execute(hub_setting.delete().where(hub_setting.c.key.like("diag.source_policy%")))


# ------------------------------------------------------------------ HC-19: synchronous edge snapshot hold
def test_edge_snapshot_hold_starts_at_flushed_position_and_holds_until_catch_up(env: Any) -> None:
    from alelt_common.models import SnapshotChunkHeader
    from alelt_common.util import compress
    from alelt_hub.db import get_engine
    from alelt_hub.snapshots import SnapshotService

    cid, a = _conn(env)
    w = _worker("iq-hc19")
    a.send([ev(1, "A", "INSERT", "S.KEYED", after={"ID": 1, "NAME": "one"}), ev(2, "A", "COMMIT")])
    _drain(w)                                                     # flushed through lri(2)
    a.send([ev(3, "B", "UPDATE", "S.KEYED", after={"ID": 1, "NAME": "uno"}), ev(4, "B", "COMMIT")])  # received only
    assert SnapshotService(get_engine(), w.pipeline(cid)).start(["S.KEYED"]) == ["S.KEYED"]
    h = _hold(cid, "S.KEYED", "snapshot")
    assert h["held"] and h["start_pos"] == lri(2), h            # flushed position, not the received batch
    _drain(w)
    d = MEM[cid]
    assert d.live("KEYED")[("1",)]["NAME"] == "one"               # held: the main stream did not apply batch B
    snap = next(c for c in env.get(f"/agent/v1/commands?agent_id={a.agent_id}&wait=0", headers=a.h).json()
                if c["name"] == "SNAPSHOT_TABLE")
    hdr = SnapshotChunkHeader(snapshot_id=uuid.UUID(snap["args"]["snapshot_id"]), table=TableRef(schema="S", name="KEYED"),
                              chunk_no=0, rows=1, last=True, codec="zlib")
    assert env.post("/agent/v1/snapshot", headers={**a.h, "X-ALELT-Snapshot": hdr.model_dump_json(by_alias=True),
                                                   "X-ALELT-Agent-Id": a.agent_id},
                    content=compress(json.dumps({"ID": 1, "NAME": "one"}).encode() + b"\n", "zlib")).status_code == 200

    w.run_once()                                                  # snapshot_load job only
    h = _hold(cid, "S.KEYED", "snapshot")
    assert h["state"] == "CATCHING_UP" and (h["held"] or h["body"].get("catchup_done")), h
    _drain(w, ready=True)
    h = _hold(cid, "S.KEYED", "snapshot")
    assert h["body"].get("catchup_done") and not h["held"], h      # caught up under the lock, then released
    assert d.live("KEYED")[("1",)]["NAME"] == "uno"               # B applied by the catch-up
    a.send([ev(5, "C", "INSERT", "S.KEYED", after={"ID": 2, "NAME": "two"}), ev(6, "C", "COMMIT")])
    _drain(w)
    assert ("2",) in d.live("KEYED")                               # main stream applies the table again
    ups = [x for x in d.applied if x[1] == "S.KEYED" and x[2] == "UPDATE"]
    assert len(ups) == 1, d.applied                                # exactly once (catch-up, never the main stream)


# ------------------------------------------------------------------ F-22: shared flush cadence
def test_shared_flush_cadence_one_destination_txn_for_all_tables(env: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALELT_FLUSH_CADENCE_S", "900")             # the connection group accepts 15 min
    cid, a = _conn(env, doc={"schemas": {"S": {"enabled": True}}})
    w = _worker("iq-f22")
    for i in range(3):                                         # three batches, two tables, inside one cadence
        a.send([ev(10 * i + 1, f"T{i}", "INSERT", "S.KEYED", after={"ID": i, "NAME": f"n{i}"}),
                ev(10 * i + 2, f"T{i}", "INSERT", "S.LOG", after={"MSG": f"m{i}", "N": i}),
                ev(10 * i + 3, f"T{i}", "COMMIT")])
        _drain(w)
    d = MEM.setdefault(cid, MemDest())
    p = w.pipeline(cid)
    assert p.loader.max_latency_s == 900                       # the group accepts 15 min: one txn per cadence
    assert d.calls == 0 and p.loader.pending()
    p.loader.buf.since -= 901                                  # cadence elapsed
    _drain(w, ready=True)
    assert d.calls == 1                                        # ONE destination transaction for both tables
    assert len(d.live("KEYED")) == 3 and len(d.live("LOG")) == 3
    _set_doc(cid, {"schemas": {"S": {"enabled": True, "tables": {"LOG": {"schedule": {"latency_target_s": 60}}}}}})
    p.refresh_policy()
    assert p.loader.max_latency_s == 60                        # a table with a tighter target bounds the cadence
