"""Test doubles: a JSON-lines SourceConnector stub and a fake edge agent (protocol v1 over httpx).

Raw format of FakeSource: one JSON object per line:
  {"pos": "1:000000000000000A", "txid": "T1", "kind": "INSERT", "table": "S.T", "before": {...}, "after": {...}}
  optional "note": a source completeness signal (`alelt-signal:...`, kind OTHER), e.g. a needs_keyed_reread
Lines that do not parse are quarantined (R7).
"""
from __future__ import annotations

import json
import socket
import uuid
from datetime import datetime, timezone
from typing import Any

from alelt_common.models import (
    BatchHeader,
    CanonicalType,
    CatalogSnapshot,
    ChangeEvent,
    ChangeKind,
    ColumnInfo,
    Position,
    QuarantineRecord,
    RawRef,
    TableRef,
)
from alelt_common.spi import DecodeResult, SourceConnector
from alelt_common.util import compress, sha256_hex


def lri(n: int, part1: int = 1) -> str:
    return f"{part1}:{n:016X}"


class FakeSource(SourceConnector):
    engine = "fake"
    capture_mode = "edge"

    def parse_position(self, text: str) -> Position:
        return Position(engine=self.engine, text=text)

    def compare(self, a: Position, b: Position) -> int:
        def k(p: Position) -> tuple[int, int]:
            x, _, y = p.text.partition(":")
            return int(x), int(y, 16)
        ka, kb = k(a), k(b)
        return (ka > kb) - (ka < kb)

    def canonical_type(self, col: ColumnInfo) -> CanonicalType:
        return col.canonical_type

    def decode(self, raw: bytes, catalog: CatalogSnapshot, batch_id: Any) -> DecodeResult:
        events: list[ChangeEvent] = []
        quarantined: list[QuarantineRecord] = []
        off = 0
        bid = uuid.UUID(str(batch_id))
        for line in raw.splitlines(keepends=True):
            ref = RawRef(batch_id=bid, offset=off, length=len(line))
            off += len(line)
            if not line.strip():
                continue
            try:
                d = json.loads(line)
                s, _, n = (d.get("table") or ".").partition(".")
                events.append(ChangeEvent(
                    position=Position(engine=self.engine, text=d["pos"]), txid=d["txid"], kind=ChangeKind(d["kind"]),
                    table=TableRef(schema=s, name=n) if n else None, before=d.get("before"), after=d.get("after"),
                    commit_ts=datetime.now(timezone.utc) if d["kind"] == "COMMIT" else None, raw_ref=ref,
                    note=d.get("note")))
            except Exception as e:  # noqa: BLE001
                quarantined.append(QuarantineRecord(position=None, table=None, reason=f"undecodable: {e}",
                                                    raw_ref=ref, raw_hex=line[:32].hex()))
        return DecodeResult(events=events, quarantined=quarantined)


def fake_catalog(connection_id: str, engine: str = "fake") -> CatalogSnapshot:
    """Two tables: S.KEYED (ID INT PK, NAME STRING, AMT DECIMAL(10,2)) and S.LOG (keyless)."""
    from alelt_common.models import TableInfo

    def col(name: str, i: int, t: CanonicalType, key: int | None = None, ln: int | None = None,
            sc: int | None = None) -> ColumnInfo:
        return ColumnInfo(name=name, ordinal=i, engine_type=t.value, canonical_type=t, length=ln, scale=sc,
                          nullable=key is None, key_seq=key)
    return CatalogSnapshot(
        connection_id=connection_id, engine=engine, taken_at=datetime.now(timezone.utc),
        position=Position(engine=engine, text=lri(0)),
        tables=[
            TableInfo(table=TableRef(schema="S", name="KEYED"), capture_enabled=True, columns=[
                col("ID", 0, CanonicalType.INT, key=1), col("NAME", 1, CanonicalType.STRING, ln=40),
                col("AMT", 2, CanonicalType.DECIMAL, ln=10, sc=2), col("SECRET", 3, CanonicalType.STRING, ln=40)]),
            TableInfo(table=TableRef(schema="S", name="LOG"), capture_enabled=True, columns=[
                col("MSG", 0, CanonicalType.STRING, ln=100), col("N", 1, CanonicalType.INT)]),
        ])


def ev(pos: int, txid: str, kind: str, table: str | None = None, before: dict[str, Any] | None = None,
       after: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"pos": lri(pos), "txid": txid, "kind": kind, "table": table, "before": before, "after": after}


class FakeAgent:
    """Speaks protocol v1 against a hub (httpx.Client or FastAPI TestClient)."""

    def __init__(self, client: Any, token: str, agent_id: str = "fake-agent", connection_id: str = "fakeconn",
                 engine: str = "fake") -> None:
        self.c = client
        self.agent_id, self.connection_id, self.engine = agent_id, connection_id, engine
        self.h = {"X-ALELT-Protocol": "1", "X-ALELT-Agent-Token": token}
        self.next_from = lri(0)

    def register(self) -> dict[str, Any]:
        r = self.c.post("/agent/v1/register", headers=self.h, json={
            "agent_id": self.agent_id, "connection_id": self.connection_id, "engine": self.engine,
            "version": "test", "hostname": socket.gethostname(), "database": "FAKE"})
        r.raise_for_status()
        return r.json()  # type: ignore[no-any-return]

    def catalog(self, snap: CatalogSnapshot) -> dict[str, Any]:
        r = self.c.post("/agent/v1/catalog", headers={**self.h, "Content-Type": "application/json"},
                        content=snap.model_dump_json(by_alias=True))
        r.raise_for_status()
        return r.json()  # type: ignore[no-any-return]

    def make_batch(self, events: list[dict[str, Any]], codec: str = "zlib",
                   from_pos: str | None = None) -> tuple[BatchHeader, bytes]:
        raw = b"".join(json.dumps(e).encode() + b"\n" for e in events)
        last = events[-1]["pos"]
        hdr = BatchHeader(batch_id=uuid.uuid4(), connection_id=self.connection_id, agent_id=self.agent_id,
                          engine=self.engine, from_pos=Position(engine=self.engine, text=from_pos or self.next_from),
                          to_pos=Position(engine=self.engine, text=last),
                          next_pos=Position(engine=self.engine, text=last), rec_count=len(events),
                          raw_bytes=len(raw), sha256=sha256_hex(raw), codec=codec)  # type: ignore[arg-type]
        return hdr, compress(raw, codec)

    def post(self, hdr: BatchHeader, body: bytes) -> Any:
        r = self.c.post("/agent/v1/blocks", headers={**self.h, "X-ALELT-Batch": hdr.model_dump_json()}, content=body)
        if r.status_code == 200 and hdr.next_pos:
            self.next_from = hdr.next_pos.text
        return r

    def send(self, events: list[dict[str, Any]]) -> Any:
        return self.post(*self.make_batch(events))


# ------------------------------------------------------------------ in-process edge rig (lifecycle / completion tests)
def sim_catalog(connection_id: str, n: int, lob_every: int = 6, engine: str = "fake",
                schema: str = "SIM") -> CatalogSnapshot:
    """`n` tables SIM.T000.. (ID INT PK, NAME, AMT DECIMAL); every `lob_every`-th table also has an out-of-row DOC
    column (CLOB-like: its value never travels in the log, a change of it is a needs_keyed_reread signal)."""
    from alelt_common.models import TableInfo

    def col(name: str, i: int, t: CanonicalType, et: str, key: int | None = None, ln: int | None = None,
            sc: int | None = None) -> ColumnInfo:
        return ColumnInfo(name=name, ordinal=i, engine_type=et, canonical_type=t, length=ln, scale=sc,
                          nullable=key is None, key_seq=key)
    tabs = []
    for i in range(n):
        cols = [col("ID", 0, CanonicalType.INT, "INTEGER", key=1),
                col("NAME", 1, CanonicalType.STRING, "VARCHAR", ln=40),
                col("AMT", 2, CanonicalType.DECIMAL, "DECIMAL", ln=10, sc=2)]
        if lob_every and i % lob_every == 0:
            cols.append(col("DOC", 3, CanonicalType.STRING, "CLOB"))
        tabs.append(TableInfo(table=TableRef(schema=schema, name=f"T{i:03d}"), capture_enabled=True, columns=cols))
    return CatalogSnapshot(connection_id=connection_id, engine=engine, taken_at=datetime.now(timezone.utc),
                           position=Position(engine=engine, text=lri(0)), tables=tabs)


class _Refused(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class SimEdge:
    """Source truth + the agent side of a FakeAgent: ships change batches (out-of-row DOC values as needs_keyed_reread
    signals, never in the log), answers SNAPSHOT_TABLE (text chunks, resumable with `key_from`, `last_key` in the
    header) and VERIFY_TABLE (rows / aggregates, honouring `columns`) from the truth, in a thread.

    Knobs for failure tests: `frozen` = (read position, truth) used by reads (a read that happened earlier);
    `delay_s` before answering; `refuse` = {command name: error code} answered ok:false; `paused` = answer nothing;
    `max_bytes` = VERIFY_TABLE result cap (truncation); `heartbeat` = heartbeat every N s (0 = never);
    `chunk_fail_after` = SNAPSHOT_TABLE stops (no ack) after N chunks once (crash simulation)."""

    def __init__(self, client: Any, agent: FakeAgent, cat: CatalogSnapshot) -> None:
        import threading

        self.c, self.a = client, agent
        self.infos = {t.table.fqn: t for t in cat.tables}
        self.truth: dict[str, dict[int, dict[str, Any]]] = {f: {} for f in self.infos}
        self.pos = 0
        self.lock = threading.RLock()
        self.stop = False
        self.seen: dict[str, int] = {}
        self.args: list[tuple[str, dict[str, Any]]] = []
        self.frozen: tuple[int, dict[str, dict[int, dict[str, Any]]]] | None = None
        self.delay_s = 0.0
        self.refuse: dict[str, str] = {}
        self.paused = False
        self.max_bytes: int | None = None
        self.heartbeat = 1.0                       # heartbeats (with the read position) every second
        self.chunk_fail_after: int | None = None
        self.oversize_ids: set[int] = set()        # VERIFY_TABLE: these rows alone exceed the result cap
        self.t = threading.Thread(target=self._run, daemon=True)

    def lob(self, fqn: str) -> bool:
        return any(c.name == "DOC" for c in self.infos[fqn].columns)

    # ---------------------------------------------------------------- source changes
    def seed(self, fqn: str, rid: int, name: str, doc: str | None = None) -> None:
        """A row that exists before capture starts (never in the log)."""
        with self.lock:
            self.truth[fqn][rid] = self._row(fqn, rid, name, doc)

    def _row(self, fqn: str, rid: int, name: str, doc: str | None) -> dict[str, Any]:
        r: dict[str, Any] = {"ID": rid, "NAME": name, "AMT": f"{rid}.50"}
        if self.lob(fqn):
            r["DOC"] = doc
        return r

    def tx(self, ops: list[tuple[str, str, int, str | None, str | None]], rollback: bool = False) -> None:
        """ops: (kind, fqn, id, name, doc). DOC never travels in the log: a change of it is a signal."""
        import copy

        from alelt_common import completeness as fc

        with self.lock:
            evs: list[dict[str, Any]] = []
            txid = uuid.uuid4().hex[:8]
            saved = copy.deepcopy(self.truth) if rollback else None
            for kind, fqn, rid, name, doc in ops:
                self.pos += 1
                t = self.truth[fqn]
                if kind == "DELETE":
                    t.pop(rid, None)
                    evs.append({"pos": lri(self.pos), "txid": txid, "kind": "DELETE", "table": fqn,
                                "before": {"ID": rid}, "after": None})
                    continue
                old = t.get(rid)
                row = self._row(fqn, rid, name or (old or {}).get("NAME") or "",
                                doc if doc is not None else (old or {}).get("DOC"))
                t[rid] = row
                img = {k: v for k, v in row.items() if k != "DOC"}
                evs.append({"pos": lri(self.pos), "txid": txid, "kind": kind, "table": fqn,
                            "before": {"ID": rid} if kind == "UPDATE" else None, "after": img})
                if self.lob(fqn) and doc is not None:
                    self.pos += 1
                    s, _, n = fqn.partition(".")
                    sig = fc.keyed_reread(TableRef(schema=s, name=n), [{"ID": rid}],
                                          "value stored out of row (LOB): re-read the row by key", "fake:lob",
                                          columns=["DOC"])
                    evs.append({"pos": lri(self.pos), "txid": txid, "kind": "OTHER", "table": fqn,
                                "note": sig.to_note()})
            self.pos += 1
            evs.append({"pos": lri(self.pos), "txid": txid, "kind": "ROLLBACK" if rollback else "COMMIT"})
            if saved is not None:
                self.truth = saved
            r = self.a.send(evs)
            assert r.status_code == 200, r.text

    # ---------------------------------------------------------------- agent side
    def __enter__(self) -> "SimEdge":
        self.t.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.stop = True
        self.t.join(10)

    def _run(self) -> None:
        import time as _t

        last_hb = 0.0
        while not self.stop:
            if self.heartbeat and _t.monotonic() - last_hb > self.heartbeat:
                last_hb = _t.monotonic()
                try:
                    self.c.post("/agent/v1/heartbeat", headers=self.a.h, json={
                        "agent_id": self.a.agent_id, "state": "CAPTURING",
                        "last_read_pos": {"engine": "fake", "text": lri(self.pos)},
                        "at": datetime.now(timezone.utc).isoformat()})
                except Exception:  # noqa: BLE001
                    pass
            if self.paused:
                _t.sleep(0.05)
                continue
            try:
                r = self.c.get(f"/agent/v1/commands?agent_id={self.a.agent_id}&wait=0", headers=self.a.h)
                cmds = r.json() if r.status_code == 200 else []
            except Exception:  # noqa: BLE001 - test client busy: next round
                cmds = []
            for cmd in cmds:
                self.seen[cmd["name"]] = self.seen.get(cmd["name"], 0) + 1
                self.args.append((cmd["name"], cmd["args"]))
                if self.delay_s:
                    _t.sleep(self.delay_s)
                code = self.refuse.get(cmd["name"])
                try:
                    if code:
                        raise _Refused(code)
                    res = self._snapshot(cmd["args"]) if cmd["name"] == "SNAPSHOT_TABLE" else \
                        self._verify(cmd["args"]) if cmd["name"] == "VERIFY_TABLE" else {}
                    if res is None:
                        continue                      # crash simulation: no ack
                    body = {"ok": True, "result": res}
                except _Refused as e:
                    body = {"ok": False, "result": {"code": e.code, "message": f"refused ({e.code})"}}
                except Exception as e:  # noqa: BLE001
                    body = {"ok": False, "result": {"code": "SOURCE_ERROR", "message": f"{type(e).__name__}: {e}"}}
                self.c.post(f"/agent/v1/commands/{cmd['command_id']}/ack?agent_id={self.a.agent_id}",
                            headers=self.a.h, json=body)
            _t.sleep(0.02)

    def _snapshot(self, args: dict[str, Any]) -> dict[str, Any] | None:
        import zlib

        fqn = f"{args['table']['schema']}.{args['table']['name']}"
        kf = args.get("key_from")
        with self.lock:                                   # consistent read (CS): no change ships meanwhile
            start = lri(self.pos)
            rows = [{k: (None if v is None else str(v)) for k, v in r.items()}
                    for _, r in sorted(self.truth[fqn].items()) if not kf or int(r["ID"]) > int(kf[0])]
        size = int(args.get("chunk_rows") or 50_000)
        chunks = [rows[i:i + size] for i in range(0, len(rows), size)] or [[]]
        first = int(args.get("chunk_from") or 0)
        for n, ch in enumerate(chunks):
            if self.chunk_fail_after is not None and n >= self.chunk_fail_after:
                self.chunk_fail_after = None
                return None
            hdr = {"snapshot_id": args["snapshot_id"], "table": args["table"], "chunk_no": first + n,
                   "rows": len(ch), "last": n == len(chunks) - 1, "start_pos": {"engine": "fake", "text": start},
                   "codec": "zlib", "format": "jsonl", "key_from": kf,
                   "last_key": [ch[-1]["ID"]] if ch else kf}
            body = zlib.compress(b"".join(json.dumps(r).encode() + b"\n" for r in ch))
            self.c.post("/agent/v1/snapshot", headers={**self.a.h, "X-ALELT-Snapshot": json.dumps(hdr),
                                                       "X-ALELT-Agent-Id": self.a.agent_id}, content=body
                        ).raise_for_status()
        return {"snapshot_id": args["snapshot_id"], "chunks": len(chunks), "rows": len(rows), "start_pos": start}

    def _verify(self, raw: dict[str, Any]) -> dict[str, Any]:
        from decimal import Decimal

        from alelt_common import verify_edge as ve

        a = ve.validate_args(raw)
        fqn = f"{a['table']['schema']}.{a['table']['name']}"
        cls = {"ID": "int", "NAME": "str", "AMT": "dec", **({"DOC": "cnt"} if self.lob(fqn) else {})}
        with self.lock:
            at, truth = self.frozen if self.frozen is not None else (self.pos, self.truth)
            rows = [{**r, "AMT": Decimal(r["AMT"])} for _, r in sorted(truth[fqn].items())]
        kw: dict[str, Any] = {} if self.max_bytes is None else {"max_bytes": self.max_bytes}
        res = ve.compute_from_rows(a, rows, ["ID"], cls, lambda t: tuple(int(x) for x in t), "fake",
                                   lambda: lri(at), **kw)
        if self.oversize_ids and "rows" in a["checks"]:
            big = [r for r in res["rows"] if r.get("ID") is not None and int(r["ID"]) in self.oversize_ids]
            if big:                                   # the edge stops before a row that does not fit
                cut = res["rows"].index(big[0])
                res["rows"], res["truncated"] = res["rows"][:cut], True
        return res
