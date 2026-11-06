"""Test double: an in-memory REMOTE source (hub-side capture) with typed values, a change log and positions."""
from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from alelt_common.canonical_jsonl import decode_batch
from alelt_common.models import (
    CanonicalType,
    CatalogSnapshot,
    ChangeEvent,
    ChangeKind,
    ColumnInfo,
    Position,
    TableInfo,
    TableRef,
)
from alelt_common.spi import DecodeResult, SourceConnector, SourceGapError

REF = TableRef(schema="S", name="ITEMS")


class World:
    """Shared state of the fake source (one per test connection)."""

    def __init__(self) -> None:
        self.rows: dict[int, dict[str, Any]] = {}
        self.log: list[tuple[int, ChangeEvent]] = []      # (position number, event)
        self.pos = 0
        self.stream_from: list[str | None] = []
        self.password = "s3cret"
        self.gap = False

    def txn(self, ops: list[tuple[str, dict[str, Any]]]) -> None:
        tx = f"T{self.pos + 1}"
        for kind, row in ops:
            self.pos += 1
            before = self.rows.get(row["ID"])
            if kind == "DELETE":
                self.rows.pop(row["ID"], None)
                after = None
            else:
                self.rows[row["ID"]] = {**(before or {}), **row}
                after = dict(self.rows[row["ID"]])
            self.log.append((self.pos, ChangeEvent(position=Position(engine="fakeremote", text=p(self.pos)), txid=tx,
                                                   kind=ChangeKind(kind), table=REF, before=before, after=after)))
        self.pos += 1
        self.log.append((self.pos, ChangeEvent(position=Position(engine="fakeremote", text=p(self.pos)), txid=tx,
                                               kind=ChangeKind.COMMIT, commit_ts=datetime.now(timezone.utc))))


WORLDS: dict[str, World] = {}


def p(n: int) -> str:
    return f"p:{n:08d}"


def catalog(cid: str) -> CatalogSnapshot:
    def col(name: str, i: int, t: CanonicalType, key: int | None = None, ln: int | None = None,
            sc: int | None = None) -> ColumnInfo:
        return ColumnInfo(name=name, ordinal=i, engine_type=t.value, canonical_type=t, length=ln, scale=sc,
                          nullable=key is None, key_seq=key)
    return CatalogSnapshot(connection_id=cid, engine="fakeremote", taken_at=datetime.now(timezone.utc), tables=[
        TableInfo(table=REF, capture_enabled=True, columns=[
            col("ID", 0, CanonicalType.INT, key=1), col("NAME", 1, CanonicalType.STRING, ln=10),
            col("AMT", 2, CanonicalType.DECIMAL, ln=12, sc=3), col("DATA", 3, CanonicalType.BINARY, ln=16),
            col("TS", 4, CanonicalType.LOCALDATETIME)])])


class GapError(SourceGapError):
    pass


class FakeRemote(SourceConnector):
    engine = "fakeremote"
    capture_mode = "remote"

    def __init__(self, settings: dict[str, Any] | None = None, password: str | None = None,
                 connection_id: str | None = None) -> None:
        self.cid = connection_id or str((settings or {}).get("world") or "")
        self.password = password

    @property
    def w(self) -> World:
        return WORLDS[self.cid]

    def parse_position(self, text: str) -> Position:
        return Position(engine=self.engine, text=text)

    def compare(self, a: Position, b: Position) -> int:
        return (a.text > b.text) - (a.text < b.text)

    def canonical_type(self, col: ColumnInfo) -> CanonicalType:
        return col.canonical_type

    def decode(self, raw: bytes, catalog: CatalogSnapshot, batch_id: Any) -> DecodeResult:
        return decode_batch(raw, self.engine, batch_id)

    def read_catalog(self) -> CatalogSnapshot:
        return catalog(self.cid)

    def current_position(self) -> Position | None:
        return Position(engine=self.engine, text=p(self.w.pos))

    def snapshot_rows(self, table: TableInfo, key_range: Any, chunk_rows: int) -> Iterator[list[dict[str, Any]]]:
        rows = [dict(self.w.rows[k]) for k in sorted(self.w.rows)]
        for i in range(0, len(rows), chunk_rows):
            yield rows[i:i + chunk_rows]

    def stream(self, from_pos: Position | None) -> Iterator[ChangeEvent | None]:
        self.w.stream_from.append(from_pos.text if from_pos else None)
        if self.w.gap:
            raise GapError("changes after the resume position were purged")
        start = from_pos.text if from_pos else p(0)
        for _n, ev in list(self.w.log):
            if ev.position.text > start:
                yield ev
        while True:
            time.sleep(0.05)
            yield None

    def prerequisite_checks(self, catalog: Any, probes: dict[str, Any], tables: Any = None,
                            settings: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        ok = self.password == WORLDS[self.cid].password if self.cid in WORLDS else False
        return [{"id": "login", "label": "login with the stored secret", "status": "PASS" if ok else "FAIL",
                 "detail": "", "fix_sql": None}]


def item(i: int, name: str = "x", amt: str = "1.000", data: bytes = b"\x00\xff", ts: datetime | None = None
         ) -> dict[str, Any]:
    return {"ID": i, "NAME": name, "AMT": Decimal(amt), "DATA": data, "TS": ts or datetime(2026, 1, 2, 3, 4, 5, 123456)}
