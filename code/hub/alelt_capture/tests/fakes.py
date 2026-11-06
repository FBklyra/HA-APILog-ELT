"""Scripted remote source for runner tests: an in-memory 'log' of committed transactions with integer positions."""
from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, ClassVar

from alelt_common.canonical_jsonl import decode_batch
from alelt_common.models import (CanonicalType, CatalogSnapshot, ChangeEvent, ChangeKind, ColumnInfo, Position,
                              TableInfo, TableRef)
from alelt_common.spi import DecodeResult, SourceConnector

T = TableRef(schema="s", name="t")


class FakeRemote(SourceConnector):
    engine = "fakeremote"
    capture_mode = "remote"
    display_name = "Fake remote"
    log: ClassVar[list[list[ChangeEvent]]] = []            # committed txns, commit order
    starts: ClassVar[list[str | None]] = []                # from_pos of every stream() call
    acks: ClassVar[list[str]] = []
    on_ack: ClassVar[Any] = None
    columns: ClassVar[list[str]] = ["id", "v"]

    def __init__(self, settings: dict[str, Any] | None = None, password: str | None = None,
                 connection_id: str = "") -> None:
        self.settings = settings or {}

    @classmethod
    def reset(cls) -> None:
        cls.log, cls.starts, cls.acks, cls.on_ack, cls.columns = [], [], [], None, ["id", "v"]

    @classmethod
    def add_txn(cls, n_rows: int, big: str = "") -> str:
        base = (len(cls.log) + 1) * 1000
        txid = str(len(cls.log) + 1)
        evs = [ChangeEvent(position=Position(engine=cls.engine, text=str(base + i)), txid=txid,
                           kind=ChangeKind.INSERT, table=T, after={"id": base + i, "v": big or f"r{base + i}"})
               for i in range(n_rows)]
        evs.append(ChangeEvent(position=Position(engine=cls.engine, text=str(base + 999)), txid=txid,
                               kind=ChangeKind.COMMIT, commit_ts=datetime(2026, 11, 4, tzinfo=timezone.utc)))
        cls.log.append(evs)
        return str(base + 999)

    def parse_position(self, text: str) -> Position:
        return Position(engine=self.engine, text=str(int(text)))

    def compare(self, a: Position, b: Position) -> int:
        x, y = int(a.text), int(b.text)
        return (x > y) - (x < y)

    def canonical_type(self, col: ColumnInfo) -> CanonicalType:
        return col.canonical_type

    def decode(self, raw: bytes, catalog: CatalogSnapshot, batch_id: Any) -> DecodeResult:
        return decode_batch(raw, self.engine, batch_id)

    def read_catalog(self) -> CatalogSnapshot:
        cols = [ColumnInfo(name=c, ordinal=i, engine_type="int" if c == "id" else "text",
                           canonical_type=CanonicalType.INT if c == "id" else CanonicalType.STRING,
                           key_seq=1 if c == "id" else None) for i, c in enumerate(self.columns)]
        return CatalogSnapshot(connection_id="x", engine=self.engine, taken_at=datetime.now(timezone.utc),
                               tables=[TableInfo(table=T, capture_enabled=True, columns=cols)])

    def stream(self, from_pos: Position | None) -> Iterator[ChangeEvent | None]:
        type(self).starts.append(from_pos.text if from_pos else None)
        p = int(from_pos.text) if from_pos else -1
        for txn in list(self.log):
            if int(txn[-1].position.text) > p:
                yield from txn
        while True:
            yield None

    def ack(self, pos: Position) -> None:
        if type(self).on_ack is not None:
            type(self).on_ack(pos)
        type(self).acks.append(pos.text)

    def snapshot_rows(self, table: TableInfo, key_range: Any, chunk_rows: int) -> Iterator[list[dict[str, Any]]]:
        rows = self.settings.get("rows") or []
        for i in range(0, len(rows), chunk_rows):
            yield rows[i:i + chunk_rows]

    def current_position(self) -> Position | None:
        return Position(engine=self.engine, text="5")
