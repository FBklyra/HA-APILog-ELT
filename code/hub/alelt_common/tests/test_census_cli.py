"""alelt_capture.census.run_census: read-only census over a connector stream (no ack, stops at --to / idle)."""
from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from alelt_capture.census import run_census  # noqa: E402
from alelt_common import completeness as FC  # noqa: E402
from alelt_common.models import ChangeEvent, ChangeKind, Position, TableRef  # noqa: E402
from alelt_common.spi import SourceConnector  # noqa: E402


class Fake(SourceConnector):
    engine = "fake"
    capture_mode = "remote"

    def __init__(self) -> None:
        self.acked: list[str] = []
        self.closed = False

    def parse_position(self, text: str) -> Position:
        return Position(engine="fake", text=text)

    def compare(self, a: Position, b: Position) -> int:
        return int(a.text) - int(b.text)

    def canonical_type(self, col):  # type: ignore[no-untyped-def]
        raise NotImplementedError

    def decode(self, raw, catalog, batch_id):  # type: ignore[no-untyped-def]
        raise NotImplementedError

    def ack(self, pos: Position) -> None:
        self.acked.append(pos.text)

    def close(self) -> None:
        self.closed = True

    def stream(self, from_pos: Position | None) -> Iterator[ChangeEvent | None]:
        t = TableRef(schema="s", name="t")
        for i in range(1, 6):
            self.census.count("rec:I", FC.RecordClass.DECODED)
            yield ChangeEvent(position=Position(engine="fake", text=str(i * 10 - 1)), txid=str(i),
                              kind=ChangeKind.INSERT, table=t, after={"id": i})
            if i == 2:
                self.census.count("rec:X", FC.RecordClass.NEEDS_KEYED_REREAD, "partial")
                yield FC.keyed_reread(t, [{"id": 2}], "partial image: x").event(
                    Position(engine="fake", text="19"), "2")
            if i == 3:
                self.census.unexplained("rec:?")
            yield ChangeEvent(position=Position(engine="fake", text=str(i * 10)), txid=str(i), kind=ChangeKind.COMMIT)
        while True:
            yield None


def test_census_stops_at_to_and_never_acks() -> None:
    src = Fake()
    rep = run_census(src, None, Position(engine="fake", text="20"), idle_s=0.01)
    assert rep["stopped_by"] == "to_pos" and rep["last_commit"] == "20" and src.closed and not src.acked
    assert rep["events"] == {"INSERT": 2, "COMMIT": 2, "OTHER": 1} and rep["unexplained"] == 0
    assert rep["signals"] == [{"class": "needs_keyed_reread", "table": "s.t", "reason": "partial image", "count": 1}]


def test_census_idle_stop_reports_unexplained() -> None:
    rep = run_census(Fake(), None, None, idle_s=0.01, max_s=5)
    assert rep["stopped_by"] == "idle" and rep["unexplained"] == 1 and rep["events"]["COMMIT"] == 5
