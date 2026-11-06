"""Generic source-completeness contract (alelt_common/completeness.py, X-132)."""
from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from alelt_common import completeness as FC  # noqa: E402
from alelt_common.canonical_jsonl import decode_batch, encode_events  # noqa: E402
from alelt_common.models import ChangeEvent, ChangeKind, Position, TableInfo, TableRef  # noqa: E402
from alelt_common.spi import SourceConnector  # noqa: E402

T = TableRef(schema="s", name="t")
P = Position(engine="x", text="1")


def test_signal_roundtrip_through_the_canonical_batch() -> None:
    sigs = [FC.keyed_reread(T, [{"id": 7, "d": date(2026, 1, 2), "n": Decimal("1.50"), "b": b"\x00\x01"}],
                            "partial image", "binlog:31", columns=["c"]),
            FC.keyed_reread(T, None, "range", key_range=([1], [datetime(2026, 1, 1, tzinfo=timezone.utc)])),
            FC.catalog_refresh(T, "shape changed", "pgoutput:R"),
            FC.unsupported(None, "connection-wide", "x")]
    evs = [s.event(P, "tx1") for s in sigs]
    assert evs[2].kind is ChangeKind.DDL and evs[0].kind is ChangeKind.OTHER and evs[3].table is None
    back = decode_batch(encode_events(evs), "x", "8b0f2a5c-6c55-4d1e-9a52-1d1f0a0b0c0d")
    assert not back.quarantined
    got = [FC.signal_of(e) for e in back.events]
    assert all(g is not None for g in got)
    assert got[0].keys == sigs[0].keys and got[0].detail == {"columns": ["c"]} and got[0].table == T  # type: ignore[union-attr]
    assert got[1].key_range == ([1], [datetime(2026, 1, 1, tzinfo=timezone.utc)])  # type: ignore[union-attr]
    assert [g.cls for g in got] == [s.cls for s in sigs]  # type: ignore[union-attr]


def test_ordinary_events_are_not_signals_and_decoded_is_not_a_signal_class() -> None:
    assert FC.signal_of(ChangeEvent(position=P, txid="1", kind=ChangeKind.OTHER, note="quarantine:x")) is None
    assert FC.signal_of(ChangeEvent(position=P, txid="1", kind=ChangeKind.UPDATE, note=FC.SIGNAL_PREFIX + "{}")) is None
    with pytest.raises(ValueError):
        FC.SourceSignal(FC.RecordClass.DECODED, "x")
    with pytest.raises(ValueError):
        FC.SourceSignal(FC.RecordClass.IGNORED, "x")


def test_census_counts_and_unexplained() -> None:
    c = FC.Census("x")
    c.count("r:I", FC.RecordClass.DECODED, "ignored reason is dropped for decoded")
    c.count("r:I", "decoded")
    c.count("r:O", FC.RecordClass.IGNORED, "origin")
    c.signal(FC.unsupported(T, "bad: detail", "r:X"))
    assert c.by_class() == {"decoded": 2, "ignored": 1, "unsupported": 1} and c.unexplained_total == 0
    c.unexplained("r:?")
    rep = c.report()
    assert rep["unexplained"] == 1 and rep["total"] == 5
    assert {"record": "r:I", "class": "decoded", "reason": "", "count": 2} in rep["rows"]
    d = FC.Census("x")
    d.merge(c)
    assert d.total == 5


def test_spi_defaults_census_per_instance_and_keyed_reread_not_implemented() -> None:
    class S(SourceConnector):
        engine = "x"
        capture_mode = "remote"

        def parse_position(self, text: str) -> Position:
            return Position(engine="x", text=text)

        def compare(self, a: Position, b: Position) -> int:
            return 0

        def canonical_type(self, col):  # type: ignore[no-untyped-def]
            raise NotImplementedError

        def decode(self, raw, catalog, batch_id):  # type: ignore[no-untyped-def]
            raise NotImplementedError

    a, b = S(), S()
    a.census.count("r", "decoded")
    assert a.census is a.census and b.census.total == 0 and a.census.engine == "x"
    with pytest.raises(NotImplementedError):
        a.keyed_reread(TableInfo(table=T), [{"id": 1}])
