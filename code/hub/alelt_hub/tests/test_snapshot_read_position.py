"""SRC-cap-03 (hub side): RemoteRunner reports the connector's per-table consistent-read position."""
from __future__ import annotations

from typing import Any

from alelt_common.models import Position
from alelt_hub.snapshots import RemoteRunner
from alelt_snapshot.state import SnapshotJob


class _Src:
    def __init__(self, sp: dict[str, Any]) -> None:
        self.snapshot_positions = sp

    def parse_position(self, text: str) -> Position:
        if text == "bad":
            raise ValueError(text)
        return Position(engine="mariadb", text=text)


def test_remote_runner_read_position_from_connector() -> None:
    job = SnapshotJob(table="S.T", dest_name="T")
    assert RemoteRunner(_Src({"S.T": {"position": "0-1-42", "exact": True}})).read_position(job).text == "0-1-42"
    assert RemoteRunner(_Src({})).read_position(job) is None
    assert RemoteRunner(_Src({"S.T": {"position": "bad"}})).read_position(job) is None
    assert RemoteRunner(object()).read_position(job) is None
