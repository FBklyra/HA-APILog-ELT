"""Per-table `schedule` field in tables.yaml (parsed into TablePolicy.schedule)."""
from __future__ import annotations

from pathlib import Path

import pytest

from alelt_common.models import TableRef
from alelt_loader import load_policy

DOC = """connection_id: c1
output_profile: alelt
schemas:
  S:
    tables:
      FAST:
        schedule: {class: 15m, latency_target_s: 900}
      NIGHTLY:
        schedule: {class: daily, latency_target_s: 86400, window: {start: "23:00", end: "05:00", tz: UTC}}
      PLAIN: {}
"""


def _pol(tmp_path: Path, text: str = DOC):  # type: ignore[no-untyped-def]
    p = tmp_path / "tables.yaml"
    p.write_text(text, encoding="utf-8")
    return load_policy("c1", p)


def test_schedule_parsed(tmp_path: Path) -> None:
    pol = _pol(tmp_path)
    fast = pol.table(TableRef(schema="S", name="FAST")).schedule
    assert fast is not None and fast.schedule_class == "15m" and fast.latency_target_s == 900 and fast.window is None
    night = pol.table(TableRef(schema="S", name="NIGHTLY")).schedule
    assert night is not None and night.window == {"start": "23:00", "end": "05:00", "tz": "UTC"}
    assert pol.table(TableRef(schema="S", name="PLAIN")).schedule is None


def test_latency_target_fallback(tmp_path: Path) -> None:
    pol = _pol(tmp_path)
    assert pol.latency_target_s(TableRef(schema="S", name="FAST"), 120) == 900
    assert pol.latency_target_s(TableRef(schema="S", name="PLAIN"), 120) == 120
    assert pol.latency_target_s(TableRef(schema="S", name="UNLISTED"), 60) == 60


@pytest.mark.parametrize("bad", ["schedule: 5", "schedule: {latency_target_s: -1}",
                                 "schedule: {window: {start: '23:00'}}"])
def test_schedule_invalid(tmp_path: Path, bad: str) -> None:
    with pytest.raises(ValueError):
        _pol(tmp_path, f"connection_id: c1\nschemas:\n  S:\n    tables:\n      T:\n        {bad}\n")


def test_schedule_modes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """alelt_hub.schedule acts on: window -> window mode, long latency -> deferred rounds, else streaming."""
    from alelt_hub import schedule

    pol = _pol(tmp_path)
    monkeypatch.setenv("ALELT_SCHEDULE_DEFER_MIN_S", "900")
    modes = {f: schedule.mode(tp.schedule) for f, tp in pol.tables.items()}
    assert modes == {"S.FAST": "deferred", "S.NIGHTLY": "window", "S.PLAIN": "stream"}
    monkeypatch.setenv("ALELT_SCHEDULE_DEFER_MIN_S", "3600")
    assert schedule.mode(pol.tables["S.FAST"].schedule) == "stream"
    assert set(schedule.scheduled_tables(pol)) == {"S.NIGHTLY"}
