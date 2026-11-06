"""Test-strategy 4.2: supporting results are not ladder levels; stale or SKIP results are never evidence."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from alelt_hub.ladder import index


def _w(root: Path, rel: str, status: str, age_h: float) -> None:
    p = root / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    at = (datetime.now(timezone.utc) - timedelta(hours=age_h)).isoformat()
    p.write_text(json.dumps({"level": rel.split("/")[0][:3].rstrip("-"), "status": status, "finished_at": at}))


def test_index(tmp_path: Path) -> None:
    _w(tmp_path, "L3/result.json", "FAIL", 40)
    _w(tmp_path, "L4/result.json", "SKIP", 1)
    _w(tmp_path, "L4-local/result.json", "PASS", 1)              # supporting: not the L4 gate
    _w(tmp_path, "L5/result.json", "PASS", 2)
    _w(tmp_path, "L6/result.soak-local.json", "PASS", 1)
    _w(tmp_path, "L10/result.json", "PASS", 100)
    got = {d["file"]: d for d in index(tmp_path)}
    assert set(got) == {"L3/result.json", "L4/result.json", "L5/result.json", "L10/result.json"}
    assert got["L5/result.json"]["evidence"] and not got["L5/result.json"]["stale"]
    assert got["L10/result.json"]["stale"] and not got["L10/result.json"]["evidence"]
    assert not got["L4/result.json"]["evidence"]                 # SKIP is never evidence
    allf = {d["file"]: d for d in index(tmp_path, formal_only=False)}
    assert not allf["L4-local/result.json"]["formal"] and not allf["L6/result.soak-local.json"]["formal"]
