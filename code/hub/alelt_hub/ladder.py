"""Ladder evidence index (test-strategy 4.2): which `tests/ladder/*/result*.json` are formal levels, which are
supporting evidence, and which are stale - so a stale or supporting PASS is never displayed as a green ladder level.

- formal level   = directory `L0`..`L10`, file `result.json` (the gate file, X-15)
- supporting     = anything else (`L4-local/result.*.json`, `L6/result.soak-local.json`, parts like `result.chaos.json`)
- stale          = finished more than `ALELT_LADDER_STALE_H` hours ago (default 24) - evidence predates current code
- evidence       = status PASS and not stale; SKIP is never evidence
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

FORMAL = re.compile(r"^L(?:[0-9]|10)$")


def _finished(d: dict[str, Any]) -> datetime | None:
    v = d.get("finished_at") or d.get("started_at")
    if not v:
        return None
    try:
        t = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def annotate(d: dict[str, Any], path: Path, now: datetime | None = None, stale_h: float | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    stale_h = float(os.environ.get("ALELT_LADDER_STALE_H", "24")) if stale_h is None else stale_h
    fin = _finished(d)
    age_h = None if fin is None else (now - fin).total_seconds() / 3600.0
    formal = bool(FORMAL.match(path.parent.name)) and path.name == "result.json"
    stale = age_h is None or age_h > stale_h
    return {**d, "file": f"{path.parent.name}/{path.name}", "formal": formal, "age_hours": None if age_h is None
            else round(age_h, 1), "stale": stale, "evidence": d.get("status") == "PASS" and not stale}


def index(root: Path, formal_only: bool = True) -> list[dict[str, Any]]:
    out = []
    for f in sorted(root.glob("*/result*.json")):
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        a = annotate(d, f)
        if a["formal"] or not formal_only:
            out.append(a)
    return out
