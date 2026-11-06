"""Harness logic without a database or Db2 (FIX-DESIGN WP5): terminal vs transient states, progress fingerprint incl.
completion + agent liveness, refusal without a schedule override, dry-run expectations per class."""
from __future__ import annotations

import importlib.util
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

LADDER = Path(__file__).resolve().parents[2]
REPO = LADDER.parents[1]
for p in (str(LADDER), str(REPO / "hub")):
    if p not in sys.path:
        sys.path.insert(0, p)

from _common import convergence as cv  # noqa: E402


def _row(**kw: Any) -> dict[str, Any]:
    return {"table": "S.T", "integrity": "IN_SYNC", "held_by": [], "step": None, "snapshot_error": None, **kw}


def test_terminal_vs_transient_table() -> None:
    terminal = [_row(step="manual", integrity="OUT_OF_SYNC"), _row(snapshot_error="re-issued 5 times"),
                _row(table="*", held_by=["integrity"])]
    transient = [_row(integrity="SUSPECT", held_by=["integrity"], step="catalog"),
                 _row(integrity="REDECODING", step="redecode"), _row(integrity="OUT_OF_SYNC", step="resync"),
                 _row(phase="INITIAL_LOAD", held_by=["snapshot"]), _row(phase="CATCH_UP"),
                 _row(held_by=["schedule"]), _row(pending_completions=12)]
    assert [cv.classify(d) for d in terminal] == ["terminal"] * 3
    assert [cv.classify(d) for d in transient] == ["transient"] * len(transient)
    assert len(cv.terminal_failures(terminal)) == 3 and cv.terminal_failures(transient) == []


def test_wait_ends_on_terminal_and_reports_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cv, "diagnosis", lambda e, c=None: [_row(step="manual", integrity="OUT_OF_SYNC")])
    monkeypatch.setattr(cv, "queue_state", lambda e: {})
    monkeypatch.setattr(cv, "terminal_events", lambda e, c=None: [])
    monkeypatch.setattr(cv, "agent_liveness", lambda e, c=None: {"agent": "a", "heartbeat_age_s": 1})
    monkeypatch.setattr(cv, "recent_incidents", lambda e, c=None, n=5: ["TABLE_OUT_OF_SYNC S.T OPEN x1"])
    r = cv.wait_converged(lambda: (False, "x"), 30, engine=object(), interval_s=0.01)
    assert not r.ok and r.reason == "terminal" and "operator" in str(r.extra["terminal"])
    assert r.extra["agent"]["agent"] == "a" and r.extra["incidents"]


def test_stall_detection_fingerprint_moves_with_completion_and_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cv, "diagnosis", lambda e, c=None: [_row(phase="STREAMING", pending_completions=3)])
    monkeypatch.setattr(cv, "queue_state", lambda e: {})
    monkeypatch.setattr(cv, "terminal_events", lambda e, c=None: [])
    monkeypatch.setattr(cv, "agent_liveness", lambda e, c=None: {})
    monkeypatch.setattr(cv, "recent_incidents", lambda e, c=None, n=5: [])
    ticks = {"n": 0}

    def moving(e: Any, c: Any = None) -> tuple[Any, ...]:
        ticks["n"] += 1                       # pending completions / heartbeat change every poll: progress
        return (), (), (), (("PENDING", 10 - ticks["n"] % 5),), str(ticks["n"]), "x"
    monkeypatch.setattr(cv, "progress", moving)
    r = cv.wait_converged(lambda: (False, "same"), 0.3, engine=object(), interval_s=0.01, stall_s=0.1)
    assert r.reason == "timeout"              # progress seen: never declared stalled
    monkeypatch.setattr(cv, "progress", lambda e, c=None: ((), (), (), (), "hb", "ack"))
    r = cv.wait_converged(lambda: (False, "same"), 5, engine=object(), interval_s=0.01, stall_s=0.1)
    assert r.reason == "stalled"
    assert cv.default_stall() == 300 or cv.default_stall() > 0


def _catalog_module() -> Any:
    spec = importlib.util.spec_from_file_location(f"e2e_catalog_{uuid.uuid4().hex[:6]}",
                                                  LADDER / "L4-local" / "e2e_catalog.py")
    mod = importlib.util.module_from_spec(spec)   # type: ignore[arg-type]
    spec.loader.exec_module(mod)                  # type: ignore[union-attr]
    return mod


DOC = {"connection_id": "c", "schemas": {"S": {"tables": {
    "W": {"schedule": {"window": {"start": "23:00", "end": "05:00", "tz": "America/Toronto"}}},
    "D": {"schedule": {"latency_target_s": 3600}},
    "F": {"schedule": {"latency_target_s": 60}}}}}}


def _args(**kw: Any) -> Any:
    return SimpleNamespace(**{"no_schedule": False, "window_open_after_s": None, "schedule_scale": 1.0,
                              "drain_timeout_s": 1500.0, "churn_minutes": 3.0, "initial_load_timeout_s": 900.0, **kw})


def test_refusal_without_schedule_override() -> None:
    m = _catalog_module()
    assert "window" in (m.schedule_refusal(DOC, _args()) or "")
    assert m.schedule_refusal(DOC, _args(window_open_after_s=300)) is None
    assert m.schedule_refusal(DOC, _args(no_schedule=True)) is None
    assert "beyond" in (m.schedule_refusal(DOC, _args(window_open_after_s=10_000)) or "")
    nowin = {"schemas": {"S": {"tables": {"D": {"schedule": {"latency_target_s": 3600}}}}}}
    assert m.schedule_refusal(nowin, _args()) is None


def test_dry_run_expectations_per_class() -> None:
    m = _catalog_module()
    e = m.expectations(DOC, _args(schedule_scale=0.05, window_open_after_s=300))
    assert e["classes"]["window"]["tables"] == 1
    assert e["classes"]["deferred"]["tables"] == 1 and e["classes"]["deferred"]["latency_targets_s"] == [180]
    assert e["classes"]["stream"]["tables"] == 1 and e["classes"]["stream"]["latency_targets_s"] == [3]
    assert "early == 0" in e["classes"]["window"]["expectation"] and e["refusal"] is None
    e2 = m.expectations(DOC, _args(no_schedule=True))
    assert set(e2["classes"]) == {"stream"} and e2["classes"]["stream"]["tables"] == 3
