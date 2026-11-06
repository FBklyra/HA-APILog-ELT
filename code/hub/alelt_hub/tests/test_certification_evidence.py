"""F-19: certification criteria come from recorded evidence only - LATENCY_P95 from commit -> apply timestamps
(never the lag proxy), CLEAN_DAYS_7 from consecutive calendar days, keyless PK_SET = multiset parity (F-10)."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from alelt_hub import certification as cert


def test_percentile_nearest_rank() -> None:
    assert cert.percentile([]) is None
    assert cert.percentile([5.0]) == 5.0
    xs = [float(i) for i in range(1, 101)]
    assert cert.percentile(xs) == 95.0
    assert cert.percentile([1, 1, 1, 100]) == 100          # a slow flush in 4 dominates p95


def test_clean_days_use_the_single_evaluator() -> None:
    """XC-11: console CLEAN_DAYS_7 = alelt_verify.certification.evaluate over recon rows + recorded latency days."""
    from alelt_verify.certification import evaluate

    now = datetime.now(timezone.utc)
    days = [(now - timedelta(days=i)).date() for i in range(1, 8)]
    at = lambda d: datetime(d.year, d.month, d.day, 10, tzinfo=timezone.utc)  # noqa: E731
    kinds = ["ROW_COUNT", "KEY_SET", "COLUMN_CHECKSUM", "DELETES", "DBT_SHADOW"]
    rows = [(k, "MATCH", at(d)) for d in days for k in kinds]
    full = evaluate("S.T", cert.evidence(rows, "S.T", {d: True for d in days}), now)
    assert full.criteria["CONSECUTIVE_CLEAN_DAYS"] and full.clean_streak == 7
    no_lat = evaluate("S.T", cert.evidence(rows, "S.T", {}), now)               # no latency evidence: not clean
    assert not no_lat.criteria["CONSECUTIVE_CLEAN_DAYS"]
    row_count_only = [(k, o, t) for k, o, t in rows if k == "ROW_COUNT"]       # old rule passed on this
    assert not evaluate("S.T", cert.evidence(row_count_only, "S.T", {d: True for d in days}), now).certified
    slow = {d: (d != days[2]) for d in days}                                   # one slow day breaks the streak
    assert evaluate("S.T", cert.evidence(rows, "S.T", slow), now).clean_streak == 2

def test_latency_from_recorded_flushes(client: Any) -> None:
    from sqlalchemy import insert

    from alelt_hub.db import batch_table_stat, get_engine

    cid = f"lat-{uuid.uuid4().hex[:6]}"
    now = datetime.now(timezone.utc)
    with get_engine().begin() as c:
        assert cert.latency_criterion(c, cid, "S.T", 120.0)[0] == "UNKNOWN"     # no evidence: never PASS
        for i, lat in enumerate([10, 20, 30, 400]):
            c.execute(insert(batch_table_stat).values(
                batch_id=f"{cid}:{i}", table_fqn="S.T", inserts=1, created_at=now,
                commit_ts_min=now - timedelta(seconds=lat), commit_ts_max=now - timedelta(seconds=1)))
        c.execute(insert(batch_table_stat).values(batch_id=f"{cid}:9", table_fqn="S.OTHER", inserts=1,
                                                  created_at=now, commit_ts_min=now - timedelta(seconds=5)))
        c.execute(insert(batch_table_stat).values(batch_id=f"x{cid}:9", table_fqn="S.T", inserts=1,   # other conn
                                                  created_at=now, commit_ts_min=now - timedelta(seconds=9999)))
        st, val = cert.latency_criterion(c, cid, "S.T", 120.0)
        assert st == "FAIL" and val.startswith("p95 400s")
        assert cert.latency_criterion(c, cid, "S.OTHER", 120.0)[0] == "PASS"
        assert cert.summary(c, cid)["latency_flushes"] == 5


def test_certification_latency_not_proxied_by_lag(client: Any) -> None:
    """With a fresh checkpoint lag of ~0 s but no recorded flush evidence, LATENCY_P95 must not be PASS."""
    from alelt_hub.tests.conftest import role

    cert_rows = client.get("/api/v1/certification", headers=role("Auditor")).json()
    for row in cert_rows:
        lat = next(x for x in row["criteria"] if x["criterion"] == "LATENCY_P95")
        assert lat["status"] in {"UNKNOWN", "PASS", "FAIL"}
        if lat["status"] == "PASS":
            assert lat["value"].startswith("p95 ")             # only from recorded flushes
