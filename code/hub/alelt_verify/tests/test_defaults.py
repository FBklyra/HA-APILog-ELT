"""Default suite: counts + aggregates per key range + sampled rows; key diff only on mismatch; hash buckets optional."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from alelt_verify import CheckName, ListSide, MemoryReconStore, Status, Verifier, canon
from alelt_verify.runner import DestinationSide


def _src(n: int = 250) -> list[dict]:
    return [{"ID": i, "NAME": f"n{i}  ", "AMT": Decimal(f"{i}.10"), "TS": datetime(2026, 1, 1, 12, i % 60),
             "BIN": bytes([i % 256])} for i in range(1, n + 1)]


def _dst(rows: list[dict]) -> list[dict]:      # destination shape: lower-case names, tz-aware, trimmed, scale kept
    return [{"id": r["ID"], "name": r["NAME"].rstrip(), "amt": r["AMT"] + Decimal("0.00"),
             "ts": r["TS"].replace(tzinfo=timezone.utc), "bin": r["BIN"], "_alelt_deleted": False,
             "_alelt_synced": datetime.now(timezone.utc)} for r in rows]


def _run(ref: list[dict], cand: list[dict], **kw: object) -> object:
    v = Verifier(DestinationSide(None), DestinationSide(None), MemoryReconStore())
    rs = ListSide(lambda _t: ref).rows("S.T")
    cs = ListSide(lambda _t: cand, deleted="_ALELT_DELETED").rows("S.T")
    return v.default_suite("S.T", rs, cs, ["ID"], ["ID", "NAME", "AMT", "TS", "BIN"], range_rows=50, **kw)


def test_canonical_forms_equal_across_engines() -> None:
    assert canon(Decimal("1.10")) == canon(Decimal("1.1000")) == "1.1"
    assert canon(datetime(2026, 1, 1, 1, tzinfo=timezone.utc)) == canon(datetime(2026, 1, 1, 1))
    assert canon("ab  ") == "ab" and canon(b"\x01") == "01" and canon(True) == "true"


def test_clean_match_runs_no_key_diff_and_no_buckets() -> None:
    src = _src()
    out = _run(src, _dst(src))
    assert out.outcome == "MATCH"
    kinds = {r.check for r in out.results}
    assert kinds == {CheckName.ROW_COUNT, CheckName.COLUMN_AGGREGATES, CheckName.SAMPLED_ROWS}   # no KEY_SET, no hashes
    assert out.summary["ranges"] == 5


def test_mismatch_is_located_and_key_diff_listed() -> None:
    src = _src()
    dst = _dst(src)
    dst = [r for r in dst if r["id"] != 120]                  # missing row
    dst[10]["amt"] = Decimal("999")                           # changed value in range 0
    dst.append({**dst[0], "id": 9999})                        # extra row
    out = _run(src, dst)
    assert out.outcome == "MISMATCH"
    agg = [r for r in out.results if r.check is CheckName.COLUMN_AGGREGATES and r.status is Status.FAIL]
    assert {r.bucket for r in agg} >= {0, 2} and any(r.column == "AMT" for r in agg)
    keys = [r for r in out.results if r.check is CheckName.KEY_SET and r.key]
    assert {(r.key, r.actual) for r in keys} == {('["120"]', "missing"), ('["9999"]', "row")}


def test_soft_deleted_rows_are_not_live_and_hash_buckets_optional() -> None:
    src = _src(20)
    dst = _dst(src) + [{**_dst(src)[0], "id": 77, "_alelt_deleted": True}]
    out = _run(src, dst, hash_buckets=8)
    assert out.outcome == "MATCH"
    assert any(r.check is CheckName.COLUMN_CHECKSUM for r in out.results)     # buckets only when asked
