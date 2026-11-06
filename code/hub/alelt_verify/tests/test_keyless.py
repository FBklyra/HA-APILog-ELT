"""F-10: keyless tables. The surrogate id collapses identical rows (documented semantic, X-28); parity for keyless
tables is a multiset comparison, never a key-set comparison."""
from __future__ import annotations

from alelt_verify import ListSide, MemoryReconStore, Status, Verifier
from alelt_verify.runner import DestinationSide

COLS = ["CODE", "QTY"]


def _run(ref: list[dict], cand: list[dict]) -> object:
    v = Verifier(DestinationSide(None), DestinationSide(None), MemoryReconStore())
    rs = ListSide(lambda _t: ref).rows("S.K")
    cs = ListSide(lambda _t: cand).rows("S.K")
    return v.default_suite("S.K", rs, cs, [], COLS, range_rows=50)


def test_duplicate_shift_is_a_mismatch() -> None:
    """[X, X, Y] vs [X, Y, Y]: same count, same distinct set - only a multiset check sees it."""
    x, y = {"CODE": "A", "QTY": 1}, {"CODE": "B", "QTY": 2}
    r = _run([x, dict(x), y], [x, y, dict(y)])
    assert r.status is Status.FAIL
    assert "ROW_MULTISET" in r.summary["checks"]


def test_identical_multisets_match() -> None:
    x, y = {"CODE": "A", "QTY": 1}, {"CODE": "B", "QTY": 2}
    r = _run([x, dict(x), y], [dict(x), y, dict(x)])
    assert r.status is Status.PASS


def test_surrogate_collapses_identical_rows_documented() -> None:
    """Negative test pinning the documented approximation: two identical keyless rows share one surrogate id, so the
    landing (keyed by the surrogate) holds one row for them. Changing this needs a contract decision (OQ-03)."""
    from alelt_loader.loader import surrogate_id

    row = {"CODE": "A", "QTY": 1}
    assert surrogate_id(row, COLS) == surrogate_id(dict(row), COLS)
