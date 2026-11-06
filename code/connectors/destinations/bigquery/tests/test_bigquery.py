"""alelt_dst_bigquery: script generation tests (always) + conformance against bigquery-emulator when
ALELT_TEST_BIGQUERY_URL is set (bigquery://alelttest/?endpoint=http://localhost:59050&emulator=1)."""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from alelt_common.models import CanonicalType, ColumnInfo
from alelt_dst_bigquery import BigQueryDestination, BigQueryDialect, BigQueryExecutor
from alelt_dst_kit.conformance import DestinationConformance, INFO, MAP, ROW1, ROW2, TABLES, cs, e, p


class FakeClient:
    def __init__(self) -> None:
        self.sent: list[str] = []

    def query(self, sql: str, **kw: Any) -> Any:
        self.sent.append(sql)

        class R:
            schema: list[Any] = []

            class session_info:  # noqa: N801
                session_id = "sess-1"

            def result(self, timeout: float = 0) -> Any:
                return self

            def __iter__(self) -> Any:
                return iter([])
        return R()


def _d(url: str = "bigquery://proj/") -> tuple[BigQueryDestination, FakeClient]:
    d = BigQueryDestination(url, schema="land")
    fc = FakeClient()
    d._ex = BigQueryExecutor(fc, d.d)
    return d, fc


def test_literals() -> None:
    d = BigQueryDialect()
    assert d.string_lit("a'b\\c\nd") == "'a\\'b\\\\c\\nd'"
    assert d.lit(Decimal("1.50"), "NUMERIC(9,2)") == "CAST('1.50' AS NUMERIC)"
    assert d.lit(None, "DATE") == "CAST(NULL AS DATE)"
    assert d.lit(b"\x01", "BYTES") == "FROM_HEX('01')"
    assert d.lit(date(2026, 1, 2), "DATE") == "CAST('2026-01-02' AS DATE)"
    assert d.lit(datetime(2026, 1, 2, 3, tzinfo=timezone.utc), "TIMESTAMP").endswith("+00:00' AS TIMESTAMP)")
    assert d.lit({"a": 1}, "JSON") == "PARSE_JSON('{\"a\": 1}')"
    big = ColumnInfo(name="x", ordinal=0, engine_type="DECIMAL", canonical_type=CanonicalType.DECIMAL, length=31,
                     scale=0)
    assert d.ptype(big) == "BIGNUMERIC(31,0)"


def test_one_script_per_batch() -> None:
    d, fc = _d()
    d.ensure_table(INFO, "orders", None)
    fc.sent.clear()
    d.apply("s", cs(e(1, "INSERT", after=ROW1), e(2, "INSERT", after=ROW2), e(3, "DELETE", before={"ID": 2})),
            p(3), "b1", MAP, TABLES)
    scripts = [s for s in fc.sent if s.startswith("BEGIN TRANSACTION;")]
    assert len(scripts) == 1
    s = scripts[0]
    assert s.endswith("COMMIT TRANSACTION;") and s.count("MERGE `land`.`orders` AS t USING (SELECT ") == 2
    assert "ASSERT (SELECT COUNT(*) FROM `alelt_control`.`checkpoint` WHERE `stream` = 's') = 0 AS" in s
    assert "INSERT INTO `alelt_control`.`checkpoint`" in s and s.index("ASSERT") < s.index("MERGE")
    assert "INSERT INTO `alelt_control`.`audit_batch`" in s and "CAST('1.50' AS NUMERIC)" in s


def test_crash_sends_nothing() -> None:
    d, fc = _d()
    d.ensure_table(INFO, "orders", None)
    fc.sent.clear()

    def boom(pt: str) -> None:
        raise RuntimeError(pt)
    d._hook = boom  # type: ignore[method-assign]
    with pytest.raises(RuntimeError):
        d.apply("s", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
    assert not any(x.startswith("BEGIN") for x in fc.sent)


def test_hard_delete_sql() -> None:
    d, fc = _d("bigquery://proj/?delete_mode=hard")
    d.ensure_table(INFO, "orders", None)
    d.apply("s", cs(e(2, "DELETE", before={"ID": 5})), p(2), "b", MAP, TABLES)
    assert any("DELETE FROM `land`.`orders` AS t WHERE EXISTS (SELECT 1 FROM (SELECT " in x for x in fc.sent)


def test_clustered_tables_and_size_bounded_statements() -> None:
    d, fc = _d()
    d.ensure_table(INFO, "orders", None)
    ddl = next(x for x in fc.sent if "CREATE TABLE IF NOT EXISTS `land`.`orders`" in x)
    assert ddl.endswith("CLUSTER BY `id`")
    fc.sent.clear()
    big = "x" * 20_000
    d.apply("s", cs(*(e(i, "INSERT", after={**ROW1, "ID": i, "NAME": big}) for i in range(1, 60))), p(59), "b1",
            MAP, TABLES)
    assert "BEGIN TRANSACTION" in fc.sent and "COMMIT TRANSACTION" in fc.sent      # > 1 MB: session transaction
    body = [x for x in fc.sent if "MERGE `land`.`orders`" in x]
    assert sum(x.count("MERGE `land`.`orders`") for x in body) >= 4    # rows split by rendered size
    assert all(len(x.encode()) < 1_000_000 for x in fc.sent)
    assert any("ASSERT" in x for x in fc.sent)


URL = os.environ.get("ALELT_TEST_BIGQUERY_URL")


@pytest.mark.skipif(not URL, reason="ALELT_TEST_BIGQUERY_URL not set")
class TestBigQueryEmulatorConformance(DestinationConformance):
    """Conformance on bigquery-emulator. Emulator-only limits (README "Known emulator limits") are xfail with reason
    "emulator limit"; the emulator is restarted between test groups and after a crash (conftest)."""

    def make(self, schema: str, delete_mode: str = "soft") -> Any:
        url = URL or ""
        if "timeout=" not in url:                       # a hung emulator job fails the test instead of hanging it
            url += ("&" if "?" in url else "?") + "timeout=120"
        return BigQueryDestination(url, schema=schema, control_schema=f"{schema}_ctl", delete_mode=delete_mode)

    @pytest.mark.xfail(reason="emulator limit: DELETE ... WHERE EXISTS (correlated subquery) deletes nothing",
                       strict=False)
    def test_hard_delete_mode(self) -> None:
        super().test_hard_delete_mode()


def test_string_literal_control_chars_and_invalid_utf8() -> None:
    """DST-bigquery-04: NUL and other control characters escaped; lone surrogates refused."""
    d = BigQueryDialect()
    assert d.string_lit("a\x00b\x1fc\x7f\td") == r"'a\x00b\x1fc\x7f\td'"
    with pytest.raises(ValueError, match="surrogate"):
        d.string_lit("bad\udce9")
