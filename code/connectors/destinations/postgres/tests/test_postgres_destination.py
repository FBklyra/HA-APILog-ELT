"""alelt_dst_postgres against a real Postgres. Needs ALELT_TEST_PG_URL=postgresql://user:pw@host:port (admin)."""
from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import date
from decimal import Decimal

import pytest

psycopg = pytest.importorskip("psycopg")

from alelt_common.models import (  # noqa: E402
    CanonicalType, ChangeEvent, ChangeKind, ChangeSet, ColumnInfo, Position, TableInfo, TableRef)
from alelt_dst_postgres import PostgresDestination, default_compare  # noqa: E402

ADMIN = os.environ.get("ALELT_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not ADMIN, reason="ALELT_TEST_PG_URL not set")
T = TableRef(schema="S", name="ORDERS")
INFO = TableInfo(table=T, columns=[
    ColumnInfo(name="ID", ordinal=0, engine_type="INTEGER", canonical_type=CanonicalType.INT, key_seq=1, nullable=False),
    ColumnInfo(name="NAME", ordinal=1, engine_type="VARCHAR", canonical_type=CanonicalType.STRING, length=20),
    ColumnInfo(name="AMT", ordinal=2, engine_type="DECIMAL", canonical_type=CanonicalType.DECIMAL, length=9, scale=2),
    ColumnInfo(name="D", ordinal=3, engine_type="DATE", canonical_type=CanonicalType.LOCALDATE),
])


@pytest.fixture(scope="module")
def url() -> Iterator[str]:
    db = f"alelt_test_dst_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(f"{ADMIN}/postgres", autocommit=True) as c:
        c.execute(f"CREATE DATABASE {db}")
    yield f"{ADMIN}/{db}"
    with psycopg.connect(f"{ADMIN}/postgres", autocommit=True) as c:
        c.execute(f"DROP DATABASE {db} WITH (FORCE)")


def p(n: int) -> Position:
    return Position(engine="db2", text=f"1:{n:016X}")


def e(n: int, kind: str, before: dict | None = None, after: dict | None = None) -> ChangeEvent:  # type: ignore[type-arg]
    return ChangeEvent(position=p(n), txid="t", kind=ChangeKind(kind), table=T, before=before, after=after)


def test_apply_idempotent_soft_delete(url: str) -> None:
    d = PostgresDestination(url, schema="land")
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    sets = [ChangeSet(txid="a", commit_position=p(3), commit_ts=None, events=[
        e(1, "INSERT", after={"ID": 1, "NAME": "a", "AMT": Decimal("1.50"), "D": date(2026, 1, 2)}),
        e(2, "INSERT", after={"ID": 2, "NAME": "b", "AMT": None, "D": None}),
        e(3, "DELETE", before={"ID": 2})])]
    st = d.apply("s1", sets, p(3), "b1", {T: "orders"}, {T: INFO})
    assert st and st[0].inserts == 2 and st[0].deletes == 1
    for _ in range(3):                                          # replay x3 = no-op
        assert d.apply("s1", sets, p(3), "b1", {T: "orders"}, {T: INFO}) == []
    rows = {r["id"]: r for r in d.fetch_rows("orders")}
    assert rows[1]["amt"] == Decimal("1.50") and rows[1]["_alelt_deleted"] is False
    assert rows[2]["_alelt_deleted"] is True and rows[2]["name"] == "b"     # soft delete keeps the row
    assert d.read_checkpoint("s1").text == p(3).text  # type: ignore[union-attr]
    assert len(d.audit_rows("s1")) == 1
    upd = [ChangeSet(txid="b", commit_position=p(4), commit_ts=None,
                     events=[e(4, "UPDATE", before={"ID": 1}, after={"ID": 1, "NAME": "z"})])]
    st = d.apply("s1", upd, p(4), "b2", {T: "orders"}, {T: INFO})
    assert st[0].updates == 1
    assert {r["id"]: r["name"] for r in d.fetch_rows("orders")}[1] == "z"
    assert d.row_count("orders") == 1 and d.row_count("orders", include_deleted=True) == 2


def test_atomic_rollback_on_error(url: str) -> None:
    d = PostgresDestination(url, schema="land2")
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    bad = [ChangeSet(txid="x", commit_position=p(9), commit_ts=None, events=[
        e(8, "INSERT", after={"ID": 7, "NAME": "ok"}), e(9, "INSERT", after={"NAME": "no key"})])]
    with pytest.raises(ValueError):
        d.apply("s2", bad, p(9), "bx", {T: "orders"}, {T: INFO})
    assert d.row_count("orders") == 0 and d.read_checkpoint("s2") is None


def test_profile_and_truncate(url: str) -> None:
    d = PostgresDestination(url, schema="land3", output_profile="alelt")
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    d.load_snapshot("orders", INFO, [{"ID": 1, "NAME": "s"}, {"ID": 2, "NAME": "t"}])
    tr = ChangeEvent(position=p(5), txid="t", kind=ChangeKind.TRUNCATE, table=T)
    d.apply("s3", [ChangeSet(txid="t", commit_position=p(5), commit_ts=None, events=[tr])], p(5), "bt",
            {T: "orders"}, {T: INFO})
    assert d.row_count("orders") == 0 and d.row_count("orders", include_deleted=True) == 2


def test_default_compare() -> None:
    assert default_compare(Position(engine="", text="1:000000000000000A"),
                           Position(engine="", text="1:0000000000000009")) > 0
    assert default_compare(Position(engine="", text="2:0"), Position(engine="", text="10:0")) < 0


JT = TableRef(schema="S", name="DOCS")
JINFO = TableInfo(table=JT, columns=[
    ColumnInfo(name="ID", ordinal=0, engine_type="INTEGER", canonical_type=CanonicalType.INT, key_seq=1, nullable=False),
    ColumnInfo(name="J", ordinal=1, engine_type="JSON", canonical_type=CanonicalType.JSON),
    ColumnInfo(name="TS", ordinal=2, engine_type="TIMESTAMP", canonical_type=CanonicalType.LOCALDATETIME),
])


def test_snapshot_json_values_like_cdc(url: str) -> None:
    """Snapshot rows may carry JSON as dict / list (connector values, plain chunks): stored exactly like the CDC
    path's JSON text; a tagged value of another column (version skew) is decoded, never 'cannot adapt dict'."""
    from datetime import datetime

    d = PostgresDestination(url, schema="snapj")
    d.bootstrap()
    d.ensure_table(JINFO, "docs", None)
    n = d.load_snapshot("docs", JINFO, [
        {"ID": 1, "J": {"a": 1, "b": [1, 2]}, "TS": None},
        {"ID": 2, "J": [1, "x", None], "TS": {"$ldt": "2026-01-02T03:04:05"}},
        {"ID": 3, "J": '{"a": 1, "b": [1, 2]}', "TS": None}])
    assert n == 3
    d.apply("sj", [ChangeSet(txid="c", commit_position=p(9), commit_ts=None, events=[ChangeEvent(
        position=p(9), txid="c", kind=ChangeKind.INSERT, table=JT, after={"ID": 4, "J": '[1, "x", null]'})])],
        p(9), "bj", {JT: "docs"}, {JT: JINFO})
    rows = {r["id"]: r for r in d.fetch_rows("docs")}
    assert rows[1]["j"] == rows[3]["j"] == {"a": 1, "b": [1, 2]}
    assert rows[2]["j"] == rows[4]["j"] == [1, "x", None]
    assert rows[2]["ts"] == datetime(2026, 1, 2, 3, 4, 5)
    with pytest.raises(ValueError):
        d.load_snapshot("docs", JINFO, [{"ID": 5, "J": None, "TS": {"not": "a tag"}}])


def test_legacy_control_tables_keep_working(url: str) -> None:
    """Control tables created by the earlier hand-written connector (bigserial audit id, jsonb rows_by_table,
    position NOT NULL, no watermarks column) are migrated in place at bootstrap."""
    with psycopg.connect(url, autocommit=True) as c:
        c.execute("CREATE SCHEMA IF NOT EXISTS legctl")
        c.execute("CREATE TABLE legctl.checkpoint (stream text PRIMARY KEY, position text NOT NULL, "
                  "batch_id text, updated_at timestamptz NOT NULL DEFAULT now())")
        c.execute("CREATE TABLE legctl.audit_batch (id bigserial PRIMARY KEY, batch_id text NOT NULL, stream text "
                  "NOT NULL, from_position text, to_position text NOT NULL, rows_by_table jsonb NOT NULL, total_rows "
                  "integer NOT NULL, duration_ms integer NOT NULL, status text NOT NULL, applied_at timestamptz NOT "
                  "NULL DEFAULT now())")
        c.execute("INSERT INTO legctl.checkpoint (stream, position, batch_id) VALUES ('old', %s, 'b0')", (p(2).text,))
    d = PostgresDestination(url, schema="legland", control_schema="legctl")
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    sets = [ChangeSet(txid="a", commit_position=p(3), commit_ts=None, events=[e(3, "INSERT", after={"ID": 1})])]
    assert d.apply("old", sets, p(3), "b1", {T: "orders"}, {T: INFO})
    assert d.read_checkpoint("old").text == p(3).text  # type: ignore[union-attr]
    assert {k: v.text for k, v in d.read_watermarks("old").items()} == {"orders": p(3).text}
    assert [a["batch_id"] for a in d.audit_rows("old")] == ["b1"]


from alelt_dst_kit.conformance import DestinationConformance  # noqa: E402


@pytest.mark.usefixtures("url")
class TestPostgresConformance(DestinationConformance):
    @pytest.fixture(autouse=True)
    def _bind(self, url: str) -> None:
        type(self).url = url

    def make(self, schema: str, delete_mode: str = "soft") -> PostgresDestination:
        return PostgresDestination(self.url, schema=schema, control_schema=f"{schema}_c",  # type: ignore[attr-defined]
                                   system_columns_on=delete_mode != "hard")
