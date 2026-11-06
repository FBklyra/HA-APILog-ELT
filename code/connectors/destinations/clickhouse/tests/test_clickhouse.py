"""alelt_dst_clickhouse: unit tests + conformance suite against a live server (ALELT_TEST_CLICKHOUSE_URL,
e.g. clickhouse://alelttest:alelttest_local_only@localhost:58123/)."""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from alelt_common.models import CanonicalType, ColumnInfo
from alelt_dst_clickhouse import ClickHouseDestination, ch_type
from alelt_dst_clickhouse.destination import _lit
from alelt_dst_kit.conformance import DestinationConformance

URL = os.environ.get("ALELT_TEST_CLICKHOUSE_URL")


def test_types() -> None:
    c = ColumnInfo(name="a", ordinal=0, engine_type="DECIMAL", canonical_type=CanonicalType.DECIMAL, length=9, scale=2)
    assert ch_type(c) == "Nullable(Decimal(9,2))"
    k = c.model_copy(update={"key_seq": 1})
    assert ch_type(k) == "Decimal(9,2)"
    s = ColumnInfo(name="s", ordinal=0, engine_type="VARCHAR", canonical_type=CanonicalType.STRING, length=5,
                   nullable=False)
    assert ch_type(s) == "String"


def test_literals() -> None:
    assert _lit("it's \\") == r"'it\'s \\'"
    assert _lit(date(2026, 1, 2)) == "toDate32('2026-01-02')"
    assert _lit(datetime(2026, 1, 2, 3, tzinfo=timezone.utc)).startswith("toDateTime64('2026-01-02 03:00:00'")
    assert _lit(b"\x01\xff") == "unhex('01ff')" and _lit(None) == "NULL" and _lit(Decimal("1.5")) == "1.5"


@pytest.mark.skipif(not URL, reason="ALELT_TEST_CLICKHOUSE_URL not set")
class TestClickHouseConformance(DestinationConformance):
    def make(self, schema: str, delete_mode: str = "soft") -> Any:
        return ClickHouseDestination(URL or "", schema=schema, control_schema=f"{schema}_ctl", delete_mode=delete_mode)


def test_writer_fence_registry_rules() -> None:
    """DST-clickhouse-01: main stream needs the pipeline lock; replay streams the pipeline or replay lock; a dead lock
    is refused."""
    from alelt_common.spi import WriterFenceError, hold_writer, release_writer, require_writer

    release_writer("stream:*")
    with pytest.raises(WriterFenceError, match="no hub stream lock"):
        require_writer("cid")
    hold_writer("replay:cid", lambda: True)
    require_writer("cid#pass:1")
    with pytest.raises(WriterFenceError):
        require_writer("cid")                                  # main stream: replay lock is not enough
    hold_writer("pipeline:cid", lambda: False)
    with pytest.raises(WriterFenceError, match="lost"):
        require_writer("cid")
    release_writer("pipeline:cid")
    release_writer("replay:cid")


def test_hub_try_lock_registers_and_releases_the_fence(tmp_path: Any) -> None:
    import sqlalchemy as sa

    from alelt_common.spi import release_writer, writer_fence
    from alelt_hub.dialect import try_lock, unlock

    release_writer("stream:*")
    eng = sa.create_engine(f"sqlite:///{tmp_path / 'hub.db'}")
    h = try_lock(eng, "pipeline:c1")
    assert h is not None and writer_fence("c1") is not None and writer_fence("c1")()  # type: ignore[misc]
    unlock(h, "pipeline:c1")
    assert writer_fence("c1") is None


@pytest.mark.skipif(not URL, reason="ALELT_TEST_CLICKHOUSE_URL not set")
class TestClickHouseSingleWriter:
    """DST-clickhouse-01 live: no fence -> refused before anything is written; a second writer taking over the
    stream fences the first one out at its commit point; DST-clickhouse-02 crash after the commit point; -03 dates."""

    def _dest(self) -> Any:
        import uuid

        from alelt_dst_kit.conformance import INFO, KEYLESS

        s = f"aleltc_{uuid.uuid4().hex[:8]}"
        d = ClickHouseDestination(URL or "", schema=s, control_schema=f"{s}_ctl")
        d.bootstrap()
        d.ensure_table(INFO, "orders", None)
        d.ensure_table(KEYLESS, "evlog", None)
        return d, s

    def test_no_fence_refused_and_takeover_fences_out(self) -> None:
        from alelt_common.spi import WriterFenceError, release_writer
        from alelt_dst_kit.conformance import MAP, ROW1, ROW2, TABLES, cs, e, p

        d1, s = self._dest()
        release_writer("stream:*")
        with pytest.raises(WriterFenceError):
            d1.apply("sw", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
        assert d1.read_checkpoint("sw") is None and not d1.fetch_rows("orders")
        from alelt_common.spi import hold_writer

        hold_writer("pipeline:sw", lambda: True)
        d1.apply("sw", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
        d2 = ClickHouseDestination(URL or "", schema=s, control_schema=f"{s}_ctl")
        from alelt_dst_kit.conformance import INFO, KEYLESS

        d2.ensure_table(INFO, "orders", None)
        d2.ensure_table(KEYLESS, "evlog", None)

        def takeover(pt: str) -> None:     # d2 (new lock holder) claims the stream while d1 is staging
            if pt == "before_commit":
                d2._claim_writer("sw")
        d1._hook = takeover  # type: ignore[method-assign]
        with pytest.raises(WriterFenceError, match="fenced out"):
            d1.apply("sw", cs(e(4, "INSERT", after=ROW2)), p(2), "b2", MAP, TABLES)
        assert d1.read_checkpoint("sw").text == p(1).text  # type: ignore[union-attr]
        assert d2.apply("sw", cs(e(4, "INSERT", after=ROW2)), p(2), "b2", MAP, TABLES)
        release_writer("pipeline:sw")

    def test_crash_after_commit_point_rolls_forward_on_read(self) -> None:
        from alelt_dst_kit.conformance import MAP, ROW1, TABLES, cs, e, p

        d, s = self._dest()
        real = d._move
        d._move = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("crash after commit point"))  # type: ignore
        with pytest.raises(RuntimeError):
            d.apply("cr", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
        d._move = real  # type: ignore[method-assign]
        r = ClickHouseDestination(URL or "", schema=s, control_schema=f"{s}_ctl")
        assert r.read_checkpoint("cr").text == p(1).text  # type: ignore[union-attr]
        assert len(r.fetch_rows("orders")) == 1               # committed stage rows rolled forward on read

    def test_out_of_range_date_rejected_not_clamped(self) -> None:
        from alelt_dst_kit.conformance import MAP, ROW1, TABLES, cs, e, p

        d, _ = self._dest()
        bad = {**ROW1}
        col = next(k for k, v in ROW1.items() if isinstance(v, (date, datetime)))
        bad[col] = date(1800, 1, 1) if type(ROW1[col]) is date else datetime(1800, 1, 1, tzinfo=timezone.utc)
        with pytest.raises(ValueError):
            d.apply("dr", cs(e(1, "INSERT", after=bad)), p(1), "b1", MAP, TABLES)


@pytest.mark.skipif(not URL, reason="ALELT_TEST_CLICKHOUSE_URL not set")
class TestClickHouseVersions:
    def test_replay_stream_after_main_stream_wins_and_snapshot_ordering(self) -> None:
        """DST-clickhouse-08: a catch-up stream (own seq 1) writing after the main stream (seq 5) must win; a
        snapshot after both wins over them; the next CDC batch wins over the snapshot."""
        import uuid

        from alelt_dst_kit.conformance import INFO, KEYLESS, MAP, ROW1, TABLES, cs, e, p

        s = f"aleltc_{uuid.uuid4().hex[:8]}"
        d = ClickHouseDestination(URL or "", schema=s, control_schema=f"{s}_ctl")
        d.bootstrap()
        d.ensure_table(INFO, "orders", None)
        d.ensure_table(KEYLESS, "evlog", None)
        for i in range(1, 6):
            d.apply("m", cs(e(i, "INSERT" if i == 1 else "UPDATE", before=None if i == 1 else {"ID": 1},
                              after={**ROW1, "NAME": f"main{i}"})), p(i), f"b{i}", MAP, TABLES)
        d.apply("m#catchup:x", cs(e(1, "UPDATE", before={"ID": 1}, after={**ROW1, "NAME": "replay"})), p(1),
                "r1", MAP, TABLES)
        name = lambda: {str(r["name"]) for r in d.fetch_rows("orders")}  # noqa: E731
        assert name() == {"replay"}
        d.load_snapshot("orders", INFO, [{**ROW1, "NAME": "snap"}])
        assert name() == {"snap"}
        d.apply("m", cs(e(6, "UPDATE", before={"ID": 1}, after={**ROW1, "NAME": "after-snap"})), p(6), "b6",
                MAP, TABLES)
        assert name() == {"after-snap"}

    def test_stage_gets_columns_added_to_target(self) -> None:
        """DST-clickhouse-05: a target column added outside add_column (golden DDL) reaches the existing stage."""
        import uuid

        from alelt_dst_kit.conformance import INFO, KEYLESS, MAP, ROW1, ROW2, TABLES, cs, e, p

        s = f"aleltc_{uuid.uuid4().hex[:8]}"
        d = ClickHouseDestination(URL or "", schema=s, control_schema=f"{s}_ctl")
        d.bootstrap()
        d.ensure_table(INFO, "orders", None)
        d.ensure_table(KEYLESS, "evlog", None)
        d.apply("st", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
        d._cmd(f"ALTER TABLE {d.t('orders')} ADD COLUMN extra Nullable(String)")
        d.apply("st", cs(e(2, "INSERT", after=ROW2)), p(2), "b2", MAP, TABLES)
        assert len(d.fetch_rows("orders")) == 2


class _Capture:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def command(self, sql: str, **kw: Any) -> None:
        self.sql.append(sql)

    def query(self, sql: str, parameters: Any = None) -> Any:
        self.sql.append(sql)

        class R:
            column_names: list[str] = []
            result_rows: list[Any] = []
        return R()


def test_cluster_and_replicated_ddl_generation() -> None:
    """DST-clickhouse-04: ?cluster= adds ON CLUSTER to landing / control DDL, ?replicated=on uses Replicated*
    engines; per-writer stage tables stay local; default URL = unchanged single-node SQL."""
    from alelt_dst_kit.conformance import INFO

    cap = _Capture()
    d = ClickHouseDestination("clickhouse://h:8123/?cluster=c1&replicated=on", schema="land", control_schema="ctl",
                              client=cap)
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    ddl = [s for s in cap.sql if s.startswith(("CREATE", "ALTER"))]
    assert all(" ON CLUSTER `c1`" in s for s in ddl if "`stg_" not in s), ddl
    land = next(s for s in ddl if s.startswith("CREATE TABLE IF NOT EXISTS `land`.`orders`"))
    assert "ENGINE = ReplicatedReplacingMergeTree(_alelt_ver, _alelt_is_deleted)" in land
    assert any("ENGINE = ReplicatedReplacingMergeTree(seq)" in s for s in ddl)
    assert d.cluster_sql("CREATE TABLE IF NOT EXISTS `ctl`.`stg_x` AS `land`.`orders` ENGINE = MergeTree ORDER BY "
                         "tuple()").endswith("ENGINE = MergeTree ORDER BY tuple()")          # stage: local
    plain = ClickHouseDestination("clickhouse://h:8123/", schema="land", control_schema="ctl", client=_Capture())
    sql = "CREATE TABLE IF NOT EXISTS `land`.`t` (a Int32) ENGINE = ReplacingMergeTree(v) ORDER BY a"
    assert plain.cluster_sql(sql) == sql


def test_lookup_reads_newest_version_without_final() -> None:
    """DST-clickhouse-06: the per-batch lookup never uses FINAL; newest version per key, hard deletes dropped."""
    d = ClickHouseDestination("clickhouse://h:8123/", schema="land", control_schema="ctl", client=_Capture())
    sql = d.lookup_sql("orders", ["id", "name"], ["id"], "(1), (2)")
    assert " FINAL" not in sql and "ORDER BY _alelt_ver DESC LIMIT 1 BY `id`" in sql and "`id` IN ((1), (2))" in sql
    assert sql.split(" FROM ")[0].endswith("_alelt_is_deleted")
