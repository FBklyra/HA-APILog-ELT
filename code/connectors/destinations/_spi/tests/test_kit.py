"""alelt_dst_kit: planning, type maps, urls, registry + the conformance suite against the in-memory reference."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from alelt_common.models import CanonicalType, ColumnInfo
from alelt_dst_kit import parse_url, physical_type, plan_batch, type_map
from alelt_dst_kit.conformance import INFO, MAP, ROW1, TABLES, DestinationConformance, cs, e, p
from alelt_dst_kit.memory import MemoryDestination
from alelt_dst_kit.registry import DESTINATIONS, destination_ref
from alelt_loader.policy import system_columns


class TestMemoryConformance(DestinationConformance):
    def make(self, schema: str, delete_mode: str = "soft") -> Any:
        return MemoryDestination("memory://conf", schema=schema, delete_mode=delete_mode)


def test_plan_pk_update_and_partial_merge() -> None:
    plans = plan_batch(cs(e(1, "INSERT", after=ROW1), e(2, "UPDATE", before={"ID": 1}, after={"ID": 1, "NAME": "q"}),
                          e(3, "UPDATE", before={"ID": 1}, after={"ID": 5, "NAME": "r"})),
                       MAP, TABLES, system_columns(None))
    ops = list(plans["orders"].ops.values())
    assert [(o.op, o.key) for o in ops] == [("D", (1,)), ("U", (5,))]
    assert ops[1].partial


def test_plan_truncate_resets_and_keyless() -> None:
    plans = plan_batch(cs(e(1, "INSERT", after=ROW1), e(2, "TRUNCATE")), MAP, TABLES, system_columns(None))
    assert plans["orders"].truncate and not plans["orders"].ops


def test_plan_missing_key() -> None:
    with pytest.raises(ValueError):
        plan_batch(cs(e(1, "INSERT", after={"NAME": "x"})), MAP, TABLES, system_columns(None))


def test_type_map_placeholders() -> None:
    tm = type_map("nosuch", {"types": {"DECIMAL": "numeric({p},{s})", "STRING": "varchar({n})"},
                             "defaults": {"STRING_without_length": "text"}, "limits": {"STRING_max_length": 100}})
    c = ColumnInfo(name="a", ordinal=0, engine_type="x", canonical_type=CanonicalType.DECIMAL, length=50, scale=60)
    assert physical_type(c, tm) == "numeric(38,38)"
    s = ColumnInfo(name="s", ordinal=0, engine_type="x", canonical_type=CanonicalType.STRING, length=500)
    assert physical_type(s, tm) == "text"
    u = ColumnInfo(name="u", ordinal=0, engine_type="x", canonical_type=CanonicalType.UNKNOWN)
    with pytest.raises(ValueError):
        physical_type(u, tm)


def test_url_env_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALELT_X_PW", "s3cr@t")
    u = parse_url("clickhouse://alelt:${ALELT_X_PW}@h:8123/db?secure=1")
    assert (u.user, u.password, u.host, u.port, u.path, u.flag("secure")) == ("alelt", "s3cr@t", "h", 8123, "/db", True)
    k = parse_url("kafka://a:9092,b:9093?topic_prefix=alelt")
    assert k.hosts == "a:9092,b:9093" and k.host == "a" and k.port == 9092


def test_registry_names() -> None:
    assert {"iceberg", "delta", "clickhouse", "kafka", "bigquery", "databricks", "redshift", "fabric"} <= set(DESTINATIONS)
    assert destination_ref("clickhouse").endswith(":ClickHouseDestination")
    with pytest.raises(ValueError):
        destination_ref("nope")


def test_memory_stats_existence() -> None:
    d = MemoryDestination("memory://stats", schema="x")
    d.ensure_table(INFO, "orders", None)
    st = d.apply("s", cs(e(1, "INSERT", after=ROW1)), e(1, "INSERT").position, "b", MAP, TABLES)
    assert st[0].inserts == 1
    st = d.apply("s", cs(e(2, "INSERT", after={**ROW1, "AMT": Decimal("9")})), e(2, "INSERT").position, "b2",
                 MAP, TABLES)
    assert st[0].updates == 1


# ------------------------------------------------------------------ snapshot JSON values (same forms as CDC)
def _jinfo(keyless: bool = False) -> Any:
    from alelt_common.models import TableInfo, TableRef

    return TableInfo(table=TableRef(schema="S", name="DOCS"), columns=[
        ColumnInfo(name="ID", ordinal=0, engine_type="INT", canonical_type=CanonicalType.INT,
                   key_seq=None if keyless else 1),
        ColumnInfo(name="J", ordinal=1, engine_type="JSON", canonical_type=CanonicalType.JSON)])


@pytest.mark.parametrize("keyless", [False, True])
def test_snapshot_json_dict_list_like_cdc(keyless: bool) -> None:
    """KitDestination.load_snapshot (lake / warehouse / streaming plugins): JSON dict / list -> the JSON text the CDC
    path carries; keyless surrogate ids therefore match between snapshot and CDC rows."""
    from alelt_common.models import ChangeEvent, ChangeKind, ChangeSet, Position

    info = _jinfo(keyless)
    d = MemoryDestination("memory://snapj", schema=f"snapj{int(keyless)}")
    d.bootstrap()
    d.ensure_table(info, "docs", None)
    assert d.load_snapshot("docs", info, [{"ID": 1, "J": {"a": [1, None]}}, {"ID": 2, "J": [1, "x"]}]) == 2
    pos = Position(engine="db2", text="00000009")
    d.apply("s", [ChangeSet(txid="c", commit_position=pos, commit_ts=None, events=[ChangeEvent(
        position=pos, txid="c", kind=ChangeKind.INSERT, table=info.table, after={"ID": 1, "J": '{"a":[1,null]}'})])],
        pos, "b", {info.table: "docs"}, {info.table: info})
    rows = sorted(d.fetch_rows("docs"), key=lambda r: (r["id"], r["j"]))
    assert [(r["id"], r["j"]) for r in rows] == [(1, '{"a":[1,null]}'), (2, '[1,"x"]')]   # CDC row = same row
    with pytest.raises(ValueError):
        d.load_snapshot("docs", info, [{"ID": {"x": 1}, "J": None}])


def test_snapshot_json_kafka_message_like_cdc() -> None:
    import json

    from alelt_dst_kit.registry import destination_ref as ref

    ref("kafka")                                          # plugin folder on sys.path
    from alelt_dst_kafka import KafkaDestination, MemoryBus

    bus = MemoryBus()
    info = _jinfo()
    d = KafkaDestination("kafka://m:9092?topic_prefix=j", schema="s", bus=bus)
    d.bootstrap()
    d.ensure_table(info, "docs", None)
    d.load_snapshot("docs", info, [{"ID": 1, "J": {"a": 1}}, {"ID": 2, "J": [1]}])
    vals = [json.loads(v)["row"]["j"] for _, v in bus.topics["j.s.docs"]]
    assert vals == ['{"a":1}', "[1]"]


def test_canonical_snapshot_row() -> None:
    from datetime import datetime

    from alelt_common.canonical_jsonl import Undecodable, canonical_snapshot_row

    t = {"J": CanonicalType.JSON, "X": CanonicalType.XML, "TS": CanonicalType.LOCALDATETIME, "S": CanonicalType.STRING}
    out = canonical_snapshot_row({"J": {"$dec": "1"}, "X": "<a/>", "TS": {"$ldt": "2026-01-02T03:04:05"},
                                  "S": [1], "EXTRA": {"k": 1}}, t)
    assert out["J"] == '{"$dec":"1"}'                     # JSON column: never tag-decoded, always text
    assert out["TS"] == datetime(2026, 1, 2, 3, 4, 5) and out["X"] == "<a/>"
    assert isinstance(out["S"], Undecodable) and out["EXTRA"] == {"k": 1}
    assert canonical_snapshot_row({"J": None, "TS": None}, t) == {"J": None, "TS": None}
    with pytest.raises(ValueError):
        canonical_snapshot_row({"TS": {"$nope": "x"}}, t, strict=True)
    assert isinstance(canonical_snapshot_row({"TS": {"$ldt": "x"}}, t, decode_tags=False)["TS"], Undecodable)


# ------------------------------------------------------------------ one registry (worker, wizard, lab)
def test_registry_single_source() -> None:
    import json
    import os
    from pathlib import Path

    from alelt_dst_kit import registry

    assert registry.DESTINATIONS["sql"][0] == "alelt_dst_sql:SqlDestination"
    assert registry.engines()[:3] == ["postgres", "sql", "snowflake"]
    assert set(registry.LABELS) == set(registry.DESTINATIONS)
    for eng, (ref, folder) in registry.DESTINATIONS.items():
        pkg = ref.split(":")[0].split(".")[0]
        assert (registry.plugin_dir(eng) / pkg).is_dir(), (eng, folder)       # type: ignore[operator]
    cat = {x["engine"]: x for x in registry.catalog()}
    assert cat["delta"]["plugin"].endswith(":DeltaDestination")
    os.environ["ALELT_DESTINATIONS"] = json.dumps({"mine": "x.y:Z", "delta": "a.b:C"})
    try:
        assert registry.engines()[-1] == "mine" and registry.destination_ref("delta") == "a.b:C"
    finally:
        del os.environ["ALELT_DESTINATIONS"]
    # the hub keeps no own engine map (single source of truth)
    hub = Path(registry.__file__).resolve().parents[4] / "hub" / "alelt_hub" / "pipeline.py"
    assert "alelt_dst_postgres.connector:PostgresDestination" not in hub.read_text(encoding="utf-8")


def test_alelt_verify_destination_side_works_on_every_kit_destination() -> None:
    """alelt_verify.DestinationSide passes schema/profile/synced_before keywords: no TypeError (R5 of the review)."""
    from alelt_verify.runner import DestinationSide

    d = MemoryDestination("memory://side", schema="side")
    d.ensure_table(INFO, "orders", None)
    d.apply("s", cs(e(1, "INSERT", after=ROW1), e(2, "INSERT", after={**ROW1, "ID": 2})), p(2), "b", MAP, TABLES)
    side = DestinationSide(d, schema="side", profile=None)
    assert side.row_count("orders") == 2
    assert side.key_buckets("orders", ["ID"], 4)
    assert set(side.column_checksums("orders", ["ID"], ["NAME"], 4)) == {"NAME"}
    assert side.deleted_keys("orders", ["ID"]) == []
    assert set(side.sample_rows("orders", ["ID"], ["NAME"], [(1,)])) == {(1,)}


def test_complete_rows_refuses_null_fill_on_lookup_miss() -> None:
    from alelt_dst_kit.plan import complete_rows

    plans = plan_batch(cs(e(2, "UPDATE", before={"ID": 1}, after={"ID": 1, "NAME": "q"})), MAP, TABLES,
                       system_columns(None))
    with pytest.raises(ValueError, match="lookup miss"):
        complete_rows(plans["orders"], {})


def test_keyless_partial_before_image_rejected() -> None:
    from alelt_dst_kit.conformance import K

    with pytest.raises(ValueError, match="keyless_partial_image"):
        plan_batch(cs(e(1, "DELETE", before={"MSG": "x"}, table=K)), MAP, TABLES, system_columns(None))


def test_sqlmerge_rows_by_table_unbounded() -> None:
    from alelt_dst_kit.sqlmerge import Dialect, RecordingExecutor, SqlMergeDestination

    class D(Dialect):
        type_engine = "nosuch"
        fallback = {"types": {"STRING": "VARCHAR({n})", "LONG": "BIGINT", "INSTANT": "TIMESTAMP"},
                    "defaults": {"STRING_without_length": "VARCHAR(65535)"}}
    d = SqlMergeDestination("x://h/db", executor=RecordingExecutor(), dialect=D())
    audit = next(s for s in d.control_ddl() if "audit_batch" in s)
    assert "(4000)" not in audit


def test_decimal_cap_keeps_integer_digits() -> None:
    """DST-kit-07 / DST-sql-02: DECIMAL(50,30) on a 38-digit engine keeps its 20 integer digits (scale gives way)."""
    from alelt_dst_kit.types import cap_decimal

    assert cap_decimal(50, 30, 38) == (38, 18)
    assert cap_decimal(10, 2, 38) == (10, 2)
    assert cap_decimal(45, 0, 38) == (38, 0)
    tm = type_map("nosuch", {"types": {"DECIMAL": "numeric({p},{s})"}, "limits": {"DECIMAL_max_precision": 38}})
    c = ColumnInfo(name="a", ordinal=0, engine_type="x", canonical_type=CanonicalType.DECIMAL, length=50, scale=30)
    assert physical_type(c, tm) == "numeric(38,18)"


def test_url_unset_env_secret_is_an_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    """DST-kit-08: an unset ${ENV} fails at parse time with the variable name, never an empty password; *_FILE works."""
    from alelt_dst_kit.urls import UnresolvedSecretError

    monkeypatch.delenv("ALELT_NOPE_PW", raising=False)
    monkeypatch.delenv("ALELT_NOPE_PW_FILE", raising=False)
    with pytest.raises(UnresolvedSecretError, match="ALELT_NOPE_PW"):
        parse_url("clickhouse://alelt:${ALELT_NOPE_PW}@h:8123/db")
    f = tmp_path / "pw"
    f.write_text("fromfile\n", encoding="utf-8")
    monkeypatch.setenv("ALELT_NOPE_PW_FILE", str(f))
    assert parse_url("clickhouse://alelt:${ALELT_NOPE_PW}@h:8123/db").password == "fromfile"


def test_sqlmerge_literals_float_specials_and_strict_json() -> None:
    """DST-kit-14: NaN / inf are typed text literals (valid SQL); unknown objects are refused, not str()'ed."""
    from alelt_dst_kit.sqlmerge import Dialect

    d = Dialect()
    assert d.lit(float("nan"), "DOUBLE") == "CAST('NaN' AS DOUBLE)"
    assert d.lit(float("-inf"), "DOUBLE") == "CAST('-Infinity' AS DOUBLE)"
    assert d.lit({"a": Decimal("1.5")}, "JSON") == "CAST('{\"a\":\"1.5\"}' AS JSON)"
    with pytest.raises(TypeError):
        d.lit({"a": object()}, "JSON")


def test_dbapi_executor_reconnects_after_a_broken_connection() -> None:
    """DST-kit-11: a disconnect drops the cached connection; the next transaction opens a new one."""
    from alelt_dst_kit.sqlmerge import DbapiExecutor

    class OperationalError(Exception):
        pass

    made: list[Any] = []

    class Conn:
        def __init__(self) -> None:
            self.closed = 0

        def cursor(self) -> Any:
            class C:
                def execute(self, *a: Any) -> None:
                    if len(made) == 1:
                        raise OperationalError("server closed the connection unexpectedly")
                rowcount = 0
                description = None

                def close(self) -> None:
                    pass
            return C()

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

        def close(self) -> None:
            pass

    def connect() -> Any:
        made.append(Conn())
        return made[-1]

    ex = DbapiExecutor(connect)
    with pytest.raises(OperationalError):
        with ex.transaction() as t:
            t.execute("SELECT 1")
    with ex.transaction() as t:
        t.execute("SELECT 1")
    assert len(made) == 2


def test_sqlmerge_add_column_with_upper_case_idents() -> None:
    """DST-kit-12: case=upper - an existing column (reported lower-case by information_schema) is not re-added."""
    from alelt_dst_kit.sqlmerge import Dialect, RecordingExecutor, SqlMergeDestination

    class D(Dialect):
        type_engine = "nosuch"
        fallback = {"types": {"STRING": "VARCHAR({n})", "LONG": "BIGINT", "INSTANT": "TIMESTAMP"},
                    "defaults": {"STRING_without_length": "VARCHAR(65535)"}}
    ex = RecordingExecutor()
    d = SqlMergeDestination("x://h/db?case=upper", executor=ex, dialect=D())
    d._existing = lambda dest: {"name"}  # type: ignore[method-assign]
    col = ColumnInfo(name="name", ordinal=0, engine_type="x", canonical_type=CanonicalType.STRING, length=5)
    n = len(ex.log)
    d.add_column("orders", col)
    assert len(ex.log) == n
    d._existing = lambda dest: set()  # type: ignore[method-assign]
    d.add_column("orders", col)
    assert len(ex.log) == n + 1 and "ADD" in ex.log[-1][0].upper()


def test_connection_id_schema_made_safe_per_engine() -> None:
    """DST-loader-05: the default landing schema (= connection id, e.g. 'erp-prod') is mapped to a name the
    engine accepts (BigQuery datasets [A-Za-z0-9_], Kafka topics [A-Za-z0-9._-])."""
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    for sub in ("bigquery", "kafka"):
        sys.path.insert(0, str(root / sub))
    from alelt_dst_bigquery import BigQueryDestination
    from alelt_dst_kafka import KafkaDestination, MemoryBus
    from alelt_dst_kit.sqlmerge import RecordingExecutor

    bq = BigQueryDestination("bigquery://p/?emulator=1", schema="erp-prod", executor=RecordingExecutor())
    assert bq.schema == "erp_prod"
    k = KafkaDestination("kafka://m:9092", schema="erp main:prod", bus=MemoryBus())
    assert k.schema == "erp_main_prod"
    assert MemoryDestination(schema="erp-prod").schema == "erp-prod"   # quoting engines: unchanged


def test_snapshot_chunks_skip_the_current_row_lookup() -> None:
    """DST-kit-06: snapshot rows are full images - engines that rewrite whole rows do not look up the chunk's keys."""
    from alelt_dst_kit.conformance import ROW2

    calls: list[int] = []

    class Full(MemoryDestination):
        needs_full_rows = True

        def lookup(self, plan: Any, keys: list[Any]) -> dict[Any, Any]:
            calls.append(len(keys))
            return super().lookup(plan, keys)

    d = Full(schema="snap")
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    assert d.load_snapshot("orders", INFO, [{**ROW1}, {**ROW2}]) == 2 and calls == []
    d.apply("s", cs(e(1, "UPDATE", before={"ID": 1}, after={"ID": 1, "NAME": "x"})), p(1), "b1", MAP, TABLES)
    assert calls == [1]                                    # CDC partial images still completed


def test_byte_sized_varchar_engines_reserve_utf8_bytes_per_character() -> None:
    """DST-redshift-03 / DST-fabric-04: VARCHAR(n) counts bytes on Redshift / Fabric; a source VARCHAR(20) of
    characters needs 80 bytes in UTF-8; above the engine maximum the over-max type is used."""
    import importlib
    import sys
    from pathlib import Path

    for pkg in ("redshift", "fabric"):
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / pkg))
    col = ColumnInfo(name="C", ordinal=0, engine_type="VARCHAR", canonical_type=CanonicalType.STRING, length=20)
    big = ColumnInfo(name="C", ordinal=0, engine_type="VARCHAR", canonical_type=CanonicalType.STRING, length=5000)
    rs = importlib.import_module("alelt_dst_redshift.destination")
    fb = importlib.import_module("alelt_dst_fabric.destination")
    rmap = type_map("redshift", rs._FALLBACK) if hasattr(rs, "_FALLBACK") else None
    fmap = type_map("fabric", fb._FALLBACK)
    assert physical_type(col, fmap).lower() == "varchar(80)"
    assert physical_type(big, fmap).lower() == "varchar(max)"          # 20 000 bytes > 8000
    if rmap is not None:
        assert physical_type(col, rmap).upper() == "VARCHAR(80)"
        assert physical_type(big, rmap).upper() == "VARCHAR(20000)"
    plain = {"types": {"STRING": "varchar({n})"}, "limits": {"STRING_max_length": 4000}}
    assert physical_type(col, plain) == "varchar(20)"                   # character-sized engines unchanged


def test_fabric_temp_tables_use_the_database_collation_off_fabric() -> None:
    import importlib
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "fabric"))

    fb = importlib.import_module("alelt_dst_fabric.destination")
    assert fb._collate_db_default('"N" varchar(80) NULL') == '"N" varchar(80) COLLATE DATABASE_DEFAULT NULL'
    assert fb._collate_db_default('"I" int') == '"I" int'
    stmts, _ = fb.TsqlDialect("sqlserver").temp_table("t", ['"N" varchar(8)'])
    assert "COLLATE DATABASE_DEFAULT" in stmts[1]
    stmts, _ = fb.TsqlDialect("fabric").temp_table("t", ['"N" varchar(8)'])
    assert "COLLATE" not in stmts[1]


def test_sub_microsecond_timestamp_text_refused_never_truncated() -> None:
    """DST-iceberg-06 / XC-36: timestamp text with digits below the microsecond is refused (every engine stores
    microseconds); zero padding beyond 6 digits is exact and accepted."""
    from datetime import datetime, timezone

    from alelt_dst_kit.arrowutil import SubMicrosecondError, parse_timestamp_text

    assert parse_timestamp_text("2026-01-02 03:04:05.123456000000") == datetime(2026, 1, 2, 3, 4, 5, 123456)
    assert parse_timestamp_text("2026-01-02T03:04:05.1+00:00") == datetime(2026, 1, 2, 3, 4, 5, 100000,
                                                                            tzinfo=timezone.utc)
    with pytest.raises(SubMicrosecondError):
        parse_timestamp_text("2026-01-02 03:04:05.123456789")
