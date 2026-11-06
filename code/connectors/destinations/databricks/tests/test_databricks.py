"""alelt_dst_databricks: SQL generation tests (no local Databricks environment)."""
from __future__ import annotations

from typing import Any

from alelt_dst_databricks import DatabricksDestination, DatabricksDialect
from alelt_dst_kit import ControlState
from alelt_dst_kit.conformance import INFO, MAP, ROW1, ROW2, TABLES, cs, e, p
from alelt_dst_kit.sqlmerge import RecordingExecutor


def _d(url: str = "databricks://token:x@adb-1.azuredatabricks.net/?http_path=/sql/1.0/warehouses/w&catalog=main"
       ) -> tuple[Any, RecordingExecutor]:
    ex = RecordingExecutor({"LIMIT 0": (["id", "name", "amt", "d", "ts", "flag", "_alelt_synced", "_alelt_deleted"], [])})
    return DatabricksDestination(url, schema="land", executor=ex), ex


def test_ddl() -> None:
    d, ex = _d()
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    sql = [s for s, _ in ex.log]
    assert "CREATE SCHEMA IF NOT EXISTS `main`.`land`" in sql
    ddl = next(s for s in sql if s.startswith("CREATE TABLE IF NOT EXISTS `main`.`land`.`orders`"))
    assert "`ts` TIMESTAMP_NTZ" in ddl and "`amt` DECIMAL(9,2)" in ddl and "USING DELTA" in ddl
    assert "PRIMARY KEY" not in next(s for s in sql if "`checkpoint`" in s and s.startswith("CREATE TABLE"))


def test_idempotent_writes_per_statement() -> None:
    d, ex = _d()
    d.ensure_table(INFO, "orders", None)
    ex.log.clear()
    states = [ControlState(), ControlState(), ControlState(p(3).text, 1, "b1")]   # read, CAS pre-check, read-back
    d.control_state = lambda s: states.pop(0)  # type: ignore[method-assign]
    d.apply("strm", cs(e(1, "INSERT", after=ROW1), e(2, "INSERT", after=ROW2), e(3, "DELETE", before={"ID": 2})),
            p(3), "b1", MAP, TABLES)
    sql = [s for s, _ in ex.log]
    ck = next(s for s in sql if s.startswith("MERGE INTO `main`.`alelt_control`.`checkpoint`"))
    assert "WHEN MATCHED AND t.`seq` = 0 THEN UPDATE SET *" in ck and "WHEN NOT MATCHED THEN INSERT *" in ck
    writes = [i for i, s in enumerate(sql) if s.startswith(("MERGE", "INSERT", "UPDATE", "DELETE"))]
    for i in writes:                                           # every write preceded by its txn app id/version
        assert sql[i - 2] == "SET spark.databricks.delta.write.txnAppId = strm"
        assert sql[i - 1].startswith("SET spark.databricks.delta.write.txnVersion = 1000")
    versions = [int(s.rsplit("=", 1)[1]) for s in sql if "txnVersion =" in s]
    assert versions == sorted(versions) and len(set(versions)) == len(versions)
    assert sql[writes[-1]].startswith("MERGE INTO `main`.`alelt_control`.`checkpoint`")       # checkpoint last
    assert sql[-2:] == ["RESET spark.databricks.delta.write.txnAppId", "RESET spark.databricks.delta.write.txnVersion"]
    assert "CAST('1.50' AS DECIMAL(9,2))" in "\n".join(sql)


def test_literals() -> None:
    dl = DatabricksDialect()
    assert dl.string_lit("a'b\\") == "'a\\'b\\\\'" and dl.bytes_lit(b"\x0a") == "X'0a'"


def test_conflicting_writer_detected_by_read_back() -> None:
    import pytest

    from alelt_common.spi import CheckpointConflictError

    d, ex = _d()
    d.ensure_table(INFO, "orders", None)
    states = [ControlState(p(1).text, 1, "b1"), ControlState(p(1).text, 1, "b1"), ControlState(p(5).text, 2, "bX")]
    d.control_state = lambda s: states.pop(0)  # type: ignore[method-assign]
    with pytest.raises(CheckpointConflictError):
        d.apply("strm", cs(e(3, "INSERT", after=ROW1)), p(3), "b2", MAP, TABLES)
    ck = [s for s, _ in ex.log if s.startswith("MERGE INTO `main`.`alelt_control`.`checkpoint`")][-1]
    assert "WHEN MATCHED AND t.`seq` = 1 THEN UPDATE SET *" in ck and "NOT MATCHED" not in ck
    states[:] = [ControlState(p(1).text, 1, "b1"), ControlState(p(4).text, 2, "bY")]   # moved before any write
    n = len(ex.log)
    with pytest.raises(CheckpointConflictError):
        d.apply("strm", cs(e(5, "INSERT", after=ROW1)), p(5), "b2", MAP, TABLES)
    assert not any(s.startswith(("MERGE", "INSERT")) for s, _ in ex.log[n:])


def test_txn_app_id_is_parse_safe() -> None:
    from alelt_dst_databricks.destination import txn_app_id

    assert txn_app_id("strm") == "strm"
    a = txn_app_id("cid#catchup:S.T x")
    assert all(c.isalnum() or c in "_.-" for c in a) and a != txn_app_id("cid#catchup:S.T_x")


def test_inline_values_table_and_few_statements_for_many_rows() -> None:
    """DST-databricks-04: rows staged as ONE inline VALUES table per statement, chunked by size: 2 000 rows of one
    table = one MERGE (was 4 with 500-row UNION ALL sources)."""
    d, ex = _d()
    d.ensure_table(INFO, "orders", None)
    ex.log.clear()
    states = [ControlState(), ControlState(), ControlState(p(2001).text, 1, "b1")]
    d.control_state = lambda s: states.pop(0)  # type: ignore[method-assign]
    evs = [e(i, "INSERT", after={**ROW1, "ID": i}) for i in range(1, 2001)]
    d.apply("strm", cs(*evs), p(2001), "b1", MAP, TABLES)
    sql = [s for s, _ in ex.log]
    merges = [s for s in sql if s.startswith("MERGE INTO") and "`orders`" in s]
    assert len(merges) == 1
    assert "FROM VALUES (" in merges[0] and " UNION ALL " not in merges[0] and ") AS v(`id`" in merges[0]
