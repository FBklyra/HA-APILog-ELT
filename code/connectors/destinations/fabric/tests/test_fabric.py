"""alelt_dst_fabric: T-SQL generation tests (always) + conformance against SQL Server / Azure SQL Edge as a T-SQL
APPROXIMATION of Fabric (ALELT_TEST_FABRIC_TSQL_URL=fabric://sa:<pw>@localhost:51433/<own test db>?flavor=sqlserver
&encrypt=no&trust_server_certificate=yes - the database must exist and be dedicated to the test)."""
from __future__ import annotations

import os
from typing import Any

import pytest

from alelt_dst_fabric import FabricDestination
from alelt_dst_kit.conformance import DestinationConformance, INFO, MAP, ROW1, ROW2, TABLES, cs, e, p
from alelt_dst_kit.sqlmerge import RecordingExecutor


def _rec(url: str = "fabric://u:p@ws.datawarehouse.fabric.microsoft.com/wh") -> tuple[Any, RecordingExecutor]:
    ex = RecordingExecutor()
    return FabricDestination(url, schema="land", executor=ex), ex


def test_ddl_fabric() -> None:
    d, ex = _rec()
    d.bootstrap()
    d.ensure_table(INFO, "orders", None)
    sql = [s for s, _ in ex.log]
    assert "IF SCHEMA_ID(N'land') IS NULL EXEC(N'CREATE SCHEMA [land]')" in sql
    ddl = next(s for s in sql if "CREATE TABLE [land].[orders]" in s)
    assert "[name] varchar(80)" in ddl and "[amt] decimal(9,2)" in ddl and "[_alelt_deleted] bit NOT NULL" in ddl
    assert "[_alelt_synced] datetime2(6) NOT NULL" in ddl and "PRIMARY KEY NONCLUSTERED ([id]) NOT ENFORCED" in ddl


def test_apply_tsql() -> None:
    d, ex = _rec()
    d.ensure_table(INFO, "orders", None)
    d.apply("s", cs(e(1, "INSERT", after=ROW1), e(2, "INSERT", after=ROW2), e(3, "DELETE", before={"ID": 2})),
            p(3), "b1", MAP, TABLES)
    tx = ex.committed[-1]
    sql = [s for s, _ in tx]
    assert not any("TABLOCKX" in s for s in sql)                          # fabric: snapshot isolation, no hints
    m = [s for s in sql if s.startswith("MERGE INTO")]
    assert m and all(s.endswith(";") for s in m) and "USING #alelt_stage_" in m[0]
    assert any(s.startswith("CREATE TABLE #alelt_stage_") for s in sql)
    rows = next(r for s, r in tx if s.startswith("INSERT INTO #alelt_stage_"))
    assert all(x[-1].tzinfo is None for x in rows)                        # datetime2 = naive UTC


def test_sqlserver_flavor_locks() -> None:
    d, ex = _rec("fabric://u:p@h/db?flavor=sqlserver")
    d.ensure_table(INFO, "orders", None)
    d.apply("s", cs(e(1, "INSERT", after=ROW1)), p(1), "b1", MAP, TABLES)
    assert ex.committed[-1][0][0] == "SELECT COUNT(*) FROM [alelt_control].[checkpoint] WITH (TABLOCKX, HOLDLOCK)"


URL = os.environ.get("ALELT_TEST_FABRIC_TSQL_URL")


@pytest.mark.skipif(not URL, reason="ALELT_TEST_FABRIC_TSQL_URL not set")
class TestFabricOnSqlEdgeConformance(DestinationConformance):
    def make(self, schema: str, delete_mode: str = "soft") -> Any:
        return FabricDestination(URL or "", schema=schema, control_schema=f"{schema}_ctl", delete_mode=delete_mode)
