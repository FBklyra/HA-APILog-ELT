# alelt_dst_fabric — Microsoft Fabric Warehouse / Azure Synapse destination (T-SQL)

`alelt_dst_fabric.destination:FabricDestination` (engine name `fabric`). Client: `pyodbc` (MIT) + Microsoft ODBC Driver 18 for SQL Server.

| Area | Detail |
|---|---|
| URL (`ALELT_DEST_URL`) | `fabric://<client-id>:${ALELT_FABRIC_SECRET}@<endpoint>.datawarehouse.fabric.microsoft.com:1433/<warehouse>?authentication=ActiveDirectoryServicePrincipal&flavor=fabric`; `flavor = fabric` (default) / `synapse` / `sqlserver`; `driver`, `encrypt`, `trust_server_certificate`. |
| Transaction (R6) | one T-SQL transaction per batch: [checkpoint lock: synapse/sqlserver UPDLOCK+HOLDLOCK; Fabric uses snapshot isolation — a concurrent conflicting commit fails] → per table: [truncate] → per op group: #temp table + fast executemany → MERGE or DELETE … WHERE EXISTS → checkpoint DELETE+INSERT → audit → COMMIT. Errors ⇒ ROLLBACK. |
| Fabric specifics | varchar (UTF-8 collation) instead of nvarchar, datetime2(6) for instants (UTC), primary keys NONCLUSTERED NOT ENFORCED; MERGE and #temp tables need a current Fabric Warehouse release. Types: `config/type_maps/canonical_to_fabric.yaml`. |
| Local stand-in | `flavor=sqlserver` on our own `alelt-test-sqledge` (Azure SQL Edge), database `alelt_dst_fabric_test` — a T-SQL approximation only; Fabric-only behaviour (snapshot-isolation conflicts, Entra ID auth, NOT ENFORCED keys) is NOT tested locally. Never use another project's SQL Server container. |

Verified: T-SQL generation unit tests; conformance. Not run against a real Fabric workspace.

Tests: `pytest connectors/destinations/fabric/tests` with `ALELT_TEST_FABRIC_TSQL_URL=fabric://sa:<pw>@127.0.0.1:51433/alelt_dst_fabric_test?flavor=sqlserver&encrypt=no&trust_server_certificate=yes` (the database must exist and be dedicated to tests).
