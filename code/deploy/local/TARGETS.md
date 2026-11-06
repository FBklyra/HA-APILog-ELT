# Local destination targets

The local targets environment **is the test lab**: `lab/docker-compose.lab.yml` (profiles from `lab/catalog.yaml`, containers `alelt-lab-*`, ports `127.0.0.1:47xxx`). There is no separate targets compose file. See `lab/README.md`.

```powershell
.\lab\lab.ps1 up delta            # SeaweedFS S3 (bucket "lake")
.\lab\lab.ps1 up iceberg          # SeaweedFS + Iceberg REST catalog + Trino
.\lab\lab.ps1 up clickhouse redpanda bigquery landing
.\lab\lab.ps1 down                # stop everything (volumes kept; `reset` deletes them)
```

| Destination | Lab profile | `ALELT_DEST_URL` / test variable |
|---|---|---|
| delta | `delta` | `delta+s3://lake/<prefix>?endpoint=http://127.0.0.1:47900&region=us-east-1&access_key=lab&secret_key=lab_local_only&allow_http=1` (`ALELT_TEST_DELTA_S3_URL`) |
| iceberg | `iceberg` | `iceberg+rest://127.0.0.1:47181/?warehouse=s3://lake/iceberg&s3.endpoint=http://127.0.0.1:47900&s3.access-key-id=lab&s3.secret-access-key=lab_local_only&s3.path-style-access=true&s3.region=us-east-1` (`ALELT_TEST_ICEBERG_URL`); Trino read-back `http://127.0.0.1:47808` (catalog `iceberg`) |
| clickhouse | `clickhouse` | `clickhouse://lab:lab_local_only@127.0.0.1:47823/?secure=0` (`ALELT_TEST_CLICKHOUSE_URL`) |
| kafka | `redpanda` | `kafka://127.0.0.1:47092?partitions=1&replication=1` (`ALELT_TEST_KAFKA_URL`) |
| bigquery (emulator) | `bigquery` | `bigquery://lab-project/?endpoint=http://127.0.0.1:47050&emulator=1` (`ALELT_TEST_BIGQUERY_URL`; limits: `connectors/destinations/bigquery/README.md`) |
| postgres landing | `landing` | `postgresql://lab:lab_local_only@127.0.0.1:47442/landing` |
| redshift (stand-in) | — | `alelt-test-postgres` with `compat=postgres` (`ALELT_TEST_REDSHIFT_PG_URL`) |
| fabric (stand-in) | — | own `alelt-test-sqledge`, database `alelt_dst_fabric_test`, `flavor=sqlserver` (`ALELT_TEST_FABRIC_TSQL_URL`) |
| databricks, snowflake | — | cloud only (SQL generation tests locally; trials: console Local test lab guidance) |

End-to-end examples are coming in a later release.

Never point tests at containers that belong to other projects (`local-*`); use only `alelt-lab-*` / `alelt-test-*`.
