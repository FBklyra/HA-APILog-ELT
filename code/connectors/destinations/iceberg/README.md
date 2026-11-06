# alelt_dst_iceberg — Apache Iceberg destination

`alelt_dst_iceberg.destination:IcebergDestination` (engine name `iceberg`). Client: `pyiceberg` (Apache-2.0) + `pyarrow`.

| Area | Detail |
|---|---|
| URL (`ALELT_DEST_URL`) | REST catalog: `iceberg+rest://catalog:8181/?warehouse=s3://bucket/wh&s3.endpoint=http://host:port&s3.access-key-id=${K}&s3.secret-access-key=${S}&s3.path-style-access=true` (`https=1` for TLS). Local: `iceberg+sql:///C:/lake?warehouse=file:///C:/lake/wh` (SQLite catalog). Every query parameter except the plugin's own (`schema, control_schema, delete_mode, output_profile, case, https`) is passed to the catalog as a property (ADLS `adls.*`, GCS `gcs.*`). |
| Layout | namespace `<schema>` = landing tables; namespace `<control>` = `checkpoint`, `audit_batch`. Types: `config/type_maps/canonical_to_iceberg.yaml`. |
| Exactly-once | one pyiceberg transaction per table = ONE snapshot with the copy-on-write upsert AND the table property `alelt.checkpoint.<stream>` = {seq, position, batch_id}; the snapshot summary carries `alelt.stream / alelt.position / alelt.batch_id / alelt.seq`. A table already holding this batch is skipped; one written by another writer for this seq rejects the batch. Compare-and-set (F-05): checkpoint re-read before the first table commit and at the control snapshot of the final commit; catalog optimistic concurrency (`CommitFailedException`) -> `CheckpointConflictError`. The stream checkpoint (+ watermarks) commits last. |
| Deletes / PK update | soft (default) or `delete_mode=hard`; PK update = delete old + upsert new. |
| S3-compatible stores | with an explicit `s3.endpoint` the plugin defaults `AWS_REQUEST_CHECKSUM_CALCULATION` / `AWS_RESPONSE_CHECKSUM_VALIDATION` to `WHEN_REQUIRED` (unless already set): recent AWS SDKs send aws-chunked checksum trailers that some stores (SeaweedFS 3.85, older MinIO) save verbatim, corrupting metadata/Avro files. The REST catalog server needs the same two variables (set in `lab/docker-compose.lab.yml`). |
| Limits | No multi-table commit in pyiceberg: per-table atomicity, the retry completes the rest exactly once. Copy-on-write rewrites the affected data files — fine for CDC micro-batches; compact periodically. |
| Readers | Trino / Spark / Snowflake / DuckDB through the same catalog (lab: `lab up iceberg` = SeaweedFS + REST catalog + Trino). |

Verified: conformance on SQLite catalog (local + S3 moto) and on the lab REST catalog + SeaweedFS (31 passed).

Tests: `pytest connectors/destinations/iceberg/tests`; lab: `ALELT_TEST_ICEBERG_URL=iceberg+rest://127.0.0.1:47181/?warehouse=s3://lake/iceberg&s3.endpoint=http://127.0.0.1:47900&s3.access-key-id=lab&s3.secret-access-key=lab_local_only&s3.path-style-access=true&s3.region=us-east-1`.
