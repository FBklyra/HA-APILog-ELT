# alelt_dst_delta — Delta Lake destination

`alelt_dst_delta.destination:DeltaDestination` (engine name `delta`). Client: `deltalake` (delta-rs, Apache-2.0) + `pyarrow`. No Spark needed.

| Area | Detail |
|---|---|
| URL (`ALELT_DEST_URL`) | `delta+file:///C:/lake` · `delta+s3://bucket/prefix?endpoint=http://host:port&region=us-east-1&access_key=${K}&secret_key=${S}&allow_http=1` · `delta+az://container/prefix?opt.azure_storage_account_name=…` · `delta+gs://bucket/prefix?opt.google_service_account=…`. `opt.<KEY>` is passed to delta-rs `storage_options` as-is. |
| Layout | `<base>/<schema>/<table>` (landing), `<base>/<control>/checkpoint`, `<base>/<control>/audit_batch`. Types: `config/type_maps/canonical_to_delta.yaml`. |
| Exactly-once | every table commit carries an application transaction `txn{appId=<stream>, version=<seq>}` + commitInfo `alelt.stream / alelt.position / alelt.batch_id / alelt.seq` in the SAME commit as the MERGE. A table already holding this batch (seq + position) is skipped (crash after it); one written by another writer for this seq rejects the batch. Compare-and-set (F-05): checkpoint re-read before the first table commit and at the control-table version the final MERGE reads; delta-rs optimistic concurrency (`CommitFailedError`) -> `CheckpointConflictError`. On S3 the log commit uses conditional PUT by default (`unsafe_rename=1` = explicit single-writer opt-in). The stream checkpoint (+ watermarks) commits last. |
| Concurrent writers | on S3 set a commit coordination option (`opt.conditional_put=etag`); `opt.AWS_S3_ALLOW_UNSAFE_RENAME=true` (single writer) is the default when an explicit endpoint is given (local S3 tests). |
| Deletes / PK update | soft (default, `<deleted>` flag) or `delete_mode=hard`; PK update = delete old key + upsert new key (`alelt_dst_kit.plan`). |
| Limits | No multi-table commit in Delta: after a crash each table is fully old or fully new; the retry finishes the rest exactly once. |
| Readers | DuckDB (`delta` extension), Spark, Databricks, Trino (Delta connector). |

Verified: conformance on a local dir and S3 (moto).

Tests: `pytest connectors/destinations/delta/tests`; lab S3: `ALELT_TEST_DELTA_S3_URL=delta+s3://lake/t?endpoint=http://127.0.0.1:47900&region=us-east-1&access_key=lab&secret_key=lab_local_only&allow_http=1`.
