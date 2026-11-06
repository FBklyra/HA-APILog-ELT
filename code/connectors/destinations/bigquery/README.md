# alelt_dst_bigquery — Google BigQuery destination

`alelt_dst_bigquery.destination:BigQueryDestination` (engine name `bigquery`). Client: `google-cloud-bigquery` (Apache-2.0).

| Area | Detail |
|---|---|
| URL (`ALELT_DEST_URL`) | `bigquery://<project>/?location=EU` (Application Default Credentials) · `credentials_file=<service-account json>` · emulator: `bigquery://<project>/?endpoint=http://127.0.0.1:47050&emulator=1`. `timeout=<s>` per script (default 600). |
| Datasets | `<schema>` (landing), `<control>` (`checkpoint`, `audit_batch`). Types: `config/type_maps/canonical_to_bigquery.yaml`. |
| Exactly-once (R6) | ONE multi-statement transaction (script) per batch: `BEGIN TRANSACTION; [truncate]; MERGE … USING (inline rows); ASSERT checkpoint row = planned base (compare-and-set, F-05); conditional UPDATE/INSERT checkpoint (+ watermarks); INSERT audit; COMMIT TRANSACTION;` — all or nothing; the checkpoint is also pre-checked before the script, and read back in emulator mode (the emulator does not enforce ASSERT; it also panics after a few hundred jobs — restart it between suites). Values are typed literals (no load job); batches are chunked under the query size limit. |
| Deletes / PK update | soft (default) or `delete_mode=hard` (DELETE … WHERE EXISTS); PK update = delete old + upsert new. |

## Emulator mode (`emulator=1`, local tests only)

`ghcr.io/goccy/bigquery-emulator` (lab profile `bigquery`) differs from BigQuery; the plugin adapts:

| Emulator behaviour | Plugin in emulator mode |
|---|---|
| no `PRIMARY KEY … NOT ENFORCED` | omitted |
| datasets only through the API; existing dataset → HTTP 500 (not 409), which the client retries for minutes | `get_dataset` first, create without retry |
| SQL errors returned as HTTP 500 → client retries | queries sent with `retry=None, job_retry=None` so errors surface |
| `MERGE` source must be a single table (no inline `SELECT … UNION ALL`) | inline rows go to a script `CREATE TEMP TABLE` (no explicit DROP: it fails in the emulator) |

Known emulator limits (not plugin defects): a failing statement inside `BEGIN … COMMIT` does **not** roll back earlier statements (no real atomicity — the plugin's crash tests pass only because a crashed batch never sends its script); `DELETE … WHERE EXISTS (correlated subquery)` deletes nothing (hard-delete mode fails); the emulator process can crash (Go panic) or hang after many scripts. The test suite handles this itself (`tests/conftest.py`): it restarts the container `alelt-lab-bigquery` (`ALELT_TEST_BIGQUERY_CONTAINER`) before every group of 4 tests (`ALELT_TEST_BIGQUERY_GROUP`) and after a test during which the emulator died; such a test and the hard-delete test are reported as xfail "emulator limit", never as passes. Scripts time out after 120 s in the tests (a hung emulator job fails instead of blocking). Last run: 26 passed, 2 xfail (1 emulator crash, hard delete). The 0.6.6 image is amd64-only; on ARM64 a local `:latest` arm64 tag is used via `lab/.env.local`.

Verified: script-generation unit tests; on the emulator: conformance 9 of 10 (fail: hard delete = emulator EXISTS limit; long runs need an emulator restart after a crash). Not run against the real service (BigQuery sandbox: see the console Local test lab guidance).

Tests: `pytest connectors/destinations/bigquery/tests`; emulator: `ALELT_TEST_BIGQUERY_URL=bigquery://lab-project/?endpoint=http://127.0.0.1:47050&emulator=1`.
