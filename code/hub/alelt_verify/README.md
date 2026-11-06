# alelt_verify — reconciliation and certification evidence

| Module | Role |
|---|---|
| `checks.py` | Pure comparators: row count, key-set buckets, column checksums, sampled rows (CHAR padding / decimal scale normalized), deletes, latency p50/p95/p99, dbt shadow diffs |
| `runner.py` | `Verifier(reference, candidate, store)`; `DestinationSide` adapts destination verify helpers (schema + output profile + `synced_before` for rows settled in both) |
| `models.py` | `ReconResult` (table, column, key, bucket, position window, raw ref) -> `ALELT_CONTROL.RECON_RESULT` row; stores |
| `edge.py` | Edge sources (F-03): source side computed ON the edge by `VERIFY_TABLE` (aggregates per key range, keys / rows only where something differs); `run_edge_suite(mk, call, ...)`, transport = `alelt_hub.edge_verify.caller` |
| `reference.py` | Reference-landing parity (X-124): two schemas in one destination, `ReferenceMapping` (system columns, renames, deleted / synced columns, settled window), vendor mappings from extensions (`reference_mappings`) |
| `certification.py` | `evaluate(table, results, now)`: master spec §10 from recorded evidence only — 7 complete clean days, every required check PASS each day, no FAIL/ERROR, no open failure today |

Errors while checking are recorded as `ERROR` evidence (they block certification).
Tests: `cd hub && python -m unittest discover -s alelt_verify/tests -t .`
