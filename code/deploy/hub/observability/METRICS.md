# Hub metrics contract (Prometheus)

Exported by `hub-api` on `/metrics` (agent values relayed from `HEARTBEAT`); workers on `:9100/metrics`.
Alert rules: [`alerts.yml`](alerts.yml). Names are a contract with `hub/alelt_hub` — change both together.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `alelt_capture_lag_seconds` | gauge | connection_id, table? | now − commit_ts of last applied change |
| `alelt_capture_lag_lri_distance` | gauge | connection_id | source head position − applied position (engine units, Db2 LRI part2 delta) |
| `alelt_latency_target_seconds` | gauge | connection_id | configured lag/latency target (default 120) |
| `alelt_latency_p95_seconds` | gauge | table | commit → destination p95, 1 h window |
| `alelt_batches_received_total` | counter | connection_id, status | raw batches received (ok / sha_mismatch / duplicate) |
| `alelt_decode_errors_total` | counter | connection_id, reason | decoder failures |
| `alelt_table_integrity` | gauge | connection_id, table, state | 1 for the table's current integrity state (IN_SYNC / SUSPECT / REDECODING / OUT_OF_SYNC / RESYNCING), F-01 |
| `alelt_table_held` | gauge | connection_id, table, kind | 1 while the table is held (snapshot / schedule / integrity) — why a table stopped progressing |
| `alelt_integrity_events_total` | counter | connection_id, event | hold / suspect / stop_stream / recovered_<how> |
| `alelt_quarantine_out_of_scope_total` | counter | connection_id | quarantined records of tables outside the replicated scope (counted, never held) |
| `alelt_replay_pass_seconds` | histogram | connection_id | one multi-table replay pass (F-08) |
| `alelt_replay_passes_total` | counter | connection_id, then | replay passes started (attach / rehold) |
| `alelt_decode_raw_total` | counter | connection_id | raw batch decodes (decoded-event cache misses) |
| `alelt_quarantine_total` | counter | table, reason | records sent to quarantine (R7) |
| `alelt_merge_duration_seconds` | histogram | table | destination apply+checkpoint transaction |
| `alelt_spool_bytes` | gauge | location (agent\|hub), agent_id? | spool usage |
| `alelt_spool_capacity_bytes` | gauge | location, agent_id? | spool cap |
| `alelt_archive_retention_seconds` | gauge | connection_id | DBA-owned archive-log retention recorded for the connection (0 = unknown) (X-129) |
| `alelt_archive_needed_age_seconds` | gauge | connection_id, stream | how far back in the source log the hub may still need to read: max(capture lag, oldest open txn) |
| `alelt_archive_horizon_seconds` | gauge | connection_id, stream | retention - needed age; < 0 = needed logs probably recycled (alerts AleltArchiveHorizon*) |
| `alelt_agent_credential_expiry_timestamp_seconds` | gauge | agent_id | expiry of the agent's live per-agent credential (F-14) |
| `alelt_config_file_drift` | gauge | connection_id | 1 = the ALELT_TABLES_CONFIG file differs from the active config revision (not applied; F-18) |
| `alelt_capture_enabled` | gauge 0/1 | connection_id, table | 0 = DATA CAPTURE CHANGES lost on an in-scope table |
| `alelt_recon_mismatch_total` | counter | table, check | failed reconciliation checks |
| `alelt_cert_expiry_timestamp_seconds` | gauge | kind (agent\|hub\|ca\|dest_key), subject | not-after, epoch s |
| `alelt_agent_last_heartbeat_timestamp_seconds` | gauge | agent_id | last heartbeat, epoch s |
| `alelt_agent_cpu_percent` / `alelt_agent_mem_mb` | gauge | agent_id | from heartbeat |
| `alelt_governor_pauses_total` | counter | agent_id, reason | governor throttling events |
| `alelt_queue_depth` | gauge | queue | pending jobs (decode/load/snapshot/verify) |
| `alelt_destination_credits_used` | gauge | warehouse | credits used in current resource-monitor period |

Worker liveness: each worker touches `/tmp/alelt_worker_alive` every poll loop (compose health check).
