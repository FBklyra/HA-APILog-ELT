# ALELT console ↔ hub API contract — `/api/v1` (contract version 1)

Typed source of truth: [`src/api/contract.ts`](src/api/contract.ts). Permission matrix: [`src/lib/endpointPermissions.ts`](src/lib/endpointPermissions.ts) + [`src/lib/rbac.ts`](src/lib/rbac.ts).
Reference behaviour: the in-browser mock [`src/api/mock.ts`](src/api/mock.ts) implements every rule below; hub tests may reuse its cases (`src/api/mock.test.ts`).
Wire models mirror `hub/alelt_common/models.py` (`Position`, `TableRef` with field `schema`, `CanonicalType`, `CommandName`, `Limits`, `QuarantineRecord`, `PROBES`).

## Conventions

| Topic | Rule |
|---|---|
| Format | JSON, `snake_case`, timestamps ISO-8601 UTC (`…Z`), ids are strings. Lists that can grow return `Page<T> = {items, total, next_cursor}`. |
| Auth (prod) | `Authorization: Bearer <Entra ID access token>` (audience `api://alelt-hub`, scope `Console.Access`). Roles come from Entra group → role mappings (`GET /users`). |
| Auth (dev only) | `X-ALELT-Dev-Role: Viewer\|DataOperator\|PlatformAdmin\|Auditor`, optional `X-ALELT-Dev-User: <email>`. Hub honours them **only when `ALELT_DEV_AUTH=1`**; nginx strips them in prod. `GET /me` returns `dev: true`. |
| Correlation | Client sends `X-ALELT-Correlation-Id`; hub echoes it, logs it and stores it on the `audit_event`. |
| Mutations | Every mutating body carries `reason` (non-empty) → 400 `REASON_REQUIRED`. Every mutation writes one `audit_event`. |
| Destructive | Body also carries `confirm_text` that must equal `"<ACTION> <target>"` (`expectedConfirmText`): `RESYNC <SCHEMA>.<TABLE>`, `REPROCESS <SCHEMA>.<TABLE>`, `PAUSE <agent_id>`, `SHUTDOWN <agent_id>`, `SWITCH <table FQN or wave name>`, `ROLLBACK <…>`, `REVISION_ROLLBACK <rev id>` → 400 `CONFIRM_MISMATCH`. |
| Errors | `{"error": {"code", "message", "correlation_id"}}`. 401 unauthenticated · 403 `FORBIDDEN` (role) · 404 · 409 state conflict (`TWO_PERSON_RULE`, `NOT_CERTIFIED`, `CRITERIA_NOT_MET`, `CAPTURE_DISABLED`, `LOCKOUT`, …). |
| Secrets | Never returned. Credentials are metadata only (`name, kind, rotated_at, expires_at`). |
| Active source | `ActiveSource = LEGACY \| ALELT` (legacy pipeline stays live until decommission). |
| Positions | Opaque `Position{engine, text}`; console displays `text`, never parses it. |
| Output profile | `Overview.output_profile` (string) + resolved `Overview.system_columns{synced, deleted}`. Core profiles: `alelt` (default, `_ALELT_SYNCED/_ALELT_DELETED`) and `golden_spec` (names from the golden spec of the existing landing schema). Extensions may add profiles; the console only displays what the hub resolves. |

## Endpoints

Permission → roles: `view.dashboard` all · `view.tables` Viewer/DataOperator/PlatformAdmin · `view.recon` Viewer/DataOperator · `view.certification` DataOperator/Auditor · DataOperator: `table.*`, `import.run`, `revision.rollback`, `schema.decide`, `recon.run`, `cert.signoff`, `wave.plan`, `switch.request`, `switch.approve` · PlatformAdmin: `agent.view`, `agent.command`, `hub.view`, `retention.edit`, `users.manage` · PlatformAdmin + Auditor: `audit.read`, `audit.export` · all roles: `view.incidents` · DataOperator + PlatformAdmin: `incident.act` · DataOperator: `incident.reveal` · PlatformAdmin: `llm.configure`.

| # | Method & path | Permission | Request | Response | Notes |
|---|---|---|---|---|---|
| 1 | `GET /me` | any | — | `Me` | roles resolved by hub |
| 2 | `GET /overview` | view.dashboard | — | `Overview` | includes `ladder: LadderResult[]` |
| 3 | `GET /ladder` | view.dashboard | — | `LadderResult[]` | from `tests/ladder/Lx/result.json` |
| 4 | `GET /tables` | view.tables | — | `Page<TableSummary>` | |
| 5 | `GET /tables/{id}` | view.tables | — | `TableDetail` | columns, schema history, recon, checkpoint |
| 6 | `PUT /tables/{id}/config` | table.configure | `TableConfigUpdate` | `TableDetail` | new `config_revision`; key columns cannot be disabled; enabling needs DCC (409 `CAPTURE_DISABLED`) |
| 7 | `POST /tables/{id}/resync` | table.resync | `{reason, confirm_text}` | `{job_id}` | destructive |
| 8 | `POST /tables/{id}/reprocess` | table.reprocess | `{reason, confirm_text, from_pos, to_pos}` | `{job_id}` | re-decode from raw archive, no Db2 access |
| 9 | `GET /batches` | view.recon | `?status&cursor` | `Page<Batch>` | |
| 10 | `GET /quarantine` | view.recon | `?status&cursor` | `Page<QuarantineItem>` | raw_ref + first bytes hex |
| 11 | `GET /agents` | agent.view | — | `Agent[]` | heartbeat, spool, limits, cert expiry, governor events |
| 12 | `POST /agents/{id}/commands` | agent.command | `AgentCommandRequest{name, args, reason, confirm_text?}` | `CommandAccepted` | whitelist only (R5) → 400 `COMMAND_NOT_WHITELISTED`; PAUSE (kill switch) / SHUTDOWN destructive ; START_CAPTURE args `{resume: true}` (durable position) or `{from_pos}` (non-empty) → else 400 `INVALID_ARGS` |
| 13 | `GET /revisions` | table.configure | — | `Revision[]` (newest first) | |
| 14 | `GET /revisions/{id}/diff` | table.configure | `?against=current` | `DiffEntry[]` | |
| 15 | `POST /revisions/{id}/rollback` | revision.rollback | `{reason, confirm_text}` | `Revision` | creates a new revision (`source: rollback`) |
| 16 | `GET /revisions/{id}/export` | table.configure | — | `text/yaml` | `config/tables.yaml` format |
| 17 | `POST /import/runs` | import.run | `{reason}` | `ImportRun` | runs `alelt_import`, diff vs current |
| 18 | `GET /import/runs` | import.run | — | `ImportRun[]` | |
| 19 | `POST /import/runs/{id}/accept` | import.run | `{reason}` | `Revision` | only when `status = DONE` |
| 19a | `GET /import/runs/{id}/host-map` / `PUT /import/runs/{id}/host-map` | import.run | `HostMapUpdate{mappings, reason?}` | `HostMap{hosts[{host, connections, sides, prod_denied, test_host, test_denied}], complete}` | X-117: apply (`ApplyRequest.host_map` inline too) → 409 `HOST_MAP_REQUIRED` until every host is mapped; target on the deny list → 409 `PROD_TARGET`; console checks each target live with `POST /guard/check` |
| 20 | `GET /schema-changes` | schema.decide | — | `SchemaChange[]` | |
| 21 | `POST /schema-changes/{id}/decision` | schema.decide | `{reason, decision: APPROVE\|REJECT}` | `SchemaChange` | only `PENDING` |
| 22 | `GET /recon` | view.recon | — | `ReconRun[]` | results with drill-down `{key, column, position}` |
| 23 | `POST /recon/runs` | recon.run | `{reason, table_ids, check_kind}` | `ReconRun` | |
| 24 | `GET /certification` | view.certification | — | `TableCertification[]` | §10 criteria, clean days, sign-offs |
| 25 | `POST /certification/{id}/signoff` | cert.signoff | `{reason}` | `TableCertification` | 409 `CRITERIA_NOT_MET` unless all PASS |
| 26 | `GET /certification/waves` | view.certification | — | `Wave[]` | |
| 27 | `POST /certification/waves` | wave.plan | `WaveCreate` | `Wave` | |
| 28 | `POST /switch` | switch.request | `SwitchRequestCreate{scope, target, reason, confirm_text}` | `SwitchRequest` (`PENDING_APPROVAL`) | target `ALELT` requires every table certified; `LEGACY` = rollback to the legacy pipeline |
| 29 | `GET /switch/requests` | view.certification | — | `SwitchRequest[]` | |
| 30 | `POST /switch/requests/{id}/approve` | switch.approve | `{reason, confirm_text}` | `SwitchRequest` (`EXECUTED`) | **two-person rule**: approver ≠ requester → 409 `TWO_PERSON_RULE`; expired → 409; executes `SWITCH_SOURCE` |
| 31 | `POST /switch/requests/{id}/reject` | switch.approve | `{reason}` | `SwitchRequest` | |
| 32 | `GET /hub/health` | hub.view | — | `HubHealth` | services, queues, spool, destination credits, credential metadata |
| 33 | `GET /retention` / `PUT /retention` | retention.edit | `RetentionUpdate` | `Retention` | `raw_archive_days` 30..90 (ADR-007) |
| 34 | `GET /users` / `PUT /users/group-mappings` | users.manage | `GroupMappingsUpdate` | `UsersAndRoles` | must keep ≥ 1 PlatformAdmin mapping (409 `LOCKOUT`) |
| 35 | `GET /audit` | audit.read | `?q&from&to&actor&action&cursor` | `Page<AuditEvent>` | |
| 36 | `GET /audit/export` | audit.export | `?format=csv\|json&from&to` | file | console also exports client-side from `GET /audit` |
| 37 | `GET /topology` | hub.view | — | `TopologyStatus` | mode, per-layer HA status, hub nodes (api/worker heartbeats), capture leases per connection, last/in-flight switch operation, `install_options` (X-50) |
| 38 | `POST /topology/plan` | hub.view | `TopologyPlanRequest{to, db_url?, spool_url?}` | `TopologyPlan{from,to,steps,blockers,warnings,command}` | dry run only; apply is the CLI `alelt topology apply` (hub restarts on the new metadata/spool) |
| 39 | `POST /topology/connections/{id}/release-lease` | agent.command | `{reason}` | `LeaseReleased{released, holder, epoch}` | forced failover: lease expires now, standby acquires with epoch + 1; audited |
| 40 | `GET /meta` | any | — | `Meta{llm{configured,enabled,provider,model}, diagnose{rules,auto_actions}}` | UI shows "AI diagnosis not configured — rules-only" |
| 41 | `GET /incidents` | view.incidents | `?status=ACTIVE\|OPEN\|DIAGNOSED\|RESOLVED&severity` | `IncidentSummary[]` | dedup by signature; `classification` PLATFORM_DEFECT/SOURCE_DATA/CONFIGURATION/INFRASTRUCTURE/DESTINATION/UNKNOWN |
| 42 | `GET /incidents/summary` | view.dashboard | — | `IncidentCounts` | Overview badge |
| 43 | `GET /incidents/{id}` | view.incidents | — | `IncidentDetail` | timeline, findings, actions, AI runs, code fixes, `source_data` (no values) |
| 44 | `GET /incidents/{id}/evidence` | view.incidents | — | redacted evidence pack (file) | no row values / secrets; audited |
| 45 | `POST /incidents/{id}/diagnose` | view.incidents (+ incident.act and `reason` when `use_llm`) | `{use_llm?, reason?, question?}` | `IncidentDetail` | Stage A always; Stage B only if configured; LLM output never creates actions |
| 46 | `POST /incident-actions/{id}/decision` | incident.act | `{reason, decision: APPROVE\|REJECT}` | `{id,status,result}` | APPROVE executes (retry job / re-decode / re-sync / SNAPSHOT_CATALOG / RESUME / source-data policy); audited |
| 47 | `POST /incidents/{id}/resolve` | incident.act | `{reason, confirmed_root_cause, save_known_issue}` | `IncidentDetail` | learning loop: known-issue signature matched first next time |
| 48 | `GET /incidents/{id}/regression-note` | view.incidents | — | text/markdown | regression / golden corpus case note |
| 49 | `POST /incidents/{id}/code-fix` | incident.act | `{reason}` | `CodeFix` | PLATFORM_DEFECT only (409 `CODE_FIX_UNAVAILABLE`); patch validated (allowed paths, `git apply --check`, test included); never applied |
| 50 | `POST /code-fixes/{id}/review` / `GET /code-fixes/{id}/patch` | incident.act / view.incidents | `{reason, decision}` | `CodeFix` / text/x-diff | PROPOSED → REVIEWED\|REJECTED → EXPORTED (`alelt diagnose export-patch <incident>`) |
| 51 | `POST /incidents/{id}/source-values` | incident.reveal (DataOperator) | `{reason}` | raw samples | SOURCE_DATA raw values stay on the hub; audited |
| 52 | `POST /diagnose/explain` | view.incidents | `{kind: incident\|job\|batch\|quarantine\|agent_event\|alert, ref_id, labels?}` | `ExplainResult` | "Explain with AI" on any error; LLM only with incident.act + configured, else rules |
| 53 | `POST /diagnose/scan` | incident.act | — | `{triggers}` | runs the trigger sweep now (worker runs it every minute) |
| 54 | `GET /admin/llm` / `PUT /admin/llm` / `POST /admin/llm/test` | llm.configure (PlatformAdmin) | `LlmSettingsUpdate` (`api_key` write-only; "" clears) | `LlmSettings{key_set,configured,...}` / `LlmTestResult` | key encrypted at rest (`ALELT_DIAG_SECRET_KEY[_FILE]`, else 409 `NO_ENCRYPTION_KEY`); env `ALELT_DIAG_LLM_*` = defaults |

## Alignment notes for the hub stream

- The original endpoint list (`overview, tables, tables/{id}, tables/{id}/config, batches, quarantine, agents, agents/{id}/commands, revisions, audit, recon, certification, switch`) is covered as rows 2–16, 22–31, 35. **Additional endpoints the console needs**: `me`, `ladder`, `tables/{id}/resync|reprocess`, `import/runs*`, `schema-changes*`, `recon/runs`, `certification/waves`, `switch/requests*`, `hub/health`, `retention`, `users*`, `audit/export`.
- Two-person rule and confirm text must be enforced **server-side**; the console only mirrors them for UX.
- Switch requests expire (default 4 h); the approver's identity is stored in `audit_event` and `switch_state`.
- L7+ production steps: hub must refuse `POST /switch` (and L7+ production actions) unless the previous ladder level `result.json` is `PASS` (prompt 12 rule); return 409 `LADDER_GATE`.
