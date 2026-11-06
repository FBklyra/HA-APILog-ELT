# ALELT runbooks

Rules that apply to **every** runbook (the platform rules R1-R10):

| Rule | Meaning for operators |
|---|---|
| R1 | Never write to the ERP Db2 database. No runbook step runs DDL/DML there. |
| R2 | Never modify legacy-pipeline objects (connector, source user, destination schemas/roles/warehouse). |
| R3 | Never delete, move or rename Db2 archive log files — the DBA owns retention. |
| R4/R5 | The agent only connects outbound; the hub only sends whitelisted commands. |
| R9 | Secrets live in Key Vault (hub) / Windows cert store + DPAPI (agent). Never paste them in tickets, chat or Git. |

Every console action below is audited (`Audit` screen). Destructive actions require typed confirmation and, where marked, a second approver.

## Index

| Area | Runbook | Who |
|---|---|---|
| Install | [install-hub.md](install-hub.md) | PlatformAdmin |
| Install | [install-agent.md](install-agent.md) | PlatformAdmin + DBA |
| Security | [rotate-certs-keys.md](rotate-certs-keys.md) | PlatformAdmin |
| Emergency | [kill-switch.md](kill-switch.md) | PlatformAdmin, DBA, on-call |
| Data | [resync-table.md](resync-table.md) | DataOperator |
| Data | [reprocess-range.md](reprocess-range.md) | DataOperator |
| Upgrade | [upgrade-agent.md](upgrade-agent.md) | PlatformAdmin |
| DR | [dr-rebuild-hub.md](dr-rebuild-hub.md) | PlatformAdmin |
| HA | [agent-failover-drill.md](agent-failover-drill.md) (guide: [deploy/HA.md](../deploy/HA.md)) | PlatformAdmin + DBA |
| Migration | [parallel-run.md](parallel-run.md) | DataOperator |
| Migration | [cutover.md](cutover.md) | DataOperator + approver |
| Migration | [rollback.md](rollback.md) | DataOperator (any on-call) |

Engine/vendor-specific migration runbooks live in private extensions (`extensions/<name>/runbooks/`), not in the core.
| Incidents | [ai-incident-diagnosis.md](ai-incident-diagnosis.md) | DataOperator, PlatformAdmin |

## Incidents

| Alert (`deploy/hub/observability/alerts.yml`) | First runbook |
|---|---|
| AleltAgentOffline, AleltSpoolAbove70Pct | [kill-switch.md](kill-switch.md) §Diagnose, then agent host checks |
| AleltCaptureLagAboveTarget | Console Overview → queues; scale workers ([install-hub.md](install-hub.md) §Scale) |
| AleltArchiveRetentionHeadroomLow | Escalate to DBA (ask for longer retention; **never** touch archive files) → else [resync-table.md](resync-table.md) |
| AleltCaptureLost | DBA re-enables DATA CAPTURE CHANGES (their change, not ours) → [resync-table.md](resync-table.md) |
| AleltReconMismatch | [reprocess-range.md](reprocess-range.md) |
| AleltQuarantineGrowing | [ai-incident-diagnosis.md](ai-incident-diagnosis.md) |
| AleltCertExpiring30d | [rotate-certs-keys.md](rotate-certs-keys.md) |
| AleltHubDown | `docker compose ps` / logs; if VM lost → [dr-rebuild-hub.md](dr-rebuild-hub.md) |

## System columns

Names come from the connection's **output profile**: `alelt` (default) → `_ALELT_SYNCED` / `_ALELT_DELETED`; `golden_spec` → names taken from the golden spec of the existing landing schema (`config/golden/`), used when replacing a legacy pipeline. Runbooks write `<SYNCED>` / `<DELETED>` for whichever applies.
