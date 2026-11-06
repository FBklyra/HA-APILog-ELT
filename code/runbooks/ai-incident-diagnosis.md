# AI-assisted incident diagnosis (read-only, human approval)

An LLM assistant drafts a root-cause hypothesis and a proposed fix from quarantine records and logs. It **cannot act**: every remediation is executed by a human through the normal runbooks.

## Boundaries

| Allowed | Not allowed |
|---|---|
| Read quarantine records (reason, position, table, raw hex excerpt), batch metadata, decoder/loader logs, catalog snapshot, golden DDL, recent config revisions | Any write: no console mutation, no agent command, no SQL on Db2 or destination |
| Read-only hub API token with role **Auditor**-equivalent scope `diagnose:read` | Secrets, credentials, connection strings (redacted before the prompt) |
| Produce a report: hypothesis, evidence (record ids, positions), confidence, proposed runbook + parameters | Releasing quarantine, reprocessing, re-sync — humans only |

Data handling: the model endpoint must be approved by security (tenant-hosted, no training on inputs). Row values from ERP are minimised: column names + types + hex excerpt of the failing record only; PII columns masked.

## Procedure

| # | Step | Who |
|---|---|---|
| 1 | Alert `AleltQuarantineGrowing` / `AleltReconMismatch` / decode error spike | on-call |
| 2 | Console → Reconciliation → Quarantine → select records → **Diagnose** (or CLI `python -m alelt_hub.diagnose --table T --since 2h`) | DataOperator |
| 3 | Hub assembles a redacted evidence bundle and sends it to the model; bundle + prompt + response stored and audited | automatic |
| 4 | Review the report: does the evidence support the hypothesis? Reproduce with the golden corpus (`tests/golden/`) | DataOperator / developer |
| 5 | **Approve** or reject in console (approval is recorded; it executes nothing) | DataOperator |
| 6 | Apply the fix via the normal path: code change + tests (R10) → [reprocess-range.md](reprocess-range.md) / release quarantine | developer + DataOperator |
| 7 | Add the failing record to the decoder corpus so it is regression-tested | developer |

Typical findings: unmapped source type (`UNKNOWN` → R7), catalog version mismatch after DDL, codepage issue, compressed row not yet supported (OQ-04), destination type overflow vs golden DDL.

## Built-in self-diagnosis (`hub/alelt_diagnose`)

| Step | What happens |
|---|---|
| Trigger | An incident opens automatically (job failure, quarantine, source-data check, agent offline/error/auth, governor pause, backpressure, capture lost, gap, destination error, recon mismatch, lag). Same failure = same incident (count grows). |
| Stage A | Deterministic rules give root cause, classification, confidence, evidence refs, fix steps and an optional action. Always on, offline. |
| Stage B | Console **Ask AI** / **Explain with AI** (DataOperator, PlatformAdmin) when an AI endpoint is configured (console → *AI assistant*). Only the redacted evidence pack is sent; the answer is recorded and never executed. |
| Act | Approve / reject the suggested action in console (reason, audited). |
| Defect | PLATFORM_DEFECT: **Propose code fix** → review the diff → `alelt diagnose export-patch <incident>` → normal PR → after release approve **re-decode range**. |
| Source data | SOURCE_DATA: give the plain-language explanation + read-only SQL to the data owner / DBA; choose a handling option; raw values only via **Reveal** (DataOperator, audited). |
| Learn | **Confirm root cause** stores a known-issue signature: next occurrence is matched first. Download the regression note for the golden corpus. |
