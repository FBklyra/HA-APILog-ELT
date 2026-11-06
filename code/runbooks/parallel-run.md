# Parallel run (L7–L8) — ALELT beside the legacy pipeline, legacy untouched

ALELT writes to its own landing schema. With output profile `golden_spec` its output matches the existing landing schema's contract, so consumers can switch per table. Production dbt keeps reading the legacy pipeline until cutover.

## Guardrails

| Guardrail | How it is enforced | Verify |
|---|---|---|
| Legacy pipeline untouched (R2) | Separate source user, destination schema/role/warehouse; no grants changed on legacy objects | Daily config-drift diff of the legacy configuration = expected |
| Source protected (R1) | Agent CPU/mem caps (Job Object) + governor; snapshots off-peak only; kill switch | Agent screen: no governor breach |
| Archive logs (R3) | Never touched; alert when lag approaches DBA retention | `AleltArchiveRetentionHeadroomLow` silent |
| Destination cost | Dedicated warehouse, auto-suspend 60 s + resource monitor (suspend at quota) | Hub & destination screen: credits vs monitor |
| dbt production | Unchanged; shadow target `DBT_ALELT_SHADOW` built from the ALELT landing schema | dbt prod job untouched |

## Start (L7 — one low-churn table)

| # | Step |
|---|---|
| 1 | L0–L6 `result.json` green (console refuses L7 otherwise) |
| 2 | Install agent ([install-agent.md](install-agent.md)), kill switch drilled |
| 3 | Tables → enable the L7 table → snapshot ([resync-table.md](resync-table.md) flow) → `IN_SYNC` |
| 4 | 7 consecutive days: parity vs legacy, zero impact alerts |

## Weekly cadence (L8 — all in-scope tables)

| Activity | Tool |
|---|---|
| Parity dashboard, drill into mismatches | Console Overview + Reconciliation |
| Shadow dbt models vs production (`audit_helper`) | dbt shadow target `DBT_ALELT_SHADOW` |
| Latency / freshness ALELT vs legacy | Console Overview |
| Sign off certified tables (§10: counts, key set, checksums, deletes, p95, dbt diff = 0, 7 clean days) | Console Certification |
| Plan cutover waves (low-risk tables first) | Console Certification → Waves |

## dbt shadow target (profiles.yml excerpt)

```yaml
my_project:
  outputs:
    alelt_shadow:
      type: snowflake
      schema: DBT_ALELT_SHADOW
      role: DBT_ALELT_SHADOW_ROLE       # SELECT on the ALELT landing schema only
      warehouse: ALELT_WH
      # source schema overridden: vars: { source_schema: <ALELT_LANDING_SCHEMA> }
```
