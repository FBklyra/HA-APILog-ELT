# Architecture Decision Records

| ADR | Decision | Status |
|---|---|---|
| [001](ADR-001.md) | Log-based capture via `db2ReadLog` | Accepted |
| [002](ADR-002.md) | Agent ships raw log records; decoding on the hub | Accepted |
| [003](ADR-003.md) | Outbound HTTPS + mTLS, agent-initiated control channel | Accepted |
| [004](ADR-004.md) | Golden-spec destination contract, configurable output profile (`alelt` / `golden_spec`) | Accepted |
| [005](ADR-005.md) | Exactly-once via checkpoint compare-and-set in the same transaction; per-table watermarks; completion floor, empty flush | Accepted (amended twice) |
| [006](ADR-006.md) | Stable view layer, per-table switch | Accepted |
| [007](ADR-007.md) | Raw block archive in destination stage | Accepted |
| [008](ADR-008.md) | Config as code, versioned revisions | Accepted |
| [009](ADR-009.md) | Entra ID OIDC, four roles | Accepted |
| [010](ADR-010.md) | Db2 edge uses CLI `SQLConnect` (OQ-05 resolved) | Accepted |
| [011](ADR-011.md) | Engine-agnostic SPIs + canonical model + completeness contract | Accepted (refreshed) |
| [012](ADR-012.md) | Table lifecycle and gate precedence (initial load default, schedule = changes only) | Accepted |
| [013](ADR-013.md) | Out-of-row completion in the data path (not integrity) | Accepted |
