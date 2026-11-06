# L10 — Legacy pipeline decommission checklist

Exit criterion (the test ladder, neutral wording): **all tables certified + 30 days stable**, then the legacy
pipeline is decommissioned at its renewal date. Legacy-pipeline objects are never modified before this level (R2).
Tick a box only with evidence (link, ticket, audit id). `l10_check.py` reads this file: the level is green only
when every box is ticked, every *Evidence* cell is filled and L9 is PASS.

Owner: data platform lead · Approvers: data team lead + IT platform lead (two-person rule).

## A. Preconditions

| # | Item | Done | Evidence |
|---|---|---|---|
| A1 | Every in-scope table certified (L8 `result.json` PASS, console screen 8) | [ ] | |
| A2 | Every wave switched to ALELT and rollback to LEGACY drilled (L9 PASS per wave) | [ ] | |
| A3 | 30 consecutive stable days after the last wave (no P1/P2 incident, recon clean, lag ≤ target) | [ ] | |
| A4 | dbt production runs green on ALELT views for 30 days | [ ] | |
| A5 | Legacy pipeline renewal date confirmed (OQ-07) and decommission date agreed with Finance | [ ] | |

## B. Pause (hot rollback still possible)

| # | Item | Done | Evidence |
|---|---|---|---|
| B1 | Change request approved (CAB) for pausing the legacy pipeline connector | [ ] | |
| B2 | Legacy connector **paused** (not deleted); legacy landing schema left intact | [ ] | |
| B3 | `alelt_import diff` shows no legacy config drift on pause day | [ ] | |
| B4 | Rollback path re-validated: `SWITCH_SOURCE(table,'LEGACY')` still points to the frozen legacy tables (read-only fallback) | [ ] | |
| B5 | 30 days with the legacy connector paused, no rollback needed | [ ] | |

## C. Delete at renewal

| # | Item | Done | Evidence |
|---|---|---|---|
| C1 | Final evidence bundle exported (audit log, certification, recon history) and archived | [ ] | |
| C2 | Legacy landing schema retained/archived per data-retention policy (decision recorded) | [ ] | |
| C3 | Legacy connector deleted; its Db2 user disabled by the DBA (not by ALELT) | [ ] | |
| C4 | Legacy destination role/warehouse removed by the destination admins; resource monitor reviewed | [ ] | |
| C5 | Stable views and switch procedure: LEGACY branch removed only after the C2 decision | [ ] | |
| C6 | Legacy contract not renewed; cost tracking updated (savings baseline) | [ ] | |
| C7 | Runbooks updated (legacy rollback path retired) | [ ] | |

## Sign-off

| Role | Name | Date | Signature / audit id |
|---|---|---|---|
| Data team lead | | | |
| IT platform lead | | | |
