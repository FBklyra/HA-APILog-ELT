# ALELT (HA-APILog-ELT) v1.0.0 — release notes

Release date: 2026-11-05 · Licence: Apache-2.0 · Status: **early release**

ALELT is an open-source, engine-agnostic change data capture (CDC) / ELT platform. It reads committed changes from
operational databases through their public log APIs and applies them to warehouses, lakes, SQL databases and
streams, with the data and the checkpoint written in one destination transaction (idempotent replay).

This is the first public release. It is an **early release**: interfaces and identifiers may still change, and
not every connector has been proven against a live production engine. Read the known issues before you use it.

## Features

- Log-based capture through public vendor APIs: Windows x64 edge agent (`aleltagent.exe`, C), Linux capture
  daemon (`aleltd`, Python), or remote hub-side capture.
- Outbound-only agents; whitelisted, typed hub commands; CPU / memory governor; production deny list.
- Raw batches made durable before acknowledgement, then decode, transaction assembly (rollbacks never emitted) and
  apply plus checkpoint compare-and-set in one destination transaction.
- Initial snapshot coordinated with the capture start position; schema-drift handling; quarantine instead of
  silent divergence.
- Web console (English, French, Spanish, Brazilian Portuguese): setup wizard, pipelines, reconciliation and
  certification, rule-based plus optional AI-assisted diagnosis, audit log.
- Prometheus metrics and alert rules; structured JSON logs.
- Packaging: one container image (hub API, worker, migrations, console), PostgreSQL or SQLite metadata,
  standalone or highly available (Docker Compose / Swarm, Helm chart in the source tree).

## Supported sources

| Engine | Public API | Capture | Status |
|---|---|---|---|
| IBM Db2 LUW 11.5+ | `db2ReadLog` | Windows x64 edge agent | beta |
| PostgreSQL 14+ | logical decoding (`pgoutput`) | remote or Linux daemon | beta |
| MySQL 8.4 / 9.x | replication protocol (ROW binlog, GTID) | remote or Linux daemon | beta |
| MariaDB 10.6+ | replication protocol (ROW binlog) | remote or Linux daemon | beta |
| Microsoft SQL Server 2019+ | CDC table functions | remote or Linux daemon | beta |
| Oracle Database 19c+ | LogMiner | remote or Linux daemon | planned |

## Supported targets

| Destination | Status |
|---|---|
| PostgreSQL | beta |
| MySQL, MariaDB, SQL Server, Db2, SQLite, DuckDB (generic SQL destination) | beta |
| Snowflake | beta (contract-tested; live tests need an account) |
| Oracle (generic SQL), ClickHouse, Apache Iceberg, Delta Lake, Kafka API, Google BigQuery | planned |
| Microsoft Fabric, Amazon Redshift, Databricks | planned |

*beta* = implemented and verified end-to-end in the local test ladder; *planned* = implemented or in development,
not yet verified against a live engine.

## Prebuilt packages (`precompiled/`)

| Package | Content |
|---|---|
| `docker/ha-apilog-elt-1.0.0-linux-amd64.tar.gz` | container image `ghcr.io/fbklyra/ha-apilog-elt:1.0.0` (linux/amd64); the worker runs from the same image (`command: worker`) |
| `windows-x64/aleltagent-1.0.0-windows-x64.zip` | Windows x64 edge agent, Db2 source plugin, install / verify / uninstall scripts |
| `linux-x64/aleltd-1.0.0-linux-x64.tar.gz` | Linux capture daemon, systemd unit, `install.sh`, per-engine wheels |
| `quickstart-sqlite/`, `quickstart-postgres/`, `quickstart-ha/` | Docker Compose bundles referencing the GHCR image |

Checksums: `SHA256SUMS`. Usage: `README-precompiled.md`.

## Known issues

**All issues below are known and actively being fixed.** Each entry in
[`KNOWN-ISSUES.md`](../code/KNOWN-ISSUES.md) carries the status "In progress — fix being worked on". In summary:

- some hub tests fail only when the whole suite runs in one session (order-dependent leftovers) — Status: In
  progress — fix being worked on;
- the Db2 → warehouse end-to-end run has not been repeated on the final code, and the independent code review of
  the initial-load fix (WP0–WP6) is pending — Status: In progress — fix being worked on;
- the test suites were not re-run after the internal rename to `alelt` (verified mechanically only) — Status: In
  progress — fix being worked on;
- the Windows agent does not yet run hub commands on a dedicated command thread (D-17) — Status: In progress — fix
  being worked on;
- open data-semantics, source and destination items from the internal code review — Status: In progress — fix
  being worked on.
