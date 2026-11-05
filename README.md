# HA-APILog-ELT

**Self-hosted, high-availability, log-based CDC / ELT engine.**

HA-APILog-ELT reads database transaction logs through **publicly documented vendor APIs only**
and loads analytical warehouses with fenced failover and a checkpoint-in-transaction
exactly-once design. Every tier can run redundant: capture agents, hub API, workers, spool,
and metadata.

> **Status:** under active development — **not production-ready yet.**
> **Target:** enterprise-grade at the highest level — zero data loss, exactly-once delivery,
> and no single point of failure in any tier, proven by continuous verification and chaos testing.
> See the [roadmap](#roadmap) for the hardening and features still to come.

---

## Why

- **Self-hosted, in your own cloud or data center.** Runs in your own account as Docker
  (standalone or HA) with your own PostgreSQL; capture agents run as paired Windows services
  or Linux daemons. No vendor-hosted component: your data never passes through a third party.
- **Log-based capture.** No triggers, no query polling, minimal load on the source.
- **High availability by design.** Fencing tokens, durable acknowledgements, transactional checkpoints.
- **Never drops data silently.** Anything that cannot be decoded is quarantined with its raw bytes and position, and raises an alert.
- **Extremely easy to administer.** One web console for every task, single sign-on, roles, and one command to switch between standalone and HA.
- **Self-diagnosing and self-improving.** Every incident gets an automatic root-cause analysis and, for platform defects, a proposed code fix with a regression test — turning production issues into reviewed improvements fast. A human approves every fix.
- **Painless migration from your current ELT tool.** Import existing connections, tables and schedules, run both side by side, and switch table by table with instant rollback.

## Sources

| Engine          | Capture method (public API)      |
|-----------------|----------------------------------|
| IBM Db2 LUW     | `db2ReadLog`                     |
| PostgreSQL      | Logical replication (`pgoutput`) |
| MySQL / MariaDB | Binary log replication protocol  |
| Oracle          | LogMiner                         |
| SQL Server      | Change Data Capture              |

## Destinations

Snowflake · BigQuery · Databricks · Redshift · ClickHouse · Microsoft Fabric · Delta Lake ·
Apache Iceberg · PostgreSQL and other SQL engines · Kafka

## Easy administration

| What you need            | How it works                                                                                                        |
|--------------------------|---------------------------------------------------------------------------------------------------------------------|
| Day-to-day operations    | One web console: connections, tables, lag, health, quarantine, reconciliation, topology and HA status               |
| Access control           | Single sign-on (OpenID Connect, e.g. Microsoft Entra ID) with roles: Viewer, Data Operator, Platform Admin, Auditor |
| Safe changes             | Sensitive actions (such as switching a table to production) require a second person's approval                      |
| Configuration as code    | Every setting lives in versioned YAML; console edits create audited revisions you can diff and roll back            |
| Setup                    | A guided wizard creates a pipeline in a few steps: source, tables, destination, schedule                            |
| HA without re-installing | One command (`topology apply`) moves a running installation from standalone to HA and back, resumably               |
| Troubleshooting          | Built-in incident diagnosis drafts a root cause and a fix from the evidence; a human always approves                |
| Languages                | Console in English, French, Spanish and Portuguese                                                                  |
| Runbooks                 | Step-by-step guides for install, upgrade, failover drills, resync, reprocessing, cutover and rollback               |

## Migrate from an existing ELT installation

Bring your current pipelines over without rebuilding them by hand, and without changing anything
downstream.

| Step                    | What happens                                                                                                                                         |
|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1. Import               | Read-only import of your legacy tool's configuration (live API or exported files): connections, tables, column settings, schedules, users            |
| 2. Review               | The import becomes a migration plan you can edit; secrets are masked and production hosts are protected by a deny list                               |
| 3. Match the output     | Your current warehouse tables are read to capture the exact names, types and system columns, so downstream models and reports keep working unchanged |
| 4. Parallel run         | Both tools load side by side into separate schemas; reconciliation compares row counts, keys, checksums and deletes                                  |
| 5. Switch per table     | Each table moves to HA-APILog-ELT through a view switch, after it passes certification                                                               |
| 6. Roll back in minutes | Any table can be switched back instantly if something looks wrong                                                                                    |

Import plugins are pluggable: a generic file-based format works for any legacy tool, and
vendor-specific plugins can be added as extensions.

## Built-in self-diagnosis and code-fix proposals (AI-assisted)

Every failure is diagnosed automatically, inside the platform. An optional LLM call explains the
root cause and proposes a fix — it never acts on its own.

**Why it matters:** root-cause diagnosis and ready-to-review code-fix proposals shorten the path from
"something broke in production" to "tested fix released" from days to hours. Every real-world issue
becomes a patch plus a regression test, so the platform gets more robust with each incident and
evolves faster.

| Stage                       | What happens                                                                                                                                                                                |
|-----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1. Detect                   | Job failures, quarantined records, lag, agent and reconciliation problems open an incident automatically; repeats of the same failure are grouped by signature                              |
| 2. Collect evidence         | An evidence pack is assembled (errors, positions, catalog, config revisions, logs) and redacted: no row values, PII or secrets ever leave the host                                          |
| 3. Rule-based diagnosis     | Deterministic signature rules run offline, always on, and name the known root causes with confidence and fix steps                                                                          |
| 4. LLM diagnosis (optional) | The redacted evidence is sent to the LLM you choose — Anthropic, any OpenAI-compatible endpoint, or a local Ollama model — which returns a structured root cause, evidence and proposed fix |
| 5. Proposed actions         | Suggested remediations (resync, reprocess a range, release quarantine) wait for human approval in the console                                                                               |
| 6. Code-fix proposals       | For platform defects, the LLM can draft a validated patch with a regression test; it is exported for the normal review process, never applied by the hub                                    |
| 7. Learning loop            | A confirmed root cause becomes a known-issue signature, so the next occurrence is diagnosed instantly without the LLM                                                                       |

Safeguards: off by default, provider-agnostic, API key encrypted at rest, every call recorded
(provider, model, prompt hash, answer, tokens), read-only access, and a human approves every action.

## Architecture

```
Source DB ──► Capture agent ──► Hub API ──► Spool (S3) ──► Workers ──► Warehouse
   (log)      (outbound only)   (N replicas)              (decode, assemble, load)
                                     │
                              Metadata (PostgreSQL HA)
```

## Deployment modes

Two ways to install. You can switch between them after installation.

| Component           | Standalone (one host)                | HA (every part redundant)                                                  |
|---------------------|--------------------------------------|----------------------------------------------------------------------------|
| Hub (API + workers) | One Docker host                      | Docker replicas on 2 or more hosts (Compose/Swarm or Kubernetes/Helm)      |
| Metadata database   | PostgreSQL (SQLite for testing only) | PostgreSQL HA (managed service or Patroni)                                 |
| Spool storage       | Local disk volume                    | S3-compatible object store                                                 |
| Capture agent       | One Windows service or Linux daemon  | Two agents, one active and one standby (Windows services or Linux daemons) |

## What happens when something fails (HA mode)

| If this fails…        | …this happens                                                    | Why no data is lost or duplicated                                                                    |
|-----------------------|------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| Active capture agent  | The standby agent takes over automatically after a short timeout | Only one agent at a time holds the capture lease; data still sent by the old agent is rejected       |
| One hub API replica   | The load balancer sends traffic to the other replicas            | Replicas keep no state of their own                                                                  |
| One worker            | Another worker picks up its job                                  | Each stream is processed by one worker at a time; unfinished jobs are retried                        |
| Spool storage         | Agents keep data in their local spool and retry                  | An agent deletes its local copy only after the hub confirms a durable write                          |
| Metadata primary      | PostgreSQL fails over to its replica; work resumes               | Positions are re-read from the destination checkpoint                                                |
| Destination warehouse | Loading pauses and retries until it is back                      | The checkpoint is saved in the same transaction as the data, so a retry never applies a change twice |

## Consistency guarantees (today)

| Guarantee                     | How it is enforced                                                                                                                 |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| Exactly-once apply            | The source log position is committed in the same warehouse transaction as the rows it produced (compare-and-set on the checkpoint) |
| No split brain between agents | Capture lease with an epoch fencing token, checked inside the hub's batch transaction                                              |
| Durable acknowledgements      | A batch is acknowledged only after it is written to the spool store and recorded in metadata                                       |
| No gaps or overlaps           | Every batch must start exactly where the previous one ended                                                                        |
| Safe schema changes           | Additive changes are applied automatically; any other change holds the table until an operator approves it                         |
| No silent loss                | Undecodable records are quarantined with raw bytes, position and reason                                                            |
| Source log loss detected      | A lost or invalidated PostgreSQL replication slot is detected and triggers a resync                                                |

## Verification

- **Reconciliation:** sampled row counts, key sets, column checksums and delete parity between source and destination.
- **Per-table certification gate:** a table is certified only after consecutive clean days of parity.
- **Metrics:** Prometheus metrics for lease changes, fencing rejections, queue depth, capture lag and spool usage.

## Roadmap

### Planned HA hardening

These items close the remaining gaps on the way to the enterprise-grade target.

| #  | Planned capability                  | What it adds                                                                                                                            |
|----|-------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| 1  | Source-lineage fencing              | Detects a source database failover (Db2 HADR takeover, PostgreSQL promotion) and repairs changes the new primary never received         |
| 2  | PostgreSQL failover slots           | Capture continues after a PostgreSQL primary failover without a full resync (PostgreSQL 17+)                                            |
| 3  | Fencing for every writer            | Snapshot, catch-up, replay and re-read writes check a per-table generation number inside the warehouse transaction; no stale overwrites |
| 4  | Worker fencing tokens               | Each stream owner gets a generation number checked at the destination; a paused or stuck worker stops itself and cancels its query      |
| 5  | Schema-change position accuracy     | Tables held at the exact log position of a schema change; strict row validation against the catalog                                     |
| 6  | Durable acknowledgements everywhere | Synchronous metadata replication required in HA mode, plus a manifest per batch in object storage to rebuild lost records automatically |
| 7  | Warehouse lock isolation            | Per-connection checkpoint tables, capped concurrency per warehouse, priority for the live stream over replays                           |
| 8  | Source log retention guardrails     | Continuous monitoring of PostgreSQL WAL and Db2 archive log headroom, with alerts before a position becomes unrecoverable               |
| 9  | Long destination outages            | The hub keeps accepting data for up to 72 hours so source logs are released                                                             |
| 10 | Mandatory fencing in HA mode        | Every connection must run under a capture lease when the topology is HA                                                                 |
| 11 | Continuous invariant checker        | Verifies contiguous, non-overlapping applied ranges and a forward-only checkpoint for every stream                                      |
| 12 | Real-account exactly-once proof     | Crash-point tests on a real Snowflake account                                                                                           |
| 13 | Chaos test suite                    | Automated failover tests: metadata, source database, workers, warehouse, object storage, schema changes                                 |
| 14 | SLO alerting                        | Alerts on unplanned failover, lineage change, invariant violation, retention headroom, lock waits and stuck jobs                        |

### Fan-out: several targets from the same source, at the same time

Capture a source once and deliver the same changes to several targets simultaneously — for
example Snowflake and Azure Data Lake Storage — each target with its own checkpoint, lag and
exactly-once guarantee.

| Feature                   | Goal                                                                                                                                                                |
|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Multi-destination fan-out | Read a source once and deliver the same changes to several targets at the same time (for example a warehouse and a data lake), each with its own checkpoint and lag |
| Per-table routing         | Send different tables of the same source to different targets from a single definition                                                                              |
| Pipeline multiplexing     | Several pipelines share one source capture stream instead of reading the source log repeatedly                                                                      |

## Design principles

1. **Read-only on the source.** Only the database's own log API is used.
2. **Outbound-only agents.** No listening port on the database host.
3. **Whitelisted commands.** The hub never sends arbitrary SQL to an agent.
4. **Exactly-once.** A log position is committed in the destination in the same transaction as the rows it produced.
5. **No silent loss.** Undecodable records go to quarantine with full evidence.
6. **Secrets never in code, config or logs.**

## Getting started

Installation guides, runbooks and the HA guide will be published with the code.

## Contributing

Contributions are welcome under the Developer Certificate of Origin (DCO).
Clean-room rule: no proprietary code, documentation or reverse-engineered material from any
commercial product.

## License

Licensed under the [Apache License 2.0](LICENSE).

## Disclaimer

HA-APILog-ELT is an independent project. It uses only publicly documented database APIs and
is not affiliated with, endorsed by, or derived from any commercial ELT vendor. All product
names are trademarks of their respective owners. Provided "as is", without warranty of any
kind.
