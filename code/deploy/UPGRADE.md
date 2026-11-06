# Upgrade contract

What an operator can rely on when moving from one ALELT release to the next, and how to roll back. Covers the hub
metadata schema, the agent ↔ hub protocol, the container image and the pipeline configuration.

## 1. Order of upgrade

| Step | Action | Gate |
|---|---|---|
| 1 | Back up the metadata DB (`deploy/hub/backup`, `pg_dump`, or a copy of the SQLite file with the hub stopped). | Backup file exists and has been restored once in a test. |
| 2 | Pull the new image. Run `alelt migrate --check`. | Exit 1 means steps are pending. That is expected before an upgrade. |
| 3 | Stop the workers, then run the `migrate` job/service of the **new** image (`alelt migrate`). | Exit 0. Exit 2 means the schema is newer than the image (see §4). |
| 4 | Roll the API replicas, then the workers, onto the new image. | `/readyz` is 200. `alelt migrate --check` exits 0. |
| 5 | Upgrade the agents (MSI / `aleltd` package), one connection at a time. | Each agent registers. No `426` in the gateway log. |

**The hub is upgraded first.** A hub accepts agents of protocol `MIN_PROTOCOL_VERSION..PROTOCOL_VERSION`
(`alelt_common/__init__.py`), so an N-1 agent keeps working against an N hub. If an agent is newer than its hub, it gets
`426 ... upgrade the hub first`. If an agent is older than the window, it gets `426 ... upgrade the agent`.

With `ALELT_AUTO_MIGRATE=0`, API, worker and capture processes never migrate the schema. This is the default in the HA
compose file, the Helm chart and `deploy/hub`. They refuse to start while steps are pending
(`SchemaPending: run alelt migrate first`). Single-host installs (`deploy/local`) keep `ALELT_AUTO_MIGRATE=1`, so the
first process to start migrates the schema under a lock.

## 2. Metadata schema migrations (`hub/alelt_hub/migrations.py`)

- **Forward-only and versioned.** Each step has a number, and `schema_migration` records the release that applied it.
  A step is never edited after its release and never reverted.
- **Structure is additive.** Tables and columns declared in `db.py` are created when missing. New columns must be
  nullable or have a constant default. Any other change (backfill, rename, type change) is a numbered data step.
- **Tested from every previous release.** `hub/alelt_hub/tests/schemas/<release>.<dialect>.sql` holds the frozen schema
  of each release. `test_upgrade.py` loads each one into SQLite and PostgreSQL, adds data in the shape that release
  wrote, upgrades, and checks the data and the re-run (a no-op). **When you cut a release, add its frozen schema**
  (`python <scratch>/freeze.py` → same format as `0.1.0.*.sql`).
- Steps so far: `1` = 0.1.0 baseline, `2` = config revisions (`config_doc`). See §5.

## 3. Image rollback

Migrations only move forward, so a rollback has two parts: the previous image **and** the metadata backup from §1.

1. Stop the API, workers and capture.
2. Restore the metadata backup taken before `alelt migrate`.
3. Start the previous image.
4. Agents resume from the destination checkpoint. Batches received after the backup are re-sent by the agents (their
   spool is only trimmed up to the hub's durable ACK) or replayed from the source log. Delivery stays exactly-once,
   because the destination checkpoint is authoritative (R6).

If you start an older image against a migrated database **without** restoring the backup, it refuses to start:
`SchemaTooNew: metadata schema is at version N but this release knows only <= M`. The data is never silently
reinterpreted.

## 4. Agent credentials

- **Production (`ALELT_ENV=prod`) never accepts static shared tokens.** The hub refuses to start if `ALELT_AGENT_TOKENS` is
  set. Use `ALELT_AGENT_AUTH=mtls` behind a TLS terminator listed in `ALELT_TRUSTED_PROXIES`, or enrollment.
- **Enrollment** (console Setup → *Enroll agent*): a one-time `alelte_` token bound to one connection and one agent id.
  - An agent that declares the capability `credential.v1` gets a per-agent `alelta_` credential in the register response.
    Its first use retires the enrollment token.
  - The agent rotates its credential with `POST /agent/v1/credential/rotate`. The old credential keeps working for
    `ALELT_AGENT_CREDENTIAL_GRACE_S` (300 s).
  - Credentials expire after `ALELT_AGENT_CREDENTIAL_TTL_DAYS` (90). The alert `AleltAgentCredentialExpiring` fires 7 days
    before expiry.
  - A claimed enrollment token stops working `ALELT_ENROLLMENT_CLAIMED_TTL_HOURS` (24 in production) after its claim.
    Agents that cannot store a credential must be re-enrolled.
- **Compromised host:** `POST /api/v1/setup/agents/{agent_id}/credentials/revoke` revokes every credential and
  enrollment of that agent.

## 5. Configuration migration

- The metadata DB is the only source of truth for pipeline configuration (ADR-008, F-18). The upgrade to step 2 moves
  each pipeline's 0.1 configuration into one config revision:
  - `pipeline_definition.tables_doc`, or else the `ALELT_TABLES_CONFIG` document;
  - plus console overrides.
  - Tables in `HISTORY` sync mode are **disabled**, with the original mode kept as `legacy_sync_mode`. HISTORY is out of
    scope for this release (X-127).
- tables.yaml documents carry `config_version`. Older documents are upgraded when imported. A document from a newer
  release is refused.
- After the upgrade, editing a YAML file changes nothing. Apply it with `alelt config import FILE --reason "..."` (or
  the console import), which creates a revision. `alelt config diff` and the metric `alelt_config_file_drift` show files
  that differ from the active revision. `alelt config export <connection>` writes the active revision as YAML for Git.

## 6. Release checklist (upgrade-relevant)

- [ ] Add the frozen schema files for the new release, and make sure `test_upgrade.py` passes on SQLite and PostgreSQL.
- [ ] If the protocol changed: raise `PROTOCOL_VERSION`, keep `MIN_PROTOCOL_VERSION = PROTOCOL_VERSION - 1`, and add
      a test with an N-1 agent.
- [ ] If the tables.yaml format changed: raise `CONFIG_VERSION` and add the conversion in
      `config_store.migrate_doc`.
- [ ] Release notes say whether the release contains a data step (which means a rollback needs the backup).

## Metadata migration 3 — lifecycle, completion, job lanes

`alelt migrate` (or `--check` first) applies step 3 (`lifecycle_completion`):
- creates `table_completion`, `table_lifecycle`, `pipeline_request`, `job.priority`, `checkpoint_mirror.dest_position`
  and the claim index;
- derives a lifecycle phase for every enabled table: snapshot hold IN_SYNC → STREAMING, SNAPSHOTTING / RESYNC →
  INITIAL_LOAD, CATCHING_UP → CATCH_UP; a table without a snapshot row on a connection that already flushed →
  **STREAMING, mode=legacy** (no retroactive snapshot; resync from the console if wanted); else NEW (initial load);
- turns keyed re-read keys parked on integrity rows into completion entries and releases those integrity rows when
  their only reason was out-of-row values; resolves the matching quarantine rows;
- collapses queued decode jobs into one marker per connection.
Release note: initial load is now the default for every connection and every entry point; set `initial_load: none`
to keep starting empty. Agents: the allow-list accepts `SCHEMA.*`; agents of the previous release keep working (the
hub re-issues snapshots from the top when an agent sends no `last_key`).
