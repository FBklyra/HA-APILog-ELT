# DR — rebuild the hub VM from code + destination checkpoint

Principle: the **destination checkpoint** (`ALELT_CONTROL.CHECKPOINT`, written in the same transaction as each MERGE — R6) is the source of truth for "what is applied". Hub metadata is restored from backup when available; without it, the hub is rebuilt from Git config and resumes from the checkpoint. The agent keeps spooling (backpressure, never drops) while the hub is down.

RTO target 4 h · RPO 0 for replicated data (checkpoint) · ≤ 15 min for hub metadata (WAL archive off-site).

| # | Step | Check |
|---|---|---|
| 1 | Declare incident. If Db2-side load is a concern, kill switch B on Db2 server ([kill-switch.md](kill-switch.md)) — otherwise let the agent spool | Agent spool headroom (`AleltSpoolAbove70Pct`) |
| 2 | Recreate VM, disks, NSG, managed identity (IaC or [`deploy/hub/README.md`](../deploy/hub/README.md#azure)); same DNS names | — |
| 3 | [install-hub.md](install-hub.md) steps 1–7 (same release tag as before the incident) | Images built |
| 4a | **Metadata restore (preferred)**: `azcopy copy "$ALELT_BACKUP_BLOB_URL/wal/base/<latest>" /data/backup/wal/base/ --recursive` + WAL; restore base backup into `/data/pg/data`, set `restore_command = 'cp /var/lib/postgresql/wal_archive/%f %p'`, `touch recovery.signal`, start postgres only | Postgres reaches end of WAL, promotes |
| 4b | Alternative: `pg_restore -d alelt` latest nightly dump | Up to 24 h of hub metadata lost (audit/recon history) — recorded in incident |
| 4c | No backup at all: start with empty DB; `migrate`; import `config/tables.yaml` as a revision | Tables + config visible |
| 5 | `docker compose up -d` | All healthy |
| 6 | Hub reads destination checkpoint per stream and sets resume position = checkpoint (never older metadata) | Console Overview: resume position = destination checkpoint |
| 7 | Agent reconnects (`register`) and receives `resume_from`; re-sends unACKed spool; duplicates rejected by `batch_id` / position ≤ checkpoint | No gap, no duplicate (recon) |
| 8 | Hub local spool (`/data/spool`) lost? Batches not yet applied are re-sent by the agent if still in its spool; otherwise re-read from Db2 log from checkpoint (needs archive retention) | `AleltArchiveRetentionHeadroomLow` silent |
| 9 | Run recon on all tables; compare row counts against pre-incident report | Green |
| 10 | Close incident; exchange entry with timings (feeds L6 DR drill result) | — |

Lab drill (L6): destroy VM, run this runbook, prove "no gap, no duplicate" via `tests/ladder/L6/l6_chaos.py`.

## HA install (S3 spool + Postgres HA) — [deploy/HA.md](../deploy/HA.md)

Losing one host/zone is not a DR event in HA (other replicas carry on). Use this section when the **whole** hub site is lost.

| # | Step | Check |
|---|---|---|
| H1 | Metadata: restore the Postgres HA cluster with PITR to the latest point (managed service restore or Patroni/pgBackRest); keep the same endpoint name if possible | `SELECT max(seq) FROM batch` close to the incident time |
| H2 | Spool: the S3 bucket is the spool and raw archive — nothing to restore if the store survived; otherwise restore the bucket from its replica/versioning | object count ≈ `SELECT count(*) FROM batch` |
| H3 | Redeploy the HA stack from Git with the same release (`deploy/ha` or `helm upgrade --install … -f values-ha.yaml`) pointing at the restored DB + bucket | all replicas `/readyz` 200 |
| H4 | `alelt topology status` | metadata, spool, api, workers `ha: true`; nodes visible; no operation stuck |
| H5 | An operation was running (`topology.operation.status` not `DONE`)? Re-run the same `alelt topology apply --to …` (resumable, idempotent) or `alelt topology abort` | operation `DONE` / aborted, agents RESUMED |
| H6 | Agent pair: one holder per connection, epoch incremented once after the outage at most | console *Topology & HA* |
| H7 | Continue with steps 6–10 above (checkpoint resume, agent resend, recon) | no gap, no duplicate |
