# ALELT — high availability (HA) guide for cloud and data engineers

One image (`alelt`), two install shapes: **standalone** (one host) or **HA** (every layer redundant).
You can start standalone and switch to HA later — and back — with one command (`alelt topology`).
Contracts: this document (sections 3-8).

## 1. Choose

| | Standalone (non-HA) | HA |
|---|---|---|
| Availability | single host; hub down = capture keeps spooling on the agent, loading stops until restart | survives loss of any one host / zone / pod; no operator action for API, worker, agent failover |
| Components | 1 api + 1 worker (or `alelt all`), bundled Postgres or SQLite, local spool volume | N api behind an LB, N workers on ≥ 2 hosts, Postgres HA, S3-compatible spool, active/passive agents (optional) |
| Install | `deploy/local` (compose) · Helm `values-standalone.yaml` | `deploy/ha` (compose / Swarm) · Helm `values-ha.yaml` |
| Infra cost | 1 VM (2–4 vCPU) | ≥ 2 VMs or a K8s namespace + managed Postgres HA + object store bucket |
| Ops effort | low: backups of one volume + DB | medium: Postgres HA, bucket lifecycle, LB, failover drills |
| When | evaluation, small installs, lab, non-critical feeds | production feeds with an availability SLO, multi-zone sites, maintenance without downtime |

Rule of thumb: production with an SLO → HA. Everything else → standalone, switch later.

## 2. Architecture per layer

| Layer | HA mechanism | Standalone |
|---|---|---|
| Capture agent (Windows) | Service recovery actions (auto-restart) **+ active/passive pair** (e.g. agent on Db2 HADR primary and standby host). The hub grants a **capture lease** per connection (`lease_id`, holder, TTL, **epoch** = fencing token) to exactly one agent; the standby polls and takes over when the lease expires; the hub rejects batches from a non-holder or a stale epoch (`423 lease_fenced`). The new holder resumes from the hub durable position. | one agent + service recovery |
| Hub API | N stateless replicas behind any LB/proxy (health `GET /readyz`) | 1 |
| Workers | N workers on ≥ 2 hosts; each job claimed once (`SKIP LOCKED`); per-stream ordering lock (Postgres advisory lock); a dead worker's job is re-queued | 1 (a few on SQLite) |
| Hub spool + raw archive | **blob store** `ALELT_SPOOL_URL=s3://bucket/prefix` (any S3-compatible store); ACK only after the store confirmed the write | `file:///var/lib/alelt/spool` (volume) |
| Metadata | **Postgres HA** (managed service or Patroni/repmgr) — URL only | Postgres or SQLite (SQLite = single host only) |
| Destination | exactly-once checkpoint in the same transaction as the data (R6) → any worker can fail at any time; replay is idempotent | same |
| Orchestration | `deploy/ha/docker-compose.ha.yml` (2+ hosts, Swarm-compatible) or Helm `deploy/helm/alelt` (replicas, PDBs, anti-affinity, probes) | `deploy/local/docker-compose.yml` |

Settings that change between modes (everything else is identical):

| Setting | Standalone | HA |
|---|---|---|
| `ALELT_DB_URL` | `postgresql+psycopg://…@postgres:5432/alelt` or `sqlite:////data/alelt.db` | `postgresql+psycopg://…@<pg-ha-endpoint>:5432/alelt` |
| `ALELT_SPOOL_URL` | `file:///var/lib/alelt/spool` (default) | `s3://bucket/prefix` + `ALELT_S3_*` |
| replicas | api 1, worker 1 | api ≥ 2, worker ≥ 2 |
| agent `ha.mode` | `standalone` | `active_passive` (optional, per connection) |

## 3. S3-compatible stores (`ALELT_SPOOL_URL=s3://bucket/prefix`)

| Store | `ALELT_S3_ENDPOINT` | `ALELT_S3_ADDRESSING` | Credentials |
|---|---|---|---|
| MinIO (on-prem) | `https://minio.example.internal:9000` | `path` (default with an endpoint) | access key / secret key |
| AWS S3 | empty (regional endpoint from `ALELT_S3_REGION`) | `virtual` | IAM user keys, or `AWS_*` env / session token |
| Google Cloud Storage (XML interop) | `https://storage.googleapis.com` | `path` | HMAC keys of a service account; `ALELT_S3_REGION=auto` |
| Azure Blob | the S3 endpoint of your S3 gateway (MinIO gateway or a third-party S3 proxy) | `path` | gateway keys |
| Ceph RGW / other S3 | `https://rgw.example.internal` | `path` | RGW user keys |

Env: `ALELT_S3_ENDPOINT`, `ALELT_S3_REGION` (default `us-east-1`), `ALELT_S3_ACCESS_KEY_ID`, `ALELT_S3_SECRET_ACCESS_KEY`,
`ALELT_S3_SESSION_TOKEN` (each also `_FILE`; fallback `AWS_*`), `ALELT_S3_ADDRESSING`, `ALELT_S3_TIMEOUT_S` (30).
Bucket: versioning on, lifecycle to expire objects after the raw-archive retention (console *Retention*, 30–90 days) +
margin, server-side encryption on, access only from the hub hosts. `ALELT_SPOOL_DIR` stays a local scratch dir per replica.
SSE-S3, SSE-KMS and SSE-C all work: write integrity is enforced by `Content-MD5` (the store refuses a mismatching body);
the ETag is compared with the MD5 only for objects that are not KMS / customer-key encrypted. Credentials (env or
`*_FILE`) are re-read every `ALELT_S3_CRED_TTL_S` (300 s) and at once on `ExpiredToken` / `InvalidAccessKeyId`, so rotated
STS / IRSA / mounted keys need no restart. Keys without `s3:ListBucket` work (a HEAD 403 counts as "unknown"), but
the usage gauge and `spool-copy` need it. Usage listings are cached (`ALELT_SPOOL_USAGE_TTL_S`, 300 s) and capped
(`ALELT_S3_LIST_CAP`, 2 000 000 objects).

**Metadata database.** Per-stream worker exclusivity uses PostgreSQL *session* advisory locks: connect directly or
through PgBouncer with `pool_mode = session` (transaction / statement pooling is detected and refused). A metadata
failover drops the lock with the session; the worker re-checks it before and after each pipeline run and aborts the
run when it is gone (the destination checkpoint keeps the data exactly-once). Lease expiry uses the PostgreSQL server
clock (`now()`), so API replicas with skewed clocks cannot cause a takeover; keep NTP on every host anyway (audit
times, TLS). SQLite metadata runs with `synchronous=FULL` (an ACKed batch row survives a power loss).

## 4. Install

### Standalone
```sh
cd deploy/local && cp .env.example .env    # edit POSTGRES_PASSWORD, ALELT_AGENT_TOKENS
docker compose up -d                       # or: docker compose -f docker-compose.sqlite.yml up -d
```
Helm: `helm upgrade --install alelt deploy/helm/alelt -f deploy/helm/alelt/values-standalone.yaml`

### HA — Docker Compose / Swarm (2+ hosts)
| # | Step |
|---|---|
| 1 | Provision Postgres HA (managed or Patroni); create database `alelt` + a login with CREATE on it |
| 2 | Create the bucket (`s3://alelt-spool/prod`), access keys scoped to it |
| 3 | `cd deploy/ha && cp .env.ha.example .env`; set `ALELT_DB_URL`, `ALELT_SPOOL_URL`, `ALELT_S3_*`, OIDC, agent auth |
| 4 | Secrets as files (recommended): `./secrets/alelt_db_url`, `alelt_s3_secret_access_key`, … and add `-f docker-compose.ha.secrets.yml` |
| 5 | Swarm: `docker swarm init` / join ≥ 2 nodes; `set -a; . ./.env; set +a; docker stack deploy -c docker-compose.ha.yml -c docker-compose.ha.secrets.yml alelt` |
| 5′ | No Swarm: on each host `docker compose -f docker-compose.ha.yml up -d` (same `.env`), all hosts behind your LB |
| 6 | LB: round-robin to every api on 8080, health `GET /readyz`, idle/read timeout ≥ 120 s, body ≥ 64 MB, TLS + agent mTLS terminated there (forward `X-SSL-Client-Fingerprint`). Reference: `deploy/ha/lb/haproxy.cfg` (`--profile lb`) |
| 7 | Check: `alelt topology status` → every layer `ha: true`; console → *Topology & HA* shows ≥ 2 api and ≥ 2 worker nodes |

Rehearsal on one machine: `docker compose -f docker-compose.ha.yml --profile demo --profile lb up -d --scale alelt=2 --scale alelt-worker=2` (SeaweedFS S3 + Postgres bundled, loopback only, no default passwords; see `deploy/ha/README.md`).

### HA — Kubernetes (Helm)
```sh
kubectl create secret generic alelt-db  --from-literal=ALELT_DB_URL='postgresql+psycopg://alelt:***@pg-ha:5432/alelt'
kubectl create secret generic alelt-s3  --from-literal=ALELT_S3_ACCESS_KEY_ID=*** --from-literal=ALELT_S3_SECRET_ACCESS_KEY=***
kubectl create secret generic alelt-hub --from-literal=ALELT_DEST_URL='…' --from-literal=ALELT_AGENT_TOKENS=***
helm upgrade --install alelt deploy/helm/alelt -f deploy/helm/alelt/values-ha.yaml
```
The chart refuses to render `mode: ha` with SQLite or a non-S3 spool. It sets PDBs (`minAvailable: 1`), pod
anti-affinity (`antiAffinity: preferred|required`), zone spread, `/healthz` + `/readyz` probes, worker liveness on
`/tmp/alelt_worker_alive`, non-root read-only pods, and `migrate` as a pre-install/pre-upgrade hook.
Rehearsal: `--set env.ALELT_ENV=demo --set demo.postgres.enabled=true --set demo.minio.enabled=true` (single-instance SeaweedFS S3 + Postgres, not production; refused with `ALELT_ENV=prod`). Production: pin `image.digest`.

## 5. Switch after installation

All steps are **idempotent and resumable** (state in metadata `hub_setting.topology`, each step audited as
`topology.<step>`). Re-run the same `apply` after any interruption; it continues where it stopped.

```sh
alelt topology status                       # current mode per layer, nodes, leases, running operation
alelt topology plan  --to ha --db-url postgresql+psycopg://… --spool-url s3://bucket/prefix --out /var/lib/alelt/topology
alelt topology apply --to ha --db-url … --spool-url … --out /var/lib/alelt/topology
alelt topology abort                        # stop an operation: resume agents on the current hub, nothing switched
```
Run it from any hub container (`docker compose exec alelt alelt topology …` / `kubectl exec deploy/<rel>-api -- …`).
Console → *Topology & HA* (PlatformAdmin) shows the same status, a dry-run plan in plain language and live progress.

### Standalone → HA
| # | Step | What happens | Safe because |
|---|---|---|---|
| 1 | preflight | target metadata must be Postgres and reachable; target bucket write/read/delete probe; lists blockers | nothing changed yet |
| 2 | drain + freeze | PAUSE to every agent; waits until every PAUSE is **ACKED** (a refused PAUSE blocks), agent spools are empty, no batch is `RECEIVED`/`DECODED`, no job of any kind is running (decode, snapshot, resync, reprocess, verify, replay, integrity); then **freezes writes** on the source hub (`/blocks`, `/catalog`, `/snapshot` → 503, audit `topology.freeze`) and checks again. Registered agents that are offline block the switch unless `--allow-offline`; `FAILED` batches block unless `--allow-failed` | the agents stop reading; Db2 keeps the log (R3); an agent that comes back late keeps spooling (503) and ships to the new stack |
| 3 | copy_spool | copies every spool object local → S3, verifies sha256 per object; skips objects already copied | ACKed data stays in both stores |
| 4 | migrate_metadata | SQLite/Postgres → target Postgres, table by table, from ONE consistent source snapshot; **row count, key digest and full-content digest per table verified**; sequences reset. A crash or an abort leaves the target populated: the next apply resets it | source DB untouched |
| 5 | write_config | `<out>/alelt.ha.env` + `<out>/values.ha.yaml` from the current config (**no secrets**: references to secret files) | review before use |
| 6 | resume | the copied freeze is lifted on the target, RESUME queued (once per agent, also after a crash) for the paused agents **in the target metadata**; mode `ha` recorded there. The source stays frozen | agents resume only when they reach the new hub |
| 7 | operator: restart | stop the standalone stack; start HA with the generated config (`deploy/ha` or `helm upgrade -f <out>/values.ha.yaml`) | agents reconnect, get RESUME, continue from the hub durable position — no gap, exactly-once (checkpoint) |

### HA → standalone
Same command with `--to standalone [--db-url sqlite:////data/alelt.db | postgresql+psycopg://…] --spool-url file:///var/lib/alelt/spool`:
drain → S3 → local spool copy + verify → metadata copy (optional; keep Postgres by passing the same URL) → standalone
config (api 1, worker 1) → resume. Then scale to one host: `deploy/local` with the generated `.env`, or
`helm upgrade -f <out>/values.standalone.yaml`. Agents in active/passive can stay paired; to return them to standalone
set `ha.mode: standalone` on the agent, stop the standby, then `alelt topology lease-clear --connection <id>`.

### Building blocks (also usable alone)
| Command | Use |
|---|---|
| `alelt migrate-metadata --from sqlite:////data/alelt.db --to postgresql+psycopg://… [--verify-only]` | copy hub metadata between backends; prints rows per table source vs target; non-zero exit on mismatch |
| `alelt spool-copy --from file:///var/lib/alelt/spool --to s3://bucket/prefix [--verify-only]` | copy/verify the spool (sha256 per object) |
| `alelt topology lease-clear --connection <id>` | return a connection to standalone capture (refused while a lease is live) |

Rules: run during a quiet window (drain pauses capture; lag grows by the drain time); keep the old stack stopped, not
deleted, until recon is green; abort = `alelt topology abort` (resumes agents on the old hub).

## 6. Agent active/passive (Db2 HADR primary + standby hosts)

| # | Step |
|---|---|
| 1 | Install `aleltagent.exe` on both Db2 hosts ([runbooks/install-agent.md](../runbooks/install-agent.md)); **different `agent_id`, same `connection_id`** |
| 2 | In both `aleltagent.json`: `"ha": {"mode": "active_passive", "lease_ttl_s": 30, "renew_s": 10}` (dev edge `devedge.json`: same object) |
| 3 | Both point to the hub LB URL; both register; one acquires the lease (console *Topology & HA* → holder, epoch), the other heartbeats `STANDBY` |
| 4 | Service recovery actions are set by `--install` (restart 60/120/300 s); keep them |
| 5 | Drill: [runbooks/agent-failover-drill.md](../runbooks/agent-failover-drill.md) |

Behaviour: the active renews every `renew_s`; if a renew fails for `lease_ttl_s - renew_s` or the hub answers 409/423 it
**stops reading immediately** and turns standby. The standby acquires after the TTL expires (epoch + 1,
`takeover: true`), discards its local spool, resumes at the hub durable position and starts capturing. Batches from the
old holder are rejected (`423 lease_fenced`) — no split brain, no gap (`from_pos` chain check, X-25). After a Db2 HADR
takeover the agent on the new primary is the one that can read the log; the other one's reads fail and it loses the lease.
Pauses received while standby are kept. Force a failover: console *Topology & HA* → *Release lease* (reason required):
the holder is cleared at once, its next renew gets 409 (a released lease is never revived), the next grant carries
epoch + 1, and for one TTL the released agent cannot re-acquire while a standby is polling. `/catalog` and `/snapshot`
from a non-holder are fenced (423) like `/blocks`; a standby that reports `CAPTURING` is shown as `STANDBY`;
`SNAPSHOT_TABLE` goes to the holder. `topology lease-clear` keeps the lease row, so epochs never restart at 1.
Re-acquiring one's own expired lease with nobody in between keeps the epoch (nobody else could have written). The
duplicate check of `/blocks` runs before the fence, so a stale holder re-sending a batch that is already stored gets
the duplicate ACK (harmless). Engines whose batches carry no `next_pos` resume at `to_pos` (overlap of one record,
removed by the decoder's idempotence); HA agents should always send `next_pos`.

## 7. RPO / RTO

| Failure | RPO | RTO (HA) | RTO (standalone) |
|---|---|---|---|
| Agent process crash | 0 | ≤ 60 s (service restart) | ≤ 60 s |
| Agent host / HADR primary lost | 0 (Db2 log) | ≈ `lease_ttl_s` + one poll (≈ 40 s default) | until host is back |
| One api replica | 0 | 0 (LB health check, ≤ 10 s) | minutes (restart) |
| One worker | 0 (job re-queued, checkpoint) | 0 (other worker takes the stream; stale job re-queued ≤ 10 min) | minutes |
| Hub host / zone | 0 | 0–30 s | hours (rebuild, [dr-rebuild-hub.md](../runbooks/dr-rebuild-hub.md)) |
| Metadata Postgres primary | 0 with sync replica (≤ seconds async) | provider/Patroni failover (≈ 30–60 s) | restore (hours) |
| Spool store | 0 (durable before ACK) | provider SLA; agents keep spooling (503 backpressure) | volume restore |
| Destination outage | 0 | loads resume when back (jobs retried with backoff) | same |

RPO 0 for replicated data always holds: the destination checkpoint is the truth (R6) and the source log is re-read
from the hub durable position.

## 8. Failure matrix

| Component down | Effect | Automatic recovery | Operator action |
|---|---|---|---|
| Active agent | capture stops | standby acquires lease after TTL, epoch + 1 | check old host; it rejoins as standby |
| Both agents | capture stops; Db2 keeps logs | service restart | fix host; watch archive retention headroom |
| Network agent ↔ hub | agent spools locally (cap → stop reading, never drop) | resend on reconnect; lease self-fencing if partition > TTL | none unless spool > 70 % |
| api replica | LB routes around it | orchestrator restarts it | none |
| All api replicas | agents spool; console down | restart | page on `AleltHubDown` |
| worker | its job is re-queued; per-stream lock released | other workers continue | none |
| Postgres primary | API/worker errors for the failover window; per-stream advisory locks dropped | HA failover; connections re-established (`pool_pre_ping`); a worker whose lock is gone aborts its run (`LockLost`) and the job is retried | verify replica lag |
| Bucket unreachable | `/readyz` fails; batches refused 503 | agents retry with backoff | fix store; no data lost |
| Destination | load jobs fail and retry | backoff retry | fix destination |
| LB | agents and console cannot reach the hub | redundant LB pair / cloud LB | — |
| Split brain attempt (old active alive) | its batches, catalogs and snapshot chunks → 423 | it stops reading and turns standby | none |
| Operator *Release lease* | holder's renew → 409 | standby acquires (epoch + 1); released agent waits one TTL | none |

## 9. Monitoring

| Signal | Where | Alert when |
|---|---|---|
| Lease changes | audit `agent.lease.acquire` / `agent.lease.takeover` / `agent.lease.release`; console *Topology & HA* (holder, epoch) | unplanned epoch change; no holder for > 2 × TTL |
| Fencing rejections | `alelt_batches_received_total{status="lease_fenced"}` | > 0 sustained (misconfigured pair) |
| Nodes | console *Topology & HA* → nodes `last_seen` (`hub_node`) | an api/worker not seen for > 60 s |
| Queue | `alelt_queue_depth{queue="decode"}`, `alelt_capture_lag_seconds` | above latency target |
| Agent | `alelt_agent_last_heartbeat_timestamp_seconds`, `alelt_spool_bytes{location="agent"}` | standby silent, spool > 70 % |
| Store / DB | `/readyz` per replica, Postgres replication lag, bucket errors | any replica not ready > 1 min |

## 10. Checklists

**Pre-install (HA)**
- [ ] Postgres HA endpoint (single DNS name that follows the primary), backups/PITR on, database + login created
- [ ] Bucket + scoped keys, versioning, encryption, lifecycle ≥ raw-archive retention
- [ ] ≥ 2 hosts/nodes in ≥ 2 failure domains; LB with `/readyz` checks and ≥ 120 s timeouts
- [ ] Secrets in the secret store / files, never in `.env` committed to Git (R9)
- [ ] Db2 archive log retention ≥ longest planned drain + failover window (DBA, R3)

**Go-live HA**
- [ ] `alelt topology status`: metadata, spool, api, workers all `ha: true`
- [ ] Kill one api and one worker: console stays up, lag recovers
- [ ] Agent pair: holder + standby visible; one planned failover done ([drill](../runbooks/agent-failover-drill.md))
- [ ] Recon green on all tables after the failover

**Monthly failover drill**
- [ ] Planned agent failover (stop active service) and one unplanned (kill process)
- [ ] Restart one api and one worker node during load
- [ ] Postgres switchover with the DBA team
- [ ] Record timings vs RTO table; audit entries present

**Switch day (standalone ↔ HA)**
- [ ] Quiet window agreed; DBA confirms log retention
- [ ] `topology plan` reviewed: no blockers
- [ ] `topology apply` completed: copy verified, row counts equal
- [ ] Old stack stopped (kept), new stack started with the generated config
- [ ] Agents RESUMED (console), lag back under target, recon green
- [ ] Old stack + volumes removed only after one full business day
