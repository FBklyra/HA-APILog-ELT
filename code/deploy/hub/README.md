# deploy/hub — ALELT hub, production topology (Docker Compose, any Linux host)

Cloud-neutral: runs on any Docker host — on-prem VM or bare metal, or a VM in any cloud. Only portable building
blocks: OCI images, Docker Compose, env / `.env` / Docker secrets, Postgres by URL (bundled or any managed Postgres),
TLS at the bundled nginx or at any external load balancer. No cloud SDK or cloud service is required.

Simpler single-image install (evaluation, small sites): [`deploy/local`](../local/README.md) (`alelt` product image,
`docker compose up -d`). Install procedure for this topology: [`runbooks/install-hub.md`](../../runbooks/install-hub.md).
DR: [`runbooks/dr-rebuild-hub.md`](../../runbooks/dr-rebuild-hub.md).

## Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | postgres 16, migrate (one-shot), hub-api, worker ×N, nginx (TLS + console), prometheus, alertmanager, pg-backup |
| `Dockerfile.hub` | Python 3.12 image for hub-api / worker / migrate (the product image `deploy/image/Dockerfile` can be used instead: `alelt api|worker|migrate`) |
| `nginx/Dockerfile`, `nginx/nginx.conf` | Builds `console/` (Vite → `dist`) and serves it; proxies `/api/`, `/agent/` (mTLS, WebSocket), `/healthz`, `/readyz`; `/metrics` local only |
| `postgres/postgresql.conf` | Tuning + WAL archiving |
| `backup/` | `pg_dump_loop.sh` (nightly, in container), `basebackup.sh` (weekly, host cron), `offsite-sync.sh` (optional example: copy dumps off-site; replace the copy command with your object store / backup tool, S3-compatible or other) |
| `secrets/fetch-secrets.sh` | Optional example: materialise secrets from a secret store into tmpfs files. Any mechanism that produces the files works (Docker secrets, Vault, sops, a provider secret store, manual) |
| `observability/` | `prometheus.yml`, `alerts.yml`, `alertmanager.yml`, `METRICS.md` (metric contract with `alelt_hub`) |
| `.env.example` | Non-secret settings |

## Topology

| Service | Networks | Exposed | Health check |
|---|---|---|---|
| nginx | frontend, backend | **443** | `GET /healthz` via TLS |
| hub-api | backend, egress | — (8080 internal) | `GET /readyz` |
| worker ×`ALELT_WORKERS` | backend, egress | — (9100 metrics internal) | heartbeat file `/tmp/alelt_worker_alive` < 120 s |
| postgres | backend (internal) | — | `pg_isready` |
| prometheus / alertmanager | backend, monitoring | 127.0.0.1:9090 / :9093 (SSH tunnel) | — |
| pg-backup | backend | — | — |

- Scale workers: `docker compose up -d --scale worker=6` (or `ALELT_WORKERS` in `.env`).
- Entrypoints are overridable (`ALELT_API_CMD`, `ALELT_WORKER_CMD`, `ALELT_MIGRATE_CMD`).
- Two SNI hosts on 443: console host (browser, OIDC JWT) and agent host (`ssl_verify_client on`, internal CA). nginx forwards the client-cert SHA-1 as `X-ALELT-Client-Cert-Thumbprint`; the hub matches it against `ALELT_AGENT_THUMBPRINTS` (pinning) and strips the header on the console host. An external load balancer may terminate TLS instead if it forwards the verified client-cert fingerprint in the same header.
- Managed Postgres: drop the `postgres` / `pg-backup` services and point `ALELT_DB_URL` at the managed instance (backups then follow the provider's tooling).

## Host sizing (any cloud or on-prem)

| Item | Recommendation |
|---|---|
| OS | Any current Linux with Docker Engine 24+ and Compose v2 (e.g. Ubuntu 24.04 LTS, RHEL 9, Debian 12); amd64 or arm64 |
| CPU / RAM | 8 vCPU / 32 GB for one large Db2 source; 4 vCPU / 16 GB for evaluation |
| Disks | `/data/pg` 256 GB SSD (Postgres), `/data/spool` 512 GB SSD (raw batch spool, ≥ 72 h hub-side backlog), `/data/backup` 512 GB (dumps, base backups, WAL) + off-site copy |
| Inbound | **443/tcp only**, from the source agent's egress IP and the admin/data-team range. SSH only through your bastion / jump host |
| Outbound | Destination (e.g. Snowflake account host), identity provider (OIDC JWKS), secret store, backup target, alert webhooks / SMTP, container registry |
| Secrets | Files under `ALELT_SECRETS_DIR` (pg password, destination key, alert credentials, TLS server cert/key, agent CA + CRL); rotation: [`runbooks/rotate-certs-keys.md`](../../runbooks/rotate-certs-keys.md) |
| Backups | Postgres nightly `pg_dump` + weekly base backup + continuous WAL archive → off-site, immutable storage; config YAML lives in Git; destination control tables use the destination's own time travel / backups |
| Monitoring | Prometheus + Alertmanager on the host; optional host agent of your monitoring platform |

Provider-specific notes (VM sizes, network security groups, managed identities) are optional and not required by
ALELT; the earlier Azure-specific notes in the runbooks are legacy reference only.

## Validate

```bash
cp .env.example .env
docker compose config --quiet      # syntax + interpolation
```
