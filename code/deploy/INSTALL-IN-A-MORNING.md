# Install ALELT in less than a morning

> Goal: from zero to a first verified **TEST** pipeline in ≈ 4 hours. Cloud-neutral: any Linux VM, Swarm, Kubernetes or on-prem.
> Public images and binaries are **linux/amd64** (hub) and **x64** (Windows agent, Linux daemon).
> Provided "AS IS", no warranty (`DISCLAIMER.md`). Start against **test** systems only; production needs your own approvals (change board, DBA, security).

## 0. Pick a shape (5 min)

| Shape | When | Metadata | Spool | Files |
|---|---|---|---|---|
| Single Linux VM | evaluation, small installs, lab | bundled Postgres (or SQLite) | local volume | `deploy/local` |
| Docker Swarm (≥ 2 hosts) | HA without Kubernetes | external Postgres HA | S3-compatible bucket | `deploy/ha` |
| Kubernetes (Helm) | you already run K8s | external Postgres HA | S3-compatible bucket | `deploy/helm/alelt` (`values-ha.yaml`) |
| On-prem | no cloud | same as above on your VMs / Postgres / MinIO or Ceph | | any of the above |

Standalone ↔ HA can be switched later (`alelt topology plan|apply --to ha`, `deploy/HA.md`).

## 1. Timeline

| # | Step | Time box | Done when |
|---|---|---|---|
| 1 | Prerequisites | 45 min | checklist §2 all ticked |
| 2 | Get the release + pull images | 15 min | `docker image ls alelt` shows the version |
| 3 | Configure `.env` / secrets | 20 min | no `change-me` left; secrets as files |
| 4 | Start (standalone or HA) | 15 min | `GET /readyz` = 200 |
| 5 | Setup wizard (console) | 20 min | *Setup* complete, destination bootstrapped |
| 6 | Install the capture agent (edge sources) | 30 min | agent registered + heartbeating |
| 7 | First pipeline (wizard, TEST) | 30 min | initial load done, changes flowing |
| 8 | Verify | 30 min | recon OK, failover/restart drill OK |
| | **Total** | **≈ 3 h 25 min** | |

## 2. Prerequisites (45 min)

| Item | Single VM | Swarm / Kubernetes |
|---|---|---|
| Compute | 1 VM, 2–4 vCPU, 8 GB RAM, 50 GB SSD, x86-64 Linux (Ubuntu 24.04 / RHEL 9 / Debian 12) | ≥ 2 nodes (or a namespace), same sizing per api/worker replica |
| Container runtime | Docker Engine ≥ 24 + Compose v2 | Swarm: Docker ≥ 24 on each node · K8s ≥ 1.25 + Helm 3 |
| Metadata DB | bundled | Postgres ≥ 14, HA (managed or Patroni), database `alelt` + login with CREATE |
| Object storage | — | S3-compatible bucket (versioning, SSE, lifecycle ≥ raw-archive retention) + scoped keys |
| Load balancer / ingress | optional (port 8080) | LB or ingress: TLS, health `GET /readyz`, read timeout ≥ 120 s, body ≥ 64 MB, mTLS for agents |
| Network | outbound from source hosts **to** the hub (443/8443) — the agent never listens | same |
| Identity | dev auth for evaluation; OIDC issuer for real users | OIDC issuer |
| Source access | TEST database, read-only capture user, log/CDC prerequisites (the wizard checks and prints fix-it SQL) | same |
| Vendor client libraries | obtained by you under the vendor licence (e.g. IBM Db2 client on the agent host) — never shipped | same |
| Approvals | test-system owner OK | + change board if anything touches shared infra |

Cloud examples (not requirements):

| Need | AWS | Azure | GCP | On-prem |
|---|---|---|---|---|
| Managed Postgres | RDS / Aurora PostgreSQL | Azure Database for PostgreSQL – Flexible | Cloud SQL for PostgreSQL | Patroni / repmgr |
| Object storage (S3 API) | S3 | Blob via an S3 gateway | Cloud Storage (XML interop, HMAC keys) | MinIO / Ceph RGW |
| Load balancer | ALB / NLB | Application Gateway / Load Balancer | Cloud Load Balancing | HAProxy / F5 / nginx |
| Kubernetes | EKS | AKS | GKE | any CNCF distribution |

## 3. Get the release + images (15 min)

| Option | Commands |
|---|---|
| Registry | `docker pull <registry>/<namespace>/alelt:<version>` · verify: `cosign verify <registry>/<namespace>/alelt:<version> --certificate-identity-regexp '<repo>' --certificate-oidc-issuer <issuer>` |
| Offline (air-gapped) | download release assets → `sha256sum -c SHA256SUMS` → `docker load -i alelt-<version>-linux-amd64.image.tar` |
| Bundles | `tar xzf alelt-compose-<version>.tar.gz` (compose) · `alelt-helm-<version>.tgz` (Helm) |

## 4. Configure (20 min)

| Setting | Standalone (`deploy/local/.env`) | HA (`deploy/ha/.env`, Helm secrets) |
|---|---|---|
| Image | `ALELT_IMAGE=<registry>/<ns>/alelt:<version>` | same / `image.repository`, `image.tag` |
| Metadata | `POSTGRES_PASSWORD` (bundled) or `ALELT_DB_URL` | `ALELT_DB_URL=postgresql+psycopg://user:pw@pg-ha:5432/alelt` |
| Spool | default local volume | `ALELT_SPOOL_URL=s3://bucket/prefix` + `ALELT_S3_ENDPOINT`, `ALELT_S3_REGION`, keys |
| Destination | `ALELT_DEST_URL` (landing) | same |
| Auth | evaluation: `ALELT_DEV_AUTH=1`, `ALELT_ENV=local` · real users: `ALELT_DEV_AUTH=0`, `ALELT_ENV=prod`, `ALELT_OIDC_ISSUER/AUDIENCE/JWKS_URL` | OIDC only |
| Agents | `ALELT_AGENT_TOKENS` or one-time enrollment tokens from the wizard | + mTLS thumbprints at the LB |
| Prod guard | `ALELT_PROD_DENYLIST=<prod hosts, *.prod.example, CIDRs>` | same |

- Secrets as files: any `ALELT_<NAME>_FILE=/run/secrets/x` is read into `ALELT_<NAME>` (Docker / K8s secrets). Never commit `.env`.
- Generate tokens: `python3 -c "import secrets; print(secrets.token_hex(24))"`.

## 5. Start (15 min)

| Shape | Commands | Check |
|---|---|---|
| Single VM | `cd deploy/local && cp .env.example .env && docker compose up -d` (SQLite: `-f docker-compose.sqlite.yml`) | `curl -fsS localhost:8080/readyz` |
| Swarm | `cd deploy/ha && cp .env.ha.example .env` → `set -a; . ./.env; set +a; docker stack deploy -c docker-compose.ha.yml -c docker-compose.ha.secrets.yml alelt` | `docker service ls` = all replicas up · LB `/readyz` |
| Kubernetes | `kubectl create secret generic alelt-db --from-literal=ALELT_DB_URL=...` · `kubectl create secret generic alelt-s3 ...` · `kubectl create secret generic alelt-hub ...` → `helm upgrade --install alelt alelt-helm-<version>.tgz -f values-ha.yaml` (edit `image`, `spool.url`, `s3.endpoint`, `ingress.hosts`) | `kubectl get pods` Ready · ingress `/readyz` |
| On-prem | any row above on your own VMs | same |

- Migrations run automatically (`migrate` one-shot service / Helm hook job).
- HA check: `alelt topology status` → every layer `ha: true`; console *Topology & HA* shows ≥ 2 api and ≥ 2 worker nodes.

## 6. Setup wizard (20 min) — console `https://<hub>/setup` (PlatformAdmin)

| Step | What |
|---|---|
| Metadata | shows backend; test a candidate URL |
| Admin | first PlatformAdmin |
| Destination | test + bootstrap (control schema, landing schema) |
| Prerequisites | per-source checks with fix-it SQL for the DBA (nothing is executed on the source) |
| Agents | create a one-time enrollment token bound to one connection (expires in 24 h) |
| Guard | production deny list (hosts, wildcards, CIDRs) |

## 7. Install the capture agent (30 min) — only for edge sources (e.g. Db2); remote sources need no agent

| Host | Install |
|---|---|
| Windows x64 (MSI) | `msiexec /i alelt-agent-<version>-windows-x64.msi /qn HUBURL=https://<hub> AGENTID=<id> CONNECTIONID=<cid> DATABASE=<TESTDB> CERTTHUMBPRINT=<sha1> HUBPIN=<sha256>` → service `ALELTAgent` |
| Windows x64 (zip) | unzip → elevated `install-agent.ps1 -SourceDir . -HubUrl https://<hub> -AgentId <id> -Database <TESTDB> ...` → `verify-agent.ps1` |
| Linux x64 | `tar xzf alelt-agentd-<version>-linux-x64.tar.gz && cd alelt-agentd-<version> && sudo ./deploy/linux/install.sh` (agent only; `--engines "postgres mysql"` for client libs, `--wheels DIR` offline) → edit `/etc/alelt` config → systemd unit `alelt.service` running `aleltagentd` |

- Agent rules: outbound only, read-only, whitelisted commands, CPU/memory caps, refuses deny-listed hosts.
- Active/passive (optional): install a second agent on the standby host with `ha.mode=active_passive`.

## 8. First pipeline (30 min) — console *New pipeline* (`/wizard`)

| Step | What |
|---|---|
| Source | engine, host, TEST database, capture user (wizard runs the prerequisite check) |
| Destination | landing schema |
| Tables | pick tables; latency target; output profile |
| Create | TEST-only parallel pipeline; initial snapshot then log capture from the snapshot position |

## 9. Verify (30 min)

| Check | Where | Pass |
|---|---|---|
| Changes flow | *Overview*, *Pipelines* | lag below target, no quarantined rows |
| Reconciliation | *Recon* | counts + aggregates + samples match |
| Restart drill | restart the worker / agent | no gap, no duplicate (exactly-once checkpoint) |
| HA drill (HA shapes) | kill one api + one worker; *Topology & HA* forced failover | traffic continues; standby agent takes the lease |
| Support bundle | *Hub & destination* → support bundle | zip downloads, secrets redacted |
| Backups | metadata DB backup + bucket versioning | restore tested once |

## Troubleshooting (fast paths)

| Symptom | Look at |
|---|---|
| `/readyz` not 200 | `docker compose logs migrate alelt` — DB URL / password |
| Agent 401/403 | enrollment token expired or bound to another connection; thumbprint not pinned |
| Agent 409 `PROD_TARGET` | host is on the production deny list (by design) |
| Source prerequisite FAIL | apply the fix-it SQL shown by the wizard (DBA, on the TEST system) |
| Lag grows | worker count, destination warehouse size, `Incidents` diagnosis |
