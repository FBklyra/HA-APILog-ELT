# ALELT v1.0.0 — Download and deploy the container image

This guide gets the image onto your machine or into your registry. Then you choose where to run it:

| Target | Section |
|---|---|
| Your laptop (Docker Desktop) for a quick test | [4A](#4a-docker-desktop--local-test) |
| Any Docker-compatible host (Docker Engine, Podman, Rancher Desktop, colima) | [4B](#4b-any-docker-compatible-host) |
| Pre-production / production through your CI/CD | [4C](#4c-cicd--preprod--prod) |
| Kubernetes (EKS, AKS, GKE, OpenShift, on-prem) | [4D](#4d-kubernetes-helm) |

The image is **linux/amd64 (x64)**. One image serves every role:

| Role | Command |
|---|---|
| hub API + console | `api` |
| worker | `worker` |
| schema migrations (one-shot) | `migrate` |
| API + worker in one container | `all` |
| print the version | `version` |

---

## 1. Requirements

- Docker 24+ with Compose v2, or any OCI-compatible runtime (Podman 4+, containerd, Kubernetes).
- An x64 host. On Apple Silicon or Windows ARM, Docker Desktop runs the image under emulation: fine for testing, slow for load.
- About 1 GB of free disk for the image.
- PostgreSQL 14+ for metadata (bundled in the quick starts). SQLite is for evaluation only.

---

## 2. Download the image — pick one

### Option 1: pull from GitHub Container Registry (online)

```sh
docker pull ghcr.io/fbklyra/ha-apilog-elt:1.0.0
```

### Option 2: download the release asset (single file)

From the GitHub release **v1.0.0**, download `ha-apilog-elt-1.0.0-linux-amd64.tar.gz` and `SHA256SUMS`, then:

```sh
sha256sum -c SHA256SUMS --ignore-missing          # PowerShell: Get-FileHash -Algorithm SHA256 <file>
docker load -i ha-apilog-elt-1.0.0-linux-amd64.tar.gz
```

### Option 3: from the repository (offline / air-gapped)

The image is stored in `precompiled/docker/` as 90 MB parts, because GitHub limits files to 100 MB.

```sh
git clone https://github.com/FBklyra/HA-APILog-ELT.git
cd HA-APILog-ELT/precompiled/docker
sh join.sh                      # Windows PowerShell: .\join.ps1   (joins the parts and verifies the checksum)
docker load -i ha-apilog-elt-1.0.0-linux-amd64.tar.gz
```

### Check it

```sh
docker image ls ghcr.io/fbklyra/ha-apilog-elt
docker run --rm ghcr.io/fbklyra/ha-apilog-elt:1.0.0 version
```

---

## 3. Push it to your own registry (recommended for CI/CD and cloud)

Deploy from **your** registry, not from GHCR, so that preprod and prod do not depend on an external site.

```sh
SRC=ghcr.io/fbklyra/ha-apilog-elt:1.0.0
DST=<your-registry>/alelt/ha-apilog-elt:1.0.0     # examples below

docker tag  $SRC $DST
docker push $DST
```

| Registry | `DST` example | Login |
|---|---|---|
| AWS ECR | `123456789012.dkr.ecr.<region>.amazonaws.com/alelt/ha-apilog-elt:1.0.0` | `aws ecr get-login-password \| docker login --username AWS --password-stdin <acct>.dkr.ecr.<region>.amazonaws.com` |
| Azure ACR | `<name>.azurecr.io/alelt/ha-apilog-elt:1.0.0` | `az acr login -n <name>` |
| Google Artifact Registry | `<region>-docker.pkg.dev/<project>/alelt/ha-apilog-elt:1.0.0` | `gcloud auth configure-docker <region>-docker.pkg.dev` |
| Harbor / Nexus / Artifactory / GitLab | `<host>/<project>/ha-apilog-elt:1.0.0` | `docker login <host>` |

Copy without a local Docker daemon (CI runners):

```sh
skopeo copy docker-archive:ha-apilog-elt-1.0.0-linux-amd64.tar.gz docker://$DST
# or
crane push ha-apilog-elt-1.0.0-linux-amd64.tar.gz $DST
```

Pin deployments by digest, not only by tag:

```sh
docker inspect --format '{{index .RepoDigests 0}}' $DST
```

---

## 4. Deploy — choose your target

All compose bundles are in `precompiled/` (`quickstart-sqlite`, `quickstart-postgres`, `quickstart-ha`).
To use your own registry, set `ALELT_IMAGE=<your-registry>/alelt/ha-apilog-elt:1.0.0` in `.env`.

### 4A. Docker Desktop — local test

**Fastest (SQLite, about 2 minutes):**

```sh
cd precompiled/quickstart-sqlite
cp .env.example .env
docker compose up -d
```

Open http://localhost:8080.

**Closer to production (PostgreSQL, created automatically):**

```sh
cd precompiled/quickstart-postgres
cp .env.example .env
# edit .env: set POSTGRES_PASSWORD and ALELT_AGENT_TOKENS to long random values
#   python -c "import secrets; print(secrets.token_hex(24))"
docker compose up -d
docker compose ps        # wait until "alelt" is healthy
```

What gets created on first start:

- the PostgreSQL user and metadata database;
- the landing database;
- all schema migrations, applied by the one-shot `migrate` service.

Endpoints:

| What | Where |
|---|---|
| Console / API | http://localhost:8080 |
| Health | `/healthz` (liveness), `/readyz` (readiness) |
| Metrics | `/metrics` |
| PostgreSQL (for psql / DBeaver) | `127.0.0.1:55432` |

Stop with `docker compose stop`. Remove everything, including data, with `docker compose down -v`.

### 4B. Any Docker-compatible host

The same compose files work with Docker Engine on a Linux VM, and with Podman, Rancher Desktop and colima.

```sh
# Podman
podman load -i ha-apilog-elt-1.0.0-linux-amd64.tar.gz
podman compose up -d          # or: podman-compose up -d
```

On a shared or remote host:

- keep `ALELT_BIND=127.0.0.1` and put a reverse proxy with TLS in front;
- or set `ALELT_BIND=0.0.0.0` **only** with `ALELT_DEV_AUTH=0`, `ALELT_ENV=prod` and OIDC configured.

### 4C. CI/CD — preprod / prod

Recommended pipeline:

1. **Import:** pull or load the image (section 2), verify `SHA256SUMS`, push it to your registry (section 3).
2. **Scan:** run your image scanner (for example Trivy, Grype, ECR/ACR/GAR scanning) on the digest.
3. **Configure per environment:** keep one `.env` per environment in your secret store. Do not commit it.
   - Secrets can also come from files: any `ALELT_<NAME>_FILE=/run/secrets/<x>` is read into `ALELT_<NAME>` (Docker or Kubernetes secrets).
4. **Migrate first:** run the image with command `migrate` as a one-shot job before rolling out the API and workers.
5. **Roll out:** start `api` (behind a load balancer, health check `/readyz`) and `worker` replicas.
6. **Verify:** `GET /readyz` returns 200; open the console; check `/metrics`.

Minimum production settings:

| Setting | Value |
|---|---|
| `ALELT_ENV` | `prod` |
| `ALELT_DEV_AUTH` | `0` |
| `ALELT_OIDC_ISSUER` / `ALELT_OIDC_AUDIENCE` / `ALELT_OIDC_JWKS_URL` | from your identity provider |
| `ALELT_DB_URL` | managed PostgreSQL (HA), e.g. `postgresql+psycopg://user:pass@host:5432/alelt` |
| `ALELT_DEST_URL` or the `ALELT_SNOWFLAKE_*` settings | your destination |
| `ALELT_AGENT_TOKENS` | long random values, or enrol agents from the console |
| `ALELT_IMAGE` | your registry path, pinned by digest |

High availability (two or more API replicas, two or more workers, load balancer, external PostgreSQL, S3-compatible spool):
use `precompiled/quickstart-ha` (Compose or Swarm) and read its `README-HA.md` and `QUICKSTART.md`.

Generic CI example (adapt to GitHub Actions, GitLab CI, Azure DevOps or Jenkins):

```sh
docker pull ghcr.io/fbklyra/ha-apilog-elt:1.0.0
docker tag  ghcr.io/fbklyra/ha-apilog-elt:1.0.0 $REGISTRY/alelt/ha-apilog-elt:1.0.0
docker push $REGISTRY/alelt/ha-apilog-elt:1.0.0
# deploy step (compose host)
ssh deploy@$HOST "cd /opt/alelt && ALELT_IMAGE=$REGISTRY/alelt/ha-apilog-elt:1.0.0 docker compose run --rm migrate && docker compose up -d"
```

Managed container services (ECS/Fargate, Azure Container Apps, Cloud Run):

- create one service with command `api` (port 8080, health `/readyz`);
- create one with command `worker`;
- run a one-shot task with command `migrate` before each rollout;
- use the same environment variables as above.

### 4D. Kubernetes (Helm)

A Helm chart is in `code/deploy/helm/alelt`.

```sh
helm upgrade --install alelt code/deploy/helm/alelt \
  --namespace alelt --create-namespace \
  --set image.repository=<your-registry>/alelt/ha-apilog-elt \
  --set image.tag=1.0.0
```

Review `values.yaml` for replicas, secrets, ingress and the external PostgreSQL / S3 settings before using it in preprod or prod.

---

## 5. Capture agents (next to your source databases)

The containers above are the hub. Changes are captured by an agent installed close to each source database:

| Agent | Package | Install |
|---|---|---|
| Windows service | `precompiled/windows-x64/aleltagent-1.0.0-windows-x64.zip` | `install-agent.ps1` (run as administrator) |
| Linux daemon | `precompiled/linux-x64/aleltd-1.0.0-linux-x64.tar.gz` | `sudo ./install.sh` |

Point the agent at the hub URL and give it an agent token. See `README-precompiled.md`.

---

## 6. Upgrade and rollback

- **Upgrade:**
  1. Push the new tag to your registry.
  2. Run `migrate` with the new image.
  3. Roll out `api`, then `worker`.
- **Rollback:** redeploy the previous digest. Check `code/deploy/UPGRADE.md` first: a schema migration may not be reversible.

---

## 7. Troubleshooting

| Symptom | Check |
|---|---|
| `exec format error` | The host is not x64. Use an amd64 host, or Docker Desktop with emulation. |
| `alelt` never healthy | `docker compose logs migrate alelt`. Usually the database URL or password is wrong. |
| `POSTGRES_PASSWORD` error on start | `.env` is missing: `cp .env.example .env` and set the values. |
| Port 8080 in use | Set `ALELT_HTTP_PORT` in `.env`. |
| Joined image checksum mismatch | Download the parts again; every part must be complete. |

Status: early release. See `code/KNOWN-ISSUES.md` (all listed issues are being fixed).
