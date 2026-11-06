# ALELT — install with Docker (any host)

One image (`alelt`), one compose file. Runs on Docker Desktop (Windows/macOS, x64 or ARM64) and on any Linux
Docker host (on-prem, bare metal, any cloud VM). Cloud-agnostic: settings via `.env` / Docker secrets, Postgres by URL.

| Service | Role | Port |
|---|---|---|
| `alelt` | console + REST API + agent gateway (`alelt api`) | **8080** |
| `alelt-worker` | decode → transactions → load (`alelt worker`), C Db2 decoder | 9100 (metrics, internal) |
| `migrate` | one-shot: waits for the DB, creates the landing DB, upgrades the metadata schema | — |
| `postgres` | bundled Postgres 16: metadata `alelt` + landing `alelt_landing` (named volume `pg_data`) | host `ALELT_PG_HOST_PORT` (55432 default) |

Volumes: `pg_data` (Postgres), `alelt_data` (hub spool + logs, shared by api and worker).

## Install and start

```powershell
cd deploy/local                       # quote paths that contain '&':  cd "C:\src\R&D\alelt\deploy\local"
copy .env.example .env                # Linux: cp .env.example .env ; then edit POSTGRES_PASSWORD and ALELT_AGENT_TOKENS
docker compose up -d                  # builds alelt:local on first run (or pulls ALELT_IMAGE)
start http://localhost:8080           # console (dev auth: pick a role top right; PlatformAdmin sees "Setup")
```

Check: `docker compose ps` (all `healthy`, `migrate` exited 0) · `curl http://localhost:8080/version`.

| Task | Command |
|---|---|
| Stop (keep data) | `docker compose stop` (or `down`) |
| Start again | `docker compose up -d` |
| Reset everything (deletes metadata, landing and spool) | `docker compose down -v` |
| Logs | `docker compose logs -f alelt alelt-worker` · `docker compose logs migrate` |
| Version / decoder in use | `curl http://localhost:8080/version` (`decoders.db2.backend` = `native`) |
| Support bundle | console → *Hub & destination* → *Download support bundle* (or `GET /api/v1/support/bundle` as PlatformAdmin) |
| Shell | `docker compose exec alelt sh` |

## Metadata database: which one

| Choice | When | How |
|---|---|---|
| **Bundled Postgres** (default) | Evaluation and single-host production; hub + worker in separate containers, scale workers | `docker compose up -d` |
| **SQLite** | Quick start / demo on one host, smallest footprint; api + worker in one container (`alelt all`) | `docker compose -f docker-compose.sqlite.yml up -d` (set `ALELT_DEST_URL` or Snowflake settings in `.env`: no bundled Postgres in this mode) |
| **External Postgres** | Production with a managed or on-prem Postgres (any cloud), HA/backups owned by the DBA team | set `ALELT_DB_URL` (and `ALELT_DEST_URL`) in `.env`, then `docker compose up -d --no-deps migrate alelt alelt-worker` |

The console setup wizard (*Setup* → *Metadata database*) shows the current choice and tests a candidate URL. Switching
after data exists requires a migration (data is not moved automatically).

## Capture agent (on the Db2 host)

Db2 runs natively on Windows; the capture agent stays on the host and talks outbound HTTP(S) to the hub:

```powershell
.\start-agent.ps1                 # dev edge, continuous; token = first ALELT_AGENT_TOKENS in .env; hub http://localhost:8080
.\start-agent.ps1 -Once           # read to end of log, ship, exit
.\start-agent.ps1 -Config "C:\Downloads\devedge.json" -Token alelte_...   # files + one-time token from Setup → Agent enrollment
.\start-agent.ps1 -Native         # aleltagent.exe --console (when built: edge-agent\core\build.ps1)
```

Needs an x64 Python for the dev edge (`ALELT_EDGE_PYTHON`, default `%LOCALAPPDATA%\Programs\Python\Python310\python.exe`).

Examples and test corpora for this section are coming in a later release.

## First-run setup (console → Setup, PlatformAdmin)

1. Metadata database (current engine, test a URL). 2. Administrator (dev: local admin; OIDC: group → PlatformAdmin).
3. Destination: test + bootstrap via the destination connector (Postgres or Snowflake; credentials typed there are used
once, never stored — persist them in `.env` / secrets). 4. Agent enrollment: one-time token (shown once, bound to one
connection, expires unused after 24 h) + downloadable `devedge.json` / `aleltagent.json` (no secrets) + Db2 prerequisite
checklist (archive logging, DATA CAPTURE CHANGES, DBADM). 5. Next: *Import from existing platform* or *Create new pipeline*.

## Connect DBeaver / psql to the bundled Postgres

Host `localhost`, port `ALELT_PG_HOST_PORT` (default 55432), user `POSTGRES_USER`, password `POSTGRES_PASSWORD` from `.env`;
databases `alelt` (metadata) and `alelt_landing` (landing; schema = connection, e.g. `alelt_<connection_id>`).
`psql "postgresql://alelt:<password>@localhost:55432/alelt_landing"`.

## Build and upgrade

```powershell
..\build-image.ps1                                                   # host arch, loads alelt:local
..\build-image.ps1 -Platform linux/amd64 -Verify                     # amd64 from an ARM64 laptop + C tests + golden corpus
..\build-image.ps1 -Platform linux/amd64,linux/arm64 -Tag registry.example.com/alelt:0.2.0 -Push
..\build-image.ps1 -Platform linux/amd64 -Tag alelt:0.2.0 -Output tar   # dist\alelt-0.2.0-amd64.tar (offline)
```

Linux/macOS: `deploy/build-image.sh --platform linux/amd64 --tag alelt:0.2.0 --output tar`. The C decoder is
cross-compiled on the build machine (no emulation); `-Verify` runs its tests and the golden corpus on the target
architecture (QEMU emulation for a foreign arch, included in Docker Desktop).

Upgrade: set `ALELT_IMAGE` (e.g. `alelt:0.2.0`) in `.env` (or rebuild `alelt:local`), then
`docker compose up -d`. The `migrate` service runs first on every start and upgrades the metadata schema
(idempotent, additive); api and worker start only after it succeeded. Back up first: `docker compose exec postgres
pg_dumpall -U alelt > backup.sql`. Roll back = previous `ALELT_IMAGE` (schema changes are additive).

## Install on any Linux host

```bash
# Docker Engine 24+ with Compose v2; amd64 or arm64
docker load -i alelt-0.2.0-amd64.tar          # offline, or: docker pull <registry>/alelt:0.2.0
mkdir -p /opt/alelt && cd /opt/alelt      # copy docker-compose.yml, .env.example, initdb/ here
cp .env.example .env && chmod 600 .env              # set ALELT_IMAGE, POSTGRES_PASSWORD, ALELT_AGENT_TOKENS, ALELT_DEV_AUTH=0, OIDC
docker compose up -d --no-build
```

Production: `ALELT_ENV=prod`, `ALELT_DEV_AUTH=0` + OIDC settings, TLS in front (bundled nginx of `deploy/hub`, or any load
balancer), remove the Postgres host port mapping, agent mTLS (`ALELT_AGENT_AUTH=mtls`). Secrets as files: any
`ALELT_<NAME>_FILE=/run/secrets/<file>` is read into `ALELT_<NAME>` at start (Docker secrets, Kubernetes secrets, ...).

## Troubleshooting

| Symptom | Fix |
|---|---|
| Path errors with `C:\src\R&D\...` | Quote every path (`cd "C:\src\R&D\alelt\deploy\local"`); npm `.cmd` shims break on `&`, the image build does not use them |
| `ports are not available ... 55432` on Windows | Port inside a Windows/Hyper-V excluded range (`netsh int ipv4 show excludedportrange protocol=tcp`): set `ALELT_PG_HOST_PORT=15433` (or any free port) in `.env` |
| Port 8080 in use | `ALELT_HTTP_PORT=8081` in `.env`, agent `-HubUrl http://localhost:8081` |
| Containers gone / unhealthy after Docker Desktop restart | `docker compose up -d` (restart policy `unless-stopped` normally brings them back); data is in named volumes |
| `migrate` failed | `docker compose logs migrate` (DB not reachable / wrong password: `.env` changed after the first start → `down -v` to reset, or fix the password in Postgres) |
| Agent 401 | Token mismatch: `ALELT_AGENT_TOKENS` in `.env` vs `-Token`; enrollment tokens are one-time (bound to the first agent id) and expire unused after 24 h |
| Console shows no data | Dev auth: choose a role (top right); `DataOperator` / `PlatformAdmin` see tables and batches |
| `decoders.db2.backend` = `python` | The C library failed to load (see worker logs, `db2 decoder backend` line); the Python decoder is the fallback |
