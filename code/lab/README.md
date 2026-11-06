# Local test lab

Throw-away sources and targets with **fake data**, on your machine, in a few commands — or from the admin console (*Local test lab* screen). Zero contact with any real system.

## Quick start

| Step | Windows (PowerShell) | Linux / macOS |
|---|---|---|
| See the catalog | `.\lab\lab.ps1 list` | `./lab/lab.sh list` |
| Pre-pull images (retries, timeouts) | `.\lab\lab.ps1 pull postgres mysql landing` | `./lab/lab.sh pull postgres mysql landing` |
| Start (waits for health checks) | `.\lab\lab.ps1 up postgres mysql mariadb landing` | `./lab/lab.sh up postgres mysql mariadb landing` |
| Status | `.\lab\lab.ps1 status` | `./lab/lab.sh status` |
| Demo pipeline end-to-end | `.\lab\lab.ps1 demo mysql` | `./lab/lab.sh demo mysql` |
| Stop + remove containers (keep data) | `.\lab\lab.ps1 down` | `./lab/lab.sh down` |
| Remove containers **and** data | `.\lab\lab.ps1 reset` | `./lab/lab.sh reset` |
| Console helper (optional) | `.\lab\lab.ps1 helper` | `./lab/lab.sh helper` |

Profile ids may be combined; `all`, `sources`, `targets` are shortcuts. Start a few at a time — Oracle, Db2 and SQL Server each need 2–4 GB RAM.

Raw compose (no wrapper): `COMPOSE_PROFILES=postgres,landing docker compose -p alelt-lab -f lab/docker-compose.lab.yml up -d --wait`.

## Catalog

Single source of truth: [`catalog.yaml`](catalog.yaml) (scripts read the generated `catalog.json`; the console imports the generated TypeScript). After editing: `python lab/gen_catalog.py`; `tests/static/test_lab_catalog.py` checks catalog ↔ compose ↔ generated files.

| Id | Kind | Image (pinned) | Arch | Host port(s) | Connector status |
|---|---|---|---|---|---|
| postgres | source | `postgres:16.8-bookworm` (wal_level=logical) | amd64, arm64 | 47432 | beta |
| mysql | source | `mysql:9.3.0` (ROW, GTID) | amd64, arm64 | 47306 | beta |
| mariadb | source | `mariadb:11.4.5` (ROW binlog) | amd64, arm64 | 47307 | beta |
| sqlserver | source | `mcr.microsoft.com/mssql/server:2022-CU20-ubuntu-22.04` (CDC, Agent on) | amd64 | 47143 | beta |
| oracle | source | `container-registry.oracle.com/database/free:23.7.0.0-lite` (ARCHIVELOG + supplemental logging) | amd64, arm64 | 47521 | planned |
| db2 | source | optional: native Db2 Community install (recommended), or `icr.io/db2_community/db2:12.1.2.0` on x64 with `LAB_DB2_CONTAINER=1` (`--privileged`, required by the image) | amd64 | 47500 | beta |
| landing | target | `postgres:16.8-bookworm` | amd64, arm64 | 47442 | beta |
| iceberg | target | `chrislusf/seaweedfs:3.85` (S3) + `apache/iceberg-rest-fixture:1.9.1` + `trinodb/trino:476` | amd64, arm64 | 47900, 47181, 47808 | beta |
| delta | target | `chrislusf/seaweedfs:3.85` (S3) | amd64, arm64 | 47900 | beta |
| clickhouse | target | `clickhouse/clickhouse-server:25.3.6.56` | amd64, arm64 | 47823, 47824 | beta |
| redpanda | target | `redpandadata/redpanda:v24.3.7` (Kafka API) | amd64, arm64 | 47092, 47644 | beta |
| bigquery | target | `ghcr.io/goccy/bigquery-emulator:0.6.6` (community emulator) | amd64 | 47050, 47060 | planned |
| files | target | none — DuckDB / SQLite files in `lab/data/` | any | — | beta |

Cloud-only targets (no local image) — guidance in the console and in `catalog.yaml` (`availability: no_image`): Snowflake trial, Databricks Free Edition, Microsoft Fabric trial, Amazon Redshift Serverless free trial, Google BigQuery sandbox, Oracle Cloud Free Tier.

Architecture: amd64-only images run under emulation on ARM64 machines (slow; SQL Server and Db2 may not start there). On x64 machines everything runs natively.

### Images already on the machine, machine overrides (`lab/.env.local`)

- `lab up` / `lab pull` (and the console helper) pick per image: a value in the environment or in `lab/.env.local` > the pinned tag if present > the first present compatible local tag from the catalog entry's `local_fallbacks` (printed: "X not present locally - using local Y") > pull the pinned tag. `lab pull` pulls only images with no compatible local tag. Public defaults stay pinned.
- `lab/.env.local` (gitignored, `KEY=value` lines) is passed to compose as-is: `LAB_IMG_*`, `LAB_PORT_*`, `LAB_PLATFORM_SQLSERVER` / `LAB_PLATFORM_BIGQUERY` (`linux/arm64` for an ARM-native local image), `LAB_SQLSERVER_AGENT=false`.
- `lab/.env.local` never leaves the machine: gitignored, excluded from the image build context (`.dockerignore`: `lab`, `**/.env.*`) and refused by the release deny list (`deploy/release/build-release.*`). It may name `:latest` local images; that is lab drift only, never a product default (DEP-33). The Db2 container is `--privileged` only because the IBM image requires it; it is bound to 127.0.0.1 and runs only with `LAB_DB2_CONTAINER=1`.
- ARM64 laptops: `LAB_IMG_SQLSERVER=mcr.microsoft.com/azure-sql-edge:latest` + `LAB_PLATFORM_SQLSERVER=linux/arm64` + `LAB_SQLSERVER_AGENT=false` (SQL Edge has no sqlcmd inside: the lab init runs from the host with `lab/sqlserver_init.py` (pyodbc + ODBC Driver 18); no Agent capture job: the demo kit runs `sys.sp_cdc_scan` itself, test only). `LAB_IMG_BIGQUERY=ghcr.io/goccy/bigquery-emulator:latest` + `LAB_PLATFORM_BIGQUERY=linux/arm64`.
- Db2 is optional: never part of `all` / `sources`; `up db2` starts the container only with `LAB_DB2_CONTAINER=1` (x64). Machines with a native Db2 use it as the lab Db2 source.
- S3 (SeaweedFS) creates bucket `lake` itself before it reports healthy (no one-shot init container — `compose up --wait` fails on exited containers). The Iceberg REST server runs with `AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED` (SeaweedFS 3.85 stores aws-chunked checksum trailers verbatim otherwise).

Sample data: schema `shop` (`customers` 200 rows, `orders` 1 000, `order_items` 3 000; fewer tables on Oracle / Db2), created on first start by `lab/init/<engine>/`. A capture user/role with the documented minimum grants is created too.

## Safety

| Rule | How |
|---|---|
| Isolated | compose project `alelt-lab`, network `alelt-lab`, volumes `alelt-lab-*`, containers `alelt-lab-*`; nothing else is touched |
| Local only | every port is bound to `127.0.0.1`; throw-away passwords |
| No Docker socket in containers | the hub never gets the Docker socket; the console drives the lab only through the host-side helper |
| Ports | 47xxx, outside the usual Windows reserved ranges; `up` re-checks with `netsh int ipv4 show excludedportrange protocol=tcp` and refuses busy/reserved ports. Override: `LAB_PORT_<NAME>` (e.g. `LAB_PORT_POSTGRES=48432`; the demo and console assume defaults) |
| Images | official public images, pinned tags; overridable with `LAB_IMG_<NAME>` |
| Idempotent | re-running `up` is safe; `down` keeps data; `reset` deletes it |

## Demo pipeline

`lab demo <postgres|mysql|mariadb|sqlserver>` runs an end-to-end harness (path in `ALELT_DEMO_HARNESS`) against the lab: source churn (inserts, updates, deletes, rollbacks, PK updates) → remote capture through the public log API → hub (throw-away metadata) → worker → landing Postgres (`alelt-lab-landing`), then checks exactly-once, soft deletes, checkpoint, restart without gaps/duplicates and idempotent replay. Needs `landing` running and the repo's Python dev environment (`hub/scripts/dev-env.ps1`). End-to-end examples are coming in a later release.

From the console: *Local test lab* → **Create demo pipeline** creates a TEST pipeline through the parallel-test wizard API.

## Console helper

`lab helper` starts `lab/helper/lab_helper.py` (Python stdlib): binds **127.0.0.1:47999**, prints a link `http://localhost:8080/lab#helper=…&token=…` with a one-time token (valid 8 h). The console keeps it in session storage only and never sends it to the hub. Allowed actions: pull / up / down / reset of catalog profiles. Without the helper the console still shows every command and lets you download a `lab.env` profile file.

| Endpoint | Purpose |
|---|---|
| `GET /lab/v1/catalog` | catalog |
| `GET /lab/v1/status` | per profile: `state`, `image_present` |
| `POST /lab/v1/pull`, `/up`, `/down`, `/reset` `{profiles}` | start an operation → `{op_id}` |
| `GET /lab/v1/ops/{op_id}` | progress + log tail |
