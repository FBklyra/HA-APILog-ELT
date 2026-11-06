# Architecture

ALELT = engine-agnostic, cloud-agnostic change-data-capture (CDC) and ELT platform. Sources are read only through public log APIs ([`PUBLIC-APIS.md`](PUBLIC-APIS.md)); the core knows only a canonical change model; destinations plug in through an SPI that applies data and checkpoint in one transaction.

| Diagram | Shows |
|---|---|
| [1. Context](#1-context) | who and what talks to ALELT |
| [2. Components](#2-components) | edge agent, hub services, stores, console |
| [3. Data flow](#3-data-flow) | numbered path of one committed change |
| [4. HA topology](#4-ha-topology) | active/passive agents, N hub replicas, Postgres HA, S3 spool |
| [5. Deployment options](#5-deployment-options) | laptop → Kubernetes |
| [6. Security zones](#6-security-zones) | outbound-only agent, mTLS, deny list |

## 1. Context

```mermaid
flowchart LR
  subgraph SRC[Source databases]
    DB2[(Db2)]
    PG[(PostgreSQL)]
    MY[(MySQL / MariaDB)]
    ORA[(Oracle)]
    MSS[(SQL Server)]
  end
  AG[Edge agent<br/>Windows / Linux]
  HUB[ALELT hub]
  subgraph DST[Destinations]
    WH[(Warehouses<br/>Snowflake, BigQuery...)]
    LAKE[(Lakes<br/>Iceberg, Delta)]
    SQLD[(SQL databases)]
    STRM[(Kafka API)]
  end
  ADM([Admins and data team])
  CON[Web console]

  DB2 -- db2ReadLog --> AG
  AG -- HTTPS mTLS outbound --> HUB
  PG -- pgoutput --> HUB
  MY -- binlog protocol --> HUB
  ORA -- LogMiner --> HUB
  MSS -- CDC functions --> HUB
  HUB --> WH
  HUB --> LAKE
  HUB --> SQLD
  HUB --> STRM
  ADM --> CON
  CON -- REST /api/v1 --> HUB
```

- **Edge capture** (agent next to the database) when the API is local-only (Db2 `db2ReadLog`).
- **Remote capture** (hub-side runner) when the API is a network protocol (PostgreSQL, MySQL, MariaDB, Oracle, SQL Server).

## 2. Components

```mermaid
flowchart TB
  subgraph EDGE[Edge agent]
    SVC[Service<br/>aleltagent / aleltd]
    PLG[Source plugin<br/>db2ReadLog]
    SPL[Local spool]
    GOV[Governor<br/>CPU, mem, rate]
    SVC --- PLG
    SVC --- SPL
    SVC --- GOV
  end
  subgraph HUB[Hub - one image, roles]
    API[api<br/>gateway, console API, RBAC]
    CAP[capture<br/>remote runners]
    WRK[workers]
    subgraph PIPE[Worker pipeline]
      DEC[decoder]
      TXN[txn assembly]
      LDR[loader]
    end
    INT[integrity + holds<br/>per-table watermarks]
    RPL[replay passes<br/>decoded-event cache]
    WRK --- INT
    WRK --- RPL
    SCH[schema drift]
    SNP[snapshot]
    VER[verify]
    DIA[diagnose]
    WRK --> PIPE
    WRK --- SCH
    WRK --- SNP
    WRK --- VER
    WRK --- DIA
  end
  META[(Metadata<br/>Postgres or SQLite)]
  BLOB[(Blob store<br/>file or S3)]
  CONS[Console<br/>React SPA]
  DEST[(Destination)]

  SVC -- batches --> API
  API --> BLOB
  CAP --> BLOB
  API --> META
  CAP --> META
  WRK --> META
  WRK --> BLOB
  LDR --> DEST
  CONS --> API
```

| Component | Role |
|---|---|
| Edge agent | Windows C11 service or Linux Python daemon (minimal wheel: daemon + `alelt_common` + one connector): reads raw log records, spools, ships compressed batches, obeys whitelisted commands, self-throttles |
| api | agent gateway (durable receipt, ACK after persistence), console REST API, auth (OIDC), RBAC, setup wizard |
| capture | remote-capture runners for network-protocol sources; same durable receipt as the gateway |
| workers | decode → transaction assembly → load; per-table integrity state + holds (watermarks), multi-table replay passes, snapshot, schema drift, verification, diagnosis jobs; heart-beaten job leases |
| Metadata | PostgreSQL (recommended, HA) or SQLite (single host): config revisions, batches, checkpoints mirror, audit, leases |
| Blob store | raw batches + raw archive (re-decode without touching the source): local filesystem or any S3-compatible store |
| Console | web UI for data team and platform admins, 4 languages |

## 3. Data flow

```mermaid
sequenceDiagram
  autonumber
  participant App as Application
  participant DB as Source DB
  participant Cap as Agent or runner
  participant Hub as Hub api
  participant Store as Blob store + metadata
  participant W as Worker
  participant Dst as Destination
  App->>DB: COMMIT
  DB->>DB: write change to its log
  Cap->>DB: read via public log API
  DB-->>Cap: change records
  Cap->>Hub: batch (zstd, sha256)
  Hub->>Store: persist + fsync / PUT
  Hub-->>Cap: ACK durable position
  Cap->>Cap: free spool up to ACK
  W->>Store: claim batch
  W->>W: decode to canonical events
  W->>W: assemble txns, drop rollbacks
  W->>Dst: BEGIN apply rows, checkpoint compare-and-set, watermarks, audit COMMIT
  Dst-->>W: committed
  W->>Dst: refresh views
```

| # | Guarantee |
|---|---|
| 1–4 | Read-only on the source; nothing is written there. Capture uses the engine's public API only. |
| 5–8 | ACK only after the batch is durable; the agent deletes its spool only up to the ACK (never drops; pauses when full). |
| 9–11 | Every record is classified (decoded / ignored by documented design / signalled). Undecodable records go to quarantine with raw bytes, position and reason and **hold their table** (watermark stops, certification blocked, automatic re-decode / keyed re-read / resync). Rolled-back transactions are never emitted. |
| 12–13 | Rows and checkpoint commit **in one destination transaction**, the checkpoint compare-and-set inside it → exactly-once per destination where proven (see [DESIGN.md §5.3](../DESIGN.md#53-exactly-once-status-per-destination)); replay is idempotent. |
| 14 | Consumers read stable views; per-table switch between a legacy pipeline and ALELT. |

## 4. HA topology

```mermaid
flowchart LR
  subgraph SRCH[Source host or cluster]
    A1[Agent A<br/>active]
    A2[Agent B<br/>standby]
  end
  LB[Load balancer<br/>TLS, mTLS]
  subgraph HUBS[Hub replicas]
    H1[api 1]
    H2[api 2]
    HN[api N]
    W1[worker 1..N]
    C1[capture 1..N]
  end
  PGHA[(Postgres HA<br/>primary + replica)]
  S3[(S3 spool<br/>object storage)]
  DST[(Destination)]

  A1 -- lease renew + batches --> LB
  A2 -. lease acquire polls .-> LB
  LB --> H1
  LB --> H2
  LB --> HN
  H1 --> PGHA
  H2 --> PGHA
  HN --> PGHA
  H1 --> S3
  W1 --> PGHA
  W1 --> S3
  C1 --> S3
  W1 --> DST
```

- **Capture lease** per connection (TTL, epoch fencing): only the holder may send batches; a stale epoch gets `423`; the standby takes over after TTL and resumes at the hub's durable position.
- **Hub replicas** are stateless; per-stream worker locks in the metadata database.
- **Spool on S3** so any replica can read any batch. Switch HA ↔ standalone with `alelt topology plan|apply` (see [`deploy/HA.md`](../deploy/HA.md)).

## 5. Deployment options

```mermaid
flowchart TB
  L[Laptop<br/>lab + compose, SQLite or Postgres]
  V[Single VM<br/>compose, bundled Postgres]
  S[HA compose / Swarm<br/>2+ hosts, external Postgres + S3]
  K[Kubernetes<br/>Helm chart]
  AW[Windows agent<br/>MSI or zip installer]
  AL[Linux agent<br/>install.sh, systemd]
  L --> V --> S --> K
  AW -. connects to .-> V
  AW -. connects to .-> S
  AL -. connects to .-> K
```

| Option | Metadata | Spool | Guide |
|---|---|---|---|
| Laptop (evaluation) | SQLite or bundled Postgres | local | [`lab/README.md`](../lab/README.md), [`deploy/local/`](../deploy/local/) |
| Single VM (any cloud / on-prem) | bundled or managed Postgres | local volume | [`deploy/INSTALL-IN-A-MORNING.md`](../deploy/INSTALL-IN-A-MORNING.md) |
| HA compose / Swarm | external Postgres HA | S3-compatible | [`deploy/HA.md`](../deploy/HA.md) |
| Kubernetes | external Postgres | S3-compatible | Helm chart in `deploy/helm/` |
| Agents | — | local spool | [`deploy/agent/`](../deploy/agent/) |

Release images are **linux/amd64**; agents are **x64** (Windows, Linux).

## 6. Security zones

```mermaid
flowchart LR
  subgraph Z1[Zone 1 - source network]
    DBS[(Source DB)]
    AGT[Agent<br/>no listening port]
  end
  subgraph Z2[Zone 2 - hub network]
    LBX[TLS / mTLS terminator]
    HUBX[Hub api, workers]
    GRD{{Deny list<br/>prod guard}}
    MET[(Metadata)]
    SEC[(Secrets<br/>files or vault)]
  end
  subgraph Z3[Zone 3 - destination]
    DSTX[(Warehouse / lake)]
  end
  subgraph Z4[Zone 4 - users]
    USR([Browser])
    IDP[OIDC identity provider]
  end
  DBS -- local API --> AGT
  AGT -- outbound HTTPS mTLS only --> LBX
  LBX --> HUBX
  HUBX --- GRD
  HUBX --> MET
  HUBX --> SEC
  HUBX -- TLS --> DSTX
  USR -- HTTPS + OIDC token --> LBX
  USR --> IDP
```

| Control | Detail |
|---|---|
| Outbound only | the agent opens connections to the hub; no inbound port on the database server |
| mTLS + pinned identity | client certificates or one-time enrolment tokens bound to one connection |
| Whitelisted commands | hub → agent: typed commands only (start, pause, resume, limits, catalog, snapshot, probe, shutdown); never arbitrary SQL |
| Deny list | hosts / CIDRs / accounts marked production are refused by the hub and the agent unless explicitly allowed |
| Least privilege | read-only capture users; destination role limited to its schemas |
| Secrets | files / Docker or Kubernetes secrets / vault references; never in config, logs or Git |
| Console | OIDC sign-in, roles Viewer / DataOperator / PlatformAdmin / Auditor, audited two-person rule for switches |
