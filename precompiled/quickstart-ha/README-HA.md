# ALELT â€” HA install (Docker Compose / Swarm)

Full guide (choose HA or not, switch modes, RPO/RTO, failure matrix, checklists): [`../HA.md`](../HA.md).
Kubernetes: [`../helm/alelt`](../helm/alelt). Non-HA install: [`../local`](../local).

| File | Purpose |
|---|---|
| `docker-compose.ha.yml` | `migrate` (one-shot) + `alelt` api Ã—2 + `alelt-worker` Ã—2, spread one per host; profiles `lb` (HAProxy, loopback) and `demo` (SeaweedFS S3 + Postgres, rehearsal only) |
| `docker-compose.ha.swarm.yml` | Swarm-only `deploy:` keys (replicas, placement, update/rollback) — DEP-15 |
| `docker-compose.ha.hostport.yml` | per-host install behind an external LB: publishes the api on `ALELT_API_BIND` (required) |
| `docker-compose.ha.secrets.yml` | overlay: secrets as files / Swarm secrets (`ALELT_<NAME>_FILE`) |
| `.env.ha.example` | settings template (`ALELT_DB_URL` Postgres, `ALELT_SPOOL_URL=s3://â€¦`, `ALELT_S3_*`) |
| `lb/haproxy.cfg` | reference LB: round-robin, `GET /readyz` checks, long timeouts for agent long-poll |

| Task | Command |
|---|---|
| Swarm (2+ hosts) | `set -a; . ./.env; set +a; docker stack deploy -c docker-compose.ha.yml -c docker-compose.ha.secrets.yml -c docker-compose.ha.swarm.yml alelt` (Swarm ignores `depends_on`: let `migrate` finish first) |
| Compose, one project per host behind your LB | `docker compose -f docker-compose.ha.yml -f docker-compose.ha.secrets.yml -f docker-compose.ha.hostport.yml up -d` |
| One-machine rehearsal | `docker compose -f docker-compose.ha.yml --profile demo --profile lb up -d --scale alelt=2 --scale alelt-worker=2` |
| Check | `docker compose -f docker-compose.ha.yml ps` Â· `alelt topology status` Â· console â†’ *Topology & HA* |
| Switch an existing standalone install to HA | `alelt topology plan --to ha â€¦` then `apply` ([`../HA.md`](../HA.md#switch-standalone--ha)) |

Required: external Postgres (`ALELT_DB_URL`) and an S3-compatible bucket (`ALELT_SPOOL_URL`). No shared local volume: every
replica is stateless. Production secrets are files (`docker-compose.ha.secrets.yml`, DEP-12); plain values in `.env` are for rehearsals.
Rehearsal with `demo` (no default passwords, everything on 127.0.0.1, DEP-14): choose `POSTGRES_PASSWORD`,
`ALELT_S3_SECRET_ACCESS_KEY` and `ALELT_LB_STATS_PASSWORD`, then set
`ALELT_DB_URL=postgresql+psycopg://alelt:<POSTGRES_PASSWORD>@postgres:5432/alelt`,
`ALELT_SPOOL_URL=s3://alelt-spool/demo`, `ALELT_S3_ENDPOINT=http://s3:9000`, `ALELT_S3_ACCESS_KEY_ID=aleltdemo`.
The LB stats page is `http://127.0.0.1:8404/` (basic auth). The reference LB has 16 replica slots.
