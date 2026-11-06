# ALELT quick start — high availability (Docker Compose / Swarm)

Adapted from the repository's HA install (`deploy/ha`, guide `deploy/HA.md`): one-shot `migrate`, two hub API
replicas and two workers, optional HAProxy (`lb` profile), shared S3-compatible spool, external PostgreSQL.
`README-HA.md` describes every file.

Demo on one host (5 minutes; bundled Postgres + SeaweedFS S3 via the `demo` profile):

1. `cp .env.example .env`; set `POSTGRES_PASSWORD`, `ALELT_S3_ACCESS_KEY_ID`, `ALELT_S3_SECRET_ACCESS_KEY`,
   `ALELT_LB_STATS_PASSWORD` and the demo `ALELT_DB_URL` / `ALELT_SPOOL_URL` / `ALELT_S3_ENDPOINT` values shown in the
   comments of `.env.example`.
2. `set -a; . ./.env; set +a`
3. `docker compose -f docker-compose.ha.yml --profile demo --profile lb up -d`
4. Open the load balancer address (`ALELT_LB_BIND`, default loopback) and check `docker compose ps`.
5. Production: external PostgreSQL + S3, mTLS agents (`lb/haproxy-mtls.cfg`), OIDC, `ALELT_DEV_AUTH=0`;
   overlays `docker-compose.ha.hostport.yml` (per-host behind an external LB), `docker-compose.ha.secrets.yml`
   (secrets as files) and `docker-compose.ha.swarm.yml` (Swarm placement).

See `KNOWN-ISSUES.md`.
