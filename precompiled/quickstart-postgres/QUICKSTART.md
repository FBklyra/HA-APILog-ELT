# ALELT quick start — PostgreSQL (5 minutes, standalone)

Bundled PostgreSQL 16 for metadata and a PostgreSQL landing database, hub API + worker as separate containers.

What is created automatically on first start:

- PostgreSQL user `POSTGRES_USER` (default `alelt`) and metadata database `ALELT_DB_NAME` (default `alelt`)
  — by the PostgreSQL image;
- landing database `ALELT_LANDING_DB` (default `alelt_landing`) — by `initdb/10-landing.sh`;
- all metadata tables / schema migrations — by the one-shot `migrate` service, which runs before `alelt` (API) and
  `alelt-worker` start.

Steps:

1. Requirements: Docker with Compose v2 on an x64 (amd64) host.
2. `cp .env.example .env`, then set `POSTGRES_PASSWORD` and `ALELT_AGENT_TOKENS` to long random values
   (`python -c "import secrets; print(secrets.token_hex(24))"`).
3. `docker compose up -d` (offline host: `docker load -i ha-apilog-elt-1.0.0-linux-amd64.tar.gz` first).
4. Wait for `docker compose ps` to show `alelt` healthy, then open http://localhost:8080.
5. Install an agent next to your source database and point it to the hub with the token from step 2.

Postgres is published on 127.0.0.1:55432 for psql / DBeaver. External / managed Postgres: set `ALELT_DB_URL` and
`ALELT_DEST_URL` in `.env` and run `docker compose up -d --no-deps migrate alelt alelt-worker`.
Production: `ALELT_ENV=prod`, `ALELT_DEV_AUTH=0` and OIDC settings. See `KNOWN-ISSUES.md`.
