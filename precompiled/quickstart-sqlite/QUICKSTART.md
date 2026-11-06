# ALELT quick start — SQLite (5 minutes, for testing)

One container (hub API + worker, role `all`), SQLite metadata in a Docker volume. For evaluation only.

1. Requirements: Docker Engine / Docker Desktop with Compose v2, an x64 (amd64) host or emulation.
2. Optional: `cp .env.example .env` and edit (port, agent token, destination URL).
3. Start (single command):

   ```sh
   docker compose up -d
   ```

   Offline host: `docker load -i ha-apilog-elt-1.0.0-linux-amd64.tar.gz` first (see `../docker/`).
4. Open http://localhost:8080 — dev auth is on (role picker); the console is bound to 127.0.0.1 only.
5. Add a source agent (Windows `aleltagent.exe` or Linux `aleltd`) with the token from `.env`, and set a destination
   (`ALELT_DEST_URL`, e.g. a PostgreSQL database), then `docker compose up -d` again.

Stop: `docker compose down` (add `-v` to delete the metadata volume). Logs: `docker compose logs -f`.
Production: use `../quickstart-postgres` or `../quickstart-ha`, OIDC, and `ALELT_DEV_AUTH=0`. See `KNOWN-ISSUES.md`.
