# ALELT v1.0.0 — prebuilt packages

All packages are x64 / amd64. Verify downloads first:

```sh
sha256sum -c SHA256SUMS            # Windows PowerShell: Get-FileHash -Algorithm SHA256 <file>
```

## `docker/` — container image (hub API, worker, migrations, console)

One image, several roles: `api`, `worker`, `migrate`, `all` (API and worker in one container), `version`.
The worker is the same image started with `command: worker`.

```sh
# from GitHub Container Registry
docker pull ghcr.io/fbklyra/ha-apilog-elt:1.0.0
# or offline, from this folder (if the file was split, join it first: see docker/JOIN.md when present)
docker load -i docker/ha-apilog-elt-1.0.0-linux-amd64.tar.gz
docker run --rm ghcr.io/fbklyra/ha-apilog-elt:1.0.0 version
```

## `quickstart-sqlite/` — 5-minute evaluation

`cd quickstart-sqlite && docker compose up -d`, then open http://localhost:8080. See `QUICKSTART.md`.

## `quickstart-postgres/` — standalone install with PostgreSQL metadata

`cp .env.example .env` (set the passwords / token), `docker compose up -d`. The Postgres container creates the user,
the metadata and landing databases; the `migrate` service applies all schema migrations. See `QUICKSTART.md`.

## `quickstart-ha/` — high availability (Compose / Swarm)

Two API replicas, two workers, HAProxy, external PostgreSQL and S3-compatible spool (bundled demo profile for one
host). See `QUICKSTART.md` and `README-HA.md`.

## `windows-x64/` — Windows edge agent

`aleltagent-1.0.0-windows-x64.zip`: `aleltagent.exe` (Windows service `ALELTAgent`), `aleltsrc_db2.dll` (Db2 source
plugin; needs the IBM Data Server client installed on the host — no IBM files are included), `install-agent.ps1`,
`verify-agent.ps1`, `uninstall-agent.ps1`, `aleltagent.example.json`, `README.md`.

```powershell
Expand-Archive aleltagent-1.0.0-windows-x64.zip -DestinationPath C:\Temp\alelt
cd C:\Temp\alelt\aleltagent-1.0.0-windows-x64
.\install-agent.ps1 -?            # parameters: hub URL, agent id, connection id, database, auth mode
```

## `linux-x64/` — Linux capture daemon

`aleltd-1.0.0-linux-x64.tar.gz`: the `aleltd` daemon (pure Python, Python >= 3.10), systemd unit `alelt.service`,
`install.sh` / `uninstall.sh`, per-engine wheels in `wheels/`.

```sh
tar xzf aleltd-1.0.0-linux-x64.tar.gz && cd aleltd-1.0.0-linux-x64
sudo ./install.sh --engines postgres --no-start
sudoedit /etc/alelt/aleltagent.json && sudo systemctl start alelt.service
```

Release notes: `RELEASE-NOTES.md`. Known issues: `../code/KNOWN-ISSUES.md`.
