# Install the hub (Azure VM, Ubuntu 24.04)

Prereqs: VM, disks, NSG, managed identity and Key Vault per [`deploy/hub/README.md`](../deploy/hub/README.md#azure); hub server cert and agent-CA chain/CRL in Key Vault; DNS for console and agent hostnames.

| # | Step | Command / action | Check |
|---|---|---|---|
| 1 | Base packages | `sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 azure-cli git && curl -sL https://aka.ms/downloadazcopy-v10-linux \| tar xz --strip-components=1 -C /usr/local/bin --wildcards '*/azcopy'` | `docker compose version` |
| 2 | Mount data disks | Format ext4 (`mkfs.ext4 -L aleltpg /dev/disk/azure/scsi1/lun0` …), add to `/etc/fstab` by LABEL with `nofail`: `/data/pg`, `/data/spool`, `/data/backup` | `df -h /data/*` |
| 3 | Directories | `sudo mkdir -p /data/pg/data /data/backup/{wal,dumps} && sudo chown 999:999 /data/pg/data /data/backup/wal && sudo chown 10001 /data/spool` | ownership: postgres uid 999, hub uid 10001 |
| 4 | Code | `sudo git clone <repo> /opt/alelt && cd /opt/alelt/deploy/hub && git checkout <release-tag>` | tag = signed release |
| 5 | Settings | `cp .env.example .env`; set hostnames, Entra IDs, role groups, destination account, `ALELT_KEYVAULT_NAME`, `ALELT_AGENT_THUMBPRINTS`, `ALELT_OUTPUT_PROFILE`. Edit `server_name` in `nginx/nginx.conf`, addresses in `observability/alertmanager.yml` | no secrets in `.env` |
| 6 | Secrets | `sudo ./secrets/fetch-secrets.sh` and install a systemd unit `alelt-secrets.service` (`Before=docker.service`, `ExecStart=/opt/alelt/deploy/hub/secrets/fetch-secrets.sh`) | files present in `/etc/alelt/secrets` (tmpfs) |
| 7 | Build / pull | `docker compose build` (or `docker compose pull` from the registry) | images tagged `ALELT_VERSION` |
| 8 | Start | `docker compose up -d` | `docker compose ps` all `healthy`; `migrate` exited 0 |
| 9 | Smoke | `curl -sk https://<console-host>/healthz`; browse console, sign in, Overview loads | role mapping correct (Users & roles screen) |
| 10 | Agent path | From the Db2 server: `curl.exe -sv https://<agent-host>/agent/v1/register` without a client cert → TLS handshake rejected; agent `register` with its cert → accepted (Console → Agent) | mTLS enforced |
| 11 | Backups | Host cron: `basebackup.sh` weekly, `offsite-sync.sh` every 15 min (see script headers). Run `docker compose exec pg-backup bash /backup/pg_dump_loop.sh --now` once | dump + `.sha256` in `/data/backup/dumps`, copy in Blob |
| 12 | Alerts | Fire a test: `docker compose exec alertmanager amtool alert add AleltTest severity=warning --alertmanager.url=http://localhost:9093` | Teams + email received |
| 13 | Record | Exchange entry + console Hub screen shows version | — |

## Scale

`docker compose up -d --scale worker=<N>` (8 vCPU VM: up to 6 workers). Watch `alelt_queue_depth`.

## Uninstall

`docker compose down` (keeps data). Volumes on `/data/*` are removed only by deleting the disks — confirm with the data team first; destination checkpoints make the hub rebuildable ([dr-rebuild-hub.md](dr-rebuild-hub.md)).
