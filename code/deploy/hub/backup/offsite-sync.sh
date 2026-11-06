#!/usr/bin/env bash
# Copy dumps, base backups and WAL archive to Azure Blob (immutable container recommended).
# Auth = VM system-assigned managed identity (Storage Blob Data Contributor on the container). No SAS, no keys.
# Host cron:  */15 * * * *  /opt/alelt/deploy/hub/backup/offsite-sync.sh >> /var/log/alelt-backup.log 2>&1
# DEP-25: uploads carry Content-MD5 and are compared by hash (a corrupt or partial copy is re-sent, not skipped);
# offsite retention = the container's lifecycle / immutability policy (set it to >= ALELT_BASEBACKUP_KEEP weeks + WAL).
# .env is parsed for the two keys below, never sourced (DEP-24).
set -euo pipefail
ENV_FILE="$(dirname "$0")/../.env"
env_get() { [ -f "$ENV_FILE" ] || return 0; sed -n "s/^[[:space:]]*$1=//p" "$ENV_FILE" | tail -n 1 | sed -e 's/^"\(.*\)"$/\1/' -e "s/^'\(.*\)'$/\1/"; }
ALELT_BACKUP_BLOB_URL="${ALELT_BACKUP_BLOB_URL:-$(env_get ALELT_BACKUP_BLOB_URL)}"
ALELT_BACKUP_DISK="${ALELT_BACKUP_DISK:-$(env_get ALELT_BACKUP_DISK)}"
: "${ALELT_BACKUP_BLOB_URL:?set ALELT_BACKUP_BLOB_URL in .env}"
: "${ALELT_BACKUP_DISK:?set ALELT_BACKUP_DISK in .env}"
export AZCOPY_AUTO_LOGIN_TYPE=MSI
for part in dumps wal; do
  azcopy sync "${ALELT_BACKUP_DISK}/$part" "${ALELT_BACKUP_BLOB_URL}/$part" --recursive --delete-destination=false \
    --put-md5 --compare-hash=MD5
done
echo "{\"component\":\"pg-backup\",\"event\":\"offsite_sync_ok\",\"ts\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
