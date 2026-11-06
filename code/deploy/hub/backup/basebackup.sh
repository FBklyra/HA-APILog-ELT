#!/usr/bin/env bash
# Weekly physical base backup (PITR anchor). Run from host cron in deploy/hub:
#   15 3 * * 0  cd /opt/alelt/deploy/hub && ./backup/basebackup.sh >> /var/log/alelt-backup.log 2>&1
# Uses the local socket inside the postgres container; output lands on the backup disk.
# DEP-25: retention is keyed on backup ORDER (directory names are UTC timestamps), never on mtime; WAL is pruned with
# pg_archivecleanup up to the start WAL segment of the OLDEST KEPT base backup (read from its backup_label), and a new
# backup is integrity-checked (gzip -t on every tar, manifest present) before anything older is removed.
set -euo pipefail
cd "$(dirname "$0")/.."
ts="$(date -u +%Y%m%dT%H%M%SZ)"
keep="${ALELT_BASEBACKUP_KEEP:-5}"
case "$keep" in ''|*[!0-9]*) echo "ALELT_BASEBACKUP_KEEP must be a number" >&2; exit 2 ;; esac
[ "$keep" -ge 1 ] || { echo "ALELT_BASEBACKUP_KEEP must be >= 1" >&2; exit 2; }

docker compose exec -T -e TS="$ts" -e KEEP="$keep" postgres bash -s <<'EOS'
set -euo pipefail
arch=/var/lib/postgresql/wal_archive
base=$arch/base
d=$base/$TS
mkdir -p "$d"
pg_basebackup -U "$POSTGRES_USER" -D "$d" -Ft -z -X none --checkpoint=fast --manifest-checksums=SHA256
# verify the new backup before touching older ones
[ -s "$d/backup_manifest" ] || { echo "basebackup $TS: backup_manifest missing" >&2; exit 1; }
for t in "$d"/*.tar.gz; do gzip -t "$t" || { echo "basebackup $TS: corrupt $t" >&2; exit 1; }; done
tar -xOzf "$d/base.tar.gz" backup_label > "$d/backup_label.copy"

# keep the newest $KEEP complete backups (by name = timestamp)
mapfile -t all < <(find "$base" -mindepth 1 -maxdepth 1 -type d -name '20*Z' -printf '%f\n' | sort)
n=${#all[@]}
if [ "$n" -gt "$KEEP" ]; then
  for old in "${all[@]:0:$((n - KEEP))}"; do rm -r -- "${base:?}/$old"; done
fi
oldest=$(find "$base" -mindepth 1 -maxdepth 1 -type d -name '20*Z' -printf '%f\n' | sort | head -1)
startwal=$(sed -n 's/^START WAL LOCATION: .*(file \([0-9A-F]\{24\}\))$/\1/p' "$base/$oldest/backup_label.copy" 2>/dev/null || true)
if [ -n "$startwal" ]; then
  pg_archivecleanup "$arch" "$startwal"
else
  echo "basebackup: no start WAL for the oldest kept backup ($oldest) - WAL NOT pruned" >&2
fi
EOS
echo "{\"component\":\"pg-backup\",\"event\":\"basebackup_ok\",\"ts\":\"$ts\",\"keep\":$keep}"
