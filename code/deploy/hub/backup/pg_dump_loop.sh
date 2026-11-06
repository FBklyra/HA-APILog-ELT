#!/usr/bin/env bash
# Nightly logical dump of the hub metadata DB (runs in the pg-backup container).
# Complements WAL archiving + weekly base backup (basebackup.sh) used for PITR.
set -euo pipefail
OUT=/var/backups/alelt
KEEP="${ALELT_BACKUP_KEEP_DAYS:-14}"
AT="${ALELT_BACKUP_AT:-02:30}"

umask 077
printf '%s:5432:*:%s:%s\n' "$PGHOST" "$PGUSER" "$(cat /run/secrets/pg_password)" > /tmp/.pgpass
export PGPASSFILE=/tmp/.pgpass

dump() {
  local ts f
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  f="$OUT/alelt-$ts.dump"
  pg_dump --format=custom --compress=zstd:6 --no-owner --file="$f.part" "$PGDATABASE"
  pg_restore --list "$f.part" > /dev/null          # integrity check
  mv "$f.part" "$f"
  sha256sum "$f" > "$f.sha256"
  find "$OUT" -name 'alelt-*.dump*' -mtime "+$KEEP" -delete
  echo "{\"component\":\"pg-backup\",\"event\":\"dump_ok\",\"file\":\"$f\",\"bytes\":$(stat -c %s "$f")}"
}

[[ "${1:-}" == "--now" ]] && { dump; exit 0; }

while true; do
  now=$(date -u +%s); next=$(date -u -d "today $AT" +%s)
  (( next <= now )) && next=$(date -u -d "tomorrow $AT" +%s)
  sleep $(( next - now ))
  dump || echo '{"component":"pg-backup","event":"dump_failed"}' >&2
done
