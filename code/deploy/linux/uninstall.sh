#!/bin/sh
# ALELT Linux uninstall. Stops and removes the units, code and venv.
# KEEPS /etc/alelt (config) and /var/lib/alelt (spool + state.json = capture position) and the system user,
# unless --purge is given.
#   sudo ./uninstall.sh [--purge [--force]]
# --purge refuses while the spool still holds unacknowledged batches (they are CDC changes the hub has not
# received: deleting them loses data) unless --force is given too (DEP-22).
set -eu
PURGE=0; FORCE=0
for a in "$@"; do
  case "$a" in
    --purge) PURGE=1 ;;
    --force) FORCE=1 ;;
    *) echo "unknown option: $a" >&2; exit 2 ;;
  esac
done
[ "$(id -u)" -eq 0 ] || { echo "run as root (sudo)" >&2; exit 1; }

if [ "$PURGE" -eq 1 ] && [ -d /var/lib/alelt ]; then
  PENDING=$(find /var/lib/alelt -type f \( -name '*.batch' -o -name '*.aleltb' \) 2>/dev/null | wc -l | tr -d ' ')
  if [ "$PENDING" -gt 0 ]; then
    echo "WARNING: /var/lib/alelt holds $PENDING unacknowledged spool batch file(s) - CDC changes not yet received by the hub." >&2
    if [ "$FORCE" -ne 1 ]; then
      echo "Refusing to purge. Let the agent drain the spool (hub reachable), or re-run with --purge --force to discard them." >&2
      exit 3
    fi
    echo "--force given: discarding them." >&2
  fi
fi

# only the units this package installs (DEP-22: never a prefix glob that could match someone else's alelt* unit)
UNITS=""
for t in alelt.service alelt-hub.service alelt-capture.service; do
  if systemctl cat "$t" >/dev/null 2>&1; then UNITS="$UNITS $t"; fi
done
UNITS="$UNITS $(systemctl list-units --all --plain --no-legend 'alelt-worker@*.service' 2>/dev/null | awk '{print $1}')"
for u in $UNITS; do
  systemctl stop "$u" || true                # graceful: SIGTERM -> checkpoint -> lease release
  systemctl disable "$u" 2>/dev/null || true
done
for f in alelt.service alelt-hub.service alelt-worker@.service alelt-capture.service; do
  rm -f "/etc/systemd/system/$f"
done
rm -rf /etc/systemd/system/alelt.service.d
systemctl daemon-reload
for u in $UNITS; do systemctl reset-failed "$u" 2>/dev/null || true; done
rm -rf /opt/alelt

if [ "$PURGE" -eq 1 ]; then
  rm -rf /etc/alelt /var/lib/alelt
  if id alelt >/dev/null 2>&1; then userdel alelt || true; fi
  if getent group alelt >/dev/null; then groupdel alelt 2>/dev/null || true; fi
  echo "purged: config, spool/state and user removed"
else
  echo "removed services and code; kept /etc/alelt and /var/lib/alelt (capture position). Use --purge to delete."
fi
