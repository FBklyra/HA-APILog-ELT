#!/bin/sh
# ALELT Linux install (systemd): agent and/or hub services, no Docker.
#
#   sudo ./install.sh                         agent only (alelt.service)
#   sudo ./install.sh --hub --workers 2       hub API + 2 workers (+ --capture for hub-side remote capture)
#   sudo ./install.sh --all                   agent + hub + 1 worker + capture
#   options: --src DIR (repo root, default ../..)  --python PY (>= 3.10, default python3)
#            --wheels DIR (offline pip wheel dir)  --engines "postgres mysql ..." (client libs for the agent)
#            --no-enable (install only)  --no-start
#
# Idempotent: re-running upgrades the code and the venv in place; /etc/alelt and /var/lib/alelt (spool + state.json
# = capture position) are never overwritten. Upgrade without losing the position: see README.md.
set -eu

PREFIX=/opt/alelt
ETC=/etc/alelt
STATE=/var/lib/alelt
UNITDIR=/etc/systemd/system
USER_NAME=alelt
HERE=$(cd "$(dirname "$0")" && pwd)
SRC=$(cd "$HERE/../.." && pwd)
PY=python3
AGENT=1; HUB=0; CAPTURE=0; WORKERS=0; ENABLE=1; START=1; WHEELS=""; ENGINES=""

while [ $# -gt 0 ]; do
  case "$1" in
    --agent) AGENT=1 ;;
    --hub) HUB=1; AGENT=0; [ "$WORKERS" -eq 0 ] && WORKERS=1 ;;
    --all) AGENT=1; HUB=1; CAPTURE=1; [ "$WORKERS" -eq 0 ] && WORKERS=1 ;;
    --capture) CAPTURE=1 ;;
    --workers) WORKERS="$2"; shift ;;
    --src) SRC=$(cd "$2" && pwd); shift ;;
    --python) PY="$2"; shift ;;
    --wheels) WHEELS="$2"; shift ;;
    --engines) ENGINES="$2"; shift ;;
    --no-enable) ENABLE=0; START=0 ;;
    --no-start) START=0 ;;
    -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
  shift
done

[ "$(id -u)" -eq 0 ] || { echo "run as root (sudo)" >&2; exit 1; }
command -v systemctl >/dev/null || { echo "systemd is required" >&2; exit 1; }
"$PY" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' || { echo "$PY must be >= 3.10" >&2; exit 1; }
[ -d "$SRC/edge-agent/linux/aleltd" ] || { echo "not a source tree: $SRC" >&2; exit 1; }

echo "== system user $USER_NAME"
if ! getent group "$USER_NAME" >/dev/null; then groupadd --system "$USER_NAME"; fi
if ! id "$USER_NAME" >/dev/null 2>&1; then
  useradd --system --gid "$USER_NAME" --home-dir "$STATE" --no-create-home --shell /usr/sbin/nologin "$USER_NAME"
fi

echo "== directories"
install -d -m 0755 "$PREFIX" "$PREFIX/app"
install -d -m 0750 -o root -g "$USER_NAME" "$ETC"
install -d -m 0750 -o "$USER_NAME" -g "$USER_NAME" "$STATE" "$STATE/spool"

echo "== code -> $PREFIX/app (read-only for the service)"
STAGE="$PREFIX/app.new"
rm -rf "$STAGE"
install -d -m 0755 "$STAGE"
for d in edge-agent/linux hub connectors config deploy/linux deploy/image; do
  install -d "$STAGE/$(dirname "$d")"
  cp -R "$SRC/$d" "$STAGE/$d"
done
find "$STAGE" -name '__pycache__' -type d -prune -exec rm -rf {} + 2>/dev/null || true
find "$STAGE" -name 'tests' -type d -path '*/edge-agent/*' -prune -exec rm -rf {} + 2>/dev/null || true
chown -R root:root "$STAGE"; chmod -R go-w "$STAGE"
rm -rf "$PREFIX/app.old"; [ -d "$PREFIX/app" ] && mv "$PREFIX/app" "$PREFIX/app.old"
mv "$STAGE" "$PREFIX/app"; rm -rf "$PREFIX/app.old"

echo "== python venv $PREFIX/venv"
[ -x "$PREFIX/venv/bin/python" ] || "$PY" -m venv "$PREFIX/venv"
PIP="$PREFIX/venv/bin/pip --disable-pip-version-check -q"
[ -n "$WHEELS" ] && PIP="$PIP --no-index --find-links $WHEELS"
# DEP-04 / DEP-20: hashed locks only (no resolver, every artifact verified by sha256); failures are fatal and printed.
#   agent: deploy/linux/requirements-agent.lock (alelt_common models, zstd, the pure-Python engine clients)
#   hub:   deploy/image/requirements.lock (same set as the product image)
# Per-engine selection installs only that package's locked block from the agent lock.
LOCK_AGENT="$PREFIX/app/deploy/linux/requirements-agent.lock"
LOCK_HUB="$SRC/deploy/image/requirements.lock"
[ -f "$LOCK_AGENT" ] || { echo "missing $LOCK_AGENT" >&2; exit 1; }
pick() {  # pick <lock> <package...>: write the locked blocks (name==ver + hashes) of those packages to stdout
  "$PY" - "$@" <<'PYEOF'
import re, sys
lock, want = sys.argv[1], {w.lower().replace("_", "-") for w in sys.argv[2:]}
blocks, cur = [], []
for line in open(lock, encoding="utf-8"):
    if line[:1] not in (" ", "#", "\n") and cur:
        blocks.append(cur); cur = []
    if not line.startswith("#"):
        cur.append(line)
if cur:
    blocks.append(cur)
out = [b for b in blocks if re.split(r"[=<>\[ ]", b[0].strip(), maxsplit=1)[0].lower().replace("_", "-") in want]
missing = want - {re.split(r"[=<>\[ ]", b[0].strip(), maxsplit=1)[0].lower().replace("_", "-") for b in out}
if missing:
    sys.exit("not in lock: " + ", ".join(sorted(missing)))
sys.stdout.write("".join("".join(b) for b in out))
PYEOF
}
REQ=$(mktemp); trap 'rm -f "$REQ"' EXIT
PKGS="pydantic pydantic-core annotated-types typing-extensions typing-inspection zstandard"
for e in $ENGINES; do
  case "$e" in
    postgres) PKGS="$PKGS psycopg2-binary" ;;
    mysql|mariadb) PKGS="$PKGS pymysql" ;;
    sqlserver) PKGS="$PKGS python-tds" ;;
    oracle) PKGS="$PKGS oracledb cryptography cffi pycparser" ;;
    db2) echo "   db2: install the IBM Data Server client on the host (libdb2.so); not a pip package" ;;
    *) echo "unknown engine $e" >&2; exit 2 ;;
  esac
done
pick "$LOCK_AGENT" $PKGS > "$REQ"
$PIP install --require-hashes --no-deps -r "$REQ"
if [ "$HUB" -eq 1 ] || [ "$CAPTURE" -eq 1 ]; then
  [ -f "$LOCK_HUB" ] || { echo "missing $LOCK_HUB" >&2; exit 1; }
  $PIP install --require-hashes --no-deps -r "$LOCK_HUB"
fi
"$PREFIX/venv/bin/pip" check

echo "== configuration (kept if present)"
if [ "$AGENT" -eq 1 ]; then
  [ -f "$ETC/aleltagent.json" ] || install -m 0640 -o root -g "$USER_NAME" "$HERE/aleltagent.example.json" "$ETC/aleltagent.json"
  [ -f "$ETC/agent.env" ] || install -m 0640 -o root -g "$USER_NAME" "$HERE/agent.env.example" "$ETC/agent.env"
fi
if [ "$HUB" -eq 1 ] || [ "$CAPTURE" -eq 1 ]; then
  [ -f "$ETC/hub.env" ] || install -m 0640 -o root -g "$USER_NAME" "$HERE/hub.env.example" "$ETC/hub.env"
  install -d -m 0750 -o "$USER_NAME" -g "$USER_NAME" "$STATE/hub-spool"
fi

echo "== systemd units"
UNITS=""
[ "$AGENT" -eq 1 ] && UNITS="$UNITS alelt.service"
[ "$HUB" -eq 1 ] && UNITS="$UNITS alelt-hub.service alelt-worker@.service"
[ "$CAPTURE" -eq 1 ] && UNITS="$UNITS alelt-capture.service"
for u in $UNITS; do install -m 0644 "$HERE/$u" "$UNITDIR/$u"; done
systemctl daemon-reload

if [ "$ENABLE" -eq 1 ]; then
  EN=""
  [ "$AGENT" -eq 1 ] && EN="$EN alelt.service"
  [ "$HUB" -eq 1 ] && EN="$EN alelt-hub.service"
  i=1; while [ "$i" -le "$WORKERS" ] && [ "$HUB" -eq 1 ]; do EN="$EN alelt-worker@$i.service"; i=$((i + 1)); done
  [ "$CAPTURE" -eq 1 ] && EN="$EN alelt-capture.service"
  systemctl enable $EN
  if [ "$START" -eq 1 ]; then
    if [ "$AGENT" -eq 1 ] && grep -q '"connection_id": "CHANGE-ME"' "$ETC/aleltagent.json"; then
      echo "   $ETC/aleltagent.json is still the example: edit it (and $ETC/agent.env), then: systemctl start alelt"
      EN=$(echo "$EN" | sed 's/ alelt.service//')
    fi
    # restart (not start): an upgrade picks up the new code; the agent checkpoints on SIGTERM
    [ -n "$EN" ] && systemctl restart $EN
  fi
fi
echo "== done. status: systemctl status$UNITS | logs: journalctl -u alelt -f"
