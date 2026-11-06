#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# ALELT local test lab: throw-away sources and targets with fake data (lab/catalog.yaml).
#
#   ./lab/lab.sh list | ports [ids] | pull [ids] | up <ids> | status | demo <source> | down [ids] | reset [ids] | helper
#   ids: catalog profile ids, or all | sources | targets
#
# Zero risk by design: own compose project / network / volumes (alelt-lab*), ports bound to 127.0.0.1, official
# public images with pinned tags, no Docker socket mounted into any container. Never touches other containers.
# Needs: docker (compose v2), python3 (catalog parsing, port check, demo, helper).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(dirname "$HERE")"
COMPOSE="$HERE/docker-compose.lab.yml"
PROJECT=alelt-lab
PY="${LAB_PYTHON:-$(command -v python3 || command -v python)}"
RETRIES="${LAB_RETRIES:-3}"
PULL_TIMEOUT="${LAB_PULL_TIMEOUT:-1800}"
WAIT_TIMEOUT="${LAB_WAIT_TIMEOUT:-900}"

say() { echo "[lab] $*"; }
die() { echo "[lab] ERROR: $*" >&2; exit 1; }

# catalog query helper: cat_q <expr> <ids...>  (python over catalog.json; prints lines)
cat_q() {
  "$PY" - "$HERE/catalog.json" "$@" <<'PY'
import json, sys
cat = json.load(open(sys.argv[1], encoding="utf-8")); what = sys.argv[2]; ids = sys.argv[3:]
import os
img = [p for p in cat["profiles"] if p["availability"] == "image" and not p.get("optional")]
optin = os.environ.get("LAB_DB2_CONTAINER") == "1"   # optional profiles (db2) run only when opted in (x64 hosts)
byid = {p["id"]: p for p in cat["profiles"]}
sel = []
for i in ids or (["all"] if what in ("pull", "reset", "ports") else []):
    if i == "all": sel += img
    elif i == "sources": sel += [p for p in img if p["kind"] == "source"]
    elif i == "targets": sel += [p for p in img if p["kind"] == "target"]
    elif i not in byid: sys.exit(f"unknown profile '{i}' (see: lab.sh list)")
    elif byid[i]["availability"] != "image": sys.exit(f"'{i}' has no local image (cloud service): see lab/README.md")
    elif byid[i].get("optional") and not optin: print(f"[lab] skip {i} (optional): {byid[i].get('native', '')}", file=sys.stderr)
    else: sel.append(byid[i])
seen = set(); sel = [p for p in sel if not (p["id"] in seen or seen.add(p["id"]))]
if what == "list":
    for p in cat["profiles"]:
        print(f'{p["id"]:<15} {p["kind"]:<7} {p["status"]:<8} {p["availability"]:<9} {",".join(p.get("arch", [])):<12} {p["label"]}')
elif what == "ids": print(" ".join(p["id"] for p in sel))
elif what == "svc-ids": print(",".join(p["id"] for p in sel if p.get("services")))
elif what == "services": print(" ".join(s for p in sel for s in p.get("services", [])))
elif what == "emulated":
    sys.path.insert(0, os.path.dirname(sys.argv[1])); import images
    envl = {**images.read_env_local(), **os.environ}
    print(" ".join(p["id"] for p in sel if "arm64" not in p.get("arch", [])
                   and envl.get("LAB_PLATFORM_" + p["id"].upper()) != "linux/arm64"))
elif what == "images":
    for p in sel:
        for im in p.get("images", []): print(("amd64 " if "arm64" not in p.get("arch", []) else "any ") + im)
elif what == "ports":
    for p in sel:
        for v in p.get("ports", {}).values(): print(p["id"], v, " ".join(p.get("services", [])))
elif what == "dsn":
    for p in sel:
        if p.get("dsn"): print(f'[lab] {p["id"]:<11} {p["dsn"]}')
        if p.get("notes"): print(f'[lab] {"":<11} note: {p["notes"]}')
PY
}

compose() { COMPOSE_PROFILES="$1" docker compose -p "$PROJECT" -f "$COMPOSE" "${@:2}"; }
need_docker() { docker info --format '{{.ServerVersion}}' >/dev/null 2>&1 || die "Docker is not running"; }

check_ports() {
  local running; running="$(docker ps --filter "label=com.docker.compose.project=$PROJECT" --format '{{.Names}}')"
  local bad=0 reserved=""
  if command -v netsh >/dev/null 2>&1 || command -v netsh.exe >/dev/null 2>&1; then
    reserved="$(netsh int ipv4 show excludedportrange protocol=tcp 2>/dev/null | awk '/^ *[0-9]+ +[0-9]+/{print $1, $2}')"
  fi
  while read -r id port services; do
    [ -z "${id:-}" ] && continue
    local mine=0; for s in $services; do grep -qx "$s" <<<"$running" && mine=1; done
    [ $mine = 1 ] && continue
    while read -r lo hi; do
      [ -n "${lo:-}" ] && [ "$port" -ge "$lo" ] && [ "$port" -le "$hi" ] && { echo "[lab] $id:$port in Windows reserved range $lo-$hi" >&2; bad=1; }
    done <<<"$reserved"
    if "$PY" -c "import socket,sys; s=socket.socket(); s.settimeout(0.3); sys.exit(0 if s.connect_ex(('127.0.0.1', $port)) == 0 else 1)"; then
      echo "[lab] $id:$port already in use on this host" >&2; bad=1
    fi
  done < <(cat_q ports "$@")
  [ $bad = 0 ] || die "port check failed (override with LAB_PORT_<NAME>, see lab/README.md)"
  say "ports OK"
}

cmd="${1:-help}"; shift || true
case "$cmd" in
  list) cat_q list ;;
  ports) check_ports "$@" ;;
  pull)
    need_docker
    # only images with no compatible local tag are pulled (lab/images.py; catalog local_fallbacks)
    ids_all="$(cat_q ids "${@:-all}")"
    while read -r img; do
      [ -z "$img" ] && continue
      plat=(); grep -qx "amd64 $img" <<<"$(cat_q images ${ids_all})" && plat=(--platform linux/amd64)
      ok=0
      for i in $(seq 1 "$RETRIES"); do
        say "pull $img (try $i/$RETRIES)"
        if timeout "$PULL_TIMEOUT" docker pull -q "${plat[@]}" "$img" >/dev/null; then ok=1; break; fi
        sleep $((5 * i))
      done
      [ $ok = 1 ] && say "  ok" || echo "[lab] WARNING: could not pull $img; retry later" >&2
    done < <("$PY" "$HERE/images.py" pull ${ids_all})
    ;;
  up)
    need_docker; [ $# -gt 0 ] || die "name one or more profiles (see: lab.sh list)"
    cat_q ids "$@" >/dev/null
    check_ports "$@"
    ids="$(cat_q svc-ids "$@")"
    emu="$(cat_q emulated "$@")"
    if [ -n "$emu" ] && [ "$(docker version --format '{{.Server.Arch}}')" = arm64 ]; then
      say "WARNING: amd64-only images run under emulation on this ARM64 host: $emu"
    fi
    # image choice: env / lab/.env.local > pinned if present > compatible local tag > pull pinned
    while IFS= read -r kv; do [ -n "$kv" ] && export "$kv"; done < <("$PY" "$HERE/images.py" env $(cat_q ids "$@"))
    if [ -n "$ids" ]; then
      say "starting: $ids (waits for health checks, up to ${WAIT_TIMEOUT}s)"
      compose "$ids" up -d --wait --wait-timeout "$WAIT_TIMEOUT"
    fi
    if grep -qw sqlserver <<<"$(cat_q ids "$@")"; then
      say "sqlserver: creating labdb.shop + enabling CDC (idempotent)"
      if docker exec alelt-lab-sqlserver test -x /opt/mssql-tools18/bin/sqlcmd 2>/dev/null; then
        docker exec alelt-lab-sqlserver /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P Lab_local_only1 -b -i /lab-init/10-shop.sql >/dev/null
      else "$PY" "$HERE/sqlserver_init.py"; fi   # image without sqlcmd (azure-sql-edge fallback)
    fi
    grep -qw files <<<"$(cat_q ids "$@")" && mkdir -p "$HERE/data"
    cat_q dsn "$@"
    say "next: lab.sh demo <source> | lab.sh status | lab.sh down"
    ;;
  status) docker ps -a --filter "label=com.docker.compose.project=$PROJECT" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' ;;
  down)
    need_docker
    if [ $# -eq 0 ]; then compose '*' down --remove-orphans; say "all lab containers removed (volumes kept; reset deletes them)"
    else compose "$(cat_q svc-ids "$@")" rm -s -f; fi
    ;;
  reset)
    need_docker
    ids="$(cat_q svc-ids "$@")"
    [ -n "$ids" ] && compose "$ids" rm -s -f -v
    for v in $(cat_q services "$@"); do docker volume inspect "$v" >/dev/null 2>&1 && docker volume rm "$v" >/dev/null && say "volume $v removed"; done
    grep -qw files <<<"$(cat_q ids "$@")" && rm -rf "$HERE/data/"* 2>/dev/null || true
    [ $# -eq 0 ] && docker network rm alelt-lab >/dev/null 2>&1 || true
    ;;
  demo)
    [ $# -eq 1 ] || die "usage: lab.sh demo <postgres|mysql|mariadb|sqlserver>"
    export ALELT_PG_ADMIN_URL="postgresql://lab:lab_local_only@127.0.0.1:47442"
    case "$1" in
      postgres) export ALELT_PG_SOURCE_CONTAINER=alelt-lab-postgres ;;
      mysql) export ALELT_MYSQL_TEST_PORT=47306 ALELTTEST_MYSQL_PASSWORD=lab_local_only ;;
      mariadb) export ALELT_MARIADB_TEST_PORT=47307 ALELTTEST_MYSQL_PASSWORD=lab_local_only ;;
      sqlserver) export ALELT_MSSQL_CONTAINER=alelt-lab-sqlserver ;;
      *) die "no demo pipeline for '$1' yet (postgres, mysql, mariadb, sqlserver)" ;;
    esac
    say "demo pipeline: $1 -> remote capture -> hub (throw-away metadata) -> worker -> landing (alelt-lab-landing)"
    [ -n "${ALELT_DEMO_HARNESS:-}" ] && [ -f "$ALELT_DEMO_HARNESS" ] || { say "End-to-end examples are coming in a later release. (set ALELT_DEMO_HARNESS to a local harness)"; exit 2; }
    set +e; "$PY" "$ALELT_DEMO_HARNESS" --source "$1"; rc=$?; set -e
    say "demo finished: exit $rc (0 PASS, 1 FAIL, 2 SKIP)"
    exit $rc
    ;;
  helper) exec "$PY" "$HERE/helper/lab_helper.py" "$@" ;;
  *) sed -n '3,9p' "$0" ;;
esac
