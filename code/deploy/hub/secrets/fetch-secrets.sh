#!/usr/bin/env bash
# Materialise hub secrets from Azure Key Vault into tmpfs-backed files (R9: never in Git / .env / images).
# Auth: VM system-assigned managed identity with Key Vault "Key Vault Secrets User" role.
# Run as root at boot (systemd unit in runbooks/install-hub.md) and after every rotation.
# DEP-24: both secret and TLS dirs are tmpfs (nothing on persistent disk); files are 0440 root:$ALELT_SECRETS_GID (the
# containers join that group via compose `group_add`), dirs 0750; .env is PARSED for three keys, never sourced.
set -euo pipefail
ENV_FILE="$(dirname "$0")/../.env"

env_get() {  # env_get KEY: value of KEY=... from .env (no shell evaluation; quotes stripped)
  [ -f "$ENV_FILE" ] || return 0
  sed -n "s/^[[:space:]]*$1=//p" "$ENV_FILE" | tail -n 1 | sed -e 's/^"\(.*\)"$/\1/' -e "s/^'\(.*\)'$/\1/"
}
ALELT_KEYVAULT_NAME="${ALELT_KEYVAULT_NAME:-$(env_get ALELT_KEYVAULT_NAME)}"
DIR="${ALELT_SECRETS_DIR:-$(env_get ALELT_SECRETS_DIR)}"; DIR="${DIR:-/etc/alelt/secrets}"
TLS="${ALELT_TLS_DIR:-$(env_get ALELT_TLS_DIR)}"; TLS="${TLS:-/etc/alelt/tls}"
GID="${ALELT_SECRETS_GID:-$(env_get ALELT_SECRETS_GID)}"; GID="${GID:-10001}"
: "${ALELT_KEYVAULT_NAME:?set ALELT_KEYVAULT_NAME in the environment or .env}"
case "$ALELT_KEYVAULT_NAME" in *[!A-Za-z0-9-]*) echo "invalid ALELT_KEYVAULT_NAME" >&2; exit 2 ;; esac
case "$GID" in *[!0-9]*) echo "invalid ALELT_SECRETS_GID" >&2; exit 2 ;; esac
[ "$(id -u)" -eq 0 ] || { echo "run as root" >&2; exit 1; }

for d in "$DIR" "$TLS"; do
  mkdir -p "$d"
  mountpoint -q "$d" || mount -t tmpfs -o "size=8m,mode=0750,uid=0,gid=$GID,noexec,nosuid,nodev" tmpfs "$d"
  chown "0:$GID" "$d"; chmod 0750 "$d"
done
az login --identity --allow-no-subscriptions --output none

get() { az keyvault secret show --vault-name "$ALELT_KEYVAULT_NAME" --name "$1" --query value -o tsv; }
put() { umask 077; get "$1" > "$2.tmp"; chown "0:$GID" "$2.tmp"; chmod 0440 "$2.tmp"; mv "$2.tmp" "$2"; }

#   Key Vault secret name        -> file
put alelt-pg-password               "$DIR/pg_password"
put alelt-db-url                    "$DIR/alelt_db_url"
put alelt-dest-private-key-p8       "$DIR/dest_private_key.p8"
put alelt-alert-teams-webhook       "$DIR/teams_webhook_url"
put alelt-alert-smtp-password       "$DIR/smtp_password"
put alelt-tls-server-crt            "$TLS/server.crt"
put alelt-tls-server-key            "$TLS/server.key"
put alelt-tls-agent-ca-crt          "$TLS/agent-ca.crt"
put alelt-tls-agent-ca-crl          "$TLS/agent-ca.crl"

# CRL expiry for the AleltAgentCrlExpiring alert (DEP-26): an expired CRL makes nginx reject every agent.
if [ -n "${ALELT_NODE_TEXTFILE_DIR:-$(env_get ALELT_NODE_TEXTFILE_DIR)}" ]; then
  T="${ALELT_NODE_TEXTFILE_DIR:-$(env_get ALELT_NODE_TEXTFILE_DIR)}"
  nu=$(openssl crl -in "$TLS/agent-ca.crl" -noout -nextupdate | cut -d= -f2-)
  printf 'alelt_agent_crl_next_update_timestamp_seconds %s\n' "$(date -d "$nu" +%s)" > "$T/alelt_agent_crl.prom.tmp"
  mv "$T/alelt_agent_crl.prom.tmp" "$T/alelt_agent_crl.prom"
fi
echo "secrets refreshed from $ALELT_KEYVAULT_NAME (tmpfs, 0440 root:$GID)"
