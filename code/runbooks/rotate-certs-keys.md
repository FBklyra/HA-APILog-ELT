# Rotate certificates and keys

Trigger: `AleltCertExpiring30d`, scheduled rotation (yearly), or suspected compromise (do it **now**, revoke first).
All new material goes to Key Vault; nothing is copied by hand into files, tickets or chat (R9).

| Item | Steps | Downtime |
|---|---|---|
| **Agent client cert** | 1. Enroll new cert on the Db2 server (`deploy/agent/` enrollment script) — old cert stays. 2. Add new thumbprint to `ALELT_AGENT_THUMBPRINTS` (both listed) → `docker compose up -d hub-api`. 3. Switch agent to new cert (agent config / restart service). 4. Console → Agent shows new thumbprint + expiry. 5. Remove old thumbprint, restart hub-api, revoke old cert at the CA, publish CRL → refresh `alelt-tls-agent-ca-crl`. | Agent reconnect only; spool covers it |
| **Hub server cert** | 1. Issue new cert (same SANs: console + agent hosts). 2. Update `alelt-tls-server-crt` / `alelt-tls-server-key` in Key Vault. 3. `sudo ./secrets/fetch-secrets.sh && docker compose exec nginx nginx -s reload`. 4. `openssl s_client -connect <host>:443 -servername <host>` shows new not-after. | None |
| **Agent CA chain / CRL** | Update `alelt-tls-agent-ca-crt` / `-crl` → fetch → nginx reload. CRL must be refreshed before its `nextUpdate` (nginx rejects all clients on an expired CRL). | None |
| **Destination key pair** (Snowflake) | 1. Generate new RSA key pair (encrypted PKCS#8) on an admin workstation. 2. `ALTER USER ALELT_LOADER_SVC SET RSA_PUBLIC_KEY_2='…'` (ALELT user only — never the legacy pipeline's, R2). 3. Put private key in `alelt-dest-private-key-p8`. 4. Fetch secrets, `docker compose up -d hub-api worker`. 5. Verify a batch applies. 6. `ALTER USER … UNSET RSA_PUBLIC_KEY`, shred local copy. | Workers restart; batches resume from checkpoint |
| **Postgres password** | 1. `docker compose exec postgres psql -U alelt_hub -c "\password alelt_hub"` with the new value from Key Vault. 2. Update `alelt-pg-password`. 3. Fetch secrets, `docker compose up -d` (all services). | ~1 min |
| **Alert webhook / SMTP** | Update secret, fetch, `docker compose restart alertmanager`; send a test alert. | None |

After each rotation: console Hub screen → credential rotation status updated; audit event present.
