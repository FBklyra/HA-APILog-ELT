/* SPDX-License-Identifier: Apache-2.0
 * Dev-mode agent token: environment variable, else DPAPI (machine scope)
 * protected file "devtoken.bin" next to aleltagent.json (ACL'd). Never in the
 * JSON config (R9). Production uses mTLS and needs no token.
 */
#ifndef ALELT_DEVTOKEN_H
#define ALELT_DEVTOKEN_H

#include "config.h"

int alelt_dev_token_load(const alelt_config *cfg, char *out, size_t cap);
int alelt_dev_token_save(const wchar_t *config_path, const char *token, char *err, size_t errcap);
int alelt_dev_token_path(const wchar_t *config_path, wchar_t *out, size_t cap);

/* Per-agent credential (REQ-26 / AHS-04, hub contract X-136): the enrollment token (alelte_) is exchanged
 * at registration (capability credential.v1) for a per-agent credential (alelta_). It is kept in
 * <spool_dir>\credential.bin (DPAPI machine scope; the spool dir is ACL'd SYSTEM, Administrators and
 * the service account) with its expiry, used instead of the enrollment token from then on (also after
 * a restart) and rotated before it expires. The value is never logged. One process-wide store shared
 * by every hub link (thread-safe). */
int alelt_cred_init(const alelt_config *cfg);            /* idempotent; 0 = a token is available */
void alelt_cred_get(char *out, size_t cap);           /* current token ("" = none) */
int alelt_cred_is_per_agent(void);                    /* 1 = an alelta_ credential is in use */
/* Persist atomically, THEN switch to it (the old one stays valid at the hub during the grace). */
int alelt_cred_store(const char *cred, const char *expires_iso, char *err, size_t errcap);
/* 1 when a per-agent credential passed 2/3 of its lifetime. */
int alelt_cred_rotation_due(long long now_unix);
long long alelt_iso_to_unix(const char *iso);         /* -1 = unparsable */
void alelt_cred_reset_for_tests(void);                /* forget the in-memory state (simulates a restart) */

#endif
