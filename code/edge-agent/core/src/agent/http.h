/* SPDX-License-Identifier: Apache-2.0
 * Outbound-only WinHTTP client: mTLS client cert from LocalMachine\My by
 * thumbprint, server SPKI pinning checked BEFORE the body is sent, TLS 1.2+,
 * redirects disabled. Dev mode: plain HTTP + X-ALELT-Agent-Token header.
 */
#ifndef ALELT_HTTP_H
#define ALELT_HTTP_H

#include <stddef.h>
#include <stdint.h>

#include "config.h"
#include "alelt_buf.h"

typedef struct alelt_http alelt_http;

alelt_http *alelt_http_new(const alelt_config *cfg, char *err, size_t errcap);
void alelt_http_free(alelt_http *h);
/* AHS-25 (tests, diagnostics): 0 = direct (no WPAD), 1 = system proxy, 2 = named proxy, -1 = no link. */
int alelt_http_proxy_kind(const alelt_http *h);

typedef struct alelt_http_resp {
    int status;   /* HTTP status, 0 on transport failure */
    int retry_after_s; /* Retry-After (delta seconds) when sent, else -1 */
    alelt_buf body;  /* response body (capped)              */
    char err[256];
} alelt_http_resp;

/* AHS-23: abort every in-flight request (service stop); returns how many were aborted. */
int alelt_http_abort_all(void);

void alelt_http_resp_init(alelt_http_resp *r);
void alelt_http_resp_free(alelt_http_resp *r);

/* path_query: e.g. "/agent/v1/register". extra: optional NULL-terminated list of
 * name, value pairs (X-ALELT-Batch, X-ALELT-Snapshot, X-ALELT-Reposition). content_type may be NULL. */
int alelt_http_request(alelt_http *h, const char *method, const char *path_query, const char *content_type,
                    const char *const *extra, const void *body, size_t body_len, int timeout_ms,
                    alelt_http_resp *resp);

#endif
