/* SPDX-License-Identifier: Apache-2.0 */
#include <stdio.h>
#include <string.h>

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h> /* SecureZeroMemory */

#include "alelt_json.h"
#include "alelt_util.h"
#include "hub.h"
#include "log.h"
#include "version.h"

static int classify(const alelt_http_resp *r, const char *what, char *err, size_t errcap)
{
    if (r->status == 0) {
        snprintf(err, errcap, "%s: %s", what, r->err[0] ? r->err : "transport error");
        return -1;
    }
    if (r->status >= 200 && r->status < 300) return 0;
    snprintf(err, errcap, "%s: HTTP %d %.160s", what, r->status, alelt_buf_cstr(&r->body));
    return (r->status >= 400 && r->status < 500 && r->status != 408 && r->status != 429) ? -2 : -1;
}

int alelt_hub_post_json(alelt_http *h, const char *path, const char *json, size_t len, int timeout_ms, alelt_buf *resp_body,
                     char *err, size_t errcap)
{
    alelt_http_resp r;
    int rc;
    alelt_http_resp_init(&r);
    alelt_http_request(h, "POST", path, "application/json", NULL, json, len, timeout_ms, &r);
    rc = classify(&r, path, err, errcap);
    if (rc == 0 && resp_body) alelt_buf_append(resp_body, r.body.data ? r.body.data : (const uint8_t *)"", r.body.len);
    alelt_http_resp_free(&r);
    return rc;
}

static void limits_from(const fj_val *o, alelt_limits *l)
{
    int64_t v;
    l->cpu_pct = l->mem_mb = l->batch_kb = l->max_rate_kbps = l->poll_ms = -1;
    if (fj_get_i64(o, "cpu_pct", &v) == 0 && v >= 1 && v <= 6400) l->cpu_pct = (int)v;
    if (fj_get_i64(o, "mem_mb", &v) == 0 && v >= 64 && v <= 65536) l->mem_mb = (int)v;
    if (fj_get_i64(o, "batch_kb", &v) == 0 && v >= 64 && v <= 65536) l->batch_kb = (int)v;
    if (fj_get_i64(o, "max_rate_kbps", &v) == 0 && v >= 0 && v <= 10000000) l->max_rate_kbps = (int)v;
    if (fj_get_i64(o, "poll_ms", &v) == 0 && v >= 50 && v <= 600000) l->poll_ms = (int)v;
}

int alelt_hub_register(alelt_http *h, const alelt_config *cfg, const char *hostname, alelt_register_result *out, char *err,
                    size_t errcap)
{
    alelt_buf b, resp;
    alelt_jw w;
    fj_doc *d;
    int rc;
    memset(out, 0, sizeof(*out));
    alelt_buf_init(&b, 64 * 1024);
    alelt_buf_init(&resp, 1024 * 1024);
    jw_init(&w, &b);
    jw_obj(&w); /* RegisterRequest */
    jw_key(&w, "agent_id"); jw_str(&w, cfg->agent_id);
    jw_key(&w, "connection_id"); jw_str(&w, cfg->connection_id);
    jw_key(&w, "engine"); jw_str(&w, cfg->engine);
    jw_key(&w, "version"); jw_str(&w, ALELT_AGENT_VERSION);
    jw_key(&w, "hostname"); jw_str(&w, hostname);
    jw_key(&w, "database"); jw_str(&w, cfg->database);
    jw_key(&w, "capabilities");
    jw_arr(&w);
    jw_str(&w, "raw_log");
    jw_str(&w, "catalog");
    jw_str(&w, "snapshot_jsonl");
    jw_str(&w, "verify_table");
    jw_str(&w, "codec_zlib");
    jw_str(&w, "codec_none");
    jw_str(&w, "commands_longpoll");
    jw_str(&w, "credential.v1");   /* REQ-26: stores the per-agent credential returned at registration */
    jw_str(&w, "discard_spool");   /* AHS-05: START_CAPTURE{from_pos, discard_spool} */
    jw_arr_end(&w);
    jw_obj_end(&w);
    if (jw_ok(&w) != 0) {
        alelt_buf_free(&b);
        alelt_buf_free(&resp);
        snprintf(err, errcap, "register: encode failed");
        return -1;
    }
    rc = alelt_hub_post_json(h, "/agent/v1/register", (const char *)b.data, b.len, 30000, &resp, err, errcap);
    alelt_buf_free(&b);
    if (rc != 0) {
        alelt_buf_free(&resp);
        return rc;
    }
    d = fj_parse((const char *)resp.data, resp.len, err, errcap);
    if (resp.data) SecureZeroMemory(resp.data, resp.len); /* may carry the one-time credential */
    alelt_buf_free(&resp);
    if (!d) return -1;
    {
        const fj_val *r = fj_root(d), *v;
        int acc = 0;
        if (fj_get_bool(r, "accepted", &acc) != 0) {
            snprintf(err, errcap, "register: response lacks 'accepted'");
            fj_free(d);
            return -1;
        }
        out->accepted = acc;
        v = fj_get(r, "resume_from");
        if (v && v->type == FJ_OBJ && fj_get_str(v, "text")) alelt_strcpy(out->resume_from, ALELT_POS_MAX, fj_get_str(v, "text"));
        v = fj_get(r, "limits");
        if (v && v->type == FJ_OBJ) {
            out->has_limits = 1;
            limits_from(v, &out->limits);
        }
        {
            const char *cr = fj_get_str(r, "credential"), *ex = fj_get_str(r, "credential_expires_at");
            if (cr && alelt_strcpy(out->credential, sizeof(out->credential), cr) != 0) out->credential[0] = 0;
            if (ex) alelt_strcpy(out->credential_expires_at, sizeof(out->credential_expires_at), ex);
        }
    }
    fj_free(d);
    return 0;
}

int alelt_hub_post_batch(alelt_http *h, const char *header_json, const char *reposition, long long epoch, const uint8_t *body,
                      size_t len, int timeout_ms, alelt_ack *ack, char *err, size_t errcap)
{
    alelt_http_resp r;
    fj_doc *d;
    int rc, n = 0;
    const char *extra[7];
    char ep[24];
    memset(ack, 0, sizeof(*ack));
    extra[n++] = "X-ALELT-Batch";
    extra[n++] = header_json;
    if (reposition && *reposition) {
        extra[n++] = "X-ALELT-Reposition";
        extra[n++] = reposition;
    }
    if (epoch >= 0) {
        snprintf(ep, sizeof(ep), "%lld", epoch);
        extra[n++] = "X-ALELT-Lease-Epoch";
        extra[n++] = ep;
    }
    extra[n] = NULL;
    alelt_http_resp_init(&r);
    alelt_http_request(h, "POST", "/agent/v1/blocks", "application/octet-stream", extra, body, len, timeout_ms, &r);
    rc = classify(&r, "blocks", err, errcap);
    ack->retry_after_s = r.retry_after_s;
    if (r.status == 409) {
        /* {"detail": {"error": "position gap/overlap", "expected_from": "...", "got": "..."}} (AHS-05) */
        fj_doc *gd = fj_parse((const char *)alelt_buf_cstr(&r.body), r.body.len, NULL, 0);
        if (gd) {
            const fj_val *det = fj_get(fj_root(gd), "detail");
            const char *ef = (det && det->type == FJ_OBJ) ? fj_get_str(det, "expected_from") : NULL;
            if (ef) alelt_strcpy(ack->expected_from, ALELT_POS_MAX, ef);
            fj_free(gd);
        }
        rc = -3;
    }
    if (r.status == 423) rc = -4;
    if (r.status == 426) rc = -6;  /* protocol upgrade required: stop sending, alert (XC-16) */
    if (r.status == 413) rc = -7;  /* batch larger than the hub accepts (XC-16) */
    if (rc != 0) {
        alelt_http_resp_free(&r);
        return rc;
    }
    d = fj_parse((const char *)alelt_buf_cstr(&r.body), r.body.len, err, errcap);
    alelt_http_resp_free(&r);
    if (!d) return -1;
    {
        const fj_val *root = fj_root(d), *dp = fj_get(root, "durable_pos");
        const char *bid = fj_get_str(root, "batch_id");
        int dup = 0;
        if (!bid || !dp || !fj_get_str(dp, "text")) {
            snprintf(err, errcap, "blocks: malformed BatchAck");
            fj_free(d);
            return -1;
        }
        alelt_strcpy(ack->batch_id, sizeof(ack->batch_id), bid);
        alelt_strcpy(ack->durable_pos, ALELT_POS_MAX, fj_get_str(dp, "text"));
        if (fj_get_bool(root, "duplicate", &dup) == 0) ack->duplicate = dup;
    }
    fj_free(d);
    return 0;
}

int alelt_hub_post_snapshot(alelt_http *h, const char *header_json, const uint8_t *body, size_t len, int timeout_ms,
                         char *err, size_t errcap)
{
    alelt_http_resp r;
    int rc;
    alelt_http_resp_init(&r);
    {
        const char *extra[3] = {"X-ALELT-Snapshot", header_json, NULL};
        alelt_http_request(h, "POST", "/agent/v1/snapshot", "application/octet-stream", extra, body, len, timeout_ms, &r);
    }
    rc = classify(&r, "snapshot", err, errcap);
    alelt_http_resp_free(&r);
    return rc;
}

int alelt_hub_poll_commands(alelt_http *h, const char *agent_id, int wait_s, alelt_on_command cb, void *ctx, char *err,
                         size_t errcap)
{
    alelt_http_resp r;
    char path[256];
    fj_doc *d;
    const fj_val *root, *list, *c;
    int rc;
    /* agent_id is validated as URL-safe by the caller (see agent.c). */
    if (_snprintf_s(path, sizeof(path), _TRUNCATE, "/agent/v1/commands?agent_id=%s&wait=%d", agent_id, wait_s) < 0) {
        snprintf(err, errcap, "commands: path too long");
        return -1;
    }
    alelt_http_resp_init(&r);
    alelt_http_request(h, "GET", path, NULL, NULL, NULL, 0, (wait_s + 15) * 1000, &r);
    rc = classify(&r, "commands", err, errcap);
    if (rc != 0 || r.status == 204 || r.body.len == 0) {
        alelt_http_resp_free(&r);
        return rc;
    }
    d = fj_parse((const char *)r.body.data, r.body.len, err, errcap);
    alelt_http_resp_free(&r);
    if (!d) return -1;
    root = fj_root(d);
    list = (root->type == FJ_OBJ) ? fj_get(root, "commands") : root;
    if (!list || list->type != FJ_ARR) {
        snprintf(err, errcap, "commands: expected a JSON array");
        fj_free(d);
        return -1;
    }
    for (c = list->child; c; c = c->next) cb(ctx, c);
    fj_free(d);
    return 0;
}

int alelt_hub_ack_command(alelt_http *h, const char *agent_id, const char *command_id, int ok, const char *result_json,
                       char *err, size_t errcap)
{
    char path[256];
    alelt_buf b;
    int rc;
    if (!alelt_is_uuid(command_id)) {
        snprintf(err, errcap, "ack: bad command id");
        return -2;
    }
    if (_snprintf_s(path, sizeof(path), _TRUNCATE, "/agent/v1/commands/%s/ack?agent_id=%s", command_id, agent_id) < 0) {
        snprintf(err, errcap, "ack: path too long");
        return -1;
    }
    alelt_buf_init(&b, 16 * 1024 * 1024);
    alelt_buf_printf(&b, "{\"ok\":%s,\"result\":", ok ? "true" : "false");
    alelt_buf_puts(&b, (result_json && *result_json) ? result_json : "null");
    alelt_buf_putc(&b, '}');
    rc = b.failed ? -1 : alelt_hub_post_json(h, path, (const char *)b.data, b.len, 15000, NULL, err, errcap);
    alelt_buf_free(&b);
    return rc;
}

int alelt_hub_lease(alelt_http *h, const char *op, const alelt_config *cfg, int ttl_s, const char *lease_id, long long epoch,
                 alelt_lease_grant *out, char *err, size_t errcap)
{
    alelt_buf b;
    alelt_jw w;
    alelt_http_resp r;
    char path[64];
    int rc;
    memset(out, 0, sizeof(*out));
    out->epoch = -1;
    out->capturing = -1;
    if (strcmp(op, "acquire") != 0 && strcmp(op, "renew") != 0 && strcmp(op, "release") != 0) {
        snprintf(err, errcap, "lease: bad op");
        return -2;
    }
    snprintf(path, sizeof(path), "/agent/v1/lease/%s", op);
    alelt_buf_init(&b, 16 * 1024);
    jw_init(&w, &b);
    jw_obj(&w); /* LeaseRequest */
    jw_key(&w, "agent_id"); jw_str(&w, cfg->agent_id);
    jw_key(&w, "connection_id"); jw_str(&w, cfg->connection_id);
    jw_key(&w, "ttl_s"); jw_i64(&w, ttl_s);
    if (lease_id && *lease_id) {
        jw_key(&w, "lease_id"); jw_str(&w, lease_id);
    }
    if (epoch >= 0) {
        jw_key(&w, "epoch"); jw_i64(&w, epoch);
    }
    jw_obj_end(&w);
    if (jw_ok(&w) != 0) {
        alelt_buf_free(&b);
        snprintf(err, errcap, "lease: encode failed");
        return -1;
    }
    alelt_http_resp_init(&r);
    alelt_http_request(h, "POST", path, "application/json", NULL, b.data, b.len, 15000, &r);
    alelt_buf_free(&b);
    rc = classify(&r, path, err, errcap);
    if (r.status == 409) rc = -3;
    if (rc != 0 || strcmp(op, "release") == 0) {
        alelt_http_resp_free(&r);
        return rc;
    }
    rc = alelt_lease_parse_reply((const char *)alelt_buf_cstr(&r.body), r.body.len, out, err, errcap);
    alelt_http_resp_free(&r);
    return rc;
}

int alelt_lease_parse_reply(const char *body, size_t len, alelt_lease_grant *out, char *err, size_t errcap)
{
    fj_doc *d;
    memset(out, 0, sizeof(*out));
    out->epoch = -1;
    out->capturing = -1;
    d = fj_parse(body, len, err, errcap);
    if (!d) return -1;
    {
        const fj_val *root = fj_root(d), *v;
        int cap = 0;
        int64_t i;
        double dv;
        const char *s;
        if (root->type != FJ_OBJ || fj_get_bool(root, "granted", &out->granted) != 0) {
            snprintf(err, errcap, "lease: reply lacks 'granted'");
            fj_free(d);
            return -1;
        }
        if (fj_get_bool(root, "takeover", &out->takeover) != 0) out->takeover = 0;
        if (fj_get_bool(root, "capturing", &cap) == 0) out->capturing = cap ? 1 : 0; /* null/absent = -1 */
        if (fj_get_i64(root, "epoch", &i) == 0) out->epoch = (long long)i;
        if (fj_get_i64(root, "ttl_s", &i) == 0 && i > 0 && i <= 3600) out->ttl_s = (int)i;
        if (fj_as_dbl(fj_get(root, "retry_after_s"), &dv) == 0 && dv > 0 && dv < 3600) out->retry_after_s = dv;
        s = fj_get_str(root, "holder");
        if (s) alelt_strcpy(out->holder, sizeof(out->holder), s);
        s = fj_get_str(root, "lease_id");
        if (s) alelt_strcpy(out->lease_id, sizeof(out->lease_id), s);
        v = fj_get(root, "resume_from");
        if (v && v->type == FJ_OBJ && fj_get_str(v, "text")) alelt_strcpy(out->resume_from, ALELT_POS_MAX, fj_get_str(v, "text"));
        if (out->granted && (out->epoch < 0 || !out->lease_id[0])) {
            snprintf(err, errcap, "lease: granted without lease_id/epoch");
            fj_free(d);
            return -1;
        }
    }
    fj_free(d);
    return 0;
}

int alelt_hub_rotate_credential(alelt_http *h, char *cred, size_t credcap, char *expires, size_t expcap, char *err,
                             size_t errcap)
{
    alelt_buf resp;
    fj_doc *d;
    const char *c, *e;
    int rc;
    cred[0] = 0;
    expires[0] = 0;
    alelt_buf_init(&resp, 64 * 1024);
    rc = alelt_hub_post_json(h, "/agent/v1/credential/rotate", "{}", 2, 15000, &resp, err, errcap);
    if (rc != 0) {
        alelt_buf_free(&resp);
        return rc;
    }
    d = fj_parse((const char *)alelt_buf_cstr(&resp), resp.len, err, errcap);
    if (resp.data) SecureZeroMemory(resp.data, resp.len);
    alelt_buf_free(&resp);
    if (!d) return -1;
    c = fj_get_str(fj_root(d), "credential");
    e = fj_get_str(fj_root(d), "credential_expires_at");
    rc = (c && e && alelt_strcpy(cred, credcap, c) == 0 && alelt_strcpy(expires, expcap, e) == 0) ? 0 : -1;
    if (rc != 0) snprintf(err, errcap, "credential/rotate: malformed reply");
    fj_free(d);
    return rc;
}
