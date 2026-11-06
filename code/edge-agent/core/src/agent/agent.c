/* SPDX-License-Identifier: Apache-2.0
 * Agent runtime: three threads, each with its own outbound HTTP client.
 *   control - register, heartbeat, command long-poll (whitelist, R5)
 *   worker  - owns the source plugin: capture loop, catalog, snapshot, probes,
 *             governor. Only this thread calls into the plugin.
 *   sender  - ships spooled batches oldest-first; deletes only on ACK.
 *   ha      - only with ha.mode=active_passive: acquires/renews the hub capture
 *             lease (X-47/X-48); standby = no reading, no sending.
 * Lock order: send_lock -> put_lock -> lock (the spool has its own inner lock).
 * No listening socket anywhere (R4).
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "agent.h"
#include "command.h"
#include "config.h"
#include "devtoken.h"
#include "alelt_hash.h"
#include "alelt_json.h"
#include "alelt_plugin.h"
#include "alelt_util.h"
#include "governor.h"
#include "guard.h"
#include "ha.h"
#include "http.h"
#include "hub.h"
#include "jobobj.h"
#include "log.h"
#include "spool.h"
#include "version.h"

#define QCAP 16
#define SEEN_IDS 64
#define READ_BUF_MAX ((size_t)64 * 1024 * 1024)

typedef enum alelt_state { ST_IDLE = 0, ST_CAPTURING, ST_PAUSED, ST_PAUSED_GOV, ST_ERROR, ST_STANDBY } alelt_state;

typedef struct alelt_agent {
    alelt_config cfg;
    alelt_spool spool;
    int spool_open;
    HMODULE plugin;
    const alelt_source_vtbl *vt;
    void *pctx;
    HANDLE stop_ev, cmd_ev, send_ev;
    CRITICAL_SECTION lock;
    CRITICAL_SECTION put_lock;  /* worker: lease check + spool put; HA transitions  */
    CRITICAL_SECTION send_lock; /* sender: lease check + post + ack; takeover purge */
    int console;

    /* guarded by lock */
    int user_paused;
    int gov_paused;
    int plugin_error;
    char open_err[512]; /* last source open failure (commands answered with it while the source is down) */
    int registered;
    alelt_limits limits;
    char resume_from[ALELT_POS_MAX];
    char last_read_pos[ALELT_POS_MAX];
    alelt_cmd *q;
    int qhead, qn;
    char seen[SEEN_IDS][37];
    int seen_next;
    long long batches_sent, errors;
    int exit_code;
    char hostname[256];
    alelt_ha ha; /* capture lease state, guarded by lock (standalone: always active) */

    /* worker thread only (AHS-06: shared by the capture loop and the snapshot/verify loops) */
    alelt_gov gov;
    alelt_gov_cfg gc;
    uint64_t next_gov;
    struct cap_state *cs;
} alelt_agent;

static alelt_agent *g_a = NULL;
static void (*g_stop_progress)(void) = NULL;

void alelt_agent_on_stop_progress(void (*cb)(void))
{
    g_stop_progress = cb;
}

/* join with progress callbacks (AHS-23) */
static int join_thread(HANDLE t, DWORD timeout_ms)
{
    uint64_t t0 = alelt_mono_ms();
    for (;;) {
        DWORD r = WaitForSingleObject(t, 2000);
        if (r == WAIT_OBJECT_0) return 0;
        if (g_stop_progress) g_stop_progress();
        if (alelt_mono_ms() - t0 >= timeout_ms) return -1;
    }
}

/* ------------------------------------------------------------------ host api for plugins */
static void host_log(int level, const char *component, const char *msg)
{
    alelt_log_ev(level, component ? component : "plugin", msg, NULL, NULL, NULL, NULL);
}

static int host_buf_append(alelt_host_buf *b, const void *p, size_t n)
{
    return alelt_buf_append((alelt_buf *)(void *)b, p, n);
}

static int host_should_stop(void)
{
    return g_a && WaitForSingleObject(g_a->stop_ev, 0) == WAIT_OBJECT_0;
}

static const alelt_host_api k_host = {ALELT_PLUGIN_ABI_VERSION, host_log, host_buf_append, host_should_stop};

/* ------------------------------------------------------------------ helpers */
static int stopping(alelt_agent *a)
{
    return WaitForSingleObject(a->stop_ev, 0) == WAIT_OBJECT_0;
}

/* Wait ms or until stop (returns 1 if stopping). Optional extra event. */
static int nap(alelt_agent *a, DWORD ms, HANDLE extra)
{
    HANDLE hs[2];
    DWORD n = 1, r;
    hs[0] = a->stop_ev;
    if (extra) hs[n++] = extra;
    r = WaitForMultipleObjects(n, hs, FALSE, ms);
    return r == WAIT_OBJECT_0;
}

static DWORD backoff_ms(int attempt, int max_s)
{
    uint32_t j = 0;
    uint64_t ms = 1000;
    int i;
    for (i = 0; i < attempt && ms < (uint64_t)max_s * 1000; i++) ms *= 2;
    if (ms > (uint64_t)max_s * 1000) ms = (uint64_t)max_s * 1000;
    (void)alelt_random(&j, sizeof(j));
    ms = ms / 2 + (j % (ms / 2 + 1)); /* "equal jitter" */
    return (DWORD)ms;
}

static const char *state_name(alelt_state s)
{
    switch (s) {
    case ST_CAPTURING: return "CAPTURING";
    case ST_PAUSED: return "PAUSED";
    case ST_PAUSED_GOV: return "PAUSED_BY_GOVERNOR";
    case ST_ERROR: return "ERROR";
    case ST_STANDBY: return "STANDBY";
    default: return "IDLE";
    }
}

static alelt_state cur_state(alelt_agent *a)
{
    int capturing = 0;
    alelt_state s;
    alelt_spool_snapshot(&a->spool, NULL, NULL, NULL, NULL, &capturing);
    EnterCriticalSection(&a->lock);
    if (a->ha.enabled && !a->ha.active) s = ST_STANDBY;
    else if (a->plugin_error) s = ST_ERROR;
    else if (a->gov_paused) s = ST_PAUSED_GOV;
    else if (a->user_paused) s = ST_PAUSED;
    else if (capturing) s = ST_CAPTURING;
    else s = ST_IDLE;
    LeaveCriticalSection(&a->lock);
    return s;
}

static void jw_pos(alelt_jw *w, const char *engine, const char *text)
{
    if (!text || !*text) {
        jw_null(w);
        return;
    }
    jw_obj(w);
    jw_key(w, "engine"); jw_str(w, engine);
    jw_key(w, "text"); jw_str(w, text);
    jw_obj_end(w);
}

static void count_error(alelt_agent *a)
{
    EnterCriticalSection(&a->lock);
    a->errors++;
    LeaveCriticalSection(&a->lock);
}

/* ------------------------------------------------------------------ HA helpers */
static int ha_active(alelt_agent *a)
{
    int v;
    EnterCriticalSection(&a->lock);
    v = a->ha.active;
    LeaveCriticalSection(&a->lock);
    return v;
}

/* Lease lost: once this returns no batch is spooled (put_lock) and the worker does
 * not start another READ. */
static void ha_lost(alelt_agent *a, const char *why)
{
    alelt_ha_event ev;
    EnterCriticalSection(&a->put_lock);
    EnterCriticalSection(&a->lock);
    ev = alelt_ha_on_lost(&a->ha, alelt_mono_ms());
    LeaveCriticalSection(&a->lock);
    LeaveCriticalSection(&a->put_lock);
    if (ev == ALELT_HA_LOST) alelt_logf(ALELT_LOG_WARN, "ha", "capture lease lost (%s): reading stopped, now STANDBY", why);
}

/* Reports to the hub. Command outcomes (COMMAND_RESULT / COMMAND_REJECTED /
 * PROBE_RESULT with a readable command id) go to POST /commands/{id}/ack as
 * {ok, result}; everything else is an AgentEvent on POST /events.
 * result = detail_json (valid JSON object) or {"message": detail_text}. */
static void post_event(alelt_agent *a, alelt_http *h, const char *type, const char *command_id, int ok, const char *detail_text,
                       const char *detail_json)
{
    alelt_buf b, det;
    alelt_jw w;
    char ts[ALELT_TS_LEN], err[256];
    int rc;
    if (!h) return;
    alelt_buf_init(&det, 16 * 1024 * 1024);
    if (detail_json) {
        alelt_buf_puts(&det, detail_json);
    } else {
        jw_init(&w, &det);
        jw_obj(&w);
        jw_key(&w, "message"); jw_str(&w, detail_text ? detail_text : "");
        jw_obj_end(&w);
    }
    if (command_id && alelt_is_uuid(command_id) &&
        (!strcmp(type, "COMMAND_RESULT") || !strcmp(type, "COMMAND_REJECTED") || !strcmp(type, "PROBE_RESULT"))) {
        rc = alelt_hub_ack_command(h, a->cfg.agent_id, command_id, ok, alelt_buf_cstr(&det), err, sizeof(err));
        if (rc != 0) alelt_logf(ALELT_LOG_WARN, "control", "command ack not delivered: %s", err);
        alelt_buf_free(&det);
        return;
    }
    alelt_buf_init(&b, 17 * 1024 * 1024);
    jw_init(&w, &b);
    alelt_now_iso(ts);
    jw_obj(&w); /* AgentEvent */
    jw_key(&w, "agent_id"); jw_str(&w, a->cfg.agent_id);
    jw_key(&w, "type"); jw_str(&w, type);
    jw_key(&w, "command_id"); jw_str_or_null(&w, (command_id && alelt_is_uuid(command_id)) ? command_id : NULL);
    jw_key(&w, "detail"); jw_raw(&w, (const char *)alelt_buf_cstr(&det), det.len);
    jw_key(&w, "at"); jw_str(&w, ts);
    jw_obj_end(&w);
    (void)ok;
    if (jw_ok(&w) == 0) {
        if (alelt_hub_post_json(h, "/agent/v1/events", (const char *)b.data, b.len, 15000, NULL, err, sizeof(err)) != 0) {
            alelt_logf(ALELT_LOG_WARN, "control", "event %s not delivered: %s", type, err); /* AHS-15 */
        }
    }
    alelt_buf_free(&b);
    alelt_buf_free(&det);
}

/* Typed failure ack (docs/AGENT-COMMANDS.md): {"code": NOT_ALLOWED | NOT_FOUND | NO_KEY | PAUSED | TOO_LARGE |
 * TIMEOUT | SOURCE_ERROR | UNSUPPORTED, "message": ...}; the hub branches on the code (refused vs retry later). */
static void post_fail(alelt_agent *a, alelt_http *h, const char *type, const char *command_id, const char *code,
                      const char *msg)
{
    alelt_buf det;
    alelt_jw w;
    alelt_buf_init(&det, 64 * 1024);
    jw_init(&w, &det);
    jw_obj(&w);
    jw_key(&w, "code"); jw_str(&w, code);
    jw_key(&w, "message"); jw_str(&w, msg ? msg : "");
    jw_obj_end(&w);
    post_event(a, h, type, command_id, 0, msg, jw_ok(&w) == 0 ? alelt_buf_cstr(&det) : NULL);
    alelt_buf_free(&det);
}

static void apply_limits(alelt_agent *a, const alelt_limits *req, const char *source)
{
    alelt_limits l;
    char err[256];
    EnterCriticalSection(&a->lock);
    l = a->limits;
    if (req->cpu_pct >= 0) l.cpu_pct = req->cpu_pct;
    if (req->mem_mb >= 0) l.mem_mb = req->mem_mb;
    if (req->batch_kb >= 0) l.batch_kb = req->batch_kb;
    if (req->max_rate_kbps >= 0) l.max_rate_kbps = req->max_rate_kbps;
    if (req->poll_ms >= 0) l.poll_ms = req->poll_ms;
    alelt_limits_clamp(&l, &a->cfg.limits_max); /* never above DBA-approved maxima */
    a->limits = l;
    LeaveCriticalSection(&a->lock);
    if (alelt_job_apply(&l, err, sizeof(err)) != 0) {
        alelt_logf(ALELT_LOG_ERROR, "job", "cannot update job limits: %s", err);
    }
    alelt_logf(ALELT_LOG_INFO, "agent", "limits from %s: cpu_pct=%d mem_mb=%d batch_kb=%d max_rate_kbps=%d poll_ms=%d", source,
            l.cpu_pct, l.mem_mb, l.batch_kb, l.max_rate_kbps, l.poll_ms);
}

/* ------------------------------------------------------------------ command intake (control thread) */
typedef struct poll_ctx {
    alelt_agent *a;
    alelt_http *h;
} poll_ctx;

static int seen_before(alelt_agent *a, const char *id)
{
    int i;
    for (i = 0; i < SEEN_IDS; i++) {
        if (a->seen[i][0] && strcmp(a->seen[i], id) == 0) return 1;
    }
    alelt_strcpy(a->seen[a->seen_next], 37, id);
    a->seen_next = (a->seen_next + 1) % SEEN_IDS;
    return 0;
}

static void on_command(void *vctx, const struct fj_val *v)
{
    poll_ctx *pc = (poll_ctx *)vctx;
    alelt_agent *a = pc->a;
    alelt_cmd *c;
    char err[256];
    int dup;
    c = (alelt_cmd *)malloc(sizeof(*c));
    if (!c) return;
    if (alelt_cmd_from_val(v, c, err, sizeof(err)) != 0) {
        /* R5: rejected and audited, never executed. */
        alelt_log_ev(ALELT_LOG_WARN, "command", err, NULL, NULL, NULL, c->command_id);
        post_event(a, pc->h, "COMMAND_REJECTED", c->command_id[0] ? c->command_id : NULL, 0, err, NULL);
        count_error(a);
        free(c);
        return;
    }
    EnterCriticalSection(&a->lock);
    dup = seen_before(a, c->command_id);
    LeaveCriticalSection(&a->lock);
    if (dup) {
        free(c);
        return;
    }
    alelt_logf(ALELT_LOG_INFO, "command", "accepted %s id=%s", alelt_cmd_str(c->name), c->command_id);
    switch (c->name) {
    case ALELT_CMD_PAUSE:
        EnterCriticalSection(&a->lock);
        a->user_paused = 1;
        LeaveCriticalSection(&a->lock);
        post_event(a, pc->h, "COMMAND_RESULT", c->command_id, 1, "paused", NULL);
        break;
    case ALELT_CMD_RESUME:
        EnterCriticalSection(&a->lock);
        a->user_paused = 0;
        LeaveCriticalSection(&a->lock);
        SetEvent(a->cmd_ev);
        post_event(a, pc->h, "COMMAND_RESULT", c->command_id, 1, "resumed", NULL);
        break;
    case ALELT_CMD_SET_LIMITS:
        apply_limits(a, &c->limits, "SET_LIMITS");
        post_event(a, pc->h, "COMMAND_RESULT", c->command_id, 1, "limits applied (clamped to limits_max)", NULL);
        break;
    case ALELT_CMD_SHUTDOWN:
        post_event(a, pc->h, "COMMAND_RESULT", c->command_id, 1, "shutting down", NULL);
        alelt_logf(ALELT_LOG_WARN, "command", "SHUTDOWN requested by hub");
        SetEvent(a->stop_ev);
        break;
    default:
        EnterCriticalSection(&a->lock);
        if (a->qn < QCAP) {
            a->q[(a->qhead + a->qn) % QCAP] = *c;
            a->qn++;
            LeaveCriticalSection(&a->lock);
            SetEvent(a->cmd_ev);
        } else {
            LeaveCriticalSection(&a->lock);
            alelt_log_ev(ALELT_LOG_WARN, "command", "command queue full; rejected", NULL, NULL, NULL, c->command_id);
            post_event(a, pc->h, "COMMAND_REJECTED", c->command_id, 0, "queue full, retry later", NULL);
        }
        break;
    }
    free(c);
}

static int build_heartbeat(alelt_agent *a, alelt_buf *b)
{
    alelt_jw w;
    char ts[ALELT_TS_LEN], durable[ALELT_POS_MAX], last[ALELT_POS_MAX];
    uint64_t sb = 0;
    double cpu, mem;
    long long sent, errs;
    alelt_state st = cur_state(a);
    alelt_spool_snapshot(&a->spool, &sb, NULL, durable, NULL, NULL);
    alelt_self_usage(&cpu, &mem);
    EnterCriticalSection(&a->lock);
    alelt_strcpy(last, ALELT_POS_MAX, a->last_read_pos);
    sent = a->batches_sent;
    errs = a->errors;
    LeaveCriticalSection(&a->lock);
    alelt_now_iso(ts);
    jw_init(&w, b);
    jw_obj(&w); /* Heartbeat */
    jw_key(&w, "agent_id"); jw_str(&w, a->cfg.agent_id);
    jw_key(&w, "state"); jw_str(&w, state_name(st));
    jw_key(&w, "last_read_pos"); jw_pos(&w, a->cfg.engine, last);
    jw_key(&w, "durable_pos"); jw_pos(&w, a->cfg.engine, durable);
    jw_key(&w, "spool_bytes"); jw_u64(&w, sb);
    jw_key(&w, "cpu_pct"); jw_dbl(&w, cpu);
    jw_key(&w, "mem_mb"); jw_dbl(&w, mem);
    jw_key(&w, "batches_sent"); jw_i64(&w, sent);
    jw_key(&w, "errors"); jw_i64(&w, errs);
    jw_key(&w, "at"); jw_str(&w, ts);
    jw_obj_end(&w);
    return jw_ok(&w);
}

static DWORD WINAPI control_thread(LPVOID p)
{
    alelt_agent *a = (alelt_agent *)p;
    alelt_http *h;
    char err[512];
    int attempt = 0;
    uint64_t next_hb = 0;
    poll_ctx pc;
    h = alelt_http_new(&a->cfg, err, sizeof(err));
    if (!h) {
        alelt_logf(ALELT_LOG_ERROR, "control", "hub link init failed: %s", err);
        a->exit_code = 10;
        SetEvent(a->stop_ev);
        return 1;
    }
    pc.a = a;
    pc.h = h;
    while (!stopping(a)) {
        int ok = 1;
        if (!a->registered) {
            alelt_register_result rr;
            int rc = alelt_hub_register(h, &a->cfg, a->hostname, &rr, err, sizeof(err));
            if (rc == 0 && rr.accepted) {
                EnterCriticalSection(&a->lock);
                a->registered = 1;
                alelt_strcpy(a->resume_from, ALELT_POS_MAX, rr.resume_from);
                LeaveCriticalSection(&a->lock);
                alelt_logf(ALELT_LOG_INFO, "control", "registered with hub; resume_from=%s", rr.resume_from[0] ? rr.resume_from : "-");
                if (rr.credential[0]) {
                    /* REQ-26 / AHS-04: enrollment token exchanged for a per-agent credential (shown once) */
                    char cerr[256];
                    if (alelt_cred_store(rr.credential, rr.credential_expires_at, cerr, sizeof(cerr)) == 0)
                        alelt_logf(ALELT_LOG_INFO, "control", "per-agent credential received and stored (expires %s); the "
                                "enrollment token is retired", rr.credential_expires_at);
                    else
                        alelt_logf(ALELT_LOG_ERROR, "control", "per-agent credential NOT stored (%s): the enrollment token "
                                "stops working 24 h after its first use", cerr);
                    SecureZeroMemory(rr.credential, sizeof(rr.credential));
                }
                if (rr.has_limits) apply_limits(a, &rr.limits, "register");
                attempt = 0;
                SetEvent(a->send_ev);
                SetEvent(a->cmd_ev);
            } else {
                if (rc == 0) snprintf(err, sizeof(err), "hub did not accept registration");
                alelt_logf(ALELT_LOG_WARN, "control", "register failed: %s", err);
                if (nap(a, backoff_ms(attempt++, a->cfg.backoff_max_s), NULL)) break;
                continue;
            }
        }
        if (alelt_mono_ms() >= next_hb && alelt_cred_rotation_due((long long)_time64(NULL))) {
            /* REQ-26: rotate the per-agent credential before it expires (old one valid during the grace) */
            char nc[256], ne[64], cerr[256];
            int rr = alelt_hub_rotate_credential(h, nc, sizeof(nc), ne, sizeof(ne), err, sizeof(err));
            if (rr == 0 && alelt_cred_store(nc, ne, cerr, sizeof(cerr)) == 0)
                alelt_logf(ALELT_LOG_INFO, "control", "per-agent credential rotated (new expiry %s)", ne);
            else
                alelt_logf(ALELT_LOG_WARN, "control", "credential rotation failed: %s", rr == 0 ? cerr : err);
            SecureZeroMemory(nc, sizeof(nc));
        }
        if (alelt_mono_ms() >= next_hb) {
            alelt_buf b;
            alelt_buf_init(&b, 64 * 1024);
            if (build_heartbeat(a, &b) == 0) {
                int rc = alelt_hub_post_json(h, "/agent/v1/heartbeat", (const char *)b.data, b.len, 15000, NULL, err, sizeof(err));
                if (rc != 0) {
                    ok = 0;
                    alelt_logf(ALELT_LOG_WARN, "control", "heartbeat failed: %s", err);
                    if (rc == -2) a->registered = 0; /* hub forgot us: register again */
                }
            }
            alelt_buf_free(&b);
            next_hb = alelt_mono_ms() + (uint64_t)a->cfg.heartbeat_s * 1000;
        }
        if (ok) {
            int rc = alelt_hub_poll_commands(h, a->cfg.agent_id, a->cfg.command_wait_s, on_command, &pc, err, sizeof(err));
            if (rc != 0) {
                ok = 0;
                alelt_logf(ALELT_LOG_WARN, "control", "command poll failed: %s", err);
            }
        }
        if (ok) {
            attempt = 0;
        } else if (nap(a, backoff_ms(attempt++, a->cfg.backoff_max_s), NULL)) {
            break;
        }
    }
    alelt_http_free(h);
    return 0;
}

/* ------------------------------------------------------------------ sender thread */
static DWORD WINAPI sender_thread(LPVOID p)
{
    alelt_agent *a = (alelt_agent *)p;
    alelt_http *h;
    char err[512];
    int attempt = 0;
    alelt_buf hdr, body;
    h = alelt_http_new(&a->cfg, err, sizeof(err));
    if (!h) {
        alelt_logf(ALELT_LOG_ERROR, "sender", "hub link init failed: %s", err);
        return 1;
    }
    alelt_buf_init(&hdr, 128 * 1024);
    alelt_buf_init(&body, (size_t)300 * 1024 * 1024);
    while (!stopping(a)) {
        alelt_spool_entry m;
        alelt_ack ack;
        int r, rc, rate;
        int deleted = 0;
        long long epoch;
        if (!a->registered || !ha_active(a)) { /* a standby never ships (X-48) */
            if (nap(a, 1000, a->send_ev)) break;
            continue;
        }
        alelt_buf_reset(&hdr);
        alelt_buf_reset(&body);
        EnterCriticalSection(&a->send_lock);
        EnterCriticalSection(&a->lock);
        epoch = alelt_ha_epoch(&a->ha);
        r = a->ha.active ? 1 : 0;
        LeaveCriticalSection(&a->lock);
        if (r) r = alelt_spool_oldest(&a->spool, &m, &hdr, &body);
        if (r <= 0) {
            LeaveCriticalSection(&a->send_lock);
            if (r == 0) {
                if (nap(a, 5000, a->send_ev)) break;
                continue;
            }
            alelt_logf(ALELT_LOG_ERROR, "sender", "cannot read oldest spool batch; retrying");
            count_error(a);
            if (nap(a, backoff_ms(attempt++, a->cfg.backoff_max_s), NULL)) break;
            continue;
        }
        {
            char rep[ALELT_UUID_LEN + 1];
            alelt_spool_reposition_for(&a->spool, m.from_pos, rep);
            rc = alelt_hub_post_batch(h, alelt_buf_cstr(&hdr), rep[0] ? rep : NULL, epoch, body.data, body.len,
                                   a->cfg.request_timeout_ms, &ack, err, sizeof(err));
        }
        if (rc == 0) deleted = alelt_spool_ack(&a->spool, ack.durable_pos, a->vt->compare_pos);
        if (rc == -4) ha_lost(a, "hub fenced the batch (423): another agent holds the lease");
        LeaveCriticalSection(&a->send_lock);
        if (rc == -4) {
            alelt_log_ev(ALELT_LOG_WARN, "sender", err, m.batch_id, m.from_pos, NULL, NULL);
            if (!a->cfg.ha_active_passive) {
                /* standalone agent but the connection runs active/passive: keep the spool, retry slowly */
                alelt_logf(ALELT_LOG_ERROR, "sender", "connection is in active/passive mode at the hub; set ha.mode=active_passive");
                count_error(a);
                if (nap(a, backoff_ms(attempt++, a->cfg.backoff_max_s), NULL)) break;
            }
            continue;
        }
        if (rc == -3) {
            /* Hub reports a position gap/overlap. AHS-05: when the hub already expects a position AFTER
             * this whole batch (expected_from > to_pos), the batch is covered: delete it (logged, never
             * silent). Otherwise never drop or rewrite: event SPOOL_BLOCKED_409 once per batch; the
             * operator recovers with START_CAPTURE{from_pos, discard_spool:true} (audited) or a re-sync. */
            static char blocked_id[ALELT_UUID_LEN + 1];
            int c = 0;
            if (ack.expected_from[0] && !a->vt->pos_valid(ack.expected_from)) ack.expected_from[0] = 0;
            if (ack.expected_from[0] &&
                a->vt->compare_pos(ack.expected_from, m.to_pos, &c) == 0 && c > 0) {
                int dr = alelt_spool_drop_head(&a->spool, m.batch_id);
                alelt_logf(ALELT_LOG_WARN, "sender", "batch %s (to_pos %s) is already covered by the hub (expected_from %s): %s",
                        m.batch_id, m.to_pos, ack.expected_from, dr == 1 ? "deleted from the spool" : "delete failed");
                if (dr == 1) {
                    char det[3 * ALELT_POS_MAX + 128];
                    snprintf(det, sizeof(det), "{\"batch_id\":\"%s\",\"to_pos\":\"%s\",\"expected_from\":\"%s\","
                             "\"action\":\"deleted_covered\"}", m.batch_id, m.to_pos, ack.expected_from);
                    post_event(a, h, "SPOOL_BATCH_COVERED", NULL, 1, NULL, det);
                    continue;
                }
            }
            alelt_log_ev(ALELT_LOG_ERROR, "sender", err, m.batch_id, m.from_pos, NULL, NULL);
            count_error(a);
            if (strcmp(blocked_id, m.batch_id) != 0) {
                char det[3 * ALELT_POS_MAX + 256];
                alelt_strcpy(blocked_id, sizeof(blocked_id), m.batch_id);
                snprintf(det, sizeof(det), "{\"batch_id\":\"%s\",\"from_pos\":\"%s\",\"expected_from\":%s%s%s,"
                         "\"message\":\"spool blocked by a position gap/overlap; recover with START_CAPTURE "
                         "{from_pos, discard_spool:true} or a re-sync\"}", m.batch_id, m.from_pos,
                         ack.expected_from[0] ? "\"" : "", ack.expected_from[0] ? ack.expected_from : "null",
                         ack.expected_from[0] ? "\"" : "");
                post_event(a, h, "SPOOL_BLOCKED_409", NULL, 0, NULL, det);   /* REQ-17 diagnose rule input */
            }
            if (nap(a, (DWORD)a->cfg.backoff_max_s * 1000, a->send_ev)) break;
            continue;
        }
        if (rc == -6 || rc == -7) {
            /* 426: this agent speaks an unsupported protocol; 413: the spool head is larger than the hub
             * accepts. Neither gets better by hammering: alert once per burst, retry at the slowest pace;
             * batch_kb is halved so batches read from now on are smaller (XC-16). */
            alelt_log_ev(ALELT_LOG_ERROR, "sender", err, m.batch_id, m.from_pos, NULL, NULL);
            count_error(a);
            if (attempt == 0) post_event(a, h, rc == -6 ? "PROTOCOL_UPGRADE_REQUIRED" : "BATCH_TOO_LARGE", NULL, 0, err, NULL);
            if (rc == -7) {
                EnterCriticalSection(&a->lock);
                if (a->limits.batch_kb > 128) a->limits.batch_kb /= 2;
                LeaveCriticalSection(&a->lock);
            }
            attempt++;
            if (nap(a, (DWORD)a->cfg.backoff_max_s * 1000, NULL)) break;
            continue;
        }
        if (rc != 0) {
            DWORD wait = backoff_ms(attempt++, a->cfg.backoff_max_s);
            alelt_log_ev(ALELT_LOG_WARN, "sender", err, m.batch_id, m.to_pos, NULL, NULL);
            count_error(a);
            if (rc == -2 && strstr(err, "HTTP 401")) a->registered = 0;
            if (ack.retry_after_s >= 0) {        /* XC-16: the hub says when (503 backpressure) */
                DWORD ra = (DWORD)(ack.retry_after_s > a->cfg.backoff_max_s ? a->cfg.backoff_max_s : ack.retry_after_s);
                wait = ra * 1000u + (DWORD)(alelt_mono_ms() % 1000u); /* + up to 1 s jitter */
            }
            if (nap(a, wait, NULL)) break;
            continue;
        }
        {
            EnterCriticalSection(&a->lock);
            a->batches_sent++;
            LeaveCriticalSection(&a->lock);
            alelt_log_ev(ALELT_LOG_DEBUG, "sender", ack.duplicate ? "batch acknowledged (duplicate)" : "batch acknowledged",
                      m.batch_id, ack.durable_pos, NULL, NULL);
            if (deleted == 0) {
                /* Hub durable position is behind this batch: keep it, retry later. */
                if (nap(a, backoff_ms(attempt++, a->cfg.backoff_max_s), NULL)) break;
                continue;
            }
        }
        attempt = 0;
        EnterCriticalSection(&a->lock);
        rate = a->limits.max_rate_kbps;
        LeaveCriticalSection(&a->lock);
        if (rate > 0) {
            uint64_t ms = ((uint64_t)body.len * 8u) / (uint64_t)rate; /* kbit/s -> ms */
            if (ms > 600000) ms = 600000;
            if (nap(a, (DWORD)ms, NULL)) break;
        }
    }
    alelt_buf_free(&hdr);
    alelt_buf_free(&body);
    alelt_http_free(h);
    return 0;
}

/* ------------------------------------------------------------------ worker: plugin */
static int plugin_load(alelt_agent *a, char *err, size_t errcap)
{
    wchar_t dir[ALELT_PATH_MAX], wname[64], path[ALELT_PATH_MAX];
    alelt_source_get_vtbl_fn fn;
    int i;
    if (alelt_exe_dir(dir, ALELT_PATH_MAX) != 0 || alelt_to_wide(a->cfg.plugin, wname, 64) != 0 ||
        alelt_path_join(path, ALELT_PATH_MAX, dir, wname) != 0) {
        snprintf(err, errcap, "plugin path invalid");
        return -1;
    }
    /* Engine client DLLs (e.g. Db2 BIN) are added to the process PATH only from
     * the DBA-controlled config; the plugin itself always comes from the exe dir. */
    for (i = 0; i < a->cfg.ndll_dirs; i++) {
        if (alelt_path_env_prepend(a->cfg.dll_dirs[i]) < 0) /* AHS-13: a long PATH is never replaced */
            alelt_logf(ALELT_LOG_WARN, "worker", "PATH could not be extended with dll_dirs[%d]", i);
    }
    a->plugin = LoadLibraryExW(path, NULL, LOAD_WITH_ALTERED_SEARCH_PATH);
    if (!a->plugin) {
        snprintf(err, errcap, "cannot load plugin %ls (error %lu)", path, (unsigned long)GetLastError());
        return -1;
    }
    fn = (alelt_source_get_vtbl_fn)(void *)GetProcAddress(a->plugin, ALELT_SOURCE_ENTRY);
    if (!fn || !(a->vt = fn()) || a->vt->abi_version < ALELT_PLUGIN_ABI_MIN || a->vt->abi_version > ALELT_PLUGIN_ABI_VERSION ||
        !a->vt->open || !a->vt->read ||
        !a->vt->compare_pos || !a->vt->pos_valid || !a->vt->close || !a->vt->probe_position) {
        snprintf(err, errcap, "plugin %s has an incompatible ABI", a->cfg.plugin);
        return -1;
    }
    if (strcmp(a->vt->engine, a->cfg.engine) != 0) {
        snprintf(err, errcap, "plugin engine '%s' does not match config engine '%s'", a->vt->engine, a->cfg.engine);
        return -1;
    }
    alelt_logf(ALELT_LOG_INFO, "worker", "plugin %s loaded (engine=%s version=%s)", a->cfg.plugin, a->vt->engine, a->vt->version);
    return 0;
}

static void source_event(alelt_agent *a, alelt_http *h, const char *msg);

static int plugin_ensure_open(alelt_agent *a, alelt_http *h)
{
    char err[512];
    if (a->pctx) return 0;
    err[0] = 0;
    if (a->vt->open(&k_host, alelt_buf_cstr(&a->cfg.settings_json), &a->pctx, err, sizeof(err)) != ALELT_OK) {
        a->pctx = NULL;
        EnterCriticalSection(&a->lock);
        a->plugin_error = 1;
        alelt_strcpy(a->open_err, sizeof(a->open_err), err[0] ? err : "source open failed");
        LeaveCriticalSection(&a->lock);
        alelt_logf(ALELT_LOG_ERROR, "worker", "source open failed: %s", err);
        source_event(a, h, err);
        return -1;
    }
    if (a->cfg.ndeny > 0) { /* AHS-08: the server the alias REALLY reached, checked before any read */
        alelt_buf out;
        int ok;
        alelt_buf_init(&out, 64 * 1024);
        ok = a->vt->probe && a->vt->probe(a->pctx, "server_info", (alelt_host_buf *)(void *)&out) == ALELT_OK && !out.failed;
        if (ok) alelt_buf_append(&out, "", 1); /* NUL for the parser */
        if (alelt_guard_check_connected(&a->cfg, ok ? (const char *)out.data : NULL, err, sizeof(err)) != 0) {
            alelt_buf_free(&out);
            a->vt->close(a->pctx);
            a->pctx = NULL;
            alelt_logf(ALELT_LOG_ERROR, "guard", "%s", err);
            alelt_eventlog(ALELT_LOG_ERROR, err);
            source_event(a, h, err);
            EnterCriticalSection(&a->lock);
            a->plugin_error = 1;
            a->exit_code = ALELT_EXIT_GUARD; /* clean stop, not restarted (same as the pre-open guard) */
            LeaveCriticalSection(&a->lock);
            SetEvent(a->stop_ev);
            return -1;
        }
        alelt_buf_free(&out);
    }
    EnterCriticalSection(&a->lock);
    a->plugin_error = 0;
    LeaveCriticalSection(&a->lock);
    alelt_logf(ALELT_LOG_INFO, "worker", "source connected");
    return 0;
}

/* REQ-17: source failures as AgentEvents the hub rules match on (SQL30082N, SQL1273N, ...). Classified from the
 * plugin message; detail = {message, sqlcode}. Throttled: one event per type per 60 s. */
static void source_event(alelt_agent *a, alelt_http *h, const char *msg)
{
    static uint64_t last_auth, last_log;
    const char *s = msg ? msg : "", *type = NULL, *q;
    uint64_t now = alelt_mono_ms(), *last = NULL;
    long code = 0;
    char esc[1024];
    size_t i, j;
    for (q = s; *q; q++) { /* first "SQLnnnn" / "sqlcode -nnnn" in the message */
        if ((q[0] == 'S' || q[0] == 's') && (q[1] == 'Q' || q[1] == 'q') && (q[2] == 'L' || q[2] == 'l') &&
            q[3] >= '0' && q[3] <= '9') {
            code = -strtol(q + 3, NULL, 10);
            break;
        }
        if (_strnicmp(q, "sqlcode", 7) == 0) {
            const char *n = q + 7;
            while (*n == ' ' || *n == '=' || *n == ':') n++;
            code = strtol(n, NULL, 10);
            break;
        }
    }
    if (code == -30082 || code == -1639 || code == -30083 || _strnicmp(s, "authentication", 14) == 0) {
        type = "DB2_AUTH_FAILED";
        last = &last_auth;
    } else if (code == -1273 || code == -2657 || code == -2650 || code == -1262 || strstr(s, "log file") ||
               strstr(s, "LOG_NOT_AVAILABLE")) {
        type = "LOG_NOT_AVAILABLE";
        last = &last_log;
    }
    if (!type || (*last && now - *last < 60000)) return;
    *last = now;
    {   /* AHS-15: built by the JSON writer; control chars, 0x7F and non-ASCII bytes blanked */
        alelt_buf b;
        alelt_jw w;
        for (i = 0, j = 0; s[i] && j + 1 < sizeof(esc); i++) {
            unsigned char ch = (unsigned char)s[i];
            esc[j++] = (ch >= 0x20 && ch < 0x7F) ? (char)ch : ' ';
        }
        esc[j] = 0;
        alelt_buf_init(&b, 4096);
        jw_init(&w, &b);
        jw_obj(&w);
        jw_key(&w, "message"); jw_str(&w, esc);
        jw_key(&w, "sqlcode"); jw_i64(&w, code);
        jw_obj_end(&w);
        if (jw_ok(&w) == 0) post_event(a, h, type, NULL, 0, NULL, alelt_buf_cstr(&b));
        alelt_buf_free(&b);
    }
}

static void plugin_reset(alelt_agent *a, const char *why)
{
    if (a->pctx) {
        alelt_logf(ALELT_LOG_WARN, "worker", "source reset: %s", why);
        a->vt->close(a->pctx);
        a->pctx = NULL;
    }
}

static const char *perr(alelt_agent *a)
{
    const char *e = (a->pctx && a->vt->last_error) ? a->vt->last_error(a->pctx) : NULL;
    return e ? e : "";
}

/* ------------------------------------------------------------------ worker: capture */
typedef struct cap_state {
    char chain_from[ALELT_POS_MAX]; /* durable: from_pos of the next batch (= previous next_pos) */
    char read_pos[ALELT_POS_MAX];   /* in memory: advances over empty READs                     */
    uint8_t *buf;
    size_t cap;
    int backpressure_logged;
    alelt_buf zbody;
} cap_state;

static int build_batch_header(alelt_agent *a, alelt_buf *b, const char *batch_id, const char *from, const char *to,
                              const char *next, uint32_t recs, size_t raw, const char *sha, const char *codec)
{
    alelt_jw w;
    jw_init(&w, b);
    jw_obj(&w); /* BatchHeader */
    jw_key(&w, "batch_id"); jw_str(&w, batch_id);
    jw_key(&w, "connection_id"); jw_str(&w, a->cfg.connection_id);
    jw_key(&w, "agent_id"); jw_str(&w, a->cfg.agent_id);
    jw_key(&w, "engine"); jw_str(&w, a->cfg.engine);
    jw_key(&w, "from_pos"); jw_pos(&w, a->cfg.engine, from);
    jw_key(&w, "to_pos"); jw_pos(&w, a->cfg.engine, to);
    jw_key(&w, "next_pos"); jw_pos(&w, a->cfg.engine, next);
    jw_key(&w, "rec_count"); jw_u64(&w, recs);
    jw_key(&w, "raw_bytes"); jw_u64(&w, raw);
    jw_key(&w, "sha256"); jw_str(&w, sha);
    jw_key(&w, "codec"); jw_str(&w, codec);
    jw_key(&w, "protocol"); jw_i64(&w, ALELT_PROTOCOL_VERSION);
    jw_obj_end(&w);
    return jw_ok(&w);
}

/* 1 = more data likely, 0 = idle (end of log / backpressure), -1 = error */
static int capture_step(alelt_agent *a, alelt_http *h, cap_state *cs)
{
    alelt_read_result res;
    char from[ALELT_POS_MAX], batch_id[ALELT_UUID_LEN + 1], sha[65];
    size_t want;
    int rc;
    const uint8_t *body;
    size_t body_len;
    const char *codec;
    alelt_buf hdr;

    if (!ha_active(a)) return 0; /* never READ without the capture lease */
    alelt_spool_snapshot(&a->spool, NULL, NULL, NULL, from, NULL);
    if (!from[0]) return 0;
    if (strcmp(from, cs->chain_from) != 0) { /* new chain: START_CAPTURE, restart, or batch spooled */
        alelt_strcpy(cs->chain_from, ALELT_POS_MAX, from);
        alelt_strcpy(cs->read_pos, ALELT_POS_MAX, from);
    }
    EnterCriticalSection(&a->lock);
    want = (size_t)a->limits.batch_kb * 1024u;
    LeaveCriticalSection(&a->lock);
    if (!alelt_spool_has_room(&a->spool, (uint64_t)want + want / 8 + 65536)) {
        if (!cs->backpressure_logged) {
            alelt_logf(ALELT_LOG_WARN, "worker", "spool full: reading paused (backpressure), nothing dropped");
            cs->backpressure_logged = 1;
            {
                char det[160];
                snprintf(det, sizeof(det), "{\"message\":\"spool full: reading paused, nothing dropped\","
                                           "\"spool_max_bytes\":%llu}", (unsigned long long)a->cfg.spool_max_bytes);
                post_event(a, h, "SPOOL_FULL", NULL, 0, NULL, det);   /* REQ-17 */
            }
        }
        return 0;
    }
    if (cs->backpressure_logged) {
        alelt_logf(ALELT_LOG_INFO, "worker", "spool has room again: reading resumed");
        cs->backpressure_logged = 0;
    }
    for (;;) {
        if (cs->cap < want) {
            uint8_t *nb = (uint8_t *)realloc(cs->buf, want);
            if (!nb) {
                alelt_logf(ALELT_LOG_ERROR, "worker", "cannot allocate %zu byte read buffer", want);
                return -1;
            }
            cs->buf = nb;
            cs->cap = want;
        }
        memset(&res, 0, sizeof(res));
        rc = a->vt->read(a->pctx, cs->read_pos, cs->buf, want, &res);
        if (rc == ALELT_ERR_NOSPACE && want < READ_BUF_MAX) {
            want *= 2; /* one record larger than the buffer: grow temporarily */
            continue;
        }
        break;
    }
    if (rc == ALELT_ERR_RETRY) {
        source_event(a, h, perr(a));
        plugin_reset(a, perr(a));
        return -1;
    }
    if (rc != ALELT_OK) {
        source_event(a, h, perr(a));
        alelt_log_ev(ALELT_LOG_ERROR, "worker", perr(a)[0] ? perr(a) : "read failed", NULL, from, NULL, NULL);
        count_error(a);
        return -1;
    }
    if (a->vt->abi_version >= 2 && (res.flags & ALELT_READ_LOG_REUSED)) {
        /* SRC-db2-11: positions after a restore / rollforward are ambiguous: tell the hub */
        char det[256];
        snprintf(det, sizeof(det), "{\"from_pos\":\"%s\",\"next_pos\":\"%s\",\"reason\":\"log positions reused "
                 "(restore or rollforward): verify the resume position\"}", cs->read_pos, res.next_pos);
        alelt_logf(ALELT_LOG_WARN, "worker", "source reports reused log positions at %s", cs->read_pos);
        post_event(a, h, "LOG_SEQUENCE_REUSED", NULL, 0, NULL, det);
    }
    if (res.bytes > want || (res.records == 0) != (res.bytes == 0) || !a->vt->pos_valid(res.next_pos)) {
        alelt_log_ev(ALELT_LOG_ERROR, "worker", "plugin returned an inconsistent read result", NULL, from, NULL, NULL);
        count_error(a);
        return -1;
    }
    if (res.records == 0) {
        /* Empty READ: no batch, from_pos of the next batch stays at chain_from (X-25);
         * only the in-memory read cursor skips the non-propagatable stretch. */
        alelt_strcpy(cs->read_pos, ALELT_POS_MAX, res.next_pos);
        return 0;
    }
    if (alelt_uuid4(batch_id) != 0 || alelt_sha256_hex(cs->buf, res.bytes, sha) != 0) return -1;
    alelt_buf_reset(&cs->zbody);
    if (alelt_zlib_compress(cs->buf, res.bytes, &cs->zbody) == 0 && cs->zbody.len < res.bytes) {
        body = cs->zbody.data;
        body_len = cs->zbody.len;
        codec = "zlib";
    } else {
        body = cs->buf;
        body_len = res.bytes;
        codec = "none";
    }
    alelt_buf_init(&hdr, 64 * 1024);
    EnterCriticalSection(&a->put_lock);
    if (!ha_active(a)) {
        /* lease lost during the READ: nothing is spooled, the chain does not advance */
        LeaveCriticalSection(&a->put_lock);
        alelt_buf_free(&hdr);
        return 0;
    }
    if (build_batch_header(a, &hdr, batch_id, cs->chain_from, res.last_pos, res.next_pos,
                           res.records, res.bytes, sha, codec) != 0 ||
        alelt_spool_put(&a->spool, batch_id, alelt_buf_cstr(&hdr), body, body_len, res.last_pos, res.next_pos) != 0) {
        LeaveCriticalSection(&a->put_lock);
        alelt_buf_free(&hdr);
        alelt_log_ev(ALELT_LOG_ERROR, "worker", "cannot spool batch; reading paused", batch_id, from, NULL, NULL);
        count_error(a);
        return -1;
    }
    LeaveCriticalSection(&a->put_lock);
    alelt_buf_free(&hdr);
    alelt_strcpy(cs->chain_from, ALELT_POS_MAX, res.next_pos);
    alelt_strcpy(cs->read_pos, ALELT_POS_MAX, res.next_pos);
    EnterCriticalSection(&a->lock);
    alelt_strcpy(a->last_read_pos, ALELT_POS_MAX, res.last_pos);
    LeaveCriticalSection(&a->lock);
    alelt_log_ev(ALELT_LOG_DEBUG, "worker", "batch spooled", batch_id, res.last_pos, NULL, NULL);
    SetEvent(a->send_ev);
    return res.at_end ? 0 : 1;
}

/* ------------------------------------------------------------------ worker: commands */
static void do_start_capture(alelt_agent *a, alelt_http *h, const alelt_cmd *c)
{
    char pos[ALELT_POS_MAX] = "";
    char cur[ALELT_POS_MAX] = "";
    if (c->has_from_pos) {
        if (!a->vt->pos_valid(c->from_pos)) {
            post_event(a, h, "COMMAND_REJECTED", c->command_id, 0, "from_pos is not a valid position", NULL);
            return;
        }
        {
            size_t pending = 0;
            alelt_spool_snapshot(&a->spool, NULL, &pending, NULL, NULL, NULL);
            if (pending > 0 && !c->discard_spool) {
                post_event(a, h, "COMMAND_REJECTED", c->command_id, 0,
                           "spool not empty; cannot reposition until the hub acknowledged it "
                           "(operator recovery: START_CAPTURE{from_pos, discard_spool:true})", NULL);
                return;
            }
            if (pending > 0) {
                /* AHS-05: audited operator recovery of a spool blocked by 409. No batch in flight
                 * (send_lock) and none being spooled (put_lock) while the spool is reset. */
                int dropped;
                EnterCriticalSection(&a->send_lock);
                EnterCriticalSection(&a->put_lock);
                dropped = alelt_spool_takeover(&a->spool, c->from_pos, 1);
                LeaveCriticalSection(&a->put_lock);
                LeaveCriticalSection(&a->send_lock);
                if (dropped < 0) {
                    post_event(a, h, "COMMAND_RESULT", c->command_id, 0, "discard_spool: spool reset failed", NULL);
                    return;
                }
                alelt_logf(ALELT_LOG_WARN, "worker", "START_CAPTURE %s: %d unacknowledged batch(es) DISCARDED by operator "
                        "command (discard_spool); capture restarts at %s", c->command_id, dropped, c->from_pos);
                {
                    char det[ALELT_POS_MAX + 160];
                    snprintf(det, sizeof(det), "{\"from_pos\":\"%s\",\"discarded_batches\":%d,\"command_id\":\"%s\"}",
                             c->from_pos, dropped, c->command_id);
                    post_event(a, h, "SPOOL_DISCARDED", NULL, 1, NULL, det);
                }
            }
        }
        alelt_strcpy(pos, ALELT_POS_MAX, c->from_pos);
        alelt_spool_set_reposition(&a->spool, c->command_id, pos); /* X-ALELT-Reposition for the first batch */
    } else {
        alelt_spool_snapshot(&a->spool, NULL, NULL, NULL, pos, NULL);
        if (!pos[0]) {
            EnterCriticalSection(&a->lock);
            alelt_strcpy(pos, ALELT_POS_MAX, a->resume_from);
            LeaveCriticalSection(&a->lock);
        }
        if (!pos[0]) {
            if (a->vt->probe_position(a->pctx, cur, sizeof(cur)) != ALELT_OK) {
                post_event(a, h, "COMMAND_RESULT", c->command_id, 0, perr(a), NULL);
                return;
            }
            alelt_strcpy(pos, ALELT_POS_MAX, cur);
        }
    }
    alelt_spool_set_capturing(&a->spool, 1, pos);
    EnterCriticalSection(&a->lock);
    a->user_paused = 0;
    LeaveCriticalSection(&a->lock);
    alelt_log_ev(ALELT_LOG_INFO, "worker", "capture started", NULL, pos, NULL, c->command_id);
    {
        char rj[ALELT_POS_MAX + 32];
        snprintf(rj, sizeof(rj), "{\"from_pos\":\"%s\"}", pos); /* pos is validated printable ASCII */
        post_event(a, h, "COMMAND_RESULT", c->command_id, 1, NULL, rj);
    }
}

static void do_catalog(alelt_agent *a, alelt_http *h, const alelt_cmd *c)
{
    alelt_buf inner, out;
    alelt_jw w;
    const char *schemas[ALELT_MAX_SCHEMAS];
    char ts[ALELT_TS_LEN], err[512];
    fj_doc *d;
    int i, rc;
    for (i = 0; i < c->nschemas; i++) schemas[i] = c->schemas[i];
    alelt_buf_init(&inner, (size_t)64 * 1024 * 1024);
    alelt_buf_init(&out, (size_t)64 * 1024 * 1024);
    alelt_now_iso(ts);
    rc = a->vt->catalog_json ? a->vt->catalog_json(a->pctx, schemas, (size_t)c->nschemas, (alelt_host_buf *)(void *)&inner)
                             : ALELT_ERR;
    if (rc != ALELT_OK || inner.failed) {
        if (rc == ALELT_ERR_RETRY) plugin_reset(a, perr(a));
        post_event(a, h, "COMMAND_RESULT", c->command_id, 0, perr(a)[0] ? perr(a) : "catalog failed", NULL);
        goto done;
    }
    d = fj_parse((const char *)inner.data, inner.len, err, sizeof(err));
    if (!d || fj_root(d)->type != FJ_OBJ || fj_root(d)->n == 0) {
        fj_free(d);
        post_event(a, h, "COMMAND_RESULT", c->command_id, 0, "plugin catalog JSON invalid", NULL);
        goto done;
    }
    fj_free(d);
    /* CatalogSnapshot = envelope + plugin members (inner without its opening brace). */
    jw_init(&w, &out);
    alelt_buf_putc(&out, '{');
    alelt_buf_puts(&out, "\"connection_id\":");
    fj_escape(&out, a->cfg.connection_id, strlen(a->cfg.connection_id));
    alelt_buf_puts(&out, ",\"engine\":");
    fj_escape(&out, a->cfg.engine, strlen(a->cfg.engine));
    alelt_buf_puts(&out, ",\"taken_at\":");
    fj_escape(&out, ts, strlen(ts));
    alelt_buf_putc(&out, ',');
    {
        size_t k = 0;
        while (k < inner.len && inner.data[k] != '{') k++;
        alelt_buf_append(&out, inner.data + k + 1, inner.len - k - 1);
    }
    (void)w;
    if (out.failed) {
        post_event(a, h, "COMMAND_RESULT", c->command_id, 0, "catalog too large", NULL);
        goto done;
    }
    for (i = 0; !stopping(a); i++) {
        rc = alelt_hub_post_json(h, "/agent/v1/catalog", (const char *)out.data, out.len, a->cfg.request_timeout_ms, NULL,
                              err, sizeof(err));
        if (rc == 0) {
            alelt_logf(ALELT_LOG_INFO, "worker", "catalog snapshot delivered (%zu bytes)", out.len);
            post_event(a, h, "COMMAND_RESULT", c->command_id, 1, "catalog delivered", NULL);
            break;
        }
        alelt_logf(ALELT_LOG_WARN, "worker", "catalog post failed: %s", err);
        if (rc == -2 || i >= 8) {
            post_event(a, h, "COMMAND_RESULT", c->command_id, 0, err, NULL);
            break;
        }
        if (nap(a, backoff_ms(i, a->cfg.backoff_max_s), NULL)) break;
    }
done:
    alelt_buf_free(&inner);
    alelt_buf_free(&out);
}

static void jw_keyarr(alelt_jw *w, char (*k)[ALELT_KEYVAL_MAX], size_t n)
{
    size_t i;
    if (n == 0) {
        jw_null(w);
        return;
    }
    jw_arr(w);
    for (i = 0; i < n; i++) jw_str(w, k[i]);
    jw_arr_end(w);
}

static double probe_log_util(alelt_agent *a);

/* Governor evaluation (worker thread). AHS-06: also called inside the snapshot / verify
 * loops so a long command can be paused by the governor, and released again. */
static void gov_tick(alelt_agent *a, alelt_http *h)
{
    double cpu, lu;
    int t;
    if (alelt_mono_ms() < a->next_gov) return;
    cpu = alelt_host_cpu_pct();
    lu = a->pctx ? probe_log_util(a) : -1.0;
    t = alelt_gov_step(&a->gov, &a->gc, cpu, lu);
    a->next_gov = alelt_mono_ms() + (uint64_t)a->cfg.gov_interval_s * 1000;
    if (t != 0) {
        EnterCriticalSection(&a->lock);
        a->gov_paused = a->gov.paused;
        LeaveCriticalSection(&a->lock);
        if (t > 0) {
            alelt_logf(ALELT_LOG_WARN, "governor", "PAUSED_BY_GOVERNOR: %s", a->gov.reason);
            post_event(a, h, "PAUSED_BY_GOVERNOR", NULL, 1, a->gov.reason, NULL);
        } else {
            alelt_logf(ALELT_LOG_INFO, "governor", "governor released pause");
            post_event(a, h, "GOVERNOR_RESUMED", NULL, 1, NULL, NULL);
        }
    }
}

/* AHS-06: keep the log reader going between snapshot chunks / before a verify, so a long
 * command never starves capture. Bounded by budget_ms; honours pause, lease and backpressure. */
static void capture_interleave(alelt_agent *a, alelt_http *h, uint64_t budget_ms)
{
    uint64_t t0 = alelt_mono_ms();
    int capturing = 0, paused;
    if (!a->cs) return;
    while (!stopping(a) && alelt_mono_ms() - t0 < budget_ms) {
        alelt_spool_snapshot(&a->spool, NULL, NULL, NULL, NULL, &capturing);
        EnterCriticalSection(&a->lock);
        paused = a->user_paused || a->gov_paused;
        LeaveCriticalSection(&a->lock);
        if (!capturing || paused || !ha_active(a) || !a->pctx) return;
        if (capture_step(a, h, a->cs) <= 0) return; /* idle, backpressure or error: back to the command */
    }
}

static int gov_or_user_paused(alelt_agent *a)
{
    int p;
    EnterCriticalSection(&a->lock);
    p = a->gov_paused || a->user_paused;
    LeaveCriticalSection(&a->lock);
    return p;
}

static void do_snapshot(alelt_agent *a, alelt_http *h, const alelt_cmd *c)
{
    char sid[ALELT_UUID_LEN + 1], start_pos[ALELT_POS_MAX] = "", err[512], fq[2 * ALELT_IDENT_MAX + 1];
    char (*after)[ALELT_KEYVAL_MAX];
    const char *after_p[ALELT_MAX_KEYCOLS], *to_p[ALELT_MAX_KEYCOLS];
    size_t nafter, i;
    uint32_t chunk_rows;
    int chunk_no = c->chunk_from, rc;
    alelt_snapshot_res *res;
    alelt_buf rows, z, hdr;

    snprintf(fq, sizeof(fq), "%s.%s", c->tab_schema, c->tab_name);
    if (!alelt_config_table_allowed(&a->cfg, c->tab_schema, c->tab_name)) {
        alelt_log_ev(ALELT_LOG_WARN, "snapshot", "table not in local snapshot.allowed_tables; refused", NULL, NULL, fq, c->command_id);
        post_fail(a, h, "COMMAND_REJECTED", c->command_id, "NOT_ALLOWED", "table not in snapshot.allowed_tables");
        return;
    }
    if (!a->vt->snapshot_chunk) {
        post_fail(a, h, "COMMAND_REJECTED", c->command_id, "UNSUPPORTED", "plugin has no snapshot support");
        return;
    }
    if (c->snapshot_id[0]) alelt_strcpy(sid, sizeof(sid), c->snapshot_id);
    else if (alelt_uuid4(sid) != 0) return;
    chunk_rows = (uint32_t)(c->chunk_rows > 0 ? c->chunk_rows : a->cfg.snapshot_max_chunk_rows);
    if (chunk_rows > (uint32_t)a->cfg.snapshot_max_chunk_rows) chunk_rows = (uint32_t)a->cfg.snapshot_max_chunk_rows;
    if (a->vt->probe_position(a->pctx, start_pos, sizeof(start_pos)) != ALELT_OK || !start_pos[0]) {
        /* AHS-14: without a start position the hub cannot place the overlap window: refuse */
        const char *e = perr(a)[0] ? perr(a) : "cannot read the current log position";
        alelt_log_ev(ALELT_LOG_ERROR, "snapshot", e, NULL, NULL, fq, c->command_id);
        post_fail(a, h, "COMMAND_RESULT", c->command_id, "SOURCE_ERROR", e);
        return;
    }

    res = (alelt_snapshot_res *)calloc(1, sizeof(*res));
    after = calloc(ALELT_MAX_KEYCOLS, ALELT_KEYVAL_MAX);
    if (!res || !after) {
        free(res);
        free(after);
        return;
    }
    nafter = (size_t)c->nkey_from;
    for (i = 0; i < nafter; i++) alelt_strcpy(after[i], ALELT_KEYVAL_MAX, c->key_from[i]);
    for (i = 0; i < (size_t)c->nkey_to; i++) to_p[i] = c->key_to[i];
    alelt_buf_init(&rows, (size_t)128 * 1024 * 1024);
    alelt_buf_init(&z, (size_t)130 * 1024 * 1024);
    alelt_buf_init(&hdr, 128 * 1024);
    alelt_log_ev(ALELT_LOG_INFO, "snapshot", "snapshot started", sid, start_pos, fq, c->command_id);

    for (;;) {
        alelt_snapshot_req req;
        alelt_jw w;
        int attempt = 0;
        for (;;) { /* AHS-06: the governor is re-evaluated while the snapshot waits */
            gov_tick(a, h);
            if (!gov_or_user_paused(a) || stopping(a)) break;
            (void)nap(a, 1000, NULL);
        }
        if (stopping(a)) break;
        for (i = 0; i < nafter; i++) after_p[i] = after[i];
        memset(&req, 0, sizeof(req));
        req.schema = c->tab_schema;
        req.table = c->tab_name;
        req.key_after = nafter ? after_p : NULL;
        req.nkey_after = nafter;
        req.key_to = c->nkey_to ? to_p : NULL;
        req.nkey_to = (size_t)c->nkey_to;
        req.chunk_rows = chunk_rows;
        alelt_buf_reset(&rows);
        memset(res, 0, sizeof(*res));
        rc = a->vt->snapshot_chunk(a->pctx, &req, (alelt_host_buf *)(void *)&rows, res);
        if (rc == ALELT_OK && res->nlast_key > ALELT_MAX_KEYCOLS) { /* AHS-12: validate before any use */
            post_event(a, h, "COMMAND_RESULT", c->command_id, 0, "plugin returned an invalid chunk key", NULL);
            break;
        }
        if (rc != ALELT_OK || rows.failed) {
            const char *e = perr(a)[0] ? perr(a) : "snapshot chunk failed";
            alelt_log_ev(ALELT_LOG_ERROR, "snapshot", e, sid, NULL, fq, c->command_id);
            post_fail(a, h, "COMMAND_RESULT", c->command_id, rc == ALELT_ERR_DENIED ? "NOT_ALLOWED" : "SOURCE_ERROR", e);
            if (rc == ALELT_ERR_RETRY) plugin_reset(a, e);
            break;
        }
        alelt_buf_reset(&z);
        if (alelt_zlib_compress(rows.data ? rows.data : (const uint8_t *)"", rows.len, &z) != 0) break;
        alelt_buf_reset(&hdr);
        jw_init(&w, &hdr);
        jw_obj(&w); /* SnapshotChunkHeader */
        jw_key(&w, "snapshot_id"); jw_str(&w, sid);
        jw_key(&w, "table");
        jw_obj(&w);
        jw_key(&w, "schema"); jw_str(&w, c->tab_schema);
        jw_key(&w, "name"); jw_str(&w, c->tab_name);
        jw_obj_end(&w);
        jw_key(&w, "chunk_no"); jw_i64(&w, chunk_no);
        jw_key(&w, "key_from"); jw_keyarr(&w, after, nafter);
        jw_key(&w, "key_to"); jw_keyarr(&w, res->last_key, res->nlast_key);
        jw_key(&w, "last_key"); jw_keyarr(&w, res->last_key, res->nlast_key); /* resume point (key_from) */
        jw_key(&w, "rows"); jw_u64(&w, res->rows);
        jw_key(&w, "last"); jw_bool(&w, res->last);
        jw_key(&w, "start_pos"); jw_pos(&w, a->cfg.engine, start_pos);
        jw_key(&w, "codec"); jw_str(&w, "zlib");
        jw_key(&w, "format"); jw_str(&w, "jsonl");
        jw_obj_end(&w);
        if (jw_ok(&w) != 0) break;
        for (;;) {
            rc = alelt_hub_post_snapshot(h, alelt_buf_cstr(&hdr), z.data, z.len, a->cfg.request_timeout_ms, err, sizeof(err));
            if (rc == 0 || rc == -2 || stopping(a) || attempt >= 10) break; /* bounded: the hub re-issues */
            alelt_logf(ALELT_LOG_WARN, "snapshot", "chunk post failed: %s", err);
            if (nap(a, backoff_ms(attempt++, a->cfg.backoff_max_s), NULL)) break;
        }
        if (rc != 0) {
            alelt_log_ev(ALELT_LOG_ERROR, "snapshot", err, sid, NULL, fq, c->command_id);
            post_event(a, h, "COMMAND_RESULT", c->command_id, 0, err, NULL);
            break;
        }
        chunk_no++;
        gov_tick(a, h);
        capture_interleave(a, h, 5000); /* AHS-06: capture is not starved by a long snapshot */
        if (res->last || res->rows == 0) {
            alelt_log_ev(ALELT_LOG_INFO, "snapshot", "snapshot complete", sid, start_pos, fq, c->command_id);
            post_event(a, h, "COMMAND_RESULT", c->command_id, 1, sid, NULL);
            break;
        }
        if (res->nlast_key == 0 || res->nlast_key > ALELT_MAX_KEYCOLS) {
            post_event(a, h, "COMMAND_RESULT", c->command_id, 0, "plugin returned no key for next chunk", NULL);
            break;
        }
        nafter = res->nlast_key;
        for (i = 0; i < nafter; i++) alelt_strcpy(after[i], ALELT_KEYVAL_MAX, res->last_key[i]);
    }
    alelt_buf_free(&rows);
    alelt_buf_free(&z);
    alelt_buf_free(&hdr);
    free(res);
    free(after);
}

static void do_probe(alelt_agent *a, alelt_http *h, const alelt_cmd *c)
{
    alelt_buf out;
    char err[256];
    fj_doc *d;
    alelt_buf_init(&out, 1024 * 1024);
    if (!a->vt->probe || a->vt->probe(a->pctx, c->probe, (alelt_host_buf *)(void *)&out) != ALELT_OK || out.failed) {
        post_event(a, h, "PROBE_RESULT", c->command_id, 0, perr(a)[0] ? perr(a) : "probe failed", NULL);
    } else if (!(d = fj_parse((const char *)out.data, out.len, err, sizeof(err)))) {
        post_event(a, h, "PROBE_RESULT", c->command_id, 0, "plugin probe JSON invalid", NULL);
    } else {
        fj_free(d);
        post_event(a, h, "PROBE_RESULT", c->command_id, 1, NULL, (const char *)out.data);
    }
    alelt_buf_free(&out);
}

/* VERIFY_TABLE (F-03): bounded, read-only verification reads on an allow-listed
 * table; the result (VerifyTableResult) is the command ack. Governor-aware: waits
 * while paused (gov or user) up to VERIFY_PAUSE_WAIT_MS, then refuses so the hub
 * retries later. Rate: one verify command at a time on the worker thread, at
 * least VERIFY_MIN_GAP_MS apart. Off-peak window: enforced by the hub scheduler. */
#define VERIFY_PAUSE_WAIT_MS 300000u
#define VERIFY_MIN_GAP_MS 1000u
#define VERIFY_MAX_RESULT ((size_t)8 * 1024 * 1024)
static void do_verify(alelt_agent *a, alelt_http *h, const alelt_cmd *c)
{
    static uint64_t last_end = 0;
    char fq[2 * ALELT_IDENT_MAX + 1], err[256];
    alelt_verify_req req;
    alelt_buf out;
    fj_doc *d;
    uint64_t t0;
    int rc;

    snprintf(fq, sizeof(fq), "%s.%s", c->tab_schema, c->tab_name);
    if (!alelt_config_table_allowed(&a->cfg, c->tab_schema, c->tab_name)) {
        alelt_log_ev(ALELT_LOG_WARN, "verify", "table not in local snapshot.allowed_tables; refused", NULL, NULL, fq, c->command_id);
        post_fail(a, h, "COMMAND_REJECTED", c->command_id, "NOT_ALLOWED", "table not in snapshot.allowed_tables");
        return;
    }
    if (a->vt->abi_version < 2 || !a->vt->verify_table) {
        post_fail(a, h, "COMMAND_REJECTED", c->command_id, "UNSUPPORTED", "plugin has no VERIFY_TABLE support");
        return;
    }
    t0 = alelt_mono_ms();
    while (!stopping(a) && alelt_mono_ms() - t0 < VERIFY_PAUSE_WAIT_MS) { /* AHS-06: governor re-evaluated */
        gov_tick(a, h);
        if (!gov_or_user_paused(a)) break;
        (void)nap(a, 1000, NULL);
    }
    capture_interleave(a, h, 10000); /* catch the log reader up before a long read-only verify */
    if (stopping(a)) return;
    if (gov_or_user_paused(a)) {
        post_fail(a, h, "COMMAND_RESULT", c->command_id, "PAUSED", "paused (governor or operator): verify refused, retry later");
        return;
    }
    if (last_end && alelt_mono_ms() - last_end < VERIFY_MIN_GAP_MS) (void)nap(a, VERIFY_MIN_GAP_MS, NULL);
    memset(&req, 0, sizeof(req));
    req.schema = c->tab_schema;
    req.table = c->tab_name;
    req.args_json = c->verify_args;
    req.args_len = c->verify_len;
    req.max_keys = ALELT_VERIFY_MAX_KEYS;
    req.max_key_values = ALELT_VERIFY_MAX_KEY_VALUES;
    req.max_bytes = VERIFY_MAX_RESULT;
    req.timeout_s = ALELT_VERIFY_MAX_TIMEOUT_S;
    alelt_buf_init(&out, VERIFY_MAX_RESULT + 1024 * 1024);
    alelt_log_ev(ALELT_LOG_INFO, "verify", "verify started", c->verify_id, NULL, fq, c->command_id);
    rc = a->vt->verify_table(a->pctx, &req, (alelt_host_buf *)(void *)&out);
    last_end = alelt_mono_ms();
    if (rc != ALELT_OK || out.failed) {
        const char *e = perr(a)[0] ? perr(a) : "verify failed";
        alelt_log_ev(ALELT_LOG_ERROR, "verify", e, c->verify_id, NULL, fq, c->command_id);
        post_fail(a, h, rc == ALELT_ERR_DENIED ? "COMMAND_REJECTED" : "COMMAND_RESULT", c->command_id,
                  rc == ALELT_ERR_DENIED ? "NOT_ALLOWED" : "SOURCE_ERROR", e);
        if (rc == ALELT_ERR_RETRY) plugin_reset(a, e);
    } else if (!(d = fj_parse((const char *)out.data, out.len, err, sizeof(err)))) {
        post_event(a, h, "COMMAND_RESULT", c->command_id, 0, "plugin verify JSON invalid", NULL);
    } else {
        fj_free(d);
        alelt_log_ev(ALELT_LOG_INFO, "verify", "verify complete", c->verify_id, NULL, fq, c->command_id);
        post_event(a, h, "COMMAND_RESULT", c->command_id, 1, NULL, (const char *)out.data);
    }
    alelt_buf_free(&out);
}

static double probe_log_util(alelt_agent *a)
{
    alelt_buf out;
    char err[128];
    double v = -1.0;
    fj_doc *d;
    if (!a->vt->probe) return -1.0;
    alelt_buf_init(&out, 256 * 1024);
    if (a->vt->probe(a->pctx, "db_health", (alelt_host_buf *)(void *)&out) == ALELT_OK && !out.failed) {
        d = fj_parse((const char *)out.data, out.len, err, sizeof(err));
        if (d) {
            if (fj_as_dbl(fj_get(fj_root(d), "log_utilization_pct"), &v) != 0) v = -1.0;
            fj_free(d);
        }
    }
    alelt_buf_free(&out);
    return v;
}

static DWORD WINAPI worker_thread(LPVOID p)
{
    alelt_agent *a = (alelt_agent *)p;
    alelt_http *h;
    char err[512];
    int attempt = 0;
    cap_state cs;
    alelt_cmd *cmd;

    memset(&cs, 0, sizeof(cs));
    alelt_buf_init(&cs.zbody, READ_BUF_MAX + READ_BUF_MAX / 8 + 1024);
    a->cs = &cs;
    alelt_gov_init(&a->gov);
    a->next_gov = 0;
    a->gc.enabled = a->cfg.gov_enabled;
    a->gc.host_cpu_pause_pct = a->cfg.gov_host_cpu_pause_pct;
    a->gc.host_cpu_resume_pct = a->cfg.gov_host_cpu_resume_pct;
    a->gc.sustain = a->cfg.gov_sustain;
    a->gc.max_log_util_pct = a->cfg.gov_max_log_util_pct;
    h = alelt_http_new(&a->cfg, err, sizeof(err));
    if (!h) alelt_logf(ALELT_LOG_ERROR, "worker", "hub link init failed: %s", err);
    (void)alelt_host_cpu_pct(); /* prime */

    cmd = (alelt_cmd *)calloc(1, sizeof(*cmd));
    if (!cmd) {
        alelt_logf(ALELT_LOG_ERROR, "worker", "out of memory");
        SetEvent(a->stop_ev);
        alelt_http_free(h);
        alelt_buf_free(&cs.zbody);
        return 1;
    }
    while (!stopping(a)) {
        int have = 0, capturing = 0, paused, r;
        DWORD poll_ms;

        if (!ha_active(a)) {
            /* STANDBY: no source connection (an HADR standby is not connectable), no READ;
             * plugin commands are refused - only the lease holder works on the source. */
            EnterCriticalSection(&a->lock);
            if (a->qn > 0 && a->registered) {
                *cmd = a->q[a->qhead];
                a->qhead = (a->qhead + 1) % QCAP;
                a->qn--;
                have = 1;
            }
            LeaveCriticalSection(&a->lock);
            if (have) {
                post_event(a, h, "COMMAND_REJECTED", cmd->command_id, 0,
                           "standby: this agent does not hold the capture lease (ha.mode=active_passive)", NULL);
                continue;
            }
            if (nap(a, 1000, a->cmd_ev)) break;
            continue;
        }
        if (plugin_ensure_open(a, h) != 0) {
            /* Source down (alias missing, database stopped, auth): queued source commands are answered
             * ok:false with the open error instead of waiting forever - the hub retries / diagnoses. */
            char oe[512];
            for (;;) {
                EnterCriticalSection(&a->lock);
                have = 0;
                if (a->qn > 0 && a->registered) {
                    *cmd = a->q[a->qhead];
                    a->qhead = (a->qhead + 1) % QCAP;
                    a->qn--;
                    have = 1;
                }
                alelt_strcpy(oe, sizeof(oe), a->open_err);
                LeaveCriticalSection(&a->lock);
                if (!have || stopping(a)) break;
                {
                    char msg[600];
                    snprintf(msg, sizeof(msg), "source unavailable: %s", oe);
                    post_fail(a, h, cmd->name == ALELT_CMD_PROBE ? "PROBE_RESULT" : "COMMAND_RESULT", cmd->command_id,
                              "SOURCE_ERROR", msg);
                }
            }
            if (nap(a, backoff_ms(attempt++, 60), a->cmd_ev)) break;
            continue;
        }
        attempt = 0;

        EnterCriticalSection(&a->lock);
        if (a->qn > 0 && a->registered) {
            *cmd = a->q[a->qhead];
            a->qhead = (a->qhead + 1) % QCAP;
            a->qn--;
            have = 1;
        }
        LeaveCriticalSection(&a->lock);
        if (have) {
            switch (cmd->name) {
            case ALELT_CMD_START_CAPTURE: do_start_capture(a, h, cmd); break;
            case ALELT_CMD_SNAPSHOT_CATALOG: do_catalog(a, h, cmd); break;
            case ALELT_CMD_SNAPSHOT_TABLE: do_snapshot(a, h, cmd); break;
            case ALELT_CMD_PROBE: do_probe(a, h, cmd); break;
            case ALELT_CMD_VERIFY_TABLE: do_verify(a, h, cmd); break;
            default: break;
            }
            continue;
        }

        gov_tick(a, h);

        alelt_spool_snapshot(&a->spool, NULL, NULL, NULL, NULL, &capturing);
        EnterCriticalSection(&a->lock);
        paused = a->user_paused || a->gov_paused;
        poll_ms = (DWORD)a->limits.poll_ms;
        LeaveCriticalSection(&a->lock);
        if (!capturing || paused) {
            if (nap(a, 1000, a->cmd_ev)) break;
            continue;
        }
        r = capture_step(a, h, &cs);
        if (r > 0) continue;
        if (r < 0) {
            if (nap(a, backoff_ms(attempt++, 60), a->cmd_ev)) break;
            continue;
        }
        if (nap(a, poll_ms, a->cmd_ev)) break;
    }
    if (a->pctx) {
        a->vt->close(a->pctx);
        a->pctx = NULL;
    }
    free(cmd);
    a->cs = NULL;
    free(cs.buf);
    alelt_buf_free(&cs.zbody);
    alelt_http_free(h);
    return 0;
}

/* ------------------------------------------------------------------ ha thread (active/passive) */
static void ha_apply_grant(alelt_agent *a, const alelt_lease_grant *g, uint64_t sent_ms)
{
    alelt_ha_event ev;
    int purged = 0;
    if (g->granted && g->takeover) EnterCriticalSection(&a->send_lock); /* no post of a stale batch */
    EnterCriticalSection(&a->put_lock);
    EnterCriticalSection(&a->lock);
    ev = alelt_ha_on_grant_at(&a->ha, g, sent_ms, alelt_mono_ms()); /* AHS-07: fence clock from the send time */
    if (ev == ALELT_HA_TAKEOVER) a->ha.active = 0; /* inactive until the spool is reset */
    LeaveCriticalSection(&a->lock);
    if (ev == ALELT_HA_TAKEOVER) {
        /* Another agent held the lease: local batches overlap what it shipped. The hub
         * durable position is the truth (X-48); re-read the log from there. */
        purged = alelt_spool_takeover(&a->spool, g->resume_from, 1);
        EnterCriticalSection(&a->lock);
        a->ha.active = purged >= 0;
        /* AHS-24: the hub says whether capture was running for this connection. A connection the operator
         * paused stays paused on the new holder (RESUME / START_CAPTURE continue it). Absent = running. */
        if (alelt_ha_grant_paused(g)) a->user_paused = 1;
        LeaveCriticalSection(&a->lock);
    }
    LeaveCriticalSection(&a->put_lock);
    if (g->granted && g->takeover) LeaveCriticalSection(&a->send_lock);
    switch (ev) {
    case ALELT_HA_TAKEOVER:
        if (purged < 0) {
            alelt_logf(ALELT_LOG_ERROR, "ha", "lease granted (takeover) but the local spool could not be reset; staying STANDBY");
            ha_lost(a, "spool reset failed");
            break;
        }
        alelt_logf(ALELT_LOG_WARN, "ha",
                "capture lease acquired by TAKEOVER (epoch %lld): %d stale batch(es) discarded, capture resumes at %s",
                g->epoch, purged,
                g->capturing == 0 ? "- (paused by the operator: waiting for RESUME / START_CAPTURE)"
                : g->resume_from[0] ? g->resume_from : "- (no hub position: waiting for START_CAPTURE)");
        SetEvent(a->cmd_ev);
        SetEvent(a->send_ev);
        break;
    case ALELT_HA_ACTIVE:
        alelt_logf(ALELT_LOG_INFO, "ha", "capture lease held (epoch %lld): ACTIVE, local spool kept", g->epoch);
        SetEvent(a->cmd_ev);
        SetEvent(a->send_ev);
        break;
    case ALELT_HA_LOST:
        alelt_logf(ALELT_LOG_WARN, "ha", "capture lease refused (holder %s): reading stopped, now STANDBY",
                g->holder[0] ? g->holder : "?");
        break;
    default:
        break;
    }
}

static DWORD WINAPI ha_thread(LPVOID p)
{
    alelt_agent *a = (alelt_agent *)p;
    alelt_http *h;
    char err[512];
    int standby_logged = 0;
    h = alelt_http_new(&a->cfg, err, sizeof(err));
    if (!h) {
        alelt_logf(ALELT_LOG_ERROR, "ha", "hub link init failed: %s", err);
        a->exit_code = 10;
        SetEvent(a->stop_ev);
        return 1;
    }
    alelt_logf(ALELT_LOG_INFO, "ha", "active/passive mode: lease_ttl_s=%d renew_s=%d; STANDBY until the hub grants the lease",
            a->cfg.ha_lease_ttl_s, a->cfg.ha_renew_s);
    while (!stopping(a)) {
        uint64_t now = alelt_mono_ms(), wait;
        int fence, due, active, ttl, rc;
        char lease_id[37];
        long long epoch;
        alelt_lease_grant g;
        if (!a->registered) {
            if (nap(a, 500, NULL)) break;
            continue;
        }
        EnterCriticalSection(&a->lock);
        fence = alelt_ha_must_fence(&a->ha, now);
        due = alelt_ha_due(&a->ha, now);
        active = a->ha.active;
        ttl = alelt_ha_ttl(&a->ha);
        alelt_strcpy(lease_id, sizeof(lease_id), a->ha.lease_id);
        epoch = a->ha.epoch;
        LeaveCriticalSection(&a->lock);
        if (fence) {
            ha_lost(a, "no successful renew within lease_ttl_s - renew_s (self-fencing)");
            continue;
        }
        if (due) {
            uint64_t sent = alelt_mono_ms(); /* AHS-07: stamped BEFORE the request */
            rc = alelt_hub_lease(h, active ? "renew" : "acquire", &a->cfg, ttl, active ? lease_id : NULL,
                              active ? epoch : -1, &g, err, sizeof(err));
            if (rc == 0) {
                ha_apply_grant(a, &g, sent);
                if (!g.granted && !standby_logged) {
                    alelt_logf(ALELT_LOG_INFO, "ha", "STANDBY: lease held by %s (epoch %lld)", g.holder[0] ? g.holder : "?",
                            g.epoch);
                    standby_logged = 1;
                }
                if (g.granted) standby_logged = 0;
            } else if (rc == -3) {
                ha_lost(a, "hub answered lease_lost on renew");
            } else {
                alelt_ha_event ev;
                alelt_logf(ALELT_LOG_WARN, "ha", "lease %s failed: %s", active ? "renew" : "acquire", err);
                EnterCriticalSection(&a->put_lock);
                EnterCriticalSection(&a->lock);
                ev = alelt_ha_on_error(&a->ha, alelt_mono_ms());
                LeaveCriticalSection(&a->lock);
                LeaveCriticalSection(&a->put_lock);
                if (ev == ALELT_HA_LOST)
                    alelt_logf(ALELT_LOG_WARN, "ha", "capture lease not renewed in time (self-fencing): reading stopped, now STANDBY");
            }
        }
        EnterCriticalSection(&a->lock);
        wait = alelt_ha_wait_ms(&a->ha, alelt_mono_ms());
        LeaveCriticalSection(&a->lock);
        if (wait < 50) wait = 50;
        if (wait > 5000) wait = 5000;
        if (nap(a, (DWORD)wait, NULL)) break;
    }
    if (ha_active(a)) {
        alelt_lease_grant g;
        char lease_id[37];
        long long epoch;
        EnterCriticalSection(&a->send_lock); /* no batch in flight while giving up the lease */
        EnterCriticalSection(&a->put_lock);
        EnterCriticalSection(&a->lock);
        alelt_strcpy(lease_id, sizeof(lease_id), a->ha.lease_id);
        epoch = a->ha.epoch;
        (void)alelt_ha_on_lost(&a->ha, alelt_mono_ms());
        LeaveCriticalSection(&a->lock);
        LeaveCriticalSection(&a->put_lock);
        LeaveCriticalSection(&a->send_lock);
        if (alelt_hub_lease(h, "release", &a->cfg, a->cfg.ha_lease_ttl_s, lease_id, epoch, &g, err, sizeof(err)) == 0)
            alelt_logf(ALELT_LOG_INFO, "ha", "capture lease released (epoch %lld): the standby may take over now", epoch);
        else
            alelt_logf(ALELT_LOG_WARN, "ha", "lease release failed (%s): the standby takes over after the ttl", err);
    }
    alelt_http_free(h);
    return 0;
}

/* ------------------------------------------------------------------ run */
static int url_safe(const char *s)
{
    for (; *s; s++) {
        char c = *s;
        if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.'))
            return 0;
    }
    return 1;
}

int alelt_agent_run(const wchar_t *config_path, HANDLE stop_ev, int console, alelt_running_cb on_running)
{
    alelt_agent *a;
    char err[512];
    alelt_log_cfg lc;
    HANDLE th[4] = {NULL, NULL, NULL, NULL}, th_live[4];
    int rc = 0, i, hung = 0;
    wchar_t hn[256];
    DWORD hnl = 256;

    a = (alelt_agent *)calloc(1, sizeof(*a));
    if (!a) return 2;
    a->q = (alelt_cmd *)calloc(QCAP, sizeof(alelt_cmd));
    if (!a->q) {
        free(a);
        return 2;
    }
    InitializeCriticalSection(&a->lock);
    InitializeCriticalSection(&a->put_lock);
    InitializeCriticalSection(&a->send_lock);
    a->stop_ev = stop_ev;
    a->console = console;
    a->cmd_ev = CreateEventW(NULL, FALSE, FALSE, NULL);
    a->send_ev = CreateEventW(NULL, FALSE, FALSE, NULL);
    g_a = a;

    if (alelt_config_load(config_path, &a->cfg, err, sizeof(err)) != 0) {
        alelt_logf(ALELT_LOG_ERROR, "agent", "%s", err);
        alelt_eventlog(ALELT_LOG_ERROR, err);
        rc = ALELT_EXIT_CONFIG;
        goto out;
    }
    memset(&lc, 0, sizeof(lc));
    lc.dir = a->cfg.log_dir;
    lc.level = a->cfg.log_level;
    lc.max_bytes = a->cfg.log_max_bytes;
    lc.keep = a->cfg.log_keep;
    lc.console = console;
    lc.eventlog = 1;
    if (alelt_log_open(&lc) != 0) alelt_logf(ALELT_LOG_WARN, "agent", "cannot open log file in %ls", a->cfg.log_dir);
    alelt_logf(ALELT_LOG_INFO, "agent", "ALELT agent %s starting (agent_id=%s connection_id=%s engine=%s mode=%s)",
            ALELT_AGENT_VERSION, a->cfg.agent_id, a->cfg.connection_id, a->cfg.engine,
            a->cfg.hub_mode == ALELT_HUB_MTLS ? "mtls" : "dev");
    if (!url_safe(a->cfg.agent_id)) {
        alelt_logf(ALELT_LOG_ERROR, "agent", "agent_id may only contain A-Z a-z 0-9 - _ .");
        rc = ALELT_EXIT_CONFIG;
        goto out;
    }
    if (alelt_guard_check(&a->cfg, err, sizeof(err)) != 0) { /* REQ-12: before the source is ever opened */
        alelt_logf(ALELT_LOG_ERROR, "guard", "%s", err);
        alelt_eventlog(ALELT_LOG_ERROR, err);
        rc = ALELT_EXIT_GUARD;               /* not restarted: the service reports a clean stop for this code */
        goto out;
    }
    alelt_ha_init(&a->ha, a->cfg.ha_active_passive, a->cfg.ha_lease_ttl_s, a->cfg.ha_renew_s);
    if (a->cfg.websocket) alelt_logf(ALELT_LOG_WARN, "agent", "hub.websocket requested but not built; using long-poll");
    a->limits = a->cfg.limits;
    if (alelt_job_apply(&a->limits, err, sizeof(err)) != 0) {
        if (a->cfg.require_job_limits) {
            alelt_logf(ALELT_LOG_ERROR, "job", "%s; refusing to run without resource caps (require_job_limits=true)", err);
            rc = 4;
            goto out;
        }
        alelt_logf(ALELT_LOG_WARN, "job", "%s; continuing WITHOUT job limits (require_job_limits=false)", err);
    }
    if (GetComputerNameExW(ComputerNameDnsFullyQualified, hn, &hnl)) alelt_to_utf8(hn, a->hostname, sizeof(a->hostname));
    else alelt_strcpy(a->hostname, sizeof(a->hostname), "unknown");

    if (alelt_spool_open(&a->spool, a->cfg.spool_dir, a->cfg.spool_max_bytes, err, sizeof(err)) != 0) {
        alelt_logf(ALELT_LOG_ERROR, "spool", "%s", err);
        rc = 5;
        goto out;
    }
    a->spool_open = 1;
    if (plugin_load(a, err, sizeof(err)) != 0) {
        alelt_logf(ALELT_LOG_ERROR, "worker", "%s", err);
        rc = 6;
        goto out;
    }
    if (a->cfg.capture_autostart) {
        char np[ALELT_POS_MAX];
        alelt_spool_snapshot(&a->spool, NULL, NULL, NULL, np, NULL);
        if (np[0]) alelt_spool_set_capturing(&a->spool, 1, NULL);
    }

    th[0] = CreateThread(NULL, 0, worker_thread, a, 0, NULL);
    th[1] = CreateThread(NULL, 0, sender_thread, a, 0, NULL);
    th[2] = CreateThread(NULL, 0, control_thread, a, 0, NULL);
    if (a->ha.enabled) th[3] = CreateThread(NULL, 0, ha_thread, a, 0, NULL);
    if (!th[0] || !th[1] || !th[2] || (a->ha.enabled && !th[3])) {
        alelt_logf(ALELT_LOG_ERROR, "agent", "cannot create threads (%lu)", (unsigned long)GetLastError());
        SetEvent(a->stop_ev);
        rc = 7;
    } else if (on_running) {
        on_running();
    }
    alelt_logf(ALELT_LOG_INFO, "agent", "running (outbound only, no listening sockets)");
    WaitForSingleObject(a->stop_ev, INFINITE);
    alelt_logf(ALELT_LOG_INFO, "agent", "stopping");
    {   /* AHS-23: give in-flight requests a moment, then abort them so the join below is bounded */
        DWORD alive = 0;
        for (i = 0; i < 4; i++) if (th[i]) th_live[alive++] = th[i];
        if (g_stop_progress) g_stop_progress();
        if (alive && WaitForMultipleObjects(alive, th_live, TRUE, 5000) == WAIT_TIMEOUT) {
            int n = alelt_http_abort_all();
            if (n) alelt_logf(ALELT_LOG_WARN, "agent", "stop: %d in-flight hub request(s) aborted (unsent batches stay spooled)", n);
        }
    }
    for (i = 0; i < 4; i++) {
        if (th[i]) {
            if (join_thread(th[i], 45000) != 0) {
                alelt_logf(ALELT_LOG_WARN, "agent", "thread %d did not stop in time", i);
                hung = 1;
            }
            CloseHandle(th[i]);
        }
    }
    if (rc == 0) rc = a->exit_code;
    if (hung) {
        /* A thread is still inside a blocking call: do not free shared state
         * under it. The process exits right after this returns. */
        alelt_logf(ALELT_LOG_WARN, "agent", "stopped with a hung thread (exit code %d)", rc);
        alelt_log_close();
        return rc;
    }
out:
    alelt_logf(rc ? ALELT_LOG_ERROR : ALELT_LOG_INFO, "agent", "stopped (exit code %d)", rc);
    if (a->spool_open) alelt_spool_close(&a->spool);
    /* Plugin DLL intentionally not unloaded: process is exiting. */
    alelt_config_free(&a->cfg);
    alelt_job_close();
    alelt_log_close();
    g_a = NULL;
    CloseHandle(a->cmd_ev);
    CloseHandle(a->send_ev);
    DeleteCriticalSection(&a->send_lock);
    DeleteCriticalSection(&a->put_lock);
    DeleteCriticalSection(&a->lock);
    free(a->q);
    free(a);
    return rc;
}
