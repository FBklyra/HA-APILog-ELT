/* SPDX-License-Identifier: Apache-2.0
 * aleltagent.json loader. Strict: unknown keys are rejected (keys starting with
 * "_" are comments) so that a typo never silently disables a safety limit.
 */
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <shlobj.h>

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "config.h"
#include "alelt_json.h"
#include "log.h"

#define ERR(...) (snprintf(err, errcap, __VA_ARGS__), -1)

static int keys_ok(const fj_val *o, const char *const *allowed, const char *where, char *err, size_t errcap)
{
    const fj_val *c;
    if (!o) return 0;
    if (o->type != FJ_OBJ) return ERR("config: '%s' must be an object", where);
    for (c = o->child; c; c = c->next) {
        const char *const *a;
        int ok = (c->keylen > 0 && c->key[0] == '_');
        for (a = allowed; !ok && *a; a++) {
            if (strlen(*a) == c->keylen && memcmp(*a, c->key, c->keylen) == 0) ok = 1;
        }
        if (!ok) return ERR("config: unknown key '%s' in '%s'", c->key, where);
    }
    return 0;
}

static int get_str(const fj_val *o, const char *k, char *dst, size_t cap, int required, char *err, size_t errcap)
{
    const fj_val *v = fj_get(o, k);
    if (!v || v->type == FJ_NULL) {
        if (required) return ERR("config: '%s' is required", k);
        return 0;
    }
    if (v->type != FJ_STR) return ERR("config: '%s' must be a string", k);
    if (alelt_strcpy(dst, cap, v->s) != 0) return ERR("config: '%s' too long", k);
    return 0;
}

static int get_wpath(const fj_val *o, const char *k, wchar_t *dst, size_t cap, char *err, size_t errcap)
{
    const fj_val *v = fj_get(o, k);
    wchar_t tmp[ALELT_PATH_MAX];
    DWORD n;
    if (!v || v->type == FJ_NULL) return 0;
    if (v->type != FJ_STR) return ERR("config: '%s' must be a string", k);
    if (alelt_to_wide(v->s, tmp, ALELT_PATH_MAX) != 0) return ERR("config: '%s' invalid", k);
    n = ExpandEnvironmentStringsW(tmp, dst, (DWORD)cap);
    if (n == 0 || n > cap) return ERR("config: '%s' too long", k);
    return 0;
}

static int get_int(const fj_val *o, const char *k, int *dst, int lo, int hi, char *err, size_t errcap)
{
    int64_t v;
    int r = fj_get_i64(o, k, &v);
    if (r == -1) return 0;
    if (r != 0 || v < lo || v > hi) return ERR("config: '%s' must be an integer in [%d, %d]", k, lo, hi);
    *dst = (int)v;
    return 0;
}

static int get_bool(const fj_val *o, const char *k, int *dst, char *err, size_t errcap)
{
    int r = fj_get_bool(o, k, dst);
    if (r == -2) return ERR("config: '%s' must be true/false", k);
    return 0;
}

int alelt_ident_valid(const char *s)
{
    size_t n = 0;
    if (!s || !*s) return 0;
    for (; *s; s++, n++) {
        char c = *s;
        if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '$' ||
              c == '#' || c == '@'))
            return 0;
    }
    return n < ALELT_IDENT_MAX;
}

static int parse_limits(const fj_val *o, alelt_limits *l, const char *where, char *err, size_t errcap)
{
    static const char *const keys[] = {"cpu_pct", "mem_mb", "batch_kb", "max_rate_kbps", "poll_ms", NULL};
    if (!o) return 0;
    if (keys_ok(o, keys, where, err, errcap)) return -1;
    if (get_int(o, "cpu_pct", &l->cpu_pct, 1, 6400, err, errcap)) return -1;
    if (get_int(o, "mem_mb", &l->mem_mb, 64, 65536, err, errcap)) return -1;
    if (get_int(o, "batch_kb", &l->batch_kb, 64, 65536, err, errcap)) return -1;
    if (get_int(o, "max_rate_kbps", &l->max_rate_kbps, 0, 10000000, err, errcap)) return -1;
    if (get_int(o, "poll_ms", &l->poll_ms, 50, 600000, err, errcap)) return -1;
    return 0;
}

void alelt_limits_clamp(alelt_limits *l, const alelt_limits *m)
{
    if (l->cpu_pct > m->cpu_pct) l->cpu_pct = m->cpu_pct;
    if (l->cpu_pct < 1) l->cpu_pct = 1;
    if (l->mem_mb > m->mem_mb) l->mem_mb = m->mem_mb;
    if (l->mem_mb < 64) l->mem_mb = 64;
    if (l->batch_kb > m->batch_kb) l->batch_kb = m->batch_kb;
    if (l->batch_kb < 64) l->batch_kb = 64;
    if (m->max_rate_kbps > 0 && (l->max_rate_kbps == 0 || l->max_rate_kbps > m->max_rate_kbps))
        l->max_rate_kbps = m->max_rate_kbps;
    if (l->poll_ms < 50) l->poll_ms = 50;
    if (l->poll_ms > m->poll_ms) l->poll_ms = m->poll_ms;
}

int alelt_config_default_path(wchar_t *out, size_t cap)
{
    PWSTR pd = NULL;
    int r;
    if (FAILED(SHGetKnownFolderPath(&FOLDERID_ProgramData, 0, NULL, &pd))) return -1;
    r = _snwprintf_s(out, cap, _TRUNCATE, L"%ls\\ALELT\\Agent\\aleltagent.json", pd);
    CoTaskMemFree(pd);
    return r < 0 ? -1 : 0;
}

static int default_data_dir(wchar_t *out, size_t cap, const wchar_t *leaf);

int alelt_config_default_data_dir(wchar_t *out, size_t cap, const wchar_t *leaf)
{
    return default_data_dir(out, cap, leaf);
}

static int default_data_dir(wchar_t *out, size_t cap, const wchar_t *leaf)
{
    PWSTR pd = NULL;
    int r;
    if (FAILED(SHGetKnownFolderPath(&FOLDERID_ProgramData, 0, NULL, &pd))) return -1;
    r = _snwprintf_s(out, cap, _TRUNCATE, L"%ls\\ALELT\\Agent\\%ls", pd, leaf);
    CoTaskMemFree(pd);
    return r < 0 ? -1 : 0;
}

static void defaults(alelt_config *c)
{
    c->hub_mode = ALELT_HUB_MTLS;
    c->heartbeat_s = 15;
    c->command_wait_s = 10;
    c->connect_timeout_ms = 10000;
    c->request_timeout_ms = 60000;
    c->backoff_max_s = 300;
    c->spool_max_bytes = (uint64_t)2048 * 1024 * 1024;
    c->log_level = ALELT_LOG_INFO;
    c->log_max_bytes = (uint64_t)20 * 1024 * 1024;
    c->log_keep = 5;
    c->limits.cpu_pct = 100;
    c->limits.mem_mb = 512;
    c->limits.batch_kb = 1024;
    c->limits.max_rate_kbps = 0;
    c->limits.poll_ms = 1000;
    c->limits_max.cpu_pct = 100;
    c->limits_max.mem_mb = 1024;
    c->limits_max.batch_kb = 8192;
    c->limits_max.max_rate_kbps = 0;
    c->limits_max.poll_ms = 600000;
    c->require_job_limits = 1;
    c->gov_enabled = 1;
    c->gov_interval_s = 15;
    c->gov_host_cpu_pause_pct = 85;
    c->gov_host_cpu_resume_pct = 70;
    c->gov_sustain = 3;
    c->gov_max_log_util_pct = 90;
    c->ha_active_passive = 0;
    c->ha_lease_ttl_s = 30;
    c->ha_renew_s = 10;
    alelt_strcpy(c->plugin, sizeof(c->plugin), "aleltsrc_db2.dll");
    alelt_strcpy(c->engine, sizeof(c->engine), "db2");
    alelt_strcpy(c->dev_token_env, sizeof(c->dev_token_env), "ALELT_AGENT_TOKEN");
    c->snapshot_max_chunk_rows = 5000;
    c->src_port = -1;
    (void)default_data_dir(c->spool_dir, ALELT_PATH_MAX, L"spool");
    (void)default_data_dir(c->log_dir, ALELT_PATH_MAX, L"log");
}

static void upper_ascii(char *s)
{
    for (; *s; s++) {
        if (*s >= 'a' && *s <= 'z') *s = (char)(*s - 'a' + 'A');
    }
}

/* Allow-list matcher, identical in the three agents (docs/AGENT-COMMANDS.md, shared vectors in
 * edge-agent/shared/allowlist_vectors.json): "SCHEMA.TABLE" (case-insensitive after upper-casing)
 * or "SCHEMA.*" (every base table of the schema). Entries are upper-cased at config load. */
int alelt_config_table_allowed(const alelt_config *cfg, const char *schema, const char *table)
{
    char fq[2 * ALELT_IDENT_MAX + 1], sch[ALELT_IDENT_MAX + 3];
    int i;
    if (!alelt_ident_valid(schema) || !alelt_ident_valid(table)) return 0;
    if (snprintf(fq, sizeof(fq), "%s.%s", schema, table) < 0) return 0;
    if (snprintf(sch, sizeof(sch), "%s.*", schema) < 0) return 0;
    upper_ascii(fq);
    upper_ascii(sch);
    for (i = 0; i < cfg->nallowed; i++) {
        if (strcmp(cfg->allowed_tables[i], fq) == 0 || strcmp(cfg->allowed_tables[i], sch) == 0) return 1;
    }
    return 0;
}

/* Entry syntax at config load: SCHEMA.TABLE or SCHEMA.* ; a schema wildcard is refused. */
int alelt_config_allow_entry_valid(const char *entry)
{
    char s[ALELT_IDENT_MAX], t[ALELT_IDENT_MAX];
    const char *dot = entry ? strchr(entry, '.') : NULL;
    if (!dot || dot == entry || (size_t)(dot - entry) >= ALELT_IDENT_MAX) return 0;
    memcpy(s, entry, (size_t)(dot - entry));
    s[dot - entry] = 0;
    if (alelt_strcpy(t, sizeof(t), dot + 1) != 0 || !alelt_ident_valid(s)) return 0;
    return strcmp(t, "*") == 0 || alelt_ident_valid(t);
}

void alelt_config_free(alelt_config *cfg)
{
    alelt_buf_free(&cfg->settings_json);
    free(cfg->allowed_tables);
    cfg->allowed_tables = NULL;
    cfg->nallowed = 0;
    free(cfg->deny_hosts);
    cfg->deny_hosts = NULL;
    cfg->ndeny = 0;
}

int alelt_config_load(const wchar_t *path, alelt_config *cfg, char *err, size_t errcap)
{
    static const char *const top[] = {"agent_id", "connection_id", "hub", "spool", "log", "limits", "limits_max",
                                      "governor", "source", "snapshot", "capture", "require_job_limits", "ha",
                                      "deny_hosts", "environment", "allow_prod", NULL};
    static const char *const hubk[] = {"url",           "mode",           "client_cert_thumbprint", "server_pins_sha256",
                                       "dev_token_env", "heartbeat_s",    "command_wait_s",         "connect_timeout_ms",
                                       "request_timeout_ms", "backoff_max_s", "websocket", "token_env", "proxy",
                                       "allow_insecure_http", NULL};
    static const char *const spk[] = {"dir", "max_mb", NULL};
    static const char *const lgk[] = {"dir", "level", "max_mb", "keep", NULL};
    static const char *const gvk[] = {"enabled", "interval_s", "host_cpu_pause_pct", "host_cpu_resume_pct",
                                      "sustain_samples", "max_log_util_pct", NULL};
    static const char *const srk[] = {"plugin", "engine", "database", "dll_dirs", "settings", NULL};
    static const char *const snk[] = {"allowed_tables", "max_chunk_rows", NULL};
    static const char *const cpk[] = {"autostart", NULL};
    static const char *const hak[] = {"mode", "lease_ttl_s", "renew_s", NULL};
    alelt_buf raw;
    fj_doc *doc;
    const fj_val *r, *o, *v;
    char mode[16] = "mtls";
    int rc = -1, mb;

    memset(cfg, 0, sizeof(*cfg));
    alelt_buf_init(&cfg->settings_json, 1024 * 1024);
    defaults(cfg);
    alelt_wcscpy(cfg->path, ALELT_PATH_MAX, path);
    alelt_buf_init(&raw, 4 * 1024 * 1024);
    if (alelt_read_file(path, &raw, 4 * 1024 * 1024) != 0) {
        snprintf(err, errcap, "config: cannot read %ls (error %lu)", path, (unsigned long)GetLastError());
        alelt_buf_free(&raw);
        return -1;
    }
    doc = fj_parse((const char *)raw.data, raw.len, err, errcap);
    alelt_buf_free(&raw);
    if (!doc) return -1;
    r = fj_root(doc);
    if (keys_ok(r, top, "(root)", err, errcap) || !r) goto out;

    if (get_str(r, "agent_id", cfg->agent_id, sizeof(cfg->agent_id), 1, err, errcap)) goto out;
    if (get_str(r, "connection_id", cfg->connection_id, sizeof(cfg->connection_id), 1, err, errcap)) goto out;
    if (get_bool(r, "require_job_limits", &cfg->require_job_limits, err, errcap)) goto out;

    o = fj_get(r, "hub");
    if (!o) { ERR("config: 'hub' is required"); goto out; }
    if (keys_ok(o, hubk, "hub", err, errcap)) goto out;
    if (get_str(o, "url", cfg->hub_url, sizeof(cfg->hub_url), 1, err, errcap)) goto out;
    if (get_str(o, "mode", mode, sizeof(mode), 0, err, errcap)) goto out;
    if (strcmp(mode, "mtls") == 0) cfg->hub_mode = ALELT_HUB_MTLS;
    else if (strcmp(mode, "dev") == 0 || strcmp(mode, "token") == 0) cfg->hub_mode = ALELT_HUB_DEV; /* token = enrollment token (wizard package) */
    else { ERR("config: hub.mode must be 'mtls', 'token' or 'dev'"); goto out; }
    if (get_str(o, "token_env", cfg->dev_token_env, sizeof(cfg->dev_token_env), 0, err, errcap)) goto out;
    if (get_str(o, "client_cert_thumbprint", cfg->cert_thumbprint, sizeof(cfg->cert_thumbprint), 0, err, errcap)) goto out;
    if (get_str(o, "dev_token_env", cfg->dev_token_env, sizeof(cfg->dev_token_env), 0, err, errcap)) goto out;
    v = fj_get(o, "server_pins_sha256");
    if (v && v->type != FJ_NULL) {
        const fj_val *e;
        if (v->type != FJ_ARR) { ERR("config: hub.server_pins_sha256 must be an array"); goto out; }
        for (e = v->child; e; e = e->next) {
            uint8_t tmp[32];
            size_t n = 0;
            if (cfg->npins >= ALELT_MAX_PINS) { ERR("config: at most %d pins", ALELT_MAX_PINS); goto out; }
            if (e->type != FJ_STR || alelt_unhex(e->s, tmp, sizeof(tmp), &n) != 0 || n != 32) {
                ERR("config: pin must be 64 hex chars (SHA-256 of server SubjectPublicKeyInfo)");
                goto out;
            }
            alelt_hex(tmp, 32, cfg->hub_pins[cfg->npins++], 0);
        }
    }
    if (get_int(o, "heartbeat_s", &cfg->heartbeat_s, 5, 3600, err, errcap)) goto out;
    if (get_int(o, "command_wait_s", &cfg->command_wait_s, 1, 25, err, errcap)) goto out;
    if (get_int(o, "connect_timeout_ms", &cfg->connect_timeout_ms, 1000, 120000, err, errcap)) goto out;
    if (get_int(o, "request_timeout_ms", &cfg->request_timeout_ms, 5000, 600000, err, errcap)) goto out;
    if (get_int(o, "backoff_max_s", &cfg->backoff_max_s, 5, 3600, err, errcap)) goto out;
    if (get_bool(o, "websocket", &cfg->websocket, err, errcap)) goto out;
    if (get_str(o, "proxy", cfg->hub_proxy, sizeof(cfg->hub_proxy), 0, err, errcap)) goto out;
    if (get_bool(o, "allow_insecure_http", &cfg->allow_insecure_http, err, errcap)) goto out;
    if (cfg->hub_mode == ALELT_HUB_MTLS) {
        if (_strnicmp(cfg->hub_url, "https://", 8) != 0) { ERR("config: mtls mode requires an https:// hub url"); goto out; }
        if (!cfg->cert_thumbprint[0]) { ERR("config: hub.client_cert_thumbprint is required in mtls mode"); goto out; }
        if (cfg->npins == 0) { ERR("config: hub.server_pins_sha256 is required in mtls mode"); goto out; }
    } else if (_strnicmp(cfg->hub_url, "http://", 7) != 0 && _strnicmp(cfg->hub_url, "https://", 8) != 0) {
        ERR("config: hub.url must start with http:// or https://");
        goto out;
    }

    o = fj_get(r, "spool");
    if (o) {
        if (keys_ok(o, spk, "spool", err, errcap)) goto out;
        if (get_wpath(o, "dir", cfg->spool_dir, ALELT_PATH_MAX, err, errcap)) goto out;
        mb = (int)(cfg->spool_max_bytes / (1024 * 1024));
        if (get_int(o, "max_mb", &mb, 16, 1048576, err, errcap)) goto out;
        cfg->spool_max_bytes = (uint64_t)mb * 1024 * 1024;
    }
    o = fj_get(r, "log");
    if (o) {
        char lvl[16] = "INFO";
        if (keys_ok(o, lgk, "log", err, errcap)) goto out;
        if (get_wpath(o, "dir", cfg->log_dir, ALELT_PATH_MAX, err, errcap)) goto out;
        if (get_str(o, "level", lvl, sizeof(lvl), 0, err, errcap)) goto out;
        cfg->log_level = alelt_log_level_from_str(lvl);
        mb = (int)(cfg->log_max_bytes / (1024 * 1024));
        if (get_int(o, "max_mb", &mb, 1, 1024, err, errcap)) goto out;
        cfg->log_max_bytes = (uint64_t)mb * 1024 * 1024;
        if (get_int(o, "keep", &cfg->log_keep, 1, 50, err, errcap)) goto out;
    }
    if (parse_limits(fj_get(r, "limits_max"), &cfg->limits_max, "limits_max", err, errcap)) goto out;
    if (parse_limits(fj_get(r, "limits"), &cfg->limits, "limits", err, errcap)) goto out;
    alelt_limits_clamp(&cfg->limits, &cfg->limits_max);

    o = fj_get(r, "governor");
    if (o) {
        if (keys_ok(o, gvk, "governor", err, errcap)) goto out;
        if (get_bool(o, "enabled", &cfg->gov_enabled, err, errcap)) goto out;
        if (get_int(o, "interval_s", &cfg->gov_interval_s, 1, 3600, err, errcap)) goto out;
        if (get_int(o, "host_cpu_pause_pct", &cfg->gov_host_cpu_pause_pct, 1, 100, err, errcap)) goto out;
        if (get_int(o, "host_cpu_resume_pct", &cfg->gov_host_cpu_resume_pct, 0, 100, err, errcap)) goto out;
        if (get_int(o, "sustain_samples", &cfg->gov_sustain, 1, 100, err, errcap)) goto out;
        if (get_int(o, "max_log_util_pct", &cfg->gov_max_log_util_pct, 1, 100, err, errcap)) goto out;
        if (cfg->gov_host_cpu_resume_pct >= cfg->gov_host_cpu_pause_pct) {
            ERR("config: governor.host_cpu_resume_pct must be below host_cpu_pause_pct");
            goto out;
        }
    }

    o = fj_get(r, "source");
    if (!o) { ERR("config: 'source' is required"); goto out; }
    if (keys_ok(o, srk, "source", err, errcap)) goto out;
    if (get_str(o, "plugin", cfg->plugin, sizeof(cfg->plugin), 0, err, errcap)) goto out;
    if (strchr(cfg->plugin, '\\') || strchr(cfg->plugin, '/') || strchr(cfg->plugin, ':') || strstr(cfg->plugin, "..")) {
        ERR("config: source.plugin must be a bare file name located next to aleltagent.exe");
        goto out;
    }
    if (get_str(o, "engine", cfg->engine, sizeof(cfg->engine), 0, err, errcap)) goto out;
    if (get_str(o, "database", cfg->database, sizeof(cfg->database), 1, err, errcap)) goto out;
    v = fj_get(o, "dll_dirs");
    if (v && v->type != FJ_NULL) {
        const fj_val *e;
        if (v->type != FJ_ARR) { ERR("config: source.dll_dirs must be an array"); goto out; }
        for (e = v->child; e; e = e->next) {
            wchar_t tmp[ALELT_PATH_MAX];
            if (cfg->ndll_dirs >= ALELT_MAX_DLL_DIRS || e->type != FJ_STR || alelt_to_wide(e->s, tmp, ALELT_PATH_MAX) != 0) {
                ERR("config: bad source.dll_dirs entry");
                goto out;
            }
            if (ExpandEnvironmentStringsW(tmp, cfg->dll_dirs[cfg->ndll_dirs], ALELT_PATH_MAX) == 0) {
                ERR("config: bad source.dll_dirs entry");
                goto out;
            }
            cfg->ndll_dirs++;
        }
    }
    v = fj_get(o, "settings");
    {
        alelt_jw w;
        jw_init(&w, &cfg->settings_json);
        if (!v) {
            alelt_buf_puts(&cfg->settings_json, "{}");
        } else if (v->type != FJ_OBJ) {
            ERR("config: source.settings must be an object");
            goto out;
        } else {
            /* Re-serialise the sub-object (validated by the parser). */
            const fj_val *m;
            jw_obj(&w);
            for (m = v->child; m; m = m->next) {
                if (m->type == FJ_STR && (!strcmp(m->key, "host") || !strcmp(m->key, "hostname") ||
                                          !strcmp(m->key, "server")) && !cfg->src_host[0]) {
                    if (alelt_strcpy(cfg->src_host, sizeof(cfg->src_host), m->s) != 0) { ERR("config: source host too long"); goto out; }
                }
                if (!strcmp(m->key, "port") && (m->type == FJ_NUM || m->type == FJ_STR)) {
                    int64_t pv = 0;
                    if (fj_get_i64(v, "port", &pv) == 0 && pv > 0 && pv < 65536) cfg->src_port = (int)pv;
                    else if (m->type == FJ_STR && atoi(m->s) > 0 && atoi(m->s) < 65536) cfg->src_port = atoi(m->s);
                }
                jw_key(&w, m->key);
                if (m->type == FJ_STR) jw_str(&w, m->s);
                else if (m->type == FJ_NUM) jw_raw(&w, m->s, m->n);
                else if (m->type == FJ_TRUE) jw_bool(&w, 1);
                else if (m->type == FJ_FALSE) jw_bool(&w, 0);
                else if (m->type == FJ_NULL) jw_null(&w);
                else if (m->type == FJ_ARR) {
                    const fj_val *e;
                    jw_arr(&w);
                    for (e = m->child; e; e = e->next) {
                        if (e->type != FJ_STR) { ERR("config: source.settings arrays must hold strings"); goto out; }
                        jw_str(&w, e->s);
                    }
                    jw_arr_end(&w);
                } else { ERR("config: source.settings values must be scalars or string arrays"); goto out; }
            }
            jw_obj_end(&w);
            if (jw_ok(&w) != 0) { ERR("config: source.settings too large"); goto out; }
        }
    }

    o = fj_get(r, "snapshot");
    if (o) {
        if (keys_ok(o, snk, "snapshot", err, errcap)) goto out;
        if (get_int(o, "max_chunk_rows", &cfg->snapshot_max_chunk_rows, 1, 1000000, err, errcap)) goto out;
        v = fj_get(o, "allowed_tables");
        if (v && v->type != FJ_NULL) {
            const fj_val *e;
            if (v->type != FJ_ARR || v->n > ALELT_MAX_ALLOWED_TABLES) {
                ERR("config: snapshot.allowed_tables must be an array (max %d)", ALELT_MAX_ALLOWED_TABLES);
                goto out;
            }
            cfg->allowed_tables = calloc(v->n ? v->n : 1, sizeof(*cfg->allowed_tables));
            if (!cfg->allowed_tables) { ERR("config: out of memory"); goto out; }
            for (e = v->child; e; e = e->next) {
                if (e->type != FJ_STR || !alelt_config_allow_entry_valid(e->s)) {
                    ERR("config: invalid allowed_tables entry (SCHEMA.TABLE or SCHEMA.*; '*.*' is refused)");
                    goto out;
                }
                alelt_strcpy(cfg->allowed_tables[cfg->nallowed], 2 * ALELT_IDENT_MAX + 1, e->s);
                upper_ascii(cfg->allowed_tables[cfg->nallowed]);
                cfg->nallowed++;
            }
        }
    }
    o = fj_get(r, "capture");
    if (o) {
        if (keys_ok(o, cpk, "capture", err, errcap)) goto out;
        if (get_bool(o, "autostart", &cfg->capture_autostart, err, errcap)) goto out;
    }
    o = fj_get(r, "ha");
    if (o) {
        char ham[32] = "standalone";
        if (keys_ok(o, hak, "ha", err, errcap)) goto out;
        if (get_str(o, "mode", ham, sizeof(ham), 0, err, errcap)) goto out;
        if (strcmp(ham, "active_passive") == 0) cfg->ha_active_passive = 1;
        else if (strcmp(ham, "standalone") == 0) cfg->ha_active_passive = 0;
        else { ERR("config: ha.mode must be 'standalone' or 'active_passive'"); goto out; }
        if (get_int(o, "lease_ttl_s", &cfg->ha_lease_ttl_s, 5, 300, err, errcap)) goto out;
        if (!fj_get(o, "renew_s") && cfg->ha_renew_s > cfg->ha_lease_ttl_s / 2) cfg->ha_renew_s = cfg->ha_lease_ttl_s / 3;
        if (get_int(o, "renew_s", &cfg->ha_renew_s, 1, 300, err, errcap)) goto out;
        if (cfg->ha_renew_s < 1 || cfg->ha_renew_s > cfg->ha_lease_ttl_s / 2) {
            ERR("config: ha.renew_s must be in [1, lease_ttl_s/2]");
            goto out;
        }
    }
    v = fj_get(r, "deny_hosts");
    if (v && v->type != FJ_NULL) {
        const fj_val *e;
        if (v->type != FJ_ARR || v->n > ALELT_MAX_DENY) { ERR("config: deny_hosts must be an array (max %d)", ALELT_MAX_DENY); goto out; }
        cfg->deny_hosts = calloc(v->n ? v->n : 1, sizeof(*cfg->deny_hosts));
        if (!cfg->deny_hosts) { ERR("config: out of memory"); goto out; }
        for (e = v->child; e; e = e->next) {
            char *q;
            if (e->type != FJ_STR || !e->s[0] || alelt_strcpy(cfg->deny_hosts[cfg->ndeny], ALELT_DENY_LEN + 1, e->s) != 0) {
                ERR("config: deny_hosts entries must be non-empty strings (max %d chars)", ALELT_DENY_LEN);
                goto out;
            }
            for (q = cfg->deny_hosts[cfg->ndeny]; *q; q++) *q = (char)tolower((unsigned char)*q);
            cfg->ndeny++;
        }
    }
    {
        char env[16] = "TEST";
        if (get_str(r, "environment", env, sizeof(env), 0, err, errcap)) goto out;
        if (_stricmp(env, "PROD") == 0) cfg->env_prod = 1;
        else if (_stricmp(env, "TEST") != 0) { ERR("config: environment must be TEST or PROD"); goto out; }
    }
    if (get_bool(r, "allow_prod", &cfg->allow_prod, err, errcap)) goto out;
    if (!cfg->spool_dir[0] || !cfg->log_dir[0]) { ERR("config: spool/log directory not resolvable"); goto out; }
    rc = 0;
out:
    fj_free(doc);
    if (rc != 0) alelt_config_free(cfg);
    return rc;
}

/* ------------------------------------------------------------------ initial config */
/* Database alias written when --database is not given: ALELT_DB2_DATABASE, else the Db2 sample name. */
static const char *default_database(void)
{
    static char buf[129];
    DWORD n = GetEnvironmentVariableA("ALELT_DB2_DATABASE", buf, (DWORD)sizeof buf);
    return (n > 0 && n < sizeof buf) ? buf : "SAMPLE";
}

int alelt_config_write_initial(const wchar_t *path, const alelt_init_opts *o, char *err, size_t errcap)
{
    alelt_buf b;
    alelt_jw w;
    wchar_t dir[ALELT_PATH_MAX], *slash;
    const char *mode = (o->mode && *o->mode) ? o->mode : "mtls";
    int rc;
    if (GetFileAttributesW(path) != INVALID_FILE_ATTRIBUTES) return 1; /* keep existing (upgrade) */
    alelt_wcscpy(dir, ALELT_PATH_MAX, path);
    slash = wcsrchr(dir, L'\\');
    if (slash) {
        *slash = 0;
        if (alelt_mkdirs(dir) != 0) return ERR("init-config: cannot create %ls", dir);
    }
    alelt_buf_init(&b, 1024 * 1024);
    jw_init(&w, &b);
    jw_obj(&w);
    jw_key(&w, "_comment"); jw_str(&w, "ALELT agent config. Secrets never go here (R9).");
    jw_key(&w, "agent_id"); jw_str(&w, (o->agent_id && *o->agent_id) ? o->agent_id : "edge-01");
    jw_key(&w, "connection_id"); jw_str(&w, (o->connection_id && *o->connection_id) ? o->connection_id : "erp");
    jw_key(&w, "hub");
    jw_obj(&w);
    jw_key(&w, "url"); jw_str(&w, (o->hub_url && *o->hub_url) ? o->hub_url : "https://hub.example.internal:8443");
    jw_key(&w, "mode"); jw_str(&w, mode);
    jw_key(&w, "client_cert_thumbprint"); jw_str(&w, o->thumbprint ? o->thumbprint : "");
    jw_key(&w, "server_pins_sha256");
    jw_arr(&w);
    if (o->pin && *o->pin) jw_str(&w, o->pin);
    jw_arr_end(&w);
    jw_key(&w, "heartbeat_s"); jw_i64(&w, 15);
    jw_key(&w, "command_wait_s"); jw_i64(&w, 10);
    jw_obj_end(&w);
    jw_key(&w, "spool");
    jw_obj(&w);
    jw_key(&w, "dir"); jw_str(&w, (o->spool_dir && *o->spool_dir) ? o->spool_dir : "%ProgramData%\\ALELT\\Agent\\spool");
    jw_key(&w, "max_mb"); jw_i64(&w, 2048);
    jw_obj_end(&w);
    jw_key(&w, "log");
    jw_obj(&w);
    jw_key(&w, "dir"); jw_str(&w, (o->log_dir && *o->log_dir) ? o->log_dir : "%ProgramData%\\ALELT\\Agent\\log");
    jw_key(&w, "level"); jw_str(&w, "INFO");
    jw_key(&w, "max_mb"); jw_i64(&w, 20);
    jw_key(&w, "keep"); jw_i64(&w, 5);
    jw_obj_end(&w);
    jw_key(&w, "limits");
    jw_obj(&w);
    jw_key(&w, "cpu_pct"); jw_i64(&w, 100);
    jw_key(&w, "mem_mb"); jw_i64(&w, 512);
    jw_key(&w, "batch_kb"); jw_i64(&w, 1024);
    jw_key(&w, "max_rate_kbps"); jw_i64(&w, 0);
    jw_key(&w, "poll_ms"); jw_i64(&w, 1000);
    jw_obj_end(&w);
    jw_key(&w, "limits_max");
    jw_obj(&w);
    jw_key(&w, "_comment"); jw_str(&w, "DBA-approved ceilings; hub SET_LIMITS is clamped to these.");
    jw_key(&w, "cpu_pct"); jw_i64(&w, 100);
    jw_key(&w, "mem_mb"); jw_i64(&w, 1024);
    jw_key(&w, "batch_kb"); jw_i64(&w, 8192);
    jw_key(&w, "max_rate_kbps"); jw_i64(&w, 0);
    jw_key(&w, "poll_ms"); jw_i64(&w, 600000);
    jw_obj_end(&w);
    jw_key(&w, "governor");
    jw_obj(&w);
    jw_key(&w, "enabled"); jw_bool(&w, 1);
    jw_key(&w, "interval_s"); jw_i64(&w, 15);
    jw_key(&w, "host_cpu_pause_pct"); jw_i64(&w, 85);
    jw_key(&w, "host_cpu_resume_pct"); jw_i64(&w, 70);
    jw_key(&w, "sustain_samples"); jw_i64(&w, 3);
    jw_key(&w, "max_log_util_pct"); jw_i64(&w, 90);
    jw_obj_end(&w);
    jw_key(&w, "source");
    jw_obj(&w);
    jw_key(&w, "plugin"); jw_str(&w, "aleltsrc_db2.dll");
    jw_key(&w, "engine"); jw_str(&w, "db2");
    jw_key(&w, "database"); jw_str(&w, (o->database && *o->database) ? o->database : default_database());
    jw_key(&w, "dll_dirs");
    jw_arr(&w);
    jw_str(&w, "%DB2PATH%\\BIN");
    jw_arr_end(&w);
    jw_key(&w, "settings");
    jw_obj(&w);
    jw_key(&w, "database"); jw_str(&w, (o->database && *o->database) ? o->database : default_database());
    jw_key(&w, "catalog_schemas");
    jw_arr(&w);
    jw_arr_end(&w);
    jw_obj_end(&w);
    jw_obj_end(&w);
    jw_key(&w, "snapshot");
    jw_obj(&w);
    jw_key(&w, "allowed_tables");
    jw_arr(&w);
    jw_arr_end(&w);
    jw_key(&w, "max_chunk_rows"); jw_i64(&w, 5000);
    jw_obj_end(&w);
    jw_key(&w, "capture");
    jw_obj(&w);
    jw_key(&w, "autostart"); jw_bool(&w, 0);
    jw_obj_end(&w);
    jw_key(&w, "ha");
    jw_obj(&w);
    jw_key(&w, "mode"); jw_str(&w, "standalone");
    jw_key(&w, "lease_ttl_s"); jw_i64(&w, 30);
    jw_key(&w, "renew_s"); jw_i64(&w, 10);
    jw_obj_end(&w);
    jw_obj_end(&w);
    alelt_buf_putc(&b, '\n');
    if (jw_ok(&w) != 0) {
        alelt_buf_free(&b);
        return ERR("init-config: out of memory");
    }
    rc = alelt_write_file_atomic(path, b.data, b.len);
    alelt_buf_free(&b);
    if (rc != 0) return ERR("init-config: cannot write %ls (error %lu)", path, (unsigned long)GetLastError());
    return 0;
}
