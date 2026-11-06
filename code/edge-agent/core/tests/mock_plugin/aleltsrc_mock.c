/* Mock source plugin (aleltsrc_mock.dll) - test harness only, never shipped.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Plugin ABI 2 without any database: a synthetic log that grows at rate_per_s records per
 * second, Db2 filter-ON framed (so mock_hub / the hub accept the batches), Db2 LRI text
 * positions "1:<16 hex>", one table MOCK.T1 (ID INTEGER key, V VARCHAR) for catalog /
 * snapshot / verify, and injectable faults to drive the agent's error paths:
 *
 * source.settings (aleltagent.json):
 *   rate_per_s        records per second appended to the synthetic log (default 50)
 *   start             first record number (default 256)
 *   fail_open         open() fails with this message (e.g. "SQL30082N ..." -> DB2_AUTH_FAILED)
 *   snapshot_rows     rows of MOCK.T1 (default 1000)
 *   base_unix_ms      wall-clock origin of the log: agents with the same value read the same records (HA tests)
 *   control_file      JSON file re-read at most every 200 ms; keys (all optional):
 *                       log_util (number, db_health probe), position_fail (bool: probe_position fails),
 *                       snapshot_delay_ms, verify_delay_ms (per chunk / per verify)
 * Table MOCK.BADKEY: snapshot_chunk returns nlast_key > ALELT_MAX_KEYCOLS (plugin bug simulation).
 */
#include <windows.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "alelt_buf.h"
#include "alelt_json.h"
#include "alelt_plugin.h"
#include "alelt_util.h"

#define REC_BODY 20u

/* Db2 LRI text form "<part1 decimal>:<part2 16 hex>" (same as the Db2 plugin) */
typedef struct alelt_lri {
    uint64_t part1, part2;
} alelt_lri;

static int alelt_lri_parse(const char *s, alelt_lri *out)
{
    unsigned long long p1, p2;
    char tail;
    size_t n;
    if (!s) return -1;
    n = strlen(s);
    if (n < 18 || s[n - 17] != ':') return -1;
    if (sscanf_s(s, "%llu:%llx%c", &p1, &p2, &tail, 1) != 2) return -1;
    out->part1 = p1;
    out->part2 = p2;
    return 0;
}

static int alelt_lri_format(const alelt_lri *l, char *out, size_t cap)
{
    int n = snprintf(out, cap, "%llu:%016llX", (unsigned long long)l->part1, (unsigned long long)l->part2);
    return (n < 0 || (size_t)n >= cap) ? -1 : 0;
}

static int alelt_lri_cmp(const alelt_lri *a, const alelt_lri *b)
{
    if (a->part1 != b->part1) return a->part1 < b->part1 ? -1 : 1;
    if (a->part2 != b->part2) return a->part2 < b->part2 ? -1 : 1;
    return 0;
} /* record length field included (minimum Db2 log record) */

typedef struct mockctx {
    const alelt_host_api *host;
    uint64_t t0, start;
    int64_t base_unix_ms; /* > 0: shared wall-clock log (two agents see the same records) */
    double rate;
    long long snapshot_rows;
    wchar_t control[MAX_PATH];
    uint64_t control_at;
    double log_util;
    int position_fail;
    long long snapshot_delay_ms, verify_delay_ms;
    char err[512];
} mockctx;

static void control_load(mockctx *c)
{
    alelt_buf b;
    fj_doc *d;
    char e[128];
    int64_t v;
    int bv;
    double dv;
    if (!c->control[0] || alelt_mono_ms() - c->control_at < 200) return;
    c->control_at = alelt_mono_ms();
    alelt_buf_init(&b, 64 * 1024);
    if (alelt_read_file(c->control, &b, 64 * 1024) == 0 && b.len > 0 &&
        (d = fj_parse((const char *)b.data, b.len, e, sizeof(e))) != NULL) {
        const fj_val *r = fj_root(d);
        c->log_util = fj_as_dbl(fj_get(r, "log_util"), &dv) == 0 ? dv : -1.0;
        c->position_fail = fj_get_bool(r, "position_fail", &bv) == 0 ? bv : 0;
        c->snapshot_delay_ms = fj_get_i64(r, "snapshot_delay_ms", &v) == 0 ? v : 0;
        c->verify_delay_ms = fj_get_i64(r, "verify_delay_ms", &v) == 0 ? v : 0;
        fj_free(d);
    }
    alelt_buf_free(&b);
}

static int64_t unix_ms(void)
{
    FILETIME ft;
    ULARGE_INTEGER u;
    GetSystemTimeAsFileTime(&ft);
    u.LowPart = ft.dwLowDateTime;
    u.HighPart = ft.dwHighDateTime;
    return (int64_t)(u.QuadPart / 10000u) - 11644473600000LL;
}

static uint64_t head(const mockctx *c)
{
    if (c->base_unix_ms > 0) {
        int64_t d = unix_ms() - c->base_unix_ms;
        return c->start + (uint64_t)((double)(d > 0 ? d : 0) * c->rate / 1000.0);
    }
    return c->start + (uint64_t)((double)(alelt_mono_ms() - c->t0) * c->rate / 1000.0);
}

static void fmt_pos(uint64_t n, char *out, size_t cap)
{
    alelt_lri l;
    l.part1 = 1;
    l.part2 = n;
    (void)alelt_lri_format(&l, out, cap);
}

static int nap(mockctx *c, long long ms)
{
    while (ms > 0) {
        if (c->host->should_stop && c->host->should_stop()) return -1;
        Sleep(ms > 100 ? 100 : (DWORD)ms);
        ms -= 100;
    }
    return 0;
}

static int out_append(mockctx *c, alelt_host_buf *out, const alelt_buf *b)
{
    return c->host->buf_append(out, b->data ? b->data : (const uint8_t *)"", b->len) == 0 ? ALELT_OK : ALELT_ERR;
}

static int p_open(const alelt_host_api *host, const char *settings_json, void **ctx, char *err, size_t errcap)
{
    mockctx *c;
    fj_doc *d;
    char e[128];
    const char *s;
    int64_t v;
    double dv;
    d = fj_parse(settings_json ? settings_json : "{}", settings_json ? strlen(settings_json) : 2, e, sizeof(e));
    if (!d) {
        snprintf(err, errcap, "mock settings invalid: %s", e);
        return ALELT_ERR;
    }
    if ((s = fj_get_str(fj_root(d), "fail_open")) != NULL) {
        snprintf(err, errcap, "%s", s);
        fj_free(d);
        return ALELT_ERR_RETRY;
    }
    c = (mockctx *)calloc(1, sizeof(*c));
    if (!c) {
        fj_free(d);
        return ALELT_ERR;
    }
    c->host = host;
    c->t0 = alelt_mono_ms();
    c->start = fj_get_i64(fj_root(d), "start", &v) == 0 && v > 0 ? (uint64_t)v : 256u;
    c->rate = fj_as_dbl(fj_get(fj_root(d), "rate_per_s"), &dv) == 0 && dv >= 0 ? dv : 50.0;
    c->snapshot_rows = fj_get_i64(fj_root(d), "snapshot_rows", &v) == 0 && v >= 0 ? v : 1000;
    c->log_util = -1.0;
    c->base_unix_ms = fj_get_i64(fj_root(d), "base_unix_ms", &v) == 0 ? v : 0;
    if ((s = fj_get_str(fj_root(d), "control_file")) != NULL) (void)alelt_to_wide(s, c->control, MAX_PATH);
    fj_free(d);
    control_load(c);
    *ctx = c;
    host->log(ALELT_LOG_INFO, "mock", "mock source opened");
    return ALELT_OK;
}

static int p_probe_position(void *vc, char *pos, size_t cap)
{
    mockctx *c = (mockctx *)vc;
    control_load(c);
    if (c->position_fail) {
        snprintf(c->err, sizeof(c->err), "mock: current log position not available (injected)");
        return ALELT_ERR_RETRY;
    }
    fmt_pos(head(c), pos, cap);
    return ALELT_OK;
}

static int p_read(void *vc, const char *from_pos, uint8_t *buf, size_t max_bytes, alelt_read_result *res)
{
    mockctx *c = (mockctx *)vc;
    alelt_lri f;
    uint64_t h, n, i, cnt;
    size_t rec = 56u + REC_BODY;
    if (alelt_lri_parse(from_pos, &f) != 0) {
        snprintf(c->err, sizeof(c->err), "mock: bad position %s", from_pos);
        return ALELT_ERR;
    }
    h = head(c);
    n = f.part2;
    cnt = n < h ? h - n : 0;
    if (cnt > max_bytes / rec) cnt = max_bytes / rec;
    if (cnt == 0 && n < h) return ALELT_ERR_NOSPACE;
    for (i = 0; i < cnt; i++) {
        uint8_t *p = buf + i * rec;
        uint64_t one = 1, part1 = 1, part2 = n + i;
        uint32_t len = REC_BODY;
        memset(p, 0, rec);
        memcpy(p, &one, 8);
        memcpy(p + 8, &part1, 8);
        memcpy(p + 16, &part2, 8);
        memcpy(p + 56, &len, 4);
        memcpy(p + 60, &part2, 8); /* payload: the record number */
    }
    res->bytes = (size_t)(cnt * rec);
    res->records = (uint32_t)cnt;
    res->at_end = (n + cnt >= h);
    if (cnt) {
        fmt_pos(n, res->first_pos, ALELT_POS_MAX);
        fmt_pos(n + cnt - 1, res->last_pos, ALELT_POS_MAX);
    }
    fmt_pos(n + cnt, res->next_pos, ALELT_POS_MAX);
    return ALELT_OK;
}

static int p_catalog_json(void *vc, const char *const *schemas, size_t nschemas, alelt_host_buf *out)
{
    mockctx *c = (mockctx *)vc;
    alelt_buf b;
    alelt_jw w;
    char pos[ALELT_POS_MAX];
    int rc;
    (void)schemas;
    (void)nschemas;
    fmt_pos(head(c), pos, sizeof(pos));
    alelt_buf_init(&b, 64 * 1024);
    jw_init(&w, &b);
    jw_obj(&w);
    jw_key(&w, "database_codepage"); jw_i64(&w, 1208);
    jw_key(&w, "position"); jw_obj(&w); jw_key(&w, "engine"); jw_str(&w, "db2"); jw_key(&w, "text"); jw_str(&w, pos); jw_obj_end(&w);
    jw_key(&w, "tables"); jw_arr(&w);
    jw_obj(&w);
    jw_key(&w, "table"); jw_obj(&w); jw_key(&w, "schema"); jw_str(&w, "MOCK"); jw_key(&w, "name"); jw_str(&w, "T1"); jw_obj_end(&w);
    jw_key(&w, "engine_ids"); jw_obj(&w); jw_key(&w, "tbspaceid"); jw_i64(&w, 2); jw_key(&w, "tableid"); jw_i64(&w, 4); jw_obj_end(&w);
    jw_key(&w, "capture_enabled"); jw_bool(&w, 1);
    jw_key(&w, "compression"); jw_str(&w, "N");
    jw_key(&w, "columns"); jw_arr(&w);
    jw_obj(&w);
    jw_key(&w, "name"); jw_str(&w, "ID"); jw_key(&w, "ordinal"); jw_i64(&w, 0); jw_key(&w, "engine_type"); jw_str(&w, "INTEGER");
    jw_key(&w, "canonical_type"); jw_str(&w, "INT"); jw_key(&w, "length"); jw_i64(&w, 4); jw_key(&w, "scale"); jw_i64(&w, 0);
    jw_key(&w, "nullable"); jw_bool(&w, 0); jw_key(&w, "key_seq"); jw_i64(&w, 1); jw_key(&w, "codepage"); jw_i64(&w, 0);
    jw_key(&w, "default"); jw_null(&w);
    jw_obj_end(&w);
    jw_obj(&w);
    jw_key(&w, "name"); jw_str(&w, "V"); jw_key(&w, "ordinal"); jw_i64(&w, 1); jw_key(&w, "engine_type"); jw_str(&w, "VARCHAR");
    jw_key(&w, "canonical_type"); jw_str(&w, "STRING"); jw_key(&w, "length"); jw_i64(&w, 40); jw_key(&w, "scale"); jw_i64(&w, 0);
    jw_key(&w, "nullable"); jw_bool(&w, 1); jw_key(&w, "key_seq"); jw_null(&w); jw_key(&w, "codepage"); jw_i64(&w, 1208);
    jw_key(&w, "default"); jw_null(&w);
    jw_obj_end(&w);
    jw_arr_end(&w);
    jw_obj_end(&w);
    jw_arr_end(&w);
    jw_obj_end(&w);
    rc = jw_ok(&w) == 0 ? out_append(c, out, &b) : ALELT_ERR;
    alelt_buf_free(&b);
    return rc;
}

static int p_snapshot_chunk(void *vc, const alelt_snapshot_req *req, alelt_host_buf *out, alelt_snapshot_res *res)
{
    mockctx *c = (mockctx *)vc;
    long long from = 0, to, i;
    alelt_buf b;
    int rc = ALELT_OK;
    control_load(c);
    if (strcmp(req->schema, "MOCK") != 0) {
        snprintf(c->err, sizeof(c->err), "mock: unknown table %s.%s", req->schema, req->table);
        return ALELT_ERR_NOTFOUND;
    }
    if (strcmp(req->table, "BADKEY") == 0) { /* plugin bug: more key columns than the ABI allows */
        res->rows = 1;
        res->last = 0;
        res->nlast_key = ALELT_MAX_KEYCOLS + 1;
        return c->host->buf_append(out, "{\"ID\":1}\n", 9) == 0 ? ALELT_OK : ALELT_ERR;
    }
    if (strcmp(req->table, "T1") != 0) {
        snprintf(c->err, sizeof(c->err), "mock: unknown table %s.%s", req->schema, req->table);
        return ALELT_ERR_NOTFOUND;
    }
    if (c->snapshot_delay_ms > 0 && nap(c, c->snapshot_delay_ms) != 0) {
        snprintf(c->err, sizeof(c->err), "stopping");
        return ALELT_ERR;
    }
    if (req->nkey_after > 0) from = strtoll(req->key_after[0], NULL, 10);
    to = from + (long long)req->chunk_rows;
    if (to > c->snapshot_rows) to = c->snapshot_rows;
    alelt_buf_init(&b, (size_t)64 * 1024 * 1024);
    for (i = from + 1; i <= to; i++) alelt_buf_printf(&b, "{\"ID\":%lld,\"V\":\"r%lld\"}\n", i, i);
    if (b.failed) rc = ALELT_ERR;
    if (rc == ALELT_OK) rc = out_append(c, out, &b);
    alelt_buf_free(&b);
    res->rows = (uint32_t)(to > from ? to - from : 0);
    res->last = to >= c->snapshot_rows;
    res->nlast_key = res->rows ? 1 : 0;
    if (res->rows) snprintf(res->last_key[0], ALELT_KEYVAL_MAX, "%lld", to);
    return rc;
}

static int p_probe(void *vc, const char *name, alelt_host_buf *out)
{
    mockctx *c = (mockctx *)vc;
    char t[256];
    control_load(c);
    if (strcmp(name, "db_health") == 0) {
        if (c->log_util >= 0) snprintf(t, sizeof(t), "{\"log_utilization_pct\":%.1f}", c->log_util);
        else snprintf(t, sizeof(t), "{\"log_utilization_pct\":null}");
    } else if (strcmp(name, "server_info") == 0) {
        snprintf(t, sizeof(t), "{\"host\":\"mockhost\",\"server_name\":\"MOCK\",\"database\":\"MOCKDB\"}");
    } else if (strcmp(name, "log_config") == 0) {
        snprintf(t, sizeof(t), "{\"mock\":true}");
    } else {
        snprintf(c->err, sizeof(c->err), "mock: unknown probe %s", name);
        return ALELT_ERR_DENIED;
    }
    return c->host->buf_append(out, t, strlen(t)) == 0 ? ALELT_OK : ALELT_ERR;
}

static int p_verify_table(void *vc, const alelt_verify_req *req, alelt_host_buf *out)
{
    mockctx *c = (mockctx *)vc;
    char t[256];
    control_load(c);
    if (strcmp(req->schema, "MOCK") != 0 || strcmp(req->table, "T1") != 0) {
        snprintf(c->err, sizeof(c->err), "mock: unknown table %s.%s", req->schema, req->table);
        return ALELT_ERR_NOTFOUND;
    }
    if (c->verify_delay_ms > 0 && nap(c, c->verify_delay_ms) != 0) {
        snprintf(c->err, sizeof(c->err), "stopping");
        return ALELT_ERR;
    }
    snprintf(t, sizeof(t), "{\"mock\":true,\"rows\":%lld}", c->snapshot_rows);
    return c->host->buf_append(out, t, strlen(t)) == 0 ? ALELT_OK : ALELT_ERR;
}

static int p_compare_pos(const char *a, const char *b, int *result)
{
    alelt_lri x, y;
    if (alelt_lri_parse(a, &x) != 0 || alelt_lri_parse(b, &y) != 0) return ALELT_ERR;
    *result = alelt_lri_cmp(&x, &y);
    return ALELT_OK;
}

static int p_pos_valid(const char *pos)
{
    alelt_lri l;
    return alelt_lri_parse(pos, &l) == 0;
}

static const char *p_last_error(void *vc)
{
    return ((mockctx *)vc)->err;
}

static void p_close(void *vc)
{
    free(vc);
}

static const alelt_source_vtbl k_vtbl = {
    ALELT_PLUGIN_ABI_VERSION, "db2", "mock-0.1",
    p_open, p_probe_position, p_read, p_catalog_json, p_snapshot_chunk, p_probe,
    p_compare_pos, p_pos_valid, p_last_error, p_close, p_verify_table,
};

__declspec(dllexport) const alelt_source_vtbl *alelt_source_get_vtbl(void)
{
    return &k_vtbl;
}
