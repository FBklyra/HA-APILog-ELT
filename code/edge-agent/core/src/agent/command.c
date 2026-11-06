/* SPDX-License-Identifier: Apache-2.0 */
#include <stdio.h>
#include <string.h>

#include "command.h"
#include "alelt_json.h"
#include "alelt_util.h"

#define REJ(...) (snprintf(err, errcap, __VA_ARGS__), -1)

static const struct {
    const char *s;
    alelt_cmd_name n;
} k_names[] = {
    {"START_CAPTURE", ALELT_CMD_START_CAPTURE}, {"PAUSE", ALELT_CMD_PAUSE},
    {"RESUME", ALELT_CMD_RESUME},               {"SET_LIMITS", ALELT_CMD_SET_LIMITS},
    {"SNAPSHOT_CATALOG", ALELT_CMD_SNAPSHOT_CATALOG}, {"SNAPSHOT_TABLE", ALELT_CMD_SNAPSHOT_TABLE},
    {"PROBE", ALELT_CMD_PROBE},                 {"SHUTDOWN", ALELT_CMD_SHUTDOWN},
    {"VERIFY_TABLE", ALELT_CMD_VERIFY_TABLE},
};

/* Must match alelt_common.models.VERIFY_CHECKS. */
static const char *const k_verify_checks[] = {"row_count", "aggregates", "keys", "rows", NULL};

/* Must match alelt_common.models.PROBES. */
static const char *const k_probes[] = {"current_position", "db_health", "log_config", NULL};

const char *alelt_cmd_str(alelt_cmd_name n)
{
    size_t i;
    for (i = 0; i < sizeof(k_names) / sizeof(k_names[0]); i++) {
        if (k_names[i].n == n) return k_names[i].s;
    }
    return "NONE";
}

int alelt_probe_name_valid(const char *name)
{
    const char *const *p;
    for (p = k_probes; *p; p++) {
        if (strcmp(*p, name) == 0) return 1;
    }
    return 0;
}

static int pos_text_ok(const char *s)
{
    size_t n = strlen(s);
    if (n == 0 || n >= ALELT_POS_MAX) return 0;
    for (; *s; s++) {
        if (*s < 0x21 || *s > 0x7E || *s == '"' || *s == '\\') return 0;
    }
    return 1;
}

static int empty_args(const fj_val *a)
{
    return !a || a->type == FJ_NULL || (a->type == FJ_OBJ && a->n == 0);
}

static int scalar_text(const fj_val *v, char *dst, size_t cap)
{
    if (v->type == FJ_STR) return alelt_strcpy(dst, cap, v->s);
    if (v->type == FJ_NUM) return alelt_strcpy(dst, cap, v->s);
    return -1;
}

static int key_list(const fj_val *v, char (*dst)[ALELT_KEYVAL_MAX], int *n, const char *what, char *err, size_t errcap)
{
    const fj_val *e;
    *n = 0;
    if (!v || v->type == FJ_NULL) return 0;
    if (v->type != FJ_ARR || v->n == 0 || v->n > ALELT_MAX_KEYCOLS)
        return REJ("args.%s must be a non-empty array of at most %d scalars", what, ALELT_MAX_KEYCOLS);
    for (e = v->child; e; e = e->next) {
        if (scalar_text(e, dst[*n], ALELT_KEYVAL_MAX) != 0) return REJ("args.%s values must be short strings or numbers", what);
        (*n)++;
    }
    return 0;
}

static int lim(const fj_val *a, const char *k, int *dst, int lo, int hi, char *err, size_t errcap)
{
    int64_t v;
    int r = fj_get_i64(a, k, &v);
    if (r == -1) {
        *dst = -1;
        return 0;
    }
    if (r != 0 || v < lo || v > hi) return REJ("args.%s must be an integer in [%d, %d]", k, lo, hi);
    *dst = (int)v;
    return 0;
}

/* One key tuple (1..16 short scalars) -> JSON array of strings in w. */
static int verify_key(const fj_val *v, alelt_jw *w, char *err, size_t errcap, const char *what)
{
    const fj_val *e;
    char t[ALELT_KEYVAL_MAX];
    if (!v || v->type != FJ_ARR || v->n == 0 || v->n > ALELT_MAX_KEYCOLS)
        return REJ("%s must be an array of 1..%d scalars", what, ALELT_MAX_KEYCOLS);
    jw_arr(w);
    for (e = v->child; e; e = e->next) {
        if (scalar_text(e, t, sizeof(t)) != 0) return REJ("%s values must be short strings or numbers", what);
        jw_str(w, t);
    }
    jw_arr_end(w);
    return 0;
}

/* VERIFY_TABLE: every field checked against the whitelist, then the args are
 * re-serialized from the parsed values (unknown text never reaches the plugin). */
static int parse_verify(const fj_val *a, alelt_cmd *out, char *err, size_t errcap)
{
    static const char *const ak[] = {"table", "verify_id", "checks", "columns", "ranges", "key_values", "max_keys",
                                     "timeout_s", NULL};
    static const char *const tk[] = {"schema", "name", NULL};
    static const char *const rk[] = {"after", "to", NULL};
    const fj_val *v, *e, *x;
    const char *s, *bad = NULL;
    int64_t n;
    int has_rows = 0, seen[4] = {0, 0, 0, 0}, rc = -1;
    alelt_buf b;
    alelt_jw w;

    if (!a || a->type != FJ_OBJ) return REJ("VERIFY_TABLE needs args");
    if (!fj_keys_only(a, ak, &bad)) return REJ("unknown argument '%s'", bad ? bad : "?");
    v = fj_get(a, "table");
    if (!v || v->type != FJ_OBJ || !fj_keys_only(v, tk, &bad)) return REJ("args.table must be {schema,name}");
    s = fj_get_str(v, "schema");
    if (!s || !alelt_ident_valid(s)) return REJ("invalid table schema");
    alelt_strcpy(out->tab_schema, ALELT_IDENT_MAX, s);
    s = fj_get_str(v, "name");
    if (!s || !alelt_ident_valid(s)) return REJ("invalid table name");
    alelt_strcpy(out->tab_name, ALELT_IDENT_MAX, s);
    s = fj_get_str(a, "verify_id");
    if (!s || !alelt_is_uuid(s)) return REJ("verify_id must be a UUID");
    alelt_strcpy(out->verify_id, sizeof(out->verify_id), s);

    alelt_buf_init(&b, ALELT_VERIFY_ARGS_MAX - 1);
    jw_init(&w, &b);
    jw_obj(&w);
    jw_key(&w, "table");
    jw_obj(&w);
    jw_key(&w, "schema"); jw_str(&w, out->tab_schema);
    jw_key(&w, "name"); jw_str(&w, out->tab_name);
    jw_obj_end(&w);
    jw_key(&w, "verify_id"); jw_str(&w, out->verify_id);

    v = fj_get(a, "checks");
    if (!v || v->type != FJ_ARR || v->n == 0 || v->n > 4) { REJ("checks must be an array of 1..4 names"); goto done; }
    jw_key(&w, "checks");
    jw_arr(&w);
    for (e = v->child; e; e = e->next) {
        int i, hit = -1;
        if (e->type != FJ_STR) { REJ("checks must be strings"); goto done; }
        for (i = 0; k_verify_checks[i]; i++) {
            if (strcmp(k_verify_checks[i], e->s) == 0) hit = i;
        }
        if (hit < 0) { REJ("check '%.32s' is not whitelisted", e->s); goto done; }
        if (seen[hit]++) { REJ("checks must be unique"); goto done; }
        if (hit == 3) has_rows = 1;
        jw_str(&w, k_verify_checks[hit]);
    }
    jw_arr_end(&w);

    v = fj_get(a, "columns");
    if (v && v->type != FJ_NULL) {
        if (v->type != FJ_ARR || v->n == 0 || v->n > ALELT_VERIFY_MAX_COLUMNS) {
            REJ("columns must be an array of 1..%d identifiers", ALELT_VERIFY_MAX_COLUMNS);
            goto done;
        }
        jw_key(&w, "columns");
        jw_arr(&w);
        for (e = v->child; e; e = e->next) {
            if (e->type != FJ_STR || !alelt_ident_valid(e->s)) { REJ("invalid column name"); goto done; }
            jw_str(&w, e->s);
        }
        jw_arr_end(&w);
    }

    v = fj_get(a, "ranges");
    if (v && v->type != FJ_NULL) {
        if (v->type != FJ_ARR || v->n > ALELT_VERIFY_MAX_RANGES) { REJ("ranges must be an array (max %d)", ALELT_VERIFY_MAX_RANGES); goto done; }
        jw_key(&w, "ranges");
        jw_arr(&w);
        for (e = v->child; e; e = e->next) {
            if (e->type != FJ_OBJ || !fj_keys_only(e, rk, &bad)) { REJ("each range is {after, to}"); goto done; }
            jw_obj(&w);
            x = fj_get(e, "after");
            jw_key(&w, "after");
            if (!x || x->type == FJ_NULL) jw_null(&w);
            else if (verify_key(x, &w, err, errcap, "ranges[].after")) goto done;
            x = fj_get(e, "to");
            jw_key(&w, "to");
            if (!x || x->type == FJ_NULL) jw_null(&w);
            else if (verify_key(x, &w, err, errcap, "ranges[].to")) goto done;
            jw_obj_end(&w);
        }
        jw_arr_end(&w);
    }

    v = fj_get(a, "key_values");
    if (has_rows != (v && v->type == FJ_ARR && v->n > 0)) {
        REJ("key_values is required with (and only with) the 'rows' check");
        goto done;
    }
    if (v && v->type != FJ_NULL) {
        if (v->type != FJ_ARR || v->n > ALELT_VERIFY_MAX_KEY_VALUES) {
            REJ("key_values must be an array (max %d)", ALELT_VERIFY_MAX_KEY_VALUES);
            goto done;
        }
        jw_key(&w, "key_values");
        jw_arr(&w);
        for (e = v->child; e; e = e->next) {
            if (verify_key(e, &w, err, errcap, "key_values[]")) goto done;
        }
        jw_arr_end(&w);
    }

    jw_key(&w, "max_keys");
    switch (fj_get_i64(a, "max_keys", &n)) {
    case 0:
        if (n < 1 || n > ALELT_VERIFY_MAX_KEYS) { REJ("max_keys out of range"); goto done; }
        jw_i64(&w, n);
        break;
    case -1: jw_i64(&w, ALELT_VERIFY_MAX_KEYS); break;
    default: REJ("max_keys must be an integer"); goto done;
    }
    jw_key(&w, "timeout_s");
    switch (fj_get_i64(a, "timeout_s", &n)) {
    case 0:
        if (n < 1 || n > ALELT_VERIFY_MAX_TIMEOUT_S) { REJ("timeout_s out of range"); goto done; }
        jw_i64(&w, n);
        break;
    case -1: jw_i64(&w, 300); break;
    default: REJ("timeout_s must be an integer"); goto done;
    }
    jw_obj_end(&w);
    if (jw_ok(&w) != 0 || b.failed || b.len >= sizeof(out->verify_args)) { REJ("VERIFY_TABLE args too large"); goto done; }
    memcpy(out->verify_args, b.data, b.len);
    out->verify_args[b.len] = 0;
    out->verify_len = b.len;
    rc = 0;
done:
    alelt_buf_free(&b);
    return rc;
}

int alelt_cmd_parse(const char *json, size_t len, alelt_cmd *out, char *err, size_t errcap)
{
    fj_doc *d;
    int rc;
    memset(out, 0, sizeof(*out));
    if (len > ALELT_CMD_MAX_JSON) return REJ("command too large");
    d = fj_parse(json, len, err, errcap);
    if (!d) return -1;
    rc = alelt_cmd_from_val(fj_root(d), out, err, errcap);
    fj_free(d);
    return rc;
}

int alelt_cmd_from_val(const fj_val *r, alelt_cmd *out, char *err, size_t errcap)
{
    static const char *const top[] = {"command_id", "name", "args", "issued_at", NULL};
    const fj_val *a, *v;
    const char *s, *bad = NULL;
    size_t i;
    int rc = -1;

    memset(out, 0, sizeof(*out));
    out->limits.cpu_pct = out->limits.mem_mb = out->limits.batch_kb = -1;
    out->limits.max_rate_kbps = out->limits.poll_ms = -1;
    if (!r || r->type != FJ_OBJ) { REJ("command must be a JSON object"); goto out; }

    s = fj_get_str(r, "command_id");
    if (s && alelt_is_uuid(s)) alelt_strcpy(out->command_id, sizeof(out->command_id), s);
    if (!fj_keys_only(r, top, &bad)) { REJ("unknown field '%s'", bad ? bad : "?"); goto out; }
    if (!out->command_id[0]) { REJ("command_id must be a UUID"); goto out; }

    s = fj_get_str(r, "issued_at");
    if (!s || alelt_strcpy(out->issued_at, sizeof(out->issued_at), s) != 0 || strlen(s) < 10) {
        REJ("issued_at must be an ISO-8601 timestamp");
        goto out;
    }
    s = fj_get_str(r, "name");
    if (!s) { REJ("name must be a string"); goto out; }
    for (i = 0; i < sizeof(k_names) / sizeof(k_names[0]); i++) {
        if (strcmp(k_names[i].s, s) == 0) out->name = k_names[i].n;
    }
    if (out->name == ALELT_CMD_NONE) { REJ("command '%.40s' is not whitelisted", s); goto out; }

    a = fj_get(r, "args");
    if (a && a->type != FJ_OBJ && a->type != FJ_NULL) { REJ("args must be an object"); goto out; }

    switch (out->name) {
    case ALELT_CMD_PAUSE:
    case ALELT_CMD_RESUME:
    case ALELT_CMD_SHUTDOWN:
        if (!empty_args(a)) { REJ("%s takes no arguments", alelt_cmd_str(out->name)); goto out; }
        break;

    case ALELT_CMD_START_CAPTURE: {
        static const char *const ak[] = {"from_pos", "discard_spool", NULL};
        const fj_val *ds;
        if (a && a->type == FJ_OBJ && !fj_keys_only(a, ak, &bad)) { REJ("unknown argument '%s'", bad); goto out; }
        ds = fj_get(a, "discard_spool");
        if (ds && ds->type != FJ_NULL) {
            if (ds->type != FJ_TRUE && ds->type != FJ_FALSE) { REJ("discard_spool must be a boolean"); goto out; }
            out->discard_spool = ds->type == FJ_TRUE;
        }
        v = fj_get(a, "from_pos");
        if (v && v->type == FJ_OBJ) {
            static const char *const pk[] = {"engine", "text", NULL};
            if (!fj_keys_only(v, pk, &bad)) { REJ("unknown position field '%s'", bad); goto out; }
            v = fj_get(v, "text");
        }
        if (v && v->type != FJ_NULL) {
            if (v->type != FJ_STR || !pos_text_ok(v->s)) { REJ("from_pos.text invalid"); goto out; }
            alelt_strcpy(out->from_pos, sizeof(out->from_pos), v->s);
            out->has_from_pos = 1;
        }
        if (out->discard_spool && !out->has_from_pos) { REJ("discard_spool needs from_pos"); goto out; }
        break;
    }

    case ALELT_CMD_SET_LIMITS: {
        static const char *const ak[] = {"cpu_pct", "mem_mb", "batch_kb", "max_rate_kbps", "poll_ms", NULL};
        if (empty_args(a)) { REJ("SET_LIMITS needs at least one limit"); goto out; }
        if (!fj_keys_only(a, ak, &bad)) { REJ("unknown argument '%s'", bad); goto out; }
        if (lim(a, "cpu_pct", &out->limits.cpu_pct, 1, 6400, err, errcap)) goto out;
        if (lim(a, "mem_mb", &out->limits.mem_mb, 64, 65536, err, errcap)) goto out;
        if (lim(a, "batch_kb", &out->limits.batch_kb, 64, 65536, err, errcap)) goto out;
        if (lim(a, "max_rate_kbps", &out->limits.max_rate_kbps, 0, 10000000, err, errcap)) goto out;
        if (lim(a, "poll_ms", &out->limits.poll_ms, 50, 600000, err, errcap)) goto out;
        break;
    }

    case ALELT_CMD_SNAPSHOT_CATALOG: {
        static const char *const ak[] = {"schemas", NULL};
        if (a && a->type == FJ_OBJ && !fj_keys_only(a, ak, &bad)) { REJ("unknown argument '%s'", bad); goto out; }
        v = fj_get(a, "schemas");
        if (v && v->type != FJ_NULL) {
            const fj_val *e;
            if (v->type != FJ_ARR || v->n > ALELT_MAX_SCHEMAS) { REJ("schemas must be an array (max %d)", ALELT_MAX_SCHEMAS); goto out; }
            for (e = v->child; e; e = e->next) {
                if (e->type != FJ_STR || !alelt_ident_valid(e->s)) { REJ("invalid schema name"); goto out; }
                alelt_strcpy(out->schemas[out->nschemas++], ALELT_IDENT_MAX, e->s);
            }
        }
        break;
    }

    case ALELT_CMD_SNAPSHOT_TABLE: {
        static const char *const ak[] = {"table", "key_from", "key_to", "chunk_rows", "snapshot_id", "chunk_from", NULL};
        static const char *const tk[] = {"schema", "name", NULL};
        int64_t cr;
        if (!a || a->type != FJ_OBJ) { REJ("SNAPSHOT_TABLE needs args"); goto out; }
        if (!fj_keys_only(a, ak, &bad)) { REJ("unknown argument '%s'", bad); goto out; }
        v = fj_get(a, "table");
        if (!v || v->type != FJ_OBJ || !fj_keys_only(v, tk, &bad)) { REJ("args.table must be {schema,name}"); goto out; }
        s = fj_get_str(v, "schema");
        if (!s || !alelt_ident_valid(s)) { REJ("invalid table schema"); goto out; }
        alelt_strcpy(out->tab_schema, ALELT_IDENT_MAX, s);
        s = fj_get_str(v, "name");
        if (!s || !alelt_ident_valid(s)) { REJ("invalid table name"); goto out; }
        alelt_strcpy(out->tab_name, ALELT_IDENT_MAX, s);
        if (key_list(fj_get(a, "key_from"), out->key_from, &out->nkey_from, "key_from", err, errcap)) goto out;
        if (key_list(fj_get(a, "key_to"), out->key_to, &out->nkey_to, "key_to", err, errcap)) goto out;
        switch (fj_get_i64(a, "chunk_rows", &cr)) {
        case 0:
            if (cr < 1 || cr > 1000000) { REJ("chunk_rows out of range"); goto out; }
            out->chunk_rows = (int)cr;
            break;
        case -1: break;
        default: REJ("chunk_rows must be an integer"); goto out;
        }
        switch (fj_get_i64(a, "chunk_from", &cr)) {
        case 0:
            if (cr < 0 || cr > 10000000) { REJ("chunk_from out of range"); goto out; }
            out->chunk_from = (int)cr;
            break;
        case -1: break;
        default: REJ("chunk_from must be an integer"); goto out;
        }
        s = fj_get_str(a, "snapshot_id");
        if (fj_get(a, "snapshot_id") && fj_get(a, "snapshot_id")->type != FJ_NULL) {
            if (!s || !alelt_is_uuid(s)) { REJ("snapshot_id must be a UUID"); goto out; }
            alelt_strcpy(out->snapshot_id, sizeof(out->snapshot_id), s);
        }
        break;
    }

    case ALELT_CMD_VERIFY_TABLE:
        if (parse_verify(a, out, err, errcap)) goto out;
        break;

    case ALELT_CMD_PROBE: {
        static const char *const ak[] = {"name", NULL};
        if (!a || a->type != FJ_OBJ || !fj_keys_only(a, ak, &bad)) { REJ("PROBE takes only {name}"); goto out; }
        s = fj_get_str(a, "name");
        if (!s || !alelt_probe_name_valid(s)) { REJ("probe '%.40s' is not whitelisted", s ? s : "?"); goto out; }
        alelt_strcpy(out->probe, sizeof(out->probe), s);
        break;
    }

    default:
        REJ("unsupported command");
        goto out;
    }
    rc = 0;
out:
    if (rc != 0) out->name = ALELT_CMD_NONE;
    return rc;
}
