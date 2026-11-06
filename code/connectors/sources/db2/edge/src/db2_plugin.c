/* SPDX-License-Identifier: Apache-2.0
 * ALELT Db2 LUW source plugin (aleltsrc_db2.dll) - implements alelt_source_vtbl.
 *
 * Written from the documented Db2 CLI and db2ReadLog APIs (headers come from
 * the installed Db2 client, never vendored). Verified facts: DESIGN.md 8 V1-V9.
 *
 * Safety (R1): one CLI connection, READ ONLY access mode, cursor-stability
 * isolation, autocommit on, and only the compiled-in statements of
 * db2_sql.c (catalog/monitor, WITH UR) plus the snapshot and VERIFY_TABLE
 * SELECTs of db2_build.c (catalog-validated quoted identifiers, parameter
 * markers, WITH CS: no dirty reads - F-04; CUR_COMMIT=ON keeps readers from
 * waiting on writers, see the prerequisites in the README). A COMMIT precedes every db2ReadLog call
 * so the reader never holds a unit of work (V2).
 */
/* Native Db2 CLI (db2cli.lib -> db2app64.dll) uses 32-bit SQLLEN/SQLULEN on
 * Win64; ODBC64 is only for the ODBC driver (libdb2o). Verified on 11.5: a NULL
 * indicator arrives as a 32-bit -1. Do NOT define ODBC64 / CLI_WIN64 here. */
#if defined(ODBC64) || defined(CLI_WIN64)
#error "aleltsrc_db2 must be built without ODBC64/CLI_WIN64 (native Db2 CLI uses 32-bit SQLLEN)"
#endif
/* The plugin uses the ANSI CLI entry points for its ASCII-only statements and
 * identifiers; data values are fetched as SQL_C_WCHAR explicitly. */
#undef UNICODE
#undef _UNICODE
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

#include <sqlcli1.h>
#include <sqlca.h>
#include <db2ApiDf.h>

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db2_build.h"
#include "db2_lri.h"
#include "db2_sql.h"
#include "alelt_buf.h"
#include "alelt_json.h"
#include "alelt_plugin.h"
#include "alelt_util.h"

#define PLUGIN_VERSION "0.2.0"
/* Structure version verified against a live Db2 11.5 database. */
#define ALELT_DB2_READLOG_VERSION db2Version1010
#define MAX_SCHEMAS 64
#define MAX_COLS 1012           /* Db2 LUW column limit */
#define MAX_VALUE ((size_t)32 * 1024 * 1024)
#define MAX_CHUNK_BYTES ((size_t)32 * 1024 * 1024)
#define ALELT_VERIFY_DEFAULT_KEYS 10000

typedef struct db2ctx {
    const alelt_host_api *host;
    SQLHANDLE env, dbc;
    int connected;
    char database[129];
    int nschemas;
    char schemas[MAX_SCHEMAS][ALELT_IDENT_MAX];
    char err[512];
} db2ctx;

/* ------------------------------------------------------------------ logging / errors */
static void plog(db2ctx *c, int lvl, const char *fmt, ...)
{
    char m[768];
    va_list ap;
    va_start(ap, fmt);
    (void)vsnprintf(m, sizeof(m), fmt, ap);
    va_end(ap);
    m[sizeof(m) - 1] = 0;
    if (c && c->host && c->host->log) c->host->log(lvl, "db2", m);
}

/* Returns ALELT_ERR_RETRY for connection-class failures, ALELT_ERR otherwise. */
static int cli_fail(db2ctx *c, SQLSMALLINT htype, SQLHANDLE h, const char *what)
{
    SQLCHAR state[6] = "", text[400] = "";
    SQLINTEGER native = 0;
    SQLSMALLINT len = 0;
    if (h) (void)SQLGetDiagRec(htype, h, 1, state, &native, text, (SQLSMALLINT)sizeof(text), &len);
    snprintf(c->err, sizeof(c->err), "%s: SQLSTATE %s native %ld %s", what, (const char *)state, (long)native,
             (const char *)text);
    if (strncmp((const char *)state, "08", 2) == 0 || native == -30081 || native == -1224 || native == -1776 ||
        native == -900 || native == -1032 || native == -30108) {
        c->connected = 0;
        return ALELT_ERR_RETRY;
    }
    return ALELT_ERR;
}

#define OK(r) ((r) == SQL_SUCCESS || (r) == SQL_SUCCESS_WITH_INFO)

/* ------------------------------------------------------------------ connection */
static void disconnect(db2ctx *c)
{
    if (c->dbc) {
        if (c->connected) {
            (void)SQLEndTran(SQL_HANDLE_DBC, c->dbc, SQL_COMMIT);
            (void)SQLDisconnect(c->dbc);
        }
        (void)SQLFreeHandle(SQL_HANDLE_DBC, c->dbc);
        c->dbc = NULL;
    }
    if (c->env) {
        (void)SQLFreeHandle(SQL_HANDLE_ENV, c->env);
        c->env = NULL;
    }
    c->connected = 0;
}

static int connect_db(db2ctx *c)
{
    SQLRETURN r;
    if (c->connected) return ALELT_OK;
    disconnect(c);
    r = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &c->env);
    if (!OK(r)) { snprintf(c->err, sizeof(c->err), "SQLAllocHandle(ENV) failed"); return ALELT_ERR_RETRY; }
    (void)SQLSetEnvAttr(c->env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    r = SQLAllocHandle(SQL_HANDLE_DBC, c->env, &c->dbc);
    if (!OK(r)) return cli_fail(c, SQL_HANDLE_ENV, c->env, "SQLAllocHandle(DBC)");
    (void)SQLSetConnectAttr(c->dbc, SQL_ATTR_INFO_APPLNAME, (SQLPOINTER) "ALELTAgent", SQL_NTS);
    /* Implicit OS authentication as the service account: no credentials in config (R9). */
    r = SQLConnect(c->dbc, (SQLCHAR *)c->database, SQL_NTS, NULL, 0, NULL, 0);
    if (!OK(r)) {
        int rc = cli_fail(c, SQL_HANDLE_DBC, c->dbc, "SQLConnect");
        disconnect(c);
        return rc == ALELT_ERR ? ALELT_ERR_RETRY : rc;
    }
    c->connected = 1;
    r = SQLSetConnectAttr(c->dbc, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_ONLY, 0);
    if (!OK(r)) plog(c, ALELT_LOG_WARN, "could not set READ ONLY access mode (continuing; SQL table is SELECT-only)");
    /* default CS; every statement names its isolation (catalog WITH UR, table data WITH CS) */
    r = SQLSetConnectAttr(c->dbc, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER)SQL_TXN_READ_COMMITTED, 0);
    if (!OK(r)) plog(c, ALELT_LOG_WARN, "could not set CS isolation (statements name their isolation)");
    (void)SQLSetConnectAttr(c->dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
    plog(c, ALELT_LOG_INFO, "connected to %s via Db2 CLI (read-only)", c->database);
    return ALELT_OK;
}

static void commit(db2ctx *c)
{
    if (c->connected) (void)SQLEndTran(SQL_HANDLE_DBC, c->dbc, SQL_COMMIT);
}

/* ------------------------------------------------------------------ statements */
typedef struct stmt {
    SQLHANDLE h;
} stmt;

static int st_open(db2ctx *c, stmt *s, const char *sql, const char *const *params, int nparams)
{
    SQLRETURN r;
    int i;
    s->h = NULL;
    r = SQLAllocHandle(SQL_HANDLE_STMT, c->dbc, &s->h);
    if (!OK(r)) return cli_fail(c, SQL_HANDLE_DBC, c->dbc, "SQLAllocHandle(STMT)");
    r = SQLPrepare(s->h, (SQLCHAR *)sql, SQL_NTS);
    if (!OK(r)) {
        int rc = cli_fail(c, SQL_HANDLE_STMT, s->h, "SQLPrepare");
        SQLFreeHandle(SQL_HANDLE_STMT, s->h);
        s->h = NULL;
        return rc;
    }
    for (i = 0; i < nparams; i++) {
        /* identifiers only (validated by the caller); bound as input VARCHAR */
        SQLULEN n = (SQLULEN)strlen(params[i]);
        r = SQLBindParameter(s->h, (SQLUSMALLINT)(i + 1), SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, n ? n : 1, 0,
                             (SQLPOINTER)params[i], (SQLLEN)n, NULL);
        if (!OK(r)) {
            int rc = cli_fail(c, SQL_HANDLE_STMT, s->h, "SQLBindParameter");
            SQLFreeHandle(SQL_HANDLE_STMT, s->h);
            s->h = NULL;
            return rc;
        }
    }
    r = SQLExecute(s->h);
    if (!OK(r) && r != SQL_NO_DATA) {
        int rc = cli_fail(c, SQL_HANDLE_STMT, s->h, "SQLExecute");
        SQLFreeHandle(SQL_HANDLE_STMT, s->h);
        s->h = NULL;
        return rc;
    }
    return ALELT_OK;
}

static void st_close(stmt *s)
{
    if (s->h) {
        (void)SQLCloseCursor(s->h);
        (void)SQLFreeHandle(SQL_HANDLE_STMT, s->h);
        s->h = NULL;
    }
}

/* 1 = row, 0 = end, <0 error */
static int st_fetch(db2ctx *c, stmt *s)
{
    SQLRETURN r = SQLFetch(s->h);
    if (r == SQL_NO_DATA) return 0;
    if (!OK(r)) return cli_fail(c, SQL_HANDLE_STMT, s->h, "SQLFetch");
    return 1;
}

/* Column as UTF-8 (via UTF-16 to be independent of the client code page).
 * Returns 1 = value, 0 = NULL, <0 error. Value appended to out. */
static int st_get(db2ctx *c, stmt *s, int col, alelt_buf *out)
{
    SQLWCHAR wbuf[4096];
    SQLLEN ind = 0;
    SQLRETURN r;
    size_t total = 0;
    for (;;) {
        r = SQLGetData(s->h, (SQLUSMALLINT)col, SQL_C_WCHAR, wbuf, (SQLLEN)sizeof(wbuf), &ind);
        if (r == SQL_NO_DATA) break;
        if (!OK(r)) return cli_fail(c, SQL_HANDLE_STMT, s->h, "SQLGetData");
        if (ind == SQL_NULL_DATA) return 0;
        {
            size_t nchars;
            char u8[4096 * 3 + 4];
            int n;
            if (ind == SQL_NO_TOTAL || ind >= (SQLLEN)sizeof(wbuf)) nchars = sizeof(wbuf) / sizeof(SQLWCHAR) - 1;
            else nchars = (size_t)ind / sizeof(SQLWCHAR);
            /* do not split a surrogate pair across pieces */
            n = WideCharToMultiByte(CP_UTF8, 0, (LPCWCH)wbuf, (int)nchars, u8, (int)sizeof(u8), NULL, NULL);
            if (n < 0 || (nchars > 0 && n == 0)) {
                snprintf(c->err, sizeof(c->err), "UTF-16 to UTF-8 conversion failed");
                return ALELT_ERR;
            }
            total += (size_t)n;
            if (total > MAX_VALUE) {
                snprintf(c->err, sizeof(c->err), "value larger than %zu bytes", MAX_VALUE);
                return ALELT_ERR;
            }
            if (alelt_buf_append(out, u8, (size_t)n) != 0) {
                snprintf(c->err, sizeof(c->err), "out of memory");
                return ALELT_ERR;
            }
        }
        if (r == SQL_SUCCESS) break; /* last piece */
    }
    if (out->len == 0 && out->data == NULL) alelt_buf_append(out, "", 0);
    return 1;
}

static int st_get_str(db2ctx *c, stmt *s, int col, char *dst, size_t cap)
{
    alelt_buf b;
    int r;
    size_t n;
    alelt_buf_init(&b, MAX_VALUE + 16);
    r = st_get(c, s, col, &b);
    dst[0] = 0;
    if (r == 1) {
        n = b.len;
        while (n > 0 && b.data[n - 1] == ' ') n--; /* CHAR padding in catalog columns */
        if (n >= cap) n = cap - 1;
        memcpy(dst, b.data, n);
        dst[n] = 0;
    }
    alelt_buf_free(&b);
    return r;
}

static int st_get_i64(db2ctx *c, stmt *s, int col, long long *v, int *is_null)
{
    SQLBIGINT x = 0;
    SQLLEN ind = 0;
    SQLRETURN r = SQLGetData(s->h, (SQLUSMALLINT)col, SQL_C_SBIGINT, &x, sizeof(x), &ind);
    if (!OK(r)) return cli_fail(c, SQL_HANDLE_STMT, s->h, "SQLGetData(int)");
    *is_null = (ind == SQL_NULL_DATA);
    *v = (long long)x;
    return ALELT_OK;
}

/* ------------------------------------------------------------------ positions */
static int lri_to_text(const db2LRI *l, char *out, size_t cap)
{
    alelt_lri x;
    x.part1 = l->part1;
    x.part2 = l->part2;
    return alelt_lri_format(&x, out, cap);
}

static int pos_valid(const char *pos)
{
    alelt_lri l;
    return alelt_lri_parse(pos, &l) == 0;
}

static int compare_pos(const char *a, const char *b, int *result)
{
    alelt_lri x, y;
    if (alelt_lri_parse(a, &x) != 0 || alelt_lri_parse(b, &y) != 0) return ALELT_ERR;
    *result = alelt_lri_cmp(&x, &y);
    return ALELT_OK;
}

static int sqlca_fail(db2ctx *c, const struct sqlca *ca, const char *what)
{
    int n = ca->sqlerrml;
    if (n < 0) n = 0;
    if (n > 70) n = 70;
    snprintf(c->err, sizeof(c->err), "%s: SQLCODE %ld %.*s", what, (long)ca->sqlcode, n, ca->sqlerrmc);
    if (ca->sqlcode == -1224 || ca->sqlcode == -30081 || ca->sqlcode == -1024 || ca->sqlcode == -900) {
        c->connected = 0;
        return ALELT_ERR_RETRY;
    }
    return ALELT_ERR;
}

static int rl_query(db2ctx *c, db2ReadLogInfoStruct *info)
{
    db2ReadLogStruct req;
    struct sqlca ca;
    int rc = connect_db(c);
    if (rc != ALELT_OK) return rc;
    commit(c);
    memset(&req, 0, sizeof(req));
    memset(info, 0, sizeof(*info));
    memset(&ca, 0, sizeof(ca));
    req.iCallerAction = DB2READLOG_QUERY;
    req.iFilterOption = DB2READLOG_FILTER_ON;
    req.poReadLogInfo = info;
    (void)db2ReadLog(ALELT_DB2_READLOG_VERSION, &req, &ca);
    commit(c);
    if (ca.sqlcode < 0) return sqlca_fail(c, &ca, "db2ReadLog QUERY");
    return ALELT_OK;
}

static int p_probe_position(void *vc, char *pos, size_t cap)
{
    db2ctx *c = (db2ctx *)vc;
    db2ReadLogInfoStruct info;
    int rc = rl_query(c, &info);
    if (rc != ALELT_OK) return rc;
    if (lri_to_text(&info.nextStartLRI, pos, cap) != 0) return ALELT_ERR;
    return ALELT_OK;
}

static int p_read(void *vc, const char *from_pos, uint8_t *buf, size_t max_bytes, alelt_read_result *res)
{
    db2ctx *c = (db2ctx *)vc;
    db2ReadLogStruct req;
    db2ReadLogInfoStruct info;
    db2LRI start, end;
    struct sqlca ca;
    alelt_lri f;
    int rc;

    memset(res, 0, sizeof(*res));
    if (alelt_lri_parse(from_pos, &f) != 0) {
        snprintf(c->err, sizeof(c->err), "invalid start position '%.63s'", from_pos);
        return ALELT_ERR;
    }
    if (max_bytes > 0xFFFFFFFFu) max_bytes = 0xFFFFFFFFu;
    rc = connect_db(c);
    if (rc != ALELT_OK) return rc;
    commit(c);
    start.lriType = DB2READLOG_LRI_1;
    start.part1 = f.part1;
    start.part2 = f.part2;
    /* (1, FFFF.., FFFF..) = "end of the current log" (documented sentinel) */
    end.lriType = DB2READLOG_LRI_1;
    end.part1 = 0xFFFFFFFFFFFFFFFFull;
    end.part2 = 0xFFFFFFFFFFFFFFFFull;
    memset(&req, 0, sizeof(req));
    memset(&info, 0, sizeof(info));
    memset(&ca, 0, sizeof(ca));
    req.iCallerAction = DB2READLOG_READ;
    req.piStartLRI = &start;
    req.piEndLRI = &end;
    req.poLogBuffer = (char *)buf;
    req.iLogBufferSize = (db2Uint32)max_bytes;
    req.iFilterOption = DB2READLOG_FILTER_ON;
    req.poReadLogInfo = &info;
    (void)db2ReadLog(ALELT_DB2_READLOG_VERSION, &req, &ca);
    commit(c);
    if (ca.sqlcode < 0) {
        if (ca.sqlcode == -2650) {
            /* SQL2650N carries a reason code token in sqlerrmc: always reported (SRC-db2-11) */
            char reason[72];
            int n = ca.sqlerrml < 0 ? 0 : (ca.sqlerrml > 70 ? 70 : ca.sqlerrml), i;
            memcpy(reason, ca.sqlerrmc, (size_t)n);
            reason[n] = 0;
            for (i = 0; i < n; i++) {
                if ((unsigned char)reason[i] == 0xFF || (unsigned char)reason[i] < 0x20) reason[i] = ' ';
            }
            if (info.logRecsWritten == 0 && max_bytes < (size_t)64 * 1024 * 1024) {
                snprintf(c->err, sizeof(c->err), "db2ReadLog READ: SQLCODE -2650 tokens [%s] (retried with a larger "
                         "buffer up to 64 MiB)", reason);
                return ALELT_ERR_NOSPACE;
            }
            snprintf(c->err, sizeof(c->err), "db2ReadLog READ: SQLCODE -2650 tokens [%s]", reason);
            return ALELT_ERR;
        }
        return sqlca_fail(c, &ca, "db2ReadLog READ");
    }
    if (ca.sqlcode == 2653) { /* SRC-db2-11: surfaced to the hub as an agent event */
        plog(c, ALELT_LOG_WARN, "db2ReadLog: log sequence numbers reused (restore/rollforward)");
        res->flags |= ALELT_READ_LOG_REUSED;
    }
    res->at_end = (ca.sqlcode == 2654); /* SQLU_RLOG_READ_TO_CURRENT */
    if (info.logBytesWritten > max_bytes) {
        snprintf(c->err, sizeof(c->err), "db2ReadLog wrote %lu bytes into a %zu byte buffer",
                 (unsigned long)info.logBytesWritten, max_bytes);
        return ALELT_ERR;
    }
    if (info.logRecsWritten > 0) {
        alelt_lri first, last;
        uint32_t bad = 0;
        if (alelt_db2_frame_walk(buf, info.logBytesWritten, info.logRecsWritten, &first, &last, &bad, c->err,
                              sizeof(c->err)) != 0)
            return ALELT_ERR;
        if (bad) plog(c, ALELT_LOG_WARN, "%lu record(s) with decompression sqlcode (shipped raw; hub quarantines)", (unsigned long)bad);
        if (alelt_lri_format(&first, res->first_pos, ALELT_POS_MAX) != 0 || alelt_lri_format(&last, res->last_pos, ALELT_POS_MAX) != 0)
            return ALELT_ERR;
    } else if (info.logBytesWritten != 0) {
        snprintf(c->err, sizeof(c->err), "db2ReadLog returned bytes without records");
        return ALELT_ERR;
    }
    if (lri_to_text(&info.nextStartLRI, res->next_pos, ALELT_POS_MAX) != 0) return ALELT_ERR;
    res->bytes = info.logBytesWritten;
    res->records = info.logRecsWritten;
    return ALELT_OK;
}

/* ------------------------------------------------------------------ catalog */
typedef struct colrow {
    char tab[ALELT_IDENT_MAX * 2];
    char name[ALELT_IDENT_MAX * 2];
    char type[ALELT_IDENT_MAX];
    long long colno, length, scale, keyseq, codepage;
    int key_null;
    char nulls[4];
    alelt_buf def;
    int def_null;
} colrow;

static int col_fetch(db2ctx *c, stmt *s, colrow *r)
{
    int f = st_fetch(c, s), n, rc;
    if (f <= 0) return f;
    if ((rc = st_get_str(c, s, 1, r->tab, sizeof(r->tab))) < 0) return rc;
    if ((rc = st_get_str(c, s, 2, r->name, sizeof(r->name))) < 0) return rc;
    if ((rc = st_get_i64(c, s, 3, &r->colno, &n)) < 0) return rc;
    if ((rc = st_get_str(c, s, 4, r->type, sizeof(r->type))) < 0) return rc;
    if ((rc = st_get_i64(c, s, 5, &r->length, &n)) < 0) return rc;
    if ((rc = st_get_i64(c, s, 6, &r->scale, &n)) < 0) return rc;
    if ((rc = st_get_str(c, s, 7, r->nulls, sizeof(r->nulls))) < 0) return rc;
    if ((rc = st_get_i64(c, s, 8, &r->keyseq, &r->key_null)) < 0) return rc;
    if ((rc = st_get_i64(c, s, 9, &r->codepage, &n)) < 0) return rc;
    alelt_buf_reset(&r->def);
    rc = st_get(c, s, 10, &r->def);
    if (rc < 0) return rc;
    r->def_null = (rc == 0);
    return 1;
}

static void emit_column(alelt_jw *w, const colrow *r)
{
    jw_obj(w);
    jw_key(w, "name"); jw_str(w, r->name);
    jw_key(w, "ordinal"); jw_i64(w, r->colno);
    jw_key(w, "engine_type"); jw_str(w, r->type);
    jw_key(w, "canonical_type"); jw_str(w, db2_canonical_type(r->type, r->codepage));
    jw_key(w, "length"); jw_i64(w, r->length);
    jw_key(w, "scale"); jw_i64(w, r->scale);
    jw_key(w, "nullable"); jw_bool(w, r->nulls[0] == 'Y');
    jw_key(w, "key_seq");
    if (r->key_null || r->keyseq == 0) jw_null(w); else jw_i64(w, r->keyseq);
    jw_key(w, "codepage"); jw_i64(w, r->codepage);
    jw_key(w, "default");
    if (r->def_null) jw_null(w); else jw_strn(w, (const char *)alelt_buf_cstr(&r->def), r->def.len);
    jw_obj_end(w);
}

/* Key fallback of one schema (SRC-db2-07): tables without a primary key get the
 * columns of their first unique index (by index name) as key, like the hub /
 * dev-edge catalog. */
typedef struct ukey {
    char tab[ALELT_IDENT_MAX * 2];
    char col[ALELT_IDENT_MAX * 2];
    long long seq;
} ukey;

typedef struct upart {
    char tab[ALELT_IDENT_MAX * 2];
    long long dpid, id; /* id = TBSPACEID * 65536 + PARTITIONOBJECTID */
} upart;

typedef struct ukeys {
    ukey *k;
    int n, cap;
    upart *pt;
    int npt, cappt;
    char (*pk)[ALELT_IDENT_MAX * 2];
    int npk, cappk;
} ukeys;

static void ukeys_free(ukeys *u)
{
    free(u->k);
    free(u->pk);
    free(u->pt);
    memset(u, 0, sizeof(*u));
}

static int ukeys_load(db2ctx *c, const char *schema, ukeys *u)
{
    stmt s;
    const char *p[1];
    char tab[ALELT_IDENT_MAX * 2], ind[ALELT_IDENT_MAX * 2], last_tab[ALELT_IDENT_MAX * 2] = "", first_ind[ALELT_IDENT_MAX * 2] = "";
    int rc, f, isnull;
    memset(u, 0, sizeof(*u));
    p[0] = schema;
    rc = st_open(c, &s, db2_sql[Q_PK_TABLES], p, 1);
    if (rc != ALELT_OK) return rc;
    while ((f = st_fetch(c, &s)) == 1) {
        if (u->npk == u->cappk) {
            int nc = u->cappk ? u->cappk * 2 : 256;
            void *q = realloc(u->pk, (size_t)nc * sizeof(*u->pk));
            if (!q) { f = ALELT_ERR; break; }
            u->pk = q;
            u->cappk = nc;
        }
        if ((rc = st_get_str(c, &s, 1, u->pk[u->npk], sizeof(u->pk[0]))) < 0) { f = rc; break; }
        u->npk++;
    }
    st_close(&s);
    if (f < 0) { commit(c); return f; }
    rc = st_open(c, &s, db2_sql[Q_PARTS], p, 1);
    if (rc != ALELT_OK) return rc;
    while ((f = st_fetch(c, &s)) == 1) {
        upart q;
        long long tb = 0, ob = 0;
        if ((rc = st_get_str(c, &s, 1, q.tab, sizeof(q.tab))) < 0 || (rc = st_get_i64(c, &s, 2, &q.dpid, &isnull)) < 0 ||
            (rc = st_get_i64(c, &s, 3, &tb, &isnull)) < 0 || (rc = st_get_i64(c, &s, 4, &ob, &isnull)) < 0) {
            f = rc;
            break;
        }
        q.id = tb * 65536 + ob;
        if (u->npt == u->cappt) {
            int nc = u->cappt ? u->cappt * 2 : 256;
            void *nq = realloc(u->pt, (size_t)nc * sizeof(*u->pt));
            if (!nq) { f = ALELT_ERR; break; }
            u->pt = nq;
            u->cappt = nc;
        }
        u->pt[u->npt++] = q;
    }
    st_close(&s);
    if (f < 0) { commit(c); return f; }
    rc = st_open(c, &s, db2_sql[Q_UNIQUE_KEYS], p, 1);
    if (rc != ALELT_OK) return rc;
    while ((f = st_fetch(c, &s)) == 1) {
        ukey k;
        if ((rc = st_get_str(c, &s, 1, tab, sizeof(tab))) < 0 || (rc = st_get_str(c, &s, 2, ind, sizeof(ind))) < 0 ||
            (rc = st_get_str(c, &s, 3, k.col, sizeof(k.col))) < 0 || (rc = st_get_i64(c, &s, 4, &k.seq, &isnull)) < 0) {
            f = rc;
            break;
        }
        if (strcmp(tab, last_tab) != 0) {
            alelt_strcpy(last_tab, sizeof(last_tab), tab);
            alelt_strcpy(first_ind, sizeof(first_ind), ind);
        }
        if (strcmp(ind, first_ind) != 0) continue; /* only the first unique index of the table */
        if (u->n == u->cap) {
            int nc = u->cap ? u->cap * 2 : 256;
            void *q = realloc(u->k, (size_t)nc * sizeof(*u->k));
            if (!q) { f = ALELT_ERR; break; }
            u->k = q;
            u->cap = nc;
        }
        alelt_strcpy(k.tab, sizeof(k.tab), tab);
        u->k[u->n++] = k;
    }
    st_close(&s);
    commit(c);
    return f < 0 ? f : ALELT_OK;
}

static int ukeys_has_pk(const ukeys *u, const char *tab)
{
    int i;
    for (i = 0; i < u->npk; i++) {
        if (strcmp(u->pk[i], tab) == 0) return 1;
    }
    return 0;
}

static long long ukeys_seq(const ukeys *u, const char *tab, const char *col)
{
    int i;
    for (i = 0; i < u->n; i++) {
        if (strcmp(u->k[i].tab, tab) == 0 && strcmp(u->k[i].col, col) == 0) return u->k[i].seq;
    }
    return 0;
}

static int catalog_schema(db2ctx *c, const char *schema, alelt_jw *w)
{
    stmt ts, cs;
    const char *p[1];
    colrow cr;
    int rc, have_col, n;
    ukeys uk;
    p[0] = schema;
    memset(&cr, 0, sizeof(cr));
    if ((rc = ukeys_load(c, schema, &uk)) != ALELT_OK) { ukeys_free(&uk); return rc; }
    alelt_buf_init(&cr.def, MAX_VALUE + 16);
    rc = st_open(c, &ts, db2_sql[Q_TABLES], p, 1);
    if (rc != ALELT_OK) { alelt_buf_free(&cr.def); ukeys_free(&uk); return rc; }
    rc = st_open(c, &cs, db2_sql[Q_COLUMNS], p, 1);
    if (rc != ALELT_OK) { st_close(&ts); alelt_buf_free(&cr.def); ukeys_free(&uk); return rc; }
    have_col = col_fetch(c, &cs, &cr);
    for (;;) {
        char tab[ALELT_IDENT_MAX * 2], dc[4], comp[4], mode[4], ctext[8];
        long long tbsp, tid;
        const char *cn;
        int f = st_fetch(c, &ts);
        if (f < 0) { rc = f; break; }
        if (f == 0) { rc = ALELT_OK; break; }
        if ((rc = st_get_str(c, &ts, 1, tab, sizeof(tab))) < 0 || (rc = st_get_i64(c, &ts, 2, &tbsp, &n)) < 0 ||
            (rc = st_get_i64(c, &ts, 3, &tid, &n)) < 0 || (rc = st_get_str(c, &ts, 4, dc, sizeof(dc))) < 0 ||
            (rc = st_get_str(c, &ts, 5, comp, sizeof(comp))) < 0 || (rc = st_get_str(c, &ts, 6, mode, sizeof(mode))) < 0)
            break;
        jw_obj(w);
        jw_key(w, "table");
        jw_obj(w);
        jw_key(w, "schema"); jw_str(w, schema);
        jw_key(w, "name"); jw_str(w, tab);
        jw_obj_end(w);
        jw_key(w, "engine_ids");
        jw_obj(w);
        jw_key(w, "tbspaceid"); jw_i64(w, tbsp);
        jw_key(w, "tableid"); jw_i64(w, tid);
        if (tbsp < 0 || tid < 0) { /* partitioned: SYSCAT.TABLES ids are placeholders, records carry partition ids */
            int pi;
            for (pi = 0; pi < uk.npt; pi++) {
                char pk[40];
                if (strcmp(uk.pt[pi].tab, tab) != 0) continue;
                snprintf(pk, sizeof(pk), "part.%lld", uk.pt[pi].dpid);
                jw_key(w, pk); jw_i64(w, uk.pt[pi].id);
            }
        }
        jw_obj_end(w);
        jw_key(w, "capture_enabled"); jw_bool(w, dc[0] == 'Y' || dc[0] == 'L');
        cn = db2_compression_text(comp, mode, ctext, sizeof(ctext)) == 0 && ctext[0] ? ctext : NULL;
        jw_key(w, "compression"); jw_str_or_null(w, cn);
        jw_key(w, "columns");
        jw_arr(w);
        while (have_col == 1 && strcmp(cr.tab, tab) < 0) have_col = col_fetch(c, &cs, &cr); /* defensive */
        while (have_col == 1 && strcmp(cr.tab, tab) == 0) {
            if (!ukeys_has_pk(&uk, tab)) {                      /* no PK: unique-index fallback key */
                long long q = ukeys_seq(&uk, tab, cr.name);
                cr.keyseq = q;
                cr.key_null = (q == 0);
            }
            emit_column(w, &cr);
            have_col = col_fetch(c, &cs, &cr);
        }
        jw_arr_end(w);
        jw_obj_end(w);
        if (have_col < 0) { rc = have_col; break; }
        if (c->host->should_stop && c->host->should_stop()) { rc = ALELT_ERR; snprintf(c->err, sizeof(c->err), "stopping"); break; }
    }
    st_close(&ts);
    st_close(&cs);
    alelt_buf_free(&cr.def);
    ukeys_free(&uk);
    commit(c);
    return rc;
}

static int list_schemas(db2ctx *c, char (*out)[ALELT_IDENT_MAX], int *n)
{
    stmt s;
    int rc = st_open(c, &s, db2_sql[Q_SCHEMAS], NULL, 0), f;
    *n = 0;
    if (rc != ALELT_OK) return rc;
    while ((f = st_fetch(c, &s)) == 1 && *n < MAX_SCHEMAS) {
        if ((rc = st_get_str(c, &s, 1, out[*n], ALELT_IDENT_MAX)) < 0) break;
        (*n)++;
    }
    st_close(&s);
    commit(c);
    return f < 0 ? f : (rc < 0 ? rc : ALELT_OK);
}

static int p_catalog_json(void *vc, const char *const *schemas, size_t nschemas, alelt_host_buf *out)
{
    db2ctx *c = (db2ctx *)vc;
    char (*list)[ALELT_IDENT_MAX];
    int n = 0, i, rc;
    alelt_buf b;
    alelt_jw w;
    char pos[ALELT_POS_MAX] = "";
    long long cp = -1;
    rc = connect_db(c);
    if (rc != ALELT_OK) return rc;
    list = calloc(MAX_SCHEMAS, ALELT_IDENT_MAX);
    if (!list) return ALELT_ERR;
    if (nschemas > 0) {
        for (i = 0; i < (int)nschemas && i < MAX_SCHEMAS; i++) alelt_strcpy(list[n++], ALELT_IDENT_MAX, schemas[i]);
    } else if (c->nschemas > 0) {
        for (i = 0; i < c->nschemas; i++) alelt_strcpy(list[n++], ALELT_IDENT_MAX, c->schemas[i]);
    } else if ((rc = list_schemas(c, list, &n)) != ALELT_OK) {
        free(list);
        return rc;
    }
    {
        stmt s;
        if (st_open(c, &s, db2_sql[Q_DB_CODEPAGE], NULL, 0) == ALELT_OK) {
            char v[32];
            if (st_fetch(c, &s) == 1 && st_get_str(c, &s, 1, v, sizeof(v)) == 1) cp = strtoll(v, NULL, 10);
            st_close(&s);
        }
        commit(c);
    }
    alelt_buf_init(&b, (size_t)60 * 1024 * 1024);
    jw_init(&w, &b);
    jw_obj(&w);
    jw_key(&w, "database_codepage");
    if (cp >= 0) jw_i64(&w, cp); else jw_null(&w);
    jw_key(&w, "tables");
    jw_arr(&w);
    rc = ALELT_OK;
    for (i = 0; i < n && rc == ALELT_OK; i++) rc = catalog_schema(c, list[i], &w);
    jw_arr_end(&w);
    /* position at which the catalog is valid: queried AFTER the catalog reads */
    if (rc == ALELT_OK) rc = p_probe_position(c, pos, sizeof(pos));
    jw_key(&w, "position");
    if (pos[0]) {
        jw_obj(&w);
        jw_key(&w, "engine"); jw_str(&w, "db2");
        jw_key(&w, "text"); jw_str(&w, pos);
        jw_obj_end(&w);
    } else {
        jw_null(&w);
    }
    jw_obj_end(&w);
    free(list);
    if (rc == ALELT_OK && jw_ok(&w) != 0) {
        snprintf(c->err, sizeof(c->err), "catalog JSON too large");
        rc = ALELT_ERR;
    }
    if (rc == ALELT_OK && c->host->buf_append(out, b.data, b.len) != 0) rc = ALELT_ERR;
    alelt_buf_free(&b);
    return rc;
}

/* ------------------------------------------------------------------ snapshot / verify: table metadata */
static int meta_alloc(db2_meta *m)
{
    memset(m, 0, sizeof(*m));
    m->cols = calloc(MAX_COLS, DB2_COLNAME_MAX);
    m->types = calloc(MAX_COLS, DB2_TYPENAME_MAX);
    m->codepage = calloc(MAX_COLS, sizeof(long long));
    return (m->cols && m->types && m->codepage) ? 0 : -1;
}

static void meta_free(db2_meta *m)
{
    free(m->cols);
    free(m->types);
    free(m->codepage);
    m->cols = NULL;
    m->types = NULL;
    m->codepage = NULL;
}

/* need_key: snapshot and keyed verify reads need a primary key. */
static int table_meta(db2ctx *c, const char *schema, const char *table, db2_meta *m, int need_key)
{
    stmt s;
    const char *p[2];
    long long ks, cnt = 0;
    int rc, f, isnull, k;
    p[0] = schema;
    p[1] = table;
    rc = st_open(c, &s, db2_sql[Q_SNAP_TABLE], p, 2);
    if (rc != ALELT_OK) return rc;
    if (st_fetch(c, &s) == 1) (void)st_get_i64(c, &s, 1, &cnt, &isnull);
    st_close(&s);
    if (cnt != 1) {
        snprintf(c->err, sizeof(c->err), "table %s.%s not found (or not a base table)", schema, table);
        commit(c);
        return ALELT_ERR_NOTFOUND;
    }
    for (k = 0; k < ALELT_MAX_KEYCOLS; k++) m->keyidx[k] = -1;
    m->ncols = m->nkeys = 0;
    rc = st_open(c, &s, db2_sql[Q_META_COLS], p, 2);
    if (rc != ALELT_OK) return rc;
    while ((f = st_fetch(c, &s)) == 1) {
        if (m->ncols >= MAX_COLS) { f = ALELT_ERR; break; }
        if ((rc = st_get_str(c, &s, 1, m->cols[m->ncols], DB2_COLNAME_MAX)) < 0) { f = rc; break; }
        if ((rc = st_get_i64(c, &s, 2, &ks, &isnull)) < 0) { f = rc; break; }
        if ((rc = st_get_str(c, &s, 3, m->types[m->ncols], DB2_TYPENAME_MAX)) < 0) { f = rc; break; }
        if ((rc = st_get_i64(c, &s, 4, &m->codepage[m->ncols], &isnull)) < 0) { f = rc; break; }
        if (!isnull && ks != 0) { /* tolerate 0 as "not part of the key" */
            if (ks < 0 || ks > ALELT_MAX_KEYCOLS) {
                snprintf(c->err, sizeof(c->err), "primary key too wide");
                f = ALELT_ERR;
                break;
            }
            m->keyidx[ks - 1] = m->ncols;
            if ((int)ks > m->nkeys) m->nkeys = (int)ks;
        }
        m->ncols++;
    }
    st_close(&s);
    commit(c);
    if (f < 0) return f;
    if (m->nkeys == 0) { /* SRC-db2-07: first unique index (by name), as in the catalog */
        char ind[ALELT_IDENT_MAX * 2], first[ALELT_IDENT_MAX * 2] = "", col[DB2_COLNAME_MAX];
        rc = st_open(c, &s, db2_sql[Q_UNIQUE_TAB], p, 2);
        if (rc != ALELT_OK) return rc;
        while ((f = st_fetch(c, &s)) == 1) {
            if ((rc = st_get_str(c, &s, 1, ind, sizeof(ind))) < 0 || (rc = st_get_str(c, &s, 2, col, sizeof(col))) < 0 ||
                (rc = st_get_i64(c, &s, 3, &ks, &isnull)) < 0) {
                f = rc;
                break;
            }
            if (!first[0]) alelt_strcpy(first, sizeof(first), ind);
            if (strcmp(ind, first) != 0) break;
            if (ks < 1 || ks > ALELT_MAX_KEYCOLS) { snprintf(c->err, sizeof(c->err), "unique key too wide"); f = ALELT_ERR; break; }
            for (k = 0; k < m->ncols; k++) {
                if (strcmp(m->cols[k], col) == 0) {
                    m->keyidx[ks - 1] = k;
                    if ((int)ks > m->nkeys) m->nkeys = (int)ks;
                }
            }
        }
        st_close(&s);
        commit(c);
        if (f < 0) return f;
    }
    if (m->nkeys == 0 && need_key) {
        snprintf(c->err, sizeof(c->err), "table %s.%s has no primary key; keyed reads impossible", schema, table);
        return ALELT_ERR_DENIED;
    }
    for (k = 0; k < m->nkeys; k++) {
        if (m->keyidx[k] < 0) { snprintf(c->err, sizeof(c->err), "primary key sequence has a gap"); return ALELT_ERR; }
    }
    return ALELT_OK;
}

/* Prepare a builder statement (R1: shape re-checked right before SQLPrepare). */
static int prep_built(db2ctx *c, SQLHANDLE *h, const alelt_buf *sql, unsigned timeout_s)
{
    SQLRETURN r = SQL_SUCCESS;
    *h = NULL;
    if (!db2_built_ok(alelt_buf_cstr(sql), sql->len)) {
        snprintf(c->err, sizeof(c->err), "refused: statement is not a read-only SELECT ... WITH CS");
        return ALELT_ERR_DENIED;
    }
    r = SQLAllocHandle(SQL_HANDLE_STMT, c->dbc, h);
    if (!OK(r)) { *h = NULL; return cli_fail(c, SQL_HANDLE_DBC, c->dbc, "SQLAllocHandle(STMT)"); }
    if (timeout_s) (void)SQLSetStmtAttr(*h, SQL_ATTR_QUERY_TIMEOUT, (SQLPOINTER)(uintptr_t)timeout_s, 0);
    r = SQLPrepare(*h, (SQLCHAR *)alelt_buf_cstr(sql), SQL_NTS);
    if (!OK(r)) return cli_fail(c, SQL_HANDLE_STMT, *h, "SQLPrepare(built)");
    return ALELT_OK;
}

static void stmt_free(SQLHANDLE *h)
{
    if (*h) {
        (void)SQLCloseCursor(*h);
        (void)SQLFreeHandle(SQL_HANDLE_STMT, *h);
        *h = NULL;
    }
}

typedef struct keybind {
    wchar_t *w[DB2_MAX_MARKERS];
    SQLLEN len[DB2_MAX_MARKERS];
} keybind;

static void keybind_free(keybind *kb)
{
    int i;
    for (i = 0; i < DB2_MAX_MARKERS; i++) {
        free(kb->w[i]);
        kb->w[i] = NULL;
    }
}

/* Bind key texts to the markers (ptab: k = lo[k], 100 + k = hi[k]); the parameter
 * type comes from SQLDescribeParam so Db2 converts the text to the key type. */
static int bind_keys(db2ctx *c, SQLHANDLE h, const int *ptab, int np, const char *const *lo, const char *const *hi,
                     keybind *kb)
{
    int i;
    keybind_free(kb);
    for (i = 0; i < np; i++) {
        const char *v = ptab[i] >= 100 ? hi[ptab[i] - 100] : lo[ptab[i]];
        SQLSMALLINT dtype = SQL_WVARCHAR, ddigits = 0, dnull = 0;
        SQLULEN dsize = 0;
        SQLRETURN r = SQL_SUCCESS;
        kb->w[i] = alelt_to_wide_alloc(v ? v : "");
        if (!kb->w[i]) { snprintf(c->err, sizeof(c->err), "bad key value"); return ALELT_ERR; }
        kb->len[i] = (SQLLEN)(wcslen(kb->w[i]) * sizeof(wchar_t));
        if (!OK(SQLDescribeParam(h, (SQLUSMALLINT)(i + 1), &dtype, &dsize, &ddigits, &dnull))) {
            dtype = SQL_WVARCHAR;
            dsize = 0;
            ddigits = 0;
        }
        if (dsize == 0) dsize = (SQLULEN)(wcslen(kb->w[i]) ? wcslen(kb->w[i]) : 1);
        r = SQLBindParameter(h, (SQLUSMALLINT)(i + 1), SQL_PARAM_INPUT, SQL_C_WCHAR, dtype, dsize, ddigits, kb->w[i],
                             kb->len[i], &kb->len[i]);
        if (!OK(r)) return cli_fail(c, SQL_HANDLE_STMT, h, "SQLBindParameter");
    }
    return ALELT_OK;
}

static int exec_stmt(db2ctx *c, SQLHANDLE h, SQLRETURN *r)
{
    *r = SQLExecute(h);
    if (!OK(*r) && *r != SQL_NO_DATA) return cli_fail(c, SQL_HANDLE_STMT, h, "SQLExecute");
    return ALELT_OK;
}

/* ------------------------------------------------------------------ snapshot */
static int p_snapshot_chunk(void *vc, const alelt_snapshot_req *rq, alelt_host_buf *out, alelt_snapshot_res *res)
{
    db2ctx *c = (db2ctx *)vc;
    db2_meta m;
    alelt_buf sql, row, val;
    int ptab[DB2_MAX_MARKERS];
    keybind *kb = NULL;
    int np = 0, i, rc;
    size_t bytes = 0;
    SQLHANDLE h = NULL;
    SQLRETURN r = SQL_SUCCESS;

    memset(res, 0, sizeof(*res));
    if (rq->chunk_rows == 0 || rq->chunk_rows > 1000000) return ALELT_ERR;
    rc = connect_db(c);
    if (rc != ALELT_OK) return rc;
    if (meta_alloc(&m) != 0) { meta_free(&m); return ALELT_ERR; }
    kb = (keybind *)calloc(1, sizeof(*kb));
    alelt_buf_init(&sql, 1024 * 1024);
    alelt_buf_init(&row, MAX_CHUNK_BYTES + MAX_VALUE);
    alelt_buf_init(&val, MAX_VALUE + 16);
    if (!kb) { rc = ALELT_ERR; goto out; }
    rc = table_meta(c, rq->schema, rq->table, &m, 1);
    if (rc != ALELT_OK) goto out;
    if ((rq->nkey_after && rq->nkey_after != (size_t)m.nkeys) || (rq->nkey_to && rq->nkey_to != (size_t)m.nkeys) ||
        db2_snapshot_sql(rq->schema, rq->table, &m, rq->nkey_after > 0, rq->nkey_to > 0, rq->chunk_rows, &sql, ptab,
                         &np) != 0) {
        snprintf(c->err, sizeof(c->err), "cannot build snapshot statement (key arity must match the primary key)");
        rc = ALELT_ERR;
        goto out;
    }
    if ((rc = prep_built(c, &h, &sql, 0)) != ALELT_OK) goto out;
    if ((rc = bind_keys(c, h, ptab, np, rq->key_after, rq->key_to, kb)) != ALELT_OK) goto out;
    if ((rc = exec_stmt(c, h, &r)) != ALELT_OK) goto out;
    {
        stmt s;
        s.h = h;
        for (;;) {
            alelt_jw w;
            int f = (r == SQL_NO_DATA) ? 0 : st_fetch(c, &s);
            if (f < 0) { rc = f; goto out; }
            if (f == 0) { res->last = 1; break; }
            alelt_buf_reset(&row);
            jw_init(&w, &row);
            jw_obj(&w);
            for (i = 0; i < m.ncols; i++) {
                int g, k;
                alelt_buf_reset(&val);
                g = st_get(c, &s, i + 1, &val);
                if (g < 0) { rc = g; goto out; }
                jw_key(&w, m.cols[i]);
                if (g == 0) jw_null(&w);
                else jw_strn(&w, alelt_buf_cstr(&val), val.len);
                for (k = 0; k < m.nkeys; k++) {
                    if (m.keyidx[k] != i) continue;
                    if (g && val.len >= ALELT_KEYVAL_MAX) { /* SRC-db2-08: never page on a truncated key */
                        snprintf(c->err, sizeof(c->err), "key column %.128s value longer than %d bytes: keyed paging "
                                 "impossible", m.cols[i], ALELT_KEYVAL_MAX - 1);
                        rc = ALELT_ERR;
                        goto out;
                    }
                    alelt_strcpy(res->last_key[k], ALELT_KEYVAL_MAX, g ? alelt_buf_cstr(&val) : "");
                }
            }
            jw_obj_end(&w);
            alelt_buf_putc(&row, '\n');
            if (jw_ok(&w) != 0 || row.failed) { snprintf(c->err, sizeof(c->err), "row too large"); rc = ALELT_ERR; goto out; }
            if (c->host->buf_append(out, row.data, row.len) != 0) {
                snprintf(c->err, sizeof(c->err), "chunk buffer full");
                rc = ALELT_ERR;
                goto out;
            }
            res->rows++;
            bytes += row.len;
            if (res->rows >= rq->chunk_rows) break;  /* more may follow */
            if (bytes >= MAX_CHUNK_BYTES) break;      /* byte cap: next chunk continues after last_key */
        }
    }
    res->nlast_key = res->rows ? (size_t)m.nkeys : 0;
    rc = ALELT_OK;
out:
    stmt_free(&h);
    commit(c);
    if (kb) keybind_free(kb);
    free(kb);
    meta_free(&m);
    alelt_buf_free(&sql);
    alelt_buf_free(&row);
    alelt_buf_free(&val);
    return rc;
}

/* ------------------------------------------------------------------ VERIFY_TABLE (F-03) */
typedef struct vstate {
    db2ctx *c;
    const alelt_verify_req *rq;
    db2_meta m;
    int *colidx;
    int ncol;
    alelt_buf sql, val;
    alelt_jw *w;
    alelt_buf *ob;
    keybind *kb;
    int truncated;
    unsigned timeout_s;
} vstate;

static int key_texts(const fj_val *arr, const char **dst, int nkeys)
{
    const fj_val *e;
    int n = 0;
    if (!arr || arr->type == FJ_NULL) return 0;
    if (arr->type != FJ_ARR) return -1;
    for (e = arr->child; e; e = e->next) {
        if (e->type != FJ_STR || n >= ALELT_MAX_KEYCOLS) return -1;
        dst[n++] = e->s;
    }
    return n == nkeys ? n : -1;
}

static void jw_val(alelt_jw *w, int g, const alelt_buf *v)
{
    if (g == 0) jw_null(w);
    else jw_strn(w, alelt_buf_cstr(v), v->len);
}

/* Aggregates (and COUNT(*)) of one range, columns in groups of <= DB2_AGG_ITEMS_MAX items. */
static int verify_range_agg(vstate *v, const char **lo, int nlo, const char **hi, int nhi, int want_agg)
{
    db2ctx *c = v->c;
    int start = 0, first = 1, rc = ALELT_OK;
    long long rows = -1;
    alelt_jw *w = v->w;
    if (want_agg) {
        jw_key(w, "agg");
        jw_obj(w);
    }
    do {
        int end = start, items = 1, np = 0, i, col = 1, isnull;
        int ptab[DB2_MAX_MARKERS];
        SQLHANDLE h = NULL;
        SQLRETURN r = SQL_SUCCESS;
        stmt s;
        long long cnt = 0;
        if (want_agg) {
            while (end < v->ncol) {
                int k = db2_agg_items(db2_agg_class(v->m.types[v->colidx[end]], v->m.codepage[v->colidx[end]]));
                if (items + k > DB2_AGG_ITEMS_MAX) break;
                items += k;
                end++;
            }
        }
        alelt_buf_reset(&v->sql);
        if (db2_verify_agg_sql(v->rq->schema, v->rq->table, &v->m, v->colidx + start, end - start, nlo > 0, nhi > 0,
                               &v->sql, ptab, &np) != 0) {
            snprintf(c->err, sizeof(c->err), "cannot build verify statement");
            return ALELT_ERR;
        }
        if ((rc = prep_built(c, &h, &v->sql, v->timeout_s)) != ALELT_OK) { stmt_free(&h); return rc; }
        if ((rc = bind_keys(c, h, ptab, np, lo, hi, v->kb)) == ALELT_OK) rc = exec_stmt(c, h, &r);
        s.h = h;
        if (rc == ALELT_OK && st_fetch(c, &s) != 1) rc = ALELT_ERR;
        if (rc == ALELT_OK) rc = st_get_i64(c, &s, col++, &cnt, &isnull);
        if (rc == ALELT_OK && first) rows = cnt;
        for (i = start; rc == ALELT_OK && i < end; i++) {
            const char *name = v->m.cols[v->colidx[i]];
            db2_agg a = db2_agg_class(v->m.types[v->colidx[i]], v->m.codepage[v->colidx[i]]);
            int g;
            long long n = 0;
            jw_key(w, name);
            jw_obj(w);
            if ((rc = st_get_i64(c, &s, col++, &n, &isnull)) != ALELT_OK) break;
            jw_key(w, "count");
            jw_i64(w, isnull ? 0 : n);
            if (a <= AGG_TMP) {
                alelt_buf_reset(&v->val);
                if ((g = st_get(c, &s, col++, &v->val)) < 0) { rc = g; break; }
                jw_key(w, "min"); jw_val(w, g, &v->val);
                alelt_buf_reset(&v->val);
                if ((g = st_get(c, &s, col++, &v->val)) < 0) { rc = g; break; }
                jw_key(w, "max"); jw_val(w, g, &v->val);
            }
            if (a == AGG_INT || a == AGG_DEC || a == AGG_STR) {
                alelt_buf_reset(&v->val);
                if ((g = st_get(c, &s, col++, &v->val)) < 0) { rc = g; break; }
                jw_key(w, a == AGG_STR ? "len" : "sum"); jw_val(w, g, &v->val);
            }
            jw_obj_end(w);
        }
        stmt_free(&h);
        commit(c);
        if (rc != ALELT_OK) return rc < 0 ? rc : ALELT_ERR;
        first = 0;
        start = end;
        if (c->host->should_stop && c->host->should_stop()) { snprintf(c->err, sizeof(c->err), "stopping"); return ALELT_ERR; }
    } while (want_agg && start < v->ncol);
    if (want_agg) jw_obj_end(w);
    jw_key(w, "rows");
    jw_i64(w, rows);
    return ALELT_OK;
}

static int verify_range_keys(vstate *v, const char **lo, int nlo, const char **hi, int nhi, unsigned max_keys)
{
    db2ctx *c = v->c;
    int ptab[DB2_MAX_MARKERS], np = 0, rc, k;
    unsigned n = 0;
    SQLHANDLE h = NULL;
    SQLRETURN r = SQL_SUCCESS;
    stmt s;
    alelt_jw *w = v->w;
    alelt_buf_reset(&v->sql);
    if (db2_verify_keys_sql(v->rq->schema, v->rq->table, &v->m, nlo > 0, nhi > 0, max_keys + 1, &v->sql, ptab, &np) != 0) {
        snprintf(c->err, sizeof(c->err), "cannot build verify key statement");
        return ALELT_ERR;
    }
    if ((rc = prep_built(c, &h, &v->sql, v->timeout_s)) != ALELT_OK) { stmt_free(&h); return rc; }
    if ((rc = bind_keys(c, h, ptab, np, lo, hi, v->kb)) == ALELT_OK) rc = exec_stmt(c, h, &r);
    s.h = h;
    jw_key(w, "keys");
    jw_arr(w);
    while (rc == ALELT_OK && r != SQL_NO_DATA) {
        int f = st_fetch(c, &s);
        if (f < 0) { rc = f; break; }
        if (f == 0) break;
        if (n >= max_keys || v->ob->len >= v->rq->max_bytes) {
            v->truncated |= (v->ob->len >= v->rq->max_bytes);
            n = max_keys + 1;
            break;
        }
        jw_arr(w);
        for (k = 0; k < v->m.nkeys; k++) {
            int g;
            alelt_buf_reset(&v->val);
            if ((g = st_get(c, &s, k + 1, &v->val)) < 0) { rc = g; break; }
            jw_val(w, g, &v->val);
        }
        jw_arr_end(w);
        n++;
    }
    jw_arr_end(w);
    jw_key(w, "keys_truncated");
    jw_bool(w, n > max_keys);
    stmt_free(&h);
    commit(c);
    return rc;
}

/* Conversion errors of a key value (too long, not a number, not a date): such a
 * key cannot exist in the table, so a keyed re-read reports it missing. */
static int key_cannot_exist(const char *err)
{
    static const char *const st[] = {"SQLSTATE 22001", "SQLSTATE 22003", "SQLSTATE 22005", "SQLSTATE 22007", "SQLSTATE 22008",
                                     "SQLSTATE 22018", NULL};
    int i;
    for (i = 0; st[i]; i++) {
        if (strstr(err, st[i])) return 1;
    }
    return 0;
}

static int verify_rows(vstate *v, const fj_val *kv)
{
    db2ctx *c = v->c;
    int ptab[DB2_MAX_MARKERS], np = 0, rc, i;
    unsigned done = 0;
    SQLHANDLE h = NULL;
    const fj_val *e;
    alelt_jw *w = v->w;
    alelt_buf missing;
    alelt_jw mw;
    alelt_buf_reset(&v->sql);
    if (db2_verify_rows_sql(v->rq->schema, v->rq->table, &v->m, v->colidx, v->ncol, &v->sql, ptab, &np) != 0) {
        snprintf(c->err, sizeof(c->err), "cannot build keyed re-read statement");
        return ALELT_ERR;
    }
    if ((rc = prep_built(c, &h, &v->sql, v->timeout_s)) != ALELT_OK) { stmt_free(&h); return rc; }
    alelt_buf_init(&missing, (size_t)4 * 1024 * 1024);
    jw_init(&mw, &missing);
    jw_arr(&mw);
    jw_key(w, "rows");
    jw_arr(w);
    for (e = kv->child; e && rc == ALELT_OK; e = e->next) {
        const char *k[ALELT_MAX_KEYCOLS];
        SQLRETURN r = SQL_SUCCESS;
        stmt s;
        int f;
        if (done >= v->rq->max_key_values || v->ob->len >= v->rq->max_bytes) { v->truncated = 1; break; }
        if (key_texts(e, k, v->m.nkeys) != v->m.nkeys) {
            snprintf(c->err, sizeof(c->err), "key_values: arity must match the primary key (%d)", v->m.nkeys);
            rc = ALELT_ERR_DENIED;
            break;
        }
        if ((rc = bind_keys(c, h, ptab, np, k, NULL, v->kb)) != ALELT_OK) break;
        if ((rc = exec_stmt(c, h, &r)) != ALELT_OK) {
            if (!key_cannot_exist(c->err)) break;
            rc = ALELT_OK; /* the key value does not fit the key column: no such row */
            r = SQL_NO_DATA;
        }
        s.h = h;
        f = (r == SQL_NO_DATA) ? 0 : st_fetch(c, &s);
        if (f < 0) { rc = f; break; }
        if (f == 0) {
            jw_arr(&mw);
            for (i = 0; i < v->m.nkeys; i++) jw_str(&mw, k[i]);
            jw_arr_end(&mw);
        } else {
            jw_obj(w);
            for (i = 0; i < v->ncol; i++) {
                int g;
                alelt_buf_reset(&v->val);
                if ((g = st_get(c, &s, i + 1, &v->val)) < 0) { rc = g; break; }
                jw_key(w, v->m.cols[v->colidx[i]]);
                jw_val(w, g, &v->val);
            }
            jw_obj_end(w);
        }
        (void)SQLFreeStmt(h, SQL_CLOSE);
        done++;
    }
    jw_arr_end(w);
    jw_arr_end(&mw);
    jw_key(w, "missing");
    if (jw_ok(&mw) == 0) jw_raw(w, (const char *)alelt_buf_cstr(&missing), missing.len);
    else jw_raw(w, "[]", 2);
    alelt_buf_free(&missing);
    stmt_free(&h);
    commit(c);
    return rc;
}

static int has_check(const fj_val *checks, const char *name)
{
    const fj_val *e;
    for (e = checks ? checks->child : NULL; e; e = e->next) {
        if (e->type == FJ_STR && strcmp(e->s, name) == 0) return 1;
    }
    return 0;
}

static int p_verify_table(void *vc, const alelt_verify_req *rq, alelt_host_buf *out)
{
    static const char *const ak[] = {"table", "verify_id", "checks", "columns", "ranges", "key_values", "max_keys",
                                     "timeout_s", NULL};
    db2ctx *c = (db2ctx *)vc;
    vstate v;
    alelt_buf b;
    alelt_jw w;
    fj_doc *d = NULL;
    const fj_val *a, *checks, *cols, *ranges, *kv, *t;
    const char *vid, *bad = NULL;
    char pos[ALELT_POS_MAX] = "";
    int64_t mk = ALELT_VERIFY_DEFAULT_KEYS, to = 300;
    int rc, i, want_rc, want_agg, want_keys, want_rows;
    DWORD t0 = GetTickCount();

    memset(&v, 0, sizeof(v));
    c->err[0] = 0;
    d = fj_parse(rq->args_json, rq->args_len, c->err, sizeof(c->err));
    if (!d) return ALELT_ERR_DENIED;
    a = fj_root(d);
    t = fj_get(a, "table");
    vid = fj_get_str(a, "verify_id");
    checks = fj_get(a, "checks");
    if (a->type != FJ_OBJ || !fj_keys_only(a, ak, &bad) || !t || !vid || !checks || checks->type != FJ_ARR ||
        !fj_get_str(t, "schema") || !fj_get_str(t, "name") || strcmp(fj_get_str(t, "schema"), rq->schema) != 0 ||
        strcmp(fj_get_str(t, "name"), rq->table) != 0) {
        snprintf(c->err, sizeof(c->err), "VERIFY_TABLE args invalid");
        fj_free(d);
        return ALELT_ERR_DENIED;
    }
    want_rc = has_check(checks, "row_count");
    want_agg = has_check(checks, "aggregates");
    want_keys = has_check(checks, "keys");
    want_rows = has_check(checks, "rows");
    (void)fj_get_i64(a, "max_keys", &mk);
    (void)fj_get_i64(a, "timeout_s", &to);
    if (mk < 1 || (uint64_t)mk > rq->max_keys) mk = rq->max_keys;
    if (to < 1 || (uint64_t)to > rq->timeout_s) to = rq->timeout_s;
    cols = fj_get(a, "columns");
    ranges = fj_get(a, "ranges");
    kv = fj_get(a, "key_values");

    rc = connect_db(c);
    if (rc != ALELT_OK) { fj_free(d); return rc; }
    v.c = c;
    v.rq = rq;
    v.timeout_s = (unsigned)to;
    alelt_buf_init(&v.sql, 1024 * 1024);
    alelt_buf_init(&v.val, MAX_VALUE + 16);
    alelt_buf_init(&b, rq->max_bytes + (size_t)2 * 1024 * 1024);
    v.ob = &b;
    v.w = &w;
    v.kb = (keybind *)calloc(1, sizeof(keybind));
    v.colidx = calloc(MAX_COLS, sizeof(int));
    if (meta_alloc(&v.m) != 0 || !v.kb || !v.colidx) { rc = ALELT_ERR; goto out; }
    rc = table_meta(c, rq->schema, rq->table, &v.m, want_keys || want_rows || (ranges && ranges->type == FJ_ARR));
    if (rc != ALELT_OK) goto out;
    if (cols && cols->type == FJ_ARR) {
        const fj_val *e;
        for (e = cols->child; e; e = e->next) {
            int k, hit = -1;
            if (e->type != FJ_STR) { rc = ALELT_ERR_DENIED; break; }
            for (k = 0; k < v.m.ncols; k++) {
                if (strcmp(v.m.cols[k], e->s) == 0) hit = k;
            }
            if (hit < 0 || v.ncol >= MAX_COLS) {
                snprintf(c->err, sizeof(c->err), "column %.128s is not a column of %s.%s", e->s, rq->schema, rq->table);
                rc = ALELT_ERR_DENIED;
                break;
            }
            v.colidx[v.ncol++] = hit;
        }
        if (rc != ALELT_OK) goto out;
    } else {
        for (i = 0; i < v.m.ncols; i++) v.colidx[v.ncol++] = i;
    }
    if ((rc = p_probe_position(c, pos, sizeof(pos))) != ALELT_OK) goto out;

    jw_init(&w, &b);
    jw_obj(&w); /* VerifyTableResult */
    jw_key(&w, "verify_id"); jw_str(&w, vid);
    jw_key(&w, "table");
    jw_obj(&w);
    jw_key(&w, "schema"); jw_str(&w, rq->schema);
    jw_key(&w, "name"); jw_str(&w, rq->table);
    jw_obj_end(&w);
    jw_key(&w, "engine"); jw_str(&w, "db2");
    jw_key(&w, "isolation"); jw_str(&w, "CS");
    jw_key(&w, "key_columns");
    jw_arr(&w);
    for (i = 0; i < v.m.nkeys; i++) jw_str(&w, v.m.cols[v.m.keyidx[i]]);
    jw_arr_end(&w);
    jw_key(&w, "columns");
    jw_obj(&w);
    for (i = 0; i < v.ncol; i++) {
        jw_key(&w, v.m.cols[v.colidx[i]]);
        jw_str(&w, db2_agg_name(db2_agg_class(v.m.types[v.colidx[i]], v.m.codepage[v.colidx[i]])));
    }
    jw_obj_end(&w);
    jw_key(&w, "position_before"); jw_str(&w, pos);
    jw_key(&w, "ranges");
    jw_arr(&w);
    if (want_rc || want_agg || want_keys) {
        const fj_val *e = (ranges && ranges->type == FJ_ARR) ? ranges->child : NULL;
        int idx = 0, whole = !(ranges && ranges->type == FJ_ARR);
        while (rc == ALELT_OK && (whole || e)) {
            const char *lo[ALELT_MAX_KEYCOLS], *hi[ALELT_MAX_KEYCOLS];
            int nlo = 0, nhi = 0;
            if (!whole) {
                nlo = key_texts(fj_get(e, "after"), lo, v.m.nkeys);
                nhi = key_texts(fj_get(e, "to"), hi, v.m.nkeys);
                if (nlo < 0 || nhi < 0) {
                    snprintf(c->err, sizeof(c->err), "range %d: key arity must match the primary key (%d)", idx, v.m.nkeys);
                    rc = ALELT_ERR_DENIED;
                    break;
                }
            }
            jw_obj(&w);
            jw_key(&w, "index"); jw_i64(&w, idx);
            if (want_rc || want_agg) rc = verify_range_agg(&v, lo, nlo, hi, nhi, want_agg);
            if (rc == ALELT_OK && want_keys) rc = verify_range_keys(&v, lo, nlo, hi, nhi, (unsigned)mk);
            jw_obj_end(&w);
            idx++;
            if (whole) break;
            e = e->next;
        }
    }
    jw_arr_end(&w);
    if (rc == ALELT_OK && want_rows) {
        if (!kv || kv->type != FJ_ARR) {
            snprintf(c->err, sizeof(c->err), "rows check needs key_values");
            rc = ALELT_ERR_DENIED;
        } else {
            rc = verify_rows(&v, kv);
        }
    }
    if (rc != ALELT_OK) goto out;
    if ((rc = p_probe_position(c, pos, sizeof(pos))) != ALELT_OK) goto out;
    jw_key(&w, "position_after"); jw_str(&w, pos);
    jw_key(&w, "truncated"); jw_bool(&w, v.truncated);
    jw_key(&w, "elapsed_ms"); jw_u64(&w, (uint64_t)(GetTickCount() - t0));
    jw_obj_end(&w);
    if (jw_ok(&w) != 0) {
        snprintf(c->err, sizeof(c->err), "verify result too large");
        rc = ALELT_ERR;
        goto out;
    }
    if (c->host->buf_append(out, b.data, b.len) != 0) { snprintf(c->err, sizeof(c->err), "result buffer full"); rc = ALELT_ERR; }
out:
    if (v.kb) keybind_free(v.kb);
    free(v.kb);
    free(v.colidx);
    meta_free(&v.m);
    alelt_buf_free(&v.sql);
    alelt_buf_free(&v.val);
    alelt_buf_free(&b);
    commit(c);
    fj_free(d);
    return rc;
}

/* ------------------------------------------------------------------ probes */
static int is_json_number(const char *s)
{
    char err[8];
    fj_doc *d = fj_parse(s, strlen(s), err, sizeof(err));
    int r = d && fj_root(d)->type == FJ_NUM;
    fj_free(d);
    return r;
}

/* Emits every column of the first row as lower-case members. */
static int row_members(db2ctx *c, db2_qid q, alelt_jw *w)
{
    stmt s;
    SQLSMALLINT ncol = 0, i;
    int rc = st_open(c, &s, db2_sql[q], NULL, 0), f;
    if (rc != ALELT_OK) return rc;
    (void)SQLNumResultCols(s.h, &ncol);
    f = st_fetch(c, &s);
    if (f == 1) {
        for (i = 1; i <= ncol; i++) {
            SQLCHAR name[129];
            SQLSMALLINT nl = 0, dt, dd, nu;
            SQLULEN cs;
            char v[256], *p;
            if (!OK(SQLDescribeCol(s.h, (SQLUSMALLINT)i, name, (SQLSMALLINT)sizeof(name), &nl, &dt, &cs, &dd, &nu))) continue;
            for (p = (char *)name; *p; p++) {
                if (*p >= 'A' && *p <= 'Z') *p = (char)(*p + 32);
            }
            if (!strcmp((char *)name, "log_utilization_percent")) alelt_strcpy((char *)name, sizeof(name), "log_utilization_pct");
            jw_key(w, (const char *)name);
            rc = st_get_str(c, &s, i, v, sizeof(v));
            if (rc < 0) break;
            if (rc == 0) jw_null(w);
            else if (is_json_number(v)) jw_raw(w, v, strlen(v));
            else jw_str(w, v);
            rc = ALELT_OK;
        }
    } else if (f < 0) {
        rc = f;
    }
    st_close(&s);
    commit(c);
    return rc < 0 ? rc : ALELT_OK;
}

static int p_probe(void *vc, const char *name, alelt_host_buf *out)
{
    db2ctx *c = (db2ctx *)vc;
    alelt_buf b;
    alelt_jw w;
    int rc = connect_db(c);
    if (rc != ALELT_OK) return rc;
    alelt_buf_init(&b, 256 * 1024);
    jw_init(&w, &b);
    jw_obj(&w);
    jw_key(&w, "probe"); jw_str(&w, name);
    if (!strcmp(name, "current_position")) {
        char pos[ALELT_POS_MAX];
        rc = p_probe_position(c, pos, sizeof(pos));
        if (rc == ALELT_OK) { jw_key(&w, "position"); jw_str(&w, pos); }
    } else if (!strcmp(name, "db_health")) {
        rc = row_members(c, Q_LOG_UTIL, &w);
        if (rc == ALELT_OK) rc = row_members(c, Q_MON_DB, &w);
    } else if (!strcmp(name, "server_info")) { /* AHS-08: agent-internal (not a hub command): post-connect guard */
        stmt s;
        char host[256], sv[256];
        SQLSMALLINT n = 0;
        host[0] = 0;
        rc = st_open(c, &s, db2_sql[Q_SERVER_HOST], NULL, 0);
        if (rc == ALELT_OK) {
            if (st_fetch(c, &s) == 1 && st_get_str(c, &s, 1, host, sizeof(host)) != 1) host[0] = 0;
            st_close(&s);
            commit(c);
        }
        rc = ALELT_OK;               /* an unreadable host is reported as null: the agent refuses (fail closed) */
        jw_key(&w, "host");
        if (host[0]) {
            char *e = host + strlen(host);
            while (e > host && e[-1] == ' ') *--e = 0;
            jw_str(&w, host);
        } else {
            jw_null(&w);
        }
        if (OK(SQLGetInfo(c->dbc, SQL_SERVER_NAME, sv, (SQLSMALLINT)sizeof(sv), &n))) {
            jw_key(&w, "server_name"); jw_str(&w, sv);
        }
        if (OK(SQLGetInfo(c->dbc, SQL_DATABASE_NAME, sv, (SQLSMALLINT)sizeof(sv), &n))) {
            jw_key(&w, "database_name"); jw_str(&w, sv);
        }
    } else if (!strcmp(name, "log_config")) {
        stmt s;
        rc = st_open(c, &s, db2_sql[Q_LOG_CFG], NULL, 0);
        if (rc == ALELT_OK) {
            int f;
            while ((f = st_fetch(c, &s)) == 1) {
                char k[64], v[256];
                if (st_get_str(c, &s, 1, k, sizeof(k)) == 1) {
                    jw_key(&w, k);
                    if (st_get_str(c, &s, 2, v, sizeof(v)) == 1) jw_str(&w, v); else jw_null(&w);
                }
            }
            if (f < 0) rc = f;
            st_close(&s);
            commit(c);
        }
    } else {
        snprintf(c->err, sizeof(c->err), "probe '%.32s' not whitelisted", name);
        rc = ALELT_ERR_DENIED;
    }
    jw_obj_end(&w);
    if (rc == ALELT_OK && (jw_ok(&w) != 0 || c->host->buf_append(out, b.data, b.len) != 0)) rc = ALELT_ERR;
    alelt_buf_free(&b);
    return rc;
}

/* ------------------------------------------------------------------ open / close */
static int p_open(const alelt_host_api *host, const char *settings_json, void **out, char *err, size_t errcap)
{
    db2ctx *c;
    fj_doc *d;
    const fj_val *r, *v;
    const char *db;
    static const char *const keys[] = {"database", "catalog_schemas", NULL};
    const char *bad = NULL;
    int rc;
    *out = NULL;
    if (!host || host->abi_version < ALELT_PLUGIN_ABI_VERSION || !host->buf_append) {
        snprintf(err, errcap, "host ABI mismatch");
        return ALELT_ERR;
    }
    d = fj_parse(settings_json, strlen(settings_json), err, errcap);
    if (!d) return ALELT_ERR;
    r = fj_root(d);
    if (r->type != FJ_OBJ || !fj_keys_only(r, keys, &bad)) {
        snprintf(err, errcap, "source.settings: unknown key '%s'", bad ? bad : "?");
        fj_free(d);
        return ALELT_ERR;
    }
    db = fj_get_str(r, "database");
    if (!db || !*db || strlen(db) > 8) { /* Db2 database alias: max 8 characters */
        snprintf(err, errcap, "source.settings.database must be a Db2 database alias (1-8 chars)");
        fj_free(d);
        return ALELT_ERR;
    }
    c = (db2ctx *)calloc(1, sizeof(*c));
    if (!c) { fj_free(d); return ALELT_ERR; }
    c->host = host;
    alelt_strcpy(c->database, sizeof(c->database), db);
    v = fj_get(r, "catalog_schemas");
    if (v && v->type == FJ_ARR) {
        const fj_val *e;
        for (e = v->child; e && c->nschemas < MAX_SCHEMAS; e = e->next) {
            if (e->type != FJ_STR || alelt_strcpy(c->schemas[c->nschemas], ALELT_IDENT_MAX, e->s) != 0) {
                snprintf(err, errcap, "catalog_schemas entries must be identifiers");
                fj_free(d);
                free(c);
                return ALELT_ERR;
            }
            c->nschemas++;
        }
    }
    fj_free(d);
    rc = connect_db(c);
    if (rc != ALELT_OK) {
        snprintf(err, errcap, "%s", c->err);
        disconnect(c);
        free(c);
        return rc;
    }
    *out = c;
    return ALELT_OK;
}

static const char *p_last_error(void *vc)
{
    return vc ? ((db2ctx *)vc)->err : "";
}

static void p_close(void *vc)
{
    db2ctx *c = (db2ctx *)vc;
    if (!c) return;
    disconnect(c);
    free(c);
}

static const alelt_source_vtbl k_vtbl = {
    ALELT_PLUGIN_ABI_VERSION, "db2", PLUGIN_VERSION, p_open, p_probe_position, p_read, p_catalog_json,
    p_snapshot_chunk,      p_probe, compare_pos,  pos_valid, p_last_error, p_close, p_verify_table,
};

__declspec(dllexport) const alelt_source_vtbl *alelt_source_get_vtbl(void)
{
    return &k_vtbl;
}
