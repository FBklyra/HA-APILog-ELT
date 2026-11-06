/* SPDX-License-Identifier: Apache-2.0 */
#include <stdio.h>
#include <string.h>

#include "db2_build.h"
#include "db2_sql.h"

db2_agg db2_agg_class(const char *t, long long codepage)
{
    if (!strcmp(t, "SMALLINT") || !strcmp(t, "INTEGER") || !strcmp(t, "BIGINT")) return AGG_INT;
    if (!strcmp(t, "DECIMAL") || !strcmp(t, "NUMERIC")) return AGG_DEC;
    if (!strcmp(t, "REAL") || !strcmp(t, "DOUBLE") || !strcmp(t, "FLOAT") || !strcmp(t, "DECFLOAT")) return AGG_FLT;
    if (!strcmp(t, "DATE") || !strcmp(t, "TIME") || !strcmp(t, "TIMESTAMP")) return AGG_TMP;
    if ((!strcmp(t, "CHARACTER") || !strcmp(t, "VARCHAR") || !strcmp(t, "GRAPHIC") || !strcmp(t, "VARGRAPHIC")) &&
        codepage != 0)
        return AGG_STR;
    return AGG_CNT; /* LOB, XML, LONG VARCHAR/VARGRAPHIC, FOR BIT DATA, BOOLEAN, BINARY, distinct types */
}

const char *db2_agg_name(db2_agg a)
{
    static const char *const n[] = {"int", "dec", "flt", "tmp", "str", "cnt"};
    return (a >= AGG_INT && a <= AGG_CNT) ? n[a] : "cnt";
}

int db2_agg_items(db2_agg a)
{
    switch (a) {
    case AGG_INT:
    case AGG_DEC: return 4;
    case AGG_FLT:
    case AGG_TMP: return 3;
    case AGG_STR: return 2;
    default: return 1;
    }
}

int db2_quote_ident(alelt_buf *b, const char *id)
{
    const char *p;
    if (!*id) return -1;
    for (p = id; *p; p++) {
        if ((unsigned char)*p < 0x20 || *p == ';') return -1; /* never seen in the ERP schema; refuse oddities */
    }
    if (alelt_buf_putc(b, '"')) return -1;
    for (p = id; *p; p++) {
        if (*p == '"' && alelt_buf_putc(b, '"')) return -1;
        if (alelt_buf_putc(b, *p)) return -1;
    }
    return alelt_buf_putc(b, '"');
}

int db2_built_ok(const char *sql, size_t len)
{
    static const char suffix[] = DB2_ISO_CS;
    size_t ns = sizeof(suffix) - 1;
    if (len < 7 + ns || strncmp(sql, "SELECT ", 7) != 0) return 0;
    if (strcmp(sql + len - ns, suffix) != 0) return 0;
    if (memchr(sql, ';', len)) return 0;
    return 1;
}

static int from(alelt_buf *sql, const char *schema, const char *table)
{
    alelt_buf_puts(sql, DB2_SNAP_FROM);
    return db2_quote_ident(sql, schema) || alelt_buf_putc(sql, '.') || db2_quote_ident(sql, table);
}

/* Lexicographic key bounds: after = k1>? OR (k1=? AND k2>?) ...; to = k1<? OR (k1=? AND k2<?) ... OR (all =). */
static int key_where(alelt_buf *sql, const db2_meta *m, int has_after, int has_to, int *ptab, int *np)
{
    int i, j, any = 0;
    if (has_after || has_to) alelt_buf_puts(sql, DB2_SNAP_WHERE);
    if (has_after) {
        alelt_buf_putc(sql, '(');
        for (i = 0; i < m->nkeys; i++) {
            if (i) alelt_buf_puts(sql, " OR ");
            alelt_buf_putc(sql, '(');
            for (j = 0; j < i; j++) {
                if (db2_quote_ident(sql, m->cols[m->keyidx[j]])) return -1;
                alelt_buf_puts(sql, " = ? AND ");
                ptab[(*np)++] = j;
            }
            if (db2_quote_ident(sql, m->cols[m->keyidx[i]])) return -1;
            alelt_buf_puts(sql, " > ?)");
            ptab[(*np)++] = i;
        }
        alelt_buf_putc(sql, ')');
        any = 1;
    }
    if (has_to) {
        if (any) alelt_buf_puts(sql, " AND ");
        alelt_buf_putc(sql, '(');
        for (i = 0; i <= m->nkeys; i++) {
            int last = (i == m->nkeys);
            if (i) alelt_buf_puts(sql, " OR ");
            alelt_buf_putc(sql, '(');
            for (j = 0; j < (last ? m->nkeys : i); j++) {
                if (j) alelt_buf_puts(sql, " AND ");
                if (db2_quote_ident(sql, m->cols[m->keyidx[j]])) return -1;
                alelt_buf_puts(sql, " = ?");
                ptab[(*np)++] = 100 + j;
            }
            if (!last) {
                if (i) alelt_buf_puts(sql, " AND ");
                if (db2_quote_ident(sql, m->cols[m->keyidx[i]])) return -1;
                alelt_buf_puts(sql, " < ?");
                ptab[(*np)++] = 100 + i;
            }
            alelt_buf_putc(sql, ')');
        }
        alelt_buf_putc(sql, ')');
    }
    return 0;
}

static int order_by_key(alelt_buf *sql, const db2_meta *m)
{
    int i;
    alelt_buf_puts(sql, DB2_SNAP_ORDER);
    for (i = 0; i < m->nkeys; i++) {
        if (i) alelt_buf_putc(sql, ',');
        if (db2_quote_ident(sql, m->cols[m->keyidx[i]])) return -1;
    }
    return 0;
}

static int finish(alelt_buf *sql)
{
    if (sql->failed) return -1;
    return db2_built_ok(alelt_buf_cstr(sql), sql->len) ? 0 : -1;
}

int db2_snapshot_sql(const char *schema, const char *table, const db2_meta *m, int has_after, int has_to,
                     unsigned chunk_rows, alelt_buf *sql, int *ptab, int *nparams)
{
    int i;
    *nparams = 0;
    if (m->nkeys < 1) return -1;
    alelt_buf_puts(sql, DB2_SNAP_SELECT);
    for (i = 0; i < m->ncols; i++) {
        if (i) alelt_buf_putc(sql, ',');
        if (db2_quote_ident(sql, m->cols[i])) return -1;
    }
    if (from(sql, schema, table)) return -1;
    if (key_where(sql, m, has_after, has_to, ptab, nparams)) return -1;
    if (order_by_key(sql, m)) return -1;
    alelt_buf_printf(sql, DB2_SNAP_FETCH, chunk_rows);
    return finish(sql);
}

int db2_verify_agg_sql(const char *schema, const char *table, const db2_meta *m, const int *colidx, int n,
                       int has_after, int has_to, alelt_buf *sql, int *ptab, int *nparams)
{
    int i;
    *nparams = 0;
    alelt_buf_puts(sql, DB2_SNAP_SELECT);
    alelt_buf_puts(sql, DB2_COUNT_ALL);
    for (i = 0; i < n; i++) {
        const char *c = m->cols[colidx[i]];
        db2_agg a = db2_agg_class(m->types[colidx[i]], m->codepage[colidx[i]]);
        alelt_buf_putc(sql, ',');
        switch (a) {
        case AGG_INT:
        case AGG_DEC:
        case AGG_FLT:
        case AGG_TMP:
            alelt_buf_puts(sql, "COUNT(");
            if (db2_quote_ident(sql, c)) return -1;
            alelt_buf_puts(sql, "),MIN(");
            if (db2_quote_ident(sql, c)) return -1;
            alelt_buf_puts(sql, "),MAX(");
            if (db2_quote_ident(sql, c)) return -1;
            alelt_buf_putc(sql, ')');
            if (a == AGG_INT) {
                alelt_buf_puts(sql, ",SUM(CAST(");
                if (db2_quote_ident(sql, c)) return -1;
                alelt_buf_puts(sql, " AS DECIMAL(31,0)))");
            } else if (a == AGG_DEC) {
                alelt_buf_puts(sql, ",SUM(");
                if (db2_quote_ident(sql, c)) return -1;
                alelt_buf_putc(sql, ')');
            }
            break;
        case AGG_STR:
            alelt_buf_puts(sql, "COUNT(");
            if (db2_quote_ident(sql, c)) return -1;
            alelt_buf_puts(sql, "),SUM(CAST(CHARACTER_LENGTH(RTRIM(");
            if (db2_quote_ident(sql, c)) return -1;
            alelt_buf_puts(sql, "),CODEUNITS32) AS DECIMAL(31,0)))");
            break;
        default:
            alelt_buf_puts(sql, "SUM(CASE WHEN ");
            if (db2_quote_ident(sql, c)) return -1;
            alelt_buf_puts(sql, " IS NULL THEN 0 ELSE 1 END)");
            break;
        }
    }
    if (from(sql, schema, table)) return -1;
    if ((has_after || has_to) && m->nkeys < 1) return -1;
    if (key_where(sql, m, has_after, has_to, ptab, nparams)) return -1;
    alelt_buf_puts(sql, DB2_ISO_CS);
    return finish(sql);
}

int db2_verify_keys_sql(const char *schema, const char *table, const db2_meta *m, int has_after, int has_to,
                        unsigned limit, alelt_buf *sql, int *ptab, int *nparams)
{
    int i;
    *nparams = 0;
    if (m->nkeys < 1) return -1;
    alelt_buf_puts(sql, DB2_SNAP_SELECT);
    for (i = 0; i < m->nkeys; i++) {
        if (i) alelt_buf_putc(sql, ',');
        if (db2_quote_ident(sql, m->cols[m->keyidx[i]])) return -1;
    }
    if (from(sql, schema, table)) return -1;
    if (key_where(sql, m, has_after, has_to, ptab, nparams)) return -1;
    if (order_by_key(sql, m)) return -1;
    alelt_buf_printf(sql, DB2_SNAP_FETCH, limit);
    return finish(sql);
}

int db2_verify_rows_sql(const char *schema, const char *table, const db2_meta *m, const int *colidx, int n,
                        alelt_buf *sql, int *ptab, int *nparams)
{
    int i;
    *nparams = 0;
    if (m->nkeys < 1 || n < 1) return -1;
    alelt_buf_puts(sql, DB2_SNAP_SELECT);
    for (i = 0; i < n; i++) {
        if (i) alelt_buf_putc(sql, ',');
        if (db2_quote_ident(sql, m->cols[colidx[i]])) return -1;
    }
    if (from(sql, schema, table)) return -1;
    alelt_buf_puts(sql, DB2_SNAP_WHERE);
    for (i = 0; i < m->nkeys; i++) {
        if (i) alelt_buf_puts(sql, " AND ");
        if (db2_quote_ident(sql, m->cols[m->keyidx[i]])) return -1;
        alelt_buf_puts(sql, " = ?");
        ptab[(*nparams)++] = i;
    }
    alelt_buf_puts(sql, DB2_ISO_CS);
    return finish(sql);
}

/* ------------------------------------------------------------------ catalog normalisation (XC-24) */
static const char *trim1(const char *s, char *buf, size_t cap)
{
    size_t n;
    while (*s == ' ') s++;
    n = strlen(s);
    while (n > 0 && s[n - 1] == ' ') n--;
    if (n >= cap) n = cap - 1;
    memcpy(buf, s, n);
    buf[n] = 0;
    return buf;
}

const char *db2_canonical_type(const char *typename_, long long codepage)
{
    static const struct { const char *t, *c; } map[] = {
        {"SMALLINT", "SHORT"}, {"INTEGER", "INT"}, {"INT", "INT"}, {"BIGINT", "LONG"}, {"DECIMAL", "DECIMAL"},
        {"NUMERIC", "DECIMAL"}, {"DEC", "DECIMAL"}, {"REAL", "FLOAT"}, {"DOUBLE", "DOUBLE"}, {"FLOAT", "DOUBLE"},
        {"DATE", "LOCALDATE"}, {"TIME", "LOCALTIME"}, {"TIMESTAMP", "LOCALDATETIME"}, {"BOOLEAN", "BOOLEAN"},
        {"CHARACTER", "STRING"}, {"CHAR", "STRING"}, {"VARCHAR", "STRING"}, {"CHARACTER VARYING", "STRING"},
        {"GRAPHIC", "STRING"}, {"VARGRAPHIC", "STRING"}, {"CLOB", "STRING"}, {"DBCLOB", "STRING"},
        {"LONG VARCHAR", "STRING"}, {"LONG VARGRAPHIC", "STRING"}, {"BINARY", "BINARY"},
        {"VARBINARY", "BINARY"}, {"BLOB", "BINARY"}, {"XML", "XML"},
    };
    char t[DB2_TYPENAME_MAX];
    size_t i;
    trim1(typename_ ? typename_ : "", t, sizeof(t));
    for (i = 0; t[i]; i++) {
        if (t[i] >= 'a' && t[i] <= 'z') t[i] = (char)(t[i] - 'a' + 'A');
    }
    for (i = 0; i < sizeof(map) / sizeof(map[0]); i++) {
        if (strcmp(t, map[i].t) != 0) continue;
        /* FOR BIT DATA (code page 0): only CHARACTER / VARCHAR / CLOB become BINARY, as on the hub */
        if (codepage == 0 && (!strcmp(t, "CHARACTER") || !strcmp(t, "CHAR") || !strcmp(t, "VARCHAR") ||
                              !strcmp(t, "CHARACTER VARYING") || !strcmp(t, "CLOB")))
            return "BINARY";
        return map[i].c;
    }
    return "UNKNOWN"; /* DECFLOAT, distinct / structured types: the hub type map decides (R7) */
}

int db2_compression_text(const char *comp, const char *mode, char *out, size_t cap)
{
    char c[8], m[8];
    int n;
    trim1(comp ? comp : "", c, sizeof(c));
    trim1(mode ? mode : "", m, sizeof(m));
    if (!c[0]) n = snprintf(out, cap, "%s", "");
    else if (m[0]) n = snprintf(out, cap, "%s/%s", c, m);
    else n = snprintf(out, cap, "%s", c);
    return (n < 0 || (size_t)n >= cap) ? -1 : 0;
}
