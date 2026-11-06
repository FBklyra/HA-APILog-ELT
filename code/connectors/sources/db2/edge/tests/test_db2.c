/* SPDX-License-Identifier: Apache-2.0
 * Db2 plugin unit tests (no database needed):
 *   - LRI text form parse/format/order
 *   - filter-ON buffer framing walker
 *   - R1 guard: every compiled-in statement is a read-only SELECT ... WITH UR
 *     (catalog) and every built data statement (snapshot, VERIFY_TABLE) is a
 *     read-only SELECT ... WITH CS (F-04: never WITH UR on table data); the
 *     plugin source only prepares statements from that table or the builders.
 */
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "alelttest.h"
#include "db2_build.h"
#include "db2_lri.h"
#include "db2_sql.h"

#ifndef ALELT_DB2_SRC_DIR
#error "ALELT_DB2_SRC_DIR must point at connectors/sources/db2/edge/src"
#endif

static void t_lri(void)
{
    alelt_lri a, b;
    char s[64];
    CHECK(alelt_lri_parse("4697:0000000000041B88", &a) == 0 && a.part1 == 4697 && a.part2 == 0x41B88);
    CHECK(alelt_lri_format(&a, s, sizeof(s)) == 0);
    CHECK_STR(s, "4697:0000000000041B88");
    CHECK(alelt_lri_parse("4697:0000000000041b88", &b) == 0 && alelt_lri_cmp(&a, &b) == 0);
    CHECK(alelt_lri_parse("4698:0000000000000001", &b) == 0 && alelt_lri_cmp(&a, &b) < 0);
    CHECK(alelt_lri_parse("4697:0000000000041B89", &b) == 0 && alelt_lri_cmp(&b, &a) > 0);
    CHECK(alelt_lri_parse("18446744073709551615:FFFFFFFFFFFFFFFF", &b) == 0);
    CHECK(alelt_lri_parse("18446744073709551616:0000000000000000", &b) != 0); /* overflow */
    CHECK(alelt_lri_parse("1:41B88", &b) != 0);                               /* not 16 hex */
    CHECK(alelt_lri_parse("1:0000000000041B88 ", &b) != 0);
    CHECK(alelt_lri_parse(":0000000000041B88", &b) != 0);
    CHECK(alelt_lri_parse("x:0000000000041B88", &b) != 0);
    CHECK(alelt_lri_parse(NULL, &b) != 0);
    CHECK(alelt_lri_format(&a, s, 8) != 0);
}

static size_t put_rec(uint8_t *buf, size_t off, uint64_t p1, uint64_t p2, uint32_t size, int32_t sqlcode)
{
    uint64_t one = 1;
    memset(buf + off, 0, 56 + size);
    memcpy(buf + off, &one, 8);
    memcpy(buf + off + 8, &p1, 8);
    memcpy(buf + off + 16, &p2, 8);
    memcpy(buf + off + 52, &sqlcode, 4);
    memcpy(buf + off + 56, &size, 4);
    return off + 56 + size;
}

static void t_framing(void)
{
    uint8_t buf[1024];
    alelt_lri f, l;
    uint32_t bad;
    char err[128];
    size_t n = 0;
    n = put_rec(buf, n, 10, 0x100, 40, 0);
    n = put_rec(buf, n, 10, 0x200, 120, -204);
    n = put_rec(buf, n, 11, 0x5, 24, 0);
    CHECK(alelt_db2_frame_walk(buf, n, 3, &f, &l, &bad, err, sizeof(err)) == 0);
    CHECK(f.part1 == 10 && f.part2 == 0x100 && l.part1 == 11 && l.part2 == 0x5 && bad == 1);
    CHECK(alelt_db2_frame_walk(buf, n, 2, &f, &l, &bad, err, sizeof(err)) != 0);     /* leftover bytes */
    CHECK(alelt_db2_frame_walk(buf, n, 4, &f, &l, &bad, err, sizeof(err)) != 0);     /* too few bytes */
    CHECK(alelt_db2_frame_walk(buf, n - 1, 3, &f, &l, &bad, err, sizeof(err)) != 0); /* truncated record */
    {
        uint32_t huge = 0x7FFFFFFF;
        memcpy(buf + 56, &huge, 4);
        CHECK(alelt_db2_frame_walk(buf, n, 3, &f, &l, &bad, err, sizeof(err)) != 0); /* length overrun */
        huge = 4;
        memcpy(buf + 56, &huge, 4);
        CHECK(alelt_db2_frame_walk(buf, n, 3, &f, &l, &bad, err, sizeof(err)) != 0); /* implausibly small */
    }
    CHECK(alelt_db2_frame_walk(buf, 0, 0, &f, &l, &bad, err, sizeof(err)) == 0);
}

static int has_word(const char *s, const char *w)
{
    size_t n = strlen(w);
    const char *p = s;
    while ((p = strstr(p, w)) != NULL) {
        int before = (p == s) || !(isalnum((unsigned char)p[-1]) || p[-1] == '_');
        int after = !(isalnum((unsigned char)p[n]) || p[n] == '_');
        if (before && after) return 1;
        p += n;
    }
    return 0;
}

static const char *const k_forbidden[] = {
    "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER", "GRANT", "REVOKE", "CALL", "TRUNCATE",
    "EXECUTE", "EXEC", "LOCK", "REORG", "RUNSTATS", "LOAD", "IMPORT", "EXPORT", "ADMIN_CMD", "SET", "DECLARE",
    "COMMIT", "ROLLBACK", "RENAME", "COMMENT", "REFRESH", "INTO", "FOR UPDATE", NULL};

/* iso: " WITH UR" (catalog table) or " WITH CS" (built data reads) */
static int sql_ok_iso(const char *sql, const char *iso)
{
    const char *const *f;
    size_t n = strlen(sql);
    if (strncmp(sql, "SELECT ", 7) != 0) return 0;
    if (n < 8 || strcmp(sql + n - 8, iso) != 0) return 0;
    if (strchr(sql, ';') || strstr(sql, "--") || strstr(sql, "/*")) return 0;
    for (f = k_forbidden; *f; f++) {
        if (has_word(sql, *f)) return 0;
    }
    return 1;
}

static int sql_ok(const char *sql)
{
    return sql_ok_iso(sql, " WITH UR");
}

static void t_sql_table_readonly(void)
{
    int i;
    for (i = 0; i < Q__COUNT; i++) {
        int ok = db2_sql[i] != NULL && sql_ok(db2_sql[i]);
        if (!ok) printf("\n    non-read-only statement #%d: %s\n", i, db2_sql[i] ? db2_sql[i] : "(null)");
        CHECK(ok);
    }
    /* the builders' fixed fragments: data is read WITH CS, never WITH UR (F-04) */
    CHECK(strcmp(DB2_SNAP_SELECT, "SELECT ") == 0);
    CHECK(strstr(DB2_SNAP_FETCH, " WITH CS") != NULL && strstr(DB2_SNAP_FETCH, "UR") == NULL);
    CHECK(strcmp(DB2_ISO_CS, " WITH CS") == 0);
    /* CUR_COMMIT reported by the log_config probe (prerequisite check) */
    CHECK(strstr(db2_sql[Q_LOG_CFG], "'cur_commit'") != NULL);
    /* self-test of the checker */
    CHECK(!sql_ok("DELETE FROM X WITH UR"));
    CHECK(!sql_ok("SELECT 1 FROM X; DROP TABLE X WITH UR"));
    CHECK(!sql_ok("SELECT * FROM X FOR UPDATE WITH UR"));
    CHECK(!sql_ok("SELECT * FROM X"));
}

/* Every statement the builders can produce: read-only SELECT ... WITH CS,
 * markers only for key values, identifiers quoted. */
static void t_builders(void)
{
    static char cols[4][DB2_COLNAME_MAX] = {"ID", "AMT", "NAME", "NOTE"};
    static char types[4][DB2_TYPENAME_MAX] = {"INTEGER", "DECIMAL", "VARCHAR", "CLOB"};
    static long long cps[4] = {0, 0, 1208, 1208};
    static char cols2[3][DB2_COLNAME_MAX] = {"K1", "K2", "V"};
    static char types2[3][DB2_TYPENAME_MAX] = {"CHARACTER", "TIMESTAMP", "CHARACTER"};
    static long long cps2[3] = {1208, 0, 0};
    db2_meta m, m2;
    int ptab[DB2_MAX_MARKERS], np = 0, idx[4] = {0, 1, 2, 3};
    alelt_buf b;
    memset(&m, 0, sizeof(m));
    m.ncols = 4; m.nkeys = 1; m.cols = cols; m.types = types; m.codepage = cps; m.keyidx[0] = 0;
    memset(&m2, 0, sizeof(m2));
    m2.ncols = 3; m2.nkeys = 2; m2.cols = cols2; m2.types = types2; m2.codepage = cps2;
    m2.keyidx[0] = 0; m2.keyidx[1] = 1;

    CHECK(db2_agg_class("INTEGER", 0) == AGG_INT && db2_agg_class("DECIMAL", 0) == AGG_DEC);
    CHECK(db2_agg_class("DOUBLE", 0) == AGG_FLT && db2_agg_class("TIMESTAMP", 0) == AGG_TMP);
    CHECK(db2_agg_class("VARCHAR", 1208) == AGG_STR && db2_agg_class("VARCHAR", 0) == AGG_CNT); /* FOR BIT DATA */
    CHECK(db2_agg_class("CLOB", 1208) == AGG_CNT && db2_agg_class("XML", 0) == AGG_CNT);

    alelt_buf_init(&b, 1 << 20);
    CHECK(db2_snapshot_sql("S", "T", &m, 1, 1, 50, &b, ptab, &np) == 0);
    CHECK_STR(alelt_buf_cstr(&b), "SELECT \"ID\",\"AMT\",\"NAME\",\"NOTE\" FROM \"S\".\"T\" WHERE ((\"ID\" > ?)) AND "
                               "((\"ID\" < ?) OR (\"ID\" = ?)) ORDER BY \"ID\" FETCH FIRST 50 ROWS ONLY WITH CS");
    CHECK(np == 3 && ptab[0] == 0 && ptab[1] == 100 && ptab[2] == 100);
    CHECK(sql_ok_iso(alelt_buf_cstr(&b), " WITH CS"));

    alelt_buf_reset(&b);
    CHECK(db2_verify_agg_sql("S", "T", &m, idx, 4, 0, 1, &b, ptab, &np) == 0);
    CHECK_STR(alelt_buf_cstr(&b), "SELECT COUNT(*),COUNT(\"ID\"),MIN(\"ID\"),MAX(\"ID\"),SUM(CAST(\"ID\" AS DECIMAL(31,0))),"
                               "COUNT(\"AMT\"),MIN(\"AMT\"),MAX(\"AMT\"),SUM(\"AMT\"),"
                               "COUNT(\"NAME\"),SUM(CAST(CHARACTER_LENGTH(RTRIM(\"NAME\"),CODEUNITS32) AS DECIMAL(31,0))),"
                               "SUM(CASE WHEN \"NOTE\" IS NULL THEN 0 ELSE 1 END) FROM \"S\".\"T\" WHERE "
                               "((\"ID\" < ?) OR (\"ID\" = ?)) WITH CS");
    CHECK(np == 2 && sql_ok_iso(alelt_buf_cstr(&b), " WITH CS"));

    alelt_buf_reset(&b);
    CHECK(db2_verify_agg_sql("S", "T", &m, idx, 0, 0, 0, &b, ptab, &np) == 0); /* row count only */
    CHECK_STR(alelt_buf_cstr(&b), "SELECT COUNT(*) FROM \"S\".\"T\" WITH CS");
    CHECK(np == 0);

    alelt_buf_reset(&b);
    CHECK(db2_verify_keys_sql("S", "T", &m2, 1, 0, 10001, &b, ptab, &np) == 0);
    CHECK_STR(alelt_buf_cstr(&b), "SELECT \"K1\",\"K2\" FROM \"S\".\"T\" WHERE ((\"K1\" > ?) OR (\"K1\" = ? AND \"K2\" > ?)) "
                               "ORDER BY \"K1\",\"K2\" FETCH FIRST 10001 ROWS ONLY WITH CS");
    CHECK(np == 3 && ptab[0] == 0 && ptab[1] == 0 && ptab[2] == 1 && sql_ok_iso(alelt_buf_cstr(&b), " WITH CS"));

    alelt_buf_reset(&b);
    CHECK(db2_verify_rows_sql("S", "T", &m2, idx, 3, &b, ptab, &np) == 0);
    CHECK_STR(alelt_buf_cstr(&b), "SELECT \"K1\",\"K2\",\"V\" FROM \"S\".\"T\" WHERE \"K1\" = ? AND \"K2\" = ? WITH CS");
    CHECK(np == 2 && sql_ok_iso(alelt_buf_cstr(&b), " WITH CS"));

    /* hostile identifiers are quoted (embedded quotes doubled) or refused */
    {
        static char evil[1][DB2_COLNAME_MAX] = {"X\"; DROP TABLE T --"};
        static char et[1][DB2_TYPENAME_MAX] = {"INTEGER"};
        static long long ec[1] = {0};
        db2_meta e;
        memset(&e, 0, sizeof(e));
        e.ncols = 1; e.nkeys = 1; e.cols = evil; e.types = et; e.codepage = ec; e.keyidx[0] = 0;
        alelt_buf_reset(&b);
        CHECK(db2_verify_keys_sql("S", "T", &e, 0, 0, 5, &b, ptab, &np) != 0);   /* ';' refused */
        alelt_buf_reset(&b);
        CHECK(db2_verify_agg_sql("S\"X", "T", &m, idx, 1, 0, 0, &b, ptab, &np) == 0);
        CHECK(strstr(alelt_buf_cstr(&b), "\"S\"\"X\".\"T\"") != NULL);
    }
    /* the final guard */
    CHECK(db2_built_ok("SELECT 1 FROM X WITH CS", 23));
    CHECK(!db2_built_ok("SELECT 1 FROM X WITH UR", 23));
    CHECK(!db2_built_ok("SELECT 1 FROM X; DELETE FROM X WITH CS", 38));
    CHECK(!db2_built_ok("DELETE FROM X WITH CS", 21));
    /* keyless table: no keyed statements */
    m.nkeys = 0;
    alelt_buf_reset(&b);
    CHECK(db2_snapshot_sql("S", "T", &m, 0, 0, 5, &b, ptab, &np) != 0);
    alelt_buf_reset(&b);
    CHECK(db2_verify_rows_sql("S", "T", &m, idx, 1, &b, ptab, &np) != 0);
    alelt_buf_free(&b);
}

/* The plugin source may prepare/execute SQL only from db2_sql[] (st_open) or a
 * builder buffer (prep_built). SQLExecDirect is banned outright; no data
 * statement text may name UR isolation. */
static void t_source_scan(void)
{
    char path[1024], line[4096];
    FILE *f = NULL;
    int lineno = 0, prepares = 0, ok = 1;
    snprintf(path, sizeof(path), "%s/db2_plugin.c", ALELT_DB2_SRC_DIR);
    CHECK(fopen_s(&f, path, "r") == 0 && f);
    if (!f) return;
    while (fgets(line, sizeof(line), f)) {
        lineno++;
        if (strstr(line, "SQLExecDirect")) {
            printf("\n    %s:%d uses SQLExecDirect\n", path, lineno);
            ok = 0;
        }
        if (strstr(line, "SQLPrepare(") && !strstr(line, "\"SQLPrepare")) { /* skip message strings */
            prepares++;
            if (!strstr(line, "(SQLCHAR *)sql") && !strstr(line, "(SQLCHAR *)alelt_buf_cstr(sql)")) {
                printf("\n    %s:%d prepares an unexpected statement\n", path, lineno);
                ok = 0;
            }
        }
        if (strstr(line, "WITH UR\"")) {
            printf("\n    %s:%d names WITH UR outside db2_sql.c\n", path, lineno);
            ok = 0;
        }
        if (strstr(line, "st_open(") && !strstr(line, "db2_sql[") && !strstr(line, "static int st_open")) {
            printf("\n    %s:%d st_open without db2_sql[]\n", path, lineno);
            ok = 0;
        }
    }
    fclose(f);
    CHECK(ok);
    CHECK(prepares == 2); /* st_open + prep_built */
}

/* XC-24: same normalisation as the hub / dev edge catalogs, from the shared golden file */
static void t_catalog_golden(void)
{
    char path[1024], line[512];
    FILE *f;
    int n = 0;
    snprintf(path, sizeof(path), "%s/../tests/catalog_golden.tsv", ALELT_DB2_SRC_DIR);
    if (fopen_s(&f, path, "r") != 0) f = NULL;
    CHECK(f != NULL);
    if (!f) return;
    while (fgets(line, sizeof(line), f)) {
        char *k, *a, *b, *want, *ctx = NULL, got[16];
        size_t L = strlen(line);
        while (L > 0 && (line[L - 1] == '\n' || line[L - 1] == '\r')) line[--L] = 0;
        if (!line[0] || line[0] == '#') continue;
        k = strtok_s(line, "\t", &ctx);
        a = strtok_s(NULL, "\t", &ctx);
        b = strtok_s(NULL, "\t", &ctx);
        want = strtok_s(NULL, "\t", &ctx);
        CHECK(k && a && b && want);
        if (!(k && a && b && want)) break;
        if (!strcmp(a, "-")) a = "";
        if (!strcmp(b, "-")) b = "";
        if (!strcmp(k, "type")) {
            const char *c = db2_canonical_type(a, strtoll(b, NULL, 10));
            if (strcmp(c, want) != 0) printf("  type %s/%s: got %s want %s\n", a, b, c, want);
            CHECK(strcmp(c, want) == 0);
        } else {
            CHECK(db2_compression_text(a, b, got, sizeof(got)) == 0);
            if (strcmp(got[0] ? got : "-", want) != 0) printf("  comp %s/%s: got %s want %s\n", a, b, got, want);
            CHECK(strcmp(got[0] ? got : "-", want) == 0);
        }
        n++;
    }
    fclose(f);
    CHECK(n >= 50);
}

int main(void)
{
    printf("db2 plugin\n");
    RUN(t_lri);
    RUN(t_framing);
    RUN(t_sql_table_readonly);
    RUN(t_builders);
    RUN(t_source_scan);
    RUN(t_catalog_golden);
    return aleltt_report("db2 plugin unit tests");
}
