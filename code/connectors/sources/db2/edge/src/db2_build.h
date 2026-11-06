/* SPDX-License-Identifier: Apache-2.0
 * Builders for the only data-reading statements of the Db2 plugin (R1):
 * snapshot chunks and VERIFY_TABLE reads. Pure C (no CLI) so tests/test_db2.c
 * checks every statement shape. Output = fixed fragments (db2_sql.h) +
 * catalog-validated quoted identifiers + parameter markers. Every statement is
 * a SELECT ending in WITH CS (cursor stability; with the database parameter
 * CUR_COMMIT=ON readers see the currently committed version and never wait
 * on, or return, uncommitted rows - F-04 / X-123).
 */
#ifndef ALELT_DB2_BUILD_H
#define ALELT_DB2_BUILD_H

#include <stddef.h>

#include "alelt_buf.h"
#include "alelt_plugin.h"

#define DB2_COLNAME_MAX (ALELT_IDENT_MAX * 2)
#define DB2_TYPENAME_MAX 32
/* select-list items per aggregate statement (Db2 limit 1012) */
#define DB2_AGG_ITEMS_MAX 900

typedef struct db2_meta {
    int ncols, nkeys;
    char (*cols)[DB2_COLNAME_MAX];
    char (*types)[DB2_TYPENAME_MAX];
    long long *codepage;
    int keyidx[ALELT_MAX_KEYCOLS]; /* column index (0-based) per key position */
} db2_meta;

/* Aggregate class of a column (= alelt_common.models.VERIFY_AGG_CLASSES). */
typedef enum db2_agg { AGG_INT = 0, AGG_DEC, AGG_FLT, AGG_TMP, AGG_STR, AGG_CNT } db2_agg;
db2_agg db2_agg_class(const char *typename_, long long codepage);
const char *db2_agg_name(db2_agg a);
int db2_agg_items(db2_agg a); /* result columns one column contributes */

int db2_quote_ident(alelt_buf *b, const char *id);

/* Catalog normalisation shared with the hub / dev edge catalogs (XC-24; golden file
 * tests/catalog_golden.tsv, checked by tests/test_db2.c and tests/golden/db2). */
/* SYSCAT.COLUMNS TYPENAME (+ CODEPAGE, 0 = FOR BIT DATA) -> canonical type name (alelt_src_db2.types). */
const char *db2_canonical_type(const char *typename_, long long codepage);
/* SYSCAT.TABLES COMPRESSION / ROWCOMPMODE -> "<COMPRESSION>[/<ROWCOMPMODE>]" ("" = null). 0 ok, -1 too small. */
int db2_compression_text(const char *comp, const char *mode, char *out, size_t cap);

/* Final shape guard (R1): "SELECT ..." ending in " WITH CS", no ';'. 1 = ok. */
int db2_built_ok(const char *sql, size_t len);

/* ptab receives, in marker order, the key position bound to each marker:
 * k = key_after[k] / key_values[k], 100 + k = key_to[k]. */
#define DB2_MAX_MARKERS (ALELT_MAX_KEYCOLS * (ALELT_MAX_KEYCOLS + 2) * 2)

/* SELECT <all cols> FROM "S"."T" [WHERE range] ORDER BY <pk> FETCH FIRST n ROWS ONLY WITH CS */
int db2_snapshot_sql(const char *schema, const char *table, const db2_meta *m, int has_after, int has_to,
                     unsigned chunk_rows, alelt_buf *sql, int *ptab, int *nparams);

/* SELECT COUNT(*)[, aggregates of cols[colidx[0..n)]] FROM "S"."T" [WHERE range] WITH CS */
int db2_verify_agg_sql(const char *schema, const char *table, const db2_meta *m, const int *colidx, int n,
                       int has_after, int has_to, alelt_buf *sql, int *ptab, int *nparams);

/* SELECT <pk> FROM "S"."T" [WHERE range] ORDER BY <pk> FETCH FIRST n ROWS ONLY WITH CS */
int db2_verify_keys_sql(const char *schema, const char *table, const db2_meta *m, int has_after, int has_to,
                        unsigned limit, alelt_buf *sql, int *ptab, int *nparams);

/* SELECT <cols[colidx]> FROM "S"."T" WHERE "k1" = ? AND ... WITH CS (keyed re-read) */
int db2_verify_rows_sql(const char *schema, const char *table, const db2_meta *m, const int *colidx, int n,
                        alelt_buf *sql, int *ptab, int *nparams);

#endif
