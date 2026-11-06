/* ALELT - minimal strict JSON reader (DOM, arena) and writer.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Reader: RFC 8259, UTF-8 in/out, depth-limited, duplicate keys rejected,
 * no trailing garbage, no comments. Strings are unescaped and NUL-terminated
 * (embedded NUL via \u0000 is rejected to keep C-string semantics safe).
 */
#ifndef ALELT_JSON_H
#define ALELT_JSON_H

#include <stddef.h>
#include <stdint.h>

#include "alelt_buf.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FJ_MAX_DEPTH 64
#define FJ_MAX_INPUT ((size_t)64u * 1024u * 1024u)

typedef enum fj_type { FJ_NULL = 0, FJ_FALSE, FJ_TRUE, FJ_NUM, FJ_STR, FJ_ARR, FJ_OBJ } fj_type;

typedef struct fj_val {
    fj_type type;
    size_t n;                 /* string byte length; array/object element count */
    const char *s;            /* FJ_STR: unescaped text; FJ_NUM: literal text   */
    const char *key;          /* member name when the value is inside an object */
    size_t keylen;
    struct fj_val *child;     /* first element/member                           */
    struct fj_val *next;      /* next sibling                                   */
} fj_val;

typedef struct fj_doc fj_doc;

fj_doc *fj_parse(const char *json, size_t len, char *err, size_t errcap);
void fj_free(fj_doc *d);
const fj_val *fj_root(const fj_doc *d);

const fj_val *fj_get(const fj_val *obj, const char *key);
/* NULL if missing or not a string. */
const char *fj_get_str(const fj_val *obj, const char *key);
/* 0 ok, -1 missing, -2 wrong type / out of range. */
int fj_get_i64(const fj_val *obj, const char *key, int64_t *out);
int fj_get_bool(const fj_val *obj, const char *key, int *out);
int fj_as_i64(const fj_val *v, int64_t *out);
int fj_as_dbl(const fj_val *v, double *out);
/* Returns 1 if every member key of obj is in the NULL-terminated list. */
int fj_keys_only(const fj_val *obj, const char *const *allowed, const char **bad_key);

/* ------------------------------------------------------------------ writer */
typedef struct alelt_jw {
    alelt_buf *b;
    int depth;
    int after_key;
    uint64_t need_comma; /* one bit per depth level */
    int failed;
} alelt_jw;

void jw_init(alelt_jw *w, alelt_buf *b);
void jw_obj(alelt_jw *w);
void jw_obj_end(alelt_jw *w);
void jw_arr(alelt_jw *w);
void jw_arr_end(alelt_jw *w);
void jw_key(alelt_jw *w, const char *k);
void jw_str(alelt_jw *w, const char *s);
void jw_strn(alelt_jw *w, const char *s, size_t n);
void jw_str_or_null(alelt_jw *w, const char *s);
void jw_i64(alelt_jw *w, int64_t v);
void jw_u64(alelt_jw *w, uint64_t v);
void jw_dbl(alelt_jw *w, double v);
void jw_bool(alelt_jw *w, int v);
void jw_null(alelt_jw *w);
/* Insert pre-validated JSON text as a value. */
void jw_raw(alelt_jw *w, const char *json, size_t n);
/* 0 if every write succeeded and all containers are closed. */
int jw_ok(const alelt_jw *w);

/* Escape s into b as a JSON string literal including quotes. */
int fj_escape(alelt_buf *b, const char *s, size_t n);

#ifdef __cplusplus
}
#endif
#endif
