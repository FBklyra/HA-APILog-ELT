/* SPDX-License-Identifier: Apache-2.0 */
#include "alelt_json.h"

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ arena */
typedef struct fj_block {
    struct fj_block *next;
    size_t used, cap;
    /* payload follows, max-aligned */
} fj_block;

struct fj_doc {
    fj_block *blocks;
    fj_val *root;
};

#define FJ_ALIGN 16u
#define FJ_HDR (((sizeof(fj_block) + FJ_ALIGN - 1) / FJ_ALIGN) * FJ_ALIGN)

static void *fj_alloc(fj_doc *d, size_t n)
{
    fj_block *b = d->blocks;
    uint8_t *p;
    n = (n + FJ_ALIGN - 1) & ~(size_t)(FJ_ALIGN - 1);
    if (!b || b->cap - b->used < n) {
        size_t cap = n > 65536 ? n : 65536;
        b = (fj_block *)malloc(FJ_HDR + cap);
        if (!b) {
            return NULL;
        }
        b->next = d->blocks;
        b->used = 0;
        b->cap = cap;
        d->blocks = b;
    }
    p = (uint8_t *)b + FJ_HDR + b->used;
    b->used += n;
    return p;
}

void fj_free(fj_doc *d)
{
    fj_block *b, *nb;
    if (!d) {
        return;
    }
    for (b = d->blocks; b; b = nb) {
        nb = b->next;
        free(b);
    }
    free(d);
}

const fj_val *fj_root(const fj_doc *d)
{
    return d ? d->root : NULL;
}

/* ------------------------------------------------------------------ parser */
typedef struct fj_p {
    const char *s;
    size_t len, pos;
    fj_doc *d;
    char *err;
    size_t errcap;
    int failed;
} fj_p;

static void fj_fail(fj_p *p, const char *msg)
{
    if (!p->failed) {
        p->failed = 1;
        if (p->err && p->errcap) {
            snprintf(p->err, p->errcap, "json: %s at offset %zu", msg, p->pos);
        }
    }
}

static void fj_ws(fj_p *p)
{
    while (p->pos < p->len) {
        char c = p->s[p->pos];
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
            p->pos++;
        } else {
            break;
        }
    }
}

static int hexv(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static int read_u4(fj_p *p, unsigned *out)
{
    unsigned v = 0;
    int i;
    if (p->len - p->pos < 4) {
        return -1;
    }
    for (i = 0; i < 4; i++) {
        int h = hexv(p->s[p->pos + (size_t)i]);
        if (h < 0) {
            return -1;
        }
        v = (v << 4) | (unsigned)h;
    }
    p->pos += 4;
    *out = v;
    return 0;
}

/* Validate one UTF-8 sequence starting at s[i]; returns its length or 0. */
static size_t utf8_len(const unsigned char *s, size_t avail)
{
    unsigned c = s[0];
    if (c < 0x80) return 1;
    if (c >= 0xC2 && c <= 0xDF) {
        if (avail < 2 || (s[1] & 0xC0) != 0x80) return 0;
        return 2;
    }
    if (c >= 0xE0 && c <= 0xEF) {
        if (avail < 3 || (s[1] & 0xC0) != 0x80 || (s[2] & 0xC0) != 0x80) return 0;
        if (c == 0xE0 && s[1] < 0xA0) return 0;        /* overlong */
        if (c == 0xED && s[1] >= 0xA0) return 0;       /* surrogates */
        return 3;
    }
    if (c >= 0xF0 && c <= 0xF4) {
        if (avail < 4 || (s[1] & 0xC0) != 0x80 || (s[2] & 0xC0) != 0x80 || (s[3] & 0xC0) != 0x80) return 0;
        if (c == 0xF0 && s[1] < 0x90) return 0;
        if (c == 0xF4 && s[1] >= 0x90) return 0;
        return 4;
    }
    return 0;
}

static size_t put_utf8(char *o, unsigned cp)
{
    if (cp < 0x80) {
        o[0] = (char)cp;
        return 1;
    }
    if (cp < 0x800) {
        o[0] = (char)(0xC0 | (cp >> 6));
        o[1] = (char)(0x80 | (cp & 0x3F));
        return 2;
    }
    if (cp < 0x10000) {
        o[0] = (char)(0xE0 | (cp >> 12));
        o[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
        o[2] = (char)(0x80 | (cp & 0x3F));
        return 3;
    }
    o[0] = (char)(0xF0 | (cp >> 18));
    o[1] = (char)(0x80 | ((cp >> 12) & 0x3F));
    o[2] = (char)(0x80 | ((cp >> 6) & 0x3F));
    o[3] = (char)(0x80 | (cp & 0x3F));
    return 4;
}

/* Parses a string at p->pos (which points at the opening quote). */
static int fj_string(fj_p *p, const char **out, size_t *outlen)
{
    size_t start, i, maxout = 0;
    char *o, *w;
    p->pos++; /* opening quote */
    start = p->pos;
    /* First pass: find the end and validate; output is never longer than input. */
    for (i = start; i < p->len; i++) {
        unsigned char c = (unsigned char)p->s[i];
        if (c == '"') {
            break;
        }
        if (c == '\\') {
            i++;
            if (i >= p->len) {
                break;
            }
            continue;
        }
        if (c < 0x20) {
            p->pos = i;
            fj_fail(p, "control character in string");
            return -1;
        }
    }
    if (i >= p->len) {
        fj_fail(p, "unterminated string");
        return -1;
    }
    maxout = i - start;
    o = (char *)fj_alloc(p->d, maxout + 1);
    if (!o) {
        fj_fail(p, "out of memory");
        return -1;
    }
    w = o;
    while (p->pos < p->len) {
        unsigned char c = (unsigned char)p->s[p->pos];
        if (c == '"') {
            p->pos++;
            *w = 0;
            *out = o;
            *outlen = (size_t)(w - o);
            return 0;
        }
        if (c == '\\') {
            char e;
            p->pos++;
            e = p->s[p->pos++];
            switch (e) {
            case '"': *w++ = '"'; break;
            case '\\': *w++ = '\\'; break;
            case '/': *w++ = '/'; break;
            case 'b': *w++ = '\b'; break;
            case 'f': *w++ = '\f'; break;
            case 'n': *w++ = '\n'; break;
            case 'r': *w++ = '\r'; break;
            case 't': *w++ = '\t'; break;
            case 'u': {
                unsigned cp, lo;
                if (read_u4(p, &cp) != 0) {
                    fj_fail(p, "bad \\u escape");
                    return -1;
                }
                if (cp >= 0xD800 && cp <= 0xDBFF) {
                    if (p->len - p->pos < 6 || p->s[p->pos] != '\\' || p->s[p->pos + 1] != 'u') {
                        fj_fail(p, "unpaired surrogate");
                        return -1;
                    }
                    p->pos += 2;
                    if (read_u4(p, &lo) != 0 || lo < 0xDC00 || lo > 0xDFFF) {
                        fj_fail(p, "bad low surrogate");
                        return -1;
                    }
                    cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                } else if (cp >= 0xDC00 && cp <= 0xDFFF) {
                    fj_fail(p, "unpaired surrogate");
                    return -1;
                }
                if (cp == 0) {
                    fj_fail(p, "NUL not allowed in string");
                    return -1;
                }
                /* \uXXXX is 6 input bytes and yields <= 4 output bytes; pairs 12 -> 4. */
                w += put_utf8(w, cp);
                break;
            }
            default:
                fj_fail(p, "bad escape");
                return -1;
            }
        } else {
            size_t n = utf8_len((const unsigned char *)p->s + p->pos, p->len - p->pos);
            if (n == 0) {
                fj_fail(p, "invalid UTF-8");
                return -1;
            }
            memcpy(w, p->s + p->pos, n);
            w += n;
            p->pos += n;
        }
    }
    fj_fail(p, "unterminated string");
    return -1;
}

static int fj_number(fj_p *p, fj_val *v)
{
    size_t st = p->pos;
    char *t;
    if (p->pos < p->len && p->s[p->pos] == '-') p->pos++;
    if (p->pos >= p->len) { fj_fail(p, "bad number"); return -1; }
    if (p->s[p->pos] == '0') {
        p->pos++;
    } else if (p->s[p->pos] >= '1' && p->s[p->pos] <= '9') {
        while (p->pos < p->len && p->s[p->pos] >= '0' && p->s[p->pos] <= '9') p->pos++;
    } else {
        fj_fail(p, "bad number");
        return -1;
    }
    if (p->pos < p->len && p->s[p->pos] == '.') {
        p->pos++;
        if (p->pos >= p->len || p->s[p->pos] < '0' || p->s[p->pos] > '9') { fj_fail(p, "bad fraction"); return -1; }
        while (p->pos < p->len && p->s[p->pos] >= '0' && p->s[p->pos] <= '9') p->pos++;
    }
    if (p->pos < p->len && (p->s[p->pos] == 'e' || p->s[p->pos] == 'E')) {
        p->pos++;
        if (p->pos < p->len && (p->s[p->pos] == '+' || p->s[p->pos] == '-')) p->pos++;
        if (p->pos >= p->len || p->s[p->pos] < '0' || p->s[p->pos] > '9') { fj_fail(p, "bad exponent"); return -1; }
        while (p->pos < p->len && p->s[p->pos] >= '0' && p->s[p->pos] <= '9') p->pos++;
    }
    if (p->pos - st > 64) { fj_fail(p, "number too long"); return -1; }
    t = (char *)fj_alloc(p->d, p->pos - st + 1);
    if (!t) { fj_fail(p, "out of memory"); return -1; }
    memcpy(t, p->s + st, p->pos - st);
    t[p->pos - st] = 0;
    v->type = FJ_NUM;
    v->s = t;
    v->n = p->pos - st;
    return 0;
}

static int lit(fj_p *p, const char *w)
{
    size_t n = strlen(w);
    if (p->len - p->pos < n || memcmp(p->s + p->pos, w, n) != 0) {
        fj_fail(p, "bad literal");
        return -1;
    }
    p->pos += n;
    return 0;
}

static fj_val *fj_value(fj_p *p, int depth);

static fj_val *new_val(fj_p *p)
{
    fj_val *v = (fj_val *)fj_alloc(p->d, sizeof(fj_val));
    if (!v) {
        fj_fail(p, "out of memory");
        return NULL;
    }
    memset(v, 0, sizeof(*v));
    return v;
}

/* Duplicate-key detection (AHS-16): linear for small objects, an open-addressing hash
 * set of member pointers once an object grows past FJ_DUP_LINEAR members, so a hostile
 * 1 M-key object costs O(n) instead of O(n^2). */
#define FJ_DUP_LINEAR 16u
typedef struct fj_keyset {
    const fj_val **slot;
    size_t cap, n;
} fj_keyset;

static uint32_t fj_khash(const char *k, size_t n)
{
    uint32_t h = 2166136261u;
    size_t i;
    for (i = 0; i < n; i++) h = (h ^ (uint8_t)k[i]) * 16777619u;
    return h;
}

/* 1 = inserted, 0 = duplicate, -1 = out of memory */
static int fj_keyset_add(fj_keyset *ks, const fj_val *m)
{
    size_t i;
    if ((ks->n + 1) * 2 > ks->cap) {
        size_t nc = ks->cap ? ks->cap * 2 : 64, j;
        const fj_val **ns = (const fj_val **)calloc(nc, sizeof(*ns));
        if (!ns) return -1;
        for (j = 0; j < ks->cap; j++) {
            if (ks->slot[j]) {
                size_t k = fj_khash(ks->slot[j]->key, ks->slot[j]->keylen) & (nc - 1);
                while (ns[k]) k = (k + 1) & (nc - 1);
                ns[k] = ks->slot[j];
            }
        }
        free((void *)ks->slot);
        ks->slot = ns;
        ks->cap = nc;
    }
    i = fj_khash(m->key, m->keylen) & (ks->cap - 1);
    while (ks->slot[i]) {
        if (ks->slot[i]->keylen == m->keylen && memcmp(ks->slot[i]->key, m->key, m->keylen) == 0) return 0;
        i = (i + 1) & (ks->cap - 1);
    }
    ks->slot[i] = m;
    ks->n++;
    return 1;
}

static fj_val *fj_value(fj_p *p, int depth)
{
    fj_val *v;
    char c;
    if (depth > FJ_MAX_DEPTH) {
        fj_fail(p, "nesting too deep");
        return NULL;
    }
    fj_ws(p);
    if (p->pos >= p->len) {
        fj_fail(p, "unexpected end");
        return NULL;
    }
    v = new_val(p);
    if (!v) return NULL;
    c = p->s[p->pos];
    if (c == '{') {
        fj_val *last = NULL;
        fj_keyset ks = {NULL, 0, 0};
        p->pos++;
        v->type = FJ_OBJ;
        fj_ws(p);
        if (p->pos < p->len && p->s[p->pos] == '}') {
            p->pos++;
            return v;
        }
        for (;;) {
            const char *k;
            size_t kl;
            fj_val *m, *q;
            fj_ws(p);
            if (p->pos >= p->len || p->s[p->pos] != '"') { fj_fail(p, "expected key"); goto obj_fail; }
            if (fj_string(p, &k, &kl) != 0) goto obj_fail;
            if (v->n < FJ_DUP_LINEAR) {
                for (q = v->child; q; q = q->next) {
                    if (q->keylen == kl && memcmp(q->key, k, kl) == 0) { fj_fail(p, "duplicate key"); goto obj_fail; }
                }
            }
            fj_ws(p);
            if (p->pos >= p->len || p->s[p->pos] != ':') { fj_fail(p, "expected ':'"); goto obj_fail; }
            p->pos++;
            m = fj_value(p, depth + 1);
            if (!m) goto obj_fail;
            m->key = k;
            m->keylen = kl;
            if (v->n + 1 >= FJ_DUP_LINEAR) {
                int r;
                if (!ks.slot) { /* seed the set with the members checked linearly so far */
                    for (q = v->child; q; q = q->next) {
                        if (fj_keyset_add(&ks, q) < 0) { fj_fail(p, "out of memory"); goto obj_fail; }
                    }
                }
                r = fj_keyset_add(&ks, m);
                if (r < 0) { fj_fail(p, "out of memory"); goto obj_fail; }
                if (r == 0) { fj_fail(p, "duplicate key"); goto obj_fail; }
            }
            if (last) last->next = m; else v->child = m;
            last = m;
            v->n++;
            fj_ws(p);
            if (p->pos < p->len && p->s[p->pos] == ',') { p->pos++; continue; }
            if (p->pos < p->len && p->s[p->pos] == '}') { p->pos++; free((void *)ks.slot); return v; }
            fj_fail(p, "expected ',' or '}'");
            goto obj_fail;
        }
    obj_fail:
        free((void *)ks.slot);
        return NULL;
    }
    if (c == '[') {
        fj_val *last = NULL;
        p->pos++;
        v->type = FJ_ARR;
        fj_ws(p);
        if (p->pos < p->len && p->s[p->pos] == ']') {
            p->pos++;
            return v;
        }
        for (;;) {
            fj_val *e = fj_value(p, depth + 1);
            if (!e) return NULL;
            if (last) last->next = e; else v->child = e;
            last = e;
            v->n++;
            fj_ws(p);
            if (p->pos < p->len && p->s[p->pos] == ',') { p->pos++; continue; }
            if (p->pos < p->len && p->s[p->pos] == ']') { p->pos++; return v; }
            fj_fail(p, "expected ',' or ']'");
            return NULL;
        }
    }
    if (c == '"') {
        v->type = FJ_STR;
        if (fj_string(p, &v->s, &v->n) != 0) return NULL;
        return v;
    }
    if (c == 't') { if (lit(p, "true")) return NULL; v->type = FJ_TRUE; return v; }
    if (c == 'f') { if (lit(p, "false")) return NULL; v->type = FJ_FALSE; return v; }
    if (c == 'n') { if (lit(p, "null")) return NULL; v->type = FJ_NULL; return v; }
    if (c == '-' || (c >= '0' && c <= '9')) {
        if (fj_number(p, v) != 0) return NULL;
        return v;
    }
    fj_fail(p, "unexpected character");
    return NULL;
}

fj_doc *fj_parse(const char *json, size_t len, char *err, size_t errcap)
{
    fj_p p;
    fj_doc *d;
    if (err && errcap) err[0] = 0;
    if (!json || len > FJ_MAX_INPUT) {
        if (err && errcap) snprintf(err, errcap, "json: input missing or too large");
        return NULL;
    }
    d = (fj_doc *)calloc(1, sizeof(fj_doc));
    if (!d) return NULL;
    memset(&p, 0, sizeof(p));
    p.s = json;
    p.len = len;
    p.d = d;
    p.err = err;
    p.errcap = errcap;
    /* Optional UTF-8 BOM (config files edited with Notepad). */
    if (len >= 3 && (unsigned char)json[0] == 0xEF && (unsigned char)json[1] == 0xBB && (unsigned char)json[2] == 0xBF) {
        p.pos = 3;
    }
    d->root = fj_value(&p, 0);
    if (d->root) {
        fj_ws(&p);
        if (p.pos != p.len) {
            fj_fail(&p, "trailing data");
        }
    }
    if (p.failed || !d->root) {
        fj_free(d);
        return NULL;
    }
    return d;
}

const fj_val *fj_get(const fj_val *obj, const char *key)
{
    const fj_val *c;
    size_t kl;
    if (!obj || obj->type != FJ_OBJ || !key) return NULL;
    kl = strlen(key);
    for (c = obj->child; c; c = c->next) {
        if (c->keylen == kl && memcmp(c->key, key, kl) == 0) return c;
    }
    return NULL;
}

const char *fj_get_str(const fj_val *obj, const char *key)
{
    const fj_val *v = fj_get(obj, key);
    return (v && v->type == FJ_STR) ? v->s : NULL;
}

int fj_as_i64(const fj_val *v, int64_t *out)
{
    char *end = NULL;
    long long x;
    if (!v || v->type != FJ_NUM) return -2;
    if (strchr(v->s, '.') || strchr(v->s, 'e') || strchr(v->s, 'E')) return -2;
    errno = 0;
    x = strtoll(v->s, &end, 10);
    if (errno != 0 || !end || *end != 0) return -2;
    *out = (int64_t)x;
    return 0;
}

int fj_as_dbl(const fj_val *v, double *out)
{
    char *end = NULL;
    double x;
    if (!v || v->type != FJ_NUM) return -2;
    errno = 0;
    x = strtod(v->s, &end);
    if (errno != 0 || !end || *end != 0 || !isfinite(x)) return -2;
    *out = x;
    return 0;
}

int fj_get_i64(const fj_val *obj, const char *key, int64_t *out)
{
    const fj_val *v = fj_get(obj, key);
    if (!v) return -1;
    return fj_as_i64(v, out);
}

int fj_get_bool(const fj_val *obj, const char *key, int *out)
{
    const fj_val *v = fj_get(obj, key);
    if (!v) return -1;
    if (v->type == FJ_TRUE) { *out = 1; return 0; }
    if (v->type == FJ_FALSE) { *out = 0; return 0; }
    return -2;
}

int fj_keys_only(const fj_val *obj, const char *const *allowed, const char **bad_key)
{
    const fj_val *c;
    if (!obj || obj->type != FJ_OBJ) return 0;
    for (c = obj->child; c; c = c->next) {
        const char *const *a;
        int ok = 0;
        for (a = allowed; *a; a++) {
            if (strlen(*a) == c->keylen && memcmp(*a, c->key, c->keylen) == 0) { ok = 1; break; }
        }
        if (!ok) {
            if (bad_key) *bad_key = c->key;
            return 0;
        }
    }
    return 1;
}

/* ------------------------------------------------------------------ writer */
int fj_escape(alelt_buf *b, const char *s, size_t n)
{
    static const char hx[] = "0123456789abcdef";
    size_t i, run = 0;
    if (alelt_buf_putc(b, '"')) return -1;
    for (i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        const char *rep = NULL;
        char u[7];
        switch (c) {
        case '"': rep = "\\\""; break;
        case '\\': rep = "\\\\"; break;
        case '\n': rep = "\\n"; break;
        case '\r': rep = "\\r"; break;
        case '\t': rep = "\\t"; break;
        case '\b': rep = "\\b"; break;
        case '\f': rep = "\\f"; break;
        default:
            if (c < 0x20 || c == 0x7F) {
                u[0] = '\\'; u[1] = 'u'; u[2] = '0'; u[3] = '0';
                u[4] = hx[c >> 4]; u[5] = hx[c & 15]; u[6] = 0;
                rep = u;
            }
        }
        if (rep) {
            if (run && alelt_buf_append(b, s + i - run, run)) return -1;
            run = 0;
            if (alelt_buf_puts(b, rep)) return -1;
        } else {
            run++;
        }
    }
    if (run && alelt_buf_append(b, s + n - run, run)) return -1;
    return alelt_buf_putc(b, '"');
}

void jw_init(alelt_jw *w, alelt_buf *b)
{
    w->b = b;
    w->depth = 0;
    w->after_key = 0;
    w->need_comma = 0;
    w->failed = 0;
}

static void jw_pre(alelt_jw *w)
{
    if (w->after_key) {
        w->after_key = 0;
        return;
    }
    if (w->depth >= 64) { w->failed = 1; return; }
    if (w->need_comma & ((uint64_t)1 << w->depth)) {
        if (alelt_buf_putc(w->b, ',')) w->failed = 1;
    }
    w->need_comma |= ((uint64_t)1 << w->depth);
}

static void jw_open(alelt_jw *w, char c)
{
    jw_pre(w);
    if (alelt_buf_putc(w->b, c)) w->failed = 1;
    w->depth++;
    if (w->depth >= 64) { w->failed = 1; return; }
    w->need_comma &= ~((uint64_t)1 << w->depth);
}

static void jw_close(alelt_jw *w, char c)
{
    if (w->depth <= 0 || w->after_key) { w->failed = 1; return; }
    if (alelt_buf_putc(w->b, c)) w->failed = 1;
    w->depth--;
}

void jw_obj(alelt_jw *w) { jw_open(w, '{'); }
void jw_obj_end(alelt_jw *w) { jw_close(w, '}'); }
void jw_arr(alelt_jw *w) { jw_open(w, '['); }
void jw_arr_end(alelt_jw *w) { jw_close(w, ']'); }

void jw_key(alelt_jw *w, const char *k)
{
    if (w->after_key) { w->failed = 1; return; }
    jw_pre(w);
    if (fj_escape(w->b, k, strlen(k)) || alelt_buf_putc(w->b, ':')) w->failed = 1;
    w->after_key = 1;
}

void jw_strn(alelt_jw *w, const char *s, size_t n)
{
    jw_pre(w);
    if (fj_escape(w->b, s, n)) w->failed = 1;
}

void jw_str(alelt_jw *w, const char *s) { jw_strn(w, s, strlen(s)); }

void jw_str_or_null(alelt_jw *w, const char *s)
{
    if (s && *s) jw_str(w, s); else jw_null(w);
}

void jw_i64(alelt_jw *w, int64_t v)
{
    jw_pre(w);
    if (alelt_buf_printf(w->b, "%lld", (long long)v)) w->failed = 1;
}

void jw_u64(alelt_jw *w, uint64_t v)
{
    jw_pre(w);
    if (alelt_buf_printf(w->b, "%llu", (unsigned long long)v)) w->failed = 1;
}

void jw_dbl(alelt_jw *w, double v)
{
    jw_pre(w);
    if (!isfinite(v)) v = 0.0;
    if (alelt_buf_printf(w->b, "%.3f", v)) w->failed = 1;
}

void jw_bool(alelt_jw *w, int v)
{
    jw_pre(w);
    if (alelt_buf_puts(w->b, v ? "true" : "false")) w->failed = 1;
}

void jw_null(alelt_jw *w)
{
    jw_pre(w);
    if (alelt_buf_puts(w->b, "null")) w->failed = 1;
}

void jw_raw(alelt_jw *w, const char *json, size_t n)
{
    jw_pre(w);
    if (alelt_buf_append(w->b, json, n)) w->failed = 1;
}

int jw_ok(const alelt_jw *w)
{
    return (!w->failed && !w->b->failed && w->depth == 0 && !w->after_key) ? 0 : -1;
}
