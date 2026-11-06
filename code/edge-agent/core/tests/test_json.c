/* SPDX-License-Identifier: Apache-2.0 */
#include <stdlib.h>

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

#include "alelt_json.h"
#include "alelttest.h"

static int parses(const char *s)
{
    char err[128];
    fj_doc *d = fj_parse(s, strlen(s), err, sizeof(err));
    int ok = d != NULL;
    fj_free(d);
    return ok;
}

static void t_valid(void)
{
    char err[128];
    int64_t v = 0;
    const char *s = "{\"a\":1,\"b\":[true,false,null],\"c\":\"x\\u00e9\\ud83d\\ude00\\n\",\"d\":-12.5e3}";
    fj_doc *d = fj_parse(s, strlen(s), err, sizeof(err));
    CHECK(d != NULL);
    if (!d) return;
    CHECK(fj_get_i64(fj_root(d), "a", &v) == 0 && v == 1);
    CHECK(fj_get(fj_root(d), "b")->n == 3);
    CHECK_STR(fj_get_str(fj_root(d), "c"), "x\xc3\xa9\xf0\x9f\x98\x80\n");
    CHECK(fj_get_i64(fj_root(d), "d", &v) == -2); /* not an integer */
    CHECK(fj_get(fj_root(d), "zz") == NULL);
    fj_free(d);
}

static void t_invalid(void)
{
    CHECK(!parses(""));
    CHECK(!parses("{"));
    CHECK(!parses("{\"a\":1,}"));
    CHECK(!parses("{\"a\":1}x"));
    CHECK(!parses("{\"a\":1,\"a\":2}"));        /* duplicate key */
    CHECK(!parses("\"\\u0000\""));               /* NUL */
    CHECK(!parses("\"\\ud800\""));               /* lone surrogate */
    CHECK(!parses("\"\xc3\x28\""));              /* invalid UTF-8 */
    CHECK(!parses("01"));
    CHECK(!parses("[1.]"));
    CHECK(!parses("\"a\tb\""));                  /* raw control char */
    CHECK(!parses("// c\n{}"));
    CHECK(parses("\xEF\xBB\xBF{}"));             /* BOM tolerated */
}

static void t_depth(void)
{
    char s[300];
    int i;
    for (i = 0; i < 100; i++) s[i] = '[';
    for (i = 0; i < 100; i++) s[100 + i] = ']';
    s[200] = 0;
    CHECK(!parses(s)); /* deeper than FJ_MAX_DEPTH */
    s[0] = 0;
    for (i = 0; i < 10; i++) strcat_s(s, sizeof(s), "[");
    for (i = 0; i < 10; i++) strcat_s(s, sizeof(s), "]");
    CHECK(parses(s));
}

static void t_writer_roundtrip(void)
{
    alelt_buf b;
    alelt_jw w;
    char err[128];
    fj_doc *d;
    alelt_buf_init(&b, 0);
    jw_init(&w, &b);
    jw_obj(&w);
    jw_key(&w, "s"); jw_str(&w, "q\"\\n\x01\x7f");
    jw_key(&w, "n"); jw_i64(&w, -42);
    jw_key(&w, "a"); jw_arr(&w); jw_bool(&w, 1); jw_null(&w); jw_u64(&w, 18446744073709551615ull); jw_arr_end(&w);
    jw_key(&w, "o"); jw_obj(&w); jw_obj_end(&w);
    jw_obj_end(&w);
    CHECK(jw_ok(&w) == 0);
    d = fj_parse((const char *)b.data, b.len, err, sizeof(err));
    CHECK(d != NULL);
    if (d) {
        CHECK_STR(fj_get_str(fj_root(d), "s"), "q\"\\n\x01\x7f");
        CHECK(fj_get(fj_root(d), "a")->n == 3);
        fj_free(d);
    }
    alelt_buf_reset(&b);
    jw_init(&w, &b);
    jw_obj(&w);
    jw_key(&w, "x");
    CHECK(jw_ok(&w) != 0); /* unbalanced */
    alelt_buf_free(&b);
}

/* AHS-16: duplicate keys rejected at any size; a 200 000-member object parses in linear time */
static void t_many_keys(void)
{
    size_t n = 200000, i, cap = n * 24 + 16;
    char *s = (char *)malloc(cap), err[128];
    size_t len = 0;
    fj_doc *d;
    uint64_t t0;
    CHECK(s != NULL);
    if (!s) return;
    s[len++] = '{';
    for (i = 0; i < n; i++) len += (size_t)snprintf(s + len, cap - len, "%s\"k%zu\":%zu", i ? "," : "", i, i);
    s[len++] = '}';
    t0 = GetTickCount64();
    d = fj_parse(s, len, err, sizeof(err));
    CHECK(d != NULL && fj_root(d)->n == n);
    CHECK(GetTickCount64() - t0 < 5000);
    fj_free(d);
    memcpy(s + len - 1, ",\"k199999\":1}", 13); /* duplicate of the last key, far past the linear window */
    len += 12;
    d = fj_parse(s, len, err, sizeof(err));
    CHECK(d == NULL && strstr(err, "duplicate") != NULL);
    fj_free(d);
    CHECK(fj_parse("{\"a\":1,\"b\":2,\"a\":3}", 19, err, sizeof(err)) == NULL);
    free(s);
}

static void t_buf_cap(void)
{
    alelt_buf b;
    alelt_buf_init(&b, 8);
    CHECK(alelt_buf_append(&b, "12345678", 8) == 0);
    CHECK(alelt_buf_append(&b, "9", 1) != 0);
    CHECK(b.failed);
    alelt_buf_free(&b);
}

void test_json_suite(void)
{
    printf("json\n");
    RUN(t_valid);
    RUN(t_invalid);
    RUN(t_depth);
    RUN(t_writer_roundtrip);
    RUN(t_buf_cap);
    RUN(t_many_keys);
}
