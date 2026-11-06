/* SPDX-License-Identifier: Apache-2.0
 * Spool accounting: durable put, oldest-first, delete only up to durable_pos,
 * cap => no room (backpressure), restart recovery, torn/corrupt files.
 */
#include <stdio.h>
#include <stdlib.h>

#include "alelttest.h"
#include "spool.h"

static int cmp_txt(const char *a, const char *b, int *r)
{
    *r = strcmp(a, b); /* test positions are fixed-width text */
    return 0;
}

static void tmpdir(wchar_t *out, size_t cap)
{
    wchar_t base[MAX_PATH];
    char id[37];
    wchar_t wid[37];
    GetTempPathW(MAX_PATH, base);
    alelt_uuid4(id);
    alelt_to_wide(id, wid, 37);
    _snwprintf_s(out, cap, _TRUNCATE, L"%lsaleltspool-%ls", base, wid);
}

static int put(alelt_spool *s, const char *to, const char *next, size_t body)
{
    char hdr[256], id[37];
    uint8_t *b = (uint8_t *)calloc(1, body ? body : 1);
    int r;
    alelt_uuid4(id);
    snprintf(hdr, sizeof(hdr), "{\"batch_id\":\"%s\",\"to_pos\":{\"engine\":\"t\",\"text\":\"%s\"},\"next_pos\":{\"engine\":\"t\",\"text\":\"%s\"}}", id, to, next);
    r = alelt_spool_put(s, id, hdr, b, body, to, next);
    free(b);
    return r;
}

static void t_accounting(void)
{
    static alelt_spool s;
    wchar_t dir[MAX_PATH];
    char err[256], durable[ALELT_POS_MAX], next[ALELT_POS_MAX];
    alelt_spool_entry m;
    alelt_buf h, b;
    uint64_t bytes;
    size_t n;
    int cap;
    tmpdir(dir, MAX_PATH);
    CHECK(alelt_spool_open(&s, dir, 4096, err, sizeof(err)) == 0);
    CHECK(alelt_spool_has_room(&s, 1000));
    CHECK(put(&s, "0010", "0011", 1000) == 0);
    CHECK(put(&s, "0020", "0021", 1000) == 0);
    CHECK(put(&s, "0030", "0031", 1000) == 0);
    alelt_spool_snapshot(&s, &bytes, &n, durable, next, &cap);
    CHECK(n == 3 && bytes > 3000 && strcmp(next, "0031") == 0);
    CHECK(!alelt_spool_has_room(&s, 2000)); /* cap reached -> backpressure */
    alelt_buf_init(&h, 0);
    alelt_buf_init(&b, 0);
    CHECK(alelt_spool_oldest(&s, &m, &h, &b) == 1 && strcmp(m.to_pos, "0010") == 0 && b.len == 1000);
    CHECK(alelt_spool_ack(&s, "0005", cmp_txt) == 0);  /* durable behind: nothing deleted */
    CHECK(alelt_spool_ack(&s, "0020", cmp_txt) == 2);  /* deletes <= durable only */
    alelt_spool_snapshot(&s, &bytes, &n, durable, next, &cap);
    CHECK(n == 1 && strcmp(durable, "0020") == 0);
    CHECK(alelt_spool_has_room(&s, 2000));
    CHECK(alelt_spool_ack(&s, "0015", cmp_txt) == 0);  /* durable never moves backwards */
    alelt_spool_snapshot(&s, NULL, NULL, durable, NULL, NULL);
    CHECK(strcmp(durable, "0020") == 0);
    alelt_spool_set_capturing(&s, 1, NULL);
    alelt_spool_close(&s);

    /* restart: remaining batch + positions recovered; torn .tmp removed; corrupt -> .bad */
    {
        wchar_t p[MAX_PATH];
        HANDLE f;
        DWORD wr;
        _snwprintf_s(p, MAX_PATH, _TRUNCATE, L"%ls\\00000000000000ff.tmp", dir);
        f = CreateFileW(p, GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, 0, NULL);
        WriteFile(f, "junk", 4, &wr, NULL);
        CloseHandle(f);
        _snwprintf_s(p, MAX_PATH, _TRUNCATE, L"%ls\\00000000000000fe.aleltb", dir);
        f = CreateFileW(p, GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, 0, NULL);
        WriteFile(f, "ALELTB1garbage-garbage", 19, &wr, NULL);
        CloseHandle(f);
    }
    CHECK(alelt_spool_open(&s, dir, 4096, err, sizeof(err)) == 0);
    alelt_spool_snapshot(&s, &bytes, &n, durable, next, &cap);
    CHECK(n == 1 && strcmp(next, "0031") == 0 && strcmp(durable, "0020") == 0 && cap == 1);
    CHECK(put(&s, "0040", "0041", 10) == 0);          /* sequence continues after recovery */
    alelt_buf_reset(&h);
    alelt_buf_reset(&b);
    CHECK(alelt_spool_oldest(&s, &m, &h, &b) == 1 && strcmp(m.to_pos, "0030") == 0);
    CHECK(alelt_spool_ack(&s, "0041", cmp_txt) == 2);
    alelt_spool_close(&s);
    alelt_buf_free(&h);
    alelt_buf_free(&b);
}

static void t_takeover(void)
{
    static alelt_spool s;
    wchar_t dir[MAX_PATH];
    char err[256], durable[ALELT_POS_MAX], next[ALELT_POS_MAX], rep[ALELT_UUID_LEN + 1];
    alelt_spool_entry m;
    alelt_buf h, b;
    uint64_t bytes;
    size_t n;
    int cap;
    tmpdir(dir, MAX_PATH);
    CHECK(alelt_spool_open(&s, dir, 1 << 20, err, sizeof(err)) == 0);
    CHECK(put(&s, "0010", "0011", 100) == 0);
    CHECK(put(&s, "0020", "0021", 100) == 0);
    alelt_spool_set_reposition(&s, "11111111-2222-4333-8444-555555555555", "0011");
    CHECK(alelt_spool_takeover(&s, "0050", 1) == 2); /* stale batches of the previous holder dropped */
    alelt_spool_snapshot(&s, &bytes, &n, durable, next, &cap);
    CHECK(n == 0 && bytes == 0 && strcmp(next, "0050") == 0 && strcmp(durable, "0050") == 0 && cap == 1);
    alelt_spool_reposition_for(&s, "0011", rep);
    CHECK(rep[0] == 0);
    alelt_buf_init(&h, 0);
    alelt_buf_init(&b, 0);
    CHECK(alelt_spool_oldest(&s, &m, &h, &b) == 0);
    alelt_spool_close(&s);
    CHECK(alelt_spool_open(&s, dir, 1 << 20, err, sizeof(err)) == 0); /* persisted across a restart */
    alelt_spool_snapshot(&s, NULL, &n, durable, next, &cap);
    CHECK(n == 0 && strcmp(next, "0050") == 0 && cap == 1);
    CHECK(put(&s, "0060", "0061", 10) == 0);
    CHECK(alelt_spool_takeover(&s, NULL, 1) == 1); /* no hub position: wait for START_CAPTURE */
    alelt_spool_snapshot(&s, NULL, &n, durable, next, &cap);
    CHECK(n == 0 && next[0] == 0 && durable[0] == 0 && cap == 0);
    alelt_spool_close(&s);
    alelt_buf_free(&h);
    alelt_buf_free(&b);
}

/* AHS-10: metadata from the header only, body read straight into the caller buffer;
 * truncated file (size != framing) = corrupt. AHS-05: drop_head removes only the named head. */
static void t_header_read_and_drop(void)
{
    static alelt_spool s;
    wchar_t dir[MAX_PATH], p[MAX_PATH];
    char err[256];
    alelt_spool_entry m;
    alelt_buf h, b;
    size_t n;
    uint64_t bytes;
    HANDLE f;
    LARGE_INTEGER sz;
    tmpdir(dir, MAX_PATH);
    CHECK(alelt_spool_open(&s, dir, (uint64_t)64 << 20, err, sizeof(err)) == 0);
    CHECK(put(&s, "0010", "0011", (size_t)3 << 20) == 0);
    CHECK(put(&s, "0020", "0021", 10) == 0);
    alelt_buf_init(&h, 0);
    alelt_buf_init(&b, 0);
    CHECK(alelt_spool_oldest(&s, &m, &h, &b) == 1 && b.len == ((size_t)3 << 20) && strcmp(m.next_pos, "0011") == 0);
    CHECK(alelt_spool_drop_head(&s, "not-the-head") == 0);
    CHECK(alelt_spool_drop_head(&s, m.batch_id) == 1);
    alelt_spool_snapshot(&s, &bytes, &n, NULL, NULL, NULL);
    CHECK(n == 1 && bytes < 1000);
    alelt_spool_close(&s);
    /* truncate the remaining batch by one byte: quarantined as .bad on the next open */
    _snwprintf_s(p, MAX_PATH, _TRUNCATE, L"%ls\\0000000000000002.aleltb", dir);
    f = CreateFileW(p, GENERIC_WRITE, 0, NULL, OPEN_EXISTING, 0, NULL);
    CHECK(f != INVALID_HANDLE_VALUE);
    GetFileSizeEx(f, &sz);
    sz.QuadPart -= 1;
    SetFilePointerEx(f, sz, NULL, FILE_BEGIN);
    SetEndOfFile(f);
    CloseHandle(f);
    CHECK(alelt_spool_open(&s, dir, (uint64_t)64 << 20, err, sizeof(err)) == 0);
    alelt_spool_snapshot(&s, NULL, &n, NULL, NULL, NULL);
    CHECK(n == 0);
    alelt_spool_close(&s);
    alelt_buf_free(&h);
    alelt_buf_free(&b);
}

void test_spool_suite(void)
{
    printf("spool\n");
    RUN(t_header_read_and_drop);
    RUN(t_accounting);
    RUN(t_takeover);
}
