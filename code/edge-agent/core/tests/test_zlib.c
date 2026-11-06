/* SPDX-License-Identifier: Apache-2.0
 * zlib encoder tests. A small test-only inflater (stored + fixed-Huffman
 * blocks, the only block types the encoder emits) checks round trips; the
 * Python cross-check lives in tests/verify_zlib.py.
 */
#include <stdlib.h>

#include "alelt_hash.h"
#include "alelttest.h"

typedef struct br {
    const uint8_t *p;
    size_t n, pos;
    unsigned bit;
} br;

static int getbit(br *b)
{
    int v;
    if (b->pos >= b->n) return -1;
    v = (b->p[b->pos] >> b->bit) & 1;
    if (++b->bit == 8) { b->bit = 0; b->pos++; }
    return v;
}

static int bits(br *b, unsigned n, unsigned *out)
{
    unsigned v = 0, i;
    for (i = 0; i < n; i++) {
        int x = getbit(b);
        if (x < 0) return -1;
        v |= (unsigned)x << i;
    }
    *out = v;
    return 0;
}

/* read a Huffman code MSB-first */
static int hbits(br *b, unsigned n, unsigned *out)
{
    unsigned v = 0, i;
    for (i = 0; i < n; i++) {
        int x = getbit(b);
        if (x < 0) return -1;
        v = (v << 1) | (unsigned)x;
    }
    *out = v;
    return 0;
}

static int fixed_sym(br *b)
{
    unsigned c, e;
    if (hbits(b, 7, &c)) return -1;
    if (c <= 0x17) return (int)(256 + c);
    if (hbits(b, 1, &e)) return -1;
    c = (c << 1) | e;
    if (c >= 0x30 && c <= 0xBF) return (int)(c - 0x30);
    if (c >= 0xC0 && c <= 0xC7) return (int)(280 + c - 0xC0);
    if (hbits(b, 1, &e)) return -1;
    c = (c << 1) | e;
    if (c >= 0x190 && c <= 0x1FF) return (int)(144 + c - 0x190);
    return -1;
}

static const uint16_t LB[29] = {3,4,5,6,7,8,9,10,11,13,15,17,19,23,27,31,35,43,51,59,67,83,99,115,131,163,195,227,258};
static const uint8_t LE[29] = {0,0,0,0,0,0,0,0,1,1,1,1,2,2,2,2,3,3,3,3,4,4,4,4,5,5,5,5,0};
static const uint16_t DB[30] = {1,2,3,4,5,7,9,13,17,25,33,49,65,97,129,193,257,385,513,769,1025,1537,2049,3073,4097,6145,8193,12289,16385,24577};
static const uint8_t DE[30] = {0,0,0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,11,11,12,12,13,13};

static int mini_inflate(const uint8_t *z, size_t zn, alelt_buf *out)
{
    br b;
    unsigned final = 0, type, ad;
    if (zn < 6 || z[0] != 0x78 || ((z[0] << 8) | z[1]) % 31 != 0) return -1;
    b.p = z + 2;
    b.n = zn - 6;
    b.pos = 0;
    b.bit = 0;
    while (!final) {
        if (bits(&b, 1, &final) || bits(&b, 2, &type)) return -1;
        if (type == 0) {
            unsigned len, nlen;
            if (b.bit) { b.bit = 0; b.pos++; }
            if (b.n - b.pos < 4) return -1;
            len = b.p[b.pos] | (b.p[b.pos + 1] << 8);
            nlen = b.p[b.pos + 2] | (b.p[b.pos + 3] << 8);
            if ((len ^ 0xFFFFu) != nlen) return -1;
            b.pos += 4;
            if (b.n - b.pos < len || alelt_buf_append(out, b.p + b.pos, len)) return -1;
            b.pos += len;
        } else if (type == 1) {
            for (;;) {
                int s = fixed_sym(&b);
                if (s < 0) return -1;
                if (s < 256) { if (alelt_buf_putc(out, (char)s)) return -1; continue; }
                if (s == 256) break;
                {
                    unsigned li = (unsigned)s - 257, ex, dc, len, dist, k;
                    if (li > 28 || bits(&b, LE[li], &ex)) return -1;
                    len = LB[li] + ex;
                    if (hbits(&b, 5, &dc) || dc > 29 || bits(&b, DE[dc], &ex)) return -1;
                    dist = DB[dc] + ex;
                    if (dist > out->len) return -1;
                    for (k = 0; k < len; k++) {
                        char ch = (char)out->data[out->len - dist];
                        if (alelt_buf_putc(out, ch)) return -1;
                    }
                }
            }
        } else {
            return -1;
        }
    }
    ad = ((unsigned)z[zn - 4] << 24) | ((unsigned)z[zn - 3] << 16) | ((unsigned)z[zn - 2] << 8) | z[zn - 1];
    return ad == alelt_adler32(1, out->data ? out->data : (const uint8_t *)"", out->len) ? 0 : -1;
}

static int roundtrip(const uint8_t *in, size_t n)
{
    alelt_buf z, o;
    int ok;
    alelt_buf_init(&z, 0);
    alelt_buf_init(&o, 0);
    ok = alelt_zlib_compress(in, n, &z) == 0 && mini_inflate(z.data, z.len, &o) == 0 && o.len == n &&
         (n == 0 || memcmp(o.data, in, n) == 0);
    alelt_buf_free(&z);
    alelt_buf_free(&o);
    return ok;
}

static void t_adler(void)
{
    CHECK(alelt_adler32(1, (const uint8_t *)"Wikipedia", 9) == 0x11E60398u);
}

static void t_roundtrips(void)
{
    uint8_t *m = (uint8_t *)malloc(300000);
    unsigned x = 7;
    size_t i;
    if (!m) return;
    CHECK(roundtrip((const uint8_t *)"", 0));
    CHECK(roundtrip((const uint8_t *)"a", 1));
    for (i = 0; i < 300000; i++) m[i] = (uint8_t)("ALELT"[i % 10]);
    CHECK(roundtrip(m, 300000));        /* long matches, distance 10 */
    for (i = 0; i < 300000; i++) { x = x * 1103515245u + 12345u; m[i] = (uint8_t)(x >> 16); }
    CHECK(roundtrip(m, 300000));        /* incompressible -> stored blocks > 64K */
    for (i = 0; i < 300000; i++) { x = x * 1103515245u + 12345u; m[i] = (uint8_t)((x >> 16) % 4); }
    CHECK(roundtrip(m, 300000));        /* low entropy, short matches, far windows */
    free(m);
}

static void t_ratio(void)
{
    alelt_buf z;
    uint8_t m[65536];
    size_t i;
    for (i = 0; i < sizeof(m); i++) m[i] = (uint8_t)(i % 256 < 200 ? 0 : i);
    alelt_buf_init(&z, 0);
    CHECK(alelt_zlib_compress(m, sizeof(m), &z) == 0);
    CHECK(z.len < sizeof(m) / 4);
    alelt_buf_free(&z);
}

void test_zlib_suite(void)
{
    printf("zlib encoder\n");
    RUN(t_adler);
    RUN(t_roundtrips);
    RUN(t_ratio);
}
