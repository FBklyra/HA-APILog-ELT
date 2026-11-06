/* SPDX-License-Identifier: Apache-2.0
 *
 * Small, self-contained zlib-format encoder (original implementation from
 * RFC 1950/1951). Single DEFLATE block with the fixed Huffman tables and a
 * greedy LZ77 match finder; stored blocks when that is not smaller. This is
 * deliberately simple and auditable: no dynamic Huffman trees, no lazy
 * matching. Typical ratio on Db2 log records is well below raw size, and the
 * hub decodes it with standard zlib.
 */
#include <stdlib.h>
#include <string.h>

#include "alelt_hash.h"

#define WSIZE 32768u
#define WMASK (WSIZE - 1u)
#define HBITS 15u
#define HSIZE (1u << HBITS)
#define MIN_MATCH 3u
#define MAX_MATCH 258u
#define MAX_CHAIN 48

uint32_t alelt_adler32(uint32_t adler, const uint8_t *p, size_t n)
{
    uint32_t a = adler & 0xFFFFu, b = (adler >> 16) & 0xFFFFu;
    while (n > 0) {
        size_t k = n < 5552 ? n : 5552; /* largest n keeping b < 2^32 */
        n -= k;
        while (k--) {
            a += *p++;
            b += a;
        }
        a %= 65521u;
        b %= 65521u;
    }
    return (b << 16) | a;
}

typedef struct bitw {
    alelt_buf *o;
    uint32_t acc;
    unsigned nbits;
} bitw;

static int bw_put(bitw *w, uint32_t v, unsigned n)
{
    /* n <= 16 per call */
    w->acc |= v << w->nbits;
    w->nbits += n;
    while (w->nbits >= 8) {
        if (alelt_buf_putc(w->o, (char)(w->acc & 0xFFu))) return -1;
        w->acc >>= 8;
        w->nbits -= 8;
    }
    return 0;
}

static int bw_flush(bitw *w)
{
    if (w->nbits > 0) {
        if (alelt_buf_putc(w->o, (char)(w->acc & 0xFFu))) return -1;
    }
    w->acc = 0;
    w->nbits = 0;
    return 0;
}

static uint32_t rev(uint32_t code, unsigned len)
{
    uint32_t r = 0;
    unsigned i;
    for (i = 0; i < len; i++) {
        r = (r << 1) | (code & 1u);
        code >>= 1;
    }
    return r;
}

static int put_litlen(bitw *w, unsigned sym)
{
    if (sym <= 143) return bw_put(w, rev(0x30u + sym, 8), 8);
    if (sym <= 255) return bw_put(w, rev(0x190u + (sym - 144u), 9), 9);
    if (sym <= 279) return bw_put(w, rev(sym - 256u, 7), 7);
    return bw_put(w, rev(0xC0u + (sym - 280u), 8), 8);
}

static const uint16_t LBASE[29] = {3,  4,  5,  6,  7,  8,  9,  10, 11,  13,  15,  17,  19,  23, 27,
                                   31, 35, 43, 51, 59, 67, 83, 99, 115, 131, 163, 195, 227, 258};
static const uint8_t LEXT[29] = {0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0};
static const uint16_t DBASE[30] = {1,   2,   3,   4,   5,   7,    9,    13,   17,   25,   33,   49,   65,    97,    129,
                                   193, 257, 385, 513, 769, 1025, 1537, 2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577};
static const uint8_t DEXT[30] = {0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13};

static int put_match(bitw *w, unsigned len, unsigned dist)
{
    unsigned li = 28, di = 29;
    while (li > 0 && LBASE[li] > len) li--;
    if (len == 258) li = 28;
    else if (li == 28) li = 27; /* 258 has its own code; 227..257 use code 284 */
    if (put_litlen(w, 257u + li)) return -1;
    if (LEXT[li] && bw_put(w, len - LBASE[li], LEXT[li])) return -1;
    while (di > 0 && DBASE[di] > dist) di--;
    if (bw_put(w, rev(di, 5), 5)) return -1;
    if (DEXT[di] && bw_put(w, dist - DBASE[di], DEXT[di])) return -1;
    return 0;
}

static uint32_t hash3(const uint8_t *p)
{
    uint32_t v = ((uint32_t)p[0] << 16) | ((uint32_t)p[1] << 8) | p[2];
    return (v * 2654435761u) >> (32u - HBITS);
}

static int deflate_fixed(const uint8_t *in, size_t n, alelt_buf *out)
{
    bitw w;
    int32_t *head, *prev;
    size_t i = 0;
    int rc = -1;
    head = (int32_t *)malloc(HSIZE * sizeof(int32_t));
    prev = (int32_t *)malloc(WSIZE * sizeof(int32_t));
    if (!head || !prev) goto done;
    for (i = 0; i < HSIZE; i++) head[i] = -1;
    w.o = out;
    w.acc = 0;
    w.nbits = 0;
    if (bw_put(&w, 1u, 1) || bw_put(&w, 1u, 2)) goto done; /* BFINAL=1, BTYPE=01 */
    i = 0;
    while (i < n) {
        unsigned best_len = 0, best_dist = 0;
        if (n - i >= MIN_MATCH) {
            uint32_t h = hash3(in + i);
            int32_t cand = head[h];
            int chain = MAX_CHAIN;
            size_t maxlen = n - i < MAX_MATCH ? n - i : MAX_MATCH;
            while (cand >= 0 && chain-- > 0) {
                size_t dist = i - (size_t)cand;
                size_t l = 0;
                if (dist == 0 || dist > WSIZE) break;
                while (l < maxlen && in[(size_t)cand + l] == in[i + l]) l++;
                if (l >= MIN_MATCH && l > best_len) {
                    best_len = (unsigned)l;
                    best_dist = (unsigned)dist;
                    if (l == maxlen) break;
                }
                {
                    int32_t nx = prev[(size_t)cand & WMASK];
                    if (nx >= cand) break; /* stale slot from an older window */
                    cand = nx;
                }
            }
            prev[i & WMASK] = head[h];
            head[h] = (int32_t)i;
        }
        if (best_len >= MIN_MATCH) {
            size_t k;
            if (put_match(&w, best_len, best_dist)) goto done;
            for (k = 1; k < best_len; k++) {
                size_t j = i + k;
                if (n - j >= MIN_MATCH) {
                    uint32_t h2 = hash3(in + j);
                    prev[j & WMASK] = head[h2];
                    head[h2] = (int32_t)j;
                }
            }
            i += best_len;
        } else {
            if (put_litlen(&w, in[i])) goto done;
            i++;
        }
    }
    if (put_litlen(&w, 256u) || bw_flush(&w)) goto done; /* end of block */
    rc = 0;
done:
    free(head);
    free(prev);
    return rc;
}

static int deflate_stored(const uint8_t *in, size_t n, alelt_buf *out)
{
    size_t off = 0;
    do {
        size_t k = n - off > 65535u ? 65535u : n - off;
        uint8_t h[5];
        h[0] = (uint8_t)((off + k == n) ? 1u : 0u);
        h[1] = (uint8_t)(k & 0xFFu);
        h[2] = (uint8_t)(k >> 8);
        h[3] = (uint8_t)(~k & 0xFFu);
        h[4] = (uint8_t)((~k >> 8) & 0xFFu);
        if (alelt_buf_append(out, h, 5) || alelt_buf_append(out, in + off, k)) return -1;
        off += k;
    } while (off < n);
    return 0;
}

int alelt_zlib_compress(const uint8_t *in, size_t n, alelt_buf *out)
{
    static const uint8_t hdr[2] = {0x78, 0x01}; /* deflate, 32K window, fastest; (0x7801 % 31)==0 */
    size_t start = out->len, body;
    uint32_t ad;
    uint8_t tr[4];
    if (n > 0x7FFFFFFFu) return -1;
    if (alelt_buf_append(out, hdr, 2)) return -1;
    body = out->len;
    if (deflate_fixed(in, n, out) != 0 || out->len - body >= n + 5u * (n / 65535u + 1u)) {
        if (out->failed && out->len - start > out->max / 2) return -1;
        out->failed = 0;
        out->len = body;
        if (deflate_stored(in, n, out) != 0) return -1;
    }
    ad = alelt_adler32(1u, in, n);
    tr[0] = (uint8_t)(ad >> 24);
    tr[1] = (uint8_t)(ad >> 16);
    tr[2] = (uint8_t)(ad >> 8);
    tr[3] = (uint8_t)ad;
    return alelt_buf_append(out, tr, 4);
}
