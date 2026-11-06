/* SPDX-License-Identifier: Apache-2.0 */
#include <stdio.h>
#include <string.h>

#include "db2_lri.h"

int alelt_lri_parse(const char *s, alelt_lri *out)
{
    uint64_t p1 = 0, p2 = 0;
    int digits = 0, i;
    if (!s) return -1;
    while (*s >= '0' && *s <= '9') {
        uint64_t d = (uint64_t)(*s - '0');
        if (p1 > (UINT64_MAX - d) / 10u) return -1;
        p1 = p1 * 10u + d;
        s++;
        if (++digits > 20) return -1;
    }
    if (digits == 0 || *s != ':') return -1;
    s++;
    for (i = 0; i < 16; i++) {
        char c = s[i];
        unsigned v;
        if (c >= '0' && c <= '9') v = (unsigned)(c - '0');
        else if (c >= 'A' && c <= 'F') v = (unsigned)(c - 'A' + 10);
        else if (c >= 'a' && c <= 'f') v = (unsigned)(c - 'a' + 10);
        else return -1;
        p2 = (p2 << 4) | v;
    }
    if (s[16] != 0) return -1;
    out->part1 = p1;
    out->part2 = p2;
    return 0;
}

int alelt_lri_format(const alelt_lri *l, char *out, size_t cap)
{
    int n = snprintf(out, cap, "%llu:%016llX", (unsigned long long)l->part1, (unsigned long long)l->part2);
    return (n < 0 || (size_t)n >= cap) ? -1 : 0;
}

int alelt_lri_cmp(const alelt_lri *a, const alelt_lri *b)
{
    if (a->part1 != b->part1) return a->part1 < b->part1 ? -1 : 1;
    if (a->part2 != b->part2) return a->part2 < b->part2 ? -1 : 1;
    return 0;
}

static uint32_t rd32(const uint8_t *p)
{
    uint32_t v;
    memcpy(&v, p, 4);
    return v;
}

static uint64_t rd64(const uint8_t *p)
{
    uint64_t v;
    memcpy(&v, p, 8);
    return v;
}

int alelt_db2_frame_walk(const uint8_t *buf, size_t len, uint32_t expected, alelt_lri *first, alelt_lri *last,
                      uint32_t *bad_sqlcode, char *err, size_t errcap)
{
    size_t off = 0;
    uint32_t i;
    *bad_sqlcode = 0;
    for (i = 0; i < expected; i++) {
        uint32_t size;
        alelt_lri l;
        if (len - off < ALELT_DB2_FILTER_HDR + 4) {
            snprintf(err, errcap, "truncated filter header at record %lu", (unsigned long)i);
            return -1;
        }
        if (rd64(buf + off) != 1u) {
            snprintf(err, errcap, "record %lu: first LRI is not type 1", (unsigned long)i);
            return -1;
        }
        l.part1 = rd64(buf + off + 8);
        l.part2 = rd64(buf + off + 16);
        if ((int32_t)rd32(buf + off + 52) != 0) (*bad_sqlcode)++;
        size = rd32(buf + off + ALELT_DB2_FILTER_HDR);
        if (size < ALELT_DB2_LOGREC_MIN || len - off - ALELT_DB2_FILTER_HDR < size) {
            snprintf(err, errcap, "bad record length %lu at record %lu", (unsigned long)size, (unsigned long)i);
            return -1;
        }
        if (i == 0) *first = l;
        *last = l;
        off += ALELT_DB2_FILTER_HDR + size;
    }
    if (off != len) {
        snprintf(err, errcap, "framing mismatch: walked %zu of %zu bytes", off, len);
        return -1;
    }
    return 0;
}
