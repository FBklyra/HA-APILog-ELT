/* SPDX-License-Identifier: Apache-2.0
 * Db2 type-1 LRI text form: "<part1 decimal>:<part2 as 16 upper-case hex>".
 * Ordering: part1, then part2 (unsigned).
 */
#ifndef ALELT_DB2_LRI_H
#define ALELT_DB2_LRI_H

#include <stddef.h>
#include <stdint.h>

typedef struct alelt_lri {
    uint64_t part1;
    uint64_t part2;
} alelt_lri;

int alelt_lri_parse(const char *s, alelt_lri *out);
int alelt_lri_format(const alelt_lri *l, char *out, size_t cap);
int alelt_lri_cmp(const alelt_lri *a, const alelt_lri *b);

#endif

/* ---- filter-ON db2ReadLog buffer framing (DESIGN V3) ----
 * [db2ReadLogFilterData 56 B: LRI1(24) LRI2(24) realLen u32 sqlcode i32][log record: u32 size ...]
 * Walks exactly `expected` records over `len` bytes; fails on any overrun or
 * leftover. Returns the first/last type-1 LRIs and the count of records whose
 * filter sqlcode is non-zero (decompression errors; shipped raw). */
#define ALELT_DB2_FILTER_HDR 56u
#define ALELT_DB2_LOGREC_MIN 20u
int alelt_db2_frame_walk(const uint8_t *buf, size_t len, uint32_t expected, alelt_lri *first, alelt_lri *last,
                      uint32_t *bad_sqlcode, char *err, size_t errcap);
