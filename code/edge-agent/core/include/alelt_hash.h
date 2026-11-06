/* ALELT - SHA-256 (Windows CNG / BCrypt) and zlib-format encoder.
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef ALELT_HASH_H
#define ALELT_HASH_H

#include <stddef.h>
#include <stdint.h>

#include "alelt_buf.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct alelt_sha256 {
    void *h; /* BCRYPT_HASH_HANDLE */
} alelt_sha256;

int alelt_sha256_init(alelt_sha256 *s);
int alelt_sha256_update(alelt_sha256 *s, const void *p, size_t n);
int alelt_sha256_final(alelt_sha256 *s, uint8_t out[32]);
/* One shot; hex is 64 lowercase chars + NUL. */
int alelt_sha256_hex(const void *p, size_t n, char hex[65]);

/* zlib (RFC 1950) stream with a DEFLATE (RFC 1951) body.
 * Encoder: greedy LZ77 (32 KiB window, hash chains) + fixed Huffman codes;
 * falls back to stored blocks when that is smaller. Output is decodable by
 * any standard inflater (Python zlib.decompress, etc.). Appends to out. */
int alelt_zlib_compress(const uint8_t *in, size_t n, alelt_buf *out);
uint32_t alelt_adler32(uint32_t adler, const uint8_t *p, size_t n);

#ifdef __cplusplus
}
#endif
#endif
