/* SPDX-License-Identifier: Apache-2.0
 * Test helper: aleltzlibcli <in> <out> - compresses with alelt_zlib_compress and
 * prints "<in_len> <out_len> <sha256-of-input>" (used by verify_zlib.py). */
#include <stdio.h>

#include "alelt_hash.h"
#include "alelt_util.h"

int main(int argc, char **argv)
{
    FILE *f = NULL;
    alelt_buf in, out;
    uint8_t tmp[65536];
    size_t n;
    char hx[65];
    if (argc != 3) return 2;
    alelt_buf_init(&in, 0);
    alelt_buf_init(&out, 0);
    if (fopen_s(&f, argv[1], "rb") != 0 || !f) return 2;
    while ((n = fread(tmp, 1, sizeof(tmp), f)) > 0) {
        if (alelt_buf_append(&in, tmp, n) != 0) return 3;
    }
    fclose(f);
    if (alelt_zlib_compress(in.data ? in.data : (const uint8_t *)"", in.len, &out) != 0) return 3;
    if (fopen_s(&f, argv[2], "wb") != 0 || !f) return 4;
    if (fwrite(out.data, 1, out.len, f) != out.len) return 4;
    fclose(f);
    if (alelt_sha256_hex(in.data ? in.data : (const uint8_t *)"", in.len, hx) != 0) return 5;
    printf("%zu %zu %s\n", in.len, out.len, hx);
    return 0;
}
