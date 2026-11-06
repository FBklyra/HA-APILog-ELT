/* SPDX-License-Identifier: Apache-2.0 */
#include <stdlib.h>

#include "alelt_hash.h"
#include "alelt_util.h"
#include "alelttest.h"

static void t_vectors(void)
{
    char h[65];
    uint8_t *m;
    alelt_sha256 s;
    uint8_t d[32];
    int i;
    CHECK(alelt_sha256_hex("", 0, h) == 0);
    CHECK_STR(h, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    CHECK(alelt_sha256_hex("abc", 3, h) == 0);
    CHECK_STR(h, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    CHECK(alelt_sha256_hex("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", 56, h) == 0);
    CHECK_STR(h, "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
    m = (uint8_t *)malloc(1000000);
    if (!m) return;
    memset(m, 'a', 1000000);
    /* incremental in uneven pieces */
    CHECK(alelt_sha256_init(&s) == 0);
    for (i = 0; i < 1000000; i += 777) alelt_sha256_update(&s, m + i, (size_t)(1000000 - i < 777 ? 1000000 - i : 777));
    CHECK(alelt_sha256_final(&s, d) == 0);
    alelt_hex(d, 32, h, 0);
    CHECK_STR(h, "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
    free(m);
}

void test_hash_suite(void)
{
    printf("sha256 (BCrypt)\n");
    RUN(t_vectors);
}
