/* SPDX-License-Identifier: Apache-2.0 */
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <bcrypt.h>

#include "alelt_hash.h"
#include "alelt_util.h"

/* Process-wide provider, opened once (thread-safe via InitOnce). */
static INIT_ONCE g_once = INIT_ONCE_STATIC_INIT;
static BCRYPT_ALG_HANDLE g_alg = NULL;

static BOOL CALLBACK open_alg(PINIT_ONCE once, PVOID param, PVOID *ctx)
{
    (void)once;
    (void)param;
    (void)ctx;
    return BCRYPT_SUCCESS(BCryptOpenAlgorithmProvider(&g_alg, BCRYPT_SHA256_ALGORITHM, NULL, 0)) ? TRUE : FALSE;
}

int alelt_sha256_init(alelt_sha256 *s)
{
    BCRYPT_HASH_HANDLE h = NULL;
    s->h = NULL;
    if (!InitOnceExecuteOnce(&g_once, open_alg, NULL, NULL) || !g_alg) return -1;
    /* pbHashObject NULL: CNG manages the object memory (Windows 7+). */
    if (!BCRYPT_SUCCESS(BCryptCreateHash(g_alg, &h, NULL, 0, NULL, 0, 0))) return -1;
    s->h = h;
    return 0;
}

int alelt_sha256_update(alelt_sha256 *s, const void *p, size_t n)
{
    const uint8_t *q = (const uint8_t *)p;
    if (!s->h) return -1;
    while (n > 0) {
        ULONG chunk = n > 0x40000000u ? 0x40000000u : (ULONG)n;
        if (!BCRYPT_SUCCESS(BCryptHashData((BCRYPT_HASH_HANDLE)s->h, (PUCHAR)q, chunk, 0))) return -1;
        q += chunk;
        n -= chunk;
    }
    return 0;
}

int alelt_sha256_final(alelt_sha256 *s, uint8_t out[32])
{
    NTSTATUS st;
    if (!s->h) return -1;
    st = BCryptFinishHash((BCRYPT_HASH_HANDLE)s->h, out, 32, 0);
    BCryptDestroyHash((BCRYPT_HASH_HANDLE)s->h);
    s->h = NULL;
    return BCRYPT_SUCCESS(st) ? 0 : -1;
}

int alelt_sha256_hex(const void *p, size_t n, char hex[65])
{
    alelt_sha256 s;
    uint8_t d[32];
    if (alelt_sha256_init(&s) != 0) return -1;
    if (alelt_sha256_update(&s, p, n) != 0) {
        (void)alelt_sha256_final(&s, d);
        return -1;
    }
    if (alelt_sha256_final(&s, d) != 0) return -1;
    alelt_hex(d, 32, hex, 0);
    return 0;
}
