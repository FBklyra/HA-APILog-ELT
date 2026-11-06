/* ALELT edge agent - growable byte buffer (bounded).
 * SPDX-License-Identifier: Apache-2.0
 * Copyright (c) Freddy Beltran
 */
#ifndef ALELT_BUF_H
#define ALELT_BUF_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Default hard ceiling for any single buffer (256 MiB). */
#define ALELT_BUF_DEFAULT_MAX ((size_t)256u * 1024u * 1024u)

typedef struct alelt_buf {
    uint8_t *data;  /* always NUL-terminated after any successful append */
    size_t len;     /* bytes used (excluding the terminator)              */
    size_t cap;     /* bytes allocated                                     */
    size_t max;     /* hard ceiling for len                                */
    int failed;     /* sticky: set when an append failed (OOM or ceiling)  */
} alelt_buf;

void alelt_buf_init(alelt_buf *b, size_t max);
void alelt_buf_free(alelt_buf *b);
void alelt_buf_reset(alelt_buf *b);
int alelt_buf_reserve(alelt_buf *b, size_t extra);
int alelt_buf_append(alelt_buf *b, const void *p, size_t n);
int alelt_buf_puts(alelt_buf *b, const char *s);
int alelt_buf_putc(alelt_buf *b, char c);
int alelt_buf_printf(alelt_buf *b, const char *fmt, ...);
/* Returns a NUL-terminated view ("" when empty). */
const char *alelt_buf_cstr(const alelt_buf *b);

#ifdef __cplusplus
}
#endif
#endif
