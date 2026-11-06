/* SPDX-License-Identifier: Apache-2.0 */
#include "alelt_buf.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void alelt_buf_init(alelt_buf *b, size_t max)
{
    b->data = NULL;
    b->len = 0;
    b->cap = 0;
    b->max = max ? max : ALELT_BUF_DEFAULT_MAX;
    b->failed = 0;
}

void alelt_buf_free(alelt_buf *b)
{
    free(b->data);
    b->data = NULL;
    b->len = 0;
    b->cap = 0;
    b->failed = 0;
}

void alelt_buf_reset(alelt_buf *b)
{
    b->len = 0;
    b->failed = 0;
    if (b->data) {
        b->data[0] = 0;
    }
}

int alelt_buf_reserve(alelt_buf *b, size_t extra)
{
    size_t need, ncap;
    uint8_t *p;
    if (b->failed) {
        return -1;
    }
    if (extra > b->max || b->len > b->max - extra) {
        b->failed = 1;
        return -1;
    }
    need = b->len + extra + 1; /* +1 for terminator */
    if (need <= b->cap) {
        return 0;
    }
    ncap = b->cap ? b->cap : 256;
    while (ncap < need) {
        if (ncap > ((size_t)-1) / 2) {
            ncap = need;
            break;
        }
        ncap *= 2;
    }
    if (ncap > b->max + 1) {
        ncap = b->max + 1;
    }
    if (ncap < need) {
        b->failed = 1;
        return -1;
    }
    p = (uint8_t *)realloc(b->data, ncap);
    if (!p) {
        b->failed = 1;
        return -1;
    }
    b->data = p;
    b->cap = ncap;
    return 0;
}

int alelt_buf_append(alelt_buf *b, const void *p, size_t n)
{
    if (alelt_buf_reserve(b, n) != 0) {
        return -1;
    }
    if (n) {
        memcpy(b->data + b->len, p, n);
    }
    b->len += n;
    b->data[b->len] = 0;
    return 0;
}

int alelt_buf_puts(alelt_buf *b, const char *s)
{
    return alelt_buf_append(b, s, strlen(s));
}

int alelt_buf_putc(alelt_buf *b, char c)
{
    return alelt_buf_append(b, &c, 1);
}

int alelt_buf_printf(alelt_buf *b, const char *fmt, ...)
{
    va_list ap;
    int n;
    va_start(ap, fmt);
    n = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (n < 0) {
        b->failed = 1;
        return -1;
    }
    if (alelt_buf_reserve(b, (size_t)n) != 0) {
        return -1;
    }
    va_start(ap, fmt);
    n = vsnprintf((char *)b->data + b->len, b->cap - b->len, fmt, ap);
    va_end(ap);
    if (n < 0) {
        b->failed = 1;
        return -1;
    }
    b->len += (size_t)n;
    return 0;
}

const char *alelt_buf_cstr(const alelt_buf *b)
{
    return b->data ? (const char *)b->data : "";
}
