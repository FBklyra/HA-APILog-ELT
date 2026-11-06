/* SPDX-License-Identifier: Apache-2.0
 * libFuzzer target: JSON reader + writer round trip of string values. */
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "alelt_json.h"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    char err[128];
    fj_doc *d = fj_parse((const char *)data, size, err, sizeof(err));
    if (d) {
        const fj_val *r = fj_root(d);
        if (r->type == FJ_STR) {
            alelt_buf b;
            fj_doc *d2;
            alelt_buf_init(&b, 0);
            if (fj_escape(&b, r->s, r->n) == 0) {
                d2 = fj_parse((const char *)b.data, b.len, err, sizeof(err));
                if (!d2 || fj_root(d2)->n != r->n || memcmp(fj_root(d2)->s, r->s, r->n) != 0) abort();
                fj_free(d2);
            }
            alelt_buf_free(&b);
        }
        fj_free(d);
    }
    return 0;
}
