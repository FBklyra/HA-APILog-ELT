/* SPDX-License-Identifier: Apache-2.0
 * libFuzzer target: command whitelist parser (R5). Build with the fuzz-clang preset.
 * Invariant: an accepted command always carries a whitelisted name. */
#include <stdint.h>
#include <stdlib.h>

#include "command.h"

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    static alelt_cmd c;
    char err[256];
    if (alelt_cmd_parse((const char *)data, size, &c, err, sizeof(err)) == 0) {
        if (c.name < ALELT_CMD_FIRST || c.name > ALELT_CMD_LAST) abort();
    }
    return 0;
}
