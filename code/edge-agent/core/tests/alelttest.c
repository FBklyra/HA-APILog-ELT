/* SPDX-License-Identifier: Apache-2.0 */
#include "alelttest.h"

int aleltt_checks = 0, aleltt_failures = 0;

void aleltt_fail(const char *file, int line, const char *expr)
{
    aleltt_failures++;
    printf("\n    FAILED %s:%d: %s\n", file, line, expr);
}

int aleltt_report(const char *suite)
{
    printf("%s: %d checks, %d failures\n", suite, aleltt_checks, aleltt_failures);
    return aleltt_failures ? 1 : 0;
}
