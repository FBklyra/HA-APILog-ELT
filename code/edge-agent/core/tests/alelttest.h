/* ALELT - tiny unit test framework (no dependencies).
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef ALELTTEST_H
#define ALELTTEST_H

#include <stdio.h>
#include <string.h>

extern int aleltt_checks, aleltt_failures;
void aleltt_fail(const char *file, int line, const char *expr);
int aleltt_report(const char *suite);

#define CHECK(c)                                                  \
    do {                                                          \
        aleltt_checks++;                                             \
        if (!(c)) aleltt_fail(__FILE__, __LINE__, #c);               \
    } while (0)
#define CHECK_STR(a, b) CHECK(strcmp((a), (b)) == 0)
#define RUN(fn)                                                   \
    do {                                                          \
        printf("  %-32s", #fn);                                   \
        {                                                         \
            int before_ = aleltt_failures;                           \
            fn();                                                 \
            printf("%s\n", aleltt_failures == before_ ? "ok" : "FAIL"); \
        }                                                         \
    } while (0)

#endif
