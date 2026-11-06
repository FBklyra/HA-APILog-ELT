/* SPDX-License-Identifier: Apache-2.0 */
#include "alelttest.h"

void test_json_suite(void);
void test_cmd_suite(void);
void test_hash_suite(void);
void test_zlib_suite(void);
void test_spool_suite(void);
void test_governor_suite(void);
void test_config_suite(void);
void test_ha_suite(void);
void test_guard_suite(void);
void test_cred_suite(void);

int main(void)
{
    test_json_suite();
    test_cmd_suite();
    test_hash_suite();
    test_zlib_suite();
    test_spool_suite();
    test_governor_suite();
    test_config_suite();
    test_ha_suite();
    test_guard_suite();
    test_cred_suite();
    return aleltt_report("aleltagent unit tests");
}
