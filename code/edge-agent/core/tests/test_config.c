/* SPDX-License-Identifier: Apache-2.0 */
#include <stdio.h>

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

#include "config.h"
#include "alelttest.h"

static int load(const char *json, alelt_config *c)
{
    wchar_t base[MAX_PATH], p[MAX_PATH];
    char err[512];
    int r;
    GetTempPathW(MAX_PATH, base);
    _snwprintf_s(p, MAX_PATH, _TRUNCATE, L"%lsaleltagent-test-%lu.json", base, (unsigned long)GetCurrentProcessId());
    if (alelt_write_file_atomic(p, json, strlen(json)) != 0) return -9;
    r = alelt_config_load(p, c, err, sizeof(err));
    DeleteFileW(p);
    return r;
}

#define BASE "\"agent_id\":\"a\",\"connection_id\":\"c\",\"source\":{\"database\":\"ERPTEST\",\"settings\":{\"database\":\"ERPTEST\"}}"

static void t_load(void)
{
    static alelt_config c;
    CHECK(load("{" BASE ",\"hub\":{\"url\":\"http://127.0.0.1:1\",\"mode\":\"dev\"},"
               "\"limits\":{\"cpu_pct\":300,\"batch_kb\":9999},\"limits_max\":{\"cpu_pct\":50,\"batch_kb\":2048},"
               "\"snapshot\":{\"allowed_tables\":[\"APPSCHEMA.CUSTOMER\"]},\"_note\":\"comment keys ok\"}", &c) == 0);
    CHECK(c.limits.cpu_pct == 50 && c.limits.batch_kb == 2048); /* clamped to DBA maxima */
    CHECK(alelt_config_table_allowed(&c, "APPSCHEMA", "CUSTOMER"));
    CHECK(alelt_config_table_allowed(&c, "APPSCHEMA", "client"));   /* case-insensitive after upper-casing (AGENT-COMMANDS) */
    CHECK(!alelt_config_table_allowed(&c, "APPSCHEMA", "DRIVER"));
    CHECK(c.ha_active_passive == 0 && c.ha_lease_ttl_s == 30 && c.ha_renew_s == 10); /* absent = standalone */
    alelt_config_free(&c);
    CHECK(load("{" BASE ",\"hub\":{\"url\":\"http://x\",\"mode\":\"dev\"},"
               "\"ha\":{\"mode\":\"active_passive\",\"lease_ttl_s\":20,\"renew_s\":5}}", &c) == 0);
    CHECK(c.ha_active_passive == 1 && c.ha_lease_ttl_s == 20 && c.ha_renew_s == 5);
    alelt_config_free(&c);
    CHECK(load("{" BASE ",\"hub\":{\"url\":\"http://x\",\"mode\":\"dev\"},\"ha\":{\"lease_ttl_s\":12}}", &c) == 0);
    CHECK(c.ha_active_passive == 0 && c.ha_renew_s == 4); /* default renew shrinks to ttl/3 */
    alelt_config_free(&c);
}

static void t_reject(void)
{
    static alelt_config c;
    CHECK(load("{" BASE ",\"hub\":{\"url\":\"http://x\",\"mode\":\"dev\"},\"limit\":{}}", &c) != 0);       /* typo key */
    CHECK(load("{" BASE ",\"hub\":{\"url\":\"http://x\"}}", &c) != 0);                          /* mtls needs https */
    CHECK(load("{" BASE ",\"hub\":{\"url\":\"https://x\",\"client_cert_thumbprint\":\"AB\"}}", &c) != 0); /* no pin */
    CHECK(load("{" BASE ",\"hub\":{\"url\":\"https://x\",\"mode\":\"dev\"},\"source\":{\"database\":\"D\",\"plugin\":\"..\\\\x.dll\"}}", &c) != 0);
    CHECK(load("{\"agent_id\":\"a\",\"connection_id\":\"c\",\"hub\":{\"url\":\"https://x\",\"mode\":\"dev\"},"
               "\"source\":{\"database\":\"D\",\"plugin\":\"C:\\\\evil.dll\"}}", &c) != 0);
    CHECK(load("{" BASE ",\"hub\":{\"url\":\"http://x\",\"mode\":\"dev\"},\"snapshot\":{\"allowed_tables\":[\"NOPE\"]}}", &c) != 0);
    CHECK(load("{" BASE ",\"hub\":{\"url\":\"http://x\",\"mode\":\"dev\"},\"limits\":{\"cpu_pct\":0}}", &c) != 0);
    CHECK(load("{" BASE ",\"hub\":{\"url\":\"http://x\",\"mode\":\"dev\"},\"ha\":{\"mode\":\"active-active\"}}", &c) != 0);
    CHECK(load("{" BASE ",\"hub\":{\"url\":\"http://x\",\"mode\":\"dev\"},\"ha\":{\"lease_ttl_s\":30,\"renew_s\":20}}", &c) != 0);
    CHECK(load("{" BASE ",\"hub\":{\"url\":\"http://x\",\"mode\":\"dev\"},\"ha\":{\"lease_ttl_s\":3}}", &c) != 0);
    CHECK(load("{" BASE ",\"hub\":{\"url\":\"http://x\",\"mode\":\"dev\"},\"ha\":{\"ttl\":30}}", &c) != 0);
}

/* Shared allow-list vectors (edge-agent/shared/allowlist_vectors.json; same cases in the dev edge and aleltd). */
static void t_allowlist_vectors(void)
{
    static const struct { const char *entry, *schema, *table; int want; } v[] = {
        {"S.T", "S", "T", 1}, {"S.*", "S", "ANY", 1}, {"S.*", "SX", "T", 0}, {"*.*", "S", "T", 0},
        {"s.t", "S", "T", 1}, {"S.T", "S", "U", 0}, {"X.*", "S", "U", 0}, {"s.*", "S", "OTHER", 1}};
    static const char *const invalid[] = {"*.*", "*.T", "S", ".T", "S.", "S.T*"};
    static alelt_config c;
    char json[512];
    size_t i;
    for (i = 0; i < sizeof(v) / sizeof(v[0]); i++) {
        int ok = alelt_config_allow_entry_valid(v[i].entry);
        if (!ok) { /* refused at config load (schema wildcard): never matches */
            CHECK(v[i].want == 0);
            continue;
        }
        snprintf(json, sizeof(json), "{" BASE ",\"hub\":{\"url\":\"http://x\",\"mode\":\"dev\"},"
                 "\"snapshot\":{\"allowed_tables\":[\"%s\"]}}", v[i].entry);
        CHECK(load(json, &c) == 0);
        CHECK(alelt_config_table_allowed(&c, v[i].schema, v[i].table) == v[i].want);
        alelt_config_free(&c);
    }
    for (i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        CHECK(!alelt_config_allow_entry_valid(invalid[i]));
        snprintf(json, sizeof(json), "{" BASE ",\"hub\":{\"url\":\"http://x\",\"mode\":\"dev\"},"
                 "\"snapshot\":{\"allowed_tables\":[\"%s\"]}}", invalid[i]);
        CHECK(load(json, &c) != 0);
    }
}

void test_config_suite(void)
{
    printf("config\n");
    RUN(t_load);
    RUN(t_reject);
    RUN(t_allowlist_vectors);
}
