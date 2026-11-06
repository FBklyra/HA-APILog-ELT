/* SPDX-License-Identifier: Apache-2.0 - production guard: deny_hosts + environment (REQ-12) */
#include <stdio.h>
#include <string.h>

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

#include "config.h"
#include "alelttest.h"
#include "guard.h"

static int load(const char *json, alelt_config *c)
{
    wchar_t base[MAX_PATH], p[MAX_PATH];
    char err[512];
    int r;
    GetTempPathW(MAX_PATH, base);
    _snwprintf_s(p, MAX_PATH, _TRUNCATE, L"%lsaleltagent-guard-%lu.json", base, (unsigned long)GetCurrentProcessId());
    if (alelt_write_file_atomic(p, json, strlen(json)) != 0) return -9;
    r = alelt_config_load(p, c, err, sizeof(err));
    DeleteFileW(p);
    return r;
}

#define HUB "\"agent_id\":\"a\",\"connection_id\":\"c\",\"hub\":{\"url\":\"http://x\",\"mode\":\"dev\"},"

static void t_match(void)
{
    CHECK(alelt_deny_match("db2prod.corp.local", "DB2PROD.corp.local", -1));
    CHECK(alelt_deny_match("*.prod.corp.local", "db2.prod.corp.local", -1));
    CHECK(!alelt_deny_match("*.prod.corp.local", "db2.test.corp.local", -1));
    CHECK(alelt_deny_match("db2prod:50000", "db2prod", 50000));
    CHECK(!alelt_deny_match("db2prod:50000", "db2prod", 50001));
    CHECK(alelt_deny_match("db2prod:50000", "db2prod", -1));          /* unknown port: host decides (as the hub) */
    CHECK(alelt_deny_match("10.20.0.0/16", "10.20.3.4", -1));
    CHECK(!alelt_deny_match("10.20.0.0/16", "10.21.3.4", -1));
    CHECK(alelt_deny_match("10.20.0.15", "10.20.0.15:50000", -1));
    CHECK(alelt_deny_match("xy12345.ca-central-1", "xy12345.ca-central-1.snowflakecomputing.com", -1));
    CHECK(alelt_deny_match("https://api.prod.example", "api.prod.example", -1));
    CHECK(!alelt_deny_match("prod", "production-test", -1));
    /* AHS-08 */
    CHECK(alelt_deny_match("*.prod.corp.local", "db2prod.prod.corp.local.", -1));    /* trailing-dot FQDN */
    CHECK(alelt_deny_match("db2prod.corp.local.", "DB2PROD.corp.local", -1));
    CHECK(alelt_deny_match("fd00:10::/32", "fd00:10:0:1::5", -1));                   /* IPv6 CIDR */
    CHECK(!alelt_deny_match("fd00:10::/32", "fd00:11::5", -1));
    CHECK(alelt_deny_match("::1", "[::1]:50000", -1));                               /* IPv6 literal with port */
    CHECK(alelt_deny_match("127.0.0.0/8", "localhost", -1));                         /* resolved name (v4 or v6) */
    CHECK(alelt_deny_match("10.0.0.0/8", "db2.nonexistent.invalid", -1) == ALELT_DENY_UNRESOLVED); /* fail closed */
    CHECK(!alelt_deny_match("*a*a*a*a*a*a*a*a*a*a*a*b", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", -1));
    CHECK(alelt_deny_match("db2?.prod.*", "db2a.prod.local", -1));
}

static void t_config(void)
{
    static alelt_config c;
    char err[512];
    CHECK(load("{" HUB "\"deny_hosts\":[\"DB2PROD.corp.local\",\"*.prod.local\"],\"environment\":\"TEST\","
               "\"source\":{\"database\":\"ERPTEST\",\"settings\":{\"host\":\"db2test\",\"port\":50000}}}", &c) == 0);
    CHECK(c.ndeny == 2 && strcmp(c.deny_hosts[0], "db2prod.corp.local") == 0);
    CHECK(strcmp(c.src_host, "db2test") == 0 && c.src_port == 50000);
    CHECK(alelt_guard_check(&c, err, sizeof(err)) == 0);
    alelt_config_free(&c);
    CHECK(load("{" HUB "\"deny_hosts\":[\"db2prod.corp.local\"],"
               "\"source\":{\"database\":\"ERPTEST\",\"settings\":{\"host\":\"db2prod.corp.local\"}}}", &c) == 0);
    CHECK(alelt_guard_check(&c, err, sizeof(err)) != 0 && strstr(err, "deny list") != NULL);
    alelt_config_free(&c);
    CHECK(load("{" HUB "\"deny_hosts\":[\"prodalias\"],\"source\":{\"database\":\"PRODALIAS\"}}", &c) == 0);
    CHECK(alelt_guard_check(&c, err, sizeof(err)) != 0);                 /* local catalog alias denied too */
    alelt_config_free(&c);
    CHECK(load("{" HUB "\"environment\":\"PROD\",\"source\":{\"database\":\"ERPTEST\"}}", &c) == 0);
    CHECK(alelt_guard_check(&c, err, sizeof(err)) != 0 && strstr(err, "allow_prod") != NULL);
    alelt_config_free(&c);
    CHECK(load("{" HUB "\"environment\":\"PROD\",\"allow_prod\":true,\"source\":{\"database\":\"ERPTEST\"}}", &c) == 0);
    CHECK(alelt_guard_check(&c, err, sizeof(err)) != 0); /* AHS-08: the JSON (may come from the hub) is not enough */
    {
        wchar_t ap[MAX_PATH];
        CHECK(alelt_guard_allow_prod_path(c.path, ap, MAX_PATH) == 0);
        CHECK(alelt_write_file_atomic(ap, "DBA", 3) == 0);   /* DBA-owned local file */
        CHECK(alelt_guard_check(&c, err, sizeof(err)) == 0);
        DeleteFileW(ap);
    }
    alelt_config_free(&c);
    CHECK(load("{" HUB "\"deny_hosts\":[\"10.0.0.0/8\"],\"source\":{\"database\":\"ERPTEST\",\"settings\":"
               "{\"host\":\"db2.nonexistent.invalid\"}}}", &c) == 0);
    CHECK(alelt_guard_check(&c, err, sizeof(err)) != 0 && strstr(err, "fail closed") != NULL);
    alelt_config_free(&c);
    /* the wizard package (hub.mode token + token_env, deny_hosts, environment, capture, snapshot) is accepted */
    CHECK(load("{\"agent_id\":\"a\",\"connection_id\":\"c\",\"hub\":{\"url\":\"http://h:8080\",\"mode\":\"token\","
               "\"token_env\":\"ALELT_AGENT_TOKEN\",\"heartbeat_s\":15},\"deny_hosts\":[\"db2prod\"],\"environment\":\"TEST\","
               "\"source\":{\"database\":\"ERPTEST\",\"settings\":{\"database\":\"ERPTEST\",\"catalog_schemas\":[\"APPSCHEMA\"]}},"
               "\"snapshot\":{\"allowed_tables\":[\"APPSCHEMA.CUSTOMER\"],\"max_chunk_rows\":5000},\"capture\":{\"autostart\":true}}", &c) == 0);
    CHECK(c.hub_mode == ALELT_HUB_DEV && strcmp(c.dev_token_env, "ALELT_AGENT_TOKEN") == 0 && c.capture_autostart == 1);
    alelt_config_free(&c);
    CHECK(load("{" HUB "\"environment\":\"STAGING\",\"source\":{\"database\":\"ERPTEST\"}}", &c) != 0);
    CHECK(load("{" HUB "\"deny_hosts\":[1],\"source\":{\"database\":\"ERPTEST\"}}", &c) != 0);
}

/* AHS-08: the alias says nothing - the server the connection reached (plugin probe "server_info") is checked. */
static void t_connected(void)
{
    alelt_config c;
    char err[512];
    CHECK(load("{" HUB "\"deny_hosts\":[\"*.prod.corp.local\",\"erpprod\"],\"source\":{\"database\":\"HARMLESS\"}}",
               &c) == 0);
    CHECK(alelt_guard_check(&c, err, sizeof(err)) == 0); /* the configured alias looks harmless */
    CHECK(alelt_guard_check_connected(&c, "{\"probe\":\"server_info\",\"host\":\"DB2P01.PROD.CORP.LOCAL\"}", err,
                                   sizeof(err)) == -1 && strstr(err, "DB2P01") != NULL);
    CHECK(alelt_guard_check_connected(&c, "{\"host\":\"db2t.test.corp.local\",\"database_name\":\"ERPPROD\"}", err,
                                   sizeof(err)) == -1); /* real database name behind the alias */
    CHECK(alelt_guard_check_connected(&c, "{\"host\":\"db2t.test.corp.local\",\"database_name\":\"ERPTEST\"}", err,
                                   sizeof(err)) == 0);
    CHECK(alelt_guard_check_connected(&c, "{\"host\":null}", err, sizeof(err)) == -1); /* fail closed */
    CHECK(alelt_guard_check_connected(&c, NULL, err, sizeof(err)) == -1);            /* probe failed / old plugin */
    CHECK(alelt_guard_check_connected(&c, "not json", err, sizeof(err)) == -1);
    alelt_config_free(&c);
    CHECK(load("{" HUB "\"source\":{\"database\":\"ERPTEST\"}}", &c) == 0);
    CHECK(alelt_guard_check_connected(&c, NULL, err, sizeof(err)) == 0); /* no deny list: no check */
    alelt_config_free(&c);
}

void test_guard_suite(void)
{
    printf("guard\n");
    RUN(t_match);
    RUN(t_config);
    RUN(t_connected);
}
