/* SPDX-License-Identifier: Apache-2.0
 * Per-agent credential store (REQ-26 / AHS-04): exchange, persistence across a restart,
 * rotation schedule, expiry parsing, the enrollment token is no longer used once a
 * credential is stored, DPAPI at rest and the value never written to the log.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

#include "config.h"
#include "devtoken.h"
#include "alelttest.h"
#include "http.h"
#include "log.h"

static void iso_at(long long t, char *out, size_t cap)
{
    struct tm g;
    __time64_t tt = (__time64_t)t;
    _gmtime64_s(&g, &tt);
    strftime(out, cap, "%Y-%m-%dT%H:%M:%S.123456+00:00", &g);
}

static int file_has(const wchar_t *p, const char *needle)
{
    alelt_buf b;
    int hit = 0;
    size_t i, n = strlen(needle);
    alelt_buf_init(&b, 16 * 1024 * 1024);
    if (alelt_read_file(p, &b, 16 * 1024 * 1024) == 0) {
        for (i = 0; i + n <= b.len && !hit; i++) hit = memcmp(b.data + i, needle, n) == 0;
    }
    alelt_buf_free(&b);
    return hit;
}

static void t_iso(void)
{
    CHECK(alelt_iso_to_unix("1970-01-02T00:00:00Z") == 86400);
    CHECK(alelt_iso_to_unix("1970-01-02T00:00:00.999999+00:00") == 86400);
    CHECK(alelt_iso_to_unix("1970-01-02T01:00:00+01:00") == 86400);
    CHECK(alelt_iso_to_unix("1970-01-01T23:00:00-01:00") == 86400);
    CHECK(alelt_iso_to_unix("tomorrow") == -1 && alelt_iso_to_unix("2027-01-01T00:00:00 junk") == -1);
}

static void t_exchange_restart_rotate(void)
{
    static alelt_config c;
    wchar_t base[MAX_PATH], dir[MAX_PATH], logdir[MAX_PATH], cred[MAX_PATH], logf[MAX_PATH];
    char tok[256], exp[64], err[256], id[37];
    wchar_t wid[37];
    long long now = (long long)_time64(NULL);
    alelt_log_cfg lc;

    GetTempPathW(MAX_PATH, base);
    alelt_uuid4(id);
    alelt_to_wide(id, wid, 37);
    _snwprintf_s(dir, MAX_PATH, _TRUNCATE, L"%lsaleltcred-%ls", base, wid);
    _snwprintf_s(logdir, MAX_PATH, _TRUNCATE, L"%ls\\log", dir);
    CHECK(alelt_mkdirs(dir) == 0 && alelt_mkdirs(logdir) == 0);
    memset(&lc, 0, sizeof(lc));
    lc.dir = logdir;
    lc.level = ALELT_LOG_DEBUG;
    lc.max_bytes = 1 << 20;
    lc.keep = 1;
    CHECK(alelt_log_open(&lc) == 0);

    memset(&c, 0, sizeof(c));
    c.hub_mode = ALELT_HUB_DEV;
    alelt_wcscpy(c.spool_dir, ALELT_PATH_MAX, dir);
    _snwprintf_s(c.path, ALELT_PATH_MAX, _TRUNCATE, L"%ls\\aleltagent.json", dir);
    strcpy_s(c.dev_token_env, sizeof(c.dev_token_env), "ALELT_TEST_ENROLL_TOKEN_H2");
    SetEnvironmentVariableW(L"ALELT_TEST_ENROLL_TOKEN_H2", L"alelte_enrollment_sentinel_1");

    alelt_cred_reset_for_tests();
    CHECK(alelt_cred_init(&c) == 0);
    alelt_cred_get(tok, sizeof(tok));
    CHECK(strcmp(tok, "alelte_enrollment_sentinel_1") == 0 && !alelt_cred_is_per_agent()); /* before the exchange */
    CHECK(!alelt_cred_rotation_due(now));

    iso_at(now - 10, exp, sizeof(exp));
    CHECK(alelt_cred_store("alelta_x", exp, err, sizeof(err)) != 0);                     /* expired: refused */
    CHECK(alelt_cred_store("alelta_a\nb", "2099-01-01T00:00:00Z", err, sizeof(err)) != 0); /* framing chars refused */
    iso_at(now + 90LL * 86400, exp, sizeof(exp));
    CHECK(alelt_cred_store("alelta_secret_sentinel_2", exp, err, sizeof(err)) == 0);      /* register returned one */
    alelt_cred_get(tok, sizeof(tok));
    CHECK(strcmp(tok, "alelta_secret_sentinel_2") == 0 && alelt_cred_is_per_agent());
    _snwprintf_s(cred, MAX_PATH, _TRUNCATE, L"%ls\\credential.bin", dir);
    CHECK(GetFileAttributesW(cred) != INVALID_FILE_ATTRIBUTES && !file_has(cred, "alelta_secret_sentinel_2")); /* DPAPI */

    alelt_cred_reset_for_tests(); /* restart: the stored credential wins over the enrollment token */
    CHECK(alelt_cred_init(&c) == 0);
    alelt_cred_get(tok, sizeof(tok));
    CHECK(strcmp(tok, "alelta_secret_sentinel_2") == 0 && alelt_cred_is_per_agent());

    CHECK(!alelt_cred_rotation_due(now + 59LL * 86400));  /* rotation at 2/3 of the 90-day lifetime */
    CHECK(alelt_cred_rotation_due(now + 61LL * 86400));
    CHECK(alelt_cred_rotation_due(now + 90LL * 86400 - 3600));
    iso_at(now + 90LL * 86400, exp, sizeof(exp));
    CHECK(alelt_cred_store("alelta_rotated_sentinel_3", exp, err, sizeof(err)) == 0);
    alelt_cred_reset_for_tests();
    CHECK(alelt_cred_init(&c) == 0);
    alelt_cred_get(tok, sizeof(tok));
    CHECK(strcmp(tok, "alelta_rotated_sentinel_3") == 0);

    alelt_log_close();
    _snwprintf_s(logf, MAX_PATH, _TRUNCATE, L"%ls\\aleltagent.log", logdir);
    CHECK(GetFileAttributesW(logf) != INVALID_FILE_ATTRIBUTES);
    CHECK(!file_has(logf, "sentinel")); /* no token value ever reaches the log */
    SetEnvironmentVariableW(L"ALELT_TEST_ENROLL_TOKEN_H2", NULL);
    alelt_cred_reset_for_tests();
}

/* AHS-13: a PATH longer than 8 K is extended, never replaced; AHS-19: UNC / long-path roots */
static void t_path_util(void)
{
    size_t n = 12000, i;
    wchar_t *big = (wchar_t *)calloc(n + 1, sizeof(wchar_t)), *saved, *now;
    DWORD sn = GetEnvironmentVariableW(L"PATH", NULL, 0), m;
    CHECK(big != NULL);
    if (!big) return;
    saved = (wchar_t *)calloc((size_t)sn + 1, sizeof(wchar_t));
    if (saved && sn) GetEnvironmentVariableW(L"PATH", saved, sn);
    for (i = 0; i < n; i++) big[i] = (i % 50 == 49) ? L';' : L'x';
    big[n - 3] = L'E';
    big[n - 2] = L'N';
    big[n - 1] = L'D';
    SetEnvironmentVariableW(L"PATH", big);
    CHECK(alelt_path_env_prepend(L"C:\\db2\\bin") == 1);
    CHECK(alelt_path_env_prepend(L"C:\\db2\\bin") == 0); /* already present */
    m = GetEnvironmentVariableW(L"PATH", NULL, 0);
    now = (wchar_t *)calloc((size_t)m + 1, sizeof(wchar_t));
    if (now) GetEnvironmentVariableW(L"PATH", now, m);
    CHECK(now && wcsncmp(now, L"C:\\db2\\bin;", 11) == 0 && wcslen(now) == n + 11 &&
          wcscmp(now + wcslen(now) - 3, L"END") == 0);
    SetEnvironmentVariableW(L"PATH", (saved && sn) ? saved : NULL);
    free(now);
    free(saved);
    free(big);
    CHECK(alelt_path_root_len(L"C:\\a\\b") == 3);
    CHECK(alelt_path_root_len(L"\\\\srv\\share\\spool") == 12);
    CHECK(alelt_path_root_len(L"\\\\?\\UNC\\srv\\share\\spool") == 18);
    CHECK(alelt_path_root_len(L"\\\\?\\D:\\x") == 7);
    CHECK(alelt_path_root_len(L"rel\\x") == 0);
}

/* AHS-11: a token over plain http:// only to loopback (or an explicit lab opt-in); AHS-25: no proxy
 * auto-discovery unless hub.proxy says so. alelt_http_new opens no connection. */
static void t_http_link_policy(void)
{
    static alelt_config c;
    char err[512];
    alelt_http *h;
    memset(&c, 0, sizeof(c));
    c.hub_mode = ALELT_HUB_DEV;
    strcpy_s(c.agent_id, sizeof(c.agent_id), "a1");
    c.connect_timeout_ms = 1000;
    strcpy_s(c.hub_url, sizeof(c.hub_url), "http://hub.example.com:8080");
    CHECK(alelt_http_new(&c, err, sizeof(err)) == NULL && strstr(err, "refused") != NULL);
    c.allow_insecure_http = 1;
    h = alelt_http_new(&c, err, sizeof(err));
    CHECK(h != NULL && alelt_http_proxy_kind(h) == 0); /* direct: no WPAD from the database host */
    alelt_http_free(h);
    c.allow_insecure_http = 0;
    strcpy_s(c.hub_url, sizeof(c.hub_url), "http://127.0.0.1:8080");
    strcpy_s(c.hub_proxy, sizeof(c.hub_proxy), "proxy.corp:3128");
    h = alelt_http_new(&c, err, sizeof(err));
    CHECK(h != NULL && alelt_http_proxy_kind(h) == 2);
    alelt_http_free(h);
    strcpy_s(c.hub_proxy, sizeof(c.hub_proxy), "system");
    h = alelt_http_new(&c, err, sizeof(err));
    CHECK(h != NULL && alelt_http_proxy_kind(h) == 1);
    alelt_http_free(h);
    strcpy_s(c.hub_url, sizeof(c.hub_url), "https://hub.example.com");
    c.hub_proxy[0] = 0;
    h = alelt_http_new(&c, err, sizeof(err));
    CHECK(h != NULL); /* https: always allowed */
    alelt_http_free(h);
    alelt_cred_reset_for_tests();
}

/* AHS-18: the DPAPI devtoken.bin next to the config wins over the environment variable. */
static void t_devtoken_file_over_env(void)
{
    static alelt_config c;
    wchar_t base[MAX_PATH], dir[MAX_PATH], p[ALELT_PATH_MAX], wid[37];
    char id[37], tok[256], err[256];
    GetTempPathW(MAX_PATH, base);
    alelt_uuid4(id);
    alelt_to_wide(id, wid, 37);
    _snwprintf_s(dir, MAX_PATH, _TRUNCATE, L"%lsaleltdevtok-%ls", base, wid);
    CHECK(alelt_mkdirs(dir) == 0);
    memset(&c, 0, sizeof(c));
    _snwprintf_s(c.path, ALELT_PATH_MAX, _TRUNCATE, L"%ls\\aleltagent.json", dir);
    strcpy_s(c.dev_token_env, sizeof(c.dev_token_env), "ALELT_TEST_DEVTOKEN_AHS18");
    SetEnvironmentVariableW(L"ALELT_TEST_DEVTOKEN_AHS18", L"from-env");
    CHECK(alelt_dev_token_load(&c, tok, sizeof(tok)) == 0 && strcmp(tok, "from-env") == 0);
    CHECK(alelt_dev_token_save(c.path, "from-dpapi-file", err, sizeof(err)) == 0);
    CHECK(alelt_dev_token_load(&c, tok, sizeof(tok)) == 0 && strcmp(tok, "from-dpapi-file") == 0);
    CHECK(alelt_dev_token_path(c.path, p, ALELT_PATH_MAX) == 0 && !file_has(p, "from-dpapi-file")); /* encrypted */
    DeleteFileW(p);
    SetEnvironmentVariableW(L"ALELT_TEST_DEVTOKEN_AHS18", NULL);
    CHECK(alelt_dev_token_load(&c, tok, sizeof(tok)) != 0);
    RemoveDirectoryW(dir);
}

void test_cred_suite(void)
{
    printf("credential (REQ-26) + paths\n");
    RUN(t_path_util);
    RUN(t_iso);
    RUN(t_exchange_restart_rotate);
    RUN(t_http_link_policy);
    RUN(t_devtoken_file_over_env);
}
