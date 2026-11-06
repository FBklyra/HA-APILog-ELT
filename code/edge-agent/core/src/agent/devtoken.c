/* SPDX-License-Identifier: Apache-2.0 */
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <dpapi.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "devtoken.h"
#include "log.h"

int alelt_dev_token_path(const wchar_t *config_path, wchar_t *out, size_t cap)
{
    wchar_t *slash;
    if (alelt_wcscpy(out, cap, config_path) != 0) return -1;
    slash = wcsrchr(out, L'\\');
    if (!slash) return -1;
    slash[1] = 0;
    return wcslen(out) + 13 < cap ? (wcscat_s(out, cap, L"devtoken.bin") == 0 ? 0 : -1) : -1;
}

static int load_token_file(const wchar_t *p, char *out, size_t cap)
{
    alelt_buf b;
    DATA_BLOB in, plain;
    alelt_buf_init(&b, 64 * 1024);
    if (alelt_read_file(p, &b, 64 * 1024) != 0) {
        alelt_buf_free(&b);
        return -1;
    }
    in.cbData = (DWORD)b.len;
    in.pbData = b.data;
    if (!CryptUnprotectData(&in, NULL, NULL, NULL, NULL, CRYPTPROTECT_UI_FORBIDDEN, &plain)) {
        alelt_buf_free(&b);
        return -1;
    }
    alelt_buf_free(&b);
    if (plain.cbData == 0 || plain.cbData >= cap) {
        SecureZeroMemory(plain.pbData, plain.cbData);
        LocalFree(plain.pbData);
        return -1;
    }
    memcpy(out, plain.pbData, plain.cbData);
    out[plain.cbData] = 0;
    SecureZeroMemory(plain.pbData, plain.cbData);
    LocalFree(plain.pbData);
    return 0;
}

/* AHS-18: the ACL'd DPAPI file wins; a (system-wide, readable by every local user) environment
 * variable is only a fallback and is reported. */
int alelt_dev_token_load(const alelt_config *cfg, char *out, size_t cap)
{
    wchar_t name[64], val[256], p[ALELT_PATH_MAX];
    DWORD n;
    out[0] = 0;
    if (alelt_dev_token_path(cfg->path, p, ALELT_PATH_MAX) == 0 && load_token_file(p, out, cap) == 0) return 0;
    if (cfg->dev_token_env[0] && alelt_to_wide(cfg->dev_token_env, name, 64) == 0) {
        n = GetEnvironmentVariableW(name, val, 256);
        if (n > 0 && n < 256) {
            int r = alelt_to_utf8(val, out, cap);
            SecureZeroMemory(val, sizeof(val));
            if (r == 0 && out[0]) {
                alelt_logf(ALELT_LOG_WARN, "http", "agent token read from environment variable %s (readable by local users); "
                        "prefer 'aleltagent --set-token' (DPAPI devtoken.bin)", cfg->dev_token_env);
                return 0;
            }
        }
    }
    return -1;
}

int alelt_dev_token_save(const wchar_t *config_path, const char *token, char *err, size_t errcap)
{
    wchar_t p[ALELT_PATH_MAX];
    DATA_BLOB in, enc;
    int rc;
    if (!token || !*token || strlen(token) > 250) {
        snprintf(err, errcap, "token must be 1..250 characters");
        return -1;
    }
    if (alelt_dev_token_path(config_path, p, ALELT_PATH_MAX) != 0) {
        snprintf(err, errcap, "bad config path");
        return -1;
    }
    in.cbData = (DWORD)strlen(token);
    in.pbData = (BYTE *)token;
    if (!CryptProtectData(&in, L"ALELT dev token", NULL, NULL, NULL,
                          CRYPTPROTECT_UI_FORBIDDEN | CRYPTPROTECT_LOCAL_MACHINE, &enc)) {
        snprintf(err, errcap, "CryptProtectData failed (%lu)", (unsigned long)GetLastError());
        return -1;
    }
    rc = alelt_write_file_atomic(p, enc.pbData, enc.cbData);
    LocalFree(enc.pbData);
    if (rc != 0) snprintf(err, errcap, "cannot write %ls (%lu)", p, (unsigned long)GetLastError());
    return rc;
}

/* ------------------------------------------------------------------ per-agent credential (REQ-26) */
static INIT_ONCE g_cred_once = INIT_ONCE_STATIC_INIT;
static CRITICAL_SECTION g_cred_lock;
static int g_cred_ready;
static int g_cred_per_agent;
static char g_cred[256];
static long long g_cred_exp, g_cred_issued;
static wchar_t g_cred_path[ALELT_PATH_MAX];

static BOOL CALLBACK cred_once(PINIT_ONCE o, PVOID p, PVOID *c)
{
    (void)o;
    (void)p;
    (void)c;
    InitializeCriticalSection(&g_cred_lock);
    return TRUE;
}

static void cred_lock(void)
{
    InitOnceExecuteOnce(&g_cred_once, cred_once, NULL, NULL);
    EnterCriticalSection(&g_cred_lock);
}

long long alelt_iso_to_unix(const char *iso)
{
    int y, mo, d, hh, mi, ss, n = 0;
    struct tm t;
    long long v;
    const char *z;
    if (!iso || sscanf_s(iso, "%4d-%2d-%2dT%2d:%2d:%2d%n", &y, &mo, &d, &hh, &mi, &ss, &n) != 6 || n < 19) return -1;
    memset(&t, 0, sizeof(t));
    t.tm_year = y - 1900;
    t.tm_mon = mo - 1;
    t.tm_mday = d;
    t.tm_hour = hh;
    t.tm_min = mi;
    t.tm_sec = ss;
    v = (long long)_mkgmtime64(&t);
    if (v < 0) return -1;
    z = iso + n;
    if (*z == '.') {
        z++;
        while (*z >= '0' && *z <= '9') z++;
    }
    if (*z == '+' || *z == '-') { /* local = UTC + offset */
        int oh = 0, om = 0;
        if (sscanf_s(z + 1, "%2d:%2d", &oh, &om) < 1) return -1;
        v += (*z == '+' ? -1 : 1) * ((long long)oh * 3600 + (long long)om * 60);
    } else if (*z && *z != 'Z' && *z != 'z') {
        return -1;
    }
    return v;
}

/* blob = "<credential>\n<expires_unix>\n<issued_unix>" */
static int cred_load_file(const wchar_t *p, char *cred, size_t cap, long long *exp, long long *issued)
{
    char tmp[512], *nl1, *nl2;
    int rc = -1;
    if (load_token_file(p, tmp, sizeof(tmp)) != 0) return -1;
    nl1 = strchr(tmp, '\n');
    nl2 = nl1 ? strchr(nl1 + 1, '\n') : NULL;
    if (nl1 && nl2) {
        *nl1 = 0;
        *nl2 = 0;
        *exp = _strtoi64(nl1 + 1, NULL, 10);
        *issued = _strtoi64(nl2 + 1, NULL, 10);
        if (tmp[0] && alelt_strcpy(cred, cap, tmp) == 0) rc = 0;
    }
    SecureZeroMemory(tmp, sizeof(tmp));
    return rc;
}

int alelt_cred_init(const alelt_config *cfg)
{
    int rc;
    cred_lock();
    if (!g_cred_ready) {
        long long exp = 0, iss = 0;
        g_cred_ready = 1;
        g_cred_per_agent = 0;
        g_cred[0] = 0;
        g_cred_path[0] = 0;
        if (cfg->spool_dir[0] && alelt_path_join(g_cred_path, ALELT_PATH_MAX, cfg->spool_dir, L"credential.bin") != 0)
            g_cred_path[0] = 0;
        if (cfg->hub_mode == ALELT_HUB_DEV && g_cred_path[0] &&
            cred_load_file(g_cred_path, g_cred, sizeof(g_cred), &exp, &iss) == 0) {
            g_cred_per_agent = 1;
            g_cred_exp = exp;
            g_cred_issued = iss;
            alelt_logf(ALELT_LOG_INFO, "http", "using the stored per-agent credential (the enrollment token is no longer used)");
        } else if (cfg->hub_mode == ALELT_HUB_DEV && alelt_dev_token_load(cfg, g_cred, sizeof(g_cred)) != 0) {
            alelt_logf(ALELT_LOG_WARN, "http", "token mode: no agent token found (devtoken.bin or env %s)", cfg->dev_token_env);
        }
    }
    rc = g_cred[0] ? 0 : -1;
    LeaveCriticalSection(&g_cred_lock);
    return rc;
}

void alelt_cred_get(char *out, size_t cap)
{
    cred_lock();
    if (alelt_strcpy(out, cap, g_cred) != 0) out[0] = 0;
    LeaveCriticalSection(&g_cred_lock);
}

int alelt_cred_is_per_agent(void)
{
    int v;
    cred_lock();
    v = g_cred_per_agent;
    LeaveCriticalSection(&g_cred_lock);
    return v;
}

int alelt_cred_store(const char *cred, const char *expires_iso, char *err, size_t errcap)
{
    char plain[512];
    DATA_BLOB in, enc;
    long long exp = alelt_iso_to_unix(expires_iso), now = (long long)_time64(NULL);
    int n, rc;
    if (!cred || !*cred || strlen(cred) >= sizeof(g_cred) || strpbrk(cred, "\r\n") || exp <= now) {
        snprintf(err, errcap, "credential or expiry invalid");
        return -1;
    }
    cred_lock();
    if (!g_cred_path[0]) {
        LeaveCriticalSection(&g_cred_lock);
        snprintf(err, errcap, "credential store not initialised");
        return -1;
    }
    n = snprintf(plain, sizeof(plain), "%s\n%lld\n%lld", cred, exp, now);
    in.cbData = (DWORD)(n > 0 ? n : 0);
    in.pbData = (BYTE *)plain;
    if (n <= 0 || !CryptProtectData(&in, L"ALELT agent credential", NULL, NULL, NULL,
                                    CRYPTPROTECT_UI_FORBIDDEN | CRYPTPROTECT_LOCAL_MACHINE, &enc)) {
        SecureZeroMemory(plain, sizeof(plain));
        LeaveCriticalSection(&g_cred_lock);
        snprintf(err, errcap, "CryptProtectData failed (%lu)", (unsigned long)GetLastError());
        return -1;
    }
    SecureZeroMemory(plain, sizeof(plain));
    rc = alelt_write_file_atomic(g_cred_path, enc.pbData, enc.cbData); /* durable first ... */
    LocalFree(enc.pbData);
    if (rc == 0) { /* ... then switch; a crash in between keeps the old one (valid during the grace) */
        alelt_strcpy(g_cred, sizeof(g_cred), cred);
        g_cred_per_agent = 1;
        g_cred_exp = exp;
        g_cred_issued = now;
    } else {
        snprintf(err, errcap, "cannot write credential.bin (%lu)", (unsigned long)GetLastError());
    }
    LeaveCriticalSection(&g_cred_lock);
    return rc;
}

int alelt_cred_rotation_due(long long now_unix)
{
    int due = 0;
    cred_lock();
    if (g_cred_per_agent && g_cred_exp > 0) {
        long long life = g_cred_exp - g_cred_issued;
        if (life < 0) life = 0;
        due = now_unix >= g_cred_issued + life * 2 / 3; /* short-lived credentials: no rotation storm */
    }
    LeaveCriticalSection(&g_cred_lock);
    return due;
}

void alelt_cred_reset_for_tests(void)
{
    cred_lock();
    SecureZeroMemory(g_cred, sizeof(g_cred));
    g_cred_ready = 0;
    g_cred_per_agent = 0;
    g_cred_exp = g_cred_issued = 0;
    g_cred_path[0] = 0;
    LeaveCriticalSection(&g_cred_lock);
}
