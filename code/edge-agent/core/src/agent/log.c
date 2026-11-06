/* SPDX-License-Identifier: Apache-2.0
 * Structured JSON-lines log with size rotation + Event Log for warnings/errors.
 */
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include "alelt_buf.h"
#include "alelt_json.h"
#include "alelt_util.h"
#include "log.h"
#include "version.h"

static CRITICAL_SECTION g_lock;
static int g_init = 0;
static HANDLE g_file = INVALID_HANDLE_VALUE;
static wchar_t g_path[ALELT_PATH_MAX];
static uint64_t g_size = 0;
static alelt_log_cfg g_cfg;
static wchar_t g_dir[ALELT_PATH_MAX];
static HANDLE g_evt = NULL;

static const char *lvl_name(int l)
{
    switch (l) {
    case ALELT_LOG_DEBUG: return "DEBUG";
    case ALELT_LOG_INFO: return "INFO";
    case ALELT_LOG_WARN: return "WARNING";
    default: return "ERROR";
    }
}

int alelt_log_level_from_str(const char *s)
{
    if (!s) return ALELT_LOG_INFO;
    if (alelt_stricmp_ascii(s, "DEBUG") == 0) return ALELT_LOG_DEBUG;
    if (alelt_stricmp_ascii(s, "WARNING") == 0 || alelt_stricmp_ascii(s, "WARN") == 0) return ALELT_LOG_WARN;
    if (alelt_stricmp_ascii(s, "ERROR") == 0) return ALELT_LOG_ERROR;
    return ALELT_LOG_INFO;
}

static void open_file(void)
{
    LARGE_INTEGER sz;
    if (!g_dir[0]) return;
    g_file = CreateFileW(g_path, FILE_APPEND_DATA, FILE_SHARE_READ | FILE_SHARE_DELETE, NULL, OPEN_ALWAYS,
                         FILE_ATTRIBUTE_NORMAL, NULL);
    if (g_file == INVALID_HANDLE_VALUE) return;
    alelt_low_io_priority(g_file);
    g_size = GetFileSizeEx(g_file, &sz) ? (uint64_t)sz.QuadPart : 0;
}

static void rotate(void)
{
    int i;
    wchar_t a[ALELT_PATH_MAX], b[ALELT_PATH_MAX];
    if (g_file != INVALID_HANDLE_VALUE) {
        CloseHandle(g_file);
        g_file = INVALID_HANDLE_VALUE;
    }
    for (i = g_cfg.keep; i >= 1; i--) {
        if (_snwprintf_s(b, ALELT_PATH_MAX, _TRUNCATE, L"%ls.%d", g_path, i) < 0) continue;
        if (i == 1) {
            alelt_wcscpy(a, ALELT_PATH_MAX, g_path);
        } else if (_snwprintf_s(a, ALELT_PATH_MAX, _TRUNCATE, L"%ls.%d", g_path, i - 1) < 0) {
            continue;
        }
        if (i == g_cfg.keep) DeleteFileW(b);
        (void)MoveFileExW(a, b, MOVEFILE_REPLACE_EXISTING);
    }
    open_file();
}

int alelt_log_open(const alelt_log_cfg *cfg)
{
    if (!g_init) {
        InitializeCriticalSection(&g_lock);
        g_init = 1;
    }
    EnterCriticalSection(&g_lock);
    g_cfg = *cfg;
    if (g_cfg.keep < 1) g_cfg.keep = 1;
    if (g_cfg.keep > 50) g_cfg.keep = 50;
    if (g_cfg.max_bytes < 1024 * 1024) g_cfg.max_bytes = 1024 * 1024;
    g_dir[0] = 0;
    if (cfg->dir && cfg->dir[0]) {
        if (alelt_wcscpy(g_dir, ALELT_PATH_MAX, cfg->dir) != 0 || alelt_mkdirs(g_dir) != 0 ||
            alelt_path_join(g_path, ALELT_PATH_MAX, g_dir, L"aleltagent.log") != 0) {
            g_dir[0] = 0;
        } else {
            open_file();
        }
    }
    g_cfg.dir = NULL;
    if (cfg->eventlog && !g_evt) {
        g_evt = RegisterEventSourceW(NULL, ALELT_SERVICE_NAME_W);
    }
    LeaveCriticalSection(&g_lock);
    return (cfg->dir && cfg->dir[0] && g_file == INVALID_HANDLE_VALUE) ? -1 : 0;
}

void alelt_log_close(void)
{
    if (!g_init) return;
    EnterCriticalSection(&g_lock);
    if (g_file != INVALID_HANDLE_VALUE) {
        FlushFileBuffers(g_file);
        CloseHandle(g_file);
        g_file = INVALID_HANDLE_VALUE;
    }
    if (g_evt) {
        DeregisterEventSource(g_evt);
        g_evt = NULL;
    }
    LeaveCriticalSection(&g_lock);
}

void alelt_eventlog(int level, const char *msg)
{
    wchar_t w[2048];
    const wchar_t *strs[1];
    WORD type;
    DWORD id;
    if (!g_evt) return;
    if (alelt_to_wide(msg, w, 2048) != 0) alelt_wcscpy(w, 2048, L"(message not representable)");
    strs[0] = w;
    if (level >= ALELT_LOG_ERROR) {
        type = EVENTLOG_ERROR_TYPE;
        id = (DWORD)ALELT_EVT_GENERIC_ERROR;
    } else if (level == ALELT_LOG_WARN) {
        type = EVENTLOG_WARNING_TYPE;
        id = (DWORD)ALELT_EVT_GENERIC_WARN;
    } else {
        type = EVENTLOG_INFORMATION_TYPE;
        id = (DWORD)ALELT_EVT_GENERIC_INFO;
    }
    (void)ReportEventW(g_evt, type, 0, id, NULL, 1, 0, strs, NULL);
}

void alelt_log_ev(int level, const char *component, const char *msg, const char *batch_id, const char *position,
               const char *table, const char *correlation_id)
{
    alelt_buf b;
    alelt_jw w;
    char ts[ALELT_TS_LEN];
    if (!g_init) {
        InitializeCriticalSection(&g_lock);
        g_init = 1;
        g_cfg.level = ALELT_LOG_INFO;
        g_cfg.console = 1;
    }
    if (level < g_cfg.level) return;
    alelt_buf_init(&b, 64 * 1024);
    jw_init(&w, &b);
    alelt_now_iso(ts);
    jw_obj(&w);
    jw_key(&w, "ts"); jw_str(&w, ts);
    jw_key(&w, "level"); jw_str(&w, lvl_name(level));
    jw_key(&w, "component"); jw_str(&w, component ? component : "agent");
    jw_key(&w, "msg"); jw_str(&w, msg ? msg : "");
    if (batch_id && *batch_id) { jw_key(&w, "batch_id"); jw_str(&w, batch_id); }
    if (position && *position) { jw_key(&w, "position"); jw_str(&w, position); }
    if (table && *table) { jw_key(&w, "table"); jw_str(&w, table); }
    if (correlation_id && *correlation_id) { jw_key(&w, "correlation_id"); jw_str(&w, correlation_id); }
    jw_obj_end(&w);
    alelt_buf_putc(&b, '\n');
    if (b.failed) {
        alelt_buf_free(&b);
        return;
    }
    EnterCriticalSection(&g_lock);
    if (g_file != INVALID_HANDLE_VALUE) {
        DWORD wr = 0;
        if (WriteFile(g_file, b.data, (DWORD)b.len, &wr, NULL)) g_size += wr;
        if (g_size > g_cfg.max_bytes) rotate();
    }
    if (g_cfg.console) {
        fwrite(b.data, 1, b.len, stderr);
        fflush(stderr);
    }
    if (level >= ALELT_LOG_ERROR && g_evt) {
        alelt_eventlog(level, (const char *)b.data);
    }
    LeaveCriticalSection(&g_lock);
    alelt_buf_free(&b);
}

void alelt_logf(int level, const char *component, const char *fmt, ...)
{
    char msg[2048];
    va_list ap;
    if (g_init && level < g_cfg.level) return;
    va_start(ap, fmt);
    (void)vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    msg[sizeof(msg) - 1] = 0;
    alelt_log_ev(level, component, msg, NULL, NULL, NULL, NULL);
}
