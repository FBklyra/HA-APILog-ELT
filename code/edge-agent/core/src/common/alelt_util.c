/* SPDX-License-Identifier: Apache-2.0 */
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <bcrypt.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "alelt_util.h"

int alelt_strcpy(char *dst, size_t cap, const char *src)
{
    size_t n;
    if (!dst || cap == 0) return -1;
    if (!src) src = "";
    n = strlen(src);
    if (n >= cap) {
        memcpy(dst, src, cap - 1);
        dst[cap - 1] = 0;
        return -1;
    }
    memcpy(dst, src, n + 1);
    return 0;
}

int alelt_wcscpy(wchar_t *dst, size_t cap, const wchar_t *src)
{
    size_t n;
    if (!dst || cap == 0) return -1;
    if (!src) src = L"";
    n = wcslen(src);
    if (n >= cap) {
        wmemcpy(dst, src, cap - 1);
        dst[cap - 1] = 0;
        return -1;
    }
    wmemcpy(dst, src, n + 1);
    return 0;
}

int alelt_stricmp_ascii(const char *a, const char *b)
{
    for (;; a++, b++) {
        int ca = (unsigned char)*a, cb = (unsigned char)*b;
        if (ca >= 'A' && ca <= 'Z') ca += 32;
        if (cb >= 'A' && cb <= 'Z') cb += 32;
        if (ca != cb) return ca - cb;
        if (!ca) return 0;
    }
}

int alelt_to_wide(const char *s, wchar_t *out, size_t cap)
{
    int n;
    if (!out || cap == 0 || cap > 0x7FFFFFFF) return -1;
    out[0] = 0;
    if (!s) return 0;
    n = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, s, -1, out, (int)cap);
    if (n <= 0) {
        out[0] = 0;
        return -1;
    }
    return 0;
}

int alelt_to_utf8(const wchar_t *s, char *out, size_t cap)
{
    int n;
    if (!out || cap == 0 || cap > 0x7FFFFFFF) return -1;
    out[0] = 0;
    if (!s) return 0;
    n = WideCharToMultiByte(CP_UTF8, 0, s, -1, out, (int)cap, NULL, NULL);
    if (n <= 0) {
        out[0] = 0;
        return -1;
    }
    return 0;
}

wchar_t *alelt_to_wide_alloc(const char *s)
{
    int n;
    wchar_t *w;
    if (!s) s = "";
    n = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, s, -1, NULL, 0);
    if (n <= 0) return NULL;
    w = (wchar_t *)malloc((size_t)n * sizeof(wchar_t));
    if (!w) return NULL;
    if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, s, -1, w, n) <= 0) {
        free(w);
        return NULL;
    }
    return w;
}

void alelt_hex(const uint8_t *p, size_t n, char *out, int upper)
{
    const char *hx = upper ? "0123456789ABCDEF" : "0123456789abcdef";
    size_t i;
    for (i = 0; i < n; i++) {
        out[2 * i] = hx[p[i] >> 4];
        out[2 * i + 1] = hx[p[i] & 15];
    }
    out[2 * n] = 0;
}

static int hv(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

int alelt_unhex(const char *s, uint8_t *out, size_t cap, size_t *outlen)
{
    size_t n = 0;
    while (*s) {
        int a, b;
        if (*s == ' ' || *s == ':') { s++; continue; } /* tolerate pasted thumbprints */
        a = hv(s[0]);
        if (a < 0 || !s[1]) return -1;
        b = hv(s[1]);
        if (b < 0 || n >= cap) return -1;
        out[n++] = (uint8_t)((a << 4) | b);
        s += 2;
    }
    *outlen = n;
    return 0;
}

void alelt_now_iso(char out[ALELT_TS_LEN])
{
    SYSTEMTIME st;
    GetSystemTime(&st);
    snprintf(out, ALELT_TS_LEN, "%04u-%02u-%02uT%02u:%02u:%02u.%03uZ", (unsigned)st.wYear, (unsigned)st.wMonth,
             (unsigned)st.wDay, (unsigned)st.wHour, (unsigned)st.wMinute, (unsigned)st.wSecond,
             (unsigned)st.wMilliseconds);
}

uint64_t alelt_mono_ms(void)
{
    return (uint64_t)GetTickCount64();
}

int alelt_random(void *p, size_t n)
{
    if (n > 0xFFFFFFFFu) return -1;
    return BCRYPT_SUCCESS(BCryptGenRandom(NULL, (PUCHAR)p, (ULONG)n, BCRYPT_USE_SYSTEM_PREFERRED_RNG)) ? 0 : -1;
}

int alelt_uuid4(char out[ALELT_UUID_LEN + 1])
{
    uint8_t b[16];
    char h[33];
    if (alelt_random(b, sizeof(b)) != 0) return -1;
    b[6] = (uint8_t)((b[6] & 0x0F) | 0x40);
    b[8] = (uint8_t)((b[8] & 0x3F) | 0x80);
    alelt_hex(b, 16, h, 0);
    snprintf(out, ALELT_UUID_LEN + 1, "%.8s-%.4s-%.4s-%.4s-%.12s", h, h + 8, h + 12, h + 16, h + 20);
    return 0;
}

int alelt_is_uuid(const char *s)
{
    int i;
    if (!s || strlen(s) != 36) return 0;
    for (i = 0; i < 36; i++) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (s[i] != '-') return 0;
        } else if (hv(s[i]) < 0) {
            return 0;
        }
    }
    return 1;
}

int alelt_read_file(const wchar_t *path, alelt_buf *out, size_t max)
{
    HANDLE h;
    LARGE_INTEGER sz;
    DWORD got;
    size_t total = 0;
    h = CreateFileW(path, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (h == INVALID_HANDLE_VALUE) return -1;
    if (!GetFileSizeEx(h, &sz) || sz.QuadPart < 0 || (uint64_t)sz.QuadPart > (uint64_t)max) {
        CloseHandle(h);
        return -1;
    }
    if (alelt_buf_reserve(out, (size_t)sz.QuadPart) != 0) {
        CloseHandle(h);
        return -1;
    }
    while (total < (size_t)sz.QuadPart) {
        size_t want = (size_t)sz.QuadPart - total;
        if (want > 0x40000000u) want = 0x40000000u;
        if (!ReadFile(h, out->data + out->len, (DWORD)want, &got, NULL)) {
            CloseHandle(h);
            return -1;
        }
        if (got == 0) break;
        out->len += got;
        total += got;
    }
    out->data[out->len] = 0;
    CloseHandle(h);
    return total == (size_t)sz.QuadPart ? 0 : -1;
}

int alelt_write_file_atomic(const wchar_t *path, const void *data, size_t n)
{
    wchar_t tmp[ALELT_PATH_MAX];
    HANDLE h;
    DWORD wr;
    size_t off = 0;
    if (_snwprintf_s(tmp, ALELT_PATH_MAX, _TRUNCATE, L"%ls.tmp", path) < 0) return -1;
    h = CreateFileW(tmp, GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    if (h == INVALID_HANDLE_VALUE) return -1;
    while (off < n) {
        size_t want = n - off;
        if (want > 0x40000000u) want = 0x40000000u;
        if (!WriteFile(h, (const uint8_t *)data + off, (DWORD)want, &wr, NULL) || wr == 0) {
            CloseHandle(h);
            DeleteFileW(tmp);
            return -1;
        }
        off += wr;
    }
    if (!FlushFileBuffers(h)) {
        CloseHandle(h);
        DeleteFileW(tmp);
        return -1;
    }
    CloseHandle(h);
    if (!MoveFileExW(tmp, path, MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        DeleteFileW(tmp);
        return -1;
    }
    return 0;
}

int alelt_exe_dir(wchar_t *out, size_t cap)
{
    DWORD n;
    wchar_t *slash;
    if (cap > 0x7FFFFFFF) return -1;
    n = GetModuleFileNameW(NULL, out, (DWORD)cap);
    if (n == 0 || n >= cap) return -1;
    slash = wcsrchr(out, L'\\');
    if (!slash) return -1;
    *slash = 0;
    return 0;
}

static int is_sep(wchar_t c)
{
    return c == L'\\' || c == L'/';
}

/* Length of the root that is never created (AHS-19): "C:\", "\\server\share\",
 * "\\?\C:\", "\\?\UNC\server\share\". 0 = relative path. */
size_t alelt_path_root_len(const wchar_t *p)
{
    size_t i, seps;
    if (wcsncmp(p, L"\\\\?\\UNC\\", 8) == 0) {
        i = 8;
        seps = 2;
    } else if (wcsncmp(p, L"\\\\?\\", 4) == 0 || wcsncmp(p, L"\\\\.\\", 4) == 0) {
        i = 4;
        if (p[i] && p[i + 1] == L':') return is_sep(p[i + 2]) ? i + 3 : i + 2;
        seps = 1;
    } else if (is_sep(p[0]) && is_sep(p[1])) {
        i = 2;
        seps = 2;
    } else if (p[0] && p[1] == L':') {
        return is_sep(p[2]) ? 3 : 2;
    } else {
        return 0;
    }
    while (p[i] && seps > 0) {
        if (is_sep(p[i])) seps--;
        i++;
    }
    return i;
}

int alelt_mkdirs(const wchar_t *path)
{
    wchar_t tmp[ALELT_PATH_MAX];
    size_t i, n;
    if (alelt_wcscpy(tmp, ALELT_PATH_MAX, path) != 0) return -1;
    n = wcslen(tmp);
    for (i = alelt_path_root_len(tmp); i <= n; i++) { /* the root itself is never created */
        if (i == 0) continue;
        if (tmp[i] == L'\\' || tmp[i] == L'/' || tmp[i] == 0) {
            wchar_t c = tmp[i];
            tmp[i] = 0;
            if (!CreateDirectoryW(tmp, NULL) && GetLastError() != ERROR_ALREADY_EXISTS) {
                DWORD a = GetFileAttributesW(tmp);
                if (a == INVALID_FILE_ATTRIBUTES || !(a & FILE_ATTRIBUTE_DIRECTORY)) return -1;
            }
            tmp[i] = c;
        }
    }
    return 0;
}

int alelt_path_join(wchar_t *out, size_t cap, const wchar_t *a, const wchar_t *b)
{
    size_t la = wcslen(a);
    int r;
    if (la > 0 && (a[la - 1] == L'\\' || a[la - 1] == L'/')) {
        r = _snwprintf_s(out, cap, _TRUNCATE, L"%ls%ls", a, b);
    } else {
        r = _snwprintf_s(out, cap, _TRUNCATE, L"%ls\\%ls", a, b);
    }
    return r < 0 ? -1 : 0;
}

void alelt_low_io_priority(void *handle)
{
    FILE_IO_PRIORITY_HINT_INFO hint;
    hint.PriorityHint = IoPriorityHintLow;
    (void)SetFileInformationByHandle((HANDLE)handle, FileIoPriorityHintInfo, &hint, sizeof(hint));
}

int alelt_path_env_prepend(const wchar_t *dir)
{
    DWORD need, n;
    size_t cap;
    wchar_t *cur, *nw;
    int rc = -1;
    if (!dir || !*dir) return -1;
    need = GetEnvironmentVariableW(L"PATH", NULL, 0); /* includes the terminator; 0 = unset */
    cur = (wchar_t *)calloc((size_t)need + 1, sizeof(wchar_t));
    cap = (size_t)need + wcslen(dir) + 2;
    nw = (wchar_t *)calloc(cap, sizeof(wchar_t));
    if (!cur || !nw) goto out;
    n = need ? GetEnvironmentVariableW(L"PATH", cur, need) : 0;
    if (need && (n == 0 || n >= need)) goto out; /* unreadable or grew in between: leave it alone */
    cur[n] = 0;
    if (wcsstr(cur, dir) != NULL) {
        rc = 0;
        goto out;
    }
    if (_snwprintf_s(nw, cap, _TRUNCATE, n ? L"%ls;%ls" : L"%ls%ls", dir, cur) < 0) goto out;
    rc = SetEnvironmentVariableW(L"PATH", nw) ? 1 : -1;
out:
    free(cur);
    free(nw);
    return rc;
}
