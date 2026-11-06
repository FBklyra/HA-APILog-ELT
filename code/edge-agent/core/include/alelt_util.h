/* ALELT edge agent - small OS helpers (Windows).
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef ALELT_UTIL_H
#define ALELT_UTIL_H

#include <stddef.h>
#include <stdint.h>
#include <wchar.h>

#include "alelt_buf.h"

#ifdef __cplusplus
extern "C" {
#endif

#define ALELT_UUID_LEN 36
#define ALELT_TS_LEN 32
#define ALELT_PATH_MAX 1024

/* ---- strings (always NUL-terminate, truncation reported as -1) ---- */
int alelt_strcpy(char *dst, size_t cap, const char *src);
int alelt_wcscpy(wchar_t *dst, size_t cap, const wchar_t *src);
int alelt_stricmp_ascii(const char *a, const char *b);

/* UTF-8 <-> UTF-16. Return 0 on success. */
int alelt_to_wide(const char *s, wchar_t *out, size_t cap);
int alelt_to_utf8(const wchar_t *s, char *out, size_t cap);
/* Heap variant; caller frees with free(). */
wchar_t *alelt_to_wide_alloc(const char *s);

/* ---- hex ---- */
void alelt_hex(const uint8_t *p, size_t n, char *out /* 2n+1 */, int upper);
int alelt_unhex(const char *s, uint8_t *out, size_t cap, size_t *outlen);

/* ---- time ---- */
/* UTC ISO-8601 with milliseconds and 'Z', e.g. 2026-11-03T12:00:00.123Z */
void alelt_now_iso(char out[ALELT_TS_LEN]);
uint64_t alelt_mono_ms(void);

/* ---- randomness / ids (BCryptGenRandom) ---- */
int alelt_random(void *p, size_t n);
int alelt_uuid4(char out[ALELT_UUID_LEN + 1]);
int alelt_is_uuid(const char *s);

/* ---- files ---- */
int alelt_read_file(const wchar_t *path, alelt_buf *out, size_t max);
/* Write tmp, flush to disk, atomically replace target (write-through). */
int alelt_write_file_atomic(const wchar_t *path, const void *data, size_t n);
int alelt_exe_dir(wchar_t *out, size_t cap);
int alelt_mkdirs(const wchar_t *path);
size_t alelt_path_root_len(const wchar_t *p);
/* Prepend dir to the process PATH unless already present (buffer sized from the real length, AHS-13).
 * 1 = prepended, 0 = already there, -1 = failed (PATH left untouched). */
int alelt_path_env_prepend(const wchar_t *dir);
int alelt_path_join(wchar_t *out, size_t cap, const wchar_t *a, const wchar_t *b);
/* Low I/O priority hint on a file handle (best effort). */
void alelt_low_io_priority(void *handle);

#ifdef __cplusplus
}
#endif
#endif
