/* SPDX-License-Identifier: Apache-2.0 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "alelt_json.h"
#include "log.h"
#include "spool.h"

#define MAGIC_HEAD "ALB1"
#define MAGIC_TAIL "ALBE"
#define MAX_HDR (64u * 1024u)
#define MAX_BODY ((uint64_t)256 * 1024 * 1024)

static int entry_path(const alelt_spool *s, uint64_t seq, const wchar_t *ext, wchar_t *out)
{
    return _snwprintf_s(out, ALELT_PATH_MAX, _TRUNCATE, L"%ls\\%016llx%ls", s->dir, (unsigned long long)seq, ext) < 0 ? -1 : 0;
}

static int save_state_locked(alelt_spool *s)
{
    alelt_buf b;
    alelt_jw w;
    wchar_t p[ALELT_PATH_MAX];
    int rc;
    alelt_buf_init(&b, 4096);
    jw_init(&w, &b);
    jw_obj(&w);
    jw_key(&w, "version"); jw_i64(&w, 1);
    jw_key(&w, "durable_pos"); jw_str_or_null(&w, s->durable_pos);
    jw_key(&w, "next_pos"); jw_str_or_null(&w, s->next_pos);
    jw_key(&w, "capturing"); jw_bool(&w, s->capturing);
    jw_key(&w, "reposition_command_id"); jw_str_or_null(&w, s->reposition_cmd);
    jw_key(&w, "reposition_pos"); jw_str_or_null(&w, s->reposition_pos);
    jw_obj_end(&w);
    if (jw_ok(&w) != 0 || alelt_path_join(p, ALELT_PATH_MAX, s->dir, L"state.json") != 0) {
        alelt_buf_free(&b);
        return -1;
    }
    rc = alelt_write_file_atomic(p, b.data, b.len);
    alelt_buf_free(&b);
    if (rc != 0) alelt_logf(ALELT_LOG_ERROR, "spool", "cannot persist state.json (error %lu)", (unsigned long)GetLastError());
    return rc;
}

static void load_state(alelt_spool *s)
{
    wchar_t p[ALELT_PATH_MAX];
    alelt_buf b;
    fj_doc *d;
    char err[128];
    alelt_buf_init(&b, 64 * 1024);
    if (alelt_path_join(p, ALELT_PATH_MAX, s->dir, L"state.json") == 0 && alelt_read_file(p, &b, 64 * 1024) == 0) {
        d = fj_parse((const char *)b.data, b.len, err, sizeof(err));
        if (d) {
            const char *v;
            int c = 0;
            v = fj_get_str(fj_root(d), "durable_pos");
            if (v) alelt_strcpy(s->durable_pos, ALELT_POS_MAX, v);
            v = fj_get_str(fj_root(d), "next_pos");
            if (v) alelt_strcpy(s->next_pos, ALELT_POS_MAX, v);
            if (fj_get_bool(fj_root(d), "capturing", &c) == 0) s->capturing = c;
            v = fj_get_str(fj_root(d), "reposition_command_id");
            if (v && alelt_is_uuid(v)) alelt_strcpy(s->reposition_cmd, sizeof(s->reposition_cmd), v);
            v = fj_get_str(fj_root(d), "reposition_pos");
            if (v) alelt_strcpy(s->reposition_pos, ALELT_POS_MAX, v);
            fj_free(d);
        } else {
            alelt_logf(ALELT_LOG_WARN, "spool", "state.json unreadable (%s); positions come from batches/hub", err);
        }
    }
    alelt_buf_free(&b);
}

static int push_entry(alelt_spool *s, const alelt_spool_entry *e)
{
    size_t i;
    if (s->n == s->cap) {
        size_t nc = s->cap ? s->cap * 2 : 64;
        alelt_spool_entry *ne = (alelt_spool_entry *)realloc(s->e, nc * sizeof(*ne));
        if (!ne) return -1;
        s->e = ne;
        s->cap = nc;
    }
    /* keep sorted by seq (scan order is not guaranteed) */
    i = s->n;
    while (i > 0 && s->e[i - 1].seq > e->seq) {
        s->e[i] = s->e[i - 1];
        i--;
    }
    s->e[i] = *e;
    s->n++;
    return 0;
}

static int read_exact(HANDLE h, void *dst, size_t n)
{
    uint8_t *p = (uint8_t *)dst;
    while (n > 0) {
        DWORD chunk = n > (1u << 24) ? (1u << 24) : (DWORD)n, rd = 0;
        if (!ReadFile(h, p, chunk, &rd, NULL) || rd == 0) return -1;
        p += rd;
        n -= rd;
    }
    return 0;
}

/* Reads and validates a spool file. header/body may be NULL (metadata only).
 * AHS-10: only the header is read for metadata (open/restart with a large backlog);
 * the body is read straight into the caller's buffer (no whole-file copy). The
 * framing (sizes vs file size, tail magic) is always checked. */
static int read_batch_file(const wchar_t *path, alelt_spool_entry *meta, alelt_buf *header, alelt_buf *body)
{
    HANDLE fh;
    LARGE_INTEGER fsz, off;
    uint8_t head[8], tail[4];
    char *hj = NULL;
    uint32_t hl;
    uint64_t bl;
    fj_doc *d;
    char err[128];
    const fj_val *to, *nx, *fr;
    const char *bid;
    int rc = -1;
    fh = CreateFileW(path, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                     FILE_FLAG_SEQUENTIAL_SCAN, NULL);
    if (fh == INVALID_HANDLE_VALUE) return -1;
    if (!GetFileSizeEx(fh, &fsz) || fsz.QuadPart < 20 || read_exact(fh, head, 8) != 0 || memcmp(head, MAGIC_HEAD, 4) != 0)
        goto out;
    memcpy(&hl, head + 4, 4);
    if (hl == 0 || hl > MAX_HDR || (uint64_t)8 + hl + 8 + 4 > (uint64_t)fsz.QuadPart) goto out;
    hj = (char *)malloc((size_t)hl + 1);
    if (!hj || read_exact(fh, hj, hl) != 0 || read_exact(fh, &bl, 8) != 0) goto out;
    hj[hl] = 0;
    if (bl > MAX_BODY || (uint64_t)8 + hl + 8 + bl + 4 != (uint64_t)fsz.QuadPart) goto out;
    off.QuadPart = fsz.QuadPart - 4;
    if (!SetFilePointerEx(fh, off, NULL, FILE_BEGIN) || read_exact(fh, tail, 4) != 0 || memcmp(tail, MAGIC_TAIL, 4) != 0)
        goto out;
    d = fj_parse(hj, hl, err, sizeof(err));
    if (!d) goto out;
    bid = fj_get_str(fj_root(d), "batch_id");
    to = fj_get(fj_root(d), "to_pos");
    nx = fj_get(fj_root(d), "next_pos");
    fr = fj_get(fj_root(d), "from_pos");
    if (!bid || !to) {
        fj_free(d);
        goto out;
    }
    alelt_strcpy(meta->batch_id, sizeof(meta->batch_id), bid);
    alelt_strcpy(meta->to_pos, ALELT_POS_MAX, fj_get_str(to, "text"));
    alelt_strcpy(meta->from_pos, ALELT_POS_MAX, fr ? fj_get_str(fr, "text") : NULL);
    alelt_strcpy(meta->next_pos, ALELT_POS_MAX, nx ? fj_get_str(nx, "text") : meta->to_pos);
    fj_free(d);
    meta->bytes = (uint64_t)fsz.QuadPart;
    if (header && alelt_buf_append(header, hj, hl) != 0) goto out;
    if (body && bl > 0) {
        off.QuadPart = (LONGLONG)(8 + hl + 8);
        if (!SetFilePointerEx(fh, off, NULL, FILE_BEGIN) || alelt_buf_reserve(body, (size_t)bl) != 0 ||
            read_exact(fh, body->data + body->len, (size_t)bl) != 0)
            goto out;
        body->len += (size_t)bl;
        body->data[body->len] = 0;
    }
    rc = 0;
out:
    free(hj);
    CloseHandle(fh);
    return rc;
}

int alelt_spool_open(alelt_spool *s, const wchar_t *dir, uint64_t max_bytes, char *err, size_t errcap)
{
    wchar_t pat[ALELT_PATH_MAX], p[ALELT_PATH_MAX];
    WIN32_FIND_DATAW fd;
    HANDLE h;
    memset(s, 0, sizeof(*s));
    InitializeCriticalSection(&s->lock);
    s->max_bytes = max_bytes;
    s->next_seq = 1;
    if (alelt_wcscpy(s->dir, ALELT_PATH_MAX, dir) != 0 || alelt_mkdirs(dir) != 0) {
        snprintf(err, errcap, "spool: cannot create %ls (error %lu)", dir, (unsigned long)GetLastError());
        return -1;
    }
    load_state(s);
    /* remove torn temp files */
    if (alelt_path_join(pat, ALELT_PATH_MAX, dir, L"*.tmp") == 0) {
        h = FindFirstFileW(pat, &fd);
        if (h != INVALID_HANDLE_VALUE) {
            do {
                if (alelt_path_join(p, ALELT_PATH_MAX, dir, fd.cFileName) == 0) DeleteFileW(p);
            } while (FindNextFileW(h, &fd));
            FindClose(h);
        }
    }
    if (alelt_path_join(pat, ALELT_PATH_MAX, dir, L"*.aleltb") != 0) return -1;
    h = FindFirstFileW(pat, &fd);
    if (h != INVALID_HANDLE_VALUE) {
        do {
            alelt_spool_entry e;
            wchar_t *end = NULL;
            memset(&e, 0, sizeof(e));
            e.seq = wcstoull(fd.cFileName, &end, 16);
            if (!end || _wcsicmp(end, L".aleltb") != 0 || e.seq == 0) continue;
            if (alelt_path_join(p, ALELT_PATH_MAX, dir, fd.cFileName) != 0) continue;
            if (read_batch_file(p, &e, NULL, NULL) != 0) {
                /* Not deleted: corrupt files are quarantined for inspection (R7). */
                wchar_t q[ALELT_PATH_MAX];
                alelt_logf(ALELT_LOG_ERROR, "spool", "corrupt spool file %ls renamed to .bad", fd.cFileName);
                if (_snwprintf_s(q, ALELT_PATH_MAX, _TRUNCATE, L"%ls.bad", p) >= 0) MoveFileExW(p, q, MOVEFILE_REPLACE_EXISTING);
                continue;
            }
            if (push_entry(s, &e) != 0) {
                FindClose(h);
                snprintf(err, errcap, "spool: out of memory");
                return -1;
            }
            s->total_bytes += e.bytes;
            if (e.seq >= s->next_seq) s->next_seq = e.seq + 1;
        } while (FindNextFileW(h, &fd));
        FindClose(h);
    }
    if (s->n > 0) {
        /* The newest batch on disk is authoritative for the resume point. */
        alelt_strcpy(s->next_pos, ALELT_POS_MAX, s->e[s->n - 1].next_pos);
    }
    alelt_logf(ALELT_LOG_INFO, "spool", "spool opened: %zu batches, %llu bytes, next_pos=%s durable_pos=%s capturing=%d", s->n,
            (unsigned long long)s->total_bytes, s->next_pos[0] ? s->next_pos : "-",
            s->durable_pos[0] ? s->durable_pos : "-", s->capturing);
    return 0;
}

void alelt_spool_close(alelt_spool *s)
{
    free(s->e);
    s->e = NULL;
    s->n = s->cap = 0;
    DeleteCriticalSection(&s->lock);
}

int alelt_spool_has_room(alelt_spool *s, uint64_t need)
{
    int r;
    EnterCriticalSection(&s->lock);
    r = (s->total_bytes + need <= s->max_bytes);
    LeaveCriticalSection(&s->lock);
    return r;
}

int alelt_spool_put(alelt_spool *s, const char *batch_id, const char *header_json, const uint8_t *body, size_t body_len,
                 const char *to_pos, const char *next_pos)
{
    wchar_t tmp[ALELT_PATH_MAX], fin[ALELT_PATH_MAX];
    HANDLE h;
    uint32_t hl = (uint32_t)strlen(header_json);
    uint64_t bl = body_len;
    alelt_spool_entry e;
    DWORD wr;
    int ok;
    const void *parts[6];
    DWORD lens[6];
    int i;

    if (hl == 0 || hl > MAX_HDR || bl > MAX_BODY) return -1;
    EnterCriticalSection(&s->lock);
    memset(&e, 0, sizeof(e));
    e.seq = s->next_seq++;
    if (entry_path(s, e.seq, L".tmp", tmp) != 0 || entry_path(s, e.seq, L".aleltb", fin) != 0) {
        LeaveCriticalSection(&s->lock);
        return -1;
    }
    LeaveCriticalSection(&s->lock);

    h = CreateFileW(tmp, GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    if (h == INVALID_HANDLE_VALUE) return -1;
    alelt_low_io_priority(h);
    parts[0] = MAGIC_HEAD; lens[0] = 4;
    parts[1] = &hl;        lens[1] = 4;
    parts[2] = header_json; lens[2] = hl;
    parts[3] = &bl;        lens[3] = 8;
    parts[4] = body;       lens[4] = (DWORD)bl;
    parts[5] = MAGIC_TAIL; lens[5] = 4;
    ok = 1;
    for (i = 0; i < 6 && ok; i++) {
        if (lens[i] == 0) continue;
        ok = WriteFile(h, parts[i], lens[i], &wr, NULL) && wr == lens[i];
    }
    ok = ok && FlushFileBuffers(h);
    CloseHandle(h);
    if (!ok || !MoveFileExW(tmp, fin, MOVEFILE_WRITE_THROUGH)) {
        alelt_logf(ALELT_LOG_ERROR, "spool", "spool write failed (error %lu)", (unsigned long)GetLastError());
        DeleteFileW(tmp);
        return -1;
    }
    alelt_strcpy(e.batch_id, sizeof(e.batch_id), batch_id);
    alelt_strcpy(e.to_pos, ALELT_POS_MAX, to_pos);
    alelt_strcpy(e.next_pos, ALELT_POS_MAX, next_pos);
    e.bytes = (uint64_t)4 + 4 + hl + 8 + bl + 4;
    EnterCriticalSection(&s->lock);
    if (push_entry(s, &e) != 0) {
        LeaveCriticalSection(&s->lock);
        return -1; /* file is durable; it will be picked up on restart */
    }
    s->total_bytes += e.bytes;
    alelt_strcpy(s->next_pos, ALELT_POS_MAX, next_pos);
    (void)save_state_locked(s);
    LeaveCriticalSection(&s->lock);
    return 0;
}

int alelt_spool_oldest(alelt_spool *s, alelt_spool_entry *meta, alelt_buf *header, alelt_buf *body)
{
    wchar_t p[ALELT_PATH_MAX];
    uint64_t seq;
    EnterCriticalSection(&s->lock);
    if (s->n == 0) {
        LeaveCriticalSection(&s->lock);
        return 0;
    }
    seq = s->e[0].seq;
    LeaveCriticalSection(&s->lock);
    if (entry_path(s, seq, L".aleltb", p) != 0) return -1;
    memset(meta, 0, sizeof(*meta));
    if (read_batch_file(p, meta, header, body) != 0) return -1;
    meta->seq = seq;
    return 1;
}

int alelt_spool_ack(alelt_spool *s, const char *durable_pos, alelt_pos_cmp_fn cmp)
{
    int deleted = 0;
    EnterCriticalSection(&s->lock);
    while (s->n > 0) {
        int c = 1;
        wchar_t p[ALELT_PATH_MAX];
        if (cmp(s->e[0].to_pos, durable_pos, &c) != 0 || c > 0) break;
        if (entry_path(s, s->e[0].seq, L".aleltb", p) != 0) break;
        if (!DeleteFileW(p) && GetLastError() != ERROR_FILE_NOT_FOUND) {
            alelt_logf(ALELT_LOG_ERROR, "spool", "cannot delete acknowledged batch %s (error %lu)", s->e[0].batch_id,
                    (unsigned long)GetLastError());
            break;
        }
        s->total_bytes -= (s->e[0].bytes <= s->total_bytes) ? s->e[0].bytes : s->total_bytes;
        memmove(&s->e[0], &s->e[1], (s->n - 1) * sizeof(s->e[0]));
        s->n--;
        deleted++;
    }
    {
        int c = 1;
        if (!s->durable_pos[0] || (cmp(durable_pos, s->durable_pos, &c) == 0 && c > 0)) {
            alelt_strcpy(s->durable_pos, ALELT_POS_MAX, durable_pos);
            (void)save_state_locked(s);
        }
    }
    LeaveCriticalSection(&s->lock);
    return deleted;
}

int alelt_spool_drop_head(alelt_spool *s, const char *batch_id)
{
    wchar_t p[ALELT_PATH_MAX];
    int rc = 0;
    EnterCriticalSection(&s->lock);
    if (s->n > 0 && strcmp(s->e[0].batch_id, batch_id) == 0 && entry_path(s, s->e[0].seq, L".aleltb", p) == 0) {
        if (DeleteFileW(p) || GetLastError() == ERROR_FILE_NOT_FOUND) {
            s->total_bytes -= (s->e[0].bytes <= s->total_bytes) ? s->e[0].bytes : s->total_bytes;
            memmove(&s->e[0], &s->e[1], (s->n - 1) * sizeof(s->e[0]));
            s->n--;
            rc = 1;
        } else {
            rc = -1;
        }
    }
    LeaveCriticalSection(&s->lock);
    return rc;
}

int alelt_spool_set_capturing(alelt_spool *s, int capturing, const char *next_pos)
{
    int rc;
    EnterCriticalSection(&s->lock);
    s->capturing = capturing;
    if (next_pos) alelt_strcpy(s->next_pos, ALELT_POS_MAX, next_pos);
    rc = save_state_locked(s);
    LeaveCriticalSection(&s->lock);
    return rc;
}

int alelt_spool_set_reposition(alelt_spool *s, const char *command_id, const char *pos)
{
    int rc;
    EnterCriticalSection(&s->lock);
    alelt_strcpy(s->reposition_cmd, sizeof(s->reposition_cmd), command_id);
    alelt_strcpy(s->reposition_pos, ALELT_POS_MAX, pos);
    rc = save_state_locked(s);
    LeaveCriticalSection(&s->lock);
    return rc;
}

void alelt_spool_reposition_for(alelt_spool *s, const char *from_pos, char out[ALELT_UUID_LEN + 1])
{
    out[0] = 0;
    EnterCriticalSection(&s->lock);
    if (s->reposition_cmd[0] && strcmp(s->reposition_pos, from_pos) == 0) alelt_strcpy(out, ALELT_UUID_LEN + 1, s->reposition_cmd);
    LeaveCriticalSection(&s->lock);
}

int alelt_spool_takeover(alelt_spool *s, const char *resume_pos, int capturing)
{
    int deleted = 0, failed = 0;
    EnterCriticalSection(&s->lock);
    while (s->n > 0) {
        wchar_t p[ALELT_PATH_MAX];
        if (entry_path(s, s->e[s->n - 1].seq, L".aleltb", p) != 0 ||
            (!DeleteFileW(p) && GetLastError() != ERROR_FILE_NOT_FOUND)) {
            alelt_logf(ALELT_LOG_ERROR, "spool", "takeover: cannot delete stale batch %s (error %lu)", s->e[s->n - 1].batch_id,
                    (unsigned long)GetLastError());
            failed = 1;
            break;
        }
        s->n--;
        deleted++;
    }
    if (!failed) {
        s->total_bytes = 0;
        s->reposition_cmd[0] = 0;
        s->reposition_pos[0] = 0;
        if (resume_pos && *resume_pos) {
            alelt_strcpy(s->durable_pos, ALELT_POS_MAX, resume_pos);
            alelt_strcpy(s->next_pos, ALELT_POS_MAX, resume_pos);
            s->capturing = capturing;
        } else {
            s->durable_pos[0] = 0;
            s->next_pos[0] = 0;
            s->capturing = 0;
        }
        if (save_state_locked(s) != 0) failed = 1;
    }
    LeaveCriticalSection(&s->lock);
    return failed ? -1 : deleted;
}

void alelt_spool_snapshot(alelt_spool *s, uint64_t *bytes, size_t *count, char *durable, char *next, int *capturing)
{
    EnterCriticalSection(&s->lock);
    if (bytes) *bytes = s->total_bytes;
    if (count) *count = s->n;
    if (durable) alelt_strcpy(durable, ALELT_POS_MAX, s->durable_pos);
    if (next) alelt_strcpy(next, ALELT_POS_MAX, s->next_pos);
    if (capturing) *capturing = s->capturing;
    LeaveCriticalSection(&s->lock);
}
