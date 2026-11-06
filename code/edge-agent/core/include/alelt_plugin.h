/* ALELT edge agent - source plugin ABI (DESIGN.md section 5).
 * SPDX-License-Identifier: Apache-2.0
 *
 * A source plugin is a DLL placed next to aleltagent.exe that exports
 *     const alelt_source_vtbl *alelt_source_get_vtbl(void);
 *
 * Threading: the agent calls open/probe_position/read/catalog_json/
 * snapshot_chunk/probe/close from ONE worker thread only. compare_pos and
 * pos_valid must be pure and thread-safe (the sender thread uses them).
 *
 * Memory: plugin and agent may use different CRT heaps. Output buffers owned
 * by the agent are only ever written through host->buf_append(). The raw log
 * buffer passed to read() is agent-owned with capacity max_bytes.
 *
 * Safety contract (R1/R5): a plugin executes only compiled-in, read-only
 * statements. The agent never passes SQL text to a plugin.
 *
 * ABI history: 1 = initial; 2 = verify_table (VERIFY_TABLE, F-03) appended at the
 * end of the vtbl. The agent loads plugins of ABI 1..ALELT_PLUGIN_ABI_VERSION and
 * reads verify_table only from ABI >= 2 plugins. host->abi_version is the
 * agent's ABI: plugins accept any host >= their own.
 */
#ifndef ALELT_PLUGIN_H
#define ALELT_PLUGIN_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ALELT_PLUGIN_ABI_VERSION 2u
#define ALELT_PLUGIN_ABI_MIN 1u
#define ALELT_POS_MAX 64       /* position text incl. NUL */
#define ALELT_IDENT_MAX 129    /* identifier incl. NUL     */
#define ALELT_KEYVAL_MAX 512   /* one key value as text incl. NUL */
#define ALELT_MAX_KEYCOLS 16

/* return codes */
#define ALELT_OK 0
#define ALELT_ERR (-1)          /* permanent / logic error                         */
#define ALELT_ERR_RETRY (-2)    /* transient (connection lost, database busy)      */
#define ALELT_ERR_DENIED (-3)   /* refused by safety policy                        */
#define ALELT_ERR_NOTFOUND (-4) /* object or position not found                    */
#define ALELT_ERR_NOSPACE (-5)  /* buffer too small even for one record            */

#define ALELT_LOG_DEBUG 0
#define ALELT_LOG_INFO 1
#define ALELT_LOG_WARN 2
#define ALELT_LOG_ERROR 3

typedef struct alelt_host_buf alelt_host_buf; /* opaque agent buffer */

typedef struct alelt_host_api {
    uint32_t abi_version;
    void (*log)(int level, const char *component, const char *msg);
    /* Append bytes to an agent-owned buffer. 0 = ok, -1 = full / OOM. */
    int (*buf_append)(alelt_host_buf *buf, const void *data, size_t n);
    /* Non-zero when the agent is stopping: long operations should return. */
    int (*should_stop)(void);
} alelt_host_api;

typedef struct alelt_read_result {
    size_t bytes;                 /* bytes written to buf                        */
    uint32_t records;             /* records in buf                              */
    int at_end;                   /* reader reached the current end of the log   */
    char first_pos[ALELT_POS_MAX];   /* position of the first record ("" if none)   */
    char last_pos[ALELT_POS_MAX];    /* position of the last record ("" if none)    */
    char next_pos[ALELT_POS_MAX];    /* where the next read must start              */
    uint32_t flags;               /* ABI >= 2: ALELT_READ_* (agent zeroes it)       */
} alelt_read_result;

/* alelt_read_result.flags */
#define ALELT_READ_LOG_REUSED 0x1u   /* engine reports reused log positions (restore / rollforward) */

typedef struct alelt_snapshot_req {
    const char *schema;
    const char *table;
    /* exclusive lower bound on the primary key (NULL/0 = from the start) */
    const char *const *key_after;
    size_t nkey_after;
    /* inclusive upper bound on the primary key (NULL/0 = to the end) */
    const char *const *key_to;
    size_t nkey_to;
    uint32_t chunk_rows;
} alelt_snapshot_req;

typedef struct alelt_snapshot_res {
    uint32_t rows;
    int last;                                    /* no more rows after this chunk */
    size_t nlast_key;
    char last_key[ALELT_MAX_KEYCOLS][ALELT_KEYVAL_MAX]; /* key of the last row emitted  */
} alelt_snapshot_res;

/* VERIFY_TABLE (F-03). args_json = the validated, canonically re-serialized
 * command args (alelt_common.models.VerifyTableArgs: table, verify_id, checks,
 * columns, ranges, key_values, max_keys, timeout_s); never SQL. The plugin
 * re-validates, answers only with compiled-in parameterized SELECTs and
 * appends one VerifyTableResult JSON object to out. Caps are the agent's. */
typedef struct alelt_verify_req {
    const char *schema;
    const char *table;
    const char *args_json;
    size_t args_len;
    uint32_t max_keys;       /* keys per range ("keys")                     */
    uint32_t max_key_values; /* rows re-read by key ("rows")                */
    size_t max_bytes;        /* result size cap; over it: truncated = true  */
    uint32_t timeout_s;      /* per statement                               */
} alelt_verify_req;

typedef struct alelt_source_vtbl {
    uint32_t abi_version;     /* ALELT_PLUGIN_ABI_VERSION */
    const char *engine;       /* e.g. "db2"            */
    const char *version;      /* plugin version        */

    /* settings_json: the "source.settings" object from aleltagent.json. */
    int (*open)(const alelt_host_api *host, const char *settings_json, void **ctx, char *err, size_t errcap);

    /* Current end-of-log position (cheap, no log data read). */
    int (*probe_position)(void *ctx, char *pos, size_t cap);

    /* Read raw records from from_pos into buf (capacity max_bytes).
     * Body format is engine native and opaque to the agent. */
    int (*read)(void *ctx, const char *from_pos, uint8_t *buf, size_t max_bytes, alelt_read_result *res);

    /* Catalog as a JSON object {"database_codepage":..,"position":..,"tables":[..]}
     * (CatalogSnapshot without connection_id/engine/taken_at). */
    int (*catalog_json)(void *ctx, const char *const *schemas, size_t nschemas, alelt_host_buf *out);

    /* One snapshot chunk as JSON lines (one object per row) appended to out. */
    int (*snapshot_chunk)(void *ctx, const alelt_snapshot_req *req, alelt_host_buf *out, alelt_snapshot_res *res);

    /* Whitelisted named probe -> JSON object appended to out. */
    int (*probe)(void *ctx, const char *name, alelt_host_buf *out);

    /* Pure helpers. compare: *result <0, 0, >0. */
    int (*compare_pos)(const char *a, const char *b, int *result);
    int (*pos_valid)(const char *pos);

    /* Last error text for logs (never contains credentials). */
    const char *(*last_error)(void *ctx);

    void (*close)(void *ctx);

    /* ABI >= 2 (NULL = not supported). */
    int (*verify_table)(void *ctx, const alelt_verify_req *req, alelt_host_buf *out);
} alelt_source_vtbl;

typedef const alelt_source_vtbl *(*alelt_source_get_vtbl_fn)(void);
#define ALELT_SOURCE_ENTRY "alelt_source_get_vtbl"

#ifdef __cplusplus
}
#endif
#endif
