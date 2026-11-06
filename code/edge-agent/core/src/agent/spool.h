/* SPDX-License-Identifier: Apache-2.0
 * Durable local spool of batches awaiting hub acknowledgement.
 *
 * File <seq:016llx>.aleltb = "ALB1" | u32 hdr_len | hdr JSON (BatchHeader) |
 *                         u64 body_len | body (codec applied) | "ALBE"
 * Written to .tmp, flushed, renamed write-through. Deleted only once the hub
 * acknowledged durable_pos >= batch to_pos. Never dropped otherwise.
 */
#ifndef ALELT_SPOOL_H
#define ALELT_SPOOL_H

#include <stdint.h>
#include <wchar.h>

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

#include "alelt_buf.h"
#include "alelt_plugin.h"
#include "alelt_util.h"

typedef int (*alelt_pos_cmp_fn)(const char *a, const char *b, int *result);

typedef struct alelt_spool_entry {
    uint64_t seq;
    uint64_t bytes;
    char batch_id[ALELT_UUID_LEN + 1];
    char from_pos[ALELT_POS_MAX];
    char to_pos[ALELT_POS_MAX];
    char next_pos[ALELT_POS_MAX];
} alelt_spool_entry;

typedef struct alelt_spool {
    CRITICAL_SECTION lock;
    wchar_t dir[ALELT_PATH_MAX];
    uint64_t max_bytes;
    uint64_t total_bytes;
    uint64_t next_seq;
    alelt_spool_entry *e;
    size_t n, cap;
    /* persisted state (state.json) */
    char durable_pos[ALELT_POS_MAX];
    char next_pos[ALELT_POS_MAX];   /* resume point: last spooled batch next_pos */
    int capturing;
    /* START_CAPTURE{from_pos} command id presented as X-ALELT-Reposition with the
     * batch whose from_pos equals reposition_pos (hub X-25). */
    char reposition_cmd[ALELT_UUID_LEN + 1];
    char reposition_pos[ALELT_POS_MAX];
} alelt_spool;

int alelt_spool_open(alelt_spool *s, const wchar_t *dir, uint64_t max_bytes, char *err, size_t errcap);
void alelt_spool_close(alelt_spool *s);

/* 1 if need more bytes fit under the cap. */
int alelt_spool_has_room(alelt_spool *s, uint64_t need);

/* Durably add a batch, then persist state (next_pos). */
int alelt_spool_put(alelt_spool *s, const char *batch_id, const char *header_json, const uint8_t *body, size_t body_len,
                 const char *to_pos, const char *next_pos);

/* Load the oldest batch (header JSON + body). 1 = got one, 0 = empty, -1 = error. */
int alelt_spool_oldest(alelt_spool *s, alelt_spool_entry *meta, alelt_buf *header, alelt_buf *body);

/* Apply a hub ACK: delete every batch with to_pos <= durable_pos. Returns number deleted. */
int alelt_spool_ack(alelt_spool *s, const char *durable_pos, alelt_pos_cmp_fn cmp);

/* AHS-05: delete the oldest batch if it is batch_id (the hub already holds positions past it).
 * 1 = deleted, 0 = not the head, -1 = delete failed. */
int alelt_spool_drop_head(alelt_spool *s, const char *batch_id);
int alelt_spool_set_capturing(alelt_spool *s, int capturing, const char *next_pos /* may be NULL */);
int alelt_spool_set_reposition(alelt_spool *s, const char *command_id, const char *pos);
/* Copies the reposition command id if pos matches; "" otherwise. */
void alelt_spool_reposition_for(alelt_spool *s, const char *from_pos, char out[ALELT_UUID_LEN + 1]);
/* HA takeover (X-48): another agent held the capture lease, so every local batch is stale.
 * Deletes all spooled batches, clears the reposition marker, sets durable_pos = next_pos =
 * resume_pos (hub durable position) and the capturing flag. resume_pos "" or NULL: positions
 * cleared and capturing forced to 0 (wait for START_CAPTURE). Returns batches deleted, -1 on error. */
int alelt_spool_takeover(alelt_spool *s, const char *resume_pos, int capturing);
void alelt_spool_snapshot(alelt_spool *s, uint64_t *bytes, size_t *count, char *durable, char *next, int *capturing);

#endif
