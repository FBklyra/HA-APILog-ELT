/* SPDX-License-Identifier: Apache-2.0
 * Agent <-> hub protocol v1 (docs/ARCHITECTURE.md, agent protocol). Wire JSON shapes mirror
 * hub/alelt_common/models.py field-for-field (extra="forbid" on the hub).
 *
 *   POST /agent/v1/register   body RegisterRequest        -> RegisterResponse
 *   POST /agent/v1/heartbeat  body Heartbeat              -> 2xx
 *   GET  /agent/v1/commands?agent_id=..&wait=N            -> [Command] | {"commands":[..]} | 204
 *   POST /agent/v1/commands/{id}/ack?agent_id=..  body {ok, result}  (every command result)
 *   POST /agent/v1/blocks     X-ALELT-Batch: BatchHeader, body codec(raw) -> BatchAck
 *   POST /agent/v1/catalog    body CatalogSnapshot        -> 2xx
 *   POST /agent/v1/snapshot   X-ALELT-Snapshot: SnapshotChunkHeader, body codec(jsonl) -> 2xx
 *   POST /agent/v1/events     body AgentEvent {agent_id,type,command_id,detail,at} -> 2xx (best effort)
 */
#ifndef ALELT_HUB_H
#define ALELT_HUB_H

#include "config.h"
#include "alelt_buf.h"
#include "ha.h"
#include "http.h"

struct fj_val;

typedef struct alelt_register_result {
    int accepted;
    char credential[256];           /* REQ-26: per-agent credential (alelta_), returned once; "" = none */
    char credential_expires_at[40]; /* ISO-8601 UTC */
    char resume_from[ALELT_POS_MAX];
    int has_limits;
    alelt_limits limits; /* -1 = not supplied */
} alelt_register_result;

typedef struct alelt_ack {
    char batch_id[37];
    char durable_pos[ALELT_POS_MAX];
    int duplicate;
    int retry_after_s; /* hub Retry-After on refusal (503), -1 = none */
    char expected_from[ALELT_POS_MAX]; /* 409 gap: the hub's next expected from_pos ("" = not given), AHS-05 */
} alelt_ack;

/* return: 0 ok, -1 transport/5xx (retry), -2 rejected by hub (4xx) */
int alelt_hub_register(alelt_http *h, const alelt_config *cfg, const char *hostname, alelt_register_result *out, char *err,
                    size_t errcap);
int alelt_hub_post_json(alelt_http *h, const char *path, const char *json, size_t len, int timeout_ms, alelt_buf *resp_body,
                     char *err, size_t errcap);
/* reposition: START_CAPTURE command id for X-ALELT-Reposition (NULL = none). epoch >= 0 adds
 * X-ALELT-Lease-Epoch (X-47). -3 = 409 position gap, -4 = 423 lease fenced (not the holder / stale epoch). */
int alelt_hub_post_batch(alelt_http *h, const char *header_json, const char *reposition, long long epoch, const uint8_t *body,
                      size_t len, int timeout_ms, alelt_ack *ack, char *err, size_t errcap);
/* POST /agent/v1/lease/{op}, op = "acquire" | "renew" | "release" (X-47).
 * Body {agent_id, connection_id, ttl_s, lease_id?, epoch?}; reply LeaseGrant parsed into out
 * (release: reply ignored). 0 ok, -1 transport/5xx, -2 rejected (4xx), -3 409 lease_lost. */
int alelt_hub_lease(alelt_http *h, const char *op, const alelt_config *cfg, int ttl_s, const char *lease_id, long long epoch,
                 alelt_lease_grant *out, char *err, size_t errcap);
/* LeaseGrant JSON -> out (pure, unit-tested). 0 ok, -1 malformed. */
int alelt_lease_parse_reply(const char *body, size_t len, alelt_lease_grant *out, char *err, size_t errcap);
/* POST /agent/v1/commands/{id}/ack?agent_id=..  body {"ok":..,"result":<result_json>|null} */
int alelt_hub_ack_command(alelt_http *h, const char *agent_id, const char *command_id, int ok, const char *result_json,
                       char *err, size_t errcap);
int alelt_hub_post_snapshot(alelt_http *h, const char *header_json, const uint8_t *body, size_t len, int timeout_ms,
                         char *err, size_t errcap);
/* POST /agent/v1/credential/rotate (REQ-26): new credential + expiry. 0 ok, -1 retry, -2 rejected. */
int alelt_hub_rotate_credential(alelt_http *h, char *cred, size_t credcap, char *expires, size_t expcap, char *err,
                             size_t errcap);
/* Calls on_cmd for every element of the command list. */
typedef void (*alelt_on_command)(void *ctx, const struct fj_val *cmd);
int alelt_hub_poll_commands(alelt_http *h, const char *agent_id, int wait_s, alelt_on_command cb, void *ctx, char *err,
                         size_t errcap);

#endif
