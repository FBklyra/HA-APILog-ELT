/* SPDX-License-Identifier: Apache-2.0
 * Active/passive capture lease (X-47/X-48): pure decision logic, no I/O.
 *
 * The hub grants one capture lease per connection (holder agent_id, lease_id, epoch,
 * ttl_s). The holder renews every renew_s; it stops reading on its own once
 * ttl_s - renew_s passed without a successful renew (self-fencing), so it never reads
 * after the hub could have granted the lease to the standby. The hub additionally
 * rejects batches whose X-ALELT-Lease-Epoch is not the current one (423, fencing).
 */
#ifndef ALELT_HA_H
#define ALELT_HA_H

#include <stdint.h>

#include "alelt_plugin.h"
#include "alelt_util.h"

typedef struct alelt_lease_grant {
    int granted;
    int takeover;                 /* holder changed: local spool is stale */
    char holder[128];
    char lease_id[37];
    long long epoch;              /* -1 = absent */
    int ttl_s;                    /* 0 = absent */
    char resume_from[ALELT_POS_MAX]; /* hub durable position ("" = none) */
    double retry_after_s;         /* 0 = absent */
    int capturing;                /* AHS-24: 1 = capture runs, 0 = stopped by the operator (stay idle until
                                     START_CAPTURE/RESUME), -1 = absent (older hub: previous behaviour) */
} alelt_lease_grant;

typedef enum alelt_ha_event {
    ALELT_HA_NONE = 0, /* nothing changed */
    ALELT_HA_ACTIVE,   /* became active, keep the spool (same holder) */
    ALELT_HA_TAKEOVER, /* became active after another holder: purge the spool */
    ALELT_HA_RENEWED,  /* still active */
    ALELT_HA_LOST      /* stopped being active */
} alelt_ha_event;

typedef struct alelt_ha {
    int enabled; /* 0 = standalone: always active, no epoch header */
    int ttl_s, renew_s;
    int active;
    char lease_id[37];
    long long epoch;
    uint64_t last_ok_ms; /* monotonic time of the last successful acquire/renew */
    uint64_t next_ms;    /* next acquire/renew attempt */
} alelt_ha;

void alelt_ha_init(alelt_ha *h, int enabled, int ttl_s, int renew_s);
/* 1 if an acquire (standby) or renew (active) call is due. */
int alelt_ha_due(const alelt_ha *h, uint64_t now_ms);
/* Effective lease ttl in seconds (min of configured and granted). */
int alelt_ha_ttl(const alelt_ha *h);
/* Active and the self-fence deadline (last_ok + ttl - renew) has passed. */
int alelt_ha_must_fence(const alelt_ha *h, uint64_t now_ms);
/* Milliseconds until the next decision point (acquire/renew due or fence deadline). */
uint64_t alelt_ha_wait_ms(const alelt_ha *h, uint64_t now_ms);
/* Apply a parsed acquire/renew reply received at now_ms (request sent at the same instant). */
alelt_ha_event alelt_ha_on_grant(alelt_ha *h, const alelt_lease_grant *g, uint64_t now_ms);
/* AHS-07: same, but the fence clock starts at sent_ms (taken BEFORE the request was sent: the
 * hub's ttl starts no earlier than that). A grant that arrives after its own fence deadline
 * (sent_ms + ttl - renew) is not used: the agent stays/becomes inactive and retries. */
alelt_ha_event alelt_ha_on_grant_at(alelt_ha *h, const alelt_lease_grant *g, uint64_t sent_ms, uint64_t now_ms);
/* Transport failure: retry soon; ALELT_HA_LOST once the fence deadline passed. */
alelt_ha_event alelt_ha_on_error(alelt_ha *h, uint64_t now_ms);
/* Lease lost (renew 409, batch 423). ALELT_HA_LOST if it was active. */
alelt_ha_event alelt_ha_on_lost(alelt_ha *h, uint64_t now_ms);
/* Epoch for X-ALELT-Lease-Epoch: -1 = no header (standalone or not active). */
long long alelt_ha_epoch(const alelt_ha *h);

/* AHS-24: a takeover grant whose connection the operator paused: the new holder starts paused. */
int alelt_ha_grant_paused(const alelt_lease_grant *g);

#endif
