/* SPDX-License-Identifier: Apache-2.0 */
#include <string.h>

#include "ha.h"

void alelt_ha_init(alelt_ha *h, int enabled, int ttl_s, int renew_s)
{
    memset(h, 0, sizeof(*h));
    h->enabled = enabled;
    h->ttl_s = ttl_s > 0 ? ttl_s : 30;
    h->renew_s = (renew_s > 0 && renew_s <= h->ttl_s / 2) ? renew_s : (h->ttl_s / 3 > 0 ? h->ttl_s / 3 : 1);
    h->active = enabled ? 0 : 1;
    h->epoch = -1;
}

int alelt_ha_due(const alelt_ha *h, uint64_t now_ms)
{
    return h->enabled && now_ms >= h->next_ms;
}

int alelt_ha_ttl(const alelt_ha *h)
{
    return h->ttl_s;
}

static uint64_t fence_at(const alelt_ha *h)
{
    int margin = h->ttl_s - h->renew_s;
    if (margin < 1) margin = 1;
    return h->last_ok_ms + (uint64_t)margin * 1000u;
}

int alelt_ha_must_fence(const alelt_ha *h, uint64_t now_ms)
{
    return h->enabled && h->active && now_ms >= fence_at(h);
}

uint64_t alelt_ha_wait_ms(const alelt_ha *h, uint64_t now_ms)
{
    uint64_t t;
    if (!h->enabled) return 60000;
    t = h->next_ms;
    if (h->active && fence_at(h) < t) t = fence_at(h);
    return t > now_ms ? t - now_ms : 0;
}

alelt_ha_event alelt_ha_on_grant(alelt_ha *h, const alelt_lease_grant *g, uint64_t now_ms)
{
    return alelt_ha_on_grant_at(h, g, now_ms, now_ms);
}

alelt_ha_event alelt_ha_on_grant_at(alelt_ha *h, const alelt_lease_grant *g, uint64_t sent_ms, uint64_t now_ms)
{
    int was_active, changed, margin;
    if (!h->enabled) return ALELT_HA_NONE;
    if (sent_ms > now_ms) sent_ms = now_ms;
    if (!g->granted) {
        uint64_t retry = (uint64_t)h->renew_s * 1000u;
        if (g->retry_after_s > 0 && g->retry_after_s < (double)h->renew_s) retry = (uint64_t)(g->retry_after_s * 1000.0);
        if (retry < 250) retry = 250;
        h->next_ms = now_ms + retry;
        if (h->active) {
            h->active = 0;
            return ALELT_HA_LOST;
        }
        return ALELT_HA_NONE;
    }
    was_active = h->active;
    changed = (g->epoch != h->epoch) || strcmp(g->lease_id, h->lease_id) != 0;
    alelt_strcpy(h->lease_id, sizeof(h->lease_id), g->lease_id);
    h->epoch = g->epoch;
    if (g->ttl_s > 0 && g->ttl_s < h->ttl_s) {
        h->ttl_s = g->ttl_s;
        if (h->renew_s > h->ttl_s / 2) h->renew_s = h->ttl_s / 2 > 0 ? h->ttl_s / 2 : 1;
    }
    margin = h->ttl_s - h->renew_s;
    if (margin < 1) margin = 1;
    if (now_ms >= sent_ms + (uint64_t)margin * 1000u) {
        /* reply slower than the fencing margin: the hub may already consider the lease expired */
        h->next_ms = now_ms + 250u;
        if (h->active) {
            h->active = 0;
            return ALELT_HA_LOST;
        }
        return ALELT_HA_NONE;
    }
    h->last_ok_ms = sent_ms;
    h->next_ms = sent_ms + (uint64_t)h->renew_s * 1000u;
    if (h->next_ms < now_ms) h->next_ms = now_ms;
    h->active = 1;
    if (g->takeover) return ALELT_HA_TAKEOVER;
    if (!was_active || changed) return ALELT_HA_ACTIVE;
    return ALELT_HA_RENEWED;
}

alelt_ha_event alelt_ha_on_error(alelt_ha *h, uint64_t now_ms)
{
    if (!h->enabled) return ALELT_HA_NONE;
    h->next_ms = now_ms + 1000u; /* retry soon; the renew interval keeps the margin */
    if (alelt_ha_must_fence(h, now_ms)) {
        h->active = 0;
        h->next_ms = now_ms + (uint64_t)h->renew_s * 1000u;
        return ALELT_HA_LOST;
    }
    return ALELT_HA_NONE;
}

alelt_ha_event alelt_ha_on_lost(alelt_ha *h, uint64_t now_ms)
{
    if (!h->enabled) return ALELT_HA_NONE;
    h->next_ms = now_ms + (uint64_t)h->renew_s * 1000u;
    if (h->active) {
        h->active = 0;
        return ALELT_HA_LOST;
    }
    return ALELT_HA_NONE;
}

long long alelt_ha_epoch(const alelt_ha *h)
{
    return (h->enabled && h->active) ? h->epoch : -1;
}

int alelt_ha_grant_paused(const alelt_lease_grant *g)
{
    return g && g->granted && g->capturing == 0;
}
