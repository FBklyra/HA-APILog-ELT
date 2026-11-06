/* SPDX-License-Identifier: Apache-2.0
 * Active/passive lease decisions: standby until granted, takeover vs continuation,
 * renew, self-fencing before the hub can re-grant, lost lease, hub-capped ttl.
 */
#include <stdio.h>
#include <string.h>

#include "alelttest.h"
#include "ha.h"
#include "hub.h"

static alelt_lease_grant grant(int granted, long long epoch, const char *lease, int takeover)
{
    alelt_lease_grant g;
    memset(&g, 0, sizeof(g));
    g.granted = granted;
    g.epoch = epoch;
    g.takeover = takeover;
    g.ttl_s = 30;
    strcpy_s(g.lease_id, sizeof(g.lease_id), lease);
    strcpy_s(g.holder, sizeof(g.holder), granted ? "me" : "other");
    return g;
}

static void t_standalone(void)
{
    alelt_ha h;
    alelt_ha_init(&h, 0, 30, 10);
    CHECK(h.active && alelt_ha_epoch(&h) == -1 && !alelt_ha_due(&h, 0) && !alelt_ha_must_fence(&h, 1u << 30));
}

static void t_standby_then_active(void)
{
    alelt_ha h;
    alelt_lease_grant g;
    alelt_ha_init(&h, 1, 30, 10);
    CHECK(!h.active && alelt_ha_due(&h, 0) && alelt_ha_epoch(&h) == -1);
    g = grant(0, 3, "", 0);
    CHECK(alelt_ha_on_grant(&h, &g, 1000) == ALELT_HA_NONE && !h.active);
    CHECK(!alelt_ha_due(&h, 5000) && alelt_ha_due(&h, 11000)); /* standby polls acquire every renew_s */
    g = grant(1, 4, "aaaaaaaa-0000-4000-8000-000000000004", 1);
    CHECK(alelt_ha_on_grant(&h, &g, 11000) == ALELT_HA_TAKEOVER && h.active && alelt_ha_epoch(&h) == 4);
    g.takeover = 0;
    CHECK(alelt_ha_on_grant(&h, &g, 21000) == ALELT_HA_RENEWED);
    CHECK(alelt_ha_wait_ms(&h, 21000) == 10000);
}

static void t_continuation(void)
{
    alelt_ha h;
    alelt_lease_grant g;
    alelt_ha_init(&h, 1, 30, 10);
    g = grant(1, 7, "bbbbbbbb-0000-4000-8000-000000000007", 0); /* own unexpired lease after a restart */
    CHECK(alelt_ha_on_grant(&h, &g, 0) == ALELT_HA_ACTIVE && h.active);
}

static void t_self_fence(void)
{
    alelt_ha h;
    alelt_lease_grant g;
    alelt_ha_init(&h, 1, 30, 10);
    g = grant(1, 1, "cccccccc-0000-4000-8000-000000000001", 1);
    alelt_ha_on_grant(&h, &g, 0);
    CHECK(alelt_ha_on_error(&h, 10000) == ALELT_HA_NONE && h.active); /* renew failed, still inside the margin */
    CHECK(!alelt_ha_must_fence(&h, 19999) && alelt_ha_must_fence(&h, 20000)); /* ttl - renew = 20 s */
    CHECK(alelt_ha_wait_ms(&h, 19000) <= 1000);
    CHECK(alelt_ha_on_error(&h, 20000) == ALELT_HA_LOST && !h.active && alelt_ha_epoch(&h) == -1);
    /* the hub lease expires at 30 s, strictly after the local fence at 20 s: never two readers */
}

/* AHS-07: the fence clock starts when the renew was SENT, not when the (slow) reply arrived */
static void t_delayed_reply(void)
{
    alelt_ha h;
    alelt_lease_grant g;
    alelt_ha_init(&h, 1, 30, 10);
    g = grant(1, 1, "ffffffff-0000-4000-8000-000000000001", 1);
    CHECK(alelt_ha_on_grant_at(&h, &g, 0, 0) == ALELT_HA_TAKEOVER);
    g.takeover = 0;
    CHECK(alelt_ha_on_grant_at(&h, &g, 10000, 24000) == ALELT_HA_RENEWED); /* renew sent at 10 s, reply after 14 s */
    CHECK(!alelt_ha_must_fence(&h, 29999) && alelt_ha_must_fence(&h, 30000)); /* fence = sent + 20 s, not reply + 20 s */
    /* hub ttl started no earlier than 10 s -> expires >= 40 s, strictly after the 30 s fence: never two readers */
    CHECK(alelt_ha_on_grant_at(&h, &g, 30000, 50000) == ALELT_HA_LOST && !h.active); /* reply later than the margin */
    CHECK(alelt_ha_epoch(&h) == -1 && alelt_ha_due(&h, 50250));
}

static void t_lost(void)
{
    alelt_ha h;
    alelt_lease_grant g;
    alelt_ha_init(&h, 1, 30, 10);
    g = grant(1, 2, "dddddddd-0000-4000-8000-000000000002", 0);
    alelt_ha_on_grant(&h, &g, 0);
    CHECK(alelt_ha_on_lost(&h, 100) == ALELT_HA_LOST && !h.active);
    CHECK(alelt_ha_on_lost(&h, 200) == ALELT_HA_NONE);
    g = grant(0, 3, "", 0); /* acquire refused: other holder */
    CHECK(alelt_ha_on_grant(&h, &g, 10200) == ALELT_HA_NONE && !h.active);
}

static void t_short_ttl(void)
{
    alelt_ha h;
    alelt_lease_grant g;
    alelt_ha_init(&h, 1, 30, 10);
    g = grant(1, 1, "eeeeeeee-0000-4000-8000-000000000001", 1);
    g.ttl_s = 8; /* hub caps the ttl */
    alelt_ha_on_grant(&h, &g, 0);
    CHECK(alelt_ha_ttl(&h) == 8 && h.renew_s == 4 && alelt_ha_must_fence(&h, 4000));
}

/* AHS-24: the grant says whether capture was running; absent/null = previous behaviour (-1). */
static void t_grant_capturing(void)
{
    alelt_lease_grant g;
    char err[128];
    const char *base = "{\"granted\":true,\"connection_id\":\"c\",\"holder\":\"me\","
                       "\"lease_id\":\"11111111-2222-4333-8444-555555555555\",\"epoch\":3,\"ttl_s\":30,"
                       "\"takeover\":true,\"resume_from\":{\"engine\":\"db2\",\"text\":\"0050\"}";
    char buf[512];
    snprintf(buf, sizeof(buf), "%s,\"capturing\":false}", base);
    CHECK(alelt_lease_parse_reply(buf, strlen(buf), &g, err, sizeof(err)) == 0);
    CHECK(g.granted && g.takeover && g.epoch == 3 && g.capturing == 0 && strcmp(g.resume_from, "0050") == 0);
    CHECK(alelt_ha_grant_paused(&g));
    snprintf(buf, sizeof(buf), "%s,\"capturing\":true}", base);
    CHECK(alelt_lease_parse_reply(buf, strlen(buf), &g, err, sizeof(err)) == 0 && g.capturing == 1);
    CHECK(!alelt_ha_grant_paused(&g));
    snprintf(buf, sizeof(buf), "%s,\"capturing\":null}", base);
    CHECK(alelt_lease_parse_reply(buf, strlen(buf), &g, err, sizeof(err)) == 0 && g.capturing == -1);
    snprintf(buf, sizeof(buf), "%s}", base);
    CHECK(alelt_lease_parse_reply(buf, strlen(buf), &g, err, sizeof(err)) == 0 && g.capturing == -1);
    CHECK(!alelt_ha_grant_paused(&g)); /* older hub: previous behaviour */
    CHECK(alelt_lease_parse_reply("{\"granted\":true}", 16, &g, err, sizeof(err)) == -1); /* no lease_id/epoch */
}

void test_ha_suite(void)
{
    printf("ha\n");
    RUN(t_standalone);
    RUN(t_standby_then_active);
    RUN(t_continuation);
    RUN(t_self_fence);
    RUN(t_delayed_reply);
    RUN(t_lost);
    RUN(t_short_ttl);
    RUN(t_grant_capturing);
}
