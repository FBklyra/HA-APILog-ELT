/* SPDX-License-Identifier: Apache-2.0 */
#include "alelttest.h"
#include "governor.h"

static void t_hysteresis(void)
{
    alelt_gov g;
    alelt_gov_cfg c = {1, 85, 70, 3, 90};
    alelt_gov_init(&g);
    CHECK(alelt_gov_step(&g, &c, 90, -1) == 0);
    CHECK(alelt_gov_step(&g, &c, 95, -1) == 0);
    CHECK(alelt_gov_step(&g, &c, 99, -1) == 1 && g.paused); /* 3 sustained samples */
    CHECK(alelt_gov_step(&g, &c, 75, -1) == 0 && g.paused);  /* between thresholds: hold */
    CHECK(alelt_gov_step(&g, &c, 50, -1) == 0);
    CHECK(alelt_gov_step(&g, &c, 50, -1) == 0);
    CHECK(alelt_gov_step(&g, &c, 50, -1) == -1 && !g.paused);
}

static void t_log_util_and_unknown(void)
{
    alelt_gov g;
    alelt_gov_cfg c = {1, 85, 70, 1, 80};
    alelt_gov_init(&g);
    CHECK(alelt_gov_step(&g, &c, -1, -1) == 0 && !g.paused); /* unknown inputs never pause */
    CHECK(alelt_gov_step(&g, &c, 10, 81) == 1);
    CHECK(alelt_gov_step(&g, &c, 10, 77) == 0 && g.paused);  /* within 5 pts of limit: hold */
    CHECK(alelt_gov_step(&g, &c, 10, 50) == -1);
    c.enabled = 0;
    CHECK(alelt_gov_step(&g, &c, 100, 100) == 0 && !g.paused);
}

void test_governor_suite(void)
{
    printf("governor\n");
    RUN(t_hysteresis);
    RUN(t_log_util_and_unknown);
}
