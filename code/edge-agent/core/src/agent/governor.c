/* SPDX-License-Identifier: Apache-2.0 */
#include <stdio.h>
#include <string.h>

#include "governor.h"

void alelt_gov_init(alelt_gov *g)
{
    memset(g, 0, sizeof(*g));
}

int alelt_gov_step(alelt_gov *g, const alelt_gov_cfg *c, double host_cpu, double log_util)
{
    int hot = 0, cool = 1;
    if (!c->enabled) {
        if (g->paused) {
            g->paused = 0;
            g->over = g->under = 0;
            g->reason[0] = 0;
            return -1;
        }
        return 0;
    }
    if (host_cpu >= 0.0) {
        if (host_cpu >= (double)c->host_cpu_pause_pct) {
            hot = 1;
            snprintf(g->reason, sizeof(g->reason), "host_cpu %.1f%% >= %d%%", host_cpu, c->host_cpu_pause_pct);
        }
        if (host_cpu > (double)c->host_cpu_resume_pct) cool = 0;
    }
    if (log_util >= 0.0) {
        if (log_util >= (double)c->max_log_util_pct) {
            hot = 1;
            snprintf(g->reason, sizeof(g->reason), "log_utilization %.1f%% >= %d%%", log_util, c->max_log_util_pct);
        }
        if (log_util >= (double)c->max_log_util_pct - 5.0) cool = 0;
    }
    if (hot) {
        g->over++;
        g->under = 0;
    } else if (cool) {
        g->under++;
        g->over = 0;
    } else {
        g->over = 0;
        g->under = 0;
    }
    if (!g->paused && g->over >= c->sustain) {
        g->paused = 1;
        return 1;
    }
    if (g->paused && g->under >= c->sustain) {
        g->paused = 0;
        g->reason[0] = 0;
        return -1;
    }
    return 0;
}
