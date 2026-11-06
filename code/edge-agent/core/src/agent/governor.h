/* SPDX-License-Identifier: Apache-2.0
 * Governor: pauses capture/snapshot when the host or database is under
 * pressure, with hysteresis. Inputs: host CPU % and database log utilisation %
 * (from the whitelisted "db_health" probe). Pure state machine (unit tested).
 */
#ifndef ALELT_GOVERNOR_H
#define ALELT_GOVERNOR_H

typedef struct alelt_gov_cfg {
    int enabled;
    int host_cpu_pause_pct;
    int host_cpu_resume_pct;
    int sustain;
    int max_log_util_pct;
} alelt_gov_cfg;

typedef struct alelt_gov {
    int paused;
    int over;   /* consecutive samples above the pause threshold  */
    int under;  /* consecutive samples below the resume threshold */
    char reason[96];
} alelt_gov;

void alelt_gov_init(alelt_gov *g);
/* host_cpu / log_util < 0 mean "unknown" (ignored).
 * Returns +1 when it just paused, -1 when it just resumed, 0 otherwise. */
int alelt_gov_step(alelt_gov *g, const alelt_gov_cfg *c, double host_cpu, double log_util);

#endif
