/* SPDX-License-Identifier: Apache-2.0 */
#ifndef ALELT_JOBOBJ_H
#define ALELT_JOBOBJ_H

#include "config.h"

/* Put the current process in a Job Object with a CPU hard cap (% of one core),
 * a job memory limit, BELOW_NORMAL priority class; die on unhandled exception
 * instead of showing UI. Safe to call again to change limits. */
int alelt_job_apply(const alelt_limits *l, char *err, size_t errcap);
void alelt_job_close(void);
/* Own process usage: CPU % of one core since last call, private MB. */
void alelt_self_usage(double *cpu_pct, double *mem_mb);
/* Host CPU % (all cores) since last call; -1 on first call. */
double alelt_host_cpu_pct(void);

#endif
