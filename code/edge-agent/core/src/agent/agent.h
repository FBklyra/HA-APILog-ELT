/* SPDX-License-Identifier: Apache-2.0 */
#ifndef ALELT_AGENT_H
#define ALELT_AGENT_H

#include <wchar.h>

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

typedef void (*alelt_running_cb)(void);

#define ALELT_EXIT_CONFIG 3 /* config not loadable / invalid: not restarted by the service (AHS-09) */

/* AHS-23: called about every 2 s while the agent stops (service: STOP_PENDING checkpoint). */
void alelt_agent_on_stop_progress(void (*cb)(void));

/* Runs the agent until stop_ev is signalled or the hub sends SHUTDOWN.
 * Returns 0 on clean stop, non-zero service-specific exit code on fatal error. */
int alelt_agent_run(const wchar_t *config_path, HANDLE stop_ev, int console, alelt_running_cb on_running);

#endif
