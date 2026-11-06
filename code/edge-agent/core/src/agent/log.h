/* SPDX-License-Identifier: Apache-2.0 */
#ifndef ALELT_LOG_H
#define ALELT_LOG_H

#include <stdint.h>
#include <wchar.h>

#include "alelt_plugin.h" /* ALELT_LOG_* levels */

typedef struct alelt_log_cfg {
    const wchar_t *dir;     /* NULL: console only */
    int level;              /* minimum level      */
    uint64_t max_bytes;     /* rotate above this  */
    int keep;               /* rotated files kept */
    int console;            /* mirror to stderr   */
    int eventlog;           /* errors -> Windows Event Log */
} alelt_log_cfg;

int alelt_log_open(const alelt_log_cfg *cfg);
void alelt_log_close(void);
int alelt_log_level_from_str(const char *s);

/* Structured line: {"ts","level","component","msg", + optional fields}. */
void alelt_log_ev(int level, const char *component, const char *msg, const char *batch_id, const char *position,
               const char *table, const char *correlation_id);
void alelt_logf(int level, const char *component, const char *fmt, ...);

/* Event Log message ids (see res/aleltagent_msg.mc). */
#define ALELT_EVT_GENERIC_INFO 0x40000001L
#define ALELT_EVT_GENERIC_WARN 0x80000002L
#define ALELT_EVT_GENERIC_ERROR 0xC0000003L
void alelt_eventlog(int level, const char *msg);

#endif
