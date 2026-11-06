/* SPDX-License-Identifier: Apache-2.0 */
#ifndef ALELT_CONFIG_H
#define ALELT_CONFIG_H

#include <stdint.h>
#include <wchar.h>

#include "alelt_buf.h"
#include "alelt_plugin.h"
#include "alelt_util.h"

#define ALELT_MAX_PINS 4
#define ALELT_MAX_ALLOWED_TABLES 512
#define ALELT_MAX_DLL_DIRS 4
#define ALELT_MAX_DENY 256
#define ALELT_DENY_LEN 255

typedef struct alelt_limits {
    int cpu_pct;       /* % of ONE core (Job Object hard cap)  */
    int mem_mb;        /* Job Object job memory limit          */
    int batch_kb;      /* raw bytes per batch / read buffer    */
    int max_rate_kbps; /* upload rate cap, 0 = unlimited       */
    int poll_ms;       /* idle wait at end of log              */
} alelt_limits;

typedef enum alelt_hub_mode { ALELT_HUB_MTLS = 0, ALELT_HUB_DEV = 1 } alelt_hub_mode;

typedef struct alelt_config {
    wchar_t path[ALELT_PATH_MAX];
    char agent_id[128];
    char connection_id[128];

    /* hub link */
    char hub_url[512];
    alelt_hub_mode hub_mode;
    char cert_thumbprint[64];           /* SHA-1 hex, LocalMachine\My      */
    char hub_pins[ALELT_MAX_PINS][65];     /* SHA-256 hex of server SPKI DER  */
    int npins;
    char dev_token_env[64];             /* dev mode only                   */
    char hub_proxy[256];                /* "" direct (default), "system", or host:port (AHS-25) */
    int allow_insecure_http;            /* token/dev over http:// off loopback (AHS-11; lab only) */
    int heartbeat_s;
    int command_wait_s;
    int connect_timeout_ms;
    int request_timeout_ms;
    int backoff_max_s;
    int websocket;                      /* reserved; long-poll is used     */

    /* spool / log */
    wchar_t spool_dir[ALELT_PATH_MAX];
    uint64_t spool_max_bytes;
    wchar_t log_dir[ALELT_PATH_MAX];
    int log_level;
    uint64_t log_max_bytes;
    int log_keep;

    /* limits: effective defaults and DBA-approved maxima */
    alelt_limits limits;
    alelt_limits limits_max;
    int require_job_limits;

    /* governor */
    int gov_enabled;
    int gov_interval_s;
    int gov_host_cpu_pause_pct;
    int gov_host_cpu_resume_pct;
    int gov_sustain;
    int gov_max_log_util_pct;

    /* source plugin */
    char plugin[64];                    /* file name next to aleltagent.exe   */
    char engine[32];
    char database[129];
    wchar_t dll_dirs[ALELT_MAX_DLL_DIRS][ALELT_PATH_MAX];
    int ndll_dirs;
    alelt_buf settings_json;               /* raw "source.settings" object    */

    /* snapshot allow-list (local, DBA-approved) */
    char (*allowed_tables)[2 * ALELT_IDENT_MAX + 1]; /* "SCHEMA.TABLE" */
    int nallowed;
    int snapshot_max_chunk_rows;

    /* capture */
    int capture_autostart;

    /* high availability (X-48): standalone, or active/passive pair holding a hub capture lease */
    int ha_active_passive;              /* 0 = standalone (default)        */
    int ha_lease_ttl_s;                 /* 5..300, default 30              */
    int ha_renew_s;                     /* 1..ttl/2, default 10            */

    /* production guard (REQ-12, X-55): checked before the source is opened (guard.c) */
    char (*deny_hosts)[ALELT_DENY_LEN + 1];
    int ndeny;
    int env_prod;                       /* "environment": "PROD"           */
    int allow_prod;                     /* local, DBA-owned override       */
    char src_host[256];                 /* source.settings host|hostname|server */
    int src_port;                       /* source.settings port, -1 unknown */
} alelt_config;

int alelt_config_load(const wchar_t *path, alelt_config *cfg, char *err, size_t errcap);
void alelt_config_free(alelt_config *cfg);
void alelt_limits_clamp(alelt_limits *l, const alelt_limits *max);
int alelt_config_allow_entry_valid(const char *entry);
int alelt_config_table_allowed(const alelt_config *cfg, const char *schema, const char *table);
int alelt_ident_valid(const char *s);

/* Default config location: %ProgramData%\ALELT\Agent\aleltagent.json */
int alelt_config_default_path(wchar_t *out, size_t cap);
/* %ProgramData%\ALELT\Agent\<leaf> (default spool / log directories) */
int alelt_config_default_data_dir(wchar_t *out, size_t cap, const wchar_t *leaf);

/* Write a starter config (used by --init-config / MSI). Never overwrites. */
typedef struct alelt_init_opts {
    const char *hub_url, *agent_id, *connection_id, *thumbprint, *pin, *spool_dir, *log_dir, *database, *mode;
} alelt_init_opts;
int alelt_config_write_initial(const wchar_t *path, const alelt_init_opts *o, char *err, size_t errcap);

#endif
