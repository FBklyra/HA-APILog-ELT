/* SPDX-License-Identifier: Apache-2.0 */
#ifndef ALELT_SERVICE_H
#define ALELT_SERVICE_H

#include <wchar.h>

#include "config.h"

typedef struct alelt_install_opts {
    const wchar_t *config_path;  /* absolute */
    const wchar_t *account;      /* NULL = NT SERVICE\ALELTAgent (virtual account) */
    const wchar_t *password;     /* NULL for virtual/managed/builtin accounts */
    int start_after;
    alelt_init_opts init;
} alelt_install_opts;

int alelt_svc_install(const alelt_install_opts *o);
int alelt_svc_uninstall(void);
/* For MSI installs (service created by Windows Installer): grant logon right,
 * register the Event Log source, apply ACLs. */
int alelt_svc_post_install(const wchar_t *config_path, const wchar_t *account);
int alelt_svc_start(void);
int alelt_svc_stop(void);
int alelt_svc_status(void);
/* Called for "--service": hands the process to the SCM. */
int alelt_svc_dispatch(const wchar_t *config_path);
/* Restrict ACLs on config + data directories (also used by --set-dev-token). */
int alelt_secure_paths(const wchar_t *config_path, const wchar_t *account);

#endif
