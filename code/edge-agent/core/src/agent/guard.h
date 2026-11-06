/* SPDX-License-Identifier: Apache-2.0
 * Production safety guard (REQ-12, X-55), checked before the source is ever opened:
 *  - "environment": "PROD" refuses to start unless the local, DBA-owned "allow_prod": true is set;
 *  - "deny_hosts": same entry syntax as the hub deny list (host, *.wildcard, IP, CIDR, host:port, account prefix,
 *    URL). The source host/port from source.settings (host|hostname|server, port) and the database alias are
 *    checked; a match refuses to start. The agent exits and is not restarted (GUARD exit code).
 */
#ifndef ALELT_GUARD_H
#define ALELT_GUARD_H

#include <stddef.h>

#include "config.h"

#define ALELT_EXIT_GUARD 78 /* same code as the Linux daemon (systemd RestartPreventExitStatus) */
#define ALELT_DENY_UNRESOLVED 2 /* alelt_deny_match: an address entry vs a name that does not resolve (fail closed) */
/* DBA-owned PROD override (AHS-08): this file next to aleltagent.json; "allow_prod" in the JSON is ignored. */
#define ALELT_ALLOW_PROD_FILE L"allow_prod.dba"

/* 1 when `target` (host, host:port, IPv4/IPv6, URL) matches deny-list `entry`; ALELT_DENY_UNRESOLVED when an
 * IP/CIDR entry cannot be compared because the name does not resolve; `port` <= 0 = unknown. */
int alelt_deny_match(const char *entry, const char *target, int port);

/* 0 = allowed; -1 = refused (err filled). Never contacts anything except a local DNS lookup when an IP/CIDR
 * entry must be compared with a host name. */
int alelt_guard_check(const alelt_config *cfg, char *err, size_t errcap);
int alelt_guard_allow_prod_path(const wchar_t *config_path, wchar_t *out, size_t cap);

/* AHS-08 / LNX-08: after the source connected, the server it REALLY reached (a Db2 alias goes through the node
 * directory, so the configured alias says nothing): `server_json` = the plugin probe "server_info"
 * ({"host": <server host name>, "server_name": ..., "database_name": ...}). With a deny list, an unidentifiable
 * server (no JSON / no host) is refused (fail closed). 0 = allowed, -1 = refused (err filled). */
int alelt_guard_check_connected(const alelt_config *cfg, const char *server_json, char *err, size_t errcap);

#endif
