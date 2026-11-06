/* SPDX-License-Identifier: Apache-2.0
 * Production safety guard: deny_hosts + environment (see guard.h). Matching mirrors alelt_import/guard.py.
 * AHS-08: IPv4 + IPv6 (literals, CIDR, resolved names), trailing-dot FQDNs normalised, iterative glob,
 * fail closed when an address entry has to be compared with a name that does not resolve, and the
 * PROD override comes from a DBA-owned file next to the config, never from the (hub-generated) JSON.
 */
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "alelt_json.h"
#include "guard.h"
#include "log.h"

#define HOST_MAX 256
#define MAX_ADDRS 32

typedef struct ip16 {
    uint8_t b[16]; /* IPv4 stored as ::ffff:a.b.c.d */
} ip16;

static void lower(char *s)
{
    for (; *s; s++) *s = (char)tolower((unsigned char)*s);
}

static void strip_dot(char *s)
{
    size_t n = strlen(s);
    while (n > 0 && s[n - 1] == '.') s[--n] = 0;
}

/* host part + optional port of a target / entry: strips scheme://, user@, /path, [ ], trailing dot */
static void split(const char *in, char *host, size_t cap, int *port)
{
    const char *s = in, *p;
    char tmp[HOST_MAX];
    size_t n;
    *port = -1;
    host[0] = 0;
    if (!in) return;
    p = strstr(s, "://");
    if (p) s = p + 3;
    n = p ? strcspn(s, "/?#") : strcspn(s, "/#"); /* '?' is a wildcard unless this is a URL */
    p = memchr(s, '@', n);
    if (p) {
        n -= (size_t)(p + 1 - s);
        s = p + 1;
    }
    if (n >= sizeof(tmp)) n = sizeof(tmp) - 1;
    memcpy(tmp, s, n);
    tmp[n] = 0;
    if (tmp[0] == '[') { /* [v6]:port */
        char *e = strchr(tmp, ']');
        if (e) {
            *e = 0;
            if (e[1] == ':') *port = atoi(e + 2);
            alelt_strcpy(host, cap, tmp + 1);
        }
    } else {
        char *c = strchr(tmp, ':');
        if (c && !strchr(c + 1, ':')) { /* exactly one ':' = host:port; more = bare IPv6 */
            *c = 0;
            if (c[1]) *port = atoi(c + 1);
        }
        alelt_strcpy(host, cap, tmp);
    }
    lower(host);
    strip_dot(host);
}

/* Iterative wildcard match ('*', '?'): O(len(p) * len(s)) worst case, no recursion. */
static int glob(const char *p, const char *s)
{
    const char *star = NULL, *ss = s;
    while (*s) {
        if (*p == '?' || (*p && *p == *s)) {
            p++;
            s++;
        } else if (*p == '*') {
            star = p++;
            ss = s;
        } else if (star) {
            p = star + 1;
            s = ++ss;
        } else {
            return 0;
        }
    }
    while (*p == '*') p++;
    return !*p;
}

/* IPv4 or IPv6 literal -> 16 bytes; 0 if not a literal */
static int ip_literal(const char *s, ip16 *out)
{
    struct in_addr a4;
    struct in6_addr a6;
    memset(out, 0, sizeof(*out));
    if (inet_pton(AF_INET, s, &a4) == 1) {
        out->b[10] = out->b[11] = 0xFF;
        memcpy(out->b + 12, &a4, 4);
        return 1;
    }
    if (inet_pton(AF_INET6, s, &a6) == 1) {
        memcpy(out->b, &a6, 16);
        return 1;
    }
    return 0;
}

/* entry "addr" or "addr/len" (v4 or v6): 1 = parsed; *bits = prefix length on the 128-bit form */
static int net_entry(const char *e, ip16 *net, int *bits)
{
    char tmp[80];
    char *sl;
    int len = -1, v4;
    if (alelt_strcpy(tmp, sizeof(tmp), e) != 0) return 0;
    sl = strchr(tmp, '/');
    if (sl) {
        *sl = 0;
        len = atoi(sl + 1);
    }
    strip_dot(tmp);
    if (tmp[0] == '[') { /* [v6] */
        size_t n = strlen(tmp);
        if (n < 3 || tmp[n - 1] != ']') return 0;
        memmove(tmp, tmp + 1, n - 2);
        tmp[n - 2] = 0;
    }
    if (!ip_literal(tmp, net)) return 0;
    v4 = strchr(tmp, ':') == NULL;
    if (len < 0) len = v4 ? 32 : 128;
    if (len > (v4 ? 32 : 128)) return 0;
    *bits = v4 ? len + 96 : len;
    return 1;
}

static int in_net(const ip16 *ip, const ip16 *net, int bits)
{
    int i;
    for (i = 0; i < 16 && bits > 0; i++, bits -= 8) {
        uint8_t m = bits >= 8 ? 0xFF : (uint8_t)(0xFF << (8 - bits));
        if ((ip->b[i] & m) != (net->b[i] & m)) return 0;
    }
    return 1;
}

/* Resolved addresses of a name (v4 + v6). -1 = resolution failed. */
static int resolve(const char *host, ip16 *out, int cap)
{
    WSADATA wsa;
    struct addrinfo hints, *res = NULL, *r;
    int n = 0;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) return -1;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    if (getaddrinfo(host, NULL, &hints, &res) != 0) {
        WSACleanup();
        return -1;
    }
    for (r = res; r && n < cap; r = r->ai_next) {
        memset(&out[n], 0, sizeof(out[n]));
        if (r->ai_family == AF_INET) {
            const struct sockaddr_in *sa = (const struct sockaddr_in *)(const void *)r->ai_addr;
            out[n].b[10] = out[n].b[11] = 0xFF;
            memcpy(out[n].b + 12, &sa->sin_addr, 4);
            n++;
        } else if (r->ai_family == AF_INET6) {
            const struct sockaddr_in6 *sa = (const struct sockaddr_in6 *)(const void *)r->ai_addr;
            memcpy(out[n].b, &sa->sin6_addr, 16);
            n++;
        }
    }
    freeaddrinfo(res);
    WSACleanup();
    return n > 0 ? n : -1;
}

int alelt_deny_match(const char *entry, const char *target, int port)
{
    char eh[HOST_MAX], th[HOST_MAX], e2[HOST_MAX];
    int ep, tp, bits, i, n;
    ip16 net, ip, addrs[MAX_ADDRS];
    if (!entry || !target || !*entry || !*target) return 0;
    split(target, th, sizeof(th), &tp);
    if (port <= 0) port = tp;
    if (!th[0]) return 0;
    alelt_strcpy(e2, sizeof(e2), entry);
    lower(e2);
    if (!strstr(e2, "://") && net_entry(e2, &net, &bits)) { /* address or CIDR entry (v4/v6) */
        if (ip_literal(th, &ip)) return in_net(&ip, &net, bits);
        n = resolve(th, addrs, MAX_ADDRS);
        if (n < 0) return ALELT_DENY_UNRESOLVED; /* fail closed: cannot prove it is not the denied address */
        for (i = 0; i < n; i++)
            if (in_net(&addrs[i], &net, bits)) return 1;
        return 0;
    }
    split(entry, eh, sizeof(eh), &ep);
    if (!eh[0]) return 0;
    if (ep > 0 && port > 0 && ep != port) return 0;
    if (net_entry(eh, &net, &bits)) { /* "ip:port" entry */
        if (ip_literal(th, &ip)) return in_net(&ip, &net, bits);
        n = resolve(th, addrs, MAX_ADDRS);
        if (n < 0) return ALELT_DENY_UNRESOLVED;
        for (i = 0; i < n; i++)
            if (in_net(&addrs[i], &net, bits)) return 1;
        return 0;
    }
    if (strchr(eh, '*') || strchr(eh, '?')) return glob(eh, th);
    if (strcmp(th, eh) == 0) return 1;
    return strncmp(th, eh, strlen(eh)) == 0 && th[strlen(eh)] == '.'; /* account id prefix: <id>.<anything> */
}

static int ip_entry(const char *e)
{
    ip16 n;
    int b;
    return net_entry(e, &n, &b);
}

int alelt_guard_allow_prod_path(const wchar_t *config_path, wchar_t *out, size_t cap)
{
    wchar_t *slash;
    if (alelt_wcscpy(out, cap, config_path) != 0) return -1;
    slash = wcsrchr(out, L'\\');
    if (!slash) return -1;
    slash[1] = 0;
    return wcscat_s(out, cap, ALELT_ALLOW_PROD_FILE) == 0 ? 0 : -1;
}

static int dba_allows_prod(const alelt_config *cfg)
{
    wchar_t p[ALELT_PATH_MAX];
    DWORD at;
    if (alelt_guard_allow_prod_path(cfg->path, p, ALELT_PATH_MAX) != 0) return 0;
    at = GetFileAttributesW(p);
    return at != INVALID_FILE_ATTRIBUTES && !(at & FILE_ATTRIBUTE_DIRECTORY);
}

int alelt_guard_check(const alelt_config *cfg, char *err, size_t errcap)
{
    int i, m;
    if (cfg->env_prod && !dba_allows_prod(cfg)) {
        snprintf(err, errcap, "environment=PROD: refused - the DBA must create the local file '%ls' next to the "
                              "agent config (\"allow_prod\" in the JSON is not accepted: that file may come from the "
                              "hub)", ALELT_ALLOW_PROD_FILE);
        return -1;
    }
    if (cfg->allow_prod && !cfg->env_prod)
        alelt_logf(ALELT_LOG_WARN, "guard", "\"allow_prod\" in the config is ignored; the PROD override is the DBA file %ls",
                ALELT_ALLOW_PROD_FILE);
    for (i = 0; i < cfg->ndeny; i++) {
        const char *e = cfg->deny_hosts[i];
        if (cfg->src_host[0] && (m = alelt_deny_match(e, cfg->src_host, cfg->src_port)) != 0) {
            if (m == ALELT_DENY_UNRESOLVED)
                snprintf(err, errcap, "source host '%s' cannot be resolved, so it cannot be checked against the "
                                      "production deny list entry '%s': refused (fail closed)", cfg->src_host, e);
            else
                snprintf(err, errcap, "source host '%s' matches the production deny list entry '%s': the agent will "
                                      "not connect to it", cfg->src_host, e);
            return -1;
        }
        if (cfg->database[0] && !ip_entry(e) && alelt_deny_match(e, cfg->database, -1) == 1) {
            snprintf(err, errcap, "source database alias '%s' matches the production deny list entry '%s': the "
                                  "agent will not connect to it", cfg->database, e);
            return -1;
        }
    }
    return 0;
}

int alelt_guard_check_connected(const alelt_config *cfg, const char *server_json, char *err, size_t errcap)
{
    char perr[128];
    fj_doc *d;
    const char *host = NULL, *dbn = NULL;
    int i, m, rc = 0;
    if (cfg->ndeny <= 0) return 0;
    d = server_json ? fj_parse(server_json, strlen(server_json), perr, sizeof(perr)) : NULL;
    if (d && fj_root(d)->type == FJ_OBJ) {
        host = fj_get_str(fj_root(d), "host");
        dbn = fj_get_str(fj_root(d), "database_name");
    }
    if (!host || !*host) {
        snprintf(err, errcap, "deny_hosts is set but the connected Db2 server could not be identified: refused "
                              "(fail closed)");
        fj_free(d);
        return -1;
    }
    for (i = 0; i < cfg->ndeny && rc == 0; i++) {
        const char *e = cfg->deny_hosts[i];
        if ((m = alelt_deny_match(e, host, -1)) != 0) {
            snprintf(err, errcap, m == ALELT_DENY_UNRESOLVED
                         ? "connected Db2 server '%s' cannot be resolved to compare with the deny list entry '%s': "
                           "refused (fail closed)"
                         : "connected Db2 server '%s' (reached through alias '%s') matches the production deny list "
                           "entry '%s': disconnected, the agent will not capture it",
                     host, m == ALELT_DENY_UNRESOLVED ? e : cfg->database, e);
            rc = -1;
        } else if (dbn && *dbn && !ip_entry(e) && alelt_deny_match(e, dbn, -1) == 1) {
            snprintf(err, errcap, "connected database '%s' on '%s' matches the production deny list entry '%s'",
                     dbn, host, e);
            rc = -1;
        }
    }
    fj_free(d);
    return rc;
}
