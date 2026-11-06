/* SPDX-License-Identifier: Apache-2.0 */
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <wincrypt.h>
#include <winhttp.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "devtoken.h"
#include "alelt_hash.h"
#include "http.h"
#include "log.h"
#include "version.h"

#define MAX_RESP ((size_t)8 * 1024 * 1024)

struct alelt_http {
    HINTERNET session;
    HINTERNET connect;
    int secure;
    alelt_hub_mode mode;
    wchar_t host[256];
    INTERNET_PORT port;
    char base_path[256];
    PCCERT_CONTEXT client_cert;
    char pins[ALELT_MAX_PINS][65];
    int npins;
    char agent_id[128];
    int connect_timeout_ms;
    int proxy_kind; /* AHS-25: 0 direct (default), 1 system, 2 named */
};

/* AHS-23: in-flight requests, so a stop can abort a POST stuck for request_timeout_ms (<= 600 s). */
#define MAX_INFLIGHT 32
static SRWLOCK g_inflight_lock = SRWLOCK_INIT;
static HINTERNET g_inflight[MAX_INFLIGHT];

static void inflight_add(HINTERNET r)
{
    int i;
    AcquireSRWLockExclusive(&g_inflight_lock);
    for (i = 0; i < MAX_INFLIGHT; i++) {
        if (!g_inflight[i]) {
            g_inflight[i] = r;
            break;
        }
    }
    ReleaseSRWLockExclusive(&g_inflight_lock);
}

/* 1 = still registered (the owner closes it), 0 = already closed by alelt_http_abort_all */
static int inflight_remove(HINTERNET r)
{
    int i, found = 0;
    AcquireSRWLockExclusive(&g_inflight_lock);
    for (i = 0; i < MAX_INFLIGHT; i++) {
        if (g_inflight[i] == r) {
            g_inflight[i] = NULL;
            found = 1;
            break;
        }
    }
    ReleaseSRWLockExclusive(&g_inflight_lock);
    return found;
}

int alelt_http_abort_all(void)
{
    int i, n = 0;
    AcquireSRWLockExclusive(&g_inflight_lock);
    for (i = 0; i < MAX_INFLIGHT; i++) {
        if (g_inflight[i]) {
            WinHttpCloseHandle(g_inflight[i]); /* the blocking call in the owner thread fails (cancelled) */
            g_inflight[i] = NULL;
            n++;
        }
    }
    ReleaseSRWLockExclusive(&g_inflight_lock);
    return n;
}

static int host_is_loopback(const wchar_t *h)
{
    return _wcsicmp(h, L"localhost") == 0 || wcsncmp(h, L"127.", 4) == 0 || wcscmp(h, L"::1") == 0 ||
           wcscmp(h, L"[::1]") == 0;
}

void alelt_http_resp_init(alelt_http_resp *r)
{
    r->status = 0;
    r->retry_after_s = -1;
    r->err[0] = 0;
    alelt_buf_init(&r->body, MAX_RESP);
}

void alelt_http_resp_free(alelt_http_resp *r)
{
    alelt_buf_free(&r->body);
}

static PCCERT_CONTEXT find_client_cert(const char *thumb_hex, char *err, size_t errcap)
{
    HCERTSTORE st;
    CRYPT_HASH_BLOB blob;
    BYTE hash[20];
    size_t n = 0;
    PCCERT_CONTEXT c;
    if (alelt_unhex(thumb_hex, hash, sizeof(hash), &n) != 0 || n != 20) {
        snprintf(err, errcap, "client_cert_thumbprint must be 40 hex chars (SHA-1)");
        return NULL;
    }
    st = CertOpenStore(CERT_STORE_PROV_SYSTEM_W, 0, 0,
                       CERT_SYSTEM_STORE_LOCAL_MACHINE | CERT_STORE_READONLY_FLAG | CERT_STORE_OPEN_EXISTING_FLAG, L"MY");
    if (!st) {
        snprintf(err, errcap, "cannot open LocalMachine\\My (error %lu)", (unsigned long)GetLastError());
        return NULL;
    }
    blob.cbData = 20;
    blob.pbData = hash;
    c = CertFindCertificateInStore(st, X509_ASN_ENCODING | PKCS_7_ASN_ENCODING, 0, CERT_FIND_SHA1_HASH, &blob, NULL);
    if (!c) snprintf(err, errcap, "client certificate %s not found in LocalMachine\\My", thumb_hex);
    CertCloseStore(st, 0);
    return c; /* context keeps its own reference to the store */
}

alelt_http *alelt_http_new(const alelt_config *cfg, char *err, size_t errcap)
{
    alelt_http *h;
    URL_COMPONENTSW uc;
    wchar_t url[512];
    DWORD protos;
    char tmp[256];
    h = (alelt_http *)calloc(1, sizeof(*h));
    if (!h) {
        snprintf(err, errcap, "out of memory");
        return NULL;
    }
    h->mode = cfg->hub_mode;
    h->connect_timeout_ms = cfg->connect_timeout_ms;
    alelt_strcpy(h->agent_id, sizeof(h->agent_id), cfg->agent_id);
    memcpy(h->pins, cfg->hub_pins, sizeof(h->pins));
    h->npins = cfg->npins;
    if (alelt_to_wide(cfg->hub_url, url, 512) != 0) {
        snprintf(err, errcap, "bad hub url");
        goto fail;
    }
    ZeroMemory(&uc, sizeof(uc));
    uc.dwStructSize = sizeof(uc);
    uc.dwSchemeLength = (DWORD)-1;
    uc.dwHostNameLength = (DWORD)-1;
    uc.dwUrlPathLength = (DWORD)-1;
    if (!WinHttpCrackUrl(url, 0, 0, &uc) || uc.dwHostNameLength == 0 || uc.dwHostNameLength >= 256) {
        snprintf(err, errcap, "cannot parse hub url");
        goto fail;
    }
    wmemcpy(h->host, uc.lpszHostName, uc.dwHostNameLength);
    h->host[uc.dwHostNameLength] = 0;
    h->port = uc.nPort;
    h->secure = (uc.nScheme == INTERNET_SCHEME_HTTPS);
    if (uc.dwUrlPathLength > 0) {
        wchar_t p[256];
        if (uc.dwUrlPathLength >= 256) { snprintf(err, errcap, "hub url path too long"); goto fail; }
        wmemcpy(p, uc.lpszUrlPath, uc.dwUrlPathLength);
        p[uc.dwUrlPathLength] = 0;
        if (alelt_to_utf8(p, h->base_path, sizeof(h->base_path)) != 0) goto fail;
        while (h->base_path[0] && h->base_path[strlen(h->base_path) - 1] == '/') h->base_path[strlen(h->base_path) - 1] = 0;
    }
    if (h->mode == ALELT_HUB_MTLS && !h->secure) {
        snprintf(err, errcap, "mtls mode requires https");
        goto fail;
    }
    if (h->mode == ALELT_HUB_MTLS) {
        h->client_cert = find_client_cert(cfg->cert_thumbprint, err, errcap);
        if (!h->client_cert) goto fail;
    } else {
        if (!h->secure && !host_is_loopback(h->host) && !cfg->allow_insecure_http) {
            /* AHS-11: a bearer token over plain http off this host is refused */
            snprintf(err, errcap, "token mode over plain http:// to a non-loopback hub is refused; use https:// "
                                  "(or hub.allow_insecure_http=true for an isolated lab)");
            goto fail;
        }
        (void)alelt_cred_init(cfg); /* per-agent credential if stored, else the enrollment / dev token */
        if (!h->secure)
            alelt_logf(ALELT_LOG_WARN, "http", "hub link over plain http (token mode) - not for production");
    }
    if (_snprintf_s(tmp, sizeof(tmp), _TRUNCATE, "ALELTAgent/%s", ALELT_AGENT_VERSION) < 0) goto fail;
    {
        wchar_t ua[64];
        alelt_to_wide(tmp, ua, 64);
        /* AHS-25: no WPAD / auto-proxy discovery from a service on the database host by default.
         * hub.proxy: "" = direct, "system" = WinHTTP automatic/system proxy, else "host:port". */
        if (!cfg->hub_proxy[0]) {
            h->session = WinHttpOpen(ua, WINHTTP_ACCESS_TYPE_NO_PROXY, WINHTTP_NO_PROXY_NAME, WINHTTP_NO_PROXY_BYPASS, 0);
        } else if (_stricmp(cfg->hub_proxy, "system") == 0) {
            h->session = WinHttpOpen(ua, WINHTTP_ACCESS_TYPE_AUTOMATIC_PROXY, WINHTTP_NO_PROXY_NAME, WINHTTP_NO_PROXY_BYPASS, 0);
            h->proxy_kind = 1;
            if (!h->session)
                h->session = WinHttpOpen(ua, WINHTTP_ACCESS_TYPE_DEFAULT_PROXY, WINHTTP_NO_PROXY_NAME, WINHTTP_NO_PROXY_BYPASS, 0);
        } else {
            wchar_t px[256];
            if (alelt_to_wide(cfg->hub_proxy, px, 256) != 0) { snprintf(err, errcap, "bad hub.proxy"); goto fail; }
            h->session = WinHttpOpen(ua, WINHTTP_ACCESS_TYPE_NAMED_PROXY, px, L"<local>", 0);
            h->proxy_kind = 2;
        }
    }
    if (!h->session) {
        snprintf(err, errcap, "WinHttpOpen failed (%lu)", (unsigned long)GetLastError());
        goto fail;
    }
    protos = WINHTTP_FLAG_SECURE_PROTOCOL_TLS1_2 | WINHTTP_FLAG_SECURE_PROTOCOL_TLS1_3;
    if (!WinHttpSetOption(h->session, WINHTTP_OPTION_SECURE_PROTOCOLS, &protos, sizeof(protos))) {
        protos = WINHTTP_FLAG_SECURE_PROTOCOL_TLS1_2; /* older OS without TLS 1.3 */
        if (!WinHttpSetOption(h->session, WINHTTP_OPTION_SECURE_PROTOCOLS, &protos, sizeof(protos))) {
            snprintf(err, errcap, "cannot restrict TLS protocols (%lu)", (unsigned long)GetLastError());
            goto fail;
        }
    }
    h->connect = WinHttpConnect(h->session, h->host, h->port, 0);
    if (!h->connect) {
        snprintf(err, errcap, "WinHttpConnect failed (%lu)", (unsigned long)GetLastError());
        goto fail;
    }
    return h;
fail:
    alelt_http_free(h);
    return NULL;
}

void alelt_http_free(alelt_http *h)
{
    if (!h) return;
    if (h->connect) WinHttpCloseHandle(h->connect);
    if (h->session) WinHttpCloseHandle(h->session);
    if (h->client_cert) CertFreeCertificateContext(h->client_cert);
    free(h);
}

/* SHA-256 over the DER SubjectPublicKeyInfo of the server leaf certificate. */
static int check_pin(alelt_http *h, HINTERNET req, char *err, size_t errcap)
{
    PCCERT_CONTEXT sc = NULL;
    DWORD len = sizeof(sc);
    BYTE *der = NULL;
    DWORD derlen = 0;
    char hex[65];
    int i, ok = 0;
    if (!WinHttpQueryOption(req, WINHTTP_OPTION_SERVER_CERT_CONTEXT, &sc, &len) || !sc) {
        snprintf(err, errcap, "no server certificate to pin (%lu)", (unsigned long)GetLastError());
        return -1;
    }
    if (!CryptEncodeObjectEx(X509_ASN_ENCODING, X509_PUBLIC_KEY_INFO, &sc->pCertInfo->SubjectPublicKeyInfo,
                             CRYPT_ENCODE_ALLOC_FLAG, NULL, &der, &derlen)) {
        CertFreeCertificateContext(sc);
        snprintf(err, errcap, "cannot encode server SPKI (%lu)", (unsigned long)GetLastError());
        return -1;
    }
    CertFreeCertificateContext(sc);
    if (alelt_sha256_hex(der, derlen, hex) != 0) {
        LocalFree(der);
        snprintf(err, errcap, "sha256 failed");
        return -1;
    }
    LocalFree(der);
    for (i = 0; i < h->npins; i++) {
        if (strcmp(hex, h->pins[i]) == 0) ok = 1;
    }
    if (!ok) {
        snprintf(err, errcap, "hub certificate pin mismatch (got %s)", hex);
        return -1;
    }
    return 0;
}

static int add_header(alelt_buf *hb, const char *name, const char *value)
{
    /* Reject CR/LF to prevent header injection. */
    if (strpbrk(value, "\r\n") || strpbrk(name, "\r\n:")) return -1;
    return alelt_buf_printf(hb, "%s: %s\r\n", name, value);
}

int alelt_http_request(alelt_http *h, const char *method, const char *path_query, const char *content_type,
                    const char *const *extra, const void *body, size_t body_len, int timeout_ms,
                    alelt_http_resp *resp)
{
    HINTERNET req = NULL;
    wchar_t wmethod[16], *wpath = NULL, *whdr = NULL;
    char path[1024], token[256];
    alelt_buf hb;
    DWORD opt, status = 0, slen = sizeof(status);
    size_t off = 0;
    int rc = -1;

    resp->status = 0;
    resp->retry_after_s = -1;
    resp->err[0] = 0;
    alelt_buf_reset(&resp->body);
    if (body_len > 0xFFFFFFFFu) {
        snprintf(resp->err, sizeof(resp->err), "body too large");
        return -1;
    }
    alelt_buf_init(&hb, 64 * 1024);
    token[0] = 0;
    if (h->mode == ALELT_HUB_DEV) alelt_cred_get(token, sizeof(token)); /* current credential (rotation-safe) */
    if (_snprintf_s(path, sizeof(path), _TRUNCATE, "%s%s", h->base_path, path_query) < 0 ||
        alelt_to_wide(method, wmethod, 16) != 0 || !(wpath = alelt_to_wide_alloc(path))) {
        snprintf(resp->err, sizeof(resp->err), "bad request line");
        goto out;
    }
    if (add_header(&hb, "X-ALELT-Protocol", "1") || add_header(&hb, "X-ALELT-Agent-Id", h->agent_id) ||
        (content_type && add_header(&hb, "Content-Type", content_type)) ||
        (h->mode == ALELT_HUB_DEV && token[0] && add_header(&hb, "X-ALELT-Agent-Token", token))) {
        snprintf(resp->err, sizeof(resp->err), "bad header");
        goto out;
    }
    for (; extra && extra[0]; extra += 2) {
        if (extra[1] && add_header(&hb, extra[0], extra[1])) {
            snprintf(resp->err, sizeof(resp->err), "bad header");
            goto out;
        }
    }
    whdr = alelt_to_wide_alloc(alelt_buf_cstr(&hb));
    if (!whdr) goto out;

    req = WinHttpOpenRequest(h->connect, wmethod, wpath, NULL, WINHTTP_NO_REFERER, WINHTTP_DEFAULT_ACCEPT_TYPES,
                             h->secure ? WINHTTP_FLAG_SECURE : 0);
    if (!req) {
        snprintf(resp->err, sizeof(resp->err), "WinHttpOpenRequest failed (%lu)", (unsigned long)GetLastError());
        goto out;
    }
    inflight_add(req);
    opt = WINHTTP_DISABLE_REDIRECTS | WINHTTP_DISABLE_COOKIES | WINHTTP_DISABLE_AUTHENTICATION;
    (void)WinHttpSetOption(req, WINHTTP_OPTION_DISABLE_FEATURE, &opt, sizeof(opt));
    if (!WinHttpSetTimeouts(req, h->connect_timeout_ms, h->connect_timeout_ms, timeout_ms, timeout_ms)) goto out;
    if (h->secure) {
        if (h->client_cert) {
            if (!WinHttpSetOption(req, WINHTTP_OPTION_CLIENT_CERT_CONTEXT, (LPVOID)h->client_cert, sizeof(CERT_CONTEXT))) {
                snprintf(resp->err, sizeof(resp->err), "cannot attach client cert (%lu)", (unsigned long)GetLastError());
                goto out;
            }
        } else {
            (void)WinHttpSetOption(req, WINHTTP_OPTION_CLIENT_CERT_CONTEXT, WINHTTP_NO_CLIENT_CERT_CONTEXT, 0);
        }
    }
    /* Headers + TLS handshake only; body follows after the pin check. */
    if (!WinHttpSendRequest(req, whdr, (DWORD)-1L, WINHTTP_NO_REQUEST_DATA, 0, (DWORD)body_len, 0)) {
        snprintf(resp->err, sizeof(resp->err), "send failed (%lu)", (unsigned long)GetLastError());
        goto out;
    }
    if (h->secure && (h->mode == ALELT_HUB_MTLS || h->npins > 0)) {
        if (check_pin(h, req, resp->err, sizeof(resp->err)) != 0) {
            alelt_log_ev(ALELT_LOG_ERROR, "http", resp->err, NULL, NULL, NULL, NULL);
            goto out; /* closing the handle aborts the request before any body byte */
        }
    }
    while (off < body_len) {
        DWORD chunk = (DWORD)((body_len - off) > 65536 ? 65536 : (body_len - off)), wr = 0;
        if (!WinHttpWriteData(req, (const uint8_t *)body + off, chunk, &wr) || wr == 0) {
            snprintf(resp->err, sizeof(resp->err), "write failed (%lu)", (unsigned long)GetLastError());
            goto out;
        }
        off += wr;
    }
    if (!WinHttpReceiveResponse(req, NULL)) {
        snprintf(resp->err, sizeof(resp->err), "no response (%lu)", (unsigned long)GetLastError());
        goto out;
    }
    if (!WinHttpQueryHeaders(req, WINHTTP_QUERY_STATUS_CODE | WINHTTP_QUERY_FLAG_NUMBER, WINHTTP_HEADER_NAME_BY_INDEX,
                             &status, &slen, WINHTTP_NO_HEADER_INDEX)) {
        snprintf(resp->err, sizeof(resp->err), "no status (%lu)", (unsigned long)GetLastError());
        goto out;
    }
    {
        /* XC-16: honour Retry-After (delta-seconds form only; an HTTP date is ignored) */
        wchar_t ra[32];
        DWORD ralen = sizeof(ra);
        if (WinHttpQueryHeaders(req, WINHTTP_QUERY_RETRY_AFTER, WINHTTP_HEADER_NAME_BY_INDEX, ra, &ralen,
                                WINHTTP_NO_HEADER_INDEX)) {
            long v = 0;
            int ok = 1, i;
            for (i = 0; ra[i]; i++) {
                if (ra[i] < L'0' || ra[i] > L'9' || v > 86400) { ok = 0; break; }
                v = v * 10 + (ra[i] - L'0');
            }
            if (ok && i > 0) resp->retry_after_s = (int)v;
        }
    }
    for (;;) {
        DWORD avail = 0, rd = 0;
        if (!WinHttpQueryDataAvailable(req, &avail)) {
            snprintf(resp->err, sizeof(resp->err), "read failed (%lu)", (unsigned long)GetLastError());
            goto out;
        }
        if (avail == 0) break;
        if (avail > 65536) avail = 65536;
        if (alelt_buf_reserve(&resp->body, avail) != 0) {
            snprintf(resp->err, sizeof(resp->err), "response too large");
            goto out;
        }
        if (!WinHttpReadData(req, resp->body.data + resp->body.len, avail, &rd)) {
            snprintf(resp->err, sizeof(resp->err), "read failed (%lu)", (unsigned long)GetLastError());
            goto out;
        }
        if (rd == 0) break;
        resp->body.len += rd;
        resp->body.data[resp->body.len] = 0;
    }
    resp->status = (int)status;
    rc = 0;
out:
    if (req && inflight_remove(req)) WinHttpCloseHandle(req);
    SecureZeroMemory(token, sizeof(token));
    if (whdr) SecureZeroMemory(whdr, wcslen(whdr) * sizeof(wchar_t));
    if (hb.data) SecureZeroMemory(hb.data, hb.len);
    free(wpath);
    free(whdr);
    alelt_buf_free(&hb);
    return rc;
}

int alelt_http_proxy_kind(const alelt_http *h)
{
    return h ? h->proxy_kind : -1;
}
