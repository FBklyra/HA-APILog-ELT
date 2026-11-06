/* SPDX-License-Identifier: Apache-2.0
 * aleltagent.exe - ALELT edge agent entry point.
 */
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

#include <stdio.h>
#include <string.h>

#include "agent.h"
#include "config.h"
#include "devtoken.h"
#include "log.h"
#include "service.h"
#include "version.h"

static HANDLE g_console_stop = NULL;

static BOOL WINAPI console_ctrl(DWORD type)
{
    (void)type;
    if (g_console_stop) SetEvent(g_console_stop);
    return TRUE;
}

static void usage(void)
{
    printf("ALELT agent %s\n\n"
           "  aleltagent.exe --install [--account <user>] [--password-stdin] [--config <path>] [--no-start]\n"
           "              [--hub-url <url>] [--agent-id <id>] [--connection-id <id>] [--thumbprint <sha1>]\n"
           "              [--pin <sha256>] [--spool-dir <dir>] [--log-dir <dir>] [--database <db>] [--mode mtls|dev]\n"
           "  aleltagent.exe --init-config [same config options]   write a starter config (never overwrites)\n"
           "  aleltagent.exe --uninstall | --start | --stop | --status\n"
           "  aleltagent.exe --console [--config <path>]          run in the foreground (Ctrl+C stops)\n"
           "  aleltagent.exe --check-config [--config <path>]\n"
           "  aleltagent.exe --set-dev-token [--config <path>]    reads the dev token from stdin (DPAPI)\n"
           "  aleltagent.exe --version\n\n"
           "Default config: %%ProgramData%%\\ALELT\\Agent\\aleltagent.json\n",
           ALELT_AGENT_VERSION);
}

static int read_stdin_line(char *out, size_t cap)
{
    size_t n;
    if (!fgets(out, (int)cap, stdin)) return -1;
    n = strlen(out);
    while (n > 0 && (out[n - 1] == '\n' || out[n - 1] == '\r')) out[--n] = 0;
    return 0;
}

int wmain(int argc, wchar_t **argv)
{
    enum { NONE, INSTALL, INITCFG, UNINSTALL, START, STOP, STATUS, CONSOLE, SERVICE, CHECK, SETTOKEN, VERSION, POSTINSTALL } mode = NONE;
    wchar_t cfgpath[ALELT_PATH_MAX] = L"", full[ALELT_PATH_MAX];
    const wchar_t *account = NULL;
    int pw_stdin = 0, no_start = 0, i, rc;
    char u8[9][512];
    alelt_install_opts io;
    memset(&io, 0, sizeof(io));
    memset(u8, 0, sizeof(u8));

    for (i = 1; i < argc; i++) {
        const wchar_t *a = argv[i];
        const wchar_t *val = (i + 1 < argc) ? argv[i + 1] : NULL;
        int slot = -1;
        if (!wcscmp(a, L"--install")) mode = INSTALL;
        else if (!wcscmp(a, L"--init-config")) mode = INITCFG;
        else if (!wcscmp(a, L"--uninstall")) mode = UNINSTALL;
        else if (!wcscmp(a, L"--start")) mode = START;
        else if (!wcscmp(a, L"--stop")) mode = STOP;
        else if (!wcscmp(a, L"--status")) mode = STATUS;
        else if (!wcscmp(a, L"--console")) mode = CONSOLE;
        else if (!wcscmp(a, L"--service")) mode = SERVICE;
        else if (!wcscmp(a, L"--check-config")) mode = CHECK;
        else if (!wcscmp(a, L"--set-dev-token")) mode = SETTOKEN;
        else if (!wcscmp(a, L"--version")) mode = VERSION;
        else if (!wcscmp(a, L"--post-install")) mode = POSTINSTALL;
        else if (!wcscmp(a, L"--password-stdin")) pw_stdin = 1;
        else if (!wcscmp(a, L"--no-start")) no_start = 1;
        else if (!wcscmp(a, L"--config") && val) { alelt_wcscpy(cfgpath, ALELT_PATH_MAX, val); i++; }
        else if (!wcscmp(a, L"--account") && val) { account = val; i++; }
        else if (!wcscmp(a, L"--hub-url")) slot = 0;
        else if (!wcscmp(a, L"--agent-id")) slot = 1;
        else if (!wcscmp(a, L"--connection-id")) slot = 2;
        else if (!wcscmp(a, L"--thumbprint")) slot = 3;
        else if (!wcscmp(a, L"--pin")) slot = 4;
        else if (!wcscmp(a, L"--spool-dir")) slot = 5;
        else if (!wcscmp(a, L"--log-dir")) slot = 6;
        else if (!wcscmp(a, L"--database")) slot = 7;
        else if (!wcscmp(a, L"--mode")) slot = 8;
        else { usage(); return 1; }
        if (slot >= 0) {
            if (!val || alelt_to_utf8(val, u8[slot], sizeof(u8[slot])) != 0) { usage(); return 1; }
            i++;
        }
    }
    io.init.hub_url = u8[0];
    io.init.agent_id = u8[1];
    io.init.connection_id = u8[2];
    io.init.thumbprint = u8[3];
    io.init.pin = u8[4];
    io.init.spool_dir = u8[5];
    io.init.log_dir = u8[6];
    io.init.database = u8[7];
    io.init.mode = u8[8];

    if (mode == NONE) { usage(); return 1; }
    if (mode == VERSION) { printf("%s\n", ALELT_AGENT_VERSION); return 0; }
    if (!cfgpath[0] && alelt_config_default_path(cfgpath, ALELT_PATH_MAX) != 0) {
        fprintf(stderr, "error: cannot resolve default config path\n");
        return 1;
    }
    if (!GetFullPathNameW(cfgpath, ALELT_PATH_MAX, full, NULL)) {
        fprintf(stderr, "error: bad config path\n");
        return 1;
    }

    switch (mode) {
    case INSTALL: {
        char pw[256] = "";
        wchar_t wpw[256] = L"";
        io.config_path = full;
        io.account = account;
        io.start_after = !no_start;
        if (pw_stdin) {
            if (read_stdin_line(pw, sizeof(pw)) != 0 || alelt_to_wide(pw, wpw, 256) != 0) {
                fprintf(stderr, "error: cannot read password from stdin\n");
                return 1;
            }
            io.password = wpw;
        }
        rc = alelt_svc_install(&io);
        SecureZeroMemory(pw, sizeof(pw));
        SecureZeroMemory(wpw, sizeof(wpw));
        return rc;
    }
    case INITCFG: {
        char err[512];
        rc = alelt_config_write_initial(full, &io.init, err, sizeof(err));
        if (rc < 0) { fprintf(stderr, "error: %s\n", err); return 1; }
        printf(rc == 1 ? "config exists, kept: %ls\n" : "config written: %ls\n", full);
        return 0;
    }
    case UNINSTALL: return alelt_svc_uninstall();
    case POSTINSTALL: return alelt_svc_post_install(full, account);
    case START: return alelt_svc_start();
    case STOP: return alelt_svc_stop();
    case STATUS: return alelt_svc_status();
    case SERVICE: return alelt_svc_dispatch(full);
    case CHECK: {
        alelt_config c;
        char err[512];
        if (alelt_config_load(full, &c, err, sizeof(err)) != 0) { fprintf(stderr, "invalid: %s\n", err); return 1; }
        printf("config OK: agent_id=%s connection_id=%s hub=%s mode=%s plugin=%s database=%s allowed_tables=%d\n",
               c.agent_id, c.connection_id, c.hub_url, c.hub_mode == ALELT_HUB_MTLS ? "mtls" : "dev", c.plugin, c.database,
               c.nallowed);
        alelt_config_free(&c);
        return 0;
    }
    case SETTOKEN: {
        char tok[256], err[512];
        fprintf(stderr, "dev token (stdin): ");
        if (read_stdin_line(tok, sizeof(tok)) != 0) return 1;
        rc = alelt_dev_token_save(full, tok, err, sizeof(err));
        SecureZeroMemory(tok, sizeof(tok));
        if (rc != 0) { fprintf(stderr, "error: %s\n", err); return 1; }
        (void)alelt_secure_paths(full, account);
        fprintf(stderr, "\ndev token stored (DPAPI, machine scope)\n");
        return 0;
    }
    case CONSOLE:
        g_console_stop = CreateEventW(NULL, TRUE, FALSE, NULL);
        if (!g_console_stop) return 1;
        SetConsoleCtrlHandler(console_ctrl, TRUE);
        rc = alelt_agent_run(full, g_console_stop, 1, NULL);
        CloseHandle(g_console_stop);
        return rc;
    default:
        usage();
        return 1;
    }
}
