/* SPDX-License-Identifier: Apache-2.0
 * Windows service lifecycle + built-in installer/uninstaller.
 */
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <aclapi.h>
#include <ntsecapi.h>
#include <sddl.h>

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "agent.h"
#include "devtoken.h"
#include "log.h"
#include "service.h"
#include "guard.h"
#include "version.h"

#define VIRTUAL_ACCOUNT L"NT SERVICE\\" ALELT_SERVICE_NAME_W
#define EVT_KEY L"SYSTEM\\CurrentControlSet\\Services\\EventLog\\Application\\" ALELT_SERVICE_NAME_W

static void say(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stdout, fmt, ap);
    va_end(ap);
    fputc('\n', stdout);
    fflush(stdout);
}

static void say_err(const char *what)
{
    fprintf(stderr, "error: %s (Win32 error %lu)\n", what, (unsigned long)GetLastError());
}

/* ------------------------------------------------------------------ ACLs */
static int sid_string_for(const wchar_t *account, wchar_t *out, size_t cap)
{
    BYTE sid[SECURITY_MAX_SID_SIZE];
    DWORD sl = sizeof(sid);
    wchar_t dom[256];
    DWORD dl = 256;
    SID_NAME_USE use;
    LPWSTR s = NULL;
    int r;
    if (!LookupAccountNameW(NULL, account, sid, &sl, dom, &dl, &use)) return -1;
    if (!ConvertSidToStringSidW(sid, &s)) return -1;
    r = alelt_wcscpy(out, cap, s);
    LocalFree(s);
    return r;
}

static int apply_sddl(const wchar_t *path, const wchar_t *sddl)
{
    PSECURITY_DESCRIPTOR sd = NULL;
    PACL dacl = NULL;
    BOOL present = FALSE, def = FALSE;
    DWORD r;
    if (!ConvertStringSecurityDescriptorToSecurityDescriptorW(sddl, SDDL_REVISION_1, &sd, NULL)) return -1;
    if (!GetSecurityDescriptorDacl(sd, &present, &dacl, &def) || !present) {
        LocalFree(sd);
        return -1;
    }
    r = SetNamedSecurityInfoW((LPWSTR)path, SE_FILE_OBJECT, DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                              NULL, NULL, dacl, NULL);
    LocalFree(sd);
    SetLastError(r);
    return r == ERROR_SUCCESS ? 0 : -1;
}

int alelt_secure_paths(const wchar_t *config_path, const wchar_t *account)
{
    wchar_t svc[192] = L"", acc[192] = L"", sddl[1024], dir[ALELT_PATH_MAX], *slash, tok[ALELT_PATH_MAX];
    alelt_config cfg;
    char err[512];
    int rc = 0;
    if (sid_string_for(VIRTUAL_ACCOUNT, svc, 192) != 0) svc[0] = 0;
    if (account && *account && _wcsicmp(account, VIRTUAL_ACCOUNT) != 0 && _wcsicmp(account, L"LocalSystem") != 0) {
        if (sid_string_for(account, acc, 192) != 0) {
            say_err("cannot resolve service account SID");
            return -1;
        }
    }
    /* config directory: admins/system full, service read (inherited by new files) */
    alelt_wcscpy(dir, ALELT_PATH_MAX, config_path);
    slash = wcsrchr(dir, L'\\');
    if (slash) {
        *slash = 0;
        _snwprintf_s(sddl, 1024, _TRUNCATE, L"D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)%ls%ls%ls%ls%ls%ls", svc[0] ? L"(A;OICI;FR;;;" : L"",
                     svc, svc[0] ? L")" : L"", acc[0] ? L"(A;OICI;FR;;;" : L"", acc, acc[0] ? L")" : L"");
        if (apply_sddl(dir, sddl) != 0) { say_err("cannot set ACL on config directory"); rc = -1; }
    }
    _snwprintf_s(sddl, 1024, _TRUNCATE, L"D:P(A;;FA;;;SY)(A;;FA;;;BA)%ls%ls%ls%ls%ls%ls", svc[0] ? L"(A;;FR;;;" : L"", svc,
                 svc[0] ? L")" : L"", acc[0] ? L"(A;;FR;;;" : L"", acc, acc[0] ? L")" : L"");
    if (apply_sddl(config_path, sddl) != 0) { say_err("cannot set ACL on config file"); rc = -1; }
    if (alelt_dev_token_path(config_path, tok, ALELT_PATH_MAX) == 0 && GetFileAttributesW(tok) != INVALID_FILE_ATTRIBUTES) {
        if (apply_sddl(tok, sddl) != 0) { say_err("cannot set ACL on devtoken.bin"); rc = -1; }
    }
    /* data directories: service account modify. AHS-09: when the config is not loadable yet the default
     * directories are secured anyway, so the spool / log are never left with inherited Users access. */
    {
        wchar_t defs[2][ALELT_PATH_MAX];
        const wchar_t *dirs[2] = {NULL, NULL};
        int i, loaded = alelt_config_load(config_path, &cfg, err, sizeof(err)) == 0;
        if (loaded) {
            dirs[0] = cfg.spool_dir;
            dirs[1] = cfg.log_dir;
        } else {
            say("warning: config not loadable yet (%s); securing the default data directories", err);
            if (alelt_config_default_data_dir(defs[0], ALELT_PATH_MAX, L"spool") == 0) dirs[0] = defs[0];
            if (alelt_config_default_data_dir(defs[1], ALELT_PATH_MAX, L"log") == 0) dirs[1] = defs[1];
        }
        _snwprintf_s(sddl, 1024, _TRUNCATE, L"D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)%ls%ls%ls%ls%ls%ls", svc[0] ? L"(A;OICI;0x1301bf;;;" : L"",
                     svc, svc[0] ? L")" : L"", acc[0] ? L"(A;OICI;0x1301bf;;;" : L"", acc, acc[0] ? L")" : L"");
        for (i = 0; i < 2; i++) {
            if (!dirs[i]) continue;
            if (alelt_mkdirs(dirs[i]) != 0 || apply_sddl(dirs[i], sddl) != 0) {
                say_err("cannot create/secure a data directory");
                rc = -1;
            }
        }
        if (loaded) alelt_config_free(&cfg);
    }
    return rc;
}

/* ------------------------------------------------------------------ install helpers */
static int is_builtin_account(const wchar_t *a)
{
    return !a || !*a || _wcsicmp(a, L"LocalSystem") == 0 || _wcsnicmp(a, L"NT SERVICE\\", 11) == 0 ||
           _wcsnicmp(a, L"NT AUTHORITY\\", 13) == 0;
}

static int grant_logon_as_service(const wchar_t *account)
{
    BYTE sid[SECURITY_MAX_SID_SIZE];
    DWORD sl = sizeof(sid);
    wchar_t dom[256];
    DWORD dl = 256;
    SID_NAME_USE use;
    LSA_OBJECT_ATTRIBUTES oa;
    LSA_HANDLE pol = NULL;
    LSA_UNICODE_STRING right;
    NTSTATUS st;
    static wchar_t name[] = SE_SERVICE_LOGON_NAME;
    if (!LookupAccountNameW(NULL, account, sid, &sl, dom, &dl, &use)) return -1;
    ZeroMemory(&oa, sizeof(oa));
    if (LsaOpenPolicy(NULL, &oa, POLICY_CREATE_ACCOUNT | POLICY_LOOKUP_NAMES, &pol) != 0) return -1;
    right.Buffer = name;
    right.Length = (USHORT)(wcslen(name) * sizeof(wchar_t));
    right.MaximumLength = (USHORT)(right.Length + sizeof(wchar_t));
    st = LsaAddAccountRights(pol, (PSID)sid, &right, 1);
    LsaClose(pol);
    return st == 0 ? 0 : -1;
}

static int register_event_source(const wchar_t *exe)
{
    HKEY k;
    DWORD types = EVENTLOG_ERROR_TYPE | EVENTLOG_WARNING_TYPE | EVENTLOG_INFORMATION_TYPE;
    LSTATUS r = RegCreateKeyExW(HKEY_LOCAL_MACHINE, EVT_KEY, 0, NULL, 0, KEY_SET_VALUE, NULL, &k, NULL);
    if (r != ERROR_SUCCESS) return -1;
    r = RegSetValueExW(k, L"EventMessageFile", 0, REG_EXPAND_SZ, (const BYTE *)exe, (DWORD)((wcslen(exe) + 1) * sizeof(wchar_t)));
    if (r == ERROR_SUCCESS) r = RegSetValueExW(k, L"TypesSupported", 0, REG_DWORD, (const BYTE *)&types, sizeof(types));
    RegCloseKey(k);
    return r == ERROR_SUCCESS ? 0 : -1;
}

static int configure_service(SC_HANDLE s)
{
    SERVICE_DESCRIPTIONW d;
    SERVICE_DELAYED_AUTO_START_INFO da;
    SERVICE_FAILURE_ACTIONSW fa;
    SERVICE_FAILURE_ACTIONS_FLAG ff;
    SERVICE_SID_INFO si;
    SC_ACTION act[3];
    static wchar_t desc[] = ALELT_SERVICE_DESC_W;
    int ok = 1;
    d.lpDescription = desc;
    ok &= ChangeServiceConfig2W(s, SERVICE_CONFIG_DESCRIPTION, &d) ? 1 : 0;
    da.fDelayedAutostart = TRUE; /* start after Db2 and the network are up */
    ok &= ChangeServiceConfig2W(s, SERVICE_CONFIG_DELAYED_AUTO_START_INFO, &da) ? 1 : 0;
    act[0].Type = SC_ACTION_RESTART; act[0].Delay = 60 * 1000;
    act[1].Type = SC_ACTION_RESTART; act[1].Delay = 120 * 1000;
    act[2].Type = SC_ACTION_RESTART; act[2].Delay = 300 * 1000;
    ZeroMemory(&fa, sizeof(fa));
    fa.dwResetPeriod = 24 * 3600;
    fa.cActions = 3;
    fa.lpsaActions = act;
    ok &= ChangeServiceConfig2W(s, SERVICE_CONFIG_FAILURE_ACTIONS, &fa) ? 1 : 0;
    ff.fFailureActionsOnNonCrashFailures = TRUE; /* non-zero exit code also restarts */
    ok &= ChangeServiceConfig2W(s, SERVICE_CONFIG_FAILURE_ACTIONS_FLAG, &ff) ? 1 : 0;
    si.dwServiceSidType = SERVICE_SID_TYPE_UNRESTRICTED;
    ok &= ChangeServiceConfig2W(s, SERVICE_CONFIG_SERVICE_SID_INFO, &si) ? 1 : 0;
    return ok ? 0 : -1;
}

int alelt_svc_post_install(const wchar_t *config_path, const wchar_t *account)
{
    const wchar_t *acc = (account && *account) ? account : VIRTUAL_ACCOUNT;
    wchar_t exe[ALELT_PATH_MAX];
    int rc = 0;
    if (!is_builtin_account(acc) && grant_logon_as_service(acc) != 0) {
        say_err("could not grant 'Log on as a service'");
        rc = 1;
    }
    if (GetModuleFileNameW(NULL, exe, ALELT_PATH_MAX) && register_event_source(exe) != 0) {
        say_err("Event Log source registration failed");
        rc = 1;
    }
    if (alelt_secure_paths(config_path, acc) != 0) rc = 1;
    return rc;
}

int alelt_svc_install(const alelt_install_opts *o)
{
    wchar_t exe[ALELT_PATH_MAX], bin[ALELT_PATH_MAX * 2 + 64];
    const wchar_t *account = (o->account && *o->account) ? o->account : VIRTUAL_ACCOUNT;
    SC_HANDLE scm, s;
    char err[512];
    int r;
    if (!GetModuleFileNameW(NULL, exe, ALELT_PATH_MAX)) return 1;
    if (_snwprintf_s(bin, sizeof(bin) / sizeof(bin[0]), _TRUNCATE, L"\"%ls\" --service --config \"%ls\"", exe, o->config_path) < 0)
        return 1;
    r = alelt_config_write_initial(o->config_path, &o->init, err, sizeof(err));
    if (r < 0) {
        fprintf(stderr, "error: %s\n", err);
        return 1;
    }
    say(r == 1 ? "config kept: %ls" : "config created: %ls (edit before starting)", o->config_path);

    if (o->start_after) {
        /* AHS-09: never install-and-start a service whose config does not load (it would only exit and be
         * restarted). Checked BEFORE the SCM is touched: nothing is registered for an invalid config. */
        static alelt_config chk;
        if (alelt_config_load(o->config_path, &chk, err, sizeof(err)) != 0) {
            say("service NOT installed: the config is not valid yet (%s). Edit %ls, then run --install again "
                "(or --install --no-start to register it now)", err, o->config_path);
            return 2;
        }
        alelt_config_free(&chk);
    }

    scm = OpenSCManagerW(NULL, NULL, SC_MANAGER_CONNECT | SC_MANAGER_CREATE_SERVICE);
    if (!scm) { say_err("OpenSCManager (run elevated)"); return 1; }
    s = CreateServiceW(scm, ALELT_SERVICE_NAME_W, ALELT_SERVICE_DISPLAY_W, SERVICE_ALL_ACCESS, SERVICE_WIN32_OWN_PROCESS,
                       SERVICE_AUTO_START, SERVICE_ERROR_NORMAL, bin, NULL, NULL, NULL, account, o->password);
    if (!s && GetLastError() == ERROR_SERVICE_EXISTS) {
        s = OpenServiceW(scm, ALELT_SERVICE_NAME_W, SERVICE_ALL_ACCESS);
        if (s && !ChangeServiceConfigW(s, SERVICE_WIN32_OWN_PROCESS, SERVICE_AUTO_START, SERVICE_ERROR_NORMAL, bin, NULL, NULL,
                                       NULL, account, o->password, ALELT_SERVICE_DISPLAY_W)) {
            say_err("ChangeServiceConfig");
            CloseServiceHandle(s);
            CloseServiceHandle(scm);
            return 1;
        }
        say("service existed: configuration updated (upgrade; spool and positions kept)");
    }
    if (!s) {
        say_err("CreateService");
        CloseServiceHandle(scm);
        return 1;
    }
    if (configure_service(s) != 0) say_err("warning: some service options could not be set");
    if (!is_builtin_account(account)) {
        if (grant_logon_as_service(account) != 0) say("warning: could not grant 'Log on as a service' to the account");
    }
    if (register_event_source(exe) != 0) say_err("warning: Event Log source registration failed");
    if (alelt_secure_paths(o->config_path, account) != 0) say("warning: some ACLs could not be applied");
    say("service %ls installed (account %ls)", ALELT_SERVICE_NAME_W, account);
    if (o->start_after) {
        if (!StartServiceW(s, 0, NULL) && GetLastError() != ERROR_SERVICE_ALREADY_RUNNING) say_err("StartService");
        else say("service started");
    }
    CloseServiceHandle(s);
    CloseServiceHandle(scm);
    return 0;
}

static int wait_state(SC_HANDLE s, DWORD want, DWORD timeout_ms)
{
    SERVICE_STATUS_PROCESS sp;
    DWORD need, t0 = GetTickCount();
    for (;;) {
        if (!QueryServiceStatusEx(s, SC_STATUS_PROCESS_INFO, (LPBYTE)&sp, sizeof(sp), &need)) return -1;
        if (sp.dwCurrentState == want) return 0;
        if (GetTickCount() - t0 > timeout_ms) return -1;
        Sleep(500);
    }
}

int alelt_svc_stop(void)
{
    SC_HANDLE scm, s;
    SERVICE_STATUS st;
    int rc = 0;
    scm = OpenSCManagerW(NULL, NULL, SC_MANAGER_CONNECT);
    if (!scm) { say_err("OpenSCManager"); return 1; }
    s = OpenServiceW(scm, ALELT_SERVICE_NAME_W, SERVICE_STOP | SERVICE_QUERY_STATUS);
    if (!s) { say_err("OpenService"); CloseServiceHandle(scm); return 1; }
    if (!ControlService(s, SERVICE_CONTROL_STOP, &st) && GetLastError() != ERROR_SERVICE_NOT_ACTIVE) {
        say_err("ControlService(STOP)");
        rc = 1;
    } else if (wait_state(s, SERVICE_STOPPED, 200000) != 0) { /* worst case: 5 s abort grace + joins (AHS-23) */
        say("service did not reach STOPPED within 200 s");
        rc = 1;
    } else {
        say("service stopped");
    }
    CloseServiceHandle(s);
    CloseServiceHandle(scm);
    return rc;
}

int alelt_svc_start(void)
{
    SC_HANDLE scm, s;
    int rc = 0;
    scm = OpenSCManagerW(NULL, NULL, SC_MANAGER_CONNECT);
    if (!scm) { say_err("OpenSCManager"); return 1; }
    s = OpenServiceW(scm, ALELT_SERVICE_NAME_W, SERVICE_START | SERVICE_QUERY_STATUS);
    if (!s) { say_err("OpenService"); CloseServiceHandle(scm); return 1; }
    if (!StartServiceW(s, 0, NULL) && GetLastError() != ERROR_SERVICE_ALREADY_RUNNING) {
        say_err("StartService");
        rc = 1;
    } else if (wait_state(s, SERVICE_RUNNING, 60000) != 0) {
        say("service did not reach RUNNING within 60 s (see log / Event Log)");
        rc = 1;
    } else {
        say("service running");
    }
    CloseServiceHandle(s);
    CloseServiceHandle(scm);
    return rc;
}

int alelt_svc_uninstall(void)
{
    SC_HANDLE scm, s;
    SERVICE_STATUS st;
    int rc = 0;
    scm = OpenSCManagerW(NULL, NULL, SC_MANAGER_CONNECT);
    if (!scm) { say_err("OpenSCManager (run elevated)"); return 1; }
    s = OpenServiceW(scm, ALELT_SERVICE_NAME_W, SERVICE_STOP | SERVICE_QUERY_STATUS | DELETE);
    if (s) {
        if (ControlService(s, SERVICE_CONTROL_STOP, &st)) (void)wait_state(s, SERVICE_STOPPED, 60000);
        if (!DeleteService(s) && GetLastError() != ERROR_SERVICE_MARKED_FOR_DELETE) {
            say_err("DeleteService");
            rc = 1;
        } else {
            say("service removed");
        }
        CloseServiceHandle(s);
    } else if (GetLastError() == ERROR_SERVICE_DOES_NOT_EXIST) {
        say("service not installed");
    } else {
        say_err("OpenService");
        rc = 1;
    }
    CloseServiceHandle(scm);
    if (RegDeleteKeyW(HKEY_LOCAL_MACHINE, EVT_KEY) == ERROR_SUCCESS) say("Event Log source removed");
    say("config, spool and logs are kept (remove with uninstall-agent.ps1 -Purge)");
    return rc;
}

int alelt_svc_status(void)
{
    SC_HANDLE scm, s;
    SERVICE_STATUS_PROCESS sp;
    DWORD need = 0;
    const char *names[] = {"?", "STOPPED", "START_PENDING", "STOP_PENDING", "RUNNING", "CONTINUE_PENDING", "PAUSE_PENDING", "PAUSED"};
    scm = OpenSCManagerW(NULL, NULL, SC_MANAGER_CONNECT);
    if (!scm) { say_err("OpenSCManager"); return 1; }
    s = OpenServiceW(scm, ALELT_SERVICE_NAME_W, SERVICE_QUERY_STATUS);
    if (!s) {
        say("service %ls: not installed", ALELT_SERVICE_NAME_W);
        CloseServiceHandle(scm);
        return 2;
    }
    ZeroMemory(&sp, sizeof(sp));
    if (!QueryServiceStatusEx(s, SC_STATUS_PROCESS_INFO, (LPBYTE)&sp, sizeof(sp), &need)) {
        say_err("QueryServiceStatusEx");
    } else {
        say("service %ls: %s pid=%lu exit=%lu specific=%lu", ALELT_SERVICE_NAME_W,
            sp.dwCurrentState <= 7 ? names[sp.dwCurrentState] : "?", (unsigned long)sp.dwProcessId,
            (unsigned long)sp.dwWin32ExitCode, (unsigned long)sp.dwServiceSpecificExitCode);
    }
    CloseServiceHandle(s);
    CloseServiceHandle(scm);
    return sp.dwCurrentState == SERVICE_RUNNING ? 0 : 3;
}

/* ------------------------------------------------------------------ SCM runtime */
static SERVICE_STATUS_HANDLE g_sh = NULL;
static SERVICE_STATUS g_st;
static HANDLE g_stop = NULL;
static wchar_t g_cfg[ALELT_PATH_MAX];
static DWORD g_checkpoint = 1;

static void report(DWORD state, DWORD exit_code, DWORD wait_hint)
{
    g_st.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    g_st.dwCurrentState = state;
    g_st.dwControlsAccepted = (state == SERVICE_RUNNING) ? (SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_PRESHUTDOWN) : 0;
    if (exit_code) {
        g_st.dwWin32ExitCode = ERROR_SERVICE_SPECIFIC_ERROR;
        g_st.dwServiceSpecificExitCode = exit_code;
    } else {
        g_st.dwWin32ExitCode = NO_ERROR;
        g_st.dwServiceSpecificExitCode = 0;
    }
    g_st.dwWaitHint = wait_hint;
    g_st.dwCheckPoint = (state == SERVICE_RUNNING || state == SERVICE_STOPPED) ? 0 : g_checkpoint++;
    if (g_sh) SetServiceStatus(g_sh, &g_st);
}

static DWORD WINAPI ctrl_handler(DWORD ctrl, DWORD type, LPVOID data, LPVOID ctx)
{
    (void)type;
    (void)data;
    (void)ctx;
    switch (ctrl) {
    case SERVICE_CONTROL_STOP:
    case SERVICE_CONTROL_PRESHUTDOWN:
    case SERVICE_CONTROL_SHUTDOWN:
        report(SERVICE_STOP_PENDING, 0, 60000); /* checkpoints follow while threads are joined (AHS-23) */
        SetEvent(g_stop);
        return NO_ERROR;
    case SERVICE_CONTROL_INTERROGATE:
        return NO_ERROR;
    default:
        return ERROR_CALL_NOT_IMPLEMENTED;
    }
}

static void on_stop_progress(void)
{
    report(SERVICE_STOP_PENDING, 0, 10000); /* AHS-23: SCM sees progress while threads are joined */
}

static void on_running(void)
{
    report(SERVICE_RUNNING, 0, 0);
}

static void WINAPI svc_main(DWORD argc, LPWSTR *argv)
{
    int rc;
    (void)argc;
    (void)argv;
    g_sh = RegisterServiceCtrlHandlerExW(ALELT_SERVICE_NAME_W, ctrl_handler, NULL);
    if (!g_sh) return;
    report(SERVICE_START_PENDING, 0, 30000);
    g_stop = CreateEventW(NULL, TRUE, FALSE, NULL);
    if (!g_stop) {
        report(SERVICE_STOPPED, 1, 0);
        return;
    }
    alelt_agent_on_stop_progress(on_stop_progress);
    rc = alelt_agent_run(g_cfg, g_stop, 0, on_running);
    /* REQ-12 / AHS-09: a production-guard refusal or an invalid config must not be retried by the failure
     * actions (restart on non-zero exit, every 60/120/300 s forever): report a clean stop; the reason is in
     * the agent log and the Windows Event Log. */
    report(SERVICE_STOPPED, (rc == ALELT_EXIT_GUARD || rc == ALELT_EXIT_CONFIG) ? 0 : (DWORD)rc, 0);
}

int alelt_svc_dispatch(const wchar_t *config_path)
{
    static wchar_t name[] = ALELT_SERVICE_NAME_W;
    SERVICE_TABLE_ENTRYW t[2];
    alelt_wcscpy(g_cfg, ALELT_PATH_MAX, config_path);
    t[0].lpServiceName = name;
    t[0].lpServiceProc = svc_main;
    t[1].lpServiceName = NULL;
    t[1].lpServiceProc = NULL;
    if (!StartServiceCtrlDispatcherW(t)) {
        if (GetLastError() == ERROR_FAILED_SERVICE_CONTROLLER_CONNECT) {
            fprintf(stderr, "--service is for the Service Control Manager; use --console to run interactively\n");
        }
        return 1;
    }
    return 0;
}
