/* SPDX-License-Identifier: Apache-2.0 */
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <psapi.h>

#include <stdio.h>

#include "jobobj.h"
#include "log.h"

static HANDLE g_job = NULL;
static SRWLOCK g_usage_lock = SRWLOCK_INIT;

int alelt_job_apply(const alelt_limits *l, char *err, size_t errcap)
{
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION ext;
    JOBOBJECT_CPU_RATE_CONTROL_INFORMATION cpu;
    SYSTEM_INFO si;
    DWORD ncpu, rate;
    int fresh = 0;

    if (!g_job) {
        g_job = CreateJobObjectW(NULL, NULL);
        if (!g_job) {
            snprintf(err, errcap, "CreateJobObject failed (%lu)", (unsigned long)GetLastError());
            return -1;
        }
        fresh = 1;
    }
    ZeroMemory(&ext, sizeof(ext));
    ext.BasicLimitInformation.LimitFlags =
        JOB_OBJECT_LIMIT_JOB_MEMORY | JOB_OBJECT_LIMIT_PRIORITY_CLASS | JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION;
    ext.BasicLimitInformation.PriorityClass = BELOW_NORMAL_PRIORITY_CLASS;
    ext.JobMemoryLimit = (SIZE_T)l->mem_mb * 1024u * 1024u;
    if (!SetInformationJobObject(g_job, JobObjectExtendedLimitInformation, &ext, sizeof(ext))) {
        snprintf(err, errcap, "job memory/priority limit failed (%lu)", (unsigned long)GetLastError());
        return -1;
    }
    GetSystemInfo(&si);
    ncpu = si.dwNumberOfProcessors ? si.dwNumberOfProcessors : 1;
    /* CpuRate is in 1/100 % of ALL processors: one core of N = 10000/N. */
    rate = (DWORD)(((uint64_t)(unsigned)l->cpu_pct * 100u) / ncpu);
    if (rate < 1) rate = 1;
    if (rate > 10000) rate = 10000;
    ZeroMemory(&cpu, sizeof(cpu));
    cpu.ControlFlags = JOB_OBJECT_CPU_RATE_CONTROL_ENABLE | JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP;
    cpu.CpuRate = rate;
    if (!SetInformationJobObject(g_job, JobObjectCpuRateControlInformation, &cpu, sizeof(cpu))) {
        snprintf(err, errcap, "job CPU rate cap failed (%lu)", (unsigned long)GetLastError());
        return -1;
    }
    if (fresh) {
        if (!AssignProcessToJobObject(g_job, GetCurrentProcess())) {
            snprintf(err, errcap, "AssignProcessToJobObject failed (%lu)", (unsigned long)GetLastError());
            return -1;
        }
    }
    (void)SetPriorityClass(GetCurrentProcess(), BELOW_NORMAL_PRIORITY_CLASS);
    alelt_logf(ALELT_LOG_INFO, "job", "job limits applied: cpu_pct=%d (CpuRate=%lu of %lu cpus) mem_mb=%d priority=BELOW_NORMAL",
            l->cpu_pct, (unsigned long)rate, (unsigned long)ncpu, l->mem_mb);
    return 0;
}

void alelt_job_close(void)
{
    if (g_job) {
        CloseHandle(g_job);
        g_job = NULL;
    }
}

static uint64_t ft64(FILETIME f)
{
    return ((uint64_t)f.dwHighDateTime << 32) | f.dwLowDateTime;
}

void alelt_self_usage(double *cpu_pct, double *mem_mb)
{
    static uint64_t last_cpu = 0, last_wall = 0;
    FILETIME c, e, k, u, now;
    PROCESS_MEMORY_COUNTERS_EX pm;
    *cpu_pct = 0.0;
    *mem_mb = 0.0;
    AcquireSRWLockExclusive(&g_usage_lock);
    GetSystemTimeAsFileTime(&now);
    if (GetProcessTimes(GetCurrentProcess(), &c, &e, &k, &u)) {
        uint64_t cpu = ft64(k) + ft64(u), wall = ft64(now);
        if (last_wall && wall > last_wall) *cpu_pct = 100.0 * (double)(cpu - last_cpu) / (double)(wall - last_wall);
        last_cpu = cpu;
        last_wall = wall;
    }
    ReleaseSRWLockExclusive(&g_usage_lock);
    ZeroMemory(&pm, sizeof(pm));
    pm.cb = sizeof(pm);
    if (GetProcessMemoryInfo(GetCurrentProcess(), (PROCESS_MEMORY_COUNTERS *)&pm, sizeof(pm))) {
        *mem_mb = (double)pm.PrivateUsage / (1024.0 * 1024.0);
    }
}

double alelt_host_cpu_pct(void)
{
    static uint64_t li = 0, lk = 0, lu = 0;
    FILETIME i, k, u;
    double r = -1.0;
    if (!GetSystemTimes(&i, &k, &u)) return -1.0;
    AcquireSRWLockExclusive(&g_usage_lock);
    if (lk || lu) {
        uint64_t di = ft64(i) - li, dk = ft64(k) - lk, du = ft64(u) - lu;
        uint64_t total = dk + du; /* kernel time includes idle */
        if (total > 0) r = 100.0 * (double)(total - di) / (double)total;
    }
    li = ft64(i);
    lk = ft64(k);
    lu = ft64(u);
    ReleaseSRWLockExclusive(&g_usage_lock);
    return r;
}
