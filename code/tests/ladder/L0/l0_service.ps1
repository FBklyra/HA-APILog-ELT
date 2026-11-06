<#
.SYNOPSIS
  ALELT L0 - agent service lifecycle: install, start, register/heartbeat (via l0_link.py), no listening
  ports, restart, stop, uninstall. No Db2 access.
.DESCRIPTION
  Requires an elevated PowerShell (service install). Starts the fake hub (l0_link.py) in the background, installs
  aleltagent.exe pointing at it, and checks the SCM lifecycle. Writes result.service.json, then merges
  service + link parts into result.json (tests/ladder/_common/ladder_result.py merge).
.EXAMPLE
  .\l0_service.ps1 -LinkDurationSec 86400      # agent = edge-agent\core\build\x64-release\bin\aleltagent.exe (build.ps1 preset)
.EXAMPLE
  .\l0_service.ps1 -LinkDurationSec 300     # shortened CI/dev run
#>
[CmdletBinding()]
param(
    # AHS-26: default = the CMake preset output of edge-agent/core/build.ps1 (the old edge-agent\build\Release path never existed)
    [string]$AgentExe = (Join-Path $PSScriptRoot '..\..\..\edge-agent\core\build\x64-release\bin\aleltagent.exe'),
    [string]$ServiceName = 'ALELTAgent',
    [string]$Python = $(if (Get-Command python -ErrorAction SilentlyContinue) { (Get-Command python).Source } else { 'python' }),
    [int]$HubPort = 18443,
    [int]$LinkDurationSec = 86400,
    [string]$Environment = 'dev'
)
$ErrorActionPreference = 'Stop'
$started = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
$checks = New-Object System.Collections.Generic.List[object]

function Add-Check([string]$Name, $Ok, [string]$Detail = '') {
    $status = if ($null -eq $Ok) { 'SKIP' } elseif ($Ok) { 'PASS' } else { 'FAIL' }
    $checks.Add([ordered]@{ name = $Name; status = $status; detail = $Detail })
    Write-Host ("  [{0}] {1}{2}" -f $status, $Name, $(if ($Detail) { " - $Detail" } else { '' }))
}

function Write-Result {
    $states = $checks | ForEach-Object { $_.status } | Sort-Object -Unique
    $overall = if ($states -contains 'FAIL') { 'FAIL' } elseif (($states | Measure-Object).Count -eq 1 -and $states -eq 'PASS') { 'PASS' } else { 'SKIP' }
    $result = [ordered]@{
        schema_version = 1; level = 'L0'; name = 'agent service lifecycle'; status = $overall
        environment = $Environment; started_at = $started
        finished_at = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
        host = $env:COMPUTERNAME; checks = $checks; artifacts = @()
        exit_criterion = '24 h heartbeat, clean restart/stop, no listening ports'
    }
    $out = Join-Path $PSScriptRoot 'result.service.json'
    $result | ConvertTo-Json -Depth 5 | Set-Content -Path $out -Encoding utf8
    Write-Host "L0 service $overall -> $out"
    if (Test-Path $Python) {
        & $Python (Join-Path $PSScriptRoot '..\_common\ladder_result.py') merge L0 service link
    }
    exit $(switch ($overall) { 'PASS' { 0 } 'SKIP' { 2 } default { 1 } })
}

function Get-ListeningPorts([int]$ProcessId) {
    @(Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue | Where-Object OwningProcess -eq $ProcessId |
        ForEach-Object { "$($_.LocalAddress):$($_.LocalPort)" }) +
    @(Get-NetUDPEndpoint -ErrorAction SilentlyContinue | Where-Object OwningProcess -eq $ProcessId |
        ForEach-Object { "udp $($_.LocalAddress):$($_.LocalPort)" })
}

function Wait-ServiceState([string]$State, [int]$TimeoutSec = 30) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $s = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
        if ($s -and $s.Status -eq $State) { return $true }
        Start-Sleep -Milliseconds 500
    }
    return $false
}

# ---------------------------------------------------------------- prerequisites
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not (Test-Path $AgentExe)) {
    Add-Check 'aleltagent.exe built' $null "not found: $AgentExe (edge-agent 3.1 not built yet)"
    Write-Result
}
if (-not $isAdmin) {
    Add-Check 'elevated shell' $null 'run from an elevated PowerShell (service install)'
    Write-Result
}
Add-Check 'aleltagent.exe built' $true (Get-Item $AgentExe).VersionInfo.FileVersion

# ---------------------------------------------------------------- fake hub
# Real protocol (AHS-26): mock hub with request/response + long-poll commands; the C agent has no WebSocket.
$token = 'l0-' + [guid]::NewGuid().ToString('N')
$link = Start-Process -FilePath $Python -PassThru -NoNewWindow -ArgumentList @(
    (Join-Path $PSScriptRoot 'l0_link.py'), '--external', '--port', $HubPort, '--token', $token,
    '--duration', $LinkDurationSec, '--connect-timeout', 120)
Start-Sleep -Seconds 3
$work = Join-Path $env:ProgramData 'ALELT\L0'
New-Item -ItemType Directory -Force -Path $work | Out-Null
$cfg = Join-Path $work 'aleltagent.json'

try {
    # ---------------------------------------------------------------- install + start
    # dev/token mode on loopback, alias that does not exist (L0 = no Db2 access); token via DPAPI (machine scope)
    & $AgentExe --install --no-start --config $cfg --hub-url "http://127.0.0.1:$HubPort" --agent-id l0-agent `
        --connection-id l0conn --database L0NODB --mode dev --spool-dir (Join-Path $work 'spool') | Out-Null
    Add-Check 'install (--install)' ($LASTEXITCODE -eq 0 -and (Get-Service $ServiceName -ErrorAction SilentlyContinue)) "exit $LASTEXITCODE"
    $token | & $AgentExe --set-dev-token --config $cfg | Out-Null
    Add-Check 'dev token stored (DPAPI)' ($LASTEXITCODE -eq 0) "exit $LASTEXITCODE"
    Start-Service $ServiceName
    Add-Check 'service starts' (Wait-ServiceState 'Running')
    $svcPid = (Get-CimInstance Win32_Service -Filter "Name='$ServiceName'").ProcessId
    Start-Sleep -Seconds 10
    $ports = Get-ListeningPorts $svcPid
    Add-Check 'no listening ports (R4)' ($ports.Count -eq 0) ($ports -join ', ')
    $job = Get-CimInstance Win32_Process -Filter "ProcessId=$svcPid"
    Add-Check 'runs below normal priority' ($job.Priority -le 6) "priority $($job.Priority)"

    # ---------------------------------------------------------------- restart
    Restart-Service $ServiceName
    Add-Check 'clean restart' (Wait-ServiceState 'Running')
    $svcPid = (Get-CimInstance Win32_Service -Filter "Name='$ServiceName'").ProcessId

    # ---------------------------------------------------------------- run for the link duration
    Write-Host "waiting for fake hub ($LinkDurationSec s)..."
    $link.WaitForExit()
    $ports = Get-ListeningPorts $svcPid
    Add-Check 'no listening ports at end of run (R4)' ($ports.Count -eq 0) ($ports -join ', ')

    # ---------------------------------------------------------------- stop + uninstall
    Stop-Service $ServiceName
    Add-Check 'clean stop' (Wait-ServiceState 'Stopped')
    $errs = @(Get-WinEvent -FilterHashtable @{ LogName = 'Application'; ProviderName = $ServiceName; Level = 2
                                               StartTime = [datetime]$started } -ErrorAction SilentlyContinue)
    Add-Check 'no error events in Event Log' ($errs.Count -eq 0) "$($errs.Count) error(s)"
    & $AgentExe --uninstall | Out-Null
    Start-Sleep -Seconds 2
    Add-Check 'uninstall (--uninstall)' (-not (Get-Service $ServiceName -ErrorAction SilentlyContinue)) "exit $LASTEXITCODE"
}
catch {
    Add-Check 'lifecycle' $false $_.Exception.Message
}
finally {
    if (-not $link.HasExited) { Stop-Process -Id $link.Id -Force }
}
Write-Result
