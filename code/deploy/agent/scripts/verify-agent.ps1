<#
.SYNOPSIS
  Verifies a ALELT Agent installation: service state and configuration,
  no listening sockets (R4), Job Object limits, config ACL, log tail.
  Exit code 0 = all checks passed. SPDX-License-Identifier: Apache-2.0
#>
[CmdletBinding()]
param(
  [string] $ConfigPath = "$env:ProgramData\ALELT\Agent\aleltagent.json",
  [int] $LogLines = 20
)
$fail = 0
function Check([string] $name, [bool] $ok, [string] $detail = '') {
  $tag = if ($ok) { 'PASS' } else { 'FAIL' }
  Write-Host ("[{0}] {1} {2}" -f $tag, $name, $detail)
  if (-not $ok) { $script:fail++ }
}

$svc = Get-CimInstance Win32_Service -Filter "Name='ALELTAgent'"
Check 'service installed' ($null -ne $svc)
if ($svc) {
  Check 'service running' ($svc.State -eq 'Running') "state=$($svc.State) account=$($svc.StartName)"
  Check 'start mode auto' ($svc.StartMode -eq 'Auto')
  Check 'not LocalSystem' ($svc.StartName -ne 'LocalSystem') '(dedicated account recommended)'
  $q = sc.exe qfailure ALELTAgent | Out-String
  Check 'recovery actions' ($q -match 'RESTART')

  if ($svc.ProcessId -gt 0) {
    $pid_ = [int]$svc.ProcessId
    $listen = @(Get-NetTCPConnection -State Listen -OwningProcess $pid_ -ErrorAction SilentlyContinue)
    $udp = @(Get-NetUDPEndpoint -OwningProcess $pid_ -ErrorAction SilentlyContinue)
    Check 'no listening TCP sockets (R4)' ($listen.Count -eq 0) "count=$($listen.Count)"
    Check 'no UDP endpoints' ($udp.Count -eq 0) "count=$($udp.Count)"
    $out = @(Get-NetTCPConnection -OwningProcess $pid_ -ErrorAction SilentlyContinue | Where-Object State -eq 'Established')
    Write-Host ("       outbound connections: {0}" -f (($out | ForEach-Object { "$($_.RemoteAddress):$($_.RemotePort)" }) -join ', '))

    $p = Get-Process -Id $pid_
    Check 'priority BELOW_NORMAL' ($p.PriorityClass -eq 'BelowNormal') "priority=$($p.PriorityClass)"
    # Job Object membership (IsProcessInJob)
    $sig = '[DllImport("kernel32.dll", SetLastError=true)] public static extern bool IsProcessInJob(System.IntPtr p, System.IntPtr j, out bool r);'
    $k = Add-Type -MemberDefinition $sig -Name K32 -Namespace AleltVerify -PassThru -ErrorAction SilentlyContinue
    $inJob = $false
    if ($k) { [void]$k::IsProcessInJob($p.Handle, [IntPtr]::Zero, [ref]$inJob) }
    Check 'process in Job Object (CPU/memory caps)' $inJob
    Write-Host ("       private memory: {0:N1} MB" -f ($p.PrivateMemorySize64 / 1MB))
  }
}

Check 'config exists' (Test-Path $ConfigPath) $ConfigPath
if (Test-Path $ConfigPath) {
  $acl = Get-Acl $ConfigPath
  $broad = $acl.Access | Where-Object { $_.IdentityReference -match 'Everyone|Users|Authenticated Users' }
  Check 'config ACL restricted' ($null -eq $broad) (($acl.Access | ForEach-Object IdentityReference) -join ', ')
  $exe = $svc.PathName -replace '^"([^"]+)".*$', '$1'
  if ($exe -and (Test-Path $exe)) {
    & $exe --check-config --config $ConfigPath | Write-Host
    Check 'config valid' ($LASTEXITCODE -eq 0)
  }
  try {
    $cfg = Get-Content $ConfigPath -Raw | ConvertFrom-Json
    $spool = [Environment]::ExpandEnvironmentVariables($cfg.spool.dir)
    $log = [Environment]::ExpandEnvironmentVariables($cfg.log.dir)
    # DEP-23: the spool / data dirs hold positions and raw log batches: no broad read for local users
    foreach ($d in @($spool, $log, (Split-Path -Parent $ConfigPath)) | Where-Object { $_ -and (Test-Path $_) } | Select-Object -Unique) {
      $dacl = Get-Acl $d
      $wide = $dacl.Access | Where-Object { $_.IdentityReference -match 'Everyone|\\Users$|Authenticated Users' -and $_.AccessControlType -eq 'Allow' }
      Check "ACL restricted: $d" ($null -eq $wide) (($dacl.Access | ForEach-Object IdentityReference) -join ', ')
    }
    if ($spool) {
      $n = @(Get-ChildItem $spool -Filter *.aleltb -ErrorAction SilentlyContinue).Count
      Write-Host "       spool: $spool ($n unacknowledged batches)"
      if (Test-Path "$spool\state.json") { Write-Host ("       state: " + (Get-Content "$spool\state.json" -Raw)) }
    }
    if ($log -and (Test-Path "$log\aleltagent.log")) {
      Write-Host "       last $LogLines log lines:"
      Get-Content "$log\aleltagent.log" -Tail $LogLines | ForEach-Object { "         $_" }
    }
  } catch { Write-Warning "cannot parse config: $_" }
}
$ev = $null
try { $ev = Get-WinEvent -FilterHashtable @{ LogName = 'Application'; ProviderName = 'ALELTAgent'; Level = 2 } -MaxEvents 5 -ErrorAction Stop } catch { }
if ($ev) { Write-Host '       recent Event Log errors:'; $ev | ForEach-Object { "         $($_.TimeCreated) $($_.Message.Substring(0, [Math]::Min(200, $_.Message.Length)))" } }

if ($fail) { Write-Host "$fail check(s) failed"; exit 1 }
Write-Host 'All checks passed'
exit 0
