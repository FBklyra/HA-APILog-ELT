<#
.SYNOPSIS
  Removes the ALELTAgent service. Keeps config, spool (unsent batches +
  positions) and logs unless -Purge is given. Never touches Db2 or its archive
  logs (R3). Must run elevated. SPDX-License-Identifier: Apache-2.0
#>
[CmdletBinding(SupportsShouldProcess)]
param(
  [string] $InstallDir = "$env:ProgramFiles\ALELT\Agent",
  [string] $DataDir = "$env:ProgramData\ALELT\Agent",
  [switch] $Purge
)
$ErrorActionPreference = 'Stop'
$exe = Join-Path $InstallDir 'aleltagent.exe'
if (Test-Path $exe) {
  & $exe --uninstall
} else {
  $s = Get-Service ALELTAgent -ErrorAction SilentlyContinue
  if ($s) { Stop-Service ALELTAgent -Force -ErrorAction SilentlyContinue; sc.exe delete ALELTAgent | Out-Null }
}
if ($Purge) {
  $spool = Join-Path $DataDir 'spool'
  $pending = @(Get-ChildItem $spool -Filter *.aleltb -ErrorAction SilentlyContinue).Count
  if ($pending -gt 0) { Write-Warning "$pending unacknowledged batch(es) in $spool will be deleted (hub has not confirmed them)." }
  if ($PSCmdlet.ShouldProcess($DataDir, 'Delete config, spool and logs')) {
    Remove-Item -Recurse -Force $DataDir -ErrorAction SilentlyContinue
  }
  if ($PSCmdlet.ShouldProcess($InstallDir, 'Delete binaries')) {
    Remove-Item -Recurse -Force $InstallDir -ErrorAction SilentlyContinue
  }
}
Write-Host 'ALELT Agent removed.'
