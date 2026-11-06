<#
.SYNOPSIS
  Installs the ALELT Agent (aleltagent.exe) as the Windows service ALELTAgent.
.DESCRIPTION
  Copies aleltagent.exe + aleltsrc_db2.dll to the install folder, writes a starter
  aleltagent.json (kept if present: upgrades never lose config, spool or positions),
  creates/updates the service via the built-in installer, applies ACLs and
  verifies. Must run elevated. SPDX-License-Identifier: Apache-2.0
.EXAMPLE
  .\install-agent.ps1 -SourceDir .\bin -HubUrl https://hub:8443 -AgentId edge-01 -Database <DATABASE> `
     -CertThumbprint 0123...ABCD -HubPin 5f3a...e9 -SpoolDir E:\ALELT\spool -Account 'CORP\svc-alelt$'
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory)] [string] $SourceDir,
  [Parameter(Mandatory)] [string] $HubUrl,
  [Parameter(Mandatory)] [string] $AgentId,
  [Parameter(Mandatory)] [string] $Database,
  [string] $ConnectionId = 'erp',
  [string] $CertThumbprint = '',
  [string] $HubPin = '',
  [ValidateSet('mtls', 'dev')] [string] $Mode = 'mtls',
  [string] $SpoolDir = '',
  [string] $InstallDir = "$env:ProgramFiles\ALELT\Agent",
  [string] $ConfigPath = "$env:ProgramData\ALELT\Agent\aleltagent.json",
  [string] $Account = '',            # empty = NT SERVICE\ALELTAgent; gMSA ends with '$'
  [System.Management.Automation.PSCredential] $Credential, # for a regular user account
  [switch] $NoStart,
  # DEP-09: unsigned / invalidly signed binaries are refused unless explicitly allowed (dev builds only)
  [switch] $AllowUnsigned
)
$ErrorActionPreference = 'Stop'

$id = [Security.Principal.WindowsIdentity]::GetCurrent()
if (-not (New-Object Security.Principal.WindowsPrincipal($id)).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
  throw 'Run this script from an elevated PowerShell.'
}
foreach ($f in 'aleltagent.exe', 'aleltsrc_db2.dll') {
  if (-not (Test-Path (Join-Path $SourceDir $f))) { throw "Missing $f in $SourceDir" }
}

# Spool must not share a volume with Db2 data/log/archive (runbook rule).
if ($SpoolDir) {
  $spoolDrive = (Split-Path -Qualifier $SpoolDir).ToUpper()
  Write-Host "Spool volume: $spoolDrive - confirm with the DBA it holds no Db2 data, active log or archive log."
}

$svc = Get-Service -Name ALELTAgent -ErrorAction SilentlyContinue
if ($svc -and $svc.Status -ne 'Stopped') {
  Write-Host 'Stopping existing service for upgrade...'
  Stop-Service ALELTAgent -Force
  $svc.WaitForStatus('Stopped', '00:01:00')
}

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
# Verify Authenticode signatures BEFORE anything is copied into the service folder (DEP-09).
foreach ($f in 'aleltagent.exe', 'aleltsrc_db2.dll') {
  $sig = Get-AuthenticodeSignature (Join-Path $SourceDir $f)
  if ($sig.Status -ne 'Valid') {
    if (-not $AllowUnsigned) {
      throw "$f is not validly signed ($($sig.Status)). Use a signed release build, or -AllowUnsigned for a dev build."
    }
    Write-Warning "$f is not validly signed ($($sig.Status)); continuing because -AllowUnsigned was given (dev only)."
  } else {
    Write-Host "$f signed by: $($sig.SignerCertificate.Subject)"
  }
}
Copy-Item (Join-Path $SourceDir 'aleltagent.exe') $InstallDir -Force
Copy-Item (Join-Path $SourceDir 'aleltsrc_db2.dll') $InstallDir -Force

$exe = Join-Path $InstallDir 'aleltagent.exe'
$aleltArgs = @('--install', '--config', $ConfigPath, '--hub-url', $HubUrl, '--agent-id', $AgentId,
          '--connection-id', $ConnectionId, '--database', $Database, '--mode', $Mode)
if ($CertThumbprint) { $aleltArgs += @('--thumbprint', ($CertThumbprint -replace '\s', '')) }
if ($HubPin)         { $aleltArgs += @('--pin', $HubPin) }
if ($SpoolDir)       { $aleltArgs += @('--spool-dir', $SpoolDir) }
if ($NoStart)        { $aleltArgs += '--no-start' }
if ($Credential) {
  $aleltArgs += @('--account', $Credential.UserName, '--password-stdin')
  $Credential.GetNetworkCredential().Password | & $exe @aleltArgs
} else {
  if ($Account) { $aleltArgs += @('--account', $Account) }
  & $exe @aleltArgs
}
if ($LASTEXITCODE -ne 0) { throw "aleltagent.exe --install failed ($LASTEXITCODE)" }

& $exe --check-config --config $ConfigPath
if ($LASTEXITCODE -ne 0) {
  Write-Warning "Config is not valid yet: edit $ConfigPath, then: Start-Service ALELTAgent"
  exit 0
}
& (Join-Path $PSScriptRoot 'verify-agent.ps1') -ConfigPath $ConfigPath
