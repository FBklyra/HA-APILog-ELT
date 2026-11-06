<#
.SYNOPSIS
  Builds aleltagent.exe + aleltsrc_db2.dll (x64) and runs the unit tests.
.DESCRIPTION
  Finds Visual Studio (any edition incl. Insiders/Preview) with vswhere, loads the x64 compiler
  environment (vcvarsall amd64, or arm64_amd64 on an ARM64 machine), then:
    cmake --preset <Preset>; cmake --build --preset <Preset>; ctest --preset <Preset>
  Output: edge-agent\core\build\<Preset>\bin (git-ignored). SPDX-License-Identifier: Apache-2.0
.EXAMPLE
  .\build.ps1                       # x64-release, with tests
  .\build.ps1 -Preset x64-debug -NoTest
  .\build.ps1 -Db2Home 'C:\Program Files\IBM\SQLLIB' -Clean
#>
[CmdletBinding()]
param(
  [ValidateSet('x64-release', 'x64-debug', 'fuzz-clang')] [string] $Preset = 'x64-release',
  [string] $Db2Home = '',
  [switch] $NoTest,
  [switch] $Clean
)
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

function Find-VcVars {
  $vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio\Installer\vswhere.exe'
  $roots = @()
  if (Test-Path $vswhere) {
    $roots += & $vswhere -all -prerelease -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
    $roots += & $vswhere -all -prerelease -products * -property installationPath
  }
  $roots += Get-ChildItem "$env:ProgramFiles\Microsoft Visual Studio\*\*" -Directory -ErrorAction SilentlyContinue | ForEach-Object FullName
  foreach ($r in ($roots | Where-Object { $_ } | Select-Object -Unique)) {
    $v = Join-Path $r 'VC\Auxiliary\Build\vcvarsall.bat'
    if (Test-Path $v) { return $v }
  }
  throw 'Visual Studio C++ tools not found (install "Desktop development with C++" incl. x64 tools).'
}

$vcvars = Find-VcVars
$arch = if ($env:PROCESSOR_ARCHITECTURE -eq 'ARM64') { 'arm64_amd64' } else { 'amd64' }
Write-Host "Using $vcvars ($arch)"
$envDump = cmd /c "`"$vcvars`" $arch >nul && set"
if ($LASTEXITCODE -ne 0) { throw "vcvarsall $arch failed" }
foreach ($line in $envDump) {
  if ($line -match '^([^=]+)=(.*)$') { [Environment]::SetEnvironmentVariable($Matches[1], $Matches[2], 'Process') }
}
foreach ($tool in 'cl', 'cmake', 'ninja') {
  if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) { throw "$tool not on PATH after vcvarsall" }
}

$bin = Join-Path $PSScriptRoot "build\$Preset"
if ($Clean -and (Test-Path $bin)) { Remove-Item -Recurse -Force $bin }
$cfgArgs = @('--preset', $Preset)
if ($Db2Home) { $cfgArgs += "-DDB2_HOME=$($Db2Home -replace '\\', '/')" }
$py = Get-Command python -ErrorAction SilentlyContinue
if ($py) { $cfgArgs += "-DPython3_EXECUTABLE=$($py.Source)" }

& cmake @cfgArgs
if ($LASTEXITCODE -ne 0) { throw 'cmake configure failed' }
& cmake --build --preset $Preset
if ($LASTEXITCODE -ne 0) { throw 'build failed' }
if (-not $NoTest -and $Preset -ne 'fuzz-clang') {
  & ctest --preset $Preset
  if ($LASTEXITCODE -ne 0) { throw 'tests failed' }
}
Write-Host "Built: $bin\bin"
Get-ChildItem "$bin\bin" -Include *.exe, *.dll -Recurse | ForEach-Object { "  $($_.Name)  $([Math]::Round($_.Length / 1KB)) KB" }
