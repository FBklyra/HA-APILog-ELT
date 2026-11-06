<#
.SYNOPSIS
  Assembles the downloadable ALELT release in dist\release\<version>\ (public release = linux/amd64 + x64 only).
.DESCRIPTION
  Assets (each skipped with a WARN when its input is missing):
    alelt-<v>-linux-amd64.image.tar     docker save of the hub image (via deploy\build-image.ps1 -Platform linux/amd64 -Output tar)
    alelt-agent-<v>-windows-x64.zip     agent binaries + install/verify/uninstall scripts + example config
    alelt-agent-<v>-windows-x64.msi     only when the WiX v4 CLI (wix) is on PATH
    alelt-agentd-<v>-linux-x64.tar.gz   Linux daemon: source tree for deploy/linux/install.sh (systemd units included)
    alelt-compose-<v>.tar.gz            deploy\local + deploy\ha + deploy\hub compose bundles (.env files excluded)
    alelt-helm-<v>.tgz                  Helm chart (helm package; alpine/helm container when helm is missing)
    RELEASE-NOTES.md, SHA256SUMS
  SPDX-License-Identifier: Apache-2.0
.EXAMPLE
  .\deploy\release\build-release.ps1 -Version 0.2.0
  .\deploy\release\build-release.ps1 -Version 0.2.0 -SkipImages      # no image build (fast)
  .\deploy\release\build-release.ps1 -Version 0.2.0 -DryRun          # print the plan only
#>
[CmdletBinding()]
param(
  [string] $Version = '0.1.0',
  [string] $ImageName = 'alelt',
  [string] $AgentBinDir = '',            # default: edge-agent\core\build\x64-release\bin
  [string] $OutRoot = '',                # default: dist\release
  [string] $HelmImage = 'alpine/helm:3.16.2',
  [switch] $SkipImages,
  [switch] $BuildAgent,                  # run edge-agent\core\build.ps1 first (needs Visual Studio C++ x64 tools)
  [switch] $AgentOnly,                  # Windows agent zip/MSI only (CI Windows job; no Linux containers there)
  [switch] $DryRun
)
$ErrorActionPreference = 'Stop'
$Repo = (Resolve-Path "$PSScriptRoot\..\..").Path
if (-not $OutRoot) { $OutRoot = Join-Path $Repo 'dist\release' }
$Out = Join-Path $OutRoot $Version
if (-not $AgentBinDir) { $AgentBinDir = Join-Path $Repo 'edge-agent\core\build\x64-release\bin' }
$warnings = [System.Collections.Generic.List[string]]::new()
$assets = [System.Collections.Generic.List[string]]::new()
function Step([string] $m) { Write-Host "==> $m" }
function Warn([string] $m) { Write-Warning $m; $warnings.Add($m) }
# DEP-28: release trees never carry developer-machine artefacts or secrets (same deny list as build-release.sh)
$DenyName = '^(\.env(\..*)?|.*\.(key|pem|p8|pfx|p12|crt|jks|db|sqlite)|__pycache__|\.pytest_cache)$'
$DenyDir = '\\(secrets|\.alelt|company-config)(\\|$)'
function Test-Allowed([IO.FileSystemInfo] $f) { $f.Name -match '\.example$|^README|\.md$' }
function Invoke-Scrub([string] $dir) {
  Get-ChildItem $dir -Recurse -Force | Where-Object { -not (Test-Allowed $_) -and ($_.Name -match $DenyName -or $_.FullName.Substring($dir.Length) -match $DenyDir) } |
    Sort-Object { $_.FullName.Length } -Descending | ForEach-Object { if (Test-Path $_.FullName) { Remove-Item -Recurse -Force $_.FullName } }
  $left = @(Get-ChildItem $dir -Recurse -Force -File | Where-Object { -not (Test-Allowed $_) -and ($_.Name -match $DenyName -or $_.FullName.Substring($dir.Length) -match $DenyDir) })
  if ($left.Count) { throw "deny-listed files in release tree ${dir}: $(($left | Select-Object -First 20 | ForEach-Object FullName) -join ', ')" }
}
function Plan([string] $m) { if ($DryRun) { Write-Host "    [dry-run] $m" } }

if ($Version -notmatch '^\d+\.\d+\.\d+([-+][0-9A-Za-z.-]+)?$') { throw "Version must be SemVer (got '$Version')" }
Step "release $Version -> $Out"
if (-not $DryRun) {
  if (Test-Path $Out) { Remove-Item -Recurse -Force $Out }
  New-Item -ItemType Directory -Force -Path $Out | Out-Null
}
$stage = Join-Path ([IO.Path]::GetTempPath()) ("alelt-release-" + [guid]::NewGuid().ToString('N').Substring(0, 8))

try {
  # 1. Hub image (amd64 only for the public release)
  $imgTar = "alelt-$Version-linux-amd64.image.tar"
  if ($SkipImages) { Step 'image: skipped (-SkipImages)' }
  else {
    Step "image: build linux/amd64 $ImageName`:$Version -> $imgTar"
    Plan "deploy\build-image.ps1 -Platform linux/amd64 -Tag $ImageName`:$Version -Version $Version -Output tar -OutDir <stage>"
    if (-not $DryRun) {
      New-Item -ItemType Directory -Force -Path $stage | Out-Null
      & (Join-Path $Repo 'deploy\build-image.ps1') -Platform linux/amd64 -Tag "$ImageName`:$Version" -Version $Version -Output tar -OutDir $stage
      $built = Get-ChildItem $stage -Filter '*-amd64.tar' | Select-Object -First 1
      if (-not $built) { throw 'image tar not produced' }
      Move-Item $built.FullName (Join-Path $Out $imgTar)
      $assets.Add($imgTar)
    }
  }

  # 2. Windows agent (x64)
  if ($BuildAgent) {
    Step 'agent: build x64-release'
    Plan 'edge-agent\core\build.ps1 -Preset x64-release'
    if (-not $DryRun) { & (Join-Path $Repo 'edge-agent\core\build.ps1') -Preset x64-release }
  }
  $exes = @(Get-ChildItem $AgentBinDir -File -ErrorAction SilentlyContinue | Where-Object { ($_.Extension -eq '.exe' -and $_.BaseName -match 'agent') -or $_.Extension -eq '.dll' })
  if ($exes.Count -eq 0) { Warn "agent: no binaries in $AgentBinDir (run with -BuildAgent on a machine with Visual Studio C++ x64 tools)" }
  else {
    $zip = "alelt-agent-$Version-windows-x64.zip"
    Step "agent: $zip ($($exes.Name -join ', '))"
    Plan "zip binaries + deploy\agent\scripts\*.ps1 + example config"
    if (-not $DryRun) {
      $z = Join-Path $stage 'agent'; New-Item -ItemType Directory -Force -Path $z | Out-Null
      $exes | Copy-Item -Destination $z
      Copy-Item (Join-Path $Repo 'deploy\agent\scripts\*.ps1') $z
      Get-ChildItem (Join-Path $Repo 'edge-agent\core') -Filter '*.example.json' | Copy-Item -Destination $z
      foreach ($f in 'LICENSE', 'NOTICE', 'DISCLAIMER.md') { if (Test-Path (Join-Path $Repo $f)) { Copy-Item (Join-Path $Repo $f) $z } }
      Compress-Archive -Path (Join-Path $z '*') -DestinationPath (Join-Path $Out $zip) -Force
      $assets.Add($zip)
    }
    $wix = Get-Command wix -ErrorAction SilentlyContinue
    $msi = "alelt-agent-$Version-windows-x64.msi"
    if (-not $wix) { Warn 'agent: WiX v4 CLI not found - MSI skipped (zip + install-agent.ps1 ship instead)' }
    else {
      Step "agent: $msi"
      $wxs = Get-ChildItem (Join-Path $Repo 'deploy\agent\wix') -Filter '*.wxs' | Select-Object -First 1
      Plan "wix build $($wxs.Name) -arch x64 -ext WixToolset.Util.wixext -d BinDir=$AgentBinDir"
      if (-not $DryRun) {
        Push-Location $wxs.DirectoryName
        try {
          & wix build $wxs.Name -arch x64 -ext WixToolset.Util.wixext -d "BinDir=$AgentBinDir" -o (Join-Path $Out $msi)
          if ($LASTEXITCODE -ne 0) { throw 'wix build failed' }
        } finally { Pop-Location }
        $assets.Add($msi)
      }
    }
  }

  if ($AgentOnly) { $SkipRest = $true } else { $SkipRest = $false }
  # 3. Linux edge daemon (x64). deploy\linux\install.sh installs from a source tree (--src, default ../..), so the
  #    tarball keeps the repo layout: edge-agent\linux, hub, connectors, config, deploy\linux, deploy\image\requirements.txt
  $treeParts = @('edge-agent\linux', 'hub', 'connectors', 'config', 'deploy\linux', 'deploy\image\requirements.txt', 'deploy\image\requirements.lock')
  $hasPkg = Test-Path (Join-Path $Repo 'edge-agent\linux')
  if ($SkipRest) { }
  elseif (-not $hasPkg) { Warn 'linux daemon: edge-agent\linux not found - skipped' }
  else {
    $tgz = "alelt-agentd-$Version-linux-x64.tar.gz"
    Step "linux daemon: $tgz (source tree for deploy/linux/install.sh)"
    $present = @($treeParts | Where-Object { Test-Path (Join-Path $Repo $_) })
    if (-not (Test-Path (Join-Path $Repo 'deploy\linux\install.sh'))) { Warn 'linux daemon: deploy\linux\install.sh not found' }
    Plan "tar.gz $($present -join ', ') (no tests, caches)"
    if (-not $DryRun) {
      $d = Join-Path $stage "agentd\alelt-agentd-$Version"
      foreach ($part in $present) {
        $dst = Join-Path $d $part
        New-Item -ItemType Directory -Force -Path (Split-Path $dst) | Out-Null
        Copy-Item (Join-Path $Repo $part) $dst -Recurse
      }
      Get-ChildItem $d -Recurse -Directory -Force | Where-Object { $_.Name -in '__pycache__', '.pytest_cache', 'tests' } |
        Sort-Object { $_.FullName.Length } -Descending | ForEach-Object { if (Test-Path $_.FullName) { Remove-Item -Recurse -Force $_.FullName } }
      Invoke-Scrub $d
      foreach ($f in 'LICENSE', 'NOTICE', 'DISCLAIMER.md') { if (Test-Path (Join-Path $Repo $f)) { Copy-Item (Join-Path $Repo $f) $d } }
      & tar -czf (Join-Path $Out $tgz) -C (Join-Path $stage 'agentd') "alelt-agentd-$Version"
      if ($LASTEXITCODE -ne 0) { throw 'tar failed (daemon)' }
      $assets.Add($tgz)
    }
  }

  # 4. Compose bundle (no .env with secrets, no generated output)
  $cb = "alelt-compose-$Version.tar.gz"
  if (-not $SkipRest) {
  Step "compose bundle: $cb"
  $dirs = @('local', 'ha', 'hub') | Where-Object { Test-Path (Join-Path $Repo "deploy\$_") }
  Plan "tar.gz deploy\{$($dirs -join ',')} + deploy\HA.md (exclude .env, secrets values)"
  if (-not $DryRun) {
    $c = Join-Path $stage "compose\alelt-compose-$Version"
    New-Item -ItemType Directory -Force -Path $c | Out-Null
    foreach ($d in $dirs) { Copy-Item (Join-Path $Repo "deploy\$d") (Join-Path $c $d) -Recurse }
    Invoke-Scrub $c
    foreach ($f in 'deploy\HA.md', 'deploy\INSTALL-IN-A-MORNING.md', 'LICENSE', 'NOTICE', 'DISCLAIMER.md') { if (Test-Path (Join-Path $Repo $f)) { Copy-Item (Join-Path $Repo $f) $c } }
    & tar -czf (Join-Path $Out $cb) -C (Join-Path $stage 'compose') "alelt-compose-$Version"
    if ($LASTEXITCODE -ne 0) { throw 'tar failed (compose)' }
    $assets.Add($cb)
  }
  }

  # 5. Helm chart
  $chartYaml = Get-ChildItem (Join-Path $Repo 'deploy\helm') -Filter 'Chart.yaml' -Recurse -Depth 1 | Select-Object -First 1
  if ($SkipRest) { }
  elseif (-not $chartYaml) { Warn 'helm: no chart under deploy\helm - skipped' }
  else {
    $chart = $chartYaml.Directory
    $ht = "alelt-helm-$Version.tgz"
    Step "helm: $ht (chart $($chart.Name))"
    $helm = Get-Command helm -ErrorAction SilentlyContinue
    Plan ($(if ($helm) { 'helm' } else { "docker run $HelmImage" }) + " package $($chart.Name) --version $Version --app-version $Version")
    if (-not $DryRun) {
      $h = Join-Path $stage 'helm'; New-Item -ItemType Directory -Force -Path $h | Out-Null
      if ($helm) { & helm package $chart.FullName -d $h --version $Version --app-version $Version }
      else {
        & docker run --rm -v "$($chart.Parent.FullName):/charts:ro" -v "${h}:/out" $HelmImage package "/charts/$($chart.Name)" -d /out --version $Version --app-version $Version
      }
      if ($LASTEXITCODE -ne 0) { throw 'helm package failed' }
      $p = Get-ChildItem $h -Filter '*.tgz' | Select-Object -First 1
      Move-Item $p.FullName (Join-Path $Out $ht)
      $assets.Add($ht)
    }
  }

  # 6. Release notes + checksums
  Step 'RELEASE-NOTES.md + SHA256SUMS'
  if (-not $DryRun) {
    $tpl = Get-Content (Join-Path $PSScriptRoot 'RELEASE-NOTES.template.md') -Raw
    $list = ($assets | ForEach-Object { "| ``$_`` | $([Math]::Round((Get-Item (Join-Path $Out $_)).Length / 1MB, 2)) MB |" }) -join "`n"
    $notes = $tpl.Replace('{{VERSION}}', $Version).Replace('{{DATE}}', (Get-Date -AsUTC -Format 'yyyy-MM-dd')).Replace('{{ASSETS}}', $list)
    [IO.File]::WriteAllText((Join-Path $Out 'RELEASE-NOTES.md'), $notes)
    $sums = Get-ChildItem $Out -File | Where-Object Name -ne 'SHA256SUMS' | Sort-Object Name |
      ForEach-Object { "$((Get-FileHash $_.FullName -Algorithm SHA256).Hash.ToLower())  $($_.Name)" }
    [IO.File]::WriteAllText((Join-Path $Out 'SHA256SUMS'), (($sums -join "`n") + "`n"))
    Get-Content (Join-Path $Out 'SHA256SUMS')
  }
} finally {
  if (Test-Path $stage) { Remove-Item -Recurse -Force $stage }
}
if ($warnings.Count) { Write-Host "`nWarnings:"; $warnings | ForEach-Object { Write-Host "  - $_" } }
Write-Host "`nDone: $(if ($DryRun) { '(dry-run, nothing written)' } else { $Out })"
