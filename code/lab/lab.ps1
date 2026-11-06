# SPDX-License-Identifier: Apache-2.0
<#
.SYNOPSIS
  ALELT local test lab: throw-away sources and targets with fake data (lab/catalog.yaml).

.EXAMPLE
  .\lab\lab.ps1 list
  .\lab\lab.ps1 pull postgres mysql landing
  .\lab\lab.ps1 up postgres mysql mariadb landing iceberg
  .\lab\lab.ps1 status
  .\lab\lab.ps1 demo mysql            # end-to-end demo pipeline: mysql -> capture -> hub -> landing
  .\lab\lab.ps1 down                  # stop + remove every lab container (volumes kept)
  .\lab\lab.ps1 reset postgres        # remove containers AND data volumes of the given profiles
  .\lab\lab.ps1 helper                # host-side helper for the console "Local test lab" screen (127.0.0.1 only)

.NOTES
  Zero risk by design: own compose project / network / volumes (alelt-lab*), ports bound to 127.0.0.1, official
  public images with pinned tags, no Docker socket mounted into any container. Never touches other containers.
#>
[CmdletBinding()]
param(
  [Parameter(Position = 0)] [ValidateSet('list', 'ports', 'pull', 'up', 'down', 'status', 'reset', 'demo', 'helper', 'help')]
  [string] $Command = 'help',
  [Parameter(Position = 1, ValueFromRemainingArguments = $true)] [string[]] $Profiles = @(),
  [int] $Retries = 3,
  [int] $PullTimeoutSec = 1800,
  [int] $WaitTimeoutSec = 900
)
$ErrorActionPreference = 'Stop'
$Here = Split-Path -Parent $MyInvocation.MyCommand.Path
$Repo = Split-Path -Parent $Here
$Compose = Join-Path $Here 'docker-compose.lab.yml'
$Project = 'alelt-lab'
$Catalog = Get-Content -Raw -Encoding UTF8 (Join-Path $Here 'catalog.json') | ConvertFrom-Json
$ImageProfiles = @($Catalog.profiles | Where-Object { $_.availability -eq 'image' -and -not $_.optional })
# optional profiles (db2): never in all/sources; named explicitly they start only with LAB_DB2_CONTAINER=1 (x64 hosts).
# Machines with a native Db2 install use it as the lab Db2 source instead of the container.
$ContainerOptIn = ($env:LAB_DB2_CONTAINER -eq '1')

function Say([string] $m) { Write-Host "[lab] $m" }

function Read-EnvLocal {
  $h = @{}
  $f = Join-Path $Here '.env.local'          # machine-local image / port overrides (gitignored)
  if (Test-Path $f) {
    foreach ($line in Get-Content -Encoding UTF8 $f) {
      $t = $line.Trim()
      if ($t -and -not $t.StartsWith('#') -and $t.Contains('=')) {
        $k, $v = $t.Split('=', 2); $h[$k.Trim()] = $v.Trim().Trim('"').Trim("'")
      }
    }
  }
  return $h
}

function Test-Image([string] $img) { & docker image inspect $img *> $null; return ($LASTEXITCODE -eq 0) }

# Image choice per compose variable (catalog local_fallbacks): environment / lab/.env.local > pinned tag if present >
# first present compatible local tag (printed) > pinned tag (pulled). Same rules as lab/images.py.
function Resolve-Images($profiles) {
  $local = Read-EnvLocal
  $chosen = [ordered]@{}; $pull = @(); $done = @{}
  # every lab/.env.local entry (LAB_PORT_*, LAB_PLATFORM_*, ...) goes to compose; the process environment wins
  foreach ($k in $local.Keys) { $v = [Environment]::GetEnvironmentVariable($k); $chosen[$k] = if ($v) { $v } else { $local[$k] } }
  foreach ($p in $profiles) {
    foreach ($fb in @($p.local_fallbacks)) {
      if (-not $fb -or $done.ContainsKey($fb.var)) { continue }
      $done[$fb.var] = $true
      $over = [Environment]::GetEnvironmentVariable($fb.var)
      if (-not $over -and $local.ContainsKey($fb.var)) { $over = $local[$fb.var] }
      if ($over) {
        $chosen[$fb.var] = $over
        if ($over -ne $fb.image) { Say "image: $($fb.var)=$over (override)" }
        if (-not (Test-Image $over)) { $pull += $over }
        continue
      }
      if (Test-Image $fb.image) { $chosen[$fb.var] = $fb.image; continue }
      $alt = @($fb.tags) | Where-Object { $_ -and (Test-Image $_) } | Select-Object -First 1
      if ($alt) { $chosen[$fb.var] = $alt; Say "image: $($fb.image) not present locally - using local $alt" }
      else { $chosen[$fb.var] = $fb.image; $pull += $fb.image }
    }
  }
  return @{ chosen = $chosen; pull = $pull }
}

function Resolve-Profiles([string[]] $ids, [switch] $DefaultAll) {
  if (-not $ids -or $ids.Count -eq 0) {
    if ($DefaultAll) { return $ImageProfiles }
    throw "name one or more profiles (or 'all', 'sources', 'targets'). See: lab.ps1 list"
  }
  $out = @()
  foreach ($id in $ids) {
    switch ($id) {
      'all'     { $out += $ImageProfiles }
      'sources' { $out += $ImageProfiles | Where-Object kind -eq 'source' }
      'targets' { $out += $ImageProfiles | Where-Object kind -eq 'target' }
      default {
        $p = $Catalog.profiles | Where-Object id -eq $id
        if (-not $p) { throw "unknown profile '$id'. See: lab.ps1 list" }
        if ($p.availability -ne 'image') { throw "'$id' has no local image (cloud service): see its guidance in the console or lab/README.md" }
        if ($p.optional -and -not $ContainerOptIn) { Say "skip $id (optional): $($p.native)"; continue }
        $out += $p
      }
    }
  }
  return @($out | Sort-Object id -Unique)
}

function Invoke-Compose([string[]] $ids, [string[]] $composeArgs) {
  $env:COMPOSE_PROFILES = ($ids -join ',')
  try {
    & docker compose -p $Project -f $Compose @composeArgs
    if ($LASTEXITCODE -ne 0) { throw "docker compose $($composeArgs -join ' ') failed (exit $LASTEXITCODE)" }
  } finally { Remove-Item Env:COMPOSE_PROFILES -ErrorAction SilentlyContinue }
}

function Get-ReservedRanges {
  $ranges = @()
  if ($IsWindows -or $env:OS -eq 'Windows_NT') {
    $txt = & netsh int ipv4 show excludedportrange protocol=tcp 2>$null
    foreach ($line in $txt) {
      if ($line -match '^\s*(\d+)\s+(\d+)') { $ranges += , @([int]$Matches[1], [int]$Matches[2]) }
    }
  }
  return $ranges
}

function Test-Ports($profiles) {
  $ranges = Get-ReservedRanges
  $running = @(& docker ps --filter "label=com.docker.compose.project=$Project" --format '{{.Names}}')
  $bad = @()
  foreach ($p in $profiles) {
    if (@($p.services | Where-Object { $running -contains $_ }).Count -gt 0) { continue }   # already ours
    foreach ($prop in $p.ports.PSObject.Properties) {
      $port = [int]$prop.Value
      foreach ($r in $ranges) { if ($port -ge $r[0] -and $port -le $r[1]) { $bad += "$($p.id):$port is in the Windows reserved range $($r[0])-$($r[1])" } }
      try {
        $l = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction Stop
        if ($l) { $bad += "$($p.id):$port is already in use on this host" }
      } catch { }
    }
  }
  if ($bad.Count) {
    $bad | ForEach-Object { Write-Warning $_ }
    throw "port check failed. Override ports with LAB_PORT_<NAME> variables (see lab/README.md)."
  }
  Say "ports OK ($(@($profiles | ForEach-Object { $_.ports.PSObject.Properties.Value }) -join ', '))"
}

function Assert-Docker {
  & docker info --format '{{.ServerVersion}}' *> $null
  if ($LASTEXITCODE -ne 0) { throw 'Docker is not running. Start Docker Desktop / the Docker daemon first.' }
}

function Show-Connections($profiles) {
  foreach ($p in $profiles) {
    if ($p.dsn) { Say ("{0,-11} {1}" -f $p.id, $p.dsn) }
    if ($p.notes) { Say ("{0,-11} note: {1}" -f '', $p.notes) }
  }
}

function Invoke-PostInit($profiles) {
  if ($profiles.id -contains 'sqlserver') {
    Say 'sqlserver: creating labdb.shop + enabling CDC (idempotent)'
    & docker exec alelt-lab-sqlserver test -x /opt/mssql-tools18/bin/sqlcmd *> $null
    if ($LASTEXITCODE -eq 0) {
      & docker exec alelt-lab-sqlserver /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P Lab_local_only1 -b -i /lab-init/10-shop.sql | Out-Null
    } else {                                   # image without sqlcmd (azure-sql-edge fallback): run from the host
      $py = if ($env:LAB_PYTHON) { $env:LAB_PYTHON } else { 'python' }
      & $py (Join-Path $Here 'sqlserver_init.py')
    }
    if ($LASTEXITCODE -ne 0) { throw 'sqlserver init failed' }
  }
  if ($profiles.id -contains 'files') { New-Item -ItemType Directory -Force (Join-Path $Here 'data') | Out-Null }
}

switch ($Command) {
  'help' { Get-Help $MyInvocation.MyCommand.Path -Detailed; break }
  'list' {
    $Catalog.profiles | ForEach-Object {
      [pscustomobject]@{ id = $_.id; kind = $_.kind; status = $_.status; availability = $_.availability
                         arch = ($_.arch -join ','); label = $_.label }
    } | Format-Table -AutoSize
    break
  }
  'ports' { Test-Ports (Resolve-Profiles $Profiles -DefaultAll); break }
  'pull' {
    Assert-Docker
    $sel = Resolve-Profiles $Profiles -DefaultAll
    $need = (Resolve-Images $sel).pull               # only images with no compatible local tag
    if (-not $need) { Say 'all images (or compatible local tags) present - nothing to pull' }
    foreach ($img in $need) {
        $p = $sel | Where-Object { $_.local_fallbacks.image -contains $img -or $_.images -contains $img } | Select-Object -First 1
        $plat = if ($p -and $p.arch -notcontains 'arm64') { @('--platform', 'linux/amd64') } else { @() }
        $ok = $false
        for ($i = 1; $i -le $Retries -and -not $ok; $i++) {
          Say "pull $img (try $i/$Retries)"
          $job = Start-Job { param($a) & docker pull @a 2>&1 } -ArgumentList (, (@($plat) + $img))
          if (Wait-Job $job -Timeout $PullTimeoutSec) { Receive-Job $job | Select-Object -Last 2 | ForEach-Object { Say "  $_" } }
          else { Stop-Job $job; Say "  timeout after $PullTimeoutSec s" }
          Remove-Job $job -Force
          $ok = Test-Image $img
          if (-not $ok) { Start-Sleep -Seconds (5 * $i) }
        }
        if (-not $ok) { Write-Warning "could not pull $img (network / registry). Retry later: lab.ps1 pull $($p.id)" }
    }
    break
  }
  'up' {
    Assert-Docker
    $sel = Resolve-Profiles $Profiles
    Test-Ports $sel
    $ids = @($sel | Where-Object { $_.services.Count -gt 0 } | ForEach-Object id)
    if ($ids.Count) {
      $envl = Read-EnvLocal   # LAB_PLATFORM_<ID>=linux/arm64 (ARM-native local fallback) => not emulated
      $emulated = @($sel | Where-Object { $_.arch -notcontains 'arm64' } | ForEach-Object id | Where-Object {
        $v = "LAB_PLATFORM_$($_.ToUpper())"; ([Environment]::GetEnvironmentVariable($v) ?? $envl[$v]) -ne 'linux/arm64' })
      if ($emulated.Count -and ((& docker version --format '{{.Server.Arch}}').Trim() -eq 'arm64')) {
        Write-Warning "amd64-only images run under emulation on this ARM64 host: $($emulated -join ', ')"
      }
      $img = Resolve-Images $sel
      foreach ($k in $img.chosen.Keys) { [Environment]::SetEnvironmentVariable($k, $img.chosen[$k]) }
      if ($img.pull) { Say "will pull (no compatible local tag): $($img.pull -join ', ')" }
      Say "starting: $($ids -join ', ')  (waits for health checks, up to $WaitTimeoutSec s)"
      Invoke-Compose $ids @('up', '-d', '--wait', '--wait-timeout', "$WaitTimeoutSec")
    }
    Invoke-PostInit $sel
    Show-Connections $sel
    Say 'next: lab.ps1 demo <source>   |   lab.ps1 status   |   lab.ps1 down'
    break
  }
  'status' {
    & docker ps -a --filter "label=com.docker.compose.project=$Project" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
    break
  }
  'down' {
    Assert-Docker
    if ($Profiles.Count -eq 0) { Invoke-Compose @('*') @('down', '--remove-orphans') ; Say 'all lab containers removed (data volumes kept; use reset to delete them)' }
    else {
      $sel = Resolve-Profiles $Profiles
      Invoke-Compose @($sel.id) @('rm', '-s', '-f')
    }
    break
  }
  'reset' {
    Assert-Docker
    $sel = Resolve-Profiles $Profiles -DefaultAll
    $ids = @($sel | Where-Object { $_.services.Count -gt 0 } | ForEach-Object id)
    if ($ids.Count) { Invoke-Compose $ids @('rm', '-s', '-f', '-v') }
    foreach ($p in $sel) {
      foreach ($svc in $p.services) {
        & docker volume inspect $svc *> $null
        if ($LASTEXITCODE -eq 0) { & docker volume rm $svc | Out-Null; Say "volume $svc removed" }
      }
      if ($p.id -eq 'files') { Remove-Item -Recurse -Force (Join-Path $Here 'data/*') -ErrorAction SilentlyContinue }
    }
    if ($Profiles.Count -eq 0) { & docker network rm $Catalog.network *> $null }
    break
  }
  'demo' {
    if ($Profiles.Count -ne 1) { throw 'usage: lab.ps1 demo <postgres|mysql|mariadb|sqlserver>' }
    $src = $Profiles[0]
    $py = if ($env:LAB_PYTHON) { $env:LAB_PYTHON } else { 'python' }
    $env:ALELT_PG_ADMIN_URL = 'postgresql://lab:lab_local_only@127.0.0.1:47442'
    switch ($src) {
      'postgres'  { $env:ALELT_PG_SOURCE_CONTAINER = 'alelt-lab-postgres' }
      'mysql'     { $env:ALELT_MYSQL_TEST_PORT = '47306'; $env:ALELTTEST_MYSQL_PASSWORD = 'lab_local_only' }
      'mariadb'   { $env:ALELT_MARIADB_TEST_PORT = '47307'; $env:ALELTTEST_MYSQL_PASSWORD = 'lab_local_only' }
      'sqlserver' { $env:ALELT_MSSQL_CONTAINER = 'alelt-lab-sqlserver' }
      default { throw "no demo pipeline for '$src' yet (available: postgres, mysql, mariadb, sqlserver)" }
    }
    Say "demo pipeline: $src -> remote capture -> hub (throw-away metadata) -> worker -> landing (alelt-lab-landing)"
    if (-not $env:ALELT_DEMO_HARNESS -or -not (Test-Path $env:ALELT_DEMO_HARNESS)) { Say 'End-to-end examples are coming in a later release. (set ALELT_DEMO_HARNESS to a local harness)'; exit 2 }
    & $py $env:ALELT_DEMO_HARNESS --source $src
    $rc = $LASTEXITCODE
    Say "demo finished: exit $rc (0 PASS, 1 FAIL, 2 SKIP)"
    exit $rc
  }
  'helper' {
    $py = if ($env:LAB_PYTHON) { $env:LAB_PYTHON } else { 'python' }
    & $py (Join-Path $Here 'helper/lab_helper.py') @Profiles
    break
  }
}
