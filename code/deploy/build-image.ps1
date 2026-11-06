# Build the ALELT product image with docker buildx (multi-arch; amd64 can be built from an ARM64 laptop).
#   .\deploy\build-image.ps1                                              # linux/<host arch>, load as alelt:local
#   .\deploy\build-image.ps1 -Platform linux/amd64 -Verify                # + decoder C tests and golden corpus (amd64, emulated)
#   .\deploy\build-image.ps1 -Platform linux/amd64,linux/arm64 -Tag registry.example.com/alelt:0.1.0 -Push
#   .\deploy\build-image.ps1 -Platform linux/amd64 -Tag alelt:0.1.0 -Output tar   # dist\alelt-0.1.0-amd64.tar
# Offline install of a tar: copy it to the server, then  docker load -i alelt-0.1.0-amd64.tar
[CmdletBinding()]
param(
    [string]$Platform = '',
    [string]$Tag = 'alelt:local',
    [switch]$Push,
    [ValidateSet('', 'load', 'tar')][string]$Output = '',
    [switch]$Verify,
    [string]$Version = '0.1.0',
    [string]$Builder = '',
    [string]$OutDir = ''
)
$ErrorActionPreference = 'Stop'
$Repo = (Resolve-Path "$PSScriptRoot\..").Path
$Dockerfile = Join-Path $Repo 'deploy\image\Dockerfile'
if (-not $OutDir) { $OutDir = Join-Path $Repo 'dist' }
if (-not $Platform) { $Platform = if ((docker version --format '{{.Server.Arch}}') -eq 'arm64') { 'linux/arm64' } else { 'linux/amd64' } }
$platforms = @($Platform.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ })
if (-not $Output -and -not $Push) { $Output = if ($platforms.Count -eq 1) { 'load' } else { 'tar' } }

# Host arch only: the default builder (shares the local image cache, loads directly). Foreign or several archs:
# a docker-container builder with QEMU emulation (binfmt ships with Docker Desktop); the C decoder is cross-compiled.
$hostPlatform = if ((docker version --format '{{.Server.Arch}}') -eq 'arm64') { 'linux/arm64' } else { 'linux/amd64' }
if (-not $Builder) {
    $foreign = @($platforms | Where-Object { $_ -ne $hostPlatform })
    $Builder = if ($foreign.Count -eq 0 -and -not $Push -and $Output -ne 'tar') { 'default' } else { 'aleltbuilder' }
}
if ($Builder -ne 'default') {
    docker buildx inspect $Builder *> $null
    if ($LASTEXITCODE -ne 0) { docker buildx create --name $Builder --driver docker-container --bootstrap | Out-Null }
}
$rev = 'local'
try { $rev = (git -C "$Repo" rev-parse --short HEAD 2>$null) } catch { }
if (-not $rev) { $rev = 'local' }
$builderArgs = if ($Builder -eq 'default') { @() } else { @('--builder', $Builder) }   # default = current builder
$common = @('buildx', 'build') + $builderArgs + @('-f', "$Dockerfile",
    '--build-arg', "ALELT_BUILD_VERSION=$Version", '--build-arg', "ALELT_BUILD_REVISION=$rev",
    '--build-arg', "ALELT_BUILD_DATE=$((Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ'))")

function Invoke-Docker([string[]]$a) {
    Write-Host "docker $($a -join ' ')"
    & docker @a
    if ($LASTEXITCODE -ne 0) { throw "docker buildx failed ($LASTEXITCODE)" }
}

if ($Verify) {   # decoder C tests + golden corpus with the native decoder, per target arch (emulated if foreign)
    foreach ($p in $platforms) { Invoke-Docker ($common + @('--platform', $p, '--target', 'verify', '--progress', 'plain', "$Repo")) }
}
if ($Push) {
    Invoke-Docker ($common + @('--platform', ($platforms -join ','), '-t', $Tag, '--push', "$Repo"))
} elseif ($Output -eq 'load') {
    if ($platforms.Count -ne 1) { throw '-Output load needs exactly one platform (use -Output tar or -Push)' }
    Invoke-Docker ($common + @('--platform', $platforms[0], '-t', $Tag, '--load', "$Repo"))
} elseif ($Output -eq 'tar') {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
    foreach ($p in $platforms) {   # one docker-loadable archive per architecture
        $arch = $p.Split('/')[-1]
        $name = ($Tag -replace '[/:]', '-') + "-$arch.tar"
        $dest = Join-Path $OutDir $name
        Invoke-Docker ($common + @('--platform', $p, '-t', $Tag, '--output', "type=docker,dest=$dest", "$Repo"))
        Write-Host "wrote $dest  (install: docker load -i $name)"
    }
}
