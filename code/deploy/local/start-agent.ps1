# Start the host capture agent (Db2 runs natively on Windows) against the ALELT containers.
#   .\start-agent.ps1                      # dev edge, continuous, token from .env ALELT_AGENT_TOKENS
#   .\start-agent.ps1 -Once                # read to the current end of log, ship, exit
#   .\start-agent.ps1 -Config C:\path\devedge.json -Token alelte_...   # config + one-time token from the setup wizard
# Paths are quoted everywhere: the repository path may contain '&'.
[CmdletBinding()]
param(
    [string]$HubUrl = 'http://localhost:8080',
    [string]$Token = $env:ALELT_AGENT_TOKEN,
    [string]$Config = '',
    [string]$AgentId = 'local-agent',
    [string]$ConnectionId = $(if ($env:ALELT_CONNECTION_ID) { $env:ALELT_CONNECTION_ID } else { 'db2-local' }),
    [string]$Database = $(if ($env:ALELT_DB2_DATABASE) { $env:ALELT_DB2_DATABASE } else { 'SAMPLE' }),
    [string[]]$Schemas = @($(if ($env:ALELT_DB2_SCHEMA) { $env:ALELT_DB2_SCHEMA } else { 'APP' })),
    [string]$Python = $env:ALELT_EDGE_PYTHON,
    [switch]$Once,
    [switch]$Native          # use aleltagent.exe --console when built (edge-agent/core)
)
$ErrorActionPreference = 'Stop'
$Repo = (Resolve-Path "$PSScriptRoot\..\..").Path
$state = Join-Path $Repo '.alelt\local-agent'
New-Item -ItemType Directory -Force -Path $state | Out-Null

if (-not $Token) {
    $envFile = Join-Path $PSScriptRoot '.env'
    if (Test-Path -LiteralPath $envFile) {
        $line = Get-Content -LiteralPath $envFile | Where-Object { $_ -match '^ALELT_AGENT_TOKENS=' } | Select-Object -First 1
        if ($line) { $Token = ($line -replace '^ALELT_AGENT_TOKENS=', '').Split(',')[0].Trim() }
    }
}
if (-not $Token) { throw 'No agent token: pass -Token, set $env:ALELT_AGENT_TOKEN, or ALELT_AGENT_TOKENS in deploy/local/.env' }
$env:ALELT_AGENT_TOKEN = $Token

try { $v = Invoke-RestMethod -Uri "$HubUrl/version" -TimeoutSec 5; Write-Host "hub $HubUrl ok (ALELT $($v.version))" }
catch { throw "hub not reachable at $HubUrl - is the stack up? (cd deploy/local; docker compose up -d)" }

if ($Native) {
    $exe = Get-ChildItem -LiteralPath (Join-Path $Repo 'edge-agent\core') -Recurse -Filter aleltagent.exe -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if (-not $exe) { throw 'aleltagent.exe not built (edge-agent/core); use the dev edge (omit -Native)' }
    $cfgNative = if ($Config) { $Config } else { Join-Path $Repo 'edge-agent\core\aleltagent.example.json' }
    & $exe.FullName --console --config "$cfgNative"
    exit $LASTEXITCODE
}

if (-not $Python) { $Python = "$env:LOCALAPPDATA\Programs\Python\Python310\python.exe" }
if (-not (Test-Path -LiteralPath $Python)) { throw "x64 Python for the dev edge not found: $Python (set ALELT_EDGE_PYTHON)" }

if ($Config) {
    $cfg = Get-Content -LiteralPath $Config -Raw | ConvertFrom-Json
} else {
    $cfg = Get-Content -LiteralPath (Join-Path $Repo 'connectors\sources\db2\devedge\devedge.json') -Raw | ConvertFrom-Json
    $cfg.agent_id = $AgentId
    $cfg.connection_id = $ConnectionId
    $cfg.database = $Database
    $cfg.schemas = $Schemas
}
if (-not $cfg.db2_bin -or -not (Test-Path -LiteralPath $cfg.db2_bin)) {   # locate the local Db2 client
    $cands = @()
    if ($env:DB2PATH) { $cands += (Join-Path $env:DB2PATH 'BIN') }
    $cands += (Get-Content -LiteralPath (Join-Path $Repo 'connectors\sources\db2\devedge\devedge.json') -Raw | ConvertFrom-Json).db2_bin
    $cands += 'C:\Program Files\IBM\SQLLIB\BIN', 'D:\IBM\SQLLIB\BIN'
    $found = $cands | Where-Object { $_ -and (Test-Path -LiteralPath $_) } | Select-Object -First 1
    if (-not $found) { throw "Db2 client BIN directory not found (set db2_bin in the config or DB2PATH)" }
    $cfg.db2_bin = $found
}
$cfg.hub_url = $HubUrl
$cfg.token_env = 'ALELT_AGENT_TOKEN'
$cfg.spool_dir = (Join-Path $state 'spool')
$cfg.state_file = (Join-Path $state "$($cfg.agent_id)-state.json")
$out = Join-Path $state 'devedge.json'
$cfg | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $out -Encoding utf8

$edge = Join-Path $Repo 'connectors\sources\db2\devedge\devedge.py'
$argv = @("$edge", '--config', "$out")
if ($Once) { $argv += '--once' }
Write-Host "dev edge: agent=$($cfg.agent_id) connection=$($cfg.connection_id) db=$($cfg.database) -> $HubUrl"
& "$Python" @argv
exit $LASTEXITCODE
