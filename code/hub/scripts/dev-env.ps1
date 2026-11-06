# ALELT local dev environment. Dot-source it:  . .\hub\scripts\dev-env.ps1
# Reads the local docker Postgres credentials at runtime (never stored in files, R9).
# Container: $env:ALELT_PG_DEV_CONTAINER (default alelt-test-pgland, host :15435). Never the local-* containers (X-109).
$ErrorActionPreference = 'Stop'
$Repo = (Resolve-Path "$PSScriptRoot\..\..").Path
$pgContainer = if ($env:ALELT_PG_DEV_CONTAINER) { $env:ALELT_PG_DEV_CONTAINER } else { 'alelt-test-pgland' }
$envs = docker inspect $pgContainer --format '{{range .Config.Env}}{{println .}}{{end}}'
$pgUser = (($envs | Select-String '^POSTGRES_USER=') -replace '^POSTGRES_USER=', '').Trim()
$pgPass = (($envs | Select-String '^POSTGRES_PASSWORD=') -replace '^POSTGRES_PASSWORD=', '').Trim()
$port = (docker port $pgContainer 5432/tcp | Select-Object -First 1).Split(':')[-1].Trim()
$pw = [uri]::EscapeDataString($pgPass)
$env:ALELT_PG_ADMIN_URL = "postgresql://${pgUser}:$pw@localhost:$port"
$env:ALELT_DB_URL = "postgresql+psycopg://${pgUser}:$pw@localhost:$port/alelt"
$env:ALELT_DEST_URL = "postgresql://${pgUser}:$pw@localhost:$port/alelt_landing"
$env:ALELT_TEST_PG_URL = $env:ALELT_PG_ADMIN_URL
$env:ALELT_ENV = 'dev'
$env:ALELT_DEV_AUTH = '1'
$env:ALELT_AGENT_AUTH = 'token'
if (-not $env:ALELT_AGENT_TOKEN) { $env:ALELT_AGENT_TOKEN = [guid]::NewGuid().ToString('N') }   # per-session dev token
$env:ALELT_AGENT_TOKENS = $env:ALELT_AGENT_TOKEN
$env:ALELT_SPOOL_DIR = "$Repo\.alelt\hub-spool"
$env:ALELT_LOAD_MAX_LATENCY_S = '0'
$env:ALELT_WORKER_METRICS_PORT = '9100'
$env:PYTHONPATH = (@("hub") + @("db2", "postgres", "mysql", "mariadb", "oracle", "sqlserver" | ForEach-Object { "connectors\sources\$_" }) + @("_spi", "postgres", "snowflake", "sql", "iceberg", "delta", "kafka", "clickhouse", "bigquery" | ForEach-Object { "connectors\destinations\$_" }) | ForEach-Object { Join-Path $Repo $_ }) -join ";"
$env:PYTHONIOENCODING = 'utf-8'
Write-Host "ALELT dev env set (metadata db=alelt, landing db=alelt_landing, port $port)"
