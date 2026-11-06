# Dev edge agent against a local Db2 database (x64 Python; stdlib only). Pass -Once to read to current and exit.
param([switch]$Once)
. "$PSScriptRoot\dev-env.ps1"
$py = $env:ALELT_EDGE_PYTHON
if (-not $py) { $py = "$env:LOCALAPPDATA\Programs\Python\Python310\python.exe" }
$cfg = "$Repo\connectors\sources\db2\devedge\devedge.json"
$args = @("$Repo\connectors\sources\db2\devedge\devedge.py", '--config', $cfg)
if ($Once) { $args += '--once' }
& $py @args
