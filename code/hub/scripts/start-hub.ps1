# Hub API (agent gateway + console API) on http://localhost:8080
. "$PSScriptRoot\dev-env.ps1"
python -m uvicorn alelt_hub.app:app --host 127.0.0.1 --port 8080 --no-access-log
