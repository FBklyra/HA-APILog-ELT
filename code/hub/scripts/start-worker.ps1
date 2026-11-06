# Hub worker: decode -> txn -> load (metrics on :9100)
. "$PSScriptRoot\dev-env.ps1"
python -m alelt_hub.worker
