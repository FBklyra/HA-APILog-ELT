# L0 - Dev

**Scope.** Agent installs/uninstalls as a Windows service, registers, heartbeats; no Db2 access. Command whitelist rejects unknown commands (R5).

**Exit criterion (test ladder, DESIGN.md §13).** 24 h heartbeat, clean restart/stop, no listening ports

| File | Purpose |
|---|---|
| `l0_service.ps1` | Elevated PowerShell: install (`aleltagent.exe --install --hub-url`), start, R4 port check (TCP+UDP), priority, restart, stop, Event Log errors, uninstall. Starts `l0_link.py`, then merges parts. |
| `l0_link.py` | Runs `edge-agent/core/tests/mock_hub.py` (real protocol v1: request/response + long-poll `GET /agent/v1/commands`, every payload validated against `alelt_common.models`; the C agent has no WebSocket). Sends `SET_LIMITS`, `PROBE` and non-whitelisted `EXEC_SQL`; checks register, heartbeat continuity, ack ok / refusal. `--exe` runs `aleltagent.exe --console` itself (no admin); `--external` when `l0_service.ps1` runs the service. |

## Run

```
# full (24 h), elevated:
powershell -File l0_service.ps1 -LinkDurationSec 86400      # default exe: edge-agent\core\build\x64-release\bin\aleltagent.exe
# link only, real agent in console mode, no admin (dev evidence, part result.link.json):
python l0_link.py --duration 300
# shortened dev/CI run
powershell -File l0_service.ps1 -LinkDurationSec 300
# prove the fake hub itself (CI, no agent) -> result.selftest.json (never counts for L0)
python l0_link.py --self-test --duration 10
```

Writes `L0/result.json` (schema: `tests/ladder/_common/ladder_result.py`). Exit code 0 = PASS, 1 = FAIL, 2 = SKIP (prerequisite missing or gate closed).

Parts `result.service.json` + `result.link.json` are merged into `result.json`. Agent CLI used: `--install --no-start --config --hub-url --agent-id --connection-id --database --mode dev --spool-dir`, `--set-dev-token` (stdin, DPAPI), `--uninstall`. PROBE is answered only once the source is open, so at L0 (no Db2) it stays queued and is not a gate; SET_LIMITS proves the whitelisted path.
