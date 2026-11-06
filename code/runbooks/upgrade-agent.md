# Upgrade the edge agent

Package and scripts: [`deploy/agent/`](../deploy/agent/). Protocol compatibility: hub accepts the agent's `X-ALELT-Protocol` version (check release notes before starting).

| # | Step | Check |
|---|---|---|
| 1 | Change ticket; release passed L0–L2 in dev and L6 in lab; MSI signed | Release notes, ladder `result.json` |
| 2 | Upgrade hub first if the release requires it | Console Hub version |
| 3 | Console → Agent → **Pause** (kill switch A) | State `Paused`, spool draining to 0 |
| 4 | Wait until spool = 0 (all batches ACKed) | `alelt_spool_bytes{location="agent"}` = 0 |
| 5 | On Db2 server: `msiexec /i ALELTAgent-<ver>.msi /qn /l*v upgrade.log` (service stopped/started by MSI; config and cert preserved) | `sc query ALELTAgent` RUNNING |
| 6 | Console → Agent shows new version, state `Idle`/`Paused`; no listening port (`netstat`) | — |
| 7 | **Resume**; agent continues from durable position | Lag recovers; no gap |
| 8 | Observe 1 h: CPU within cap, no governor storms | Agent screen |

Rollback: reinstall previous MSI the same way (steps 3–7). Position state is on the hub/destination, so downgrade is safe while the protocol version is unchanged.
