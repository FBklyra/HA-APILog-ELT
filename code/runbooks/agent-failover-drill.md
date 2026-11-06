# Agent failover drill — active/passive capture pair

Who: PlatformAdmin + DBA (Db2 hosts) · When: monthly, and before go-live · Guide: [deploy/HA.md §6](../deploy/HA.md#6-agent-activepassive-db2-hadr-primary--standby-hosts)

Prerequisite: two agents (different `agent_id`, same `connection_id`), both with `"ha": {"mode": "active_passive", "lease_ttl_s": 30, "renew_s": 10}`.
R1/R3 apply: no step writes to Db2 or touches archive logs.

## Before

| # | Check | Expected |
|---|---|---|
| 1 | Console → *Topology & HA* → connection | holder = agent A, epoch = N, standby agent B heartbeat < 30 s, state `STANDBY` |
| 2 | Lag (Overview) | under latency target |
| 3 | Note the last batch: `SELECT seq, agent_id, from_lri, next_lri FROM batch WHERE connection_id='<id>' ORDER BY seq DESC LIMIT 1` | reference for the chain check |
| 4 | Generate churn on a test table (or run during normal traffic) | — |

## Drill A — planned (stop the active service)

| # | Step | Expected |
|---|---|---|
| 1 | On host A: `aleltagent.exe --stop` (or `Stop-Service ALELTAgent`) | A releases the lease on clean stop |
| 2 | Watch *Topology & HA* | B becomes holder within one `renew_s` (release) or ≤ `lease_ttl_s` + `renew_s`; epoch = N+1 |
| 3 | Audit screen | `agent.lease.release` (A), `agent.lease.takeover` / `acquire` (B) with the new epoch |
| 4 | `aleltagent.exe --start` on host A | A registers and heartbeats `STANDBY` (does not take the lease back) |

## Drill B — unplanned (kill the process / pull the network)

| # | Step | Expected |
|---|---|---|
| 1 | Kill: `Stop-Process -Name aleltagent -Force` on host A — or block outbound to the hub (firewall rule / unplug NIC) | A stops renewing |
| 2 | Wait `lease_ttl_s` (+ one poll) | B acquires, epoch N+1; with the network cut, A **stops reading by itself** after `lease_ttl_s - renew_s` |
| 3 | Kill case: service recovery restarts A within 60 s | A comes back as `STANDBY` |
| 4 | Network case: restore the network | A's queued batches are refused `423 lease_fenced` (metric `alelt_batches_received_total{status="lease_fenced"}`), A turns standby; no duplicate, no gap |

## Verify (both drills)

| # | Check | Expected |
|---|---|---|
| 1 | Batch chain: `SELECT seq, agent_id, from_lri, next_lri FROM batch WHERE connection_id='<id>' AND seq >= <ref> ORDER BY seq` | each `from_lri` = previous `next_lri`; agent_id switches A → B once |
| 2 | All batches `LOADED`, no `FAILED` jobs | Overview / Hub health |
| 3 | Recon on the churned tables | green |
| 4 | Timings | takeover ≤ `lease_ttl_s` + `renew_s` (≈ 40 s default); record in the drill log |

## Rollback / fail back

- Nothing to undo: the pair is symmetric. To make A active again, stop B's service (planned drill on B) or console →
  *Topology & HA* → **Release lease** (reason required; audited), then restart B.
- If no agent takes the lease (both `STANDBY` > 2 × TTL): check both hosts reach the hub LB, `ha.mode` and
  `connection_id` match, and that a PAUSE from a topology drain is not pending (console → Agent → commands).
- Leave active/passive: set `ha.mode: standalone` on the remaining agent, stop the other, then
  `alelt topology lease-clear --connection <id>`.
