# Linux install (systemd, no Docker)

| Unit | Role | Runs |
|---|---|---|
| `alelt.service` | edge agent next to a source database | `python -m aleltd` (`edge-agent/linux`) |
| `alelt-hub.service` | hub API + console + agent gateway (schema upgrade before start) | `alelt_hub.cli api` |
| `alelt-worker@N.service` | hub worker N (decode → txn → load); template, any number | `alelt_hub.cli worker` |
| `alelt-capture.service` | hub-side remote capture runners (`alelt_capture`) | `alelt_hub.cli capture` |

Paths: code `/opt/alelt/app` (root-owned, read-only), venv `/opt/alelt/venv`, config `/etc/alelt`
(root:alelt 0640), state `/var/lib/alelt` (`StateDirectory=`, agent spool + `spool/state.json`), user/group `alelt`.
Logs: journald only.

## Install

```sh
sudo ./install.sh                                   # agent (Python >= 3.10, systemd)
sudo ./install.sh --engines "postgres mysql"        # + client libraries for remote engines
sudo ./install.sh --hub --workers 2 --capture       # hub API + 2 workers + remote capture
sudo ./install.sh --all --wheels /media/wheels      # offline (pip wheel dir)
```

Then edit `/etc/alelt/aleltagent.json` (same keys as the Windows `aleltagent.json`, example in this folder) and
`/etc/alelt/agent.env` (token, source password), and `sudo systemctl start alelt`. The installer never overwrites
`/etc/alelt` or `/var/lib/alelt`. Uninstall: `sudo ./uninstall.sh` (keeps config + state) or `--purge`.

| Engine | Agent needs |
|---|---|
| db2 | IBM Data Server client on the host; `source.dll_dirs: ["<sqllib>/lib64"]`; drop-in below |
| postgres / mysql / mariadb / sqlserver / oracle | `--engines ...` (psycopg2-binary / PyMySQL / python-tds / oracledb) |

Db2 drop-in (instance home is under `/home`, local connections use the instance's IPC):

```ini
# /etc/systemd/system/alelt.service.d/db2.conf
[Service]
ProtectHome=read-only
Environment=DB2INSTANCE=db2inst1
SupplementaryGroups=db2iadm1
```

## Operate

| Task | Command |
|---|---|
| status | `systemctl status alelt` (STATUS line from sd_notify) |
| follow logs | `journalctl -u alelt -f` · JSON only: `journalctl -u alelt -o cat` |
| warnings and errors | `journalctl -u alelt -p warning` (priority set per line) |
| since boot / one stop | `journalctl -u alelt -b` · `journalctl -u alelt --since "10 min ago"` |
| reload config (limits, log level, governor, deny_hosts) | `systemctl reload alelt` (SIGHUP); other keys need a restart |
| positions | `sudo -u alelt /opt/alelt/venv/bin/python -m aleltd --config /etc/alelt/aleltagent.json --print-state` |
| validate config | `... -m aleltd --config /etc/alelt/aleltagent.json --check-config` |

Logs: one JSON object per line, INFO/DEBUG on stdout, WARNING+ on stderr. Under journald each line carries a
`<N>` priority prefix (stored as PRIORITY) and no `ts` field (journald timestamps it; `log.timestamps: always`
keeps it).

## Graceful stop (what `systemctl stop` does)

`SIGTERM` (`KillMode=mixed`) → the signal handler only sets an event → the main loop stops reading and runs the
checkpoint: in-flight complete transactions are written to the spool (fsync), `state.json` is replaced atomically
(tmp → fsync → rename → fsync dir: next/durable/last-read positions, capturing flag, lease epoch), the spool is
shipped for up to `shutdown.drain_s` (10 s), the HA lease is released, final heartbeat `STOPPING`, exit 0.
`shutdown.deadline_s` (45 s) bounds all of it: past it a watcher thread writes `state.json` and exits even if a
source call hangs. `TimeoutStopSec=60` is above that, so systemd never has to SIGKILL a healthy agent.

| Exit | Meaning | systemd |
|---|---|---|
| 0 | stopped (SIGTERM/SIGINT or hub `SHUTDOWN`) | stays stopped |
| 1 | runtime failure (checkpoint still written) | restarted after 10 s (`Restart=on-failure`, 5 tries / 10 min) |
| 78 | config invalid, `deny_hosts` match, `environment: PROD` without `allow_prod: true`, no cgroup memory cap with `require_job_limits` | not restarted (`RestartPreventExitStatus=78`) |

Watchdog: `WATCHDOG=1` is sent only while the main loop makes progress; a stall over `WatchdogSec=120` makes
systemd restart the agent (resume from the spool / `state.json`).

## HA pair (active/passive)

Two hosts, same `connection_id`, different `agent_id`, both `"ha": {"mode": "active_passive", "lease_ttl_s": 30,
"renew_s": 10}`. Only the lease holder opens the source; the standby heartbeats `STANDBY` and polls the lease.
`systemctl stop alelt` on the active releases the lease → the standby takes over at its next poll (≈ `renew_s`);
a crash → after at most `lease_ttl_s + renew_s`. A takeover discards the standby's stale spool and resumes at the
hub durable position (same rules as the Windows agent and `hub/alelt_common/lease_client.py`). Db2 HADR: one agent on
the primary, one on the standby host.

## Upgrade without losing the position

1. Optional for HA pairs: upgrade the standby first, then stop the active (the lease moves).
2. `sudo ./install.sh` from the new source tree: it replaces `/opt/alelt/app` + the venv, keeps `/etc/alelt` and
   `/var/lib/alelt`, and restarts the units — the restart is a graceful stop (checkpoint) + start.
3. On start the resume point is: newest spooled batch `next_pos` → hub durable position → `state.json` →
   `capture.start`. Batches already spooled are re-sent; the hub acks duplicates by `batch_id` and refuses gaps, so
   nothing is skipped or loaded twice. Check with `--print-state` and `journalctl -u alelt -b`.

Never delete `/var/lib/alelt/spool` while batches are in it (data that is not yet durable at the hub).
