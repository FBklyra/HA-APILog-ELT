# Agent command contract

The hub talks to an edge agent only through typed, whitelisted commands (R5) that the agent pulls
(`GET /agent/v1/commands`, or the WebSocket control channel) and acknowledges (`POST /agent/v1/commands/{id}/ack`).
This page is the contract for the three agents that implement it: the C Windows service (`edge-agent/core`,
`aleltagent.exe` + `aleltsrc_db2.dll`), the Linux daemon (`edge-agent/linux/aleltd`) and the Python dev edge
(`connectors/sources/db2/devedge`). Linked from [DESIGN.md](../DESIGN.md) §4.1 and
[`connectors/sources/db2/DESIGN.md`](../connectors/sources/db2/DESIGN.md) §9.

## 1. Commands used by the table lifecycle and out-of-row completion

| Command | Args (typed, whitelisted — `alelt_common.models`) | Result | Caps |
|---|---|---|---|
| `SNAPSHOT_TABLE` | `table{schema,name}`, `snapshot_id`, `chunk_rows`, optional `key_from[]` (resume after this key), `chunk_from` (first chunk number of a resumed snapshot), `key_to[]` | chunks via `POST /agent/v1/snapshot`, header `snapshot_id, table, chunk_no, key_from, last_key, rows, last, start_pos, codec, format`; then the ack `{snapshot_id, chunks, rows, start_pos}` | `chunk_rows <= max_chunk_rows`, chunk <= 8 MiB, one table at a time per agent; the hub keeps <= `ALELT_SNAPSHOT_INFLIGHT_TABLES` (8) snapshots in flight per connection |
| `VERIFY_TABLE` (keyed fetch, completion) | `checks:["rows"]`, `key_values` (<= 1000), `columns` (key + requested out-of-row columns), `timeout_s` (<= 600, the hub asks 300) | `rows[]` (text forms), `missing[]`, `position_before` (= R, the log position read before the reads), `truncated` | result <= 8 MiB; args <= 60 KiB (the hub cuts commands at `ALELT_COMPLETION_CMD_BYTES`, 48 KiB) |
| `VERIFY_TABLE` (reconciliation) | `checks` row_count / aggregates / keys, `ranges` | per range counts, aggregates, keys | as above |
| `SNAPSHOT_CATALOG` | `schemas` (optional) | catalog POSTed to `/agent/v1/catalog` | — |

`chunk_from` and `last_key` are new in this contract version. An agent that does not send `last_key` is re-issued
from the top of the table (same `snapshot_id`; the chunks after the last contiguous loaded chunk are discarded by the
hub). Text forms: values travel as the source's text form (Db2 CLI), binary values (BLOB, VARBINARY, FOR BIT DATA) as
upper-case hex; the hub types them by the catalog (`alelt_common.verify_edge.parse_text`).

## 2. Concurrency with capture and heartbeats

- Commands run on a **command thread** with its **own source connection** (Db2: a second CLI connection, read-only,
  `WITH CS`, same user): the capture thread keeps reading and shipping, the control path keeps heart-beating. One
  command at a time per agent (serial FIFO queue); SNAPSHOT_TABLE chunks are posted by the command thread; the CPU
  governor throttles the command thread like the capture thread.
  - Dev edge: `threading.Thread` + second `alelt_db2edge.Db2` connection; the poll loop only enqueues.
  - `aleltd`: same pattern (`command_loop`, `cmd_source` = a second source context).
  - C agent: capture runs between snapshot chunks and before a verify on the worker thread (AHS-06,
    `capture_interleave`), heartbeats run on the control thread. The dedicated command thread with a second plugin
    context is tracked as an agent hardening item (see `KNOWN-ISSUES.md`).
- A command id is executed once: a re-delivery of an unacknowledged command (the hub re-sends after
  `ALELT_COMMAND_REDELIVER_S`) is ignored while it is queued or running.
- Hub-side liveness: `agent.last_heartbeat` is also touched by `POST /blocks`, `POST /snapshot`,
  `GET /commands` and `POST /commands/{id}/ack`, so an agent busy with a long command is never declared stale
  (`ALELT_VERIFY_AGENT_STALE_S`) while it is visibly working.
- Hub pacing: <= `ALELT_SNAPSHOT_INFLIGHT_TABLES` (8) SNAPSHOT_TABLE and <= `ALELT_COMPLETION_INFLIGHT_CMDS` (4)
  completion VERIFY_TABLE outstanding per connection; reconciliation VERIFY_TABLE (priority 60) waits behind them.

## 3. Allow-list — one semantics everywhere

`snapshot.allowed_tables` entries are `SCHEMA.TABLE` (exact, case-insensitive after upper-casing) or `SCHEMA.*`
(every base table of the schema). A schema wildcard (`*.*`, `*.T`) is refused at config load. The deny list
(`deny_hosts`, production guard) is evaluated before the allow-list. Shared test vectors:
[`edge-agent/shared/allowlist_vectors.json`](../edge-agent/shared/allowlist_vectors.json), used by the dev edge
(`alelt_db2edge.table_allowed`), `aleltd` (`config.table_allowed`) and the C agent (`alelt_config_table_allowed`).
Installers emit the explicit list of enabled tables by default; `SCHEMA.*` is the documented choice for
`allow_new_tables: true` connections.

## 4. Errors and timeouts

- Ack: `{ok: true, result: {...}}` or `{ok: false, result: {code, message}}` with `code` in `NOT_ALLOWED`,
  `NOT_FOUND`, `NO_KEY`, `PAUSED`, `TOO_LARGE`, `TIMEOUT`, `SOURCE_ERROR`, `UNSUPPORTED`. The hub stores the result in
  `agent_command.result` and branches on the code: `NOT_ALLOWED` / `NOT_FOUND` / `NO_KEY` / `UNSUPPORTED` are refusals
  (operator), `PAUSED` / `TIMEOUT` / `SOURCE_ERROR` are retried later with backoff.
- Hub timeouts: completion VERIFY_TABLE `timeout_s=300`, hub expiry `ALELT_COMPLETION_CMD_TIMEOUT_S=360`;
  reconciliation as configured (`ALELT_VERIFY_EDGE_TIMEOUT_S`, 120 s); SNAPSHOT_TABLE has no overall timeout, but a chunk
  must arrive within `ALELT_SNAPSHOT_CHUNK_TIMEOUT_S` (600 s) of the previous one, else the command is re-issued with
  `key_from` (`ALELT_SNAPSHOT_MAX_REISSUES`, 5, then the snapshot is paused with `last_error` = terminal state).
