# Remote-capture sources — rules for connector authors (v1)

Remote sources (PostgreSQL, MySQL/MariaDB, SQL Server, Oracle, ...) are captured **from the hub** by the generic
runner `hub/alelt_capture` (worker role `capture`). The runner turns `SourceConnector.stream()` into ordinary hub
batches (same durable receipt as agent batches: blob store + `batch` row + `decode` job), so alelt_txn, alelt_loader,
alelt_verify and the destinations are unchanged. A remote connector therefore implements **only** the engine part
listed below. Reference implementation: `connectors/sources/postgres/alelt_src_postgres/`.

## 1. Packaging

| Item | Rule |
|---|---|
| Location | `connectors/sources/<engine>/alelt_src_<engine>/` (`connector.py` with the `SourceConnector` subclass), `README.md`, unit tests under `connectors/sources/<engine>/tests/`, golden corpus under `tests/golden/<engine>/`. |
| Registration | `ALELT_SOURCES` JSON (`{"<engine>": "alelt_src_<engine>.connector:<Class>"}`); the default in `alelt_hub/settings.py` lists the shipped connectors. Import failures are logged and the engine is shown as unavailable in the wizard. |
| Imports | `alelt_common` only (+ your client library). Never import `alelt_hub` / `alelt_capture` from a connector. |
| Client library | License compatible with Apache-2.0 (permissive, or LGPL used as a dynamic library). **No GPL.** Note the pip package in your README so packaging adds it to the image (`hub/alelt_hub/requirements.txt` note in EXCHANGE.md). |
| Clean room | Public, documented protocols / APIs only (X-13). |

## 2. Class contract

```python
class MySource(SourceConnector):
    engine = "mysql"                 # also Position.engine
    capture_mode = "remote"
    display_name = "MySQL / MariaDB"
    connection_fields = [...]        # wizard form: [{name, label, secret?, required?, default?}]

    def __init__(self, settings: dict | None = None, password: str | None = None, connection_id: str = "") -> None
        # no arguments => decode-only instance (hub decode path, never opens a connection)
```

| Method | Rule |
|---|---|
| `parse_position(text)` / `compare(a, b)` | Canonical text form, total order. Must not need a connection. |
| `canonical_type(col)` | Engine type → `CanonicalType`; unknown ⇒ `UNKNOWN` (column quarantined, R7). |
| `decode(raw, catalog, batch_id)` | **Always** `return alelt_common.canonical_jsonl.decode_batch(raw, self.engine, batch_id)` (§4). |
| `read_catalog()` | `CatalogSnapshot(connection_id, engine, taken_at, position=current position or None, tables=[...])`; `capture_enabled` = the table is really captured (in the publication / binlog filter / CDC-enabled). Columns: `ordinal` 0-based, `key_seq` 1..n for the PK, `canonical_type` set. Read-only queries. |
| `stream(from_pos)` | Generator, see §3. |
| `ack(pos)` | Called by the runner with a batch `next_pos` **after** the hub made that batch durable. The source may release log retention up to `pos` (Postgres `confirm_flush`, ...). Default no-op. Never called with a position that is not durable. |
| `current_position()` | Current end of the source log (snapshot start position). Optional (default `None`). |
| `snapshot_rows(table, key_range, chunk_rows)` | Iterator of row chunks (`list[dict[col → canonical value]]`), ordered by primary key; `key_range = (key_from exclusive, key_to inclusive)` as lists, either may be `None`. Consistent read (one snapshot / repeatable read). |
| `close()` | Release connections. Idempotent. Default no-op. |
| `prerequisite_checks(catalog, probes, tables, settings)` | `[{id, label, status PASS|FAIL|UNKNOWN|INFO, detail, check_sql?, fix_sql?}]`. `fix_sql` is shown to the DBA, **never executed** by ALELT. May connect (read-only) when `settings`/password were given to the constructor. |
| `read_catalog_remote(settings, password, schemas, connection_id)` | Wizard "test source": read-only catalog. |

`ack`, `close` and `current_position` are defined as no-op defaults on `alelt_common.spi.SourceConnector`.

## 3. `stream(from_pos)` semantics

| # | Rule |
|---|---|
| S1 | Yields `ChangeEvent`s (kinds `INSERT UPDATE DELETE TRUNCATE COMMIT ROLLBACK`, optionally `DDL`/`OTHER`) **or `None`** as an idle tick. Yield `None` at least every ~1 s when nothing arrives, so the runner can flush by time, ack and stop. |
| S2 | Whole transactions, **contiguous**, in **commit order**: all DML events of a txid, then its `COMMIT` (or `ROLLBACK`). No interleaving (buffer in the connector if the engine interleaves). Events of one txn share `txid`. |
| S3 | The position of every `COMMIT` / `ROLLBACK` event is a **resume token**: `stream(from_pos=p)` delivers exactly the transactions whose commit is after `p` (none twice, none skipped). Commit positions strictly increase. |
| S4 | DML event positions: any parseable position of the engine (informational, used in quarantine/diagnosis). |
| S5 | `from_pos=None` ⇒ start at the source's own retained position (e.g. replication slot) or at the current end. |
| S6 | `COMMIT` carries `commit_ts` (UTC). `before` = full old image when the engine provides it, else key columns only, else `None`. Columns whose value is not provided (e.g. unchanged TOAST) are **omitted** from `after` and listed in `note` (`unchanged:<COL>`); the loader keeps the destination value. |
| S7 | Values are canonical Python values (`bool int float Decimal str bytes date time datetime`); `INSTANT` = tz-aware UTC `datetime`, `LOCALDATETIME` = naive `datetime`; JSON / arrays = JSON text `str`. |
| S8 | On any connection / protocol error: raise. The runner closes the connector, backs off, reopens and resumes from the last **durable** position (never skips). |
| S9 | `ack()` / `close()` are called from the thread that iterates the generator, between two `next()` calls. |
| S10 | Read-only on the source except what the engine requires for capture and the DBA prepared (replication slot / publication are created by the DBA from the fix-it SQL; a connector may create its own slot only when `settings.create_slot` is true). |

## 4. Batch format (written by the runner, read by `decode`)

| Field | Value |
|---|---|
| Body | UTF-8 JSON Lines, one canonical `ChangeEvent` per line: `{"pos","txid","kind","schema","table","before","after","commit_ts","note"}` (`alelt_common.canonical_jsonl.encode_event`). |
| Values | JSON `null/bool/int/str` as is; tagged objects otherwise: `{"$dec":"12.30"}`, `{"$f":"1.5"\|"NaN"\|"Infinity"}`, `{"$bin":"<base64>"}`, `{"$date":"2026-11-03"}`, `{"$time":"12:00:01.5"}`, `{"$ldt":"2026-11-03T12:00:00"}`, `{"$ts":"2026-11-03T12:00:00+00:00"}`. |
| Codec | `zstd` when `zstandard` is installed, else `zlib`. |
| `BatchHeader` | `engine=<engine>`, `agent_id="capture:<connection_id>"`, `from_pos` = previous batch `next_pos` (X-25 chain), `to_pos` = `next_pos` = position of the last `COMMIT`/`ROLLBACK` in the batch, `rec_count` = events, `batch_id` = uuid5(connection, from_pos, to_pos) (a re-send after a crash is acked as duplicate). |
| Cut points | Batches end on a transaction boundary (after `COMMIT`/`ROLLBACK`); limits `ALELT_CAPTURE_BATCH_KB` (1024) and `ALELT_CAPTURE_BATCH_MS` (1000) are soft. |
| Large transactions (HC-08) | Never a stop for size. Connectors hold an open transaction in `alelt_common.spill.SpillBuffer` (memory `ALELT_CAPTURE_TXN_MEM_MB` = 64, then a process-private spill file) and deliver it lazily at commit. The runner ships an open transaction above `ALELT_CAPTURE_TXN_CHUNK_MB` (64) in **chunk batches** `from_pos = to_pos = next_pos = S` (S = the resume token before it; `batch_id` = uuid5(connection, S, S, chunk, txid, n)); the hub assembler collects it across batches, the checkpoint never moves inside it. After a restart the source re-delivers the transaction from S and the runner skips the lines already durable in the trailing chunk batches (count + txid from the hub). Only the spill disk cap `ALELT_CAPTURE_MAX_SPILL_MB` (65536) stops capture (`SpillLimitExceeded` -> ERROR + incident). |
| Notes / values | `note` grammar and canonical value rules: `docs/TYPE-MAP.md` (`alelt_common.notes`). |
| Undecodable line | `decode_batch` quarantines it with raw bytes (R7). |

## 5. Runner behaviour (what you can rely on)

| Step | Behaviour |
|---|---|
| Ownership | One active runner per connection: cross-process lock `capture:<connection_id>` (Postgres advisory lock / SQLite file lock). The runner registers itself as agent `capture:<connection_id>` (capability `remote`). |
| Start | `read_catalog()` → catalog version (dedup by content); refreshed every `ALELT_CAPTURE_CATALOG_S` (300 s) and when the connector reports a relation/schema change (`DDL` event). |
| Resume | Last hub batch `next_pos` (durable), else `settings.start_pos`, else `None` (S5). |
| Durability | `alelt_capture.receipt.receive()` = the gateway receipt (`alelt_hub.gateway._receive_batch`): blob store write, `batch` row, `decode` job — then `ack(next_pos)`. 503 backpressure ⇒ wait and retry the same batch, calling the connector's optional `keepalive()` every `ALELT_CAPTURE_KEEPALIVE_S` (10 s) meanwhile (PostgreSQL: standby status, so `wal_sender_timeout` does not end the session; XC-18). 409 gap ⇒ stop (bug), never re-chain silently. |
| Snapshot | `alelt_capture.snapshot.run_snapshot(...)`: `start_pos = current_position()`, then `snapshot_rows()` chunks → blob store + `snapshot_chunk` rows + `snapshot_load` jobs (same as `POST /agent/v1/snapshot`). |
| Run | `alelt capture` / `python -m alelt_capture [--connection ID] [--once]`: connections = `pipeline_definition` rows whose source engine is `remote` (settings + `secret_refs.password` resolved by `alelt_import.secrets.SecretStore`), or `ALELT_CAPTURE_CONNECTIONS` JSON `{"<cid>": {"engine", "settings", "password_env"}}`. |

## 6. Conformance checklist (each connector's tests)

1. Golden corpus of captured protocol bytes → expected canonical events (`tests/golden/<engine>/`).
2. `decode(encode(events)) == events` for every canonical type (use `alelt_common.canonical_jsonl`).
3. Rollback never emitted as DML; savepoint rollback excluded; PK update gives `before` key; delete gives key; truncate.
4. Restart: stop after N durable batches, restart → no gap, no duplicate.
5. Prerequisite checks show fix-it SQL for every FAIL.

## 7. Source completeness (X-132) — every record classified

Generic contract in `alelt_common/completeness.py`; DESIGN-REVIEW F-01 / F-02 / F-07 generalised to every engine.

| Rule | Detail |
|---|---|
| C1 | Every record / message / event / row the connector reads is counted in `self.census` (`SourceConnector.census`, per instance) as `decoded`, `ignored(reason)`, `needs_keyed_reread`, `needs_catalog_refresh` or `unsupported`; a record kind without a rule is `census.unexplained(...)` AND an `unsupported` signal. Never silently dropped, never "unchanged" by assumption. |
| C2 | `decoded` only per the vendor's public documentation (X-121). "Unchanged" values (`unchanged:<col>` note, loader keeps the destination value) only where the documentation says the value did not change AND the destination has the row (not on INSERT, not when the key changes). |
| C3 | Otherwise: re-read by key → emit the change you could decode (without the unknown columns) plus `FC.keyed_reread(table, [key], reason)`; no key available → `FC.unsupported(table, reason)` (hold + resync); table shape unknown → `FC.catalog_refresh(table, reason)` (+ re-read or hold). Connection-wide problems (e.g. a transaction whose start was not read) → `FC.unsupported(None, reason)`. |
| C4 | Signals travel as ordinary events: `signal.event(position, txid)` (`OTHER`; `DDL` for catalog refresh, so the runner refreshes the catalog), inside the transaction they belong to (before its COMMIT). Read them with `signal_of(ev)`. |
| C5 | `keyed_reread(table, keys) -> KeyedReread(rows, missing, read_from, isolation)`: hub-side SELECT by primary key in ONE consistent read (PostgreSQL REPEATABLE READ, MySQL/MariaDB consistent snapshot, Oracle `AS OF SCN`, SQL Server SNAPSHOT else READ COMMITTED); `read_from` = position taken before the read; `missing` = deleted rows. Edge engines implement it through the agent (whitelisted command). |
| C6 | Log retention / slot loss → raise a typed gap error (`SourceGapError` subclass or a name in `GAP_ERROR_NAMES`, e.g. `SlotLostError`, `BinlogPurgedError`, `RetentionGapError`) — never retry forever. The runner also stops a connection after `ALELT_CAPTURE_MAX_SAME_ERRORS` (20) identical failures without progress (circuit breaker + incident). |
| C7 | Census: `alelt census --source <engine> --from <pos> --to <pos> --settings '{...}' --password-env VAR` (`alelt_capture/census.py`) streams read-only (no ack) and prints the counts per class; exit 1 when anything is unexplained. Edge engines override `census_scan(from_pos, to_pos, idle_s, max_s)`. |
| C8 | Conformance: a golden corpus case for every completeness edge case of the engine, and a test that the census of the corpus has `unexplained == 0`. |

## keyed_reread and out-of-row completion (ADR-013)

`SourceConnector.keyed_reread(table, keys)` is called by the hub's completion stage (`alelt_hub/completion.py`) in the
control lane, in batches (up to `ALELT_COMPLETION_KEYS_PER_CMD` keys), for the keys of `needs_keyed_reread` signals whose
transaction committed. Only the columns named by the signals (`detail.columns`) are written from the returned rows;
`missing` keys are no-ops (the DELETE is in the log). `KeyedReread.read_from` semantics are unchanged: the position
taken before the consistent read; an entry is complete when a read at `read_from >= newest signal` was applied.
A connector without `keyed_reread` (NotImplementedError) makes the table `completion unavailable` (integrity → resync).
