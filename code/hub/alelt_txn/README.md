# alelt_txn — transaction assembly

`TxnAssembler.feed(events, seq) -> [ChangeSet]`

| Behaviour | Detail |
|---|---|
| Buffer | DML per txid in log order; COMMIT emits a `ChangeSet` (commit order); ROLLBACK drops the txn |
| Savepoint rollback | `COMPENSATION` (undo record) cancels the newest live event of the txn (same table when given) — undo runs in reverse order, so the txn can still COMMIT correctly |
| Spill | over the byte budget the largest txn spills to disk (length-prefixed records + CRC32); any canonical value incl. `bytes` |
| Restart | in-memory only; `low_water_seq()` = first batch still needed by an open txn; caller replays from there and skips commits ≤ destination checkpoint |
| Long txns | `long_open()`, `oldest_open_seconds()` → metric `alelt_txn_oldest_open_seconds` |

Test: `pytest hub/alelt_txn/tests` (interleaving, rollback, savepoint, spill, binary values, replay identity).
