# Destination kit — shared rules and the edge-value matrix

`alelt_dst_kit` is the base of every destination (`DestinationBase`), with the conformance suite
(`alelt_dst_kit/conformance.py`) that every engine runs.

## One DELETE rule (XC-07)

A DELETE for a key the destination does not hold writes a **tombstone**: the key, the deleted flag, the synced column,
and whatever before-image columns were delivered (all other columns NULL). The delete is never dropped silently.
Conformance cases: `test_delete_unknown_key`, `test_delete_key_only_before`. Snowflake applies the rule through a
second `WHEN NOT MATCHED AND op = 'D'` clause, and its SQL generation is tested.

## Edge values: stored exactly or refused loudly (XC-31)

`test_edge_values_stored_exactly_or_refused_loudly` sends each value in its own batch. The engine must either store it
**exactly** or **refuse** the batch with an error. On a refusal the checkpoint is unchanged and no row is visible.
Storing a different value (rounded, NULL instead of NaN, clamped date) fails the test.
- Set `ALELT_CONFORMANCE_MATRIX=<file>` to append each engine's outcomes as JSON lines.
- `edge_outcomes` on a conformance class pins an engine's documented outcome.

The table below shows the outcomes measured on 2026-11-04. Any case not listed for an engine was stored exactly.

| Engine (test target) | Refused |
|---|---|
| memory, Kafka (memory + Redpanda) | — (values are stored as delivered text / JSON) |
| PostgreSQL, Redshift stand-in (PostgreSQL) | NUL character in text, sub-microsecond timestamp text |
| SQL dialect postgres | NUL character in text, sub-microsecond timestamp text |
| SQL dialects mysql, mariadb, sqlserver; Fabric stand-in (SQL Server) | NaN, +Infinity, -Infinity, sub-microsecond timestamp text |
| SQL dialect sqlite | NaN (SQLite would store NULL), sub-microsecond timestamp text |
| SQL dialect duckdb | sub-microsecond timestamp text |
| Iceberg (local, S3, REST), Delta (local, S3) | sub-microsecond timestamp text |
| ClickHouse | dates / timestamps outside 1900-01-01 … 2299-12-31 (Date32 / DateTime64 range), sub-microsecond timestamp text |
| BigQuery emulator | see `bigquery/README.md` (emulator limits) |
| Snowflake, Databricks | not measured (no account here); NaN/±Inf are valid FLOAT values on both |

Cases: NaN, ±Infinity, NUL inside text, DECIMAL(38,10) with 28 integer digits, -1e-10, dates 0001-01-01 /
9999-12-31 / 1899-12-31, timestamps 0001-01-01 / 9999-12-31 23:59:59.999999 UTC, an instant with a +02:00 offset, and
sub-microsecond timestamp text.

Rules behind the matrix:
- **Sub-microsecond timestamps.** Values with more than 6 fractional digits arrive as text (today: Db2
  TIMESTAMP(7..12)). Every engine here stores microseconds, so `DestinationBase.refuse_lossy_values`
  refuses non-zero digits below the microsecond before anything is written. Zero padding (`.123456000`) is exact and
  accepted. Engines that store the delivered text (Kafka, memory) are not affected.
- **Decimals.** Lake formats quantize to the column scale with an 80-digit context, so 38-digit values are exact. A
  value with more fractional digits than the column holds raises; it is never rounded (`arrowutil.exact_decimal`).
- **Instants.** DuckDB sessions run with `TimeZone = 'UTC'`, because a local zone overflows near 9999-12-31 when
  values are read back.

## Byte-sized VARCHAR (DST-redshift-03, DST-fabric-04)

Redshift and Fabric (UTF-8 `varchar`) size VARCHAR in bytes, while sources count characters. Their type maps set
`limits.STRING_bytes_per_char = 4`, so a source VARCHAR(n) becomes VARCHAR(4n). Above the engine maximum the
over-max type is used (Redshift VARCHAR(65535), Fabric varchar(max)). Existing narrower columns widen through the
normal widening path.
