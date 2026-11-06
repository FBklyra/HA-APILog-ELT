# Db2 source connector (hub side) — design and log format notes

Scope: `alelt_src_db2/` (Python, reference decoder + SPI), `decoder/` (C11 `libaleltdecode`, same behaviour),
`corpus/` (golden corpus builder). Edge capture (`edge/`, `devedge/`) is owned by other streams.

Sources: IBM Db2 public documentation (log records, db2ReadLog API), plus bytes observed on Db2 LUW
11.5.9 Community (Windows x64, local test database, codepage 1208). Everything marked **verified** is covered by
a local golden corpus, checked value-by-value against `SELECT`.

## 1. Pipeline

```
raw buffer (db2ReadLog, filter ON) ─ framing.iter_records ─ dms_header ─ dms_images ─ rowimage.decode_cells ─ cells_to_values
                                                                         (C: aleltd_next_record / aleltd_dms_images_read / aleltd_decode_row)
logdecode.decode_buffer → RawEvent / RawQuarantine (stdlib)  →  connector.Db2Source.decode → ChangeEvent / QuarantineRecord
```

- Stage split: **cells** (layout, C or Python, identical) → **values** (codepage, `Decimal`, date/time; Python only).
- `decode()` never raises on bad bytes. Any undecodable record → `QuarantineRecord` with LRI, reason, raw ref, hex (R7).
- All integers in log records and row images are **little-endian** (Db2 LUW on x86-64). A big-endian source
  (AIX) would need a different build — not supported.

## 2. Buffer framing (verified)

Buffer = repeated `[db2ReadLogFilterData 56 B][log record recordSize B]`. Several READ buffers can be concatenated.

| Off | Size | Field (filter header) |
|---|---|---|
| 0 | 24 | type-1 LRI: `u64 lriType(=1), u64 part1, u64 part2` |
| 24 | 24 | type-2 LRI (ignored) |
| 48 | 4 | `realLogRecLen` |
| 52 | 4 | `sqlcode` (≠0: Db2 could not decompress → quarantine) |

| Off | Size | Field (log manager header, 40 B) |
|---|---|---|
| 0 | 4 | recordSize (incl. header) |
| 4 | 2 | recordType: `0x004E` normal, `0x0043` CLR, `0x0084` commit, `0x008A` commit w/ pending list, `0x0041` abort, `0x0052` redo (LOB manager data, initialize table), `0x0055` undo (start of out-of-row data); every other type is classified by §9 (never skipped silently) |
| 6 | 2 | recordFlag (CLR: bit `0x0002` → 8 extra header bytes) |
| 8 | 8 | LSN |
| 16 | 8 | LFS |
| 24 | 8 | back-pointer LSO |
| 32 | 6 | TID → `txid` = 12 upper hex chars |
| 38 | 2 | log stream id |

Header size: 40; CLR 40 + 16 (+ 8 if flag & 2). Observed CLRs: flag `0x0202` → 64.
Position text: `"<part1 decimal>:<part2 hex16 upper>"`; order = `(part1, part2)` (`position.py`).

**Commit body**: `u32 UTC seconds`, `u32` (unknown, 1..3 observed), `u16 authid len`, authid (blank padded).
`commit_ts` = seconds, UTC (verified ±1 s vs wall clock). **Abort body**: `u16 len`, authid.

## 3. DMS records (component 1) (verified for 161/162/163/110/111/112)

DMS header 6 B: `u8 component, u8 function, u16 tbspaceid, u16 tableid` ↔ `SYSCAT.TABLES(TBSPACEID, TABLEID)`.

| Function | Meaning | Body after 6 B DMS header | Event |
|---|---|---|---|
| 162 | insert | `u16 internal, u16 len, u16 free, RID 6, u16 offset, sub-record` (image at +14) | INSERT after |
| 161 | delete | same; image = deleted row (full) | DELETE before |
| 163 | update | `[old part as 162][DMS header 6][new part as 162]` | UPDATE before+after |
| 111 / 112 (CLR) | undo delete / undo update | as 162; image = restored old row | COMPENSATION after, note `undo DELETE/UPDATE` |
| 110 (CLR) | undo insert | `u16, u16 len, u16, RID` — no image | COMPENSATION, note `undo INSERT` |
| 165 / 164 / 166 | insert / delete / undo-delete on empty page | image at +20 (documented, **not observed**) | as 162/161/111 |
| 124 | alter table attributes | bit masks | DDL |
| other | — | — | OTHER with note |

- Image length: always taken from the sub-record's own header (bytes 2..3). In an update, the `len` field of the
  *first* part holds the **new** length and vice versa — do not use it.
- With DATA CAPTURE CHANGES, updates carry **full** before and after images (verified).
- A primary-key update is logged as a single **UPDATE** (same RID) with different key values in before/after
  (verified, `pk_update`); downstream must treat it as delete(old key)+insert(new key).
- Rollback = DML records, then CLRs in reverse order, then abort (verified). `ROLLBACK TO SAVEPOINT` produces
  CLRs **inside a committed transaction** (verified, `savepoint`) → alelt_txn must apply COMPENSATION events, not
  only drop aborted transactions.
- Updates of Db2 internal control records (updatable record type ≠ 1) appear after CREATE/ALTER on captured
  tables → emitted as OTHER `internal control record`, not quarantined.

## 4. Sub-record / formatted user data record (verified)

| Off | Size | Field |
|---|---|---|
| 0 | 1 | (outer) record type — IBM "Data manager log records": a user record has type 0 or 0x10, or the **0x04 bit** set (overflow record, seen 0xC4 / 0xD4) with an inner type having bit 0x01 (formatted) or 0x02 (value-compressed). **0x02 bit without 0x04** (seen 0x42 / 0x52) = 12-byte overflow pointer: in an update the after image "is in the preceding insert log record for the transaction, which has the x04 bit set" (IBM). Anything else → quarantine |
| 1 | 1 | reserved |
| 2 | 2 | sub-record length (incl. these 4 bytes) |
| 4 | 1 | inner record type: bit 0x01 = fixed section + formatted record; 0x02 = value-compressed offset array (not decoded → quarantine); 0 = internal metadata (ignored) |
| 5 | 1 | reserved |
| 6 | 2 | **F** = length of fixed section |
| 8 | F | fixed section: columns in COLNO order |
| 8+F | … | variable section |

Variable offsets are **relative to byte 8** (start of the fixed section).

## 5. Column encoding in the fixed section (verified unless noted)

Each column occupies `width` bytes, followed by **one trailing null byte if nullable** (`0x00` value present,
`0x01` NULL; anything else → quarantine). NULL columns keep their width (zero-filled).

| Db2 type | Width | Encoding | Canonical |
|---|---|---|---|
| SMALLINT / INTEGER / BIGINT | 2 / 4 / 8 | signed LE | SHORT / INT / LONG |
| REAL / DOUBLE | 4 / 8 | IEEE LE | FLOAT / DOUBLE |
| DECIMAL(p,s) | p/2+1 | packed BCD, high nibble first, last nibble sign (`C`/`F`/`A`/`E` +, `B`/`D` −) | DECIMAL (`Decimal`, exact scale) |
| DATE | 4 | BCD `yyyymmdd` | LOCALDATE |
| TIME | 3 | BCD `hhmmss` | LOCALTIME |
| TIMESTAMP(p) | 7 + ⌈p/2⌉ | BCD `yyyymmddhhmmss` + p fraction digits (pad nibble) | LOCALDATETIME (`datetime` if p ≤ 6, else exact ISO text `YYYY-MM-DDTHH:MM:SS.fffffffffff`) |
| BOOLEAN | 1 | 0/1 | BOOLEAN |
| CHAR(n) / BINARY(n) | n | bytes (CHAR blank padded; kept) | STRING / BINARY |
| GRAPHIC(n) | 2n | UTF-16BE (codepage 1200) | STRING |
| VARCHAR / VARGRAPHIC / VARBINARY | 4 | `u16 offset, u16 length` (bytes) into var section | STRING / BINARY |
| CLOB / BLOB / DBCLOB | 4 | `u16 offset, u16 length` → LOB descriptor (below) | STRING / BINARY |
| DECFLOAT(16/34) | 8 / 16 (verified) | row encoding not in IBM docs → column skipped, `value_unavailable:<COL>`, **keyed re-read** | UNKNOWN |
| XML | 4 (verified: offset,len → 80-byte descriptor) | descriptor only (no XML records unless `DB2_DCC_XML_SERIALIZE`) → `value_unavailable:<COL>`, **keyed re-read** | XML |
| LONG VARCHAR/VARGRAPHIC | 4 (verified: offset,len → 34-byte long field descriptor) | data in long field manager records (512-byte sectors; descriptor undocumented) → `lob_unavailable:<COL>`, **keyed re-read** | STRING |
| VARCHAR/VARGRAPHIC/VARBINARY with offset bit 0x8000 | 4 | IBM: "stored out of row", 24-byte descriptor (contents undocumented) → `lob_unavailable:<COL>`, **keyed re-read** | |

Character data: codepage from `SYSCAT.COLUMNS.CODEPAGE` (1208 UTF-8 verified incl. `é à ç ô € 日本`;
0 = FOR BIT DATA → bytes; 1200 graphic → UTF-16BE; others via Python `cp<n>` codec, unverified).

**LOB descriptor** (inside var section):

| First byte | Meaning | Handling |
|---|---|---|
| `0x69 00 00 00` + data | inline LOB (fits INLINE LENGTH) | value = data (verified CLOB/BLOB) |
| `0x80` (1 byte) | empty LOB | `''` / `b''` (verified) |
| `0x49 …` (60 B observed) | out-of-row LOB; data logged separately by LOB manager (record type `0x0052`, component 5) | column omitted from the image, note `lob_unavailable:<COL>`, event kept **plus a `needs_keyed_reread` signal** (X-121, below) |

**Out-of-row values (X-121).** IBM documents the LOB manager records (12-byte header: component 5, operation
64 add LOB data / 65 add LOB amount / 66 delete / 67 non-update, LOB and parent object ids, column number, byte offset,
original operation insert/delete/update/concat) but **not** how a LOB record maps to a row, nor the out-of-row LOB
descriptor (0x49) or the 24-byte varchar descriptor; LOB columns may also be `NOT LOGGED`. So LOB data is **not**
assembled from the log: every INSERT / UPDATE / compensation image with an out-of-row value is emitted (other columns
decoded) together with a `needs_keyed_reread(table, key from the image)` signal in the same transaction; a table
without a key gets an `unsupported` signal (hold + resync). A LOB manager data record whose completed transaction has
no row record for its table → `unsupported`. LOB changes are never treated as "unchanged".

Columns beyond the fixed section length (row written before `ALTER TABLE ADD COLUMN`) → omitted, note
`absent:<COL>` (logic in place, not exercised by the corpus).

Extra checks: DECIMAL precision > 31, TIMESTAMP precision > 12, negative length/scale → unsupported.

## 6. Error codes (Python `rowimage.Err` = C `aleltd_err`)

`SHORT 1, SUBREC_TYPE 2, NOT_USER_DATA 3, BAD_FIXED_LEN 4, BAD_NULL_FLAG 5, BAD_BCD 6, BAD_VAR_REF 7,
UNSUPPORTED_TYPE 8, BAD_BOOL 9, ARG 10, VALUE_COMPRESSED 12, POINTER 13, LAYOUT 14` (Python only: `BAD_CODEPAGE 11`;
C only: `FRAMING 20, LAYOUT 21` (DMS layout), `END 100`). C ABI version 2 (`aleltdecode_cffi` refuses another ABI and the
hub falls back to Python). Structural checks (both decoders): the catalog columns must consume the fixed section
exactly (`LAYOUT`) and var-length data must lie after it (`BAD_VAR_REF`) — a wrong catalog version fails instead of
decoding garbage (SRC-db2-04).

## 7. Catalog (`catalog.py`)

Read-only `SELECT … WITH UR` on `SYSCAT.TABLES / COLUMNS / DATATYPES (distinct types → source type) /
KEYCOLUSE+TABCONST (PK) / INDEXES+INDEXCOLUSE (first unique regular index if no PK)` and
`SYSIBMADM.DBCFG codepage`, then COMMIT + db2ReadLog QUERY → snapshot `position`. Output = `CatalogSnapshot`
(dict form is stdlib only so it runs on the x64 Python next to the Db2 client). `compression` =
`COMPRESSION[/ROWCOMPMODE]`; value compression (`V`/`B`) → quarantine (different row format, not implemented).

## 8. Verification status

| Area | Status |
|---|---|
| Framing, commit/abort, CLR header sizes | verified (corpus + whole test-database log, 530 KB) |
| INSERT/UPDATE/DELETE images, CLR 110/111/112 | verified byte-exact vs SELECT |
| All test-table types (CHAR, VARCHAR, DECIMAL, INTEGER, SMALLINT, BIGINT, DOUBLE, DATE, TIME, TIMESTAMP, CLOB inline, NULLs, identity, row-change timestamp) | verified |
| Extra: REAL, DECIMAL(31,8), FOR BIT DATA, TIMESTAMP(0/3/12), BOOLEAN, GRAPHIC, VARGRAPHIC, BINARY, VARBINARY, inline BLOB | verified |
| Overflow (0xC4/0xD4) / pointer (0x42/0x52) records | verified (local census run): x04 insert + x02 update paired into one UPDATE; x04 delete decoded, pointer delete ignored; their CLRs balanced (undo of the overflow insert matched by RID) |
| Out-of-row LOB / varchar, DECFLOAT, XML values | not decoded by design: event + `needs_keyed_reread` signal (census corpus `lob_out_of_row`, `decfloat_xml`) |
| Row compression (ADAPTIVE, dictionary built) | verified: `realLogRecLen` < record size, db2ReadLog returns decompressed images, byte-exact vs SELECT (`row_compression`) |
| Value compression | detected (inner type 0x02) → quarantine (`value_compression`) |
| LONG VARCHAR (INCLUDE LONGVAR COLUMNS), range-partitioned tables, LOAD | verified (census corpus `long_varchar`, `partitioned`, `load`) |
| Non-1208 codepages, empty-page DMS variants, ADD COLUMN images, insert multiple records (167/168), heuristic / MPP / global-pending-list commits | per IBM layout, synthetic tests only (not produced on the dev box); CCSID map unverified |
| Catalog versioning | DOM create/drop records versioned against the catalog position (§9); records of ids not in a catalog valid at a later position = counted skip; a DROP/CREATE reusing an id *before* the catalog position still decodes against the new layout — the structural checks (§6) turn most of those into quarantine |
| C library | MSVC 14.50 x64 (cross) and ARM64 `/W4 /WX /sdl /guard:cf`, ABI 2; parity with Python on every corpus case (both suites) + 1 000 000 synthetic images covering every `Err`/`Kind` path (`census/test_db2_decoder_parity.py`). libFuzzer target not run (no clang-cl on the dev box) |
| Record census | local test-database range: 100 % of records classified, 0 unexplained, 0 unsupported, 0 quarantine |

## 9. Record classification (census, X-132)

`logdecode.decode_buffer` puts every record in one `alelt_common.completeness.RecordClass` and counts it in
`RawDecode.census[(record kind, class, reason)]`; the connector merges it into `Db2Source.census` and turns signals
into `alelt-signal:` events (kind `OTHER`, `DDL` for catalog refresh) inside the record's transaction. Record kind =
`0x<rtype>/<component>/<function>/<outer sub-record types>`. `python -m alelt_src_db2.census` runs the same rules over a
live log range or stored buffers (report shape = `Census.report()` + signals + quarantine reasons).

| Record | Class |
|---|---|
| every commit / abort variant (coverage table below); DMS 161–168 / CLR 110–112, 131 | decoded |
| DML whose image has an out-of-row value (§5) | needs_keyed_reread (event kept + signal) |
| overflow insert (x04) carrying a following update's after image; paired pointer delete / CLR; undo of an overflow insert | ignored (reason) |
| unpaired overflow / pointer (partner outside the batch) | needs_keyed_reread (key known) else unsupported |
| DMS 211/212 out-of-row markers, 128 initialize table, internal metadata (inner type 0); DOM 2/3 index, 35 reorg, 130/5 undo create/drop; LOB 64/65 (row record carries the re-read), 66/67 | ignored |
| DMS 102/104/113/115/116/119/122/123/124/169–172 (DDL) on a captured table; DOM 4 drop; DOM 101 create after the catalog position | needs_catalog_refresh |
| DOM 11 truncate / import replace, DOM 12 not logged initially, LOAD start, partition attach / detach; value compression; undecodable bytes; records out of LRI order | unsupported (hold + resync / quarantine) |
| backout free, subtransaction, information only, timestamp, application information, prepares (outcome follows), backup / migration / rollforward markers | ignored |
| table id not in the catalog: table space without captured tables, or id absent from a catalog valid at a later position (dropped / out of scope; remembered in `Db2Source.foreign_ids`) | ignored ("not captured") |
| table id not in the catalog after the catalog position | needs_catalog_refresh (once per id and batch; then "not captured" if still absent) |
| anything else | **unexplained** + unsupported signal (a census with unexplained > 0 fails) |
### IBM documented record classes → our class

Source: IBM Db2 11.5 "Db2 log records" (log record header, transaction / LOB / long field / utility / data /
relation manager log records) and the db2ReadLog API.
(real records from ALDEC + synthetic records for kinds not produced on the dev box).

| IBM record (component / type) | Our class | Notes |
|---|---|---|
| TM normal commit `0x0084`, local pending list `0x008A` | decoded: COMMIT | time = body u64 @0 (seconds) |
| TM heuristic commit `0x0087` | decoded: COMMIT (note) | same layout as normal commit |
| TM MPP coordinator `0x0086` / subordinator `0x0085` commit | decoded: COMMIT (note) | time @0; partitioned DB itself fails the `single_partition` prerequisite |
| TM global pending list `0x0051` | decoded: COMMIT, no timestamp | layout has no time field |
| TM normal abort `0x0041`, heuristic abort `0x0049` | decoded: ROLLBACK | |
| TM XA / TM / MPP / federated prepare (`0x007C` / `0x007D` / `0x007B` / `0x007E`) | ignored | outcome record follows (commit / abort variants above) |
| TM backout free, application information, timestamp, subtransaction, topology change | ignored | no row data |
| Utility load start `0x004A` | unsupported(table) → hold + resync | IBM: loaded rows are not logged, target table must be resynchronized; pool / object id @4 / @6 = TBSPACEID / TABLEID (verified) |
| Utility backup, table space rollforward (PIT start/end), catalog / database migration | ignored | no row data |
| DMS insert / delete / update (161–163), empty-page variants (164–166), undo (110–112, 131) | decoded (overflow / pointer pairing §4) | `row_ref` / `target_ref` RIDs |
| DMS insert multiple records 167 / undo 168 | decoded: one INSERT / undo-INSERT per record | IBM layout: count @8, descriptions @20 (RID 6, offset 2, record) |
| DMS add / drop columns, alter column, alter LOB column info, rename table / schema, alter table attribute, change row version (102/104/113/115/116/119/122/123/124/169/170/171/172) and undo partition state 108 | needs_catalog_refresh (DDL signal) | alter attribute covers propagation (DATA CAPTURE), value compression, LF propagation bits |
| DMS update partition state 137 | add partition → needs_catalog_refresh; attach / SET INTEGRITY / detach → unsupported(table) | rows appear / disappear without row images; master table ids @6 / @8 |
| DMS initialize table 128, start of out-of-row data 211 / 212 | ignored | |
| DOM create table 101 / drop table 4 | needs_catalog_refresh (captured or after the catalog position), else ignored | |
| DOM create / drop index 2 / 3, reorg 35, undo create / drop 130 / 5 | ignored | |
| DOM truncate / import replace 11, activate not logged initially 12 | unsupported(table) | rows change without row images |
| LOB manager add LOB data / amount 64 / 65 | ignored; row re-read by key (row record carries the signal); no row record in the txn → unsupported | row mapping and descriptors undocumented (X-121) |
| LOB manager delete / non-update 66 / 67 | ignored | information only |
| Long field manager add 113 | ignored; row re-read by key (as LOB) | records exist only with INCLUDE LONGVAR COLUMNS; the row descriptor triggers the re-read either way |
| Long field manager delete / non-update 114 / 115 | ignored | information only (`0x0069`) |
| Relation manager DDL statement 4, partition information 6/8 | needs_catalog_refresh (captured table) | undo DDL 5 ignored |
| Any other type / component / function | unexplained + unsupported signal | census fails |

**Multiple log streams.** IBM: in pureScale db2ReadLog merges the member log streams and returns records "in the
order in which they took effect" (type-1 LRI). The decoder enforces strictly increasing LRIs per batch: a record out of
order quarantines and stops the batch (`unsupported`, connection-wide). Partitioned (DPF) databases return only the
connected partition's log → prerequisite `single_partition` FAIL.

**Native encryption.** IBM's db2ReadLog documentation does not mention encryption; log pages are read through the
engine, which holds the key. Prerequisite `native_encryption` reports INFO; the record census must be run on an
encrypted test copy before certifying such a database (garbled bytes would fail framing / structural checks →
quarantine, never silent).

Agent commands (SNAPSHOT_TABLE with `key_from` / `last_key`, keyed VERIFY_TABLE for out-of-row completion, typed error
codes, command thread): [`docs/AGENT-COMMANDS.md`](../../../docs/AGENT-COMMANDS.md). Out-of-row values signalled by this
decoder are completed by key in the hub's data path (ADR-013), not held.
