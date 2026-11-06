# Provenance record

Record of every external source used to build ALELT (clean-room rule, [`CONTRIBUTING.md`](../../CONTRIBUTING.md)). Add a row in the same pull request that introduces a new source.

| # | Source | Kind | Used for | How used | Recorded |
|---|---|---|---|---|---|
| P-1 | IBM Db2 11.5 documentation — `db2ReadLog` API, `db2ReadLogStruct`, `db2ReadLogInfoStruct`, `db2ReadLogFilterData` | Public IBM docs | Db2 edge plugin capture loop, record framing | Read; implementation written independently | 2026-11-03 |
| P-2 | IBM Db2 11.5 documentation — log record formats (log manager header, DMS log record header, insert/delete/update record formats, CLR, commit/abort) | Public IBM docs | Db2 decoder | Read; decoder written independently | 2026-11-03 |
| P-3 | IBM Db2 CLI documentation — `SQLConnect`, `SQLExecDirect`, `SQLEndTran` | Public IBM docs | Edge plugin connection (ADR-010) | Read | 2026-11-03 |
| P-4 | IBM Db2 SYSCAT catalog views documentation (`SYSCAT.TABLES`, `SYSCAT.COLUMNS`) | Public IBM docs | Catalog snapshot | Read | 2026-11-03 |
| P-5 | IBM Db2 sample programs shipped with the client (e.g. log-reading samples) | Public IBM sample code | Understanding call order only | **Reading reference only — not copied**, no code or structure reproduced beyond what the public API documentation defines | 2026-11-03 |
| P-6 | Observations on a local licensed Db2 11.5 Community Edition test database (`tests/dev-db2`, DESIGN.md §8 V1–V9) | Own experiments | Verified facts for decoder and capture | Own work | 2026-11-03 |
| P-7 | Snowflake documentation — `PUT`, `COPY INTO`, `MERGE`, transactions, key-pair authentication, stages, resource monitors | Public Snowflake docs | Snowflake destination | Read | 2026-11-03 |
| P-8 | Microsoft Entra ID documentation — OAuth 2.0 authorization code flow with PKCE for SPAs | Public Microsoft docs | Console sign-in | Read | 2026-11-03 |
| P-9 | Destination landing-schema DDL exported from the operator's own data warehouse (golden spec, `config/golden/`) | Operator's own data | Output contract (ADR-004) | Operator-provided; not redistributed | 2026-11-03 |
| P-10 | MySQL binlog: binary JSON column values in row events | **Not used** (X-133) | — | The format is defined only in the GPLv2 server source, not in public documentation. The core does not decode it: MySQL JSON values are re-read by primary key through SQL. An optional `JsonBinaryDecoder` hook (`connectors/sources/mysql/JSON-BINARY-PLUGIN.md`) lets a separately licensed plugin decode them; the project's own such plugin is **internal-only and not distributed** (excluded from every export, blocked by `tools/publish_check.py`). | 2026-11-04 |
| P-11 | MySQL / MariaDB reference manual + replication protocol docs: `binlog_transaction_compression` (ZSTD transaction payload), tagged GTIDs, MariaDB "Compressed Binary Log" (zlib), MariaDB `INET4` / `INET6` storage | Public vendor docs | Decoding compressed payloads, tagged GTID events / sets, compressed MariaDB events, INET values | Read; layouts confirmed by observation on our own test servers (MySQL 9.7.2, MariaDB 13.0.2); MariaDB `UUID` value order not documented for row images: re-read by key | 2026-11-04 |

Vendor-specific sources used by private extensions are recorded in the extension's own provenance file (e.g. `extensions/<name>/PROVENANCE.md`), not here.

Not used, ever: proprietary source code, NDA material, decompiled or disassembled binaries.

## Per-field provenance of the Db2 log layouts (F-25)

Every byte layout the Db2 decoder relies on is either taken from the IBM public documentation (P-1, P-2) or **observed on our own test database** (P-6: black-box observation of the bytes `db2ReadLog` returns for changes we made ourselves). No layout comes from decompilation, disassembly, debugging or tracing of IBM binaries. Fields marked *observed* are a documented risk: a future Db2 fix pack may change them, and the decoder treats any deviation as quarantine (R7), never as a guess. Detail: `connectors/sources/db2/DESIGN.md` §2–5.

| Layout / field | Provenance | Notes |
|---|---|---|
| `db2ReadLog` call sequence, `db2ReadLogStruct`, `db2ReadLogInfoStruct`, LRI | IBM doc (P-1) | V1/V2 confirmed by observation |
| `db2ReadLogFilterData` prefix (56 B: LRI1, LRI2, realLen, sqlcode) | IBM doc (P-1) | V3 confirmed by observation |
| Log manager header (40 B: size, type, flag, LSN, LFS, back-LSO, TID, stream) | IBM doc (P-2) | V4 confirmed |
| Record types `0x004E` normal, `0x0043` CLR, `0x0084` / `0x008A` commit, `0x0041` abort | IBM doc (P-2) | V5 confirmed |
| CLR header extension (+16, +8 when `flag & 2`); observed flag value `0x0202` | IBM doc (P-2) for the extension; flag value *observed* | |
| Commit body: UTC seconds, authid length + authid | IBM doc (P-2) | time verified ±1 s |
| Commit body: second `u32` (values 1..3) | *observed* (meaning unknown) | ignored by the decoder |
| DMS header (component, function, tablespace id, table id) and function codes 161/162/163/110/111/112/124 | IBM doc (P-2) | V6/V7 confirmed |
| Function codes 164/165/166 (empty-page insert/delete/undo) | IBM doc (P-2) | **not observed** |
| Insert/delete/update record bodies (internal, len, free, RID, offset, sub-record) | IBM doc (P-2) | update `len` field order swap: *observed* |
| Sub-record header (type, length, updatable record type, fixed-section length) | IBM doc (P-2) | sub-record type values 0 and 16 for user data: *observed* |
| Sub-record types 196 (`0xC4`, overflow full image) and 66 (`0x42`, 12-byte pointer) | *observed* (catalog-shaped test run, 2026-11-04) | per X-121 **not decoded** unless documented by IBM; the record is signalled `needs_keyed_reread` and the row is re-read by key through the agent |
| Column encodings in the fixed section (integers LE, IEEE floats, packed BCD DECIMAL, BCD DATE/TIME/TIMESTAMP, CHAR/GRAPHIC, var-section offset/length) | IBM doc (P-2, row format) | each type verified byte-exact against `SELECT` (L3 corpus) |
| Trailing null byte per nullable column (`0x00` present / `0x01` NULL) | *observed* | any other value ⇒ quarantine |
| LOB descriptors: `0x69 00 00 00` inline, `0x80` empty, `0x49…` (60 B) out-of-row | *observed* | out-of-row ⇒ `needs_keyed_reread` (X-121); LOB-manager records (`0x0052`) never decoded |
| DECFLOAT, XML, LONG VARCHAR/VARGRAPHIC, value compression, non-1208 code pages | not decoded | quarantine / keyed re-read; certification exclusion until decoded from documented layouts |
| Internal control records (updatable record type ≠ 1) after CREATE/ALTER | *observed* | emitted as OTHER, counted in the record census |

Open item (owner: Db2 decoder stream): confirm each *observed* row against the current IBM 11.5 log-record documentation and move it to "IBM doc" with the page reference when it is documented there.
