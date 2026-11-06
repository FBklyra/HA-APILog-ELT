# Canonical type map (all sources)

Every source connector maps its engine types to ONE canonical type set (`alelt_common.models.CanonicalType`). The
destination contract is derived from the canonical type, never from the engine type. This page is the reference
the per-engine connector code implements (XC-37); tests named next to each rule pin it.

## 1. Cross-engine rules

| Rule | Applies to | Behaviour | Test |
|---|---|---|---|
| Sub-microsecond time | Db2 TIMESTAMP(7..12), Oracle TIMESTAMP(7..9) / WITH TIME ZONE, SQL Server datetime2(7) / time(7) / datetimeoffset(7) | Canonical LOCALDATETIME / INSTANT / LOCALTIME keep **microseconds**. Lower digits are truncated. Db2 and Oracle add `precision_lost:<COL>` to the event note when a non-zero digit was dropped. SQL Server: the driver already returns microseconds, so the 100 ns digit is truncated without a note. | `oracle/tests/test_oracle_core.py::test_timestamp_ff9_keeps_microseconds_and_notes_precision_lost` |
| End of day `24:00:00` | Db2 TIME, PostgreSQL time | `23:59:59.999999` (the largest LOCALTIME; ordering kept). Db2 adds `precision_lost:<COL>`. | `test_time_end_of_day_is_the_largest_localtime`, postgres `test_parse_text` |
| Numeric NaN / Infinity | PostgreSQL numeric | No DECIMAL form in any destination: the row is quarantined with the reason (R7). float4 / float8 NaN / Infinity stay FLOAT / DOUBLE values. | postgres `test_parse_text_unconvertible` |
| MySQL TIME outside 00:00..24:00 | MySQL / MariaDB TIME (signed duration) | LOCALTIME; a value outside that range quarantines the row unless `settings.time_as_string` (then STRING `[-]HH:MM:SS[.ffffff]`). | `test_completeness_mysql` |
| Extension / user-defined types | PostgreSQL base types not in the map (citext, hstore, ltree, geometry, vector, ...) | STRING with the type's documented text output (engine type `ext:<name>`). Pseudo types stay UNKNOWN. | postgres `test_extension_types_decode_as_their_text_form` |
| CLR / variant types | SQL Server hierarchyid, geometry, geography, sql_variant | STRING = text form (`CAST(... AS nvarchar(max))`: ToString() / WKT). The CDC row only carries the CLR serialization, so the column is omitted and re-read by key. | sqlserver `test_clr_and_variant_columns_are_text_by_key_reread_never_binary` |
| Values the log does not carry | Db2 out-of-row LOB / DECFLOAT / XML / LONG, PG unchanged TOAST, MySQL MINIMAL / NOBLOB images and binary JSON, Oracle LOB on an unknown prior value, SQL Server computed / uncaptured columns | Never guessed and never NULL by assumption: omitted from the image (note), then `needs_keyed_reread` (or a hold when no key exists). | completeness tests per engine |
| Character data | every engine | Decoded to text with an explicit codepage -> codec map (Db2 CCSID, MySQL charset). PostgreSQL sessions run with `client_encoding=UTF8`. An unsupported codepage is a FAIL prerequisite (Db2 `ccsid_supported`). Bytes that are invalid in the codepage quarantine that row only. | `test_ccsid_codec_map_decodes_common_ccsids`, postgres `test_non_utf8_value_...` |
| Commit timestamps | Oracle, SQL Server | Server-local times converted with `settings.server_tz` (IANA name: DST-correct). Without it, the offset is sampled at connect. | `test_server_tz_setting_gives_dst_correct_commit_times`, oracle completeness |

## 2. Per engine

| Canonical | PostgreSQL | MySQL / MariaDB | Oracle | SQL Server | Db2 LUW |
|---|---|---|---|---|---|
| BOOLEAN | bool | bit(1), tinyint(1) is SHORT | BOOLEAN (23ai) | bit | BOOLEAN |
| SHORT / INT / LONG | int2 / int4 / int8, oid, xid | tinyint..bigint (unsigned bigint = DECIMAL(20,0)) | NUMBER(p,0) p<=4 / <=9 / <=18 | tinyint, smallint / int / bigint | SMALLINT / INTEGER / BIGINT |
| DECIMAL(p,s) | numeric, xid8 | decimal | NUMBER(p,s), NUMBER, FLOAT(b) | decimal, numeric, money (19,4), smallmoney (10,4) | DECIMAL |
| FLOAT / DOUBLE | float4 / float8 | float / double | BINARY_FLOAT / BINARY_DOUBLE | real / float | REAL / DOUBLE |
| STRING | text, varchar, bpchar, name, money (text form), uuid, inet, cidr, macaddr, interval, bit/varbit, tsvector, geometric, enum / range / composite, `ext:*` | char, varchar, text family, enum, set, uuid / inet4 / inet6 (MariaDB) | VARCHAR2, CHAR, NVARCHAR2, CLOB, NCLOB, LONG, ROWID, INTERVAL | char family, uniqueidentifier, CLR / sql_variant (text form) | CHAR, VARCHAR, GRAPHIC family, CLOB, DBCLOB |
| BINARY | bytea | binary, varbinary, blob family, bit(n>1), spatial | RAW, LONG RAW, BLOB | binary, varbinary, image, rowversion | FOR BIT DATA, BLOB, BINARY |
| LOCALDATE / LOCALTIME | date / time | date / time | - (DATE has a time) | date / time | DATE / TIME |
| LOCALDATETIME | timestamp | datetime | DATE, TIMESTAMP(n) | datetime, datetime2, smalldatetime | TIMESTAMP(n) |
| INSTANT (UTC) | timestamptz | timestamp | TIMESTAMP WITH (LOCAL) TIME ZONE | datetimeoffset | - |
| JSON / XML | json, jsonb, arrays (JSON array text) / xml | json (re-read by key unless the plugin decodes it) | JSON / XMLTYPE | - / xml | - / XML (re-read) |
| UNKNOWN (quarantine, R7) | pseudo types | unknown types | BFILE, object types | other CLR user types | DECFLOAT, distinct / structured types |

Known cross-engine differences that are kept on purpose (documented, the canonical value differs): PostgreSQL
`money` is STRING (locale-formatted text output) while SQL Server money is DECIMAL(19,4); uuid text is lower case
from PostgreSQL / MySQL and upper case from SQL Server; intervals are each engine's text form.

## 3. Event notes

`ChangeEvent.note` follows one grammar, parsed by `alelt_common.notes` (`parse`, `columns`, `format_note`):
`;`-separated tokens `key[:value]`, column lists as `key:a,b` (`key:a;key:b` is equivalent). Keys: `unchanged`,
`absent`, `lob_unavailable`, `value_unavailable`, `reread`, `precision_lost`, `lob_from_writes` (column lists);
`row_ref`, `target_ref`, `undo` (references); `no_before_image`, `uncataloged`, `cascade` (flags). Test:
`hub/alelt_common/tests/test_notes.py`.
