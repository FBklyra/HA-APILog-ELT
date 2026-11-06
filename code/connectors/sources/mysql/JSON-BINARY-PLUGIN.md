# MySQL JSON values in row events — optional decoder plugin

## Default behaviour (core)

MySQL writes JSON column values into binlog row events in its internal binary JSON storage format. The core connector
**does not decode that format.** For every row event that carries a non-NULL JSON value, the JSON column is omitted
from the change image and the row is re-read by primary key through plain SQL (`needs_keyed_reread`,
`alelt_common/completeness.py`); SQL returns the value as JSON text. The value that lands in the destination is
therefore exactly what `SELECT` returns. MariaDB stores JSON as text (`LONGTEXT`) and is decoded directly.

## Why no decoder is included

The binary JSON storage format is defined in the MySQL server source code (GPLv2), not in MySQL's public user
documentation. This project's core is Apache-2.0 and is built only from public, documented interfaces (clean room).
A decoder written from that source code would be a derivative of GPLv2 code and cannot be part of the core.

## Hook contract

Module `alelt_src_mysql/jsonhook.py`:

```python
class JsonBinaryDecoder(Protocol):
    def decode(self, raw: bytes) -> str: ...
```

| Item | Contract |
|---|---|
| Input `raw` | the bytes of ONE JSON column value exactly as carried in the row image of a WRITE / UPDATE / DELETE rows event, without its length prefix. NULL / empty values are handled by the core and never passed. |
| Output | the value as JSON text (`str`, UTF-8, any valid JSON). The core parses and re-serialises it compactly (same form as the re-read path). |
| Errors | raise any exception when the value cannot be decoded. The core then falls back to the keyed re-read for that row and counts `plugin:json_binary_decoder / needs_keyed_reread` in the source census. A result that is not valid JSON text is treated the same way. Never return a guessed value. |
| Threading | called from the capture thread; must be thread-safe and side-effect free (no I/O). |
| Performance | called once per JSON value of every captured row event; keep it allocation-light. Without a plugin every such row costs one keyed `SELECT` instead. |

Reference stub (decodes nothing, raises `NotImplementedError` = default behaviour): `alelt_src_mysql.jsonhook:StubJsonBinaryDecoder`.

## Registering a plugin

First match wins:

1. connector setting `json_binary_decoder: "your_module:YourClass"` (pipeline source settings);
2. environment variable `ALELT_MYSQL_JSON_BINARY_DECODER=your_module:YourClass`;
3. a Python entry point in group `alelt.mysql.json_binary_decoder`.

The class is instantiated without arguments. The module must be importable by the hub (`PYTHONPATH` or an installed
package).

## Testing a plugin

The golden corpus harness replays captured binlog bytes without a server:

```python
from alelt_src_mysql.assemble import replay_frames
events, asm = replay_frames(raw_albl, "mysql", catalog_dict, json_decoder=YourClass())
```

Compare the JSON values with the `SELECT` oracle of the same corpus case (`<case>.oracle.json`)
and check `asm.census.report()["unexplained"] == 0`.
with the setting configured; the landed JSON values must equal the re-read path.

## Where the format is defined

Only as a pointer: the JSON binary storage format is implemented in the MySQL server repository (the `json_binary`
header and implementation under `sql-common/` / `sql/`). This document deliberately does not describe it.

## Notice

Anyone implementing a decoder from the MySQL server source should treat the result as a **separately licensed plugin**
(most likely GPLv2), keep it **outside this repository**, and take their own legal advice before distributing it.
Existing open-source binlog libraries contain such decoders; users may evaluate them under their own licences.
