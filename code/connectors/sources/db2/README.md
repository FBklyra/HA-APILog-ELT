# Db2 source connector — hub side

| Path | What |
|---|---|
| `alelt_src_db2/position.py` | type-1 LRI `"<part1>:<part2 hex16>"` parse / compare / format |
| `alelt_src_db2/framing.py` | db2ReadLog buffer framing, DMS header, row-image location |
| `alelt_src_db2/rowimage.py` | row image → cells → canonical values |
| `alelt_src_db2/types.py` | Db2 type → canonical type, fixed widths |
| `alelt_src_db2/logdecode.py` | raw buffer → plain events / quarantine (stdlib) |
| `alelt_src_db2/catalog.py` | SYSCAT snapshot (read-only, WITH UR) |
| `alelt_src_db2/connector.py` | `Db2Source(SourceConnector)` (needs pydantic / alelt_common) |
| `alelt_src_db2/db2api.py` | ctypes Db2 CLI (`SQLConnect`) + db2ReadLog (Windows x64 client) |
| `alelt_src_db2/native.py` | switches to `libaleltdecode` when built |
| `decoder/` | C11 `libaleltdecode` + cffi binding (`decoder/python/aleltdecode_cffi.py`) |
| `corpus/` | golden corpus builder + ALELTTEST DDL |

Format notes and verification status: [`DESIGN.md`](DESIGN.md).

## Python paths

`alelt_src_db2` is importable with `connectors/sources/db2` on `sys.path`; the connector also needs `hub/`:

```powershell
$env:PYTHONPATH = "C:\src\R&D\alelt\connectors\sources\db2;C:\src\R&D\alelt\hub"
```

```python
from alelt_src_db2.connector import Db2Source
src = Db2Source()                       # uses libaleltdecode if found, else pure Python
res = src.decode(raw_bytes, catalog_snapshot, batch_id)   # DecodeResult(events, quarantined, skipped)
```

Everything except `connector.py` is stdlib only and runs on Python 3.10+.

## Golden corpus

Examples and test corpora for this section are coming in a later release.

## Tests

```powershell
python tests\golden\db2\test_db2_corpus.py         # plain script (any Python 3.10+)
python -m pytest -q tests\golden\db2           # pytest
```

Checks: no quarantine, event sequence, every value equal to SELECT, commit timestamps, rollback CLR counts,
mutation robustness, native/pure parity (when the library is found).

## Building libaleltdecode

```powershell
cmake -S connectors\sources\db2\decoder -B connectors\sources\db2\decoder\build -A x64
cmake --build connectors\sources\db2\decoder\build --config Release
ctest --test-dir connectors\sources\db2\decoder\build -C Release
```

The binding looks for `decoder/build/Release/aleltdecode.dll` (or `$env:ALELTDECODE_LIB`). The DLL architecture must
match the Python process (conda Python on this laptop is an x64 build). `ALELT_DB2_DECODER=python` forces the
pure-Python path. Fuzzing: `-DALELTDECODE_FUZZ=ON` with clang-cl (`fuzz/fuzz_aleltdecode.c`).
