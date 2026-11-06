"""cffi (ABI mode) binding to libaleltdecode. Produces exactly the cells of alelt_src_db2.rowimage.decode_cells.

Library lookup: $ALELTDECODE_LIB, else decoder/build/{Release,RelWithDebInfo,Debug,.}/(aleltdecode.dll|libaleltdecode.so|.dylib).
load() raises ImportError / OSError when cffi or the library is unavailable (caller falls back to pure Python).
"""
from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from cffi import FFI  # type: ignore[import-untyped]

from alelt_src_db2 import types as T
from alelt_src_db2.rowimage import Cell, ColumnSpec, Err, Kind, RowImageError

CDEF = """
typedef struct aleltd_record {
    size_t offset; size_t length; uint64_t lri_part1; uint64_t lri_part2; uint32_t real_len; int32_t sqlcode;
    uint32_t size; uint16_t rtype; uint16_t flag; uint64_t lsn; uint64_t lfs; uint64_t back_lso; uint8_t tid[6];
    uint32_t header_size; size_t body_offset; size_t body_length;
} aleltd_record;
typedef struct aleltd_column { int32_t type; int32_t length; int32_t scale; int32_t nullable; } aleltd_column;
typedef struct aleltd_cell { int32_t kind; int64_t i; double f; uint32_t offset; uint32_t length; char text[48]; } aleltd_cell;
int aleltd_next_record(const uint8_t *buf, size_t len, size_t *pos, aleltd_record *out);
int aleltd_decode_row(const uint8_t *sub, size_t len, const aleltd_column *cols, size_t ncols, aleltd_cell *cells, size_t *err_col);
int aleltd_abi_version(void);
"""
ABI_VERSION = 2
ALELTD_OK, ALELTD_END = 0, 100

TYPE_IDS = {"SMALLINT": 1, "INTEGER": 2, "BIGINT": 3, "REAL": 4, "DOUBLE": 5, "DECIMAL": 6, "DATE": 7,
            "TIME": 8, "TIMESTAMP": 9, "BOOLEAN": 10, "CHARACTER": 11, "GRAPHIC": 12, "BINARY": 13,
            "VARCHAR": 14, "VARGRAPHIC": 15, "VARBINARY": 16, "CLOB": 17, "BLOB": 18, "DBCLOB": 19,
            "DECFLOAT": 20, "XML": 21, "LONG VARCHAR": 22, "LONG VARGRAPHIC": 22}

_NAMES = ("aleltdecode.dll", "libaleltdecode.so", "libaleltdecode.dylib")


def _find() -> str:
    env = os.environ.get("ALELTDECODE_LIB")
    if env:
        return env
    build = Path(__file__).resolve().parents[1] / "build"
    for sub in ("Release", "RelWithDebInfo", "Debug", "."):
        for n in _NAMES:
            p = build / sub / n
            if p.is_file():
                return str(p)
    raise OSError("libaleltdecode not built (set ALELTDECODE_LIB or build decoder/ with CMake)")


class Native:
    def __init__(self, path: str | None = None) -> None:
        self.ffi = FFI()
        self.ffi.cdef(CDEF)
        self.lib: Any = self.ffi.dlopen(path or _find())
        if self.lib.aleltd_abi_version() != ABI_VERSION:
            raise OSError(f"libaleltdecode ABI {self.lib.aleltd_abi_version()} != {ABI_VERSION}")
        self._col_cache: dict[tuple[ColumnSpec, ...], Any] = {}

    def frame(self, raw: bytes) -> list[tuple[int, int, int]]:
        """(offset, length, rtype) per record until END or the first framing error."""
        pos = self.ffi.new("size_t *", 0)
        rec = self.ffi.new("aleltd_record *")
        out: list[tuple[int, int, int]] = []
        while self.lib.aleltd_next_record(raw, len(raw), pos, rec) == ALELTD_OK:
            out.append((int(rec.offset), int(rec.length), int(rec.rtype)))
        return out

    def _cols(self, cols: Sequence[ColumnSpec]) -> Any:
        key = tuple(cols)
        arr = self._col_cache.get(key)
        if arr is None:
            arr = self.ffi.new("aleltd_column[]", max(1, len(cols)))
            for i, c in enumerate(cols):
                t = T.norm(c.type)
                arr[i].type = 0 if t in T.UNSUPPORTED else TYPE_IDS.get(t, 0)
                arr[i].length, arr[i].scale, arr[i].nullable = c.length, c.scale, 1 if c.nullable else 0
            self._col_cache[key] = arr
        return arr

    def cells(self, sub: bytes, cols: Sequence[ColumnSpec]) -> list[Cell]:
        n = len(cols)
        cells = self.ffi.new("aleltd_cell[]", max(1, n))
        err_col = self.ffi.new("size_t *")
        rc = self.lib.aleltd_decode_row(sub, len(sub), self._cols(cols), n, cells, err_col)
        if rc != ALELTD_OK:
            col = cols[err_col[0]].name if err_col[0] < n else "record"
            raise RowImageError(Err(rc) if rc in Err._value2member_map_ else Err.ARG, f"native rc={rc} at {col}")
        out: list[Cell] = []
        for i in range(n):
            c = cells[i]
            k = Kind(c.kind)
            if k in (Kind.NULL, Kind.ABSENT):
                out.append((k, None))
            elif k == Kind.INT:
                out.append((k, int(c.i)))
            elif k == Kind.FLOAT:
                out.append((k, float(c.f)))
            elif k == Kind.BOOL:
                out.append((k, bool(c.i)))
            elif k == Kind.TEXT:
                out.append((k, self.ffi.string(c.text).decode("ascii")))
            elif k == Kind.BYTES:
                out.append((k, bytes(sub[c.offset:c.offset + c.length])))
            else:
                out.append((k, int(c.i)))
        return out


def load(path: str | None = None) -> Native:
    return Native(path)
