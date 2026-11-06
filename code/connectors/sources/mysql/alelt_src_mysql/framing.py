"""Raw batch framing for binlog capture (stdlib only).

    batch  ::= b"ALBL" u8 version(=1) frame*
    frame  ::= u8 kind  u32le length  payload
    kind 1 = CONTEXT  JSON decoder state at the batch start (always the first frame)
    kind 2 = EVENT    one binlog event exactly as received (19-byte header .. CRC32)

The context makes every batch decodable on its own (format description, binlog file, GTID state, the open
transaction and its table maps), so the hub can decode, replay and re-decode batches in any order.
RawRef offsets point at the EVENT payload inside the batch.
"""
from __future__ import annotations

import json
import struct
from collections.abc import Iterator
from typing import Any

MAGIC = b"ALBL"
VERSION = 1
CONTEXT, EVENT = 1, 2
_FRAME = struct.Struct("<BI")


class FramingError(ValueError):
    pass


def encode_frame(kind: int, payload: bytes) -> bytes:
    return _FRAME.pack(kind, len(payload)) + payload


def start_batch(context: dict[str, Any]) -> bytearray:
    out = bytearray(MAGIC + bytes([VERSION]))
    out += encode_frame(CONTEXT, json.dumps(context, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    return out


def iter_frames(raw: bytes) -> Iterator[tuple[int, int, bytes]]:
    """Yields (kind, payload offset, payload). Raises FramingError on a malformed frame (caller quarantines)."""
    if raw[:4] != MAGIC:
        raise FramingError("not a binlog batch (bad magic)")
    if len(raw) < 5 or raw[4] != VERSION:
        raise FramingError(f"unsupported batch version {raw[4] if len(raw) > 4 else None}")
    off = 5
    n = len(raw)
    while off < n:
        if off + _FRAME.size > n:
            raise FramingError(f"truncated frame header at {off}")
        kind, ln = _FRAME.unpack_from(raw, off)
        p = off + _FRAME.size
        if p + ln > n:
            raise FramingError(f"frame at {off} runs past the batch end")
        yield kind, p, raw[p:p + ln]
        off = p + ln


def read_context(raw: bytes) -> dict[str, Any]:
    for kind, _, payload in iter_frames(raw):
        if kind != CONTEXT:
            break
        return dict(json.loads(payload.decode("utf-8")))
    raise FramingError("batch has no CONTEXT frame")
