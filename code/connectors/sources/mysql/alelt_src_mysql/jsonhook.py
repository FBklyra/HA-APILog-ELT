"""Optional decoder hook for MySQL JSON column values in row events (X-133).

The core does NOT decode the binary JSON value MySQL writes into row events (that format is not described in MySQL's
public user documentation; see ../JSON-BINARY-PLUGIN.md). Default: the JSON column of the row is omitted and the row
is re-read by primary key through SQL (`needs_keyed_reread`), which returns JSON text.

A separately distributed plugin may implement `JsonBinaryDecoder` and be configured as `module:Class` in
- connector setting `json_binary_decoder`, or
- environment variable `ALELT_MYSQL_JSON_BINARY_DECODER`, or
- the Python entry point group `alelt.mysql.json_binary_decoder` (first entry point wins).
When configured, the connector calls `decode(raw)` for every JSON value of a row event. Any exception (or a result
that is not JSON text) falls back to the keyed re-read for that row and is counted in the source census.
"""
from __future__ import annotations

import importlib
import logging
import os
from typing import Any, Protocol, runtime_checkable

log = logging.getLogger("alelt_src_mysql")
ENTRY_POINT_GROUP = "alelt.mysql.json_binary_decoder"
ENV = "ALELT_MYSQL_JSON_BINARY_DECODER"


@runtime_checkable
class JsonBinaryDecoder(Protocol):
    """Contract of a JSON value decoder plugin.

    decode(raw) -> str
        raw:    the bytes of ONE JSON column value exactly as carried in the row image of a WRITE / UPDATE / DELETE
                rows event, without its length prefix (an empty value is never passed: SQL NULL / empty is handled by
                the core).
        return: the value as JSON text (UTF-8 `str`, any valid JSON; the core re-serialises it compactly).
        raise:  any exception when the value cannot be decoded; the core then re-reads the row by key (never
                guesses) and counts the failure. Must be thread-safe and side-effect free.
    """

    def decode(self, raw: bytes) -> str: ...


class StubJsonBinaryDecoder:
    """Reference stub: shows the signature only. It decodes nothing; every call raises NotImplementedError, so a
    connector configured with it behaves exactly like the default (keyed re-read)."""

    def decode(self, raw: bytes) -> str:
        """See JsonBinaryDecoder.decode. Not implemented in the core distribution (X-133)."""
        raise NotImplementedError("no JSON binary decoder in the core distribution; rows are re-read by key")


def _load_ref(ref: str) -> JsonBinaryDecoder:
    mod, _, cls = ref.partition(":")
    if not mod or not cls:
        raise ValueError(f"json_binary_decoder must be 'module:Class', got {ref!r}")
    obj = getattr(importlib.import_module(mod), cls)()
    if not callable(getattr(obj, "decode", None)):
        raise TypeError(f"{ref} has no decode(raw) method")
    return obj  # type: ignore[no-any-return]


def load_json_decoder(settings: dict[str, Any] | None = None) -> JsonBinaryDecoder | None:
    """The configured plugin instance, or None (default: keyed re-read)."""
    ref = str((settings or {}).get("json_binary_decoder") or os.environ.get(ENV) or "").strip()
    if ref:
        return _load_ref(ref)
    try:
        from importlib.metadata import entry_points

        eps = list(entry_points(group=ENTRY_POINT_GROUP))
    except Exception:  # noqa: BLE001 - no metadata available
        eps = []
    if eps:
        log.info("JSON binary decoder plugin from entry point %s", eps[0].value, extra={"component": "alelt_src_mysql"})
        return eps[0].load()()  # type: ignore[no-any-return]
    return None
