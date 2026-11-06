"""Select the row-image backend: libaleltdecode (C, via cffi) by default, pure Python as fallback / reference.

ALELT_DB2_DECODER: `native` (default: use the C library, fall back to Python with a warning), `require-native`
(fail when the C library cannot be loaded), `python` (force the pure-Python reference path).
Library lookup: ALELTDECODE_LIB, else decoder/build/... (see decoder/python/aleltdecode_cffi.py).
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any

from . import rowimage

_BINDING_DIR = Path(__file__).resolve().parents[1] / "decoder" / "python"
log = logging.getLogger("alelt_src_db2.native")
_state: dict[str, Any] = {"backend": "python", "reason": "not initialised", "library": None, "logged": False}


def enable(path: str | None = None) -> bool:
    """Try to switch decode_row to the native library. Returns True if active."""
    mode = os.environ.get("ALELT_DB2_DECODER", "native").lower()
    if mode == "python":
        _set("python", "forced by ALELT_DB2_DECODER=python", None)
        return False
    try:
        if str(_BINDING_DIR) not in sys.path:
            sys.path.append(str(_BINDING_DIR))
        import aleltdecode_cffi  # type: ignore[import-not-found]

        lib_path = path or os.environ.get("ALELTDECODE_LIB") or None
        nat = aleltdecode_cffi.load(lib_path)
        rowimage._native_cells = nat.cells
        real = lib_path or aleltdecode_cffi._find()
        st = os.stat(real)
        _state["abi"] = aleltdecode_cffi.ABI_VERSION
        _state["library_id"] = f"{st.st_size:x}{st.st_mtime_ns:x}"
        _set("native", "libaleltdecode loaded", real)
        return True
    except (ImportError, OSError) as e:
        if mode == "require-native":
            raise RuntimeError(f"ALELT_DB2_DECODER=require-native but libaleltdecode is unavailable: {e}") from e
        _set("python", f"fallback: {type(e).__name__}: {e}", None)
        return False


def _set(backend: str, reason: str, library: str | None) -> None:
    first = not _state["logged"] or _state["backend"] != backend
    _state.update(backend=backend, reason=reason, library=library, logged=True)
    if first:
        (log.info if backend == "native" else log.warning)(
            "db2 decoder backend: %s (%s)", backend, reason, extra={"component": "alelt_src_db2"})


def disable() -> None:
    rowimage._native_cells = None


def active() -> bool:
    return rowimage._native_cells is not None


def info() -> dict[str, Any]:
    """Backend in use (for version endpoints / support bundles). Initialises on first call."""
    if _state["reason"] == "not initialised":
        try:
            enable()
        except RuntimeError as e:
            return {"backend": "unavailable", "reason": str(e), "library": None}
    return {"backend": "native" if active() else "python", "reason": _state["reason"], "library": _state["library"],
            "abi": _state.get("abi"), "library_id": _state.get("library_id") if active() else None}
