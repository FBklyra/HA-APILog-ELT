"""ALELT Db2 LUW source connector (hub side).

Pure-stdlib modules: position, framing, types, rowimage, logdecode, catalog (dict form), db2api (Windows x64).
connector.Db2Source needs alelt_common (pydantic v2) — put <repo>/hub on sys.path.
"""
from __future__ import annotations

from typing import Any

__all__ = ["Db2Source"]
__version__ = "0.1.0"


def __getattr__(name: str) -> Any:
    if name == "Db2Source":
        from .connector import Db2Source

        return Db2Source
    raise AttributeError(name)
