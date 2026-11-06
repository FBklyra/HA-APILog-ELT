"""Which remote connections to capture and how to build their live SourceConnector.

Sources (merged, env wins):
- `pipeline_definition` rows whose source engine connector has capture_mode 'remote' (wizard / import):
  source = {engine, settings, secret_refs: {password: <ref>}} - ref resolved by alelt_import.secrets.SecretStore.
- ALELT_CAPTURE_CONNECTIONS = {"<cid>": {"engine": "postgres", "settings": {...}, "password_env": "NAME"}} (dev / ops).
"""
from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import Engine, select

from alelt_common.spi import SourceConnector
from alelt_common.util import load
from alelt_hub.db import pipeline_definition
from alelt_hub.settings import get_settings

log = logging.getLogger("alelt_capture")


@dataclass
class RemoteConnection:
    connection_id: str
    engine: str
    settings: dict[str, Any] = field(default_factory=dict)
    password_ref: str | None = None        # env:NAME | file:name (SecretStore)

    def password(self) -> str | None:
        if not self.password_ref:
            return None
        kind, _, name = self.password_ref.partition(":")
        if kind == "env":
            return os.environ.get(name)
        from alelt_import.secrets import SecretStore

        return SecretStore().resolve(self.password_ref)

    def factory(self) -> Callable[[], SourceConnector]:
        ref = get_settings().sources.get(self.engine)
        if not ref:
            raise LookupError(f"no source connector registered for engine {self.engine!r} (ALELT_SOURCES)")
        cls = load(ref)

        def make() -> SourceConnector:
            return cls(settings=self.settings, password=self.password(),  # type: ignore[no-any-return]
                       connection_id=self.connection_id)
        return make


def _is_remote(engine: str) -> bool:
    ref = get_settings().sources.get(engine)
    if not ref:
        return False
    try:
        return getattr(load(ref), "capture_mode", "edge") == "remote"
    except Exception as e:  # noqa: BLE001
        log.warning("source connector %s unavailable: %s", ref, e, extra={"component": "alelt_capture"})
        return False


def connections(engine: Engine | None = None) -> dict[str, RemoteConnection]:
    out: dict[str, RemoteConnection] = {}
    if engine is not None:
        with engine.connect() as c:
            rows = c.execute(select(pipeline_definition.c.connection_id, pipeline_definition.c.source_engine,
                                    pipeline_definition.c.source, pipeline_definition.c.status)).all()
        for cid, eng, src, status in rows:
            if status in ("DRAFT", "NEEDS_SECRETS") or not _is_remote(eng):
                continue
            src = src or {}
            out[cid] = RemoteConnection(cid, eng, dict(src.get("settings") or {}),
                                        (src.get("secret_refs") or {}).get("password"))
    raw = os.environ.get("ALELT_CAPTURE_CONNECTIONS")
    if raw:
        for cid, d in json.loads(raw).items():
            pw = d.get("password_ref") or (f"env:{d['password_env']}" if d.get("password_env") else None)
            out[cid] = RemoteConnection(cid, d["engine"], dict(d.get("settings") or {}), pw)
    return out
