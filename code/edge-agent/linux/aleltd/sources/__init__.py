"""Source plugins run in-process by the daemon.

| engine | adapter | needs |
|---|---|---|
| db2 | `db2.Db2Capture` (raw db2ReadLog blocks, alelt_db2edge) | IBM Data Server client (libdb2.so) on the host |
| postgres, mysql, mariadb, sqlserver, oracle | `remote.StreamCapture` over `SourceConnector.stream()` | alelt_common + connector + its client library |
| fake | `fake.FakeConnector` through `StreamCapture` (tests only, refused with environment=PROD) | nothing |
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict

from ..config import Config
from .base import CaptureSource, SourceError

# engine -> 'module:Class' (override with source.connector or ALELT_SOURCES, same format as the hub's ALELT_SOURCES)
REMOTE_CONNECTORS: Dict[str, str] = {
    "postgres": "alelt_src_postgres.connector:PostgresSource",
    "mysql": "alelt_src_mysql.connector:MySQLSource",
    "mariadb": "alelt_src_mariadb.connector:MariaDBSource",
    "sqlserver": "alelt_src_sqlserver.connector:SqlServerSource",
    "oracle": "alelt_src_oracle.connector:OracleSource",
}


def connector_ref(cfg: Config) -> str:
    if cfg.source.connector:
        return cfg.source.connector
    env: Dict[str, Any] = json.loads(os.environ.get("ALELT_SOURCES") or "{}")
    ref = env.get(cfg.source.engine) or REMOTE_CONNECTORS.get(cfg.source.engine)
    if not ref:
        raise SourceError(f"no connector registered for engine {cfg.source.engine!r}")
    return str(ref)


def make_source(cfg: Config) -> CaptureSource:
    if cfg.source.engine == "db2" and not cfg.source.connector:
        from .db2 import Db2Capture
        return Db2Capture(cfg)
    from .remote import StreamCapture
    if cfg.source.engine == "fake" and not cfg.source.connector:
        from .fake import FakeConnector
        return StreamCapture(cfg, lambda: FakeConnector(cfg.source.settings, cfg.connection_id), native=True)
    return StreamCapture.for_connector(connector_ref(cfg), cfg)


__all__ = ["CaptureSource", "SourceError", "make_source", "connector_ref", "REMOTE_CONNECTORS"]
