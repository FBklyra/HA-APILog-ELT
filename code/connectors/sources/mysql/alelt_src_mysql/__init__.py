"""ALELT MySQL / MariaDB binlog source connector (hub side, remote capture).
Stdlib-only modules: position, types, values, jsonhook, events, rows, decode, framing, catalog (dict form).
protocol needs PyMySQL (MIT; transport/auth only). connector.py: MySQLSource / MariaDBSource (REMOTE.md); assemble.py: decoder output -> canonical events.
"""
from __future__ import annotations

__version__ = "0.1.0"
