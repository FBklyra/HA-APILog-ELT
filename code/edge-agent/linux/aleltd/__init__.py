"""aleltd - ALELT edge agent for Linux (systemd service `alelt.service`).

Python port of the Windows service `aleltagent.exe` (edge-agent/core): same `aleltagent.json` keys, same agent <-> hub
protocol v1, same spool / state.json / HA lease semantics. Source plugins run in-process:
  db2                                       raw db2ReadLog blocks (shared library alelt_db2edge, IBM client required)
  postgres, mysql, mariadb, sqlserver, oracle  SourceConnector.stream() -> canonical JSON Lines batches
"""
VERSION = "0.1.0"
