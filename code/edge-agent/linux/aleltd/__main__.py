"""aleltd entry point:  python -m aleltd [--config PATH] [--check-config] [--print-state]

Always runs in the foreground (systemd Type=notify supervises it; no double fork, no PID file). Same command in a
console or a container: Ctrl+C / SIGTERM stop gracefully, SIGHUP reloads the configuration.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import socket
import sys
import threading
from typing import Any, List, Optional

from . import VERSION, config as cfgmod, logging_setup
from .checkpoint import StateStore
from .daemon import EXIT_CONFIG, Agent

DEFAULT_CONFIG = os.path.join(os.environ.get("CONFIGURATION_DIRECTORY", "/etc/alelt").split(":")[0],
                              "aleltagent.json")


def install_signals(agent: Agent) -> None:
    """Handlers only set events (async-signal-safe in spirit; the main loop does the work)."""
    def stop(signum: int, _frame: Any) -> None:
        agent.request_stop(signal.Signals(signum).name)

    def hup(_signum: int, _frame: Any) -> None:
        agent.request_reload()

    stops = [getattr(signal, n) for n in ("SIGTERM", "SIGINT", "SIGBREAK") if hasattr(signal, n)]
    for s in stops:
        signal.signal(s, stop)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, hup)
    # Python-level handlers run only when the main thread is back in the interpreter. If it is blocked inside a
    # source call (C library, socket), the wakeup fd still receives the signal number at once: a watcher thread
    # sets the same events, so the shutdown deadline starts counting even while the main thread is stuck.
    try:
        r, w = socket.socketpair()
        w.setblocking(False)
        signal.set_wakeup_fd(w.fileno(), warn_on_full_buffer=False)
        agent.signal_sockets = (r, w)      # keep them alive for the life of the process
    except (OSError, ValueError, TypeError):
        return

    def watch() -> None:
        while True:
            try:
                data = r.recv(64)
            except OSError:
                return
            if not data:
                return
            for b in data:
                if b in stops:
                    agent.request_stop(signal.Signals(b).name)
                elif hasattr(signal, "SIGHUP") and b == signal.SIGHUP:
                    agent.request_reload()
    threading.Thread(target=watch, name="alelt-signals", daemon=True).start()


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="aleltd", description="ALELT edge agent (Linux service)")
    ap.add_argument("--config", default=os.environ.get("ALELT_AGENT_CONFIG", DEFAULT_CONFIG))
    ap.add_argument("--check-config", action="store_true", help="validate the configuration and exit")
    ap.add_argument("--print-state", action="store_true", help="print state.json (positions) and exit")
    ap.add_argument("--foreground", action="store_true", help="accepted for clarity; the agent never daemonizes")
    ap.add_argument("--version", action="version", version=f"aleltd {VERSION}")
    args = ap.parse_args(argv)
    logging_setup.setup("INFO")
    log = logging.getLogger("aleltd")
    try:
        cfg = cfgmod.load(args.config)
    except cfgmod.ConfigError as e:
        log.error("configuration invalid: %s", e)
        return EXIT_CONFIG
    if args.check_config:
        print(json.dumps({"ok": True, "agent_id": cfg.agent_id, "connection_id": cfg.connection_id,
                          "engine": cfg.source.engine, "spool_dir": cfg.spool_dir, "ha": cfg.ha.mode,
                          "environment": cfg.environment}))
        return 0
    if args.print_state:
        print(json.dumps(StateStore(cfg.state_file).load(), indent=1))
        return 0
    logging_setup.setup(cfg.log_level, cfg.log_timestamps)
    agent = Agent(cfg)
    install_signals(agent)
    return agent.run()


if __name__ == "__main__":
    sys.exit(main())
