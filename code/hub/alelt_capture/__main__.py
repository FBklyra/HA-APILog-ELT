"""`python -m alelt_capture` / `alelt capture` - remote-capture runners for every remote connection.

One thread per connection; a connection owned by another process (lock `capture:<cid>`) is retried every
ALELT_CAPTURE_RETRY_S. --once: capture until each source was idle --idle-s seconds, then exit (tests / catch-up).
"""
from __future__ import annotations

import argparse
import logging
import signal
import threading
import time

from alelt_common.util import setup_logging
from alelt_hub.db import get_engine, init_db
from alelt_hub.settings import get_settings

from .config import connections
from .runner import CaptureConfig, CaptureError, CaptureRunner

log = logging.getLogger("alelt_capture")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="ALELT remote-capture runner")
    ap.add_argument("--connection", action="append", help="only these connection ids (repeatable)")
    ap.add_argument("--once", action="store_true", help="exit after the sources were idle --idle-s seconds")
    ap.add_argument("--idle-s", type=float, default=3.0)
    ap.add_argument("--start-pos", help="first-start position when the hub has no batch yet")
    args = ap.parse_args(argv)
    s = get_settings()
    setup_logging(s.log_level)
    init_db()
    from alelt_hub.ha import start_node_heartbeat

    eng = get_engine()
    start_node_heartbeat(eng, "capture")
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    runners: dict[str, CaptureRunner] = {}
    threads: dict[str, threading.Thread] = {}
    rc = {"code": 0}
    cfg = CaptureConfig(start_pos=args.start_pos)

    def body(r: CaptureRunner) -> None:
        while not stop.is_set():
            try:
                res = r.run(idle_exit_s=args.idle_s if args.once else None)
            except CaptureError as e:
                log.error("capture runner stopped: %s", e, extra={"component": "alelt_capture",
                                                                   "correlation_id": r.cid})
                rc["code"] = 1
                return
            if res != "locked" or args.once:
                return
            stop.wait(cfg.retry_s)        # someone else owns it: stand by

    while not stop.is_set():
        conns = connections(eng)
        if args.connection:
            conns = {k: v for k, v in conns.items() if k in set(args.connection)}
        for cid, rc_ in conns.items():
            if cid in threads and threads[cid].is_alive():
                continue
            if args.once and cid in threads:
                continue
            r = runners[cid] = CaptureRunner(eng, cid, rc_.factory(), cfg,
                                             database=str(rc_.settings.get("database") or "") or None)
            t = threads[cid] = threading.Thread(target=body, args=(r,), name=f"capture-{cid}", daemon=True)
            t.start()
        if args.once:
            for t in threads.values():
                t.join()
            break
        stop.wait(30)
    for r in runners.values():
        r.stop.set()
    for t in threads.values():
        t.join(timeout=30)
    if args.once and not runners:
        log.warning("no remote connections configured", extra={"component": "alelt_capture"})
    time.sleep(0)
    return rc["code"]


if __name__ == "__main__":
    raise SystemExit(main())
