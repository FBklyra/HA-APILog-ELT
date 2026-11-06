"""Deterministic synthetic source (engine `fake`) for tests and container drills - never for real data.

Transaction n (n = 1, 2, ...) = `rows_per_txn` INSERTs into FAKE.EVENTS + COMMIT. Event positions are 15-digit
integers: n*1000 + k for row k, n*1000 + 999 for the commit, so `stream(from_pos)` restarts exactly after the
last committed transaction <= from_pos (REMOTE.md S3). Produces `rate_tps` transactions per second (wall clock),
optionally stopping after `max_txns`. Idle ticks (None) every `idle_tick_s`. `hang_after_s` makes the stream
block and, once the stream hung, `close()` too (a driver stuck in disconnect: shutdown-deadline drills; the stream
alone no longer blocks the daemon loop, LNX-31).
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional

TABLE = {"schema": "FAKE", "name": "EVENTS"}


def fmt(v: int) -> str:
    return f"{v:015d}"


class FakeConnector:
    engine = "fake"

    def __init__(self, settings: Optional[Dict[str, Any]] = None, connection_id: str = "") -> None:
        s = settings or {}
        self.cid = connection_id
        self.rate = float(s.get("rate_tps", 5))
        self.rows = max(1, int(s.get("rows_per_txn", 1)))
        self.max_txns = int(s["max_txns"]) if s.get("max_txns") is not None else None
        self.idle = float(s.get("idle_tick_s", 0.2))
        self.hang_after_s = float(s["hang_after_s"]) if s.get("hang_after_s") is not None else None  # deadline drills
        self.acked: Optional[str] = None
        self.hung = False

    @staticmethod
    def compare(a: str, b: str) -> int:
        x, y = int(a), int(b)
        return (x > y) - (x < y)

    @staticmethod
    def parse_position(text: str) -> str:
        return fmt(int(text))

    def read_catalog(self) -> Dict[str, Any]:
        cols = [{"name": "ID", "ordinal": 0, "engine_type": "BIGINT", "canonical_type": "LONG", "nullable": False,
                 "key_seq": 1},
                {"name": "K", "ordinal": 1, "engine_type": "INTEGER", "canonical_type": "INT", "nullable": False,
                 "key_seq": 2},
                {"name": "PAYLOAD", "ordinal": 2, "engine_type": "VARCHAR", "canonical_type": "STRING",
                 "length": 64, "nullable": True}]
        return {"connection_id": self.cid, "engine": "fake", "taken_at": datetime.now(timezone.utc).isoformat(),
                "position": None, "tables": [{"table": TABLE, "capture_enabled": True, "columns": cols}]}

    def current_position(self) -> Optional[str]:
        return None

    def stream(self, from_pos: Optional[str]) -> Iterator[Optional[Dict[str, Any]]]:
        n = int(from_pos) // 1000 + 1 if from_pos else 1
        t0, made = time.monotonic(), 0
        while True:
            if self.hang_after_s is not None and time.monotonic() - t0 >= self.hang_after_s:
                self.hung = True
                time.sleep(3600)                     # simulates a source call that never returns
            if (self.max_txns is not None and n > self.max_txns) or made >= (time.monotonic() - t0) * self.rate:
                time.sleep(self.idle)
                yield None
                continue
            txid = f"t{n}"
            for k in range(1, self.rows + 1):
                yield {"pos": fmt(n * 1000 + k), "txid": txid, "kind": "INSERT", **TABLE,
                       "after": {"ID": n, "K": k, "PAYLOAD": f"row {n}.{k}"}}
            yield {"pos": fmt(n * 1000 + 999), "txid": txid, "kind": "COMMIT",
                   "commit_ts": datetime.now(timezone.utc).isoformat()}
            n += 1
            made += 1

    @staticmethod
    def encode(ev: Dict[str, Any]) -> bytes:
        d = {"pos": ev["pos"], "txid": ev["txid"], "kind": ev["kind"], "schema": ev.get("schema"),
             "table": ev.get("name"), "before": None, "after": ev.get("after"),
             "commit_ts": {"$ts": ev["commit_ts"]} if ev.get("commit_ts") else None, "note": None}
        return json.dumps(d, separators=(",", ":")).encode() + b"\n"

    def ack(self, pos: str) -> None:
        self.acked = pos

    def close(self) -> None:
        if self.hung:
            time.sleep(3600)                         # driver stuck in disconnect (deadline drill)
