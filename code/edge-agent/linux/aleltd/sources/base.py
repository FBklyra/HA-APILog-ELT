"""What the daemon needs from a source plugin."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

Batch = Tuple[Dict[str, Any], bytes]          # (BatchHeader dict, body as shipped)


class SourceError(RuntimeError):
    """Source connection / protocol error: the daemon closes the source, backs off and resumes from its chain."""


class TxnTooLarge(SourceError):
    """One transaction exceeds capture.max_txn_mb (LNX-14): capture cannot pass it until the limit is raised."""


# REQ-17 / LNX-06: source failures as AgentEvents the hub diagnose rules match (same classes as the C agent's
# source_event: SQL30082N auth, SQL1273N / SQL2657N / SQL2650N / SQL1262N log not available)
_SQLCODE = re.compile(r"(?i)(?:\bSQL(\d{4,5})[NWC]?\b|\bsqlcode\s*[=:]?\s*(-?\d+))")


def classify_source_error(msg: str) -> Optional[Tuple[str, int]]:
    """(event type, sqlcode) for the failures the hub rules know, else None."""
    m = _SQLCODE.search(msg or "")
    code = 0
    if m:
        code = -int(m.group(1)) if m.group(1) else int(m.group(2))
    low = (msg or "").lower()
    if code in (-30082, -1639, -30083) or low.startswith("authentication") or "password authentication failed" in low:
        return "DB2_AUTH_FAILED", code
    if code in (-1273, -2657, -2650, -1262) or "log file" in low or "log_not_available" in low:
        return "LOG_NOT_AVAILABLE", code
    return None


@dataclass
class Step:
    batches: List[Batch] = field(default_factory=list)
    catalog: Optional[Dict[str, Any]] = None  # spool this snapshot BEFORE the batches (schema change)
    wait_s: float = 0.0                       # suggested idle wait before the next step (0 = call again now)


class CaptureSource:
    engine = ""
    # set by the daemon: called with the connected server's identity right after a (re)connect (LNX-08)
    on_connect: Optional[Callable[[Dict[str, Any]], None]] = None
    last_stall_s: float = 0.0             # longest blocking source call in the last step (LNX-31 contract check)

    def targets(self) -> Dict[str, Any]:
        """Host-like settings checked against deny_hosts before any connection."""
        raise NotImplementedError

    def compare(self, a: str, b: str) -> int:
        raise NotImplementedError

    def validate(self, pos: str) -> str:
        """Canonical text of a position (ValueError when malformed)."""
        raise NotImplementedError

    def catalog(self, valid_from: Optional[str]) -> Dict[str, Any]:
        """CatalogSnapshot JSON valid from `valid_from` (the next batch's from_pos)."""
        raise NotImplementedError

    def initial_position(self, start: str) -> Optional[str]:
        """First start only (no spool, state or hub position): current | oldest | explicit position.
        None = let the source decide (remote engines: retained slot position or current end)."""
        raise NotImplementedError

    def reset(self, chain: Optional[str]) -> None:
        """Forget anything read after `chain` and continue reading from there."""
        raise NotImplementedError

    def step(self, chain: Optional[str], limits: Dict[str, int]) -> Step:
        """Read a bounded amount (~1 s); return complete batches whose from_pos chains from `chain`."""
        raise NotImplementedError

    def flush(self, chain: Optional[str]) -> List[Batch]:
        """Shutdown: complete transactions already read but not yet batched (partial ones are re-read later)."""
        return []

    def acked(self, durable: str) -> None:
        """`durable` (a batch next_pos) is durable at the hub."""

    def probe(self, name: str) -> Dict[str, Any]:
        raise ValueError(f"probe {name!r} not supported")

    def verify_table(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """VERIFY_TABLE (F-03): VerifyTableResult for whitelisted args (alelt_common.verify_edge.validate_args);
        read-only. Sources without verification support refuse."""
        raise NotImplementedError("VERIFY_TABLE is not supported by this source")

    def snapshot_start(self) -> str:
        """Log position read before a SNAPSHOT_TABLE (chunk header start_pos)."""
        raise NotImplementedError("SNAPSHOT_TABLE is not supported by this source")

    def snapshot_chunks(self, schema: str, table: str, chunk_rows: int, key_from: Any = None) -> Any:
        """SNAPSHOT_TABLE reads: yields (rows, key_from, last, last_key); resumes after `key_from`."""
        raise NotImplementedError("SNAPSHOT_TABLE is not supported by this source")

    def close(self) -> None:
        """Idempotent."""
