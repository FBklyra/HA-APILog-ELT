"""CaptureRunner: one remote connection, one active runner (lock `capture:<cid>`).

stream() -> whole transactions -> batch (txn boundaries only) -> durable receipt -> source.ack(next_pos).
Restart resumes from the last durable batch next_pos; the source acknowledgement never runs ahead of the hub.

Large transactions (HC-08): when the open transaction reaches ALELT_CAPTURE_TXN_CHUNK_MB (64) it is shipped in chunk
batches `from_pos = to_pos = next_pos = S` (S = the resume token before the transaction), so the hub assembler
(which spills) collects it across batches and the checkpoint never moves inside a transaction. Memory stays
bounded; capture never stops for size. On restart the source re-delivers the transaction from S: the lines already
durable in the trailing chunk batches (count + txid read from the hub) are skipped, so nothing is applied twice.
Connectors buffer open transactions in `alelt_common.spill.SpillBuffer`; only its disk cap
(ALELT_CAPTURE_MAX_SPILL_MB) stops capture (SpillLimitExceeded -> ERROR + incident, never a silent loss).
"""
from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import Engine

from alelt_common.canonical_jsonl import encode_event
from alelt_common.models import BatchHeader, ChangeKind, Position
from alelt_common.spi import SourceConnector, is_gap_error
from alelt_common.util import compress, sha256_hex, utcnow
from alelt_hub.dialect import try_lock, unlock
from alelt_hub.metrics import REGISTRY

from . import receipt

log = logging.getLogger("alelt_capture")
NS = uuid.UUID("6f1c2a52-4b0e-5d8e-9a51-6a2f1b0c9e11")   # uuid5 namespace for batch ids


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _codec() -> str:
    try:
        import zstandard  # noqa: F401
        return "zstd"
    except ImportError:
        return "zlib"


@dataclass
class CaptureConfig:
    batch_bytes: int = field(default_factory=lambda: _env_int("ALELT_CAPTURE_BATCH_KB", 1024) << 10)
    batch_ms: int = field(default_factory=lambda: _env_int("ALELT_CAPTURE_BATCH_MS", 1000))
    max_txn_bytes: int = field(default_factory=lambda: _env_int("ALELT_CAPTURE_MAX_TXN_MB", 1024) << 20)  # deprecated
    txn_chunk_bytes: int = field(default_factory=lambda: _env_int("ALELT_CAPTURE_TXN_CHUNK_MB", 64) << 20)
    keepalive_s: float = field(default_factory=lambda: float(os.environ.get("ALELT_CAPTURE_KEEPALIVE_S", "10")))
    catalog_s: int = field(default_factory=lambda: _env_int("ALELT_CAPTURE_CATALOG_S", 300))
    retry_s: float = field(default_factory=lambda: float(os.environ.get("ALELT_CAPTURE_RETRY_S", "5")))
    # circuit breaker: the same connector error this many times in a row without a durable batch = deterministic
    # (e.g. a record the connector always rejects): stop the connection + incident instead of retrying forever
    max_same_errors: int = field(default_factory=lambda: _env_int("ALELT_CAPTURE_MAX_SAME_ERRORS", 20))
    codec: str = field(default_factory=_codec)
    start_pos: str | None = None            # first start only (no durable batch yet)


class CaptureError(RuntimeError):
    pass


class CaptureRunner:
    def __init__(self, engine: Engine, connection_id: str, factory: Callable[[], SourceConnector],
                 cfg: CaptureConfig | None = None, database: str | None = None) -> None:
        self.engine = engine
        self.cid = connection_id
        self.factory = factory
        self.cfg = cfg or CaptureConfig()
        self.database = database
        self.stop = threading.Event()
        self.batches = 0
        self.events = 0
        self.state = "IDLE"
        self.last_error: str | None = None

    # ------------------------------------------------------------------ public
    def run(self, idle_exit_s: float | None = None, max_restarts: int | None = None) -> str:
        """Run until stop() (or, with idle_exit_s, until the source was idle that long with nothing pending).
        Returns 'locked' when another runner owns the connection, else 'stopped' / 'idle'."""
        key = f"capture:{self.cid}"
        lock = try_lock(self.engine, key)
        if lock is None:
            return "locked"
        restarts = 0
        same, last_sig, progress = 0, "", self.batches
        try:
            while not self.stop.is_set():
                try:
                    if self._session(idle_exit_s) == "idle":
                        return "idle"
                except receipt.GapError as e:          # chain broken: a bug or a second writer - never re-chain
                    self.state, self.last_error = "ERROR", f"gap: {e}"
                    log.error("capture stopped: position gap", extra={"component": "alelt_capture",
                                                                      "correlation_id": self.cid})
                    raise CaptureError(self.last_error) from None
                except CaptureError:
                    raise
                except Exception as e:  # noqa: BLE001 - reconnect from the durable position
                    if type(e).__name__ in ("TransactionTooLarge", "SpillLimitExceeded"):   # disk cap (HC-08)
                        self.state, self.last_error = "ERROR", f"{type(e).__name__}: {e}"
                        REGISTRY.inc("alelt_capture_errors_total", connection_id=self.cid, reason="txn_too_large")
                        log.error("capture stopped: %s (raise ALELT_CAPTURE_MAX_SPILL_MB or resync)", self.last_error,
                                  extra={"component": "alelt_capture", "correlation_id": self.cid})
                        from alelt_diagnose import hooks as diag_hooks

                        diag_hooks.capture_stuck(self.engine, self.cid, self.last_error, 1)
                        raise CaptureError(self.last_error) from e
                    if is_gap_error(e):                # REQ-20: retention passed the resume point - retry cannot help
                        self.state, self.last_error = "ERROR", f"source log gap: {type(e).__name__}: {e}"
                        REGISTRY.inc("alelt_capture_errors_total", connection_id=self.cid, reason="source_gap")
                        log.error("capture stopped: %s (resync needed)", self.last_error,
                                  extra={"component": "alelt_capture", "correlation_id": self.cid})
                        from alelt_diagnose import hooks as diag_hooks

                        diag_hooks.capture_gap(self.engine, self.cid, self.last_error)
                        raise CaptureError(self.last_error) from e
                    restarts += 1
                    sig = f"{type(e).__name__}: {str(e)[:200]}"
                    same = same + 1 if (sig == last_sig and self.batches == progress) else 1
                    last_sig, progress = sig, self.batches
                    if self.cfg.max_same_errors and same >= self.cfg.max_same_errors:
                        self.state, self.last_error = "ERROR", f"repeated failure x{same}: {sig}"
                        REGISTRY.inc("alelt_capture_errors_total", connection_id=self.cid, reason="circuit_breaker")
                        log.error("capture stopped: %s", self.last_error,
                                  extra={"component": "alelt_capture", "correlation_id": self.cid})
                        from alelt_diagnose import hooks as diag_hooks

                        diag_hooks.capture_stuck(self.engine, self.cid, self.last_error, same)
                        raise CaptureError(self.last_error) from e
                    self.state, self.last_error = "RETRY", f"{type(e).__name__}: {e}"
                    REGISTRY.inc("alelt_capture_errors_total", connection_id=self.cid, reason=type(e).__name__)
                    log.warning("capture session failed, resuming from durable position: %s", self.last_error,
                                extra={"component": "alelt_capture", "correlation_id": self.cid})
                    if max_restarts is not None and restarts > max_restarts:
                        raise
                    self.stop.wait(self.cfg.retry_s)
            return "stopped"
        finally:
            self.state = "STOPPED" if self.state != "ERROR" else self.state
            unlock(lock, key)

    # ------------------------------------------------------------------ one source session
    def _session(self, idle_exit_s: float | None) -> str:
        src = self.factory()
        try:
            eng = src.engine
            receipt.ensure_registered(self.engine, self.cid, eng, self.database)
            resume = receipt.durable_resume(self.engine, self.cid, eng)
            first = self.cfg.start_pos or receipt.first_start_pos(self.engine, self.cid)   # REQ-21: snapshot start
            start = resume or (src.parse_position(first) if first else None)
            chain: Position | None = resume
            self._refresh_catalog(src, chain or start)
            next_catalog = time.monotonic() + self.cfg.catalog_s
            need_catalog = False
            self.state = "CAPTURING"
            log.info("capture session start", extra={"component": "alelt_capture", "correlation_id": self.cid,
                                                      "position": start.text if start else None})
            txn: list[bytes] = []
            txn_bytes = 0
            txn_id: str | None = None
            chunks, skip_txid, skip_left = receipt.open_chunks(self.engine, self.cid, resume)
            chunked = (skip_txid, chunks) if chunks else None   # (txid, chunks shipped) of the open txn
            txn_ddl = False
            pending: list[bytes] = []
            pending_bytes = 0
            pending_since = 0.0
            first_pos: Position | None = None
            token: Position | None = None
            idle_since = time.monotonic()
            for ev in src.stream(start):
                now = time.monotonic()
                if ev is not None:
                    idle_since = now
                    if skip_left and ev.txid == skip_txid and ev.kind not in (ChangeKind.COMMIT, ChangeKind.ROLLBACK):
                        skip_left -= 1                 # already durable in a chunk batch before the restart
                        continue
                    if first_pos is None:
                        first_pos = ev.position
                    line = encode_event(ev)
                    txn.append(line)
                    txn_bytes += len(line)
                    txn_id = ev.txid
                    if txn_bytes >= self.cfg.txn_chunk_bytes and ev.kind not in (ChangeKind.COMMIT,
                                                                                ChangeKind.ROLLBACK):
                        anchor = chain or start
                        if anchor is not None:          # HC-08: ship the open transaction in chunks
                            if pending and token is not None:
                                chain = anchor = self._ship(src, pending, anchor, token)
                                pending, pending_bytes = [], 0
                            k = chunked[1] if chunked and chunked[0] == ev.txid else 0
                            self._ship(src, txn, anchor, anchor, chunk=(ev.txid, k))
                            chunked = (ev.txid, k + 1)
                            txn, txn_bytes, first_pos = [], 0, None
                            chain = anchor
                            continue
                    if ev.kind is ChangeKind.DDL:
                        txn_ddl = True
                    if ev.kind in (ChangeKind.COMMIT, ChangeKind.ROLLBACK):
                        if chunked and chunked[0] == ev.txid:
                            chunked, skip_left = None, 0
                        if not pending:
                            pending_since = now
                        pending.extend(txn)
                        pending_bytes += txn_bytes
                        txn, txn_bytes = [], 0
                        token = ev.position
                        if txn_ddl:            # SRC-cap-04: cut the batch at the DDL transaction
                            need_catalog, txn_ddl = True, False
                due = pending and (pending_bytes >= self.cfg.batch_bytes or need_catalog
                                   or (now - pending_since) * 1000 >= self.cfg.batch_ms or self.stop.is_set())
                if due and token is not None:
                    if now >= next_catalog and not need_catalog:
                        self._refresh_catalog(src, chain or first_pos)
                        next_catalog = now + self.cfg.catalog_s
                    # SRC-cap-05: the first batch starts at the resume token it was read from (start), not at the
                    # position of its first DML line
                    chain = self._ship(src, pending, chain or start or first_pos or token, token)
                    pending, pending_bytes, first_pos = [], 0, None
                    if need_catalog:
                        # events up to the DDL commit were shipped with the catalog in force before it; the new
                        # shape is valid from the DDL's commit token = the next batch's from_pos
                        self._refresh_catalog(src, chain)
                        need_catalog, next_catalog = False, now + self.cfg.catalog_s
                if not pending and not txn:
                    if self.stop.is_set():
                        return "stopped"
                    if idle_exit_s is not None and now - idle_since >= idle_exit_s:
                        return "idle"
            raise CaptureError("source stream ended")      # streams are endless; reconnect
        finally:
            try:
                src.close()
            except Exception:  # noqa: BLE001
                pass

    def _refresh_catalog(self, src: SourceConnector, valid_from: Position | None) -> None:
        """Catalog valid from the next batch's from_pos (the pipeline picks newest valid_from <= batch from_pos)."""
        snap = src.read_catalog()
        snap.connection_id = self.cid
        snap.position = valid_from
        vid, created = receipt.ingest_catalog(self.engine, snap)
        if created:
            log.info("catalog version stored", extra={"component": "alelt_capture", "correlation_id": f"v{vid}",
                                                      "position": valid_from.text if valid_from else None})

    def _ship(self, src: SourceConnector, lines: list[bytes], from_pos: Position, to_pos: Position,
              chunk: tuple[str, int] | None = None) -> Position:
        raw = b"".join(lines)
        body = compress(raw, self.cfg.codec)
        name = f"{self.cid}|{from_pos.text}|{to_pos.text}" + (f"|chunk|{chunk[0]}|{chunk[1]}" if chunk else "")
        hdr = BatchHeader(
            batch_id=uuid.uuid5(NS, name), connection_id=self.cid,
            agent_id=receipt.agent_id_for(self.cid), engine=src.engine, from_pos=from_pos, to_pos=to_pos,
            next_pos=to_pos, rec_count=len(lines), raw_bytes=len(raw), sha256=sha256_hex(raw),
            codec=self.cfg.codec)  # type: ignore[arg-type]
        while True:
            try:
                ack = receipt.receive(hdr, body)
                break
            except receipt.Backpressure:
                self.state = "BACKPRESSURE"
                REGISTRY.inc("alelt_capture_backpressure_total", connection_id=self.cid)
                waited = 0.0
                while waited < self.cfg.retry_s:        # XC-18: keep the source session alive while waiting
                    step = min(self.cfg.retry_s - waited, self.cfg.keepalive_s)
                    if self.stop.wait(step):
                        raise CaptureError("stopped while the hub applied backpressure") from None
                    waited += step
                    keep = getattr(src, "keepalive", None)
                    if callable(keep):
                        try:
                            keep()
                        except Exception as ke:  # noqa: BLE001 - the next read reports a dead session
                            log.warning("source keepalive failed: %s", ke, extra={"component": "alelt_capture",
                                                                                 "correlation_id": self.cid})
        self.state = "CAPTURING"
        src.ack(to_pos)                       # only now: the batch is durable at the hub
        self.batches += 1
        self.events += len(lines)
        REGISTRY.inc("alelt_capture_batches_total", connection_id=self.cid)
        REGISTRY.inc("alelt_capture_events_total", float(len(lines)), connection_id=self.cid)
        log.info("batch durable", extra={"component": "alelt_capture", "batch_id": str(hdr.batch_id),
                                         "position": to_pos.text, "correlation_id": f"dup={ack.duplicate}"})
        return to_pos

    def status(self) -> dict[str, Any]:
        return {"connection_id": self.cid, "state": self.state, "batches": self.batches, "events": self.events,
                "last_error": self.last_error, "at": utcnow().isoformat()}
