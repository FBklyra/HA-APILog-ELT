"""Remote engines (postgres, mysql, mariadb, sqlserver, oracle) captured next to the database by the agent:
`SourceConnector.stream()` -> whole transactions -> canonical JSON Lines batches (connectors/sources/_spi/REMOTE.md
§3-§4, same format and batch ids as the hub-side runner hub/alelt_capture) -> spool -> POST /agent/v1/blocks.

Batches end on transaction boundaries only. `ack(next_pos)` is called on the connector only after the hub ACKed
the batch (never ahead of the hub). A partial transaction at shutdown is not batched: the stream resumes from the
last batch's commit token and re-delivers it whole.
"""
from __future__ import annotations

import hashlib
import queue
import threading
import time
import uuid
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from ..config import Config
from .base import Batch, CaptureSource, SourceError, Step, TxnTooLarge

NS = uuid.UUID("6f1c2a52-4b0e-5d8e-9a51-6a2f1b0c9e11")   # = hub/alelt_capture/runner.py (dedup of re-sent batches)
END_KINDS = ("COMMIT", "ROLLBACK")
STEP_BUDGET_S = 1.0
STALL_S = 30.0          # SPI S1: stream() yields None at least every ~1 s; a longer block is a contract violation
STALL_RECONNECT_S = 300.0   # LNX-31: a stream() call blocked this long is abandoned: close + reconnect from the chain


class _Pull:
    """LNX-31: `next(stream)` runs on a reader thread, ONE call at a time (the connector is never called twice
    concurrently from the stream side); the daemon thread waits with a timeout, so a connector that blocks (network
    hang, server stall) never freezes heartbeats, the lease or the watchdog."""

    def __init__(self, gen: Iterator[Any]) -> None:
        self.gen = gen
        self.req = threading.Event()
        self.res: "queue.Queue[Tuple[str, Any]]" = queue.Queue(maxsize=1)
        self.busy = False
        self.since = 0.0
        self.dead = False
        threading.Thread(target=self._run, name="alelt-source-read", daemon=True).start()

    def _run(self) -> None:
        while True:
            self.req.wait()
            self.req.clear()
            if self.dead:
                return
            try:
                item: Tuple[str, Any] = ("ev", next(self.gen))
            except StopIteration:
                item = ("end", None)
            except BaseException as e:  # noqa: BLE001 - handed to the daemon thread
                item = ("err", e)
            if self.dead:
                return
            self.res.put(item)
            if item[0] != "ev":
                return

    def get(self, timeout: float) -> Optional[Tuple[str, Any]]:
        """Next item, or None while the call is still running after `timeout` s (see `stalled_s`)."""
        if not self.busy:
            self.busy, self.since = True, time.monotonic()
            self.req.set()
        try:
            item = self.res.get(timeout=max(0.0, timeout))
        except queue.Empty:
            return None
        self.busy = False
        return item

    def stalled_s(self) -> float:
        return time.monotonic() - self.since if self.busy else 0.0

    def close(self) -> None:
        """Stop the reader. A call still blocked inside the connector is abandoned (closing the connection
        unblocks it); the generator is closed only when no call is running."""
        self.dead = True
        self.req.set()
        if not self.busy and hasattr(self.gen, "close"):
            try:
                self.gen.close()  # type: ignore[union-attr]
            except Exception:  # noqa: BLE001
                pass


def check_idle_ticks(connector: Any, from_pos: Optional[Any], seconds: float = 5.0, max_gap_s: float = 2.0) -> float:
    """Conformance check for a connector (LNX-31): iterate `stream(from_pos)` for `seconds` and return the longest
    gap between two yields. A connector whose gap exceeds `max_gap_s` would stall heartbeats, the lease and the
    watchdog of the single-threaded agent: raise AssertionError."""
    gen = iter(connector.stream(from_pos))
    end = time.monotonic() + seconds
    last, worst = time.monotonic(), 0.0
    try:
        while time.monotonic() < end:
            next(gen)
            now = time.monotonic()
            worst, last = max(worst, now - last), now
    finally:
        if hasattr(gen, "close"):
            gen.close()
    if worst > max_gap_s:
        raise AssertionError(f"stream() blocked {worst:.1f}s without yielding (contract: idle tick <= {max_gap_s}s)")
    return worst


def _codec() -> str:
    try:
        import zstandard  # noqa: F401
        return "zstd"
    except ImportError:
        return "zlib"


def _compress(raw: bytes, codec: str) -> bytes:
    if codec == "zstd":
        import zstandard
        return bytes(zstandard.ZstdCompressor(level=3).compress(raw))
    import zlib
    return zlib.compress(raw, 6)


class StreamCapture(CaptureSource):
    """Adapter over a SourceConnector (pydantic ChangeEvent) or a stdlib connector (`native=True`: events are dicts
    already in canonical-line form, see fake.py)."""

    def __init__(self, cfg: Config, factory: Callable[[], Any], native: bool = False,
                 decoder: Optional[Callable[[], Any]] = None) -> None:
        self.cfg = cfg
        self.factory = factory
        self.native = native
        self.decoder_factory = decoder or factory
        self._decoder: Any = None
        self.conn: Any = None
        self.gen: Optional[Iterator[Any]] = None
        self.codec = _codec()
        self._reset_buffers()
        self.need_catalog = False
        self.next_catalog = 0.0
        self.engine = cfg.source.engine
        self.pull: Optional[_Pull] = None
        self.stall_reconnect_s = STALL_RECONNECT_S
        if not native:
            from alelt_common.canonical_jsonl import encode_event      # needs pydantic (hub/alelt_common)
            self._encode_event = encode_event

    @classmethod
    def for_connector(cls, ref: str, cfg: Config) -> "StreamCapture":
        """'module:Class' SourceConnector; a no-argument instance is decode-only (positions, no connection)."""
        import importlib
        mod, _, attr = ref.partition(":")
        try:
            klass = getattr(importlib.import_module(mod), attr)
        except (ImportError, AttributeError) as e:
            raise SourceError(f"connector {ref} unavailable: {e}") from None

        def make() -> Any:
            return klass(settings=dict(cfg.source.settings), password=cfg.source.password(),
                         connection_id=cfg.connection_id)
        return cls(cfg, make, decoder=klass)

    # ------------------------------------------------------------------ helpers
    def _reset_buffers(self) -> None:
        self.txn: List[bytes] = []
        self.txn_bytes = 0
        self.pending: List[bytes] = []
        self.pending_bytes = 0
        self.pending_since = 0.0
        self.first_pos: Optional[str] = None
        self.token: Optional[str] = None

    def _c(self) -> Any:
        if self.conn is None:
            try:
                self.conn = self.factory()
            except Exception as e:  # noqa: BLE001 - connection / prerequisite failure
                raise SourceError(f"{type(e).__name__}: {e}") from None
        return self.conn

    def _dec(self) -> Any:
        if self._decoder is None:
            self._decoder = self.decoder_factory()
        return self._decoder

    def _pos(self, text: str) -> Any:
        return text if self.native else self._dec().parse_position(text)

    def _event(self, ev: Any) -> Tuple[bytes, str, str]:
        if self.native:
            return self.conn.encode(ev), ev["kind"], ev["pos"]
        return self._encode_event(ev), ev.kind.value, ev.position.text

    # ------------------------------------------------------------------ CaptureSource
    def targets(self) -> Dict[str, Any]:
        return dict(self.cfg.source.settings)

    def compare(self, a: str, b: str) -> int:
        d = self._dec()
        if self.native:
            return int(d.compare(a, b))
        return int(d.compare(d.parse_position(a), d.parse_position(b)))

    def validate(self, pos: str) -> str:
        p = self._pos(pos)
        return p if isinstance(p, str) else str(p.text)

    def catalog(self, valid_from: Optional[str]) -> Dict[str, Any]:
        try:
            snap = self._c().read_catalog()
        except SourceError:
            raise
        except Exception as e:  # noqa: BLE001
            self.close()
            raise SourceError(f"catalog: {type(e).__name__}: {e}") from None
        d: Dict[str, Any] = snap if isinstance(snap, dict) else snap.model_dump(mode="json", by_alias=True)
        d["connection_id"] = self.cfg.connection_id
        d["position"] = {"engine": self.engine, "text": valid_from} if valid_from else None
        self.need_catalog = False
        self.next_catalog = time.monotonic() + self.cfg.capture.catalog_s
        return d

    def initial_position(self, start: str) -> Optional[str]:
        if start in ("current", "oldest"):
            return None                       # S5: the source's retained position or the current end
        return self.validate(start)

    def reset(self, chain: Optional[str]) -> None:
        self._close_gen()
        self._reset_buffers()

    def _close_gen(self) -> None:
        g, self.gen = self.gen, None
        p, self.pull = getattr(self, "pull", None), None
        if p is not None:
            p.close()
        elif g is not None and hasattr(g, "close"):
            try:
                g.close()
            except Exception:  # noqa: BLE001
                pass

    def _cut(self, chain: Optional[str]) -> Batch:
        assert self.token is not None
        raw = b"".join(self.pending)
        from_pos = chain or self.first_pos or self.token
        body = _compress(raw, self.codec)
        hdr = {"batch_id": str(uuid.uuid5(NS, f"{self.cfg.connection_id}|{from_pos}|{self.token}")),
               "connection_id": self.cfg.connection_id, "agent_id": self.cfg.agent_id, "engine": self.engine,
               "from_pos": {"engine": self.engine, "text": from_pos},
               "to_pos": {"engine": self.engine, "text": self.token},
               "next_pos": {"engine": self.engine, "text": self.token},
               "rec_count": len(self.pending), "raw_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
               "codec": self.codec, "protocol": 1}
        self.pending, self.pending_bytes, self.first_pos = [], 0, None
        return hdr, body

    def step(self, chain: Optional[str], limits: Dict[str, int]) -> Step:
        c = self._c()
        if self.gen is None:
            try:
                self.gen = iter(c.stream(self._pos(chain) if chain else None))
            except Exception as e:  # noqa: BLE001
                self.close()
                raise SourceError(f"stream: {type(e).__name__}: {e}") from None
            self.pull = _Pull(self.gen)
        st = Step()
        batch_bytes = max(16, int(limits.get("batch_kb", 1024))) * 1024
        max_txn = self.cfg.capture.max_txn_mb << 20
        end = time.monotonic() + STEP_BUDGET_S
        self.last_stall_s = 0.0
        while True:
            assert self.pull is not None
            item = self.pull.get(min(STEP_BUDGET_S, max(0.05, end - time.monotonic())))
            if item is None:                  # the connector call is still blocked: give the loop back (LNX-31)
                stalled = self.pull.stalled_s()
                self.last_stall_s = max(self.last_stall_s, stalled)
                if stalled >= self.stall_reconnect_s:
                    self.close()
                    raise SourceError(f"stream: source read blocked {stalled:.0f}s (no idle tick): reconnecting")
                return st
            what, ev = item
            if what == "end":
                self.close()
                raise SourceError("source stream ended") from None
            if what == "err":                 # S8: reconnect from the chain
                self.close()
                raise SourceError(f"stream: {type(ev).__name__}: {ev}") from None
            now = time.monotonic()
            self.last_stall_s = max(self.last_stall_s, now - self.pull.since)
            if ev is not None:
                line, kind, pos = self._event(ev)
                if self.first_pos is None and not self.txn and not self.pending:
                    self.first_pos = pos
                self.txn.append(line)
                self.txn_bytes += len(line)
                if self.txn_bytes > max_txn:
                    self.close()
                    raise TxnTooLarge(f"transaction {ev.get('txid') if isinstance(ev, dict) else ''} exceeds "
                                      f"capture.max_txn_mb ({self.cfg.capture.max_txn_mb}) after {chain}: raise "
                                      "capture.max_txn_mb (memory permitting) to let capture pass it")
                if kind == "DDL":
                    self.need_catalog = True
                if kind in END_KINDS:
                    if not self.pending:
                        self.pending_since = now
                    self.pending.extend(self.txn)
                    self.pending_bytes += self.txn_bytes
                    self.txn, self.txn_bytes = [], 0
                    self.token = pos
            due = bool(self.pending) and (self.pending_bytes >= batch_bytes
                                          or (now - self.pending_since) * 1000 >= self.cfg.capture.batch_ms)
            if due:
                if self.need_catalog or now >= self.next_catalog:
                    st.catalog = self.catalog(chain or self.first_pos)
                st.batches.append(self._cut(chain))
                return st
            if ev is None or now >= end:
                return st

    def flush(self, chain: Optional[str]) -> List[Batch]:
        return [self._cut(chain)] if self.pending and self.token else []

    def acked(self, durable: str) -> None:
        if self.pull is not None and self.pull.busy:
            return                            # never call the connector while a stream call runs; next ack catches up
        if self.conn is not None and self.gen is not None:
            try:
                self.conn.ack(self._pos(durable))
            except Exception:  # noqa: BLE001 - retention release is best effort; next ack catches up
                pass

    def probe(self, name: str) -> Dict[str, Any]:
        if name == "current_position":
            p = self._c().current_position()
            return {"current": (p if isinstance(p, str) else p.text) if p is not None else None}
        if name == "db_health":
            self.catalog(None)
            return {"ok": True}
        raise ValueError(f"probe {name!r} not whitelisted")

    def verify_table(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Generic VERIFY_TABLE: rows read by a separate connector instance through its read-only snapshot path,
        aggregated on the edge host (alelt_common.verify_edge.compute_from_rows)."""
        if self.native:
            raise SourceError("VERIFY_TABLE is not supported by this source")
        from alelt_common import verify_edge as ve
        from alelt_common.models import TableRef

        a = ve.validate_args(args)
        ref = TableRef(schema=a["table"]["schema"], name=a["table"]["name"])
        c = self.factory()
        try:
            snap = c.read_catalog()
            info = next((t for t in snap.tables if t.table == ref), None)
            if info is None:
                raise ve.VerifyArgsError(f"table {ref.fqn} not found")
            keys = [k.name for k in info.key_columns()]
            types = {col.name: col.canonical_type for col in info.columns}
            classes = {col.name: ve.agg_class(col.canonical_type, col.engine_type or "") for col in info.columns}

            def rows() -> Any:
                for page in c.snapshot_rows(info, None, 10_000):
                    yield from page

            def key_of(texts: Any) -> Any:
                return tuple(ve.parse_text(t, types[k]) for k, t in zip(keys, texts))

            def pos() -> Optional[str]:
                p = c.current_position()
                return (p if isinstance(p, str) else p.text) if p is not None else None
            return ve.compute_from_rows(a, rows(), keys, classes, key_of, self.engine, pos)
        except (ve.VerifyArgsError, SourceError):
            raise
        except Exception as e:  # noqa: BLE001
            raise SourceError(f"verify: {type(e).__name__}: {e}") from None
        finally:
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass

    def close(self) -> None:
        self._close_gen()
        self._reset_buffers()
        c, self.conn = self.conn, None
        if c is not None:
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass
