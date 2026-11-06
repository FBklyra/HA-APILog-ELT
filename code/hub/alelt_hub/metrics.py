"""Minimal Prometheus text-format registry (no prometheus_client dependency).

Metric names are a contract: deploy/hub/observability/METRICS.md (X-16).
"""
from __future__ import annotations

import threading
from collections import defaultdict
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_Labels = tuple[tuple[str, str], ...]
BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0)


def _fmt(labels: _Labels, extra: tuple[tuple[str, str], ...] = ()) -> str:
    items = labels + extra
    return "{" + ",".join(f'{a}="{b}"' for a, b in items) + "}" if items else ""


class Registry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, dict[_Labels, float]] = defaultdict(dict)
        self._gauges: dict[str, dict[_Labels, float]] = defaultdict(dict)
        self._hist: dict[str, dict[_Labels, list[float]]] = defaultdict(dict)   # buckets..., sum, count
        self._help: dict[str, str] = {}
        self._collectors: list[Callable[[Registry], None]] = []

    def describe(self, name: str, text: str) -> None:
        self._help[name] = text

    def inc(self, name: str, value: float = 1.0, **labels: str) -> None:
        key = tuple(sorted(labels.items()))
        with self._lock:
            self._counters[name][key] = self._counters[name].get(key, 0.0) + value

    def set(self, name: str, value: float, **labels: str) -> None:
        with self._lock:
            self._gauges[name][tuple(sorted(labels.items()))] = float(value)

    def observe(self, name: str, value: float, **labels: str) -> None:
        key = tuple(sorted(labels.items()))
        with self._lock:
            h = self._hist[name].setdefault(key, [0.0] * (len(BUCKETS) + 2))
            for i, b in enumerate(BUCKETS):
                if value <= b:
                    h[i] += 1
            h[-2] += value
            h[-1] += 1

    def collector(self, fn: Callable[[Registry], None]) -> None:
        """Register a callback that refreshes gauges at scrape time."""
        if fn not in self._collectors:
            self._collectors.append(fn)

    def render(self) -> str:
        for fn in self._collectors:
            try:
                fn(self)
            except Exception:  # noqa: BLE001 - a failing collector must not break /metrics
                self.inc("alelt_metrics_collector_errors_total")
        out: list[str] = []
        with self._lock:
            for kind, series in (("counter", self._counters), ("gauge", self._gauges)):
                for name, vals in sorted(series.items()):
                    out += [f"# HELP {name} {self._help.get(name, name)}", f"# TYPE {name} {kind}"]
                    out += [f"{name}{_fmt(lab)} {v}" for lab, v in sorted(vals.items())]
            for name, vals in sorted(self._hist.items()):
                out += [f"# HELP {name} {self._help.get(name, name)}", f"# TYPE {name} histogram"]
                for lab, h in sorted(vals.items()):
                    for i, b in enumerate(BUCKETS):
                        out.append(f"{name}_bucket{_fmt(lab, (('le', str(b)),))} {h[i]}")
                    out.append(f"{name}_bucket{_fmt(lab, (('le', '+Inf'),))} {h[-1]}")
                    out += [f"{name}_sum{_fmt(lab)} {h[-2]}", f"{name}_count{_fmt(lab)} {h[-1]}"]
        return "\n".join(out) + "\n"


def serve(registry: Registry, port: int) -> ThreadingHTTPServer:
    """Expose `registry` on http://0.0.0.0:<port>/metrics in a daemon thread (workers)."""
    class H(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            body = registry.render().encode() if self.path.startswith("/metrics") else b"ok\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_: object) -> None:
            pass

    srv = ThreadingHTTPServer(("0.0.0.0", port), H)
    threading.Thread(target=srv.serve_forever, daemon=True, name="metrics").start()
    return srv


REGISTRY = Registry()
for _n, _t in {
    "alelt_batches_received_total": "raw batches received (status ok / sha_mismatch / duplicate / rejected reason)",
    "alelt_batch_bytes_total": "uncompressed raw bytes durably received",
    "alelt_decode_errors_total": "decoder / pipeline failures",
    "alelt_quarantine_total": "records sent to quarantine (R7)",
    "alelt_merge_duration_seconds": "destination apply+checkpoint transaction",
    "alelt_capture_lag_seconds": "capture lag: 0 when caught up and the source is alive, else now - commit_ts of the "
                              "last applied change (at least the age of the oldest waiting batch), HC-16",
    "alelt_batch_oldest_unloaded_age_seconds": "age of the oldest received batch not applied yet (RECEIVED/DECODED)",
    "alelt_batches_unloaded": "received batches not applied yet",
    "alelt_jobs_failed": "FAILED jobs per kind",
    "alelt_job_oldest_failed_age_seconds": "age of the oldest FAILED job per kind",
    "alelt_job_stuck": "data-path jobs past max attempts, retried hourly (XC-10)",
    "alelt_checkpoint_seq": "last applied batch seq per stream (position progress)",
    "alelt_latency_target_seconds": "configured latency target",
    "alelt_spool_bytes": "spool usage",
    "alelt_capture_enabled": "1 if DATA CAPTURE CHANGES present on an in-scope table",
    "alelt_agent_last_heartbeat_timestamp_seconds": "last agent heartbeat, epoch s",
    "alelt_agent_cpu_percent": "agent CPU from heartbeat",
    "alelt_agent_mem_mb": "agent memory from heartbeat",
    "alelt_queue_depth": "pending jobs per queue",
    "alelt_txn_open": "open source transactions per stream (alelt_txn)",
    "alelt_txn_oldest_open_seconds": "age of the oldest open source transaction (alelt_txn)",
    "alelt_table_integrity": "1 for the current integrity state of a table (IN_SYNC/SUSPECT/REDECODING/OUT_OF_SYNC/"
                          "RESYNCING), F-01",
    "alelt_table_held": "1 while a table is held (kind snapshot / schedule / integrity)",
    "alelt_integrity_events_total": "integrity events (hold, suspect, stop_stream, recovered_*)",
    "alelt_quarantine_out_of_scope_total": "quarantined records of tables outside the replicated scope (counted only)",
    "alelt_replay_pass_seconds": "duration of one multi-table replay pass",
    "alelt_decode_raw_total": "raw batch decodes (decoded-event cache misses)",
    "alelt_decode_lag_batches": "batches received and not decoded yet per connection (RC4, alert AleltDecodeLag)",
    "alelt_decode_lag_seconds": "age of the oldest received, not yet decoded batch per connection (RC4)",
}.items():
    REGISTRY.describe(_n, _t)
