"""HA capture lease for the daemon: a thin adapter over the shared Python lease client
`hub/alelt_common/lease_client.py` (X-64; decision logic identical to the C agent's ha.c, X-47/X-48).

The adapter only supplies the transport (HubClient with per-call timeouts) and exposes the few attributes the
main loop reads. Standalone (`ha.mode=standalone`): always active, never calls the hub.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional, Tuple

from alelt_common.lease_client import ACTIVE, LOST, NONE, RENEWED, TAKEOVER, LeaseClient

from .hub import HubClient, HubError

log = logging.getLogger("aleltd.lease")

__all__ = ["ACTIVE", "LOST", "NONE", "RENEWED", "TAKEOVER", "Lease"]


class Lease:
    def __init__(self, enabled: bool, ttl_s: int, renew_s: int, agent_id: str, connection_id: str,
                 clock: Callable[[], float]) -> None:
        self.hub: Optional[HubClient] = None
        self.timeout_s = 10.0
        self.capturing: Optional[bool] = None
        self.client = LeaseClient(self._post, agent_id, connection_id, ttl_s=ttl_s, renew_s=renew_s,
                                  enabled=enabled, clock=clock, log=lambda m: log.info(m))
        self.clock = clock

    # transport for the shared client: (status, json); transport errors raise (the client retries / self-fences)
    def _post(self, path: str, body: Dict[str, Any]) -> Tuple[int, Any]:
        if self.hub is None:
            raise OSError("hub not configured")
        try:
            return 200, self.hub.post_json(path, body, timeout=self.timeout_s)
        except HubError as e:
            try:
                import json
                js = json.loads(e.body)
            except ValueError:
                js = None
            return e.status, js

    # ------------------------------------------------------------------ state the loop reads
    @property
    def enabled(self) -> bool:
        return self.client.enabled

    @property
    def active(self) -> bool:
        return self.client.state.active

    @property
    def epoch(self) -> int:
        return self.client.state.epoch

    @property
    def lease_id(self) -> Optional[str]:
        return self.client.state.lease_id

    @property
    def next_at(self) -> float:
        return self.client.state.next_at

    @property
    def renew_s(self) -> int:
        return self.client.state.renew_s

    def must_fence(self, now: float) -> bool:
        return self.client.state.must_fence(now)

    def due(self, now: float) -> bool:
        return self.client.state.due(now)

    def may_read(self) -> bool:
        return not self.enabled or self.client.may_read()

    def headers(self) -> Dict[str, str]:
        return self.client.headers() if self.enabled else {}

    # ------------------------------------------------------------------ actions
    def tick(self, timeout_s: float) -> Tuple[str, Optional[str]]:
        """(event kind, resume_from text)."""
        self.timeout_s = timeout_s
        ev = self.client.tick()
        self.capturing = ev.capturing          # AHS-24: operator pause carried by the grant
        rf = ev.resume_from.get("text") if isinstance(ev.resume_from, dict) else None
        return ev.kind, rf

    def on_lost(self, why: str) -> None:
        self.client.on_lost(why)

    def release(self, timeout_s: float) -> bool:
        self.timeout_s = timeout_s
        return self.client.release()
