"""Support-bundle contribution (packaging stream calls this): redacted incident summary, no row values."""
from __future__ import annotations

from typing import Any

from sqlalchemy import Engine

from .incidents import list_incidents, summary
from .redact import redact


def incidents_summary(engine: Engine, limit: int = 50) -> dict[str, Any]:
    """{"summary": {...}, "incidents": [...]} - safe to include in a support bundle (strict redaction)."""
    keep = ("id", "kind", "severity", "status", "title", "connection_id", "table", "position", "count", "first_seen",
            "last_seen", "rule_id", "root_cause", "confidence", "confirmed_cause")
    items = [{k: i.get(k) for k in keep} for i in list_incidents(engine, limit=limit)]
    return {"summary": summary(engine), "incidents": redact(items, "strict")}
