"""schema_change_handling policy: what to do with each drift event (pure)."""
from __future__ import annotations

from enum import Enum

from alelt_common.models import CanonicalType

from .config import Policy
from .drift import DriftEvent, DriftKind


class Action(str, Enum):
    APPLY = "APPLY"        # applied automatically through the destination SPI
    PENDING = "PENDING"    # pending approval in console (DataOperator)
    IGNORE = "IGNORE"


def decide(ev: DriftEvent, policy: Policy, table_enabled: bool) -> Action:
    if ev.kind == DriftKind.TABLE_ADDED:
        return Action.APPLY if policy == Policy.ALLOW_ALL else Action.PENDING
    if not table_enabled:
        return Action.IGNORE
    if ev.kind == DriftKind.CAPTURE_ENABLED:
        return Action.IGNORE
    if ev.kind == DriftKind.COLUMN_ADDED:
        if ev.new is None or ev.new.get("canonical_type") == CanonicalType.UNKNOWN.value:
            return Action.PENDING          # unmapped type (R7)
        return Action.PENDING if policy == Policy.BLOCK_ALL else Action.APPLY
    return Action.PENDING
