"""alelt_dst_delta - Delta Lake destination (delta-rs; txn app id/version idempotent writes)."""
from .destination import DeltaDestination, arrow_type, delta_type

__all__ = ["DeltaDestination", "arrow_type", "delta_type"]
