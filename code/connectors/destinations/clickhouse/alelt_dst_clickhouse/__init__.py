"""alelt_dst_clickhouse - ClickHouse destination (ReplacingMergeTree + checkpoint table, stage-then-commit)."""
from .destination import ClickHouseDestination, ch_type

__all__ = ["ClickHouseDestination", "ch_type"]
