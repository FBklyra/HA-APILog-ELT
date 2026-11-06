"""alelt_dst_kit - shared helpers + conformance suite for destination plugins (DESIGN.md §6).

Engine plugins under connectors/destinations/<engine>/ build on:
- `plan.plan_batch`      canonical change sets -> one net operation per key per destination table
- `base.KitDestination`  checkpoint/no-op/replay template around an engine-specific atomic commit
- `types.physical_type`  canonical -> physical type from config/type_maps/canonical_to_<engine>.yaml
- `sqlmerge`             MERGE + checkpoint generator for SQL warehouses (staging table or inline literals)
- `conformance`          pytest mix-in every destination test suite runs (replay x3, crash mid-apply, PK update ...)
- `registry`             engine name -> 'module:Class'
"""
from .base import ControlState, KitDestination, default_compare
from .plan import RowOp, TablePlan, plan_batch
from .registry import DESTINATIONS, destination_ref, load_destination
from .types import physical_type, type_map
from .urls import parse_url

__all__ = ["DESTINATIONS", "ControlState", "KitDestination", "RowOp", "TablePlan", "default_compare",
           "destination_ref", "load_destination", "parse_url", "physical_type", "plan_batch", "type_map"]
