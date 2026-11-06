"""alelt_dst_iceberg - Apache Iceberg destination (pyiceberg; checkpoint in table properties + snapshot summary)."""
from .destination import IcebergDestination, iceberg_type

__all__ = ["IcebergDestination", "iceberg_type"]
