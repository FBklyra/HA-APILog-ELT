"""alelt_dst_fabric - Microsoft Fabric Warehouse / Synapse (T-SQL) destination."""
from .destination import FabricDestination, TsqlDialect

__all__ = ["FabricDestination", "TsqlDialect"]
