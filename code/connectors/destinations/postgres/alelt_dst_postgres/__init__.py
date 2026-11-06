"""alelt_dst_postgres - local test destination (PostgreSQL) implementing DestinationConnector."""
import importlib.util as _ilu
import sys as _sys
from pathlib import Path as _Path

for _dep, _dir in (("alelt_dst_kit", "_spi"), ("alelt_dst_sql", "sql")):                     # repo checkout without PYTHONPATH: shared destination base
    if _ilu.find_spec(_dep) is None:
        _p = str(_Path(__file__).resolve().parents[2] / _dir)
        if _p not in _sys.path:
            _sys.path.append(_p)
from .connector import PostgresDestination, default_compare, pg_type

__all__ = ["PostgresDestination", "default_compare", "pg_type"]
