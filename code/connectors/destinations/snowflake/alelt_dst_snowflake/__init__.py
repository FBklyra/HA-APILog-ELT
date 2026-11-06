"""Snowflake destination connector."""
import importlib.util as _ilu
import sys as _sys
from pathlib import Path as _Path

for _dep, _dir in (("alelt_dst_kit", "_spi"),):                     # repo checkout without PYTHONPATH: shared destination base
    if _ilu.find_spec(_dep) is None:
        _p = str(_Path(__file__).resolve().parents[2] / _dir)
        if _p not in _sys.path:
            _sys.path.append(_p)
from .destination import SnowflakeDestination
from .profile import ALELT, OutputProfile, load_profile

__all__ = ["ALELT", "OutputProfile", "SnowflakeDestination", "load_profile"]
