"""Test path setup: hub/ on sys.path."""
import sys
from pathlib import Path

_HUB = Path(__file__).resolve().parents[2]
if str(_HUB) not in sys.path:
    sys.path.insert(0, str(_HUB))
