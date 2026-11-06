"""Put hub/, the destination kit and this plugin on sys.path."""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[4]
_HERE = Path(__file__).resolve().parents[1]
for p in (_ROOT / "hub", _ROOT / "connectors/destinations/_spi", _HERE):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))
