"""Put hub/ and the local connector folders on sys.path."""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[4]
for p in ("hub", "connectors/destinations/postgres", "connectors/destinations/sql", "connectors/destinations/_spi",
          "connectors/sources/db2"):
    if str(_ROOT / p) not in sys.path:
        sys.path.insert(0, str(_ROOT / p))
