from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[4]
for p in ("hub", "connectors/sources/postgres"):
    if str(_ROOT / p) not in sys.path:
        sys.path.insert(0, str(_ROOT / p))

GOLDEN = _ROOT / "tests/golden/postgres"
