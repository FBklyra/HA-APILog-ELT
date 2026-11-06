"""Dev edge protocol tolerance (XC-15 / XC-17 / LNX-26): limits typed + bounded like the C agent, START_CAPTURE {}
= resume (no KeyError)."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE.parents[4] / "hub"))

import devedge  # noqa: E402


class Limits(unittest.TestCase):
    def test_set_limits_bounded_and_typed(self) -> None:
        out = devedge.clamp_limits({"cpu_pct": 50, "batch_kb": 256}, {"cpu_pct": 0, "batch_kb": 10 ** 9,
                                                                       "poll_ms": "fast", "evil": 1}, None)
        self.assertEqual(out, {"cpu_pct": 1, "batch_kb": 65536})

    def test_register_never_raises_above_local_config(self) -> None:
        local = {"cpu_pct": 50, "batch_kb": 256, "poll_ms": 1000, "max_rate_kbps": 0}
        out = devedge.clamp_limits(local, {"cpu_pct": 100, "batch_kb": 1024, "poll_ms": 5000}, local)
        self.assertEqual(out["cpu_pct"], 50)
        self.assertEqual(out["batch_kb"], 256)
        self.assertEqual(out["poll_ms"], 5000)                      # slower polling is always allowed


class Resume(unittest.TestCase):
    def test_start_capture_without_position_is_resume(self) -> None:
        src = (HERE.parent / "devedge.py").read_text(encoding="utf-8")
        self.assertNotIn('args["from_pos"]', src)                 # no KeyError path left


if __name__ == "__main__":
    unittest.main()
