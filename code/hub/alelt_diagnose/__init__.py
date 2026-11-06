"""alelt_diagnose - self root-cause diagnosis for runtime errors (read-only; actions need human approval).

Flow: trigger (hook / scan / gateway middleware) -> incident (dedup by signature) -> evidence pack (redacted)
-> Stage A signature rules (always on) -> optional Stage B LLM (off by default) -> proposed actions (approval).
"""
from __future__ import annotations

__all__ = ["__version__"]
__version__ = "0.1.0"
