"""Prerequisite detection: harnesses for components not built yet record SKIP with a reason, never PASS."""
from __future__ import annotations

import importlib
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Optional

from .ladder_result import LadderResult, find_repo_path


def need_path(res: LadderResult, what: str, *parts: str, pattern: str = "*") -> Optional[Path]:
    p = find_repo_path(*parts)
    ok = p.exists() and (p.is_file() or any(p.glob(pattern)))
    if not ok:
        res.skip(what, f"missing {Path(*parts).as_posix()}" + ("" if pattern == "*" else f"/{pattern}"))
        return None
    return p


def need_module(res: LadderResult, what: str, module: str, *src: str) -> Optional[Any]:
    """Import a hub module (adds repo src dirs, e.g. 'hub', to sys.path)."""
    for s in src:
        d = str(find_repo_path(s))
        if d not in sys.path:
            sys.path.insert(0, d)
    try:
        return importlib.import_module(module)
    except Exception as e:  # noqa: BLE001 - any import failure means "not built"
        res.skip(what, f"cannot import {module}: {type(e).__name__}: {e}")
        return None


def need_env(res: LadderResult, what: str, *names: str) -> bool:
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        res.skip(what, "env not set: " + ", ".join(missing))
        return False
    return True


def need_exe(res: LadderResult, what: str, exe: str) -> Optional[str]:
    p = shutil.which(exe)
    if not p:
        res.skip(what, f"{exe} not on PATH")
    return p
