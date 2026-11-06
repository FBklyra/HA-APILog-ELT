# SPDX-License-Identifier: Apache-2.0
"""Image choice for the lab (stdlib only; used by lab.sh and the lab helper; lab.ps1 has the same rules).

Per compose override variable (catalog `local_fallbacks`: {var, image, tags}):
  1. a value already in the environment or in lab/.env.local (gitignored)  -> used as-is
  2. the pinned `image` if present locally
  3. else the first locally present compatible tag of `tags` (printed)
  4. else the pinned image (it is pulled)
  python lab/images.py env <profile ids...>    # prints VAR=image lines + notes on stderr
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
ENV_LOCAL = HERE / ".env.local"


def read_env_local(path: Path = ENV_LOCAL) -> dict[str, str]:
    out: dict[str, str] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def present(image: str) -> bool:
    try:
        return subprocess.run(["docker", "image", "inspect", image], capture_output=True, timeout=30).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def resolve(profiles: list[dict[str, Any]], env: dict[str, str] | None = None,
            is_present: Any = present) -> tuple[dict[str, str], list[str], list[str]]:
    """-> (VAR -> image to use, notes, images that must be pulled)."""
    local = read_env_local()
    env = {**local, **(os.environ if env is None else env)}
    # every lab/.env.local entry (LAB_PORT_*, LAB_PLATFORM_*, ...) is passed to compose; process env wins
    chosen: dict[str, str] = {k: env[k] for k in local}
    notes: list[str] = []
    pull: list[str] = []
    done: set[str] = set()
    for p in profiles:
        for fb in p.get("local_fallbacks", []):
            var, pinned = fb["var"], fb["image"]
            if var in done:
                continue
            done.add(var)
            if env.get(var):
                chosen[var] = env[var]
                if env[var] != pinned:
                    notes.append(f"{var}={env[var]} (override)")
                if not is_present(env[var]):
                    pull.append(env[var])
                continue
            if is_present(pinned):
                chosen[var] = pinned
                continue
            alt = next((t for t in fb.get("tags", []) if is_present(t)), None)
            if alt:
                chosen[var] = alt
                notes.append(f"{pinned} not present locally - using local {alt}")
            else:
                chosen[var] = pinned
                pull.append(pinned)
    return chosen, notes, pull


def main(argv: list[str]) -> int:
    if len(argv) < 2 or argv[1] not in ("env", "pull"):
        print(__doc__, file=sys.stderr)
        return 2
    cat = json.loads((HERE / "catalog.json").read_text(encoding="utf-8"))
    byid = {p["id"]: p for p in cat["profiles"]}
    sel = [byid[i] for i in argv[2:] if i in byid]
    chosen, notes, pull = resolve(sel)
    for n in notes:
        print(f"[lab] image: {n}", file=sys.stderr)
    if argv[1] == "env":
        for k, v in chosen.items():
            print(f"{k}={v}")
    else:
        for im in pull:
            print(im)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
