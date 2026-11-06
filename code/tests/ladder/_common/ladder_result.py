"""Ladder result writer (stdlib only, Python 3.10+).

Every harness writes ``tests/ladder/<Lx>/result.json`` (schema_version 1).
The console reads it; a level is green only when ``status == "PASS"``.
"""
from __future__ import annotations

import json
import os
import socket
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

LADDER_DIR = Path(__file__).resolve().parent.parent
STATUSES = ("PASS", "FAIL", "SKIP")

EXIT_CRITERIA = {
    "L0": "24 h heartbeat, clean restart/stop, no listening ports",
    "L1": "LRI and catalog received by hub",
    "L2": "Every block received, sha256 OK, ACK/spool cleanup OK",
    "L3": "Byte-exact vs SELECT; rollbacks never emitted",
    "L4": "Contract test vs golden DDL; replay is idempotent",
    "L5": "100 % decode match; fuzzing 1 h no crash",
    "L6": "No gaps, no duplicates, auto-recovery, CPU cap respected",
    "L7": "7 days parity vs previous pipeline, zero impact alerts",
    "L8": "Per-table certification (DESIGN.md §8.3)",
    "L9": "dbt green on ALELT; rollback < 5 min",
    "L10": "All tables certified + 30 days stable; legacy pipeline decommissioned",
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class LadderResult:
    """Collects checks and writes result.json. Overall status: FAIL if any FAIL, PASS if >=1 PASS and
    no FAIL/SKIP, else SKIP (nothing proven). A level with any SKIP is never green."""

    def __init__(self, level: str, name: str, environment: str = "dev") -> None:
        self.level = level
        self.name = name
        self.environment = environment
        self.started_at = _now()
        self.checks: list[dict[str, str]] = []
        self.artifacts: list[str] = []

    def check(self, name: str, ok: Optional[bool], detail: str = "") -> bool:
        """ok=True PASS, False FAIL, None SKIP. Returns bool(ok)."""
        status = "SKIP" if ok is None else ("PASS" if ok else "FAIL")
        self.checks.append({"name": name, "status": status, "detail": detail})
        print(f"  [{status}] {name}" + (f" - {detail}" if detail else ""), flush=True)
        return bool(ok)

    def skip(self, name: str, reason: str) -> None:
        self.check(name, None, reason)

    def artifact(self, path: Path | str) -> None:
        p = Path(path).resolve()
        try:
            self.artifacts.append(p.relative_to(LADDER_DIR.parent.parent).as_posix())
        except ValueError:
            self.artifacts.append(str(p))

    @property
    def status(self) -> str:
        states = {c["status"] for c in self.checks}
        if "FAIL" in states:
            return "FAIL"
        if states == {"PASS"}:
            return "PASS"
        return "SKIP"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "level": self.level,
            "name": self.name,
            "status": self.status,
            "environment": self.environment,
            "started_at": self.started_at,
            "finished_at": _now(),
            "host": socket.gethostname(),
            "checks": self.checks,
            "artifacts": self.artifacts,
            "exit_criterion": EXIT_CRITERIA[self.level],
        }

    def write(self, out: Optional[Path] = None, part: Optional[str] = None) -> Path:
        """part='link' writes result.link.json; merge_parts() then combines parts into result.json."""
        name = f"result.{part}.json" if part else "result.json"
        out = out or LADDER_DIR / self.level / name
        tmp = out.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.to_dict(), indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, out)
        print(f"{self.level} {self.status} -> {out}")
        return out

    def finish(self, part: Optional[str] = None) -> None:
        """Write result(.part).json and exit 0 on PASS, 2 on SKIP, 1 on FAIL."""
        self.write(part=part)
        sys.exit({"PASS": 0, "SKIP": 2}.get(self.status, 1))


def _cli() -> None:
    # python ladder_result.py merge L0 service link
    if len(sys.argv) >= 4 and sys.argv[1] == "merge":
        merge_parts(sys.argv[2], tuple(sys.argv[3:]))
    else:
        sys.exit("usage: ladder_result.py merge <level> <part>...")


def read_result(level: str) -> Optional[dict[str, object]]:
    p = LADDER_DIR / level / "result.json"
    if not p.exists():
        return None
    data: dict[str, object] = json.loads(p.read_text(encoding="utf-8"))
    return data


def require_green(level: str, res: LadderResult) -> bool:
    """Gate (prompt 12): L7+ production steps refuse to run unless the previous level is PASS.
    A closed gate is recorded as SKIP (blocked), never as PASS."""
    prev = read_result(level)
    status = None if prev is None else prev.get("status")
    if status == "PASS":
        res.check(f"gate: {level} green", True, f"{level} finished_at={prev.get('finished_at') if prev else ''}")
        return True
    res.skip(f"gate: {level} green", f"blocked: {level} " + ("result.json missing" if prev is None
                                                               else f"status={status}"))
    return False


def merge_parts(level: str, required: tuple[str, ...]) -> Path:
    """Combine result.<part>.json files (e.g. L0 service + link) into result.json. Missing part => SKIP check."""
    res = LadderResult(level, "")
    names = []
    for part in required:
        p = LADDER_DIR / level / f"result.{part}.json"
        if not p.exists():
            res.skip(f"{part}: harness", f"result.{part}.json missing (not run)")
            continue
        d = json.loads(p.read_text(encoding="utf-8"))
        names.append(str(d.get("name", part)))
        res.environment = str(d.get("environment", res.environment))
        res.started_at = min(res.started_at, str(d.get("started_at", res.started_at)))
        for c in d.get("checks", []):
            res.checks.append({**c, "name": f"{part}: {c['name']}"})
        res.artifacts.append(p.relative_to(LADDER_DIR.parent.parent).as_posix())
    res.name = " + ".join(names) or level
    return res.write()


def find_repo_path(*parts: str) -> Path:
    return LADDER_DIR.parent.parent.joinpath(*parts)


if __name__ == "__main__":
    _cli()
