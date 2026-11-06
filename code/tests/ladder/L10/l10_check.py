"""L10 check: parse l10_checklist.md; green only if L9 is PASS, every box is ticked with evidence, and both
sign-off rows are filled.

  python l10_check.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _common.ladder_result import LadderResult, require_green  # noqa: E402

HERE = Path(__file__).resolve().parent
ROW = re.compile(r"^\|\s*([A-C]\d+)\s*\|(.+?)\|\s*\[( |x|X)\]\s*\|(.*)\|\s*$")
SIGN = re.compile(r"^\|\s*(Data team lead|IT platform lead)\s*\|(.*)\|(.*)\|(.*)\|\s*$")


def main() -> None:
    res = LadderResult("L10", "legacy pipeline decommission checklist", environment="prod")
    gate = require_green("L9", res)
    md = HERE / "l10_checklist.md"
    lines = md.read_text(encoding="utf-8").splitlines()
    rows = [m for m in (ROW.match(ln) for ln in lines) if m]
    res.check("checklist parsed", len(rows) >= 17, f"{len(rows)} item(s)")
    for m in rows:
        item, text, box, evidence = m.group(1), m.group(2).strip(), m.group(3), m.group(4).strip()
        done = box.lower() == "x"
        if not gate and not done:
            res.skip(f"{item} {text[:60]}", "open")
            continue
        res.check(f"{item} {text[:60]}", done and bool(evidence), evidence or "no evidence")
    for m in (SIGN.match(ln) for ln in lines):
        if m:
            filled = all(x.strip() for x in m.groups()[1:])
            if not gate and not filled:
                res.skip(f"sign-off: {m.group(1)}", "open")
            else:
                res.check(f"sign-off: {m.group(1)}", filled)
    res.artifact(md)
    res.finish()


if __name__ == "__main__":
    main()
