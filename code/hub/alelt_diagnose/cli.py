"""`alelt diagnose <command>` (also `python -m alelt_diagnose`).

  export-patch <incident> [--out DIR] [--with-raw]   write reviewed code-fix patches + regression case
  scan                                               run the trigger sweep once
  show <incident>                                    print the incident (JSON)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="alelt diagnose", description="ALELT self-diagnosis")
    sub = ap.add_subparsers(dest="cmd", required=True)
    ex = sub.add_parser("export-patch", help="export REVIEWED code-fix proposals for the dev team's PR process")
    ex.add_argument("incident")
    ex.add_argument("--out", default=".", help="output directory (default: current)")
    ex.add_argument("--with-raw", action="store_true",
                    help="include raw record bytes from the hub raw archive in the regression case (hub host only)")
    sub.add_parser("scan", help="run the incident trigger sweep once")
    sh = sub.add_parser("show", help="print an incident as JSON")
    sh.add_argument("incident")
    args = ap.parse_args(argv)

    from alelt_hub.db import get_engine, init_db

    from . import tables  # noqa: F401 - register tables
    from . import code_fix, hooks, incidents

    eng = get_engine()
    init_db(eng)
    if args.cmd == "export-patch":
        files = code_fix.export(eng, args.incident, Path(args.out), args.with_raw)
        if not files:
            print("no REVIEWED code fix for this incident (review it in the console first)", file=sys.stderr)
            return 1
        for f in files:
            print(f)
        return 0
    if args.cmd == "scan":
        print(json.dumps({"triggers": hooks.scan(eng)}))
        return 0
    print(json.dumps(incidents.get_incident(eng, args.incident), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
