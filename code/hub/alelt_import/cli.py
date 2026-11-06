"""`alelt import <command>` - golden-spec importers (read-only). Extension commands come from config/extensions.yaml."""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from alelt_common.models import CatalogSnapshot
from alelt_common.util import load

from . import ddl as ddl_mod
from . import reports
from .readonly import Query, snowflake_from_env

REPO = Path(__file__).resolve().parents[2]


def _yaml_dump(doc: Any) -> str:
    import yaml

    return yaml.safe_dump(doc, sort_keys=True, default_flow_style=False, allow_unicode=True)


def _yaml_load(path: str | Path) -> Any:
    import yaml

    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def cmd_ddl(a: argparse.Namespace, query: Callable[[], Query]) -> int:
    ddls, cols = ddl_mod.export(query(), a.database, a.schema)
    for p in ddl_mod.write(a.out, ddls, cols):
        print(p)
    return 0


def cmd_type_map(a: argparse.Namespace, _: Callable[[], Query]) -> int:
    catalog = CatalogSnapshot.model_validate_json(Path(a.catalog).read_text(encoding="utf-8"))
    golden = ddl_mod.read_columns_csv(Path(a.golden) / "columns.csv")
    doc = reports.type_map(catalog, golden)
    Path(a.golden, "type_map.yaml").write_bytes(_yaml_dump(doc).encode("utf-8"))
    print(f"mapped {len(doc['columns'])} columns, UNRESOLVED {len(doc['UNRESOLVED'])}")
    return 0


def cmd_system_columns(a: argparse.Namespace, _: Callable[[], Query]) -> int:
    from alelt_schema.config import load_profile, profile_files

    names: list[str] = list(a.name or [])
    if a.profile:
        p = load_profile(profile_files(REPO), a.profile)
        names += [p.synced_column, p.deleted_column, p.id_column]
    golden = ddl_mod.read_columns_csv(Path(a.golden) / "columns.csv")
    out = reports.system_columns(golden, names, a.prefix or [])
    Path(a.golden, "system_columns.csv").write_bytes(out.encode("utf-8"))
    sys.stdout.write(out)
    return 0


def cmd_diff(a: argparse.Namespace, _: Callable[[], Query]) -> int:
    changes = reports.diff(_yaml_load(a.committed), _yaml_load(a.current))
    for path, old, new in changes:
        print(f"{path}: {json.dumps(old, default=str)} -> {json.dumps(new, default=str)}")
    return 1 if changes else 0


def cmd_check_golden(a: argparse.Namespace, _: Callable[[], Query]) -> int:
    from alelt_schema.config import load_connection

    cfg = load_connection(a.tables)
    missing = reports.missing_golden([cfg.dest_name(r) for r in cfg.enabled_tables()], Path(a.golden) / "ddl")
    for t in missing:
        print(f"missing golden DDL: {t}")
    return 1 if missing else 0


def build_parser(extensions: bool = True) -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="alelt import", description="Read-only golden-spec importers")
    sub = ap.add_subparsers(dest="command", required=True)
    golden = str(REPO / "config" / "golden")

    p = sub.add_parser("ddl", help="golden DDL + columns.csv from an existing destination schema")
    p.add_argument("--schema", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--out", default=golden)
    p.set_defaults(fn=cmd_ddl)

    p = sub.add_parser("type-map", help="source type -> destination type as observed in golden DDL")
    p.add_argument("--catalog", required=True, help="CatalogSnapshot JSON")
    p.add_argument("--golden", default=golden)
    p.set_defaults(fn=cmd_type_map)

    p = sub.add_parser("system-columns", help="report system columns per table")
    p.add_argument("--profile", help="output profile whose column names to report")
    p.add_argument("--name", action="append", help="exact column name (repeatable)")
    p.add_argument("--prefix", action="append", help="column name prefix (repeatable)")
    p.add_argument("--golden", default=golden)
    p.set_defaults(fn=cmd_system_columns)

    p = sub.add_parser("diff", help="diff a freshly exported config against the committed YAML")
    p.add_argument("--current", required=True)
    p.add_argument("--committed", default=str(REPO / "config" / "tables.yaml"))
    p.set_defaults(fn=cmd_diff)

    p = sub.add_parser("check-golden", help="fail if an enabled table has no golden DDL")
    p.add_argument("--tables", default=str(REPO / "config" / "tables.yaml"))
    p.add_argument("--golden", default=golden)
    p.set_defaults(fn=cmd_check_golden)

    if extensions:
        from alelt_schema.config import load_extensions

        for ext in load_extensions(REPO):
            for _, ref in sorted(ext.import_commands.items()):
                load(ref)(sub)       # register(subparsers) adds parsers with set_defaults(fn=...)
    return ap


def main(argv: Sequence[str] | None = None, query: Callable[[], Query] = snowflake_from_env,
         extensions: bool = True) -> int:
    a = build_parser(extensions).parse_args(argv)
    return int(a.fn(a, query))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
