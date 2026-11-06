"""`python -m alelt_hub.migrate` - create/upgrade the hub metadata schema (idempotent; deploy entrypoint)."""
from __future__ import annotations

import argparse
import sys

from sqlalchemy import Engine, inspect, text
from sqlalchemy.schema import CreateColumn

from alelt_common.util import setup_logging

from .db import get_engine, md
from .dialect import add_column_sql

import alelt_diagnose.tables  # noqa: E402,F401 - registers the diag_* tables on md
import alelt_hub.ha_models  # noqa: E402,F401 - registers capture_lease / hub_node (X-47, X-49)


def missing_columns(eng: Engine) -> list[tuple[str, str]]:
    insp = inspect(eng)
    have = set(insp.get_table_names())
    out = []
    for name, t in md.tables.items():
        if name in have:
            cols = {c["name"] for c in insp.get_columns(name)}
            out += [(name, c.name) for c in t.columns if c.name not in cols]
    return out


def add_missing_columns(eng: Engine) -> int:
    """Additive migration: ALTER TABLE ADD COLUMN for columns declared in db.py but absent in the database.
    New columns must be nullable or carry a constant server default (SQLite rejects utc_now() / UNIQUE here);
    destructive changes need a manual migration."""
    todo = missing_columns(eng)
    with eng.begin() as c:
        for table, col in todo:
            ddl = CreateColumn(md.tables[table].c[col]).compile(dialect=eng.dialect)
            c.execute(text(add_column_sql(eng.dialect, table, str(ddl))))
    return len(todo)


def main() -> int:
    ap = argparse.ArgumentParser(description="hub metadata migrations (forward-only, versioned; deploy/UPGRADE.md)")
    ap.add_argument("--check", action="store_true",
                    help="exit 1 if any migration is pending or the schema is newer than this release; change nothing")
    args = ap.parse_args()
    setup_logging()
    from .migrations import SchemaTooNew, status, upgrade

    eng = get_engine()
    st = status(eng)
    if args.check:
        print(f"metadata schema version {st['current']} (this release: {st['latest']}); pending: {st['pending'] or '-'}; "
              f"missing tables: {st['missing_tables'] or '-'}; missing columns: {st['missing_columns'] or '-'}"
              + (f"; NEWER than this release: {st['too_new']}" if st["too_new"] else ""))
        return 1 if st["pending"] or st["missing_tables"] or st["missing_columns"] or st["too_new"] else 0
    try:
        applied = upgrade(eng, auto=True)
    except SchemaTooNew as e:
        print(f"refused: {e}")
        return 2
    from . import config_store
    from .settings import get_settings

    seeded = config_store.seed_from_files(eng, get_settings().tables_config)
    print(f"metadata schema ready ({eng.dialect.name}, {len(md.tables)} tables, version {st['latest']}; created "
          f"{len(st['missing_tables'])} tables, added {len(st['missing_columns'])} columns, applied steps "
          f"{applied or '-'}; config seeded from files: {[c for c, _ in seeded] or '-'})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
