"""Golden spec (R8) from an existing landing schema, offline: parse uploaded DDL (GET_DDL / pg_dump / hand-written)
-> alelt_import.ddl GoldenColumn rows + per-table DDL; contract check of enabled tables against it."""
from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from .ddl import GoldenColumn, columns_csv, normalize_ddl

_CREATE = re.compile(r"create\s+(?:or\s+replace\s+)?(?:transient\s+|temporary\s+)?table\s+(?:if\s+not\s+exists\s+)?"
                     r"((?:\"[^\"]+\"|[\w$]+)(?:\s*\.\s*(?:\"[^\"]+\"|[\w$]+)){0,2})\s*\(", re.I)
_TYPE = re.compile(r"^(.*?)(?:\s+(?:NOT|NULL|DEFAULT|COLLATE|PRIMARY|UNIQUE|REFERENCES|CHECK|CONSTRAINT|COMMENT|"
                   r"IDENTITY|AUTOINCREMENT|GENERATED|WITH|MASKING)\b.*)?$", re.I | re.S)
_CONSTRAINT = re.compile(r"^(constraint|primary\s+key|unique|foreign\s+key|check)\b", re.I)


def _unquote(x: str) -> str:
    x = x.strip()
    return x[1:-1] if x.startswith('"') and x.endswith('"') else x.upper()


def _split_top(body: str) -> list[str]:
    out, depth, cur, q = [], 0, [], False
    for ch in body:
        if ch == '"':
            q = not q
        if not q:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                out.append("".join(cur))
                cur = []
                continue
        cur.append(ch)
    if "".join(cur).strip():
        out.append("".join(cur))
    return out


def parse_ddl(text: str) -> tuple[dict[str, str], list[GoldenColumn]]:
    """CREATE TABLE statements (GET_DDL output, pg_dump, hand-written) -> ({TABLE: ddl}, columns)."""
    ddls: dict[str, str] = {}
    cols: list[GoldenColumn] = []
    for m in _CREATE.finditer(text):
        name = _unquote(re.split(r"\s*\.\s*", m.group(1))[-1])
        i, depth = m.end(), 1
        while i < len(text) and depth:
            depth += {"(": 1, ")": -1}.get(text[i], 0)
            i += 1
        body = text[m.end():i - 1]
        end = text.find(";", i)
        ddls[name] = normalize_ddl(text[m.start():(end if end >= 0 else i)])
        n = 0
        for part in _split_top(body):
            p = " ".join(part.split())
            if not p or _CONSTRAINT.match(p):
                continue
            cm = re.match(r'^("(?:[^"]+)"|[\w$]+)\s+(.+)$', p)
            if not cm:
                continue
            rest = cm.group(2)
            tm = _TYPE.match(rest)
            dtype = re.sub(r"\s+", " ", (tm.group(1) if tm else rest.split()[0])).upper().strip()
            n += 1
            cols.append(GoldenColumn(name, _unquote(cm.group(1)), n, dtype, "NOT NULL" not in rest.upper()))
    return ddls, cols


def to_json(ddls: dict[str, str], cols: list[GoldenColumn]) -> dict[str, Any]:
    return {"tables": sorted(ddls), "ddl": dict(sorted(ddls.items())), "columns_csv": columns_csv(cols),
            "columns": [{"table": c.table, "column": c.column, "ordinal": c.ordinal, "data_type": c.data_type,
                         "is_nullable": c.is_nullable} for c in cols]}


def contract_check(golden_tables: Iterable[str], enabled_dest_tables: Iterable[str]) -> dict[str, list[str]]:
    """Enabled destination tables vs golden spec: which are covered, which lack golden DDL (R8)."""
    have = {t.upper() for t in golden_tables}
    want = sorted({t.upper() for t in enabled_dest_tables})
    return {"covered": [t for t in want if t in have], "missing": [t for t in want if t not in have]}
