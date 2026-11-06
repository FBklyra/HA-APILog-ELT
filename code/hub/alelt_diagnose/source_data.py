"""Source-data diagnosis: the root cause is in the values the application wrote, not in ALELT.

- `check_events()`   bounded value checks on decoded change events (future timestamps, NULL in NOT NULL, values out
                     of the declared length/precision); raw samples are kept on the hub only (incident.reveal).
- `rules`            error-text signatures (invalid encoding/dates, NULL violations, duplicates in keyless tables,
                     orphan keys) returning SOURCE_DATA findings.
- `dba_sql()`        copy-paste read-only SELECT for the DBA to list every affected row. Shown, never executed (R1).
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from .models import Classification, EvidencePack, Finding, IncidentKind, SourceDataIssue, SuggestedAction

FUTURE_SLACK = timedelta(days=1)
_FORBIDDEN = re.compile(r"\b(insert|update|delete|merge|drop|alter|create|truncate|call|grant|revoke|exec(ute)?|"
                        r"declare|set|lock|rename|comment|load|import|export|reorg|runstats|attach|detach)\b", re.I)


# ------------------------------------------------------------------ read-only SQL for the DBA
def ident(name: str) -> str:
    """Db2 delimited identifier (double quotes doubled)."""
    return '"' + name.replace('"', '""') + '"'


def is_select_only(sql: str) -> bool:
    """True iff `sql` is a single SELECT / WITH ... SELECT statement with no DML/DDL keyword (outside literals)."""
    body = re.sub(r"'(?:[^']|'')*'", "''", sql)           # string literals cannot hide keywords
    body = re.sub(r'"(?:[^"]|"")*"', '""', body)          # nor delimited identifiers
    body = re.sub(r"--[^\n]*", "", body).strip().rstrip(";").strip()
    if ";" in body or not re.match(r"(?is)^(select|with)\b", body):
        return False
    return _FORBIDDEN.search(body) is None


def dba_sql(issue: str, table: str | None, column: str | None, key_columns: list[str],
            layout: dict[str, Any] | None = None) -> str:
    """Read-only Db2 query listing the affected rows (FETCH FIRST + WITH UR: no locks on the source)."""
    if not table or "." not in table:
        return ""
    s, _, t = table.partition(".")
    tab = f"{ident(s)}.{ident(t)}"
    col = ident(column) if column else None
    keys = [ident(k) for k in key_columns]
    cols = {c.get("name"): c for c in (layout or {}).get("columns", [])}
    meta = cols.get(column or "", {})
    if col is None and issue in ("INVALID_DATE", "FUTURE_TIMESTAMP"):
        dcols = [n for n, c in cols.items() if str(c.get("canonical_type")) in ("LOCALDATE", "LOCALDATETIME", "INSTANT")]
        if not dcols:
            return ""
        parts = [(f"YEAR({ident(n)}) < 1900 OR YEAR({ident(n)}) > 2100" if issue == "INVALID_DATE"
                  else f"{ident(n)} > CURRENT TIMESTAMP + 1 DAY") for n in dcols]
        sel = ", ".join(keys + [ident(n) for n in dcols])
        sql = f"SELECT {sel} FROM {tab} WHERE {' OR '.join(parts)} FETCH FIRST 1000 ROWS ONLY WITH UR"
        return sql if is_select_only(sql) else ""
    sel = ", ".join(keys + ([col] if col else [])) or "*"
    where: str
    if issue == "FUTURE_TIMESTAMP" and col:
        where = f"{col} > CURRENT TIMESTAMP + 1 DAY" if "TIMESTAMP" in str(meta.get("engine_type", "TIMESTAMP")).upper() \
            else f"{col} > CURRENT DATE + 1 DAY"
    elif issue == "INVALID_DATE" and col:
        where = f"YEAR({col}) < 1900 OR YEAR({col}) > 2100"
    elif issue == "NULL_IN_NOT_NULL" and col:
        where = f"{col} IS NULL"
    elif issue == "OUT_OF_RANGE" and col:
        ln, sc = meta.get("length"), meta.get("scale")
        if str(meta.get("canonical_type")) == "DECIMAL" and ln:
            where = f"ABS({col}) >= 1E{int(ln) - int(sc or 0)}"
        else:
            where = f"LENGTH({col}) > {int(ln or 0)}"
    elif issue == "INVALID_ENCODING" and col:
        sel = ", ".join(keys + [f"HEX({col}) AS {ident((column or '') + '_HEX')}"])
        sql = (f"-- inspect {column}_HEX for byte sequences that are invalid in the column CCSID\n"
               f"SELECT {sel} FROM {tab} WHERE {col} IS NOT NULL ORDER BY {', '.join(keys) or col} "
               "FETCH FIRST 1000 ROWS ONLY WITH UR")
        return sql if is_select_only(sql) else ""
    elif issue == "DUPLICATE_KEYLESS":
        allc = ", ".join(ident(str(n)) for n in cols) or "*"
        sql = (f"SELECT {allc}, COUNT(*) AS DUPLICATES FROM {tab} GROUP BY {allc} HAVING COUNT(*) > 1 "
               "FETCH FIRST 1000 ROWS ONLY WITH UR")
        return sql if is_select_only(sql) else ""
    elif issue == "ORPHAN_KEY" and col:
        sql = (f"-- replace PARENT_SCHEMA.PARENT_TABLE / PARENT_KEY with the referenced table\n"
               f"SELECT {sel} FROM {tab} c WHERE c.{col} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM "
               f"\"PARENT_SCHEMA\".\"PARENT_TABLE\" p WHERE p.\"PARENT_KEY\" = c.{col}) FETCH FIRST 1000 ROWS ONLY WITH UR")
        return sql if is_select_only(sql) else ""
    else:
        return ""
    sql = f"SELECT {sel} FROM {tab} WHERE {where} FETCH FIRST 1000 ROWS ONLY WITH UR"
    return sql if is_select_only(sql) else ""


# ------------------------------------------------------------------ value checks on decoded events
def _aware(v: datetime) -> datetime:
    return v if v.tzinfo else v.replace(tzinfo=timezone.utc)


def check_events(table: Any, events: list[Any], now: datetime | None = None, limit: int = 200) -> list[dict[str, Any]]:
    """Return issues found in the first `limit` row images of `table` (alelt_common TableInfo).

    Each issue: {issue, column, position, key (raw), value (raw)} - raw parts stay on the hub (sensitive table)."""
    now = now or datetime.now(timezone.utc)
    cols = {c.name: c for c in table.columns}
    keys = [c.name for c in table.key_columns()]
    out: list[dict[str, Any]] = []
    for e in events[:limit]:
        img = e.after or {}
        if not img:
            continue
        key = {k: img.get(k) for k in keys}
        pos = e.position.text if e.position else None
        for name, v in img.items():
            c = cols.get(name)
            if c is None:
                continue
            issue = None
            if v is None and c.nullable is False and not c.key_seq:
                issue = "NULL_IN_NOT_NULL"
            elif isinstance(v, datetime) and _aware(v) > now + FUTURE_SLACK:
                issue = "FUTURE_TIMESTAMP"
            elif isinstance(v, date) and not isinstance(v, datetime) and v > (now + FUTURE_SLACK).date() \
                    and "BIRTH" not in name.upper():
                issue = "FUTURE_TIMESTAMP"
            elif isinstance(v, (datetime, date)) and v.year < 1900:
                issue = "INVALID_DATE"
            elif isinstance(v, str) and c.length and len(v) > c.length:
                issue = "OUT_OF_RANGE"
            elif isinstance(v, Decimal) and c.length and v.is_finite() and \
                    abs(v) >= Decimal(10) ** (c.length - (c.scale or 0)):
                issue = "OUT_OF_RANGE"
            if issue:
                out.append({"issue": issue, "column": name, "position": pos, "key": key, "value": v})
    return out


EXPLAIN = {
    "INVALID_ENCODING": "Some text in column {col} contains bytes that are not valid characters in the column's "
                        "encoding. The application (or an import) wrote them; ALELT kept the records aside.",
    "INVALID_DATE": "Column {col} contains dates outside any plausible range (e.g. year 0001 or 9999 used as "
                    "'unknown'). The destination may reject or misinterpret them.",
    "FUTURE_TIMESTAMP": "Column {col} contains dates/times in the future. This usually comes from a wrong clock or "
                        "a placeholder value in the application.",
    "NULL_IN_NOT_NULL": "Column {col} is declared mandatory downstream but the source rows have no value (NULL).",
    "OUT_OF_RANGE": "Values in column {col} are longer / larger than the declared column size.",
    "DUPLICATE_KEYLESS": "The table has no primary key and contains identical rows; they cannot be told apart.",
    "ORPHAN_KEY": "Rows in column {col} refer to parent records that do not exist (orphan keys).",
}
RECOMMENDED = {"INVALID_ENCODING": "QUARANTINE_SKIP", "INVALID_DATE": "COERCE", "FUTURE_TIMESTAMP": "ACCEPT_AS_IS",
               "NULL_IN_NOT_NULL": "COERCE", "OUT_OF_RANGE": "QUARANTINE_SKIP", "DUPLICATE_KEYLESS": "ACCEPT_AS_IS",
               "ORPHAN_KEY": "ACCEPT_AS_IS"}


def issue_for(issue: str, table: str | None, column: str | None, key_columns: list[str], positions: list[str],
              count: int, layout: dict[str, Any] | None) -> SourceDataIssue:
    return SourceDataIssue(issue=issue, table=table, column=column, key_columns=key_columns,  # type: ignore[arg-type]
                           positions=positions[:20], count=count,
                           explanation=EXPLAIN[issue].format(col=column or "?"), recommended=RECOMMENDED[issue],
                           dba_sql=dba_sql(issue, table, column, key_columns, layout))


# ------------------------------------------------------------------ rules (SOURCE_DATA findings)
def _layout(p: EvidencePack) -> dict[str, Any]:
    lays = p.catalog.get("table_layouts", {})
    return lays[max(lays, key=int)] if lays else {}


def _keys(lay: dict[str, Any]) -> list[str]:
    return [c["name"] for c in sorted((c for c in lay.get("columns", []) if c.get("key_seq")),
                                      key=lambda c: c["key_seq"])]


def _col(text: str, lay: dict[str, Any]) -> str | None:
    m = re.search(r'column "?([A-Za-z0-9_$#@]+)"?', text)
    if m:
        return m.group(1).upper()
    m = re.search(r"(?:bad_codepage|unsupported_type|bad_\w+):\s*([A-Za-z0-9_$#@]+)", text, re.I)
    if m:
        return m.group(1).upper()
    return None


_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("INVALID_ENCODING", ("codec can't decode", "invalid continuation byte", "invalid start byte",
                          "invalid byte sequence", "character with byte sequence")),
    ("INVALID_DATE", ("date/time field value out of range", "invalid date", "day is out of range", "month must be",
                      "year 0 is out of range", "year is out of range", "datetime field overflow", "bad_date")),
    ("NULL_IN_NOT_NULL", ("violates not-null constraint", "null value in column", "cannot be null",
                          "notnullviolation")),
    ("ORPHAN_KEY", ("orphan", "violates foreign key constraint", "foreignkeyviolation")),
    ("FUTURE_TIMESTAMP", ("timestamp in the future", "future_timestamp", "date in the future")),
]


def source_finding(p: EvidencePack) -> Finding | None:
    """One SOURCE_DATA finding from the evidence (trigger detail from check_events, or error signatures)."""
    t = p.text()
    lay = _layout(p)
    det = p.trigger.detail.get("source_issue") if p.trigger.kind == IncidentKind.SOURCE_DATA else None
    issue = col = None
    keys = _keys(lay)
    if isinstance(det, dict):
        issue, col = det.get("issue"), det.get("column")
        keys = list(det.get("key_columns") or keys)
        if det.get("column_meta") and col:
            lay = {"columns": [*lay.get("columns", []), {"name": col, **det["column_meta"]}]}
    else:
        for name, needles in _PATTERNS:
            if any(n in t for n in needles):
                issue = name
                break
        if issue is None and ("duplicate key" in t or "unique constraint" in t) and lay and not _keys(lay):
            issue = "DUPLICATE_KEYLESS"
        if issue is None:
            return None
        col = _col(t, lay)
    positions = [str(x) for x in (p.trigger.detail.get("positions") or [])] or \
        [q["position"] for q in p.quarantine if q.get("position")] or ([p.trigger.position] if p.trigger.position else [])
    count = int(p.trigger.detail.get("count") or len(p.quarantine) or 1)
    sd = issue_for(issue, p.trigger.table_fqn, col, keys, positions, count, lay)
    return Finding(
        rule_id=f"source_{issue.lower()}", title=f"Source data: {issue.replace('_', ' ').lower()}"
        + (f" in {col}" if col else ""),
        root_cause=f"The source rows contain {issue.replace('_', ' ').lower()} values"
                   + (f" in column {col}" if col else "") + f" of {p.trigger.table_fqn or 'the table'}; this is a data "
                   "quality issue in the application data, not a ALELT defect.",
        explanation=sd.explanation, confidence=0.9 if isinstance(det, dict) else 0.8,
        category=Classification.SOURCE_DATA,
        evidence_refs=[f"incident.kind={p.trigger.kind.value}", *(f"position:{x}" for x in positions[:5])],
        fix_steps=["Send the explanation and the read-only query to the data owner / DBA (query is never run by "
                   "ALELT).", f"Choose a handling option ({', '.join(sd.handling_options)}); recommended: "
                   f"{sd.recommended}.", "Correct the rows in the application if they are wrong; corrected rows "
                   "replicate normally."],
        action=SuggestedAction(kind="SOURCE_DATA_POLICY", params={"table": sd.table, "column": sd.column,
                                                                  "issue": sd.issue, "option": sd.recommended},
                               description=f"Record handling '{sd.recommended}' for {sd.issue} on "
                                           f"{sd.table}.{sd.column or '*'} (table policy, audited)."),
        source_data=sd)
