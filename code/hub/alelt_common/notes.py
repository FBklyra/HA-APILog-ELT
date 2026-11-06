"""One grammar for `ChangeEvent.note` (XC-36), shared by every source and the hub.

    note    := token (';' token)*
    token   := key [':' value]
    value   := item (',' item)*            # for column lists: `unchanged:a,b` == `unchanged:a;unchanged:b`

Column-list keys (value = column names):
    unchanged         column not in this image; the destination keeps its current value
    absent            column not present in this (older) row image (Db2 ADD COLUMN)
    lob_unavailable   out-of-row LOB / long value not in the log record (re-read by key)
    value_unavailable value form not decodable from the log (DECFLOAT, XML, ...; re-read by key)
    reread            value form not documented for row images (re-read by key)
    precision_lost    canonical value is truncated to microseconds / end of day (docs/TYPE-MAP.md)
    lob_from_writes   LOB value assembled from LOB write operations
Reference keys (single value): row_ref, target_ref (F-11 compensation), undo.
Flags (no value): no_before_image, uncataloged, cascade, partial_txn:start_not_mined (value = reason).
Anything after `alelt-signal:` is a completeness signal (alelt_common.completeness), never parsed here.
"""
from __future__ import annotations

COLUMN_KEYS = frozenset({"unchanged", "absent", "lob_unavailable", "value_unavailable", "reread",
                         "precision_lost", "lob_from_writes"})


def parse(note: str | None) -> dict[str, list[str]]:
    """note -> {key: [values]} (column-list keys split on ',' and merged; order kept, duplicates removed)."""
    out: dict[str, list[str]] = {}
    if not note or note.startswith("alelt-signal:"):
        return out
    for tok in note.split(";"):
        tok = tok.strip()
        if not tok:
            continue
        key, _, val = tok.partition(":")
        key = key.strip()
        vals = [v.strip() for v in val.split(",")] if key in COLUMN_KEYS else ([val.strip()] if val else [])
        cur = out.setdefault(key, [])
        for v in vals:
            if v and v not in cur:
                cur.append(v)
    return out


def columns(note: str | None, key: str) -> list[str]:
    """Columns listed under a column-list key, whichever form the source used."""
    return parse(note).get(key, [])


def format_note(parts: dict[str, list[str]]) -> str | None:
    """Canonical text: column-list keys as one comma list each, in a stable order."""
    toks: list[str] = []
    for k, vs in parts.items():
        if k in COLUMN_KEYS:
            if vs:
                toks.append(f"{k}:{','.join(vs)}")
        elif vs:
            toks.extend(f"{k}:{v}" for v in vs)
        else:
            toks.append(k)
    return ";".join(toks) or None
