"""Redaction: nothing that leaves the host may carry row values, PII or secrets.

Kept: structure, column names, types, error text (scrubbed), positions, offsets, counts, hex *head* of a record.
"""
from __future__ import annotations

import re
from typing import Any

# keys whose values are row data -> replaced by a type descriptor per column
VALUE_KEYS = frozenset({"before", "after", "row", "rows", "values", "value", "key", "keys", "sample", "samples",
                        "data", "drill", "image"})
SECRET_KEYS = re.compile(r"(?i)(pass(word|wd)?|pwd|secret|token|api[_-]?key|authorization|private[_-]?key|"
                         r"credential|cookie|dsn|conn(ection)?_?(string|url)|dest_url|db_url)")
HEX_HEAD = {"strict": 16, "standard": 64}      # bytes of raw record kept

_SECRET_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"-----BEGIN [A-Z ]+-----.*?-----END [A-Z ]+-----", re.S), "<pem>"),
    (re.compile(r"(\b[a-z][a-z0-9+.-]*://)[^\s:/@]+:[^\s@]+@", re.I), r"\1***:***@"),
    (re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+"), "Bearer ***"),
    (re.compile(r"\beyJ[\w-]+\.[\w-]+\.[\w-]+"), "<jwt>"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]{12,}"), "<api-key>"),
    (re.compile(r"\balelt[ea]_[A-Za-z0-9_-]{8,}"), "<agent-token>"),            # enrolment token / agent credential
    (re.compile(r"(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|x-api-key|private[_-]?key)(\s*[=:]\s*)"
                r"(\"[^\"]*\"|'[^']*'|[^\s\"';,&]+)"), r"\1\2***"),
]
_PII_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"[\w.+-]+@[\w-]+(\.[\w-]+)+"), "<email>"),
    (re.compile(r"(?i)(Key \([^)]*\)=)\([^)]*\)"), r"\1(<masked>)"),          # Postgres duplicate-key detail
    (re.compile(r"(?i)(value|row|values)\s*[=:]\s*\([^)]*\)"), r"\1=(<masked>)"),
    (re.compile(r"'[^'\n]{0,500}'"), "'<lit>'"),                                # quoted literals (values)
]


def scrub(text: str | None, level: str = "strict") -> str:
    """Remove secrets always; in strict mode also emails, key values and quoted literals."""
    if not text:
        return ""
    out = str(text)
    for rx, rep in _SECRET_PATTERNS:
        out = rx.sub(rep, out)
    if level == "strict":
        for rx, rep in _PII_PATTERNS:
            out = rx.sub(rep, out)
    return out


def describe(v: Any) -> str:
    """Type descriptor replacing a value: '<str:12>', '<int>', '<null>' ..."""
    if v is None:
        return "<null>"
    if isinstance(v, (str, bytes)):
        return f"<{type(v).__name__}:{len(v)}>"
    return f"<{type(v).__name__}>"


def mask_values(v: Any) -> Any:
    if isinstance(v, dict):
        return {str(k): describe(x) for k, x in v.items()}
    if isinstance(v, list):
        return [mask_values(x) if isinstance(x, (dict, list)) else describe(x) for x in v]
    return describe(v)


def hex_head(raw_hex: str | None, level: str = "strict") -> str | None:
    if not raw_hex:
        return None
    n = HEX_HEAD.get(level, 16) * 2
    return raw_hex[:n] + ("…" if len(raw_hex) > n else "")


def redact(obj: Any, level: str = "strict") -> Any:
    """Deep copy with secrets stripped, row values masked, strings scrubbed."""
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            ks = scrub(str(k), level)           # keys can carry text too (e.g. reason -> count maps)
            if SECRET_KEYS.search(ks) and not isinstance(v, (bool, int, float)) and v is not None:
                out[ks] = "***"
            elif ks in VALUE_KEYS and v is not None:
                out[ks] = mask_values(v)
            elif ks == "raw_hex":
                out[ks] = hex_head(v, level)
            else:
                out[ks] = redact(v, level)
        return out
    if isinstance(obj, (list, tuple)):
        return [redact(x, level) for x in obj]
    if isinstance(obj, str):
        return scrub(obj, level)
    if isinstance(obj, bytes):
        return describe(obj)
    return obj
