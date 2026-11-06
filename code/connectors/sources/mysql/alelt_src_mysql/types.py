"""MySQL / MariaDB column types: binlog type codes and catalog type -> canonical type (stdlib only).

Names returned are alelt_common.models.CanonicalType values (str, so this module has no pydantic dependency).
"""
from __future__ import annotations

import re

# binlog / protocol field type codes (enum_field_types, public client/server protocol documentation)
DECIMAL = 0
TINY = 1
SHORT = 2
LONG = 3
FLOAT = 4
DOUBLE = 5
NULL = 6
TIMESTAMP = 7
LONGLONG = 8
INT24 = 9
DATE = 10
TIME = 11
DATETIME = 12
YEAR = 13
NEWDATE = 14
VARCHAR = 15
BIT = 16
TIMESTAMP2 = 17
DATETIME2 = 18
TIME2 = 19
TYPED_ARRAY = 20
VECTOR = 242
JSON = 245
NEWDECIMAL = 246
ENUM = 247
SET = 248
TINY_BLOB = 249
MEDIUM_BLOB = 250
LONG_BLOB = 251
BLOB = 252
VAR_STRING = 253
STRING = 254
GEOMETRY = 255

TYPE_NAMES = {v: k for k, v in globals().items() if k.isupper() and isinstance(v, int)}

INTEGER_CODES = frozenset({TINY, SHORT, INT24, LONG, LONGLONG})
NUMERIC_CODES = frozenset({TINY, SHORT, INT24, LONG, LONGLONG, FLOAT, DOUBLE, NEWDECIMAL, DECIMAL})

# catalog DATA_TYPE (information_schema.COLUMNS) -> canonical, signed variant
_MAP: dict[str, str] = {
    "tinyint": "SHORT", "smallint": "SHORT", "mediumint": "INT", "int": "INT", "integer": "INT", "bigint": "LONG",
    "decimal": "DECIMAL", "numeric": "DECIMAL", "float": "FLOAT", "double": "DOUBLE", "real": "DOUBLE",
    "double precision": "DOUBLE",
    "bit": "BINARY",
    "char": "STRING", "varchar": "STRING", "tinytext": "STRING", "text": "STRING", "mediumtext": "STRING",
    "longtext": "STRING", "enum": "STRING", "set": "STRING",
    "binary": "BINARY", "varbinary": "BINARY", "tinyblob": "BINARY", "blob": "BINARY", "mediumblob": "BINARY",
    "longblob": "BINARY", "vector": "BINARY",
    "date": "LOCALDATE", "time": "LOCALTIME", "datetime": "LOCALDATETIME", "timestamp": "INSTANT", "year": "SHORT",
    "json": "JSON",
    # spatial: raw internal format (4-byte SRID + WKB)
    "geometry": "BINARY", "point": "BINARY", "linestring": "BINARY", "polygon": "BINARY", "multipoint": "BINARY",
    "multilinestring": "BINARY", "multipolygon": "BINARY", "geometrycollection": "BINARY", "geomcollection": "BINARY",
    # MariaDB extras
    "uuid": "STRING", "inet4": "STRING", "inet6": "STRING",
}
_UNSIGNED: dict[str, str] = {"tinyint": "SHORT", "smallint": "INT", "mediumint": "INT", "int": "LONG",
                             "integer": "LONG", "bigint": "DECIMAL"}   # bigint unsigned -> DECIMAL(20,0)

STRING_TYPES = frozenset({"char", "varchar", "tinytext", "text", "mediumtext", "longtext"})
BINARY_TYPES = frozenset({"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob"})


def base_type(engine_type: str) -> str:
    """'int(10) unsigned zerofill' -> 'int'; "enum('a','b')" -> 'enum'."""
    t = engine_type.strip().lower()
    m = re.match(r"[a-z ]+?(?=\(|\s+unsigned|\s+signed|\s+zerofill|$)", t)
    return (m.group(0) if m else t).strip()


def is_unsigned(engine_type: str) -> bool:
    return bool(re.search(r"\bunsigned\b", engine_type.lower()))


def type_args(engine_type: str) -> str:
    """Raw text between the first '(' and its matching ')'."""
    t = engine_type.strip()
    i = t.find("(")
    if i < 0:
        return ""
    depth, quote = 0, False
    for j in range(i, len(t)):
        ch = t[j]
        if ch == "'" and quote and j + 1 < len(t) and t[j + 1] == "'":
            continue
        if ch == "'":
            quote = not quote
        elif not quote and ch == "(":
            depth += 1
        elif not quote and ch == ")":
            depth -= 1
            if depth == 0:
                return t[i + 1:j]
    return t[i + 1:]


def enum_labels(engine_type: str) -> list[str]:
    """"enum('a','it''s')" -> ['a', "it's"] (also for set)."""
    body = type_args(engine_type)
    out: list[str] = []
    i = 0
    while i < len(body):
        if body[i] == "'":
            j = i + 1
            buf = []
            while j < len(body):
                if body[j] == "'" and j + 1 < len(body) and body[j + 1] == "'":
                    buf.append("'")
                    j += 2
                    continue
                if body[j] == "\\" and j + 1 < len(body):
                    buf.append(body[j + 1])
                    j += 2
                    continue
                if body[j] == "'":
                    break
                buf.append(body[j])
                j += 1
            out.append("".join(buf))
            i = j + 1
        else:
            i += 1
    return out


def canonical_name(engine_type: str, length: int | None = None) -> str:
    """Canonical type name for a catalog COLUMN_TYPE. bit(1) is BOOLEAN; bigint unsigned is DECIMAL(20,0)."""
    b = base_type(engine_type)
    if b == "bit":
        n = length
        if n is None:
            a = type_args(engine_type)
            n = int(a) if a.isdigit() else 1
        return "BOOLEAN" if n == 1 else "BINARY"
    if is_unsigned(engine_type) and b in _UNSIGNED:
        return _UNSIGNED[b]
    return _MAP.get(b, "UNKNOWN")


# MySQL character set -> codepage number stored in ColumnInfo.codepage (IBM CCSID numbering, as the Db2 connector:
# 1208 = UTF-8, 0 = binary) and codepage -> Python codec.
CHARSET_CODEPAGE: dict[str, int] = {
    "utf8mb4": 1208, "utf8mb3": 1208, "utf8": 1208, "binary": 0, "latin1": 1252, "ascii": 367, "ucs2": 13488,
    "utf16": 1200, "utf16le": 1202, "utf32": 1232, "cp1250": 1250, "cp1251": 1251, "cp1256": 1256, "cp1257": 1257,
    "latin2": 912, "greek": 813, "hebrew": 916, "latin5": 920, "latin7": 921, "koi8r": 878, "koi8u": 1168,
    "cp850": 850, "cp852": 852, "cp866": 866, "sjis": 943, "cp932": 932, "ujis": 954, "eucjpms": 954, "euckr": 970,
    "gbk": 1386, "gb2312": 1383, "gb18030": 5488, "big5": 950, "tis620": 874, "macroman": 1275, "macce": 1282,
    "swe7": 367, "dec8": 819, "hp8": 819, "armscii8": 819, "geostd8": 819, "keybcs2": 819,
}
CODEPAGE_CODEC: dict[int, str] = {
    1208: "utf-8", 1252: "cp1252", 367: "ascii", 13488: "utf-16-be", 1200: "utf-16-be", 1202: "utf-16-le",
    1232: "utf-32-be", 1250: "cp1250", 1251: "cp1251", 1256: "cp1256", 1257: "cp1257", 912: "iso8859-2",
    813: "iso8859-7", 916: "iso8859-8", 920: "iso8859-9", 921: "iso8859-13", 878: "koi8-r", 1168: "koi8-u",
    850: "cp850", 852: "cp852", 866: "cp866", 943: "shift_jis", 932: "cp932", 954: "euc_jp", 970: "euc_kr",
    1386: "gbk", 1383: "gb2312", 5488: "gb18030", 950: "big5", 874: "tis-620", 1275: "mac-roman",
    1282: "mac-latin2", 819: "latin-1",
}


def codepage_for(charset: str | None) -> int | None:
    if not charset:
        return None
    return CHARSET_CODEPAGE.get(charset.lower())


def codec_for(codepage: int | None) -> str | None:
    """None = binary (no decoding). Unknown / missing codepage for a character column => UTF-8."""
    if codepage == 0:
        return None
    return CODEPAGE_CODEC.get(codepage or 1208, "utf-8")
