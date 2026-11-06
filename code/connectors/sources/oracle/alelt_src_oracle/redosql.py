"""Safe parser for LogMiner SQL_REDO / SQL_UNDO statements (stdlib only, never ``eval``).

Accepted statement shapes (as reconstructed by LogMiner for one row change):

    insert into "S"."T"("C1","C2",...) values ('x',NULL,TO_DATE('..','fmt'),HEXTORAW('ab'),...);
    update "S"."T" set "C1" = 'x', "C2" = NULL where "C1" = 'y' and "C2" IS NULL and ROWID = 'AAA..';
    delete from "S"."T" where "C1" = 'y' and "C2" = '1' and ROWID = 'AAA..';

Literal syntax handled by the tokenizer: quoted strings with doubled quotes, q-quote strings (``q'[..]'``),
national strings (``N'..'``), numbers, ``NULL``, ``||`` concatenation and the functions
``TO_DATE TO_TIMESTAMP TO_TIMESTAMP_TZ HEXTORAW RAWTOHEX UNISTR CHR TO_DSINTERVAL TO_YMINTERVAL
TO_BINARY_FLOAT TO_BINARY_DOUBLE TO_NUMBER TO_CHAR EMPTY_CLOB EMPTY_BLOB TO_CLOB TO_NCLOB TO_BLOB``.
Anything else raises ``RedoSyntaxError`` (the caller quarantines the row, R7).

Values come out as ``Lit(kind, value)``; type conversion to canonical values happens in ``types.convert`` with
the catalog column type.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from . import oraformat


class RedoSyntaxError(ValueError):
    pass


# ----------------------------------------------------------------------------------- literals
@dataclass(frozen=True)
class Lit:
    kind: str      # null | str | num | date | ts | tstz | raw | empty_lob | interval | float
    value: Any = None
    lost: bool = False   # XC-36: digits below 1 microsecond were non-zero (canonical datetimes keep microseconds)

    @property
    def is_null(self) -> bool:
        return self.kind == "null"


NULL = Lit("null")


# ----------------------------------------------------------------------------------- tokenizer
@dataclass(frozen=True)
class Tok:
    kind: str      # ident | qident | str | num | op | end
    text: str
    pos: int


_OPS = ("||", ":=", "(", ")", ",", "=", ";", ".", "-", "+", ":")


def tokenize(sql: str) -> list[Tok]:
    out: list[Tok] = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c.isspace():
            i += 1
            continue
        start = i
        # q-quote: q'[...]'  q'{...}'  q'<...>'  q'(...)'  q'!...!'
        if c in "qQ" and i + 2 < n and sql[i + 1] == "'":
            d = sql[i + 2]
            close = {"[": "]", "{": "}", "<": ">", "(": ")"}.get(d, d)
            j = sql.find(close + "'", i + 3)
            if j < 0:
                raise RedoSyntaxError(f"unterminated q-quote at {start}")
            out.append(Tok("str", sql[i + 3:j], start))
            i = j + 2
            continue
        if c in "nN" and i + 1 < n and sql[i + 1] == "'":   # national string literal
            i += 1
            c = "'"
        if c == "'":
            buf: list[str] = []
            i += 1
            while True:
                j = sql.find("'", i)
                if j < 0:
                    raise RedoSyntaxError(f"unterminated string at {start}")
                buf.append(sql[i:j])
                if j + 1 < n and sql[j + 1] == "'":
                    buf.append("'")
                    i = j + 2
                    continue
                i = j + 1
                break
            out.append(Tok("str", "".join(buf), start))
            continue
        if c == '"':
            buf = []
            i += 1
            while True:
                j = sql.find('"', i)
                if j < 0:
                    raise RedoSyntaxError(f"unterminated identifier at {start}")
                buf.append(sql[i:j])
                if j + 1 < n and sql[j + 1] == '"':
                    buf.append('"')
                    i = j + 2
                    continue
                i = j + 1
                break
            out.append(Tok("qident", "".join(buf), start))
            continue
        if c.isdigit() or (c == "." and i + 1 < n and sql[i + 1].isdigit()):
            j = i
            while j < n and (sql[j].isdigit() or sql[j] == "."):
                j += 1
            if j < n and sql[j] in "eE" and j + 1 < n and (sql[j + 1].isdigit() or sql[j + 1] in "+-"):
                j += 2
                while j < n and sql[j].isdigit():
                    j += 1
            out.append(Tok("num", sql[i:j], start))
            i = j
            continue
        if c.isalpha() or c in "_$#":
            j = i
            while j < n and (sql[j].isalnum() or sql[j] in "_$#"):
                j += 1
            out.append(Tok("ident", sql[i:j], start))
            i = j
            continue
        for op in _OPS:
            if sql.startswith(op, i):
                out.append(Tok("op", op, start))
                i += len(op)
                break
        else:
            raise RedoSyntaxError(f"unexpected character {c!r} at {i}")
    out.append(Tok("end", "", n))
    return out


# ----------------------------------------------------------------------------------- statements
@dataclass
class Stmt:
    op: str                                   # INSERT | UPDATE | DELETE
    schema: str
    table: str
    values: dict[str, Lit] = field(default_factory=dict)     # INSERT values / UPDATE set
    where: dict[str, Lit] = field(default_factory=dict)      # equality / IS NULL predicates
    rowid: str | None = None


class _P:
    def __init__(self, sql: str) -> None:
        self.sql = sql
        self.t = tokenize(sql)
        self.i = 0

    # -- helpers
    def peek(self, k: int = 0) -> Tok:
        return self.t[min(self.i + k, len(self.t) - 1)]

    def next(self) -> Tok:
        tok = self.t[self.i]
        self.i = min(self.i + 1, len(self.t) - 1)
        return tok

    def kw(self, word: str) -> bool:
        tok = self.peek()
        if tok.kind == "ident" and tok.text.upper() == word:
            self.i += 1
            return True
        return False

    def expect_kw(self, word: str) -> None:
        if not self.kw(word):
            raise RedoSyntaxError(f"expected {word} at {self.peek().pos}: {self.sql[self.peek().pos:][:40]!r}")

    def expect_op(self, op: str) -> None:
        tok = self.next()
        if tok.kind != "op" or tok.text != op:
            raise RedoSyntaxError(f"expected {op!r} at {tok.pos}: {self.sql[tok.pos:][:40]!r}")

    def is_op(self, op: str) -> bool:
        tok = self.peek()
        return tok.kind == "op" and tok.text == op

    def ident(self) -> str:
        tok = self.next()
        if tok.kind == "qident":
            return tok.text
        if tok.kind == "ident":
            return tok.text.upper()       # unquoted identifiers are upper-cased by Oracle
        raise RedoSyntaxError(f"expected identifier at {tok.pos}")

    def qname(self) -> tuple[str, str]:
        a = self.ident()
        if self.is_op("."):
            self.next()
            return a, self.ident()
        return "", a

    def end(self) -> None:
        if self.is_op(";"):
            self.next()
        if self.peek().kind != "end":
            tok = self.peek()
            raise RedoSyntaxError(f"trailing text at {tok.pos}: {self.sql[tok.pos:][:40]!r}")

    # -- expressions
    def expr(self) -> Lit:
        parts = [self.term()]
        while self.is_op("||"):
            self.next()
            parts.append(self.term())
        if len(parts) == 1:
            return parts[0]
        if all(p.kind == "raw" for p in parts):
            return Lit("raw", b"".join(p.value for p in parts))
        txt = "".join("" if p.is_null else _as_text(p) for p in parts)
        return Lit("str", txt)

    def term(self) -> Lit:
        tok = self.next()
        if tok.kind == "str":
            return Lit("str", tok.text)
        if tok.kind == "num":
            return Lit("num", tok.text)
        if tok.kind == "op" and tok.text in "-+" and self.peek().kind == "num":
            return Lit("num", (tok.text if tok.text == "-" else "") + self.next().text)
        if tok.kind == "op" and tok.text == "(":
            v = self.expr()
            self.expect_op(")")
            return v
        if tok.kind == "ident":
            name = tok.text.upper()
            if name == "NULL":
                return NULL
            if self.is_op("("):
                self.next()
                args: list[Lit] = []
                if not self.is_op(")"):
                    args.append(self.expr())
                    while self.is_op(","):
                        self.next()
                        args.append(self.expr())
                self.expect_op(")")
                return _call(name, args)
        raise RedoSyntaxError(f"unsupported value at {tok.pos}: {self.sql[tok.pos:][:40]!r}")

    # -- predicates
    def where(self, st: Stmt) -> None:
        while True:
            if self.peek().kind == "ident" and self.peek().text.upper() == "ROWID" and self.peek(1).text == "=":
                self.next()
                self.next()
                v = self.expr()
                st.rowid = None if v.is_null else str(v.value)
            else:
                col = self.ident()
                if self.kw("IS"):
                    if self.kw("NOT"):
                        raise RedoSyntaxError("IS NOT NULL predicate not supported")
                    self.expect_kw("NULL")
                    st.where[col] = NULL
                else:
                    self.expect_op("=")
                    st.where[col] = self.expr()
            if not self.kw("AND"):
                break


def _as_text(v: Lit) -> str:
    if v.kind in ("str", "num", "interval", "float"):
        return str(v.value)
    raise RedoSyntaxError(f"cannot concatenate {v.kind}")


def _str_arg(args: list[Lit], k: int, fn: str) -> str:
    if len(args) <= k or args[k].kind not in ("str", "num"):
        raise RedoSyntaxError(f"{fn}: argument {k + 1} must be a string")
    return str(args[k].value)


def _sub_us(text: str) -> bool:
    """XC-36: TIMESTAMP(7..9) fraction digits below 1 us are non-zero (they are truncated: precision_lost note)."""
    m = re.search(r"[.,](\d{7,})", text)
    return bool(m and m.group(1)[6:].strip("0"))


def _call(name: str, args: list[Lit]) -> Lit:
    if args and args[0].is_null and name not in ("EMPTY_CLOB", "EMPTY_BLOB"):
        return NULL
    if name == "TO_DATE":
        fmt = _str_arg(args, 1, name) if len(args) > 1 else "YYYY-MM-DD HH24:MI:SS"
        return Lit("date", _dt(_str_arg(args, 0, name), fmt))
    if name == "TO_TIMESTAMP":
        fmt = _str_arg(args, 1, name) if len(args) > 1 else "YYYY-MM-DD HH24:MI:SS.FF"
        text = _str_arg(args, 0, name)
        return Lit("ts", _dt(text, fmt), _sub_us(text))
    if name == "TO_TIMESTAMP_TZ":
        text = _str_arg(args, 0, name)
        if len(args) > 1:
            return Lit("tstz", _dt(text, _str_arg(args, 1, name), tz=True), _sub_us(text))
        if not re.search(r"\s[+-]\d{1,2}:\d{2}$|\s[A-Za-z][\w/+-]*$", text.strip()):
            # TIMESTAMP WITH LOCAL TIME ZONE: LogMiner prints TO_TIMESTAMP_TZ('<session-tz local time>') without an
            # offset (seen live on 23ai); the miner session runs with TIME_ZONE = 'UTC' => naive value = UTC.
            return Lit("tstz", _dt(text, "YYYY-MM-DD HH24:MI:SS.FF"), _sub_us(text))
        return Lit("tstz", _dt(text, "YYYY-MM-DD HH24:MI:SS.FF TZH:TZM", tz=True), _sub_us(text))
    if name == "HEXTORAW":
        h = _str_arg(args, 0, name)
        try:
            return Lit("raw", bytes.fromhex(h if len(h) % 2 == 0 else "0" + h))
        except ValueError as e:
            raise RedoSyntaxError(f"HEXTORAW: bad hex {h[:20]!r}") from e
    if name in ("EMPTY_CLOB", "EMPTY_BLOB"):
        return Lit("empty_lob", name)
    if name in ("TO_CLOB", "TO_NCLOB", "TO_CHAR", "TO_NCHAR"):
        a = args[0] if args else NULL
        return Lit("str", _as_text(a)) if a.kind != "raw" else a
    if name == "TO_BLOB":
        return args[0] if args and args[0].kind == "raw" else _call("HEXTORAW", args)
    if name == "UNISTR":
        return Lit("str", _unistr(_str_arg(args, 0, name)))
    if name in ("CHR", "NCHR"):
        code = int(_str_arg(args, 0, name))
        return Lit("str", chr(code) if name == "NCHR" or code < 128 else bytes([code]).decode("latin-1"))
    if name in ("TO_DSINTERVAL", "TO_YMINTERVAL"):
        return Lit("interval", _str_arg(args, 0, name))
    if name in ("TO_BINARY_FLOAT", "TO_BINARY_DOUBLE"):
        return Lit("float", _str_arg(args, 0, name))
    if name == "TO_NUMBER":
        return Lit("num", _str_arg(args, 0, name))
    raise RedoSyntaxError(f"unsupported function {name}()")


def _dt(value: str, fmt: str, tz: bool = False) -> datetime:
    try:
        return oraformat.parse(value, fmt, with_tz=tz)
    except (oraformat.FormatError, ValueError) as e:
        raise RedoSyntaxError(str(e)) from e


def _unistr(s: str) -> str:
    out: list[str] = []
    i = 0
    while i < len(s):
        if s[i] == "\\":
            if s[i + 1:i + 2] == "\\":
                out.append("\\")
                i += 2
                continue
            hx = s[i + 1:i + 5]
            if len(hx) != 4:
                raise RedoSyntaxError("UNISTR: bad escape")
            out.append(chr(int(hx, 16)))
            i += 5
        else:
            out.append(s[i])
            i += 1
    # join UTF-16 surrogate pairs
    return "".join(out).encode("utf-16", "surrogatepass").decode("utf-16")


# ----------------------------------------------------------------------------------- entry point
def parse_stmt(sql: str) -> Stmt:
    """Parse one LogMiner DML statement. Raises RedoSyntaxError for anything outside the grammar."""
    p = _P(sql)
    if p.kw("INSERT"):
        p.expect_kw("INTO")
        sch, tab = p.qname()
        st = Stmt("INSERT", sch, tab)
        cols: list[str] = []
        if p.is_op("("):
            p.next()
            cols.append(p.ident())
            while p.is_op(","):
                p.next()
                cols.append(p.ident())
            p.expect_op(")")
        p.expect_kw("VALUES")
        p.expect_op("(")
        vals = [p.expr()]
        while p.is_op(","):
            p.next()
            vals.append(p.expr())
        p.expect_op(")")
        if len(cols) != len(vals):
            raise RedoSyntaxError(f"insert: {len(cols)} columns but {len(vals)} values")
        st.values = dict(zip(cols, vals))
        p.end()
        return st
    if p.kw("UPDATE"):
        sch, tab = p.qname()
        st = Stmt("UPDATE", sch, tab)
        p.expect_kw("SET")
        while True:
            col = p.ident()
            p.expect_op("=")
            st.values[col] = p.expr()
            if not p.is_op(","):
                break
            p.next()
        if p.kw("WHERE"):
            p.where(st)
        p.end()
        return st
    if p.kw("DELETE"):
        p.expect_kw("FROM")
        sch, tab = p.qname()
        st = Stmt("DELETE", sch, tab)
        if p.kw("WHERE"):
            p.where(st)
        p.end()
        return st
    raise RedoSyntaxError(f"not a DML statement: {sql[:40]!r}")


# ----------------------------------------------------------------------------------- LOB blocks
@dataclass
class LobLocator:
    column: str
    schema: str
    table: str
    where: dict[str, Lit]
    rowid: str | None
    is_binary: bool


@dataclass
class LobOp:
    kind: str                  # write | trim | erase
    offset: int = 1            # 1-based (characters for CLOB, bytes for BLOB)
    length: int = 0
    data: str | bytes | None = None


def parse_lob_locator(sql: str) -> LobLocator:
    """SELECT_LOB_LOCATOR block: ``DECLARE ... BEGIN select "COL" into loc_c from "S"."T" where ... for update;``"""
    up = sql.upper()
    k = up.find("SELECT ")
    if k < 0 or up.find(" INTO ", k) < 0:
        raise RedoSyntaxError("SELECT_LOB_LOCATOR: no select ... into")
    p = _P(sql[k:])
    p.expect_kw("SELECT")
    col = p.ident()
    p.expect_kw("INTO")
    var = p.next().text.lower()
    p.expect_kw("FROM")
    sch, tab = p.qname()
    st = Stmt("UPDATE", sch, tab)
    if p.kw("WHERE"):
        p.where(st)
    return LobLocator(col, sch, tab, st.where, st.rowid, is_binary=var.endswith("_b"))


def parse_lob_ops(sql: str) -> list[LobOp]:
    """LOB_WRITE / LOB_TRIM / LOB_ERASE blocks: ``buf_c := '...'; dbms_lob.write(loc_c, 10, 1, buf_c);``"""
    ops: list[LobOp] = []
    bufs: dict[str, Lit] = {}
    p = _P(sql)
    while p.peek().kind != "end":
        tok = p.next()
        if tok.kind != "ident":
            continue
        word = tok.text.lower()
        if word.startswith("buf_") and p.is_op(":="):
            p.next()
            bufs[word] = p.expr()
            continue
        if word == "dbms_lob" and p.is_op("."):
            p.next()
            fn = p.next().text.lower()
            p.expect_op("(")
            args: list[Any] = []
            while not p.is_op(")"):
                t = p.peek()
                if t.kind == "ident" and t.text.lower().startswith(("loc_", "buf_")):
                    args.append(p.next().text.lower())
                else:
                    args.append(p.expr())
                if p.is_op(","):
                    p.next()
            p.expect_op(")")
            nums = [int(str(a.value)) for a in args if isinstance(a, Lit) and a.kind == "num"]
            if fn == "write" and len(nums) >= 2:
                bname = next((a for a in args if isinstance(a, str) and a.startswith("buf_")), None)
                b = bufs.get(bname or "", NULL)
                ops.append(LobOp("write", offset=nums[1], length=nums[0],
                                 data=b.value if b.kind in ("str", "raw") else ("" if b.is_null else str(b.value))))
            elif fn == "trim" and nums:
                ops.append(LobOp("trim", length=nums[0]))
            elif fn == "erase" and len(nums) >= 2:
                ops.append(LobOp("erase", offset=nums[1], length=nums[0]))
            else:
                raise RedoSyntaxError(f"unsupported dbms_lob.{fn}")
    return ops

