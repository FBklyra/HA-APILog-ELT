"""Google BigQuery destination: one multi-statement transaction (script) per batch = MERGE(s) + checkpoint + audit.

URL (ALELT_DEST_URL):
  bigquery://<project>/?location=EU                       Application Default Credentials / service account
  bigquery://<project>/?endpoint=http://localhost:59050&emulator=1   bigquery-emulator (local tests)
  credentials_file=<path to service-account json> (optional; the file is read by the Google client, never logged)
Datasets: <schema> (landing) and <control> (checkpoint, audit_batch).

Script: BEGIN TRANSACTION; [UPDATE/DELETE truncate]; MERGE target USING (SELECT <literals> UNION ALL ...) ...;
DELETE+INSERT checkpoint; INSERT audit; COMMIT TRANSACTION;  - BigQuery commits all or nothing (snapshot
isolation; a concurrent conflicting DML aborts the script). Values are rendered as typed literals (no load job):
batches are chunked to keep the script under the query size limit.
Emulator mode (emulator=1): no PRIMARY KEY ... NOT ENFORCED; script sent without BEGIN/COMMIT TRANSACTION if
the emulator rejects them (documented limits in README).
"""
from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterator
from typing import Any

from alelt_common.models import CanonicalType
from alelt_dst_kit.sqlmerge import Dialect, Executor, SqlMergeDestination, Tx, bind_literals

log = logging.getLogger("alelt_dst_bigquery")

_FALLBACK = {"types": {
    "BOOLEAN": "BOOL", "SHORT": "INT64", "INT": "INT64", "LONG": "INT64", "DECIMAL": "NUMERIC({p},{s})",
    "FLOAT": "FLOAT64", "DOUBLE": "FLOAT64", "LOCALDATE": "DATE", "LOCALTIME": "TIME", "LOCALDATETIME": "DATETIME",
    "INSTANT": "TIMESTAMP", "STRING": "STRING", "BINARY": "BYTES", "JSON": "JSON", "XML": "STRING"},
    "defaults": {"DECIMAL_without_precision": "BIGNUMERIC", "STRING_without_length": "STRING"}}


class BigQueryDialect(Dialect):
    name = "bigquery"
    type_engine = "bigquery"
    fallback = _FALLBACK
    stage_mode = "inline"
    param = "?"
    lock_checkpoint = False
    cas_mode = "assert"             # script: ASSERT on the checkpoint row first, conditional UPDATE after the data

    def __init__(self, project: str | None = None, emulator: bool = False) -> None:
        super().__init__({"STRING": "STRING"})          # STRING length limits are not enforced in BigQuery
        self.project, self.emulator = project, emulator

    def q(self, ident: str) -> str:
        return "`" + ident.replace("\\", "\\\\").replace("`", "\\`") + "`"

    def table(self, schema: str, name: str) -> str:
        return f"{self.q(schema)}.{self.q(name)}"

    def ptype(self, col: Any) -> str:
        t = super().ptype(col)
        if t.startswith("NUMERIC(") and col.length:              # NUMERIC: p - s <= 29, s <= 9
            p, s = col.length, col.scale or 0
            if p - s > 29 or s > 9:
                return f"BIGNUMERIC({min(p, 76)},{min(s, 38)})"
        return t

    def cast_type(self, typ: str) -> str:
        return typ.split("(", 1)[0]

    _ESC = {"\\": "\\\\", "'": "\\'", "\n": "\\n", "\r": "\\r", "\t": "\\t"}

    def string_lit(self, s: str) -> str:
        """GoogleSQL quoted literal: every control character escaped (\\xHH, DST-bigquery-04); text that is not
        valid UTF-8 (lone surrogates) is refused, never sent."""
        out = []
        for ch in s:
            o = ord(ch)
            if ch in self._ESC:
                out.append(self._ESC[ch])
            elif o < 0x20 or o == 0x7F:
                out.append(f"\\x{o:02x}")
            elif 0xD800 <= o <= 0xDFFF:
                raise ValueError(f"string value is not valid UTF-8 (lone surrogate U+{o:04X})")
            else:
                out.append(ch)
        return "'" + "".join(out) + "'"

    def bytes_lit(self, b: bytes) -> str:
        return f"FROM_HEX('{b.hex()}')"

    def lit(self, v: Any, typ: str, ct: CanonicalType | None = None) -> str:
        if typ.startswith("JSON") and v is not None:
            import json

            return f"PARSE_JSON({self.string_lit(v if isinstance(v, str) else json.dumps(v, default=str))})"
        return super().lit(v, typ, ct)

    def create_table(self, fqn: str, coldefs: list[str], keys: list[str]) -> str:
        pk = f", PRIMARY KEY ({', '.join(map(self.q, keys))}) NOT ENFORCED" if keys and not self.emulator else ""
        return f"CREATE TABLE IF NOT EXISTS {fqn} ({', '.join(coldefs)}{pk}){self.cluster_by(coldefs, keys)}"

    def cluster_by(self, coldefs: list[str], keys: list[str]) -> str:
        """CLUSTER BY the first <= 4 clusterable key columns (DST-bigquery-02): MERGE scans prune by key instead of
        reading the whole table (on-demand cost)."""
        if self.emulator or not keys:
            return ""
        types = {c.split(" ", 1)[0]: c.split(" ", 1)[1].upper() for c in coldefs if " " in c}
        ok = [k for k in keys if not types.get(self.q(k), "").startswith(("FLOAT64", "BYTES", "JSON", "STRUCT",
                                                                         "ARRAY", "TIME "))
              and types.get(self.q(k), "") not in ("TIME", "TIME NOT NULL")][:4]
        return f" CLUSTER BY {', '.join(map(self.q, ok))}" if ok else ""

    def columns_sql(self, schema: str, table: str) -> tuple[str, Any]:
        return (f"SELECT column_name FROM {self.q(schema)}.INFORMATION_SCHEMA.COLUMNS WHERE table_name = ?",
                (table,))

    def describe_sql(self, schema: str, table: str) -> tuple[str, Any]:
        # BigQuery INFORMATION_SCHEMA views are per dataset (an unqualified one needs a region qualifier)
        return (f"SELECT column_name, data_type, is_nullable FROM {self.q(schema)}.INFORMATION_SCHEMA.COLUMNS "
                "WHERE table_name = ? ORDER BY ordinal_position", (table,))

    def tables_sql(self, schema: str) -> tuple[str, Any]:
        return (f"SELECT table_name FROM {self.q(schema)}.INFORMATION_SCHEMA.TABLES ORDER BY table_name", None)

    def widen_sql(self, fqn: str, col: str, typ: str, nullable: bool = True) -> list[str]:
        if typ.upper().startswith(("STRING", "BYTES", "JSON")) and "(" not in typ:
            return []                                   # unbounded already
        return [f"ALTER TABLE {fqn} ALTER COLUMN {self.q(col)} SET DATA TYPE {typ}"]   # NUMERIC(p,s) widening

    def types_sql(self, schema: str, table: str) -> tuple[str, Any] | None:
        if self.emulator:
            return None
        return (f"SELECT column_name, data_type, NULL, NULL, NULL FROM {self.q(schema)}.INFORMATION_SCHEMA.COLUMNS "
                "WHERE table_name = ?", (table,))

    def add_column(self, fqn: str, col: str, typ: str) -> str:
        return f"ALTER TABLE {fqn} ADD COLUMN IF NOT EXISTS {self.q(col)} {typ}"

    def merge(self, *a: Any, **kw: Any) -> str:
        return super().merge(*a, **kw).replace("MERGE INTO ", "MERGE ", 1)

    def hard_delete(self, target: str, source: str, keys: list[str]) -> str:
        on = " AND ".join(f"s.{self.q(k)} = t.{self.q(k)}" for k in keys)
        return f"DELETE FROM {target} AS t WHERE EXISTS (SELECT 1 FROM {source} AS s WHERE {on})"


class BigQueryExecutor(Executor):
    """Statements of a transaction are buffered and sent as ONE script on exit."""

    supports_query_in_tx = False

    def __init__(self, client: Any, dialect: Dialect, transactions: bool = True, timeout_s: float = 600,
                 no_retry: bool = False) -> None:
        self.client, self.d, self.transactions, self.timeout = client, dialect, transactions, timeout_s
        self.no_retry = no_retry          # emulator: it answers 500 for SQL errors; the client would retry them

    def _run(self, sql: str) -> Any:
        if self.no_retry:
            return self.client.query(sql, retry=None, job_retry=None, timeout=60).result(timeout=self.timeout,
                                                                                       retry=None)
        return self.client.query(sql).result(timeout=self.timeout)

    @contextlib.contextmanager
    def transaction(self) -> Iterator[Tx]:
        buf: list[str] = []
        d = self.d

        class T(Tx):
            def execute(self, sql: str, params: Any = None) -> None:
                buf.append(bind_literals(sql, params, d))

            def executemany(self, sql: str, rows: list[Any]) -> None:  # pragma: no cover - inline staging only
                raise NotImplementedError

            def query(self, sql: str, params: Any = None) -> tuple[list[str], list[tuple[Any, ...]]]:
                raise NotImplementedError
        yield T()
        if not buf:
            return
        body = ";\n".join(buf)
        script = f"BEGIN TRANSACTION;\n{body};\nCOMMIT TRANSACTION;" if self.transactions else body + ";"
        if len(script.encode("utf-8")) <= MAX_SCRIPT_CHARS or not self.transactions:
            self._run(script)
            return
        self._run_session(buf)

    def _run_session(self, statements: list[str]) -> None:
        """Batch larger than one script: ONE multi-statement transaction across several jobs of a BigQuery
        session (BEGIN in the first job, COMMIT in the last; ROLLBACK on error) - still all or nothing."""
        from google.cloud import bigquery

        first = self.client.query("BEGIN TRANSACTION", job_config=bigquery.QueryJobConfig(create_session=True))
        first.result(timeout=self.timeout)
        sid = first.session_info.session_id
        cfg = bigquery.QueryJobConfig(connection_properties=[bigquery.ConnectionProperty("session_id", sid)])
        try:
            parts: list[str] = []
            size = 0
            for st in statements:                 # pack statements into jobs under the size limit
                n = len(st.encode("utf-8")) + 2
                if parts and size + n > MAX_SCRIPT_CHARS:
                    self.client.query(";\n".join(parts) + ";", job_config=cfg).result(timeout=self.timeout)
                    parts, size = [], 0
                parts.append(st)
                size += n
            if parts:
                self.client.query(";\n".join(parts) + ";", job_config=cfg).result(timeout=self.timeout)
            self.client.query("COMMIT TRANSACTION", job_config=cfg).result(timeout=self.timeout)
        except BaseException:
            try:
                self.client.query("ROLLBACK TRANSACTION", job_config=cfg).result(timeout=self.timeout)
            except Exception:  # noqa: BLE001 - session gone: the transaction is rolled back with it
                pass
            raise
        finally:
            try:
                self.client.query("CALL BQ.ABORT_SESSION()", job_config=cfg).result(timeout=60)
            except Exception:  # noqa: BLE001
                pass

    def query(self, sql: str, params: Any = None) -> tuple[list[str], list[tuple[Any, ...]]]:
        res = self._run(bind_literals(sql, params, self.d))
        names = [f.name for f in res.schema]
        return names, [tuple(r.values()) for r in res]

    def execute(self, sql: str, params: Any = None) -> None:
        self._run(bind_literals(sql, params, self.d))


MAX_SCRIPT_CHARS = 900_000          # BigQuery query text limit is 1 MB (UTF-8); keep margin


class BigQueryDestination(SqlMergeDestination):
    engine = "bigquery"
    container_forbidden = r"[^A-Za-z0-9_]"          # dataset names (DST-loader-05)
    chunk_rows = 200
    chunk_chars = 250_000                # one MERGE statement (inline literals) stays well under the 1 MB limit

    def make_dialect(self) -> Dialect:
        return BigQueryDialect(self.u.host, self.u.flag("emulator"))

    @property
    def verify_after(self) -> bool:  # type: ignore[override]
        return self.u.flag("emulator")            # the emulator does not enforce ASSERT

    def source(self, pl: Any, cols: tuple[str, ...], ops: list[Any], types: dict[str, str], cts: dict[str, Any],
               ts: Any, name: str) -> tuple[str, list[tuple[str, Any, bool]], list[tuple[str, Any, bool]]]:
        src, pre, post = super().source(pl, cols, ops, types, cts, ts, name)
        if not self.u.flag("emulator"):
            return src, pre, post
        # bigquery-emulator: MERGE needs a single-table source -> the inline rows become a script temp table
        # (dropped automatically at script end; an explicit DROP of a temp table fails in the emulator)
        tmp = self.d.q(name)
        return tmp, [*pre, (f"CREATE TEMP TABLE {tmp} AS {src}", None, False)], post

    def make_executor(self) -> Executor:
        from google.cloud import bigquery

        u = self.u
        kw: dict[str, Any] = {"project": u.host}
        if u.get("endpoint"):
            from google.api_core.client_options import ClientOptions
            from google.auth.credentials import AnonymousCredentials

            kw.update(client_options=ClientOptions(api_endpoint=u.get("endpoint")),
                      credentials=AnonymousCredentials())
        elif u.get("credentials_file"):
            from google.oauth2 import service_account

            kw["credentials"] = service_account.Credentials.from_service_account_file(str(u.get("credentials_file")))
        if u.get("location"):
            kw["location"] = u.get("location")
        client = bigquery.Client(**kw)
        return BigQueryExecutor(client, self.d, transactions=u.flag("transactions", True),
                                timeout_s=float(u.get("timeout") or 600), no_retry=u.flag("emulator"))

    def bootstrap(self) -> None:
        if self.u.flag("emulator"):                        # emulator: datasets via the API (no DDL for schemas)
            ex = self.ex
            for ds in (self.schema, self.control):
                # the emulator answers 500 (not 409) for an existing dataset and the client would retry that for
                # minutes: look it up first, create without retry
                try:
                    ex.client.get_dataset(ds, retry=None, timeout=30)  # type: ignore[attr-defined]
                    continue
                except Exception:  # noqa: BLE001 - NotFound (or emulator error): try to create
                    pass
                with contextlib.suppress(Exception):
                    ex.client.create_dataset(ds, exists_ok=True, retry=None, timeout=30)  # type: ignore[attr-defined]
            for st in self.control_ddl():
                self.ex.execute(st)
            return
        super().bootstrap()

    def table_ddl(self, table: Any, dest: str) -> list[str]:
        ddl = super().table_ddl(table, dest)
        return ddl[1:] if self.u.flag("emulator") else ddl
