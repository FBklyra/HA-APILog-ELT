"""Delta Lake destination (delta-rs). Files on local disk / S3 / ADLS / GCS; Databricks, Spark, Trino, DuckDB read them.

URL (ALELT_DEST_URL):
  delta+file:///C:/lake                                    local directory
  delta+s3://bucket/prefix?endpoint=http://host:9000&region=us-east-1&access_key=${K}&secret_key=${S}&allow_http=1
  delta+az://container/prefix?opt.azure_storage_account_name=...   |  delta+gs://bucket/prefix?opt.google_service_account=...
`opt.<KEY>=<value>` is passed to delta-rs storage_options as-is. On S3 the log commit uses conditional PUT
(`conditional_put=etag`, default: atomic with concurrent writers); `unsafe_rename=1` (AWS_S3_ALLOW_UNSAFE_RENAME)
is an explicit single-writer opt-in only (never with HA workers).

Layout: <base>/<schema>/<table>, <base>/<control>/checkpoint, <base>/<control>/audit_batch.

Exactly-once (Delta transaction log):
- every table commit of a batch carries an application transaction `txn{appId=<stream>, version=<seq>}` plus
  commitInfo metadata alelt.stream / alelt.position / alelt.batch_id / alelt.seq, in the SAME commit as the MERGE;
- before writing a table the plugin reads `transaction_version(stream)` and the alelt.* commit metadata: the same
  batch (seq + position) => that table already holds it (crash after it) and is skipped; written by ANOTHER
  writer for this seq => CheckpointConflictError;
- compare-and-set (F-05): the stream checkpoint is re-read before the first table commit and at the control table
  version the final MERGE is based on; delta-rs optimistic concurrency rejects a commit whose read version is
  stale (CommitFailedError) -> CheckpointConflictError, or a no-op when the same batch won;
- the stream checkpoint (control table, also txn-tagged, incl. the per-table watermarks JSON) is committed last.
Atomicity is per table (Delta has no multi-table commit): after a crash a table is either fully at the old or
fully at the new state, never partial; the retry completes the others exactly once.
"""
from __future__ import annotations

import contextlib
import json
import logging
import os
from collections.abc import Iterator
from datetime import datetime, time as dtime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from alelt_common.models import CanonicalType, ColumnInfo, Position, TableInfo
from alelt_common.spi import ApplyStats, CheckpointConflictError
from alelt_dst_kit.arrowutil import exact_decimal
from alelt_dst_kit import ControlState, KitDestination, TablePlan, physical_type, type_map
from alelt_dst_kit.base import AlreadyApplied, wm_loads

log = logging.getLogger("alelt_dst_delta")
# bounded log / file retention (DST-delta-04); files are removed by maintain() (optimize + vacuum)
TABLE_CONFIG = {"delta.logRetentionDuration": "interval 7 days",
                "delta.deletedFileRetentionDuration": "interval 7 days", "delta.checkpointInterval": "10"}

_FALLBACK = {"types": {
    "BOOLEAN": "boolean", "SHORT": "short", "INT": "integer", "LONG": "long", "DECIMAL": "decimal({p},{s})",
    "FLOAT": "float", "DOUBLE": "double", "LOCALDATE": "date", "LOCALTIME": "string",
    "LOCALDATETIME": "timestamp_ntz", "INSTANT": "timestamp", "STRING": "string", "BINARY": "binary",
    "JSON": "string", "XML": "string"}, "defaults": {"DECIMAL_without_precision": "decimal(38,0)"},
    "limits": {"DECIMAL_max_precision": 38}}


def delta_type(col: ColumnInfo) -> str:
    return physical_type(col, type_map("delta", _FALLBACK))


def arrow_type(t: str) -> pa.DataType:
    t = t.strip().lower()
    if t.startswith("decimal"):
        p, s = (int(x) for x in t[t.index("(") + 1:t.index(")")].split(","))
        return pa.decimal128(p, s)
    return {"boolean": pa.bool_(), "byte": pa.int8(), "short": pa.int16(), "integer": pa.int32(),
            "long": pa.int64(), "float": pa.float32(), "double": pa.float64(), "date": pa.date32(),
            "string": pa.string(), "binary": pa.binary(), "timestamp_ntz": pa.timestamp("us"),
            "timestamp": pa.timestamp("us", tz="UTC")}[t]


def to_arrow_value(v: Any, ct: CanonicalType | None, at: pa.DataType) -> Any:
    if v is None:
        return None
    if ct is CanonicalType.LOCALTIME and isinstance(v, dtime):
        return v.isoformat()
    if ct is CanonicalType.JSON and not isinstance(v, str):
        return json.dumps(v, separators=(",", ":"), default=str)
    if pa.types.is_decimal(at):
        return exact_decimal(v, at.scale)
    if pa.types.is_timestamp(at) and isinstance(v, datetime):
        if at.tz and v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        if not at.tz and v.tzinfo is not None:
            return v.astimezone(timezone.utc).replace(tzinfo=None)
    return v


def from_arrow_value(v: Any, ct: CanonicalType | None) -> Any:
    if v is None:
        return None
    if ct is CanonicalType.LOCALTIME and isinstance(v, str):
        return dtime.fromisoformat(v)
    return v


class DeltaDestination(KitDestination):
    engine = "delta"
    needs_full_rows = True

    def __init__(self, url: str = "", schema: str = "landing", control_schema: str = "alelt_control",
                 output_profile: str | None = None, compare: Any = None, **kw: Any) -> None:
        super().__init__(url, schema, control_schema, output_profile, compare, **kw)
        self.base, self.storage = self._location()
        self._info: dict[str, TableInfo] = {}

    # ------------------------------------------------------------------ location
    def _location(self) -> tuple[str, dict[str, str]]:
        u = self.u
        scheme = u.scheme.split("+", 1)[-1] if "+" in u.scheme else (u.scheme or "file")
        opts = {k[4:]: v for k, v in u.params.items() if k.startswith("opt.")}
        if scheme == "file":
            p = u.path.lstrip("/") if len(u.path) > 2 and u.path[2] == ":" else u.path   # /C:/x -> C:/x
            if u.host and u.host not in ("", "localhost"):
                p = f"{u.host}:{p}" if len(u.host) == 1 else f"//{u.host}{p}"
            Path(p).mkdir(parents=True, exist_ok=True)
            return Path(p).resolve().as_posix(), opts
        base = f"{scheme}://{u.hosts}{u.path}".rstrip("/")
        if scheme in ("s3", "s3a"):
            for k, env in (("endpoint", "AWS_ENDPOINT_URL"), ("region", "AWS_REGION"),
                           ("access_key", "AWS_ACCESS_KEY_ID"), ("secret_key", "AWS_SECRET_ACCESS_KEY")):
                if u.get(k):
                    opts[env] = str(u.get(k))
            if u.flag("allow_http") or str(u.get("endpoint", "")).startswith("http://"):
                opts["AWS_ALLOW_HTTP"] = "true"
            if not u.flag("unsafe_rename") and "AWS_S3_ALLOW_UNSAFE_RENAME" not in opts:
                opts.setdefault("conditional_put", "etag")      # atomic log commits on S3 / S3-compatible stores
            elif u.flag("unsafe_rename"):
                opts["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"     # explicit opt-in: ONE writer only (no HA)
            opts.setdefault("AWS_REGION", os.environ.get("AWS_REGION", "us-east-1"))
        return base, opts

    def uri(self, dest: str) -> str:
        return f"{self.base}/{self.schema}/{self.ident(dest)}"

    def curi(self, name: str) -> str:
        return f"{self.base}/{self.control}/{name}"

    def _dt(self, uri: str) -> Any:
        from deltalake import DeltaTable

        return DeltaTable(uri, storage_options=self.storage or None)

    def _exists(self, uri: str) -> bool:
        from deltalake import DeltaTable

        return bool(DeltaTable.is_deltatable(uri, storage_options=self.storage or None))

    # ------------------------------------------------------------------ DDL
    _CKPT = pa.schema([("stream", pa.string()), ("seq", pa.int64()), ("position", pa.string()),
                       ("batch_id", pa.string()), ("updated_at", pa.timestamp("us", tz="UTC")),
                       ("watermarks", pa.string())])
    _AUDIT = pa.schema([("batch_id", pa.string()), ("stream", pa.string()), ("seq", pa.int64()),
                        ("from_position", pa.string()), ("to_position", pa.string()), ("rows_by_table", pa.string()),
                        ("total_rows", pa.int64()), ("status", pa.string()),
                        ("applied_at", pa.timestamp("us", tz="UTC"))])

    def bootstrap(self) -> None:
        from deltalake import DeltaTable

        for name, sch in (("checkpoint", self._CKPT), ("audit_batch", self._AUDIT)):
            if not self._exists(self.curi(name)):
                DeltaTable.create(self.curi(name), sch, mode="ignore", storage_options=self.storage or None,
                                  configuration=TABLE_CONFIG)
        ck = self._dt(self.curi("checkpoint"))
        if "watermarks" not in ck.schema().to_arrow().names:             # control table of an older release
            from deltalake.schema import Field, PrimitiveType

            ck.alter.add_columns([Field("watermarks", PrimitiveType("string"), nullable=True)])

    def arrow_schema(self, table: TableInfo) -> pa.Schema:
        keyless = not table.key_columns()
        fields = [pa.field(self.ident(c.name), arrow_type(delta_type(c)), nullable=not c.key_seq)
                  for c in self.data_cols(table, keyless)]
        fields += [pa.field(self.c_synced, pa.timestamp("us", tz="UTC"), nullable=False),
                   pa.field(self.c_deleted, pa.bool_(), nullable=False)]
        return pa.schema(fields)

    def ensure_table(self, table: TableInfo, dest_name: str, golden_ddl: str | None) -> None:
        from deltalake import DeltaTable

        uri = self.uri(dest_name)
        self._info[self.ident(dest_name)] = table
        sch = self.arrow_schema(table)
        if not self._exists(uri):
            DeltaTable.create(uri, sch, mode="ignore", storage_options=self.storage or None,
                              configuration={"delta.appendOnly": "false", **TABLE_CONFIG})
            return
        have = set(self._dt(uri).schema().to_arrow().names)
        for c in table.columns:
            if self.ident(c.name) not in have:
                self.add_column(dest_name, c)

    def add_column(self, dest_name: str, col: ColumnInfo) -> None:
        from deltalake.schema import Field, PrimitiveType

        t = delta_type(col)
        dt = self._dt(self.uri(dest_name))
        if self.ident(col.name) in dt.schema().to_arrow().names:
            return
        dt.alter.add_columns([Field(self.ident(col.name), PrimitiveType(t), nullable=True)])

    # ------------------------------------------------------------------ control
    def control_state(self, stream: str) -> ControlState:
        uri = self.curi("checkpoint")
        if not self._exists(uri):
            return ControlState()
        t = self._dt(uri).to_pyarrow_dataset().to_table(filter=pc.field("stream") == stream).to_pylist()
        if not t:
            return ControlState()
        r = max(t, key=lambda x: x["seq"])
        return ControlState(r["position"] or None, int(r["seq"]), r["batch_id"], wm_loads(r.get("watermarks")))

    def _state_at(self, dt: Any, stream: str) -> ControlState:
        """Checkpoint of `stream` as of the loaded version of the control table `dt`."""
        t = dt.to_pyarrow_dataset().to_table(filter=pc.field("stream") == stream).to_pylist()
        if not t:
            return ControlState()
        r = max(t, key=lambda x: x["seq"])
        return ControlState(r["position"] or None, int(r["seq"]), r["batch_id"])

    def delete_checkpoint(self, stream: str) -> None:
        self._dt(self.curi("checkpoint")).delete("stream = '" + stream.replace("'", "''") + "'")

    def maintain(self, dest_names: list[str] | None = None, retention_hours: int = 168) -> dict[str, str]:
        """Periodic maintenance (worker job, DST-delta-04): compact small files + vacuum files older than the
        retention on landing + control tables. Returns {table: 'ok' | error}."""
        out: dict[str, str] = {}
        uris = [self.uri(n) for n in (dest_names or list(self._info))] + [self.curi("checkpoint"),
                                                                         self.curi("audit_batch")]
        for uri in uris:
            try:
                dt = self._dt(uri)
                dt.optimize.compact()
                dt.vacuum(retention_hours=retention_hours, dry_run=False, enforce_retention_duration=False)
                out[uri] = "ok"
            except Exception as e:  # noqa: BLE001 - best effort, reported
                out[uri] = f"{type(e).__name__}: {e}"[:200]
        return out

    def table_marker(self, dt: Any, stream: str) -> tuple[int, str | None, str | None]:
        """(seq, position, batch_id) of the last commit of `stream` in table `dt` (txn version + alelt.* metadata)."""
        v = self._txn_version(dt, stream)
        if v < 0:
            return 0, None, None
        for h in dt.history(50):
            if h.get("alelt.stream") == stream and str(h.get("alelt.seq")) == str(v):
                return v, h.get("alelt.position"), h.get("alelt.batch_id")
        return v, None, None

    def _props(self, stream: str, seq: int, to_pos: Position, batch_id: str, app: str | None = None) -> Any:
        from deltalake import CommitProperties, Transaction

        return CommitProperties(
            app_transactions=[Transaction(app or stream, seq)],
            custom_metadata={"alelt.stream": stream, "alelt.position": to_pos.text, "alelt.batch_id": batch_id,
                             "alelt.seq": str(seq)})

    @staticmethod
    def _txn_version(dt: Any, app: str) -> int:
        try:
            v = dt.transaction_version(app)
        except Exception:  # noqa: BLE001 - no txn recorded
            return -1
        return -1 if v is None else int(v)

    # ------------------------------------------------------------------ apply
    def lookup(self, plan: TablePlan, keys: list[tuple[Any, ...]]) -> dict[tuple[Any, ...], dict[str, Any]]:
        uri = self.uri(plan.dest)
        if not keys or not self._exists(uri):
            return {}
        from alelt_loader.loader import _canon

        types = {c.name: c.canonical_type for c in plan.info.columns}
        kc = [self.ident(k) for k in plan.keys]
        out: dict[tuple[Any, ...], dict[str, Any]] = {}
        ds = self._dt(uri).to_pyarrow_dataset()
        at = ds.schema.field(kc[0]).type
        want = set(keys)
        for i in range(0, len(keys), 2000):            # typed filter on the first key column (DST-delta-05)
            vals = pa.array(list({_coerce_key(k[0], at) for k in keys[i:i + 2000]}), type=at)
            try:
                found = ds.to_table(filter=pc.field(kc[0]).isin(vals))
            except (pa.ArrowNotImplementedError, pa.ArrowInvalid):   # string_view stats (some stores): no pushdown
                t = ds.to_table()
                found = t.filter(pc.is_in(t[kc[0]].cast(at if not pa.types.is_string_view(at) else pa.string()),
                                          value_set=vals.cast(pa.string()) if pa.types.is_string_view(at) else vals))
            for r in found.to_pylist():
                rec = {c: from_arrow_value(r.get(self.ident(c)), types.get(c)) for c in plan.columns}
                k = tuple(_canon(rec[c]) for c in plan.keys)
                if k in want:
                    out[k] = rec
        return out

    def _source(self, pl: TablePlan, ts: datetime) -> pa.Table:
        info = self._info.get(self.ident(pl.dest)) or pl.info
        sch = self.arrow_schema(info)
        types = {c.name: c.canonical_type for c in pl.info.columns}
        src = {self.ident(c): c for c in pl.columns}
        cols: dict[str, list[Any]] = {f.name: [] for f in sch}
        ops: list[str] = []
        for op in pl.ops.values():
            for f in sch:
                if f.name == self.c_synced:
                    v: Any = ts
                elif f.name == self.c_deleted:
                    v = op.op == "D"
                else:
                    c = src.get(f.name)
                    v = to_arrow_value(op.row.get(c) if c else None, types.get(c or ""), f.type)
                cols[f.name].append(v)
            ops.append(op.op)
        arrays = [pa.array(cols[f.name], type=f.type) for f in sch]
        return pa.Table.from_arrays([*arrays, pa.array(ops, type=pa.string())],
                                    schema=sch.append(pa.field("_alelt_op", pa.string())))

    def commit_batch(self, stream: str | None, plans: list[TablePlan], to_pos: Position | None, batch_id: str,
                     prev: ControlState, ts: datetime, stats: dict[str, ApplyStats]) -> None:
        seq = prev.seq + 1
        if stream and to_pos:                       # compare-and-set before the first table commit (F-05)
            self.cas(stream, prev, self.control_state(stream), to_pos)
        for i, pl in enumerate(plans):
            self.count(pl, stats[pl.dest])
            uri = self.uri(pl.dest)
            dt = self._dt(uri)
            props = self._props(stream, seq, to_pos, batch_id) if stream and to_pos else None
            if stream and to_pos and self.table_decision(stream, prev, to_pos, batch_id,
                                                         *self.table_marker(dt, stream)) == "skip":
                log.info("table %s already holds %s seq %s - skipped", pl.dest, stream, seq)
                continue
            with self._commit_guard(stream, prev, to_pos):
                self._write_table(dt, uri, pl, ts, props)
            if i == 0:
                self._hook("after_data")
        self._hook("before_commit")
        if stream is None or to_pos is None:
            return
        from deltalake import write_deltalake

        cdt = self._dt(self.curi("checkpoint"))
        self.cas(stream, prev, self._state_at(cdt, stream), to_pos)     # at the version the MERGE reads
        audit = self.audit_record(stream, prev, to_pos, batch_id, stats)
        adt = self._dt(self.curi("audit_batch"))
        if self._txn_version(adt, stream) < seq:
            row = {**{k: audit[k] for k in ("batch_id", "stream", "seq", "from_position", "to_position",
                                            "rows_by_table", "total_rows", "status")}, "applied_at": ts}
            write_deltalake(adt, pa.Table.from_pylist([row], schema=self._AUDIT), mode="append",
                            commit_properties=self._props(stream, seq, to_pos, batch_id))
        ck = pa.Table.from_pylist([{"stream": stream, "seq": seq, "position": to_pos.text, "batch_id": batch_id,
                                    "updated_at": ts, "watermarks": self.wm_text()}], schema=self._ck_schema(cdt))
        with self._commit_guard(stream, prev, to_pos):                   # stream COMMIT POINT
            (cdt.merge(ck, "t.stream = s.stream", source_alias="s", target_alias="t",
                       commit_properties=self._props(stream, seq, to_pos, batch_id))
             .when_matched_update_all().when_not_matched_insert_all().execute())

    def _ck_schema(self, cdt: Any) -> pa.Schema:
        names = set(cdt.schema().to_arrow().names)
        return pa.schema([f for f in self._CKPT if f.name in names])

    @contextlib.contextmanager
    def _commit_guard(self, stream: str | None, prev: ControlState, to_pos: Position | None) -> Iterator[None]:
        """delta-rs optimistic concurrency: CommitFailedError (concurrent commit) -> no-op / conflict."""
        try:
            yield
        except Exception as e:
            if stream is None or to_pos is None or type(e).__name__ not in ("CommitFailedError",):
                raise
            found = self.control_state(stream)
            if self.pos_ge(found.position, to_pos):
                raise AlreadyApplied from e
            raise CheckpointConflictError(stream, prev.position, found.position,
                                          f"delta commit rejected: {str(e)[:200]}") from e

    def _write_table(self, dt: Any, uri: str, pl: TablePlan, ts: datetime, props: Any) -> None:
        if pl.truncate and pl.ops:                   # DST-delta-03: truncate folded into the merge = ONE commit
            self._merge(dt, pl, ts, props, truncate=True)
            return
        if pl.truncate:
            kw = {"commit_properties": props} if props else {}
            if self.hard:
                dt.delete(**kw)
            else:
                dt.update(predicate=f"NOT {_qi(self.c_deleted)}",
                          updates={_qi(self.c_deleted): "true", _qi(self.c_synced): _ts_lit(ts)}, **kw)
            dt = self._dt(uri)
        if pl.ops:
            self._merge(dt, pl, ts, props)

    def _merge(self, dt: Any, pl: TablePlan, ts: datetime, props: Any, truncate: bool = False) -> None:
        src = self._source(pl, ts)
        names = [n for n in src.schema.names if n != "_alelt_op"]
        pred = " AND ".join(f"t.{_qi(self.ident(k))} = s.{_qi(self.ident(k))}" for k in pl.keys)
        sets = {f"{_qi(n)}": f"s.{_qi(n)}" for n in names}
        kw = {"commit_properties": props} if props else {}
        m = dt.merge(src, pred, source_alias="s", target_alias="t", **kw)
        if self.hard:
            m = (m.when_matched_delete(predicate="s._alelt_op = 'D'")
                 .when_matched_update(updates=sets, predicate="s._alelt_op = 'U'")
                 .when_not_matched_insert(updates=sets, predicate="s._alelt_op = 'U'"))
        else:
            m = m.when_matched_update(updates=sets).when_not_matched_insert(updates=sets)
        if truncate:                                  # rows the batch does not re-write: truncated
            if self.hard:
                m = m.when_not_matched_by_source_delete()
            else:
                m = m.when_not_matched_by_source_update(
                    updates={_qi(self.c_deleted): "true", _qi(self.c_synced): _ts_lit(ts)},
                    predicate=f"NOT t.{_qi(self.c_deleted)}")
        m.execute()

    # ------------------------------------------------------------------ verify
    def scan_rows(self, dest_name: str, columns: list[str] | None, include_deleted: bool, key_col: str | None,
                  key_range: tuple[Any, Any] | None) -> list[dict[str, Any]]:
        """Projection + filter pushdown on the arrow dataset (row groups / files pruned by statistics)."""
        from alelt_loader.loader import _canon

        uri = self.uri(dest_name)
        if not self._exists(uri):
            return []
        ds = self._dt(uri).to_pyarrow_dataset()
        expr: Any = None
        if not include_deleted:
            expr = pc.field(self.c_deleted) == False  # noqa: E712
        if key_col and key_range:
            at = ds.schema.field(key_col).type
            lo, hi = key_range
            for v, ge in ((lo, True), (hi, False)):
                if v is not None:
                    c = pc.field(key_col) >= pa.scalar(_coerce_key(_canon(v), at), type=at) if ge else \
                        pc.field(key_col) <= pa.scalar(_coerce_key(_canon(v), at), type=at)
                    expr = c if expr is None else expr & c
        cols = columns if columns else None
        if columns == []:
            cols = [self.c_deleted]
        try:
            return list(ds.to_table(columns=cols, filter=expr).to_pylist())
        except (pa.ArrowNotImplementedError, pa.ArrowInvalid):      # string_view statistics: no pushdown
            return list(ds.to_table(columns=cols).to_pylist())

    def fetch_rows(self, dest_name: str) -> list[dict[str, Any]]:
        uri = self.uri(dest_name)
        if not self._exists(uri):
            return []
        return list(self._dt(uri).to_pyarrow_table().to_pylist())

    def audit_rows(self, stream: str) -> list[dict[str, Any]]:
        t = self._dt(self.curi("audit_batch")).to_pyarrow_dataset().to_table(filter=pc.field("stream") == stream)
        return sorted(t.to_pylist(), key=lambda r: r["seq"])

    def history(self, dest_name: str) -> list[dict[str, Any]]:
        """Delta commit history (commitInfo incl. alelt.* metadata)."""
        return list(self._dt(self.uri(dest_name)).history())


def _qi(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _ts_lit(ts: datetime) -> str:
    return f"CAST('{ts.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')}Z' AS TIMESTAMP)"


def _sql_lit(v: Any) -> str:
    """DataFusion SQL literal for a canonical key value (alelt_loader._canon form: str/int/float/bool)."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def _coerce_key(v: Any, at: pa.DataType) -> Any:
    """Canonical key (alelt_loader._canon form) -> arrow value of the key column type."""
    from datetime import date

    if v is None:
        return None
    if pa.types.is_integer(at):
        return int(v)
    if pa.types.is_decimal(at):
        return exact_decimal(str(v), at.scale)
    if pa.types.is_date(at) and isinstance(v, str):
        return date.fromisoformat(v)
    if pa.types.is_timestamp(at) and isinstance(v, str):
        d = datetime.fromisoformat(v)
        return d.replace(tzinfo=timezone.utc) if at.tz and d.tzinfo is None else d
    if pa.types.is_binary(at) and isinstance(v, str):
        return bytes.fromhex(v)
    return v
