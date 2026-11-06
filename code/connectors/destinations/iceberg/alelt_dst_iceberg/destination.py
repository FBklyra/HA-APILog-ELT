"""Apache Iceberg destination (pyiceberg). Any catalog pyiceberg supports; object store via URL properties.

URL (ALELT_DEST_URL):
  iceberg+rest://catalog:8181/?warehouse=s3://bucket/wh&s3.endpoint=http://minio:9000&s3.access-key-id=${K}
               &s3.secret-access-key=${S}&s3.path-style-access=true      (REST catalog; https=1 for TLS)
  iceberg+sql:///C:/lake?warehouse=file:///C:/lake/wh                    (SQLite catalog, local tests)
  (ADLS: adls.account-name=..; GCS: gcs.project-id=..) - every query parameter except the plugin's own
  (schema, control_schema, delete_mode, output_profile, case, https) is passed to the catalog as a property.

Layout: namespace <schema> holds the landing tables; namespace <control> holds `checkpoint` and `audit_batch`.

Exactly-once (Iceberg atomic commit):
- one pyiceberg transaction per table = ONE snapshot commit containing the copy-on-write upsert (delete matching
  keys + append new row versions) AND the table property `alelt.checkpoint.<stream>` = {seq, position, batch_id};
  snapshot summary carries alelt.stream / alelt.position / alelt.batch_id / alelt.seq;
- a table whose property already holds this batch (same seq + position) is skipped (crash after its commit); a
  property written by ANOTHER writer for this seq (other position) rejects the batch (CheckpointConflictError);
- compare-and-set (F-05): the stream checkpoint is re-read before the first table commit and again at the control
  table snapshot the final commit is based on; the catalog optimistic concurrency (commit against the loaded
  metadata / AssertRefSnapshotId) rejects a concurrent commit -> CheckpointConflictError (or no-op when the same
  batch won);
- the stream checkpoint (control table, incl. the per-table watermarks JSON) commits last. Atomicity is per table (pyiceberg has no multi-table
  commit): a table is either fully old or fully new after a crash, the retry finishes the rest exactly once.
"""
from __future__ import annotations

import contextlib
import json
import logging
import os
import warnings
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from alelt_common.models import ColumnInfo, Position, TableInfo
from alelt_common.spi import ApplyStats, CheckpointConflictError
from alelt_dst_kit import ControlState, KitDestination, TablePlan, physical_type, type_map
from alelt_dst_kit.base import AlreadyApplied, wm_loads
from alelt_dst_kit.arrowutil import arrow_type, coerce_key, from_arrow_value, to_arrow_value

log = logging.getLogger("alelt_dst_iceberg")
warnings.filterwarnings("ignore", message="Delete operation did not match any records")

_FALLBACK = {"types": {
    "BOOLEAN": "boolean", "SHORT": "int", "INT": "int", "LONG": "long", "DECIMAL": "decimal({p},{s})",
    "FLOAT": "float", "DOUBLE": "double", "LOCALDATE": "date", "LOCALTIME": "time", "LOCALDATETIME": "timestamp",
    "INSTANT": "timestamptz", "STRING": "string", "BINARY": "binary", "JSON": "string", "XML": "string"},
    "defaults": {"DECIMAL_without_precision": "decimal(38,0)"}, "limits": {"DECIMAL_max_precision": 38}}
# bounded metadata (DST-iceberg-02): old metadata.json files deleted on commit, snapshots expire after 1 day
TABLE_PROPS = {"format-version": "2", "write.metadata.delete-after-commit.enabled": "true",
               "write.metadata.previous-versions-max": "20", "history.expire.max-snapshot-age-ms": "86400000",
               "history.expire.min-snapshots-to-keep": "5"}
_OWN = {"schema", "control_schema", "delete_mode", "output_profile", "case", "https"}


def _canon_v(v: Any) -> Any:
    from alelt_loader.loader import _canon

    return _canon(v)


def iceberg_type(col: ColumnInfo) -> str:
    return physical_type(col, type_map("iceberg", _FALLBACK))


class IcebergDestination(KitDestination):
    engine = "iceberg"
    needs_full_rows = True

    def __init__(self, url: str = "", schema: str = "landing", control_schema: str = "alelt_control",
                 output_profile: str | None = None, compare: Any = None, catalog: Any = None, **kw: Any) -> None:
        super().__init__(url, schema, control_schema, output_profile, compare, **kw)
        self._catalog = catalog
        self._info: dict[str, TableInfo] = {}

    # ------------------------------------------------------------------ catalog
    def catalog_properties(self) -> dict[str, str]:
        u = self.u
        props = {k: v for k, v in u.params.items() if k not in _OWN}
        kind = u.scheme.split("+", 1)[1] if "+" in u.scheme else "rest"
        if kind == "rest":
            proto = "https" if u.flag("https") else "http"
            props.update({"type": "rest", "uri": f"{proto}://{u.hosts}{u.path.rstrip('/')}"})
        elif kind == "sql":
            p = u.path.lstrip("/") if len(u.path) > 2 and u.path[2] == ":" else u.path
            from pathlib import Path

            Path(p).mkdir(parents=True, exist_ok=True)
            props.update({"type": "sql", "uri": f"sqlite:///{Path(p).resolve().as_posix()}/catalog.db"})
            props.setdefault("warehouse", Path(p).resolve().as_posix())   # plain path: pyiceberg mis-parses file:///C:/ on Windows
        else:
            props["type"] = kind
        return props

    @property
    def catalog(self) -> Any:
        if self._catalog is None:
            from pyiceberg.catalog import load_catalog

            props = self.catalog_properties()
            if props.get("s3.endpoint"):
                # S3-compatible stores (SeaweedFS, older MinIO, ...) may store the aws-chunked checksum-trailer
                # framing of AWS SDK C++ >= 1.11.4xx (pyarrow) verbatim -> corrupt Avro/metadata files. With an
                # explicit endpoint, checksums only when required (an operator's own setting always wins).
                os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "WHEN_REQUIRED")
                os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "WHEN_REQUIRED")
            self._catalog = load_catalog("alelt", **props)
        return self._catalog

    def _id(self, dest: str) -> tuple[str, str]:
        return (self.schema, self.ident(dest))

    def _load(self, ident: tuple[str, str]) -> Any:
        return self.catalog.load_table(ident)

    def _exists(self, ident: tuple[str, str]) -> bool:
        return bool(self.catalog.table_exists(ident))

    # ------------------------------------------------------------------ DDL
    _CKPT = pa.schema([pa.field("stream", pa.string(), nullable=False), ("seq", pa.int64()),
                       ("position", pa.string()), ("batch_id", pa.string()),
                       ("updated_at", pa.timestamp("us", tz="UTC")), ("watermarks", pa.string())])
    _AUDIT = pa.schema([("batch_id", pa.string()), ("stream", pa.string()), ("seq", pa.int64()),
                        ("from_position", pa.string()), ("to_position", pa.string()), ("rows_by_table", pa.string()),
                        ("total_rows", pa.int64()), ("status", pa.string()),
                        ("applied_at", pa.timestamp("us", tz="UTC"))])

    def bootstrap(self) -> None:
        for ns in (self.schema, self.control):
            self.catalog.create_namespace_if_not_exists(ns)
        for name, sch in (("checkpoint", self._CKPT), ("audit_batch", self._AUDIT)):
            self.catalog.create_table_if_not_exists((self.control, name), schema=sch,
                                                    properties=TABLE_PROPS)
        ck = self._load((self.control, "checkpoint"))
        if "watermarks" not in {f.name for f in ck.schema().fields}:     # control table of an older release
            with ck.update_schema() as us:
                us.union_by_name(pa.schema([("watermarks", pa.string())]))

    def arrow_schema(self, table: TableInfo) -> pa.Schema:
        keyless = not table.key_columns()
        fields = [pa.field(self.ident(c.name), arrow_type(iceberg_type(c), timestamp_is_tz=False),
                           nullable=not c.key_seq) for c in self.data_cols(table, keyless)]
        fields += [pa.field(self.c_synced, pa.timestamp("us", tz="UTC"), nullable=False),
                   pa.field(self.c_deleted, pa.bool_(), nullable=False)]
        return pa.schema(fields)

    def ensure_table(self, table: TableInfo, dest_name: str, golden_ddl: str | None) -> None:
        ident = self._id(dest_name)
        self._info[self.ident(dest_name)] = table
        self.catalog.create_namespace_if_not_exists(self.schema)
        if not self._exists(ident):
            t = self.catalog.create_table_if_not_exists(ident, schema=self.arrow_schema(table),
                                                        properties=TABLE_PROPS)
            keys = [self.ident(k) for k in self.key_names(table)]
            with t.update_schema() as us:
                us.set_identifier_fields(*keys)
            return
        have = {f.name for f in self._load(ident).schema().fields}
        for c in table.columns:
            if self.ident(c.name) not in have:
                self.add_column(dest_name, c)
        self._widen(table, dest_name)

    def _widen(self, table: TableInfo, dest: str) -> None:
        """Type widening on drift (DST-kit-04): Iceberg allows int -> long and decimal(P,S) -> decimal(P',S) with
        P' > P (same scale); strings are unbounded. A scale change cannot be done in place: reported."""
        from pyiceberg.types import DecimalType, IntegerType, LongType

        t = self._load(self._id(dest))
        fields = {f.name: f for f in t.schema().fields}
        todo = []
        for c in table.columns:
            f = fields.get(self.ident(c.name))
            if f is None:
                continue
            want = iceberg_type(c)
            if isinstance(f.field_type, DecimalType) and want.startswith("decimal("):
                p, s = (int(x) for x in want[8:-1].split(","))
                if p > f.field_type.precision and s == f.field_type.scale:
                    todo.append((f.name, DecimalType(p, s)))
                elif s != f.field_type.scale and (p, s) != (f.field_type.precision, f.field_type.scale):
                    log.warning("iceberg cannot change the scale of %s.%s (%s -> %s)", dest, f.name, f.field_type, want)
            elif isinstance(f.field_type, IntegerType) and want == "long":
                todo.append((f.name, LongType()))
        if todo:
            with t.update_schema() as us:
                for name, typ in todo:
                    us.update_column(name, field_type=typ)

    def add_column(self, dest_name: str, col: ColumnInfo) -> None:
        from pyiceberg.io.pyarrow import pyarrow_to_schema  # noqa: F401  (ensures pyarrow IO is importable)

        t = self._load(self._id(dest_name))
        if self.ident(col.name) in {f.name for f in t.schema().fields}:
            return
        with t.update_schema() as us:
            us.union_by_name(pa.schema([pa.field(self.ident(col.name),
                                                 arrow_type(iceberg_type(col), timestamp_is_tz=False))]))

    # ------------------------------------------------------------------ control
    def control_state(self, stream: str) -> ControlState:
        from pyiceberg.expressions import EqualTo

        ident = (self.control, "checkpoint")
        if not self._exists(ident):
            return ControlState()
        rows = self._load(ident).scan(row_filter=EqualTo("stream", stream)).to_arrow().to_pylist()
        if not rows:
            return ControlState()
        r = max(rows, key=lambda x: x["seq"])
        return ControlState(r["position"] or None, int(r["seq"]), r["batch_id"], wm_loads(r.get("watermarks")))

    def delete_checkpoint(self, stream: str) -> None:
        from pyiceberg.expressions import EqualTo

        ck = self._load((self.control, "checkpoint"))
        with ck.transaction() as tx:
            tx.delete(EqualTo("stream", stream))

    def maintain(self, dest_names: list[str] | None = None, older_than_ms: int = 86_400_000) -> dict[str, str]:
        """Periodic maintenance (worker job): expire snapshots older than `older_than_ms` on landing + control
        tables. Returns {table: 'ok' | 'unsupported' | error}."""
        out: dict[str, str] = {}
        idents = [self._id(n) for n in (dest_names or list(self._info))] + [(self.control, "checkpoint"),
                                                                          (self.control, "audit_batch")]
        for ident in idents:
            try:
                t = self._load(ident)
                exp = getattr(getattr(t, "maintenance", None), "expire_snapshots", None)
                if exp is None:
                    out[".".join(ident)] = "unsupported"
                    continue
                from datetime import timedelta as _td, timezone as _tz
                exp().older_than(datetime.now(_tz.utc) - _td(milliseconds=older_than_ms)).commit()
                out[".".join(ident)] = "ok"
            except Exception as e:  # noqa: BLE001 - maintenance is best effort, reported
                out[".".join(ident)] = f"{type(e).__name__}: {e}"[:200]
        return out

    @staticmethod
    def table_marker(t: Any, stream: str) -> tuple[int, str | None, str | None]:
        """(seq, position, batch_id) of the last batch of `stream` committed into table `t` (table property)."""
        raw = t.properties.get(f"alelt.checkpoint.{stream}")
        if not raw:
            return 0, None, None
        m = json.loads(raw)
        return int(m["seq"]), m.get("position"), m.get("batch_id")

    @classmethod
    def table_seq(cls, t: Any, stream: str) -> int:
        return cls.table_marker(t, stream)[0]

    # ------------------------------------------------------------------ apply
    def _key_filter(self, pl: TablePlan, keys: list[tuple[Any, ...]], sch: pa.Schema) -> Any:
        from pyiceberg.expressions import And, EqualTo, In, Or

        kc = [self.ident(k) for k in pl.keys]
        if len(kc) == 1:
            at = sch.field(kc[0]).type
            return In(kc[0], {coerce_key(k[0], at) for k in keys})
        expr: Any = None
        for k in keys:
            parts = [EqualTo(c, coerce_key(v, sch.field(c).type)) for c, v in zip(kc, k)]
            term = parts[0]
            for x in parts[1:]:
                term = And(term, x)
            expr = term if expr is None else Or(expr, term)
        return expr

    def lookup(self, plan: TablePlan, keys: list[tuple[Any, ...]]) -> dict[tuple[Any, ...], dict[str, Any]]:
        ident = self._id(plan.dest)
        if not keys or not self._exists(ident):
            return {}
        from alelt_loader.loader import _canon

        t = self._load(ident)
        sch = self.arrow_schema(self._info.get(self.ident(plan.dest)) or plan.info)
        types = {c.name: c.canonical_type for c in plan.info.columns}
        out: dict[tuple[Any, ...], dict[str, Any]] = {}
        for i in range(0, len(keys), 500):
            for r in t.scan(row_filter=self._key_filter(plan, keys[i:i + 500], sch)).to_arrow().to_pylist():
                rec = {c: from_arrow_value(r.get(self.ident(c)), types.get(c)) for c in plan.columns}
                out[tuple(_canon(rec[c]) for c in plan.keys)] = rec
        return out

    def _rows_table(self, pl: TablePlan, sch: pa.Schema, ts: datetime, ops: list[Any]) -> pa.Table:
        types = {c.name: c.canonical_type for c in pl.info.columns}
        src = {self.ident(c): c for c in pl.columns}
        cols: dict[str, list[Any]] = {f.name: [] for f in sch}
        for op in ops:
            for f in sch:
                if f.name == self.c_synced:
                    v: Any = ts
                elif f.name == self.c_deleted:
                    v = op.op == "D"
                else:
                    c = src.get(f.name)
                    v = to_arrow_value(op.row.get(c) if c else None, types.get(c or ""), f.type)
                cols[f.name].append(v)
        return pa.Table.from_arrays([pa.array(cols[f.name], type=f.type) for f in sch], schema=sch)

    def commit_batch(self, stream: str | None, plans: list[TablePlan], to_pos: Position | None, batch_id: str,
                     prev: ControlState, ts: datetime, stats: dict[str, ApplyStats]) -> None:
        from pyiceberg.expressions import AlwaysTrue

        seq = prev.seq + 1
        if stream and to_pos:                       # compare-and-set before the first table commit (F-05)
            self.cas(stream, prev, self.control_state(stream), to_pos)
        snap = ({"alelt.stream": stream, "alelt.position": to_pos.text, "alelt.batch_id": batch_id, "alelt.seq": str(seq)}
                if stream and to_pos else {"alelt.snapshot_load": "true"})
        for i, pl in enumerate(plans):
            self.count(pl, stats[pl.dest])
            t = self._load(self._id(pl.dest))
            if stream and to_pos and self.table_decision(stream, prev, to_pos, batch_id,
                                                         *self.table_marker(t, stream)) == "skip":
                log.info("table %s already holds %s seq %s - skipped", pl.dest, stream, seq)
                continue
            sch = t.schema().as_arrow()
            sch = pa.schema([pa.field(f.name, f.type, nullable=f.nullable) for f in sch])
            writes = [o for o in pl.ops.values() if not (o.op == "D" and self.hard)]
            with self._commit_guard(stream, prev, to_pos), t.transaction() as tx:
                if pl.truncate:                  # DST-iceberg-03: one scan (soft), none at all (hard)
                    if self.hard:
                        cur = sch.empty_table()
                    else:
                        cur = t.scan().to_arrow()
                        live = pc.invert(cur[self.c_deleted])
                        old_syn = cur[self.c_synced]
                        cur = cur.set_column(cur.schema.get_field_index(self.c_deleted), self.c_deleted,
                                             pa.array([True] * len(cur), type=pa.bool_()))
                        syn = pa.array([ts] * len(cur), type=sch.field(self.c_synced).type)
                        cur = cur.set_column(cur.schema.get_field_index(self.c_synced), self.c_synced,
                                             pc.if_else(live, syn, old_syn))
                    if pl.ops:                                   # rows re-written by this batch replace them
                        kc = [self.ident(k) for k in pl.keys]
                        from alelt_loader.loader import _canon
                        mine = set(pl.ops)
                        keep = [tuple(_canon(r[c]) for c in kc) not in mine for r in cur.select(kc).to_pylist()]
                        cur = cur.filter(pa.array(keep, type=pa.bool_()))
                    new = pa.concat_tables([cur.cast(sch), self._rows_table(pl, sch, ts, writes)])
                    tx.overwrite(new, overwrite_filter=AlwaysTrue(), snapshot_properties=snap)
                elif pl.ops:
                    tx.delete(self._key_filter(pl, list(pl.ops), sch), snapshot_properties=snap)
                    if writes:
                        tx.append(self._rows_table(pl, sch, ts, writes), snapshot_properties=snap)
                if stream and to_pos:
                    tx.set_properties({f"alelt.checkpoint.{stream}": json.dumps(
                        {"seq": seq, "position": to_pos.text, "batch_id": batch_id})})
            if i == 0:
                self._hook("after_data")
        self._hook("before_commit")
        if stream is None or to_pos is None:
            return
        from pyiceberg.expressions import EqualTo

        ck = self._load((self.control, "checkpoint"))
        rows = ck.scan(row_filter=EqualTo("stream", stream)).to_arrow().to_pylist()
        r = max(rows, key=lambda x: x["seq"]) if rows else None
        found = ControlState(r["position"] or None, int(r["seq"]), r["batch_id"]) if r else ControlState()
        self.cas(stream, prev, found, to_pos)       # at the snapshot the commit point is based on (optimistic)
        audit = self.audit_record(stream, prev, to_pos, batch_id, stats)
        at = self._load((self.control, "audit_batch"))
        if self.table_seq(at, stream) < seq:
            row = {**{k: audit[k] for k in ("batch_id", "stream", "seq", "from_position", "to_position",
                                            "rows_by_table", "total_rows", "status")}, "applied_at": ts}
            with at.transaction() as tx:
                tx.append(pa.Table.from_pylist([row], schema=self._AUDIT), snapshot_properties=snap)
                tx.set_properties({f"alelt.checkpoint.{stream}": json.dumps({"seq": seq, "position": to_pos.text})})
        with self._commit_guard(stream, prev, to_pos), ck.transaction() as tx:     # stream COMMIT POINT
            tx.delete(EqualTo("stream", stream), snapshot_properties=snap)
            tx.append(pa.Table.from_pylist([{"stream": stream, "seq": seq, "position": to_pos.text,
                                             "batch_id": batch_id, "updated_at": ts, "watermarks": self.wm_text()}],
                                           schema=ck.schema().as_arrow()), snapshot_properties=snap)

    @contextlib.contextmanager
    def _commit_guard(self, stream: str | None, prev: ControlState, to_pos: Position | None) -> Iterator[None]:
        """Catalog optimistic concurrency: a commit based on a stale table snapshot fails (CommitFailedException)
        -> the same batch committed meanwhile (no-op) or a conflicting writer (CheckpointConflictError)."""
        try:
            yield
        except Exception as e:
            if stream is None or to_pos is None or type(e).__name__ not in ("CommitFailedException",
                                                                            "CommitStateUnknownException"):
                raise
            found = self.control_state(stream)
            if self.pos_ge(found.position, to_pos):
                raise AlreadyApplied from e
            raise CheckpointConflictError(stream, prev.position, found.position,
                                          f"iceberg commit rejected: {e}") from e

    # ------------------------------------------------------------------ verify
    def scan_rows(self, dest_name: str, columns: list[str] | None, include_deleted: bool, key_col: str | None,
                  key_range: tuple[Any, Any] | None) -> list[dict[str, Any]]:
        """Projection + predicate pushdown (pyiceberg prunes data files by column statistics)."""
        from pyiceberg.expressions import AlwaysTrue, And, EqualTo, GreaterThanOrEqual, LessThanOrEqual

        ident = self._id(dest_name)
        if not self._exists(ident):
            return []
        t = self._load(ident)
        sch = t.schema().as_arrow()
        expr: Any = AlwaysTrue()
        if not include_deleted:
            expr = And(expr, EqualTo(self.c_deleted, False))
        if key_col and key_range:
            at = sch.field(key_col).type
            lo, hi = key_range
            if lo is not None:
                expr = And(expr, GreaterThanOrEqual(key_col, coerce_key(_canon_v(lo), at)))
            if hi is not None:
                expr = And(expr, LessThanOrEqual(key_col, coerce_key(_canon_v(hi), at)))
        sel = tuple(columns) if columns else ("*",)
        if columns == []:
            sel = (self.c_deleted,)
        return list(t.scan(row_filter=expr, selected_fields=sel).to_arrow().to_pylist())

    def fetch_rows(self, dest_name: str) -> list[dict[str, Any]]:
        ident = self._id(dest_name)
        if not self._exists(ident):
            return []
        return list(self._load(ident).scan().to_arrow().to_pylist())

    def audit_rows(self, stream: str) -> list[dict[str, Any]]:
        from pyiceberg.expressions import EqualTo

        rows = self._load((self.control, "audit_batch")).scan(row_filter=EqualTo("stream", stream)).to_arrow()
        return sorted(rows.to_pylist(), key=lambda r: r["seq"])

    def snapshots(self, dest_name: str) -> list[dict[str, Any]]:
        """Snapshot log with summaries (alelt.* properties)."""
        t = self._load(self._id(dest_name))
        return [{"snapshot_id": s.snapshot_id, **(s.summary.additional_properties if s.summary else {})}
                for s in t.metadata.snapshots]
