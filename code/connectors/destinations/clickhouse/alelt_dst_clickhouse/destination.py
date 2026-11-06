"""ClickHouse destination: ReplacingMergeTree(_alelt_ver, _alelt_is_deleted) landing tables, checkpoint table,
stage-then-commit so a crash mid-apply never exposes a partial batch.

URL: clickhouse://user:password@host:8123/?secure=0   (password may be ${ENV}; database per landing schema)

Layout
- `<schema>.<table>`: source columns + `<synced> DateTime64(6,'UTC')`, `<deleted> Bool` (output profile) +
  `_alelt_ver UInt64` (= version line << 24 | op ordinal; the version line is ONE counter for all streams of the
  control schema + snapshots, stored as checkpoint.ver / <control>.version, DST-clickhouse-08) + `_alelt_is_deleted
  UInt8` (hard deletes / hard truncate).
  ENGINE ReplacingMergeTree(_alelt_ver, _alelt_is_deleted) ORDER BY <primary key> (keyless: profile id column).
  Readers use `FINAL` (or the `<table>_current` view) - latest version per key, hard-deleted rows hidden.
- `<control>.checkpoint` ReplacingMergeTree(seq) ORDER BY stream; `<control>.audit_batch` MergeTree.
- `<control>.stg_<schema>_<table>_<streamhash>`: per-stream staging copy (MergeTree) of the batch rows.

apply (exactly-once)
1. recover: stage rows with seq <= committed seq are rolled forward into the target (crash after commit),
   stage rows with seq > committed are discarded (crash before commit).
2. batch rows -> stage tables (never read by consumers).
3. COMMIT POINT: one INSERT of the checkpoint row (seq+1, to_pos) + audit row.
4. move stage -> target (INSERT SELECT, idempotent: same version + same content dedups) and truncate stage.
A replay of a committed batch is a no-op (checkpoint >= to_pos); re-moving rows is harmless (same _alelt_ver).
Committed-but-unmoved stage rows (crash after step 3) are rolled forward whenever the control state is read
(apply, read_checkpoint), not only by the next batch.
Limitation: during step 4 tables become visible one by one (each INSERT is atomic per table).

Exactly-once = VERSION-BASED IDEMPOTENCE, not a transaction (ClickHouse has no multi-statement transactions and no
conditional INSERT): a replayed batch writes identical rows (same _alelt_ver = ver << 24 | ordinal) that
ReplacingMergeTree collapses. Compare-and-set (F-05) is best effort: the checkpoint is re-read (FINAL) right before
the commit-point INSERT and must equal the state the batch was planned from (else CheckpointConflictError, nothing
committed); after the INSERT the rows of this seq are read back and a second writer of the same seq is REPORTED
(CheckpointConflictError, stage not moved). Two appliers racing inside the few milliseconds between re-read and
INSERT cannot be excluded, so ONE writer per (control schema, stream) is ENFORCED (DST-clickhouse-01):
- apply() of a stream refuses (WriterFenceError, nothing written) unless the caller holds a live hub lock covering
  the stream (`alelt_common.spi.writer_fence`: the hub registers `pipeline:<cid>` / `replay:<cid>` when it takes them);
  checked before staging and again right before the commit point;
- `<control>.writer` (stream, writer, epoch) records the current writer: a new writer (lock taken over) claims the
  stream with epoch + 1; a writer that finds another writer's claim at the commit point aborts (fenced out).
`?single_writer=off` disables the check for tools that own the database exclusively (logged as a warning).
Out-of-range dates (Date32 / DateTime64: 1900-01-01 .. 2299-12-31) are rejected (ValueError), never clamped.
"""
from __future__ import annotations

import hashlib
import re
import json
import logging
from datetime import date, datetime, time as dtime, timezone
from decimal import Decimal
from typing import Any

from alelt_common.models import CanonicalType, ColumnInfo, Position, TableInfo
from alelt_common.spi import ApplyStats, CheckpointConflictError, WriterFenceError, require_writer
from alelt_dst_kit import ControlState, KitDestination, TablePlan, physical_type, type_map
from alelt_dst_kit.base import wm_loads

log = logging.getLogger("alelt_dst_clickhouse")

ORD_BITS = 24
_FALLBACK = {"types": {
    "BOOLEAN": "Bool", "SHORT": "Int16", "INT": "Int32", "LONG": "Int64", "DECIMAL": "Decimal({p},{s})",
    "FLOAT": "Float32", "DOUBLE": "Float64", "LOCALDATE": "Date32", "LOCALTIME": "String",
    "LOCALDATETIME": "DateTime64(6, 'UTC')", "INSTANT": "DateTime64(6, 'UTC')", "STRING": "String", "BINARY": "String",
    "JSON": "String", "XML": "String"}, "defaults": {"DECIMAL_without_precision": "Decimal(38,0)"},
    "limits": {"DECIMAL_max_precision": 76}}


def q(name: str) -> str:
    return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`"


def ch_type(col: ColumnInfo, nullable: bool = True) -> str:
    t = physical_type(col, type_map("clickhouse", _FALLBACK))
    return f"Nullable({t})" if nullable and col.nullable and not col.key_seq else t


class ClickHouseDestination(KitDestination):
    engine = "clickhouse"
    needs_full_rows = True

    def __init__(self, url: str = "", schema: str = "landing", control_schema: str = "alelt_control",
                 output_profile: str | None = None, compare: Any = None, client: Any = None, **kw: Any) -> None:
        super().__init__(url, schema, control_schema, output_profile, compare, **kw)
        self._client = client
        self._tables: dict[str, TableInfo] = {}
        import os
        import socket
        import uuid

        self.writer_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:12]}"
        self.single_writer = str(self.u.get("single_writer") or "on").lower() not in ("off", "0", "false", "no")
        # DST-clickhouse-04: clusters. ?cluster=<name> adds ON CLUSTER to every DDL of landing / control objects;
        # ?replicated=on uses Replicated*MergeTree engines (default replica path / macros of the server config).
        # Per-writer stage tables stay local to the node the writer is connected to.
        self.cluster = str(self.u.get("cluster") or "").strip() or None
        self.replicated = str(self.u.get("replicated") or "off").lower() in ("on", "1", "true", "yes")
        if not self.single_writer:
            log.warning("clickhouse: single-writer enforcement disabled (single_writer=off)")

    # ------------------------------------------------------------------ plumbing
    @property
    def client(self) -> Any:
        if self._client is None:
            import clickhouse_connect

            u = self.u
            self._client = clickhouse_connect.get_client(
                host=u.host or "localhost", port=u.port or 8123, username=u.user or "default",
                password=u.password or "", secure=u.flag("secure"), connect_timeout=10,
                send_receive_timeout=int(u.get("timeout") or 300),
                settings={"mutations_sync": 2})
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def _cmd(self, sql: str, **kw: Any) -> Any:
        return self.client.command(self.cluster_sql(sql), **kw)

    _DDL_OBJ = re.compile(r"^(CREATE DATABASE IF NOT EXISTS|CREATE TABLE IF NOT EXISTS|CREATE VIEW IF NOT EXISTS|"
                          r"CREATE OR REPLACE VIEW|ALTER TABLE|TRUNCATE TABLE|DROP TABLE IF EXISTS|DROP VIEW IF EXISTS|"
                          r"DROP DATABASE IF EXISTS|DELETE FROM)\s+((?:`[^`]*`|\w+)(?:\.(?:`[^`]*`|\w+))?)")

    def cluster_sql(self, sql: str) -> str:
        """ON CLUSTER / Replicated* engines for landing and control objects (DST-clickhouse-04)."""
        if not (self.cluster or self.replicated):
            return sql
        m = self._DDL_OBJ.match(sql)
        if m is None or "`stg_" in m.group(2):               # not DDL, or a per-writer stage table (local)
            return sql
        out = sql
        if self.replicated and m.group(1).startswith("CREATE TABLE"):
            out = re.sub(r"ENGINE = (Replacing)?MergeTree", lambda x: f"ENGINE = Replicated{x.group(1) or ''}MergeTree",
                         out, count=1)
        if self.cluster and not m.group(1).startswith("DELETE"):
            end = m.end(2)
            out = out[:end] + f" ON CLUSTER {q(self.cluster)}" + out[end:]
        return out

    def _rows(self, sql: str, params: dict[str, Any] | None = None) -> tuple[list[str], list[tuple[Any, ...]]]:
        r = self.client.query(sql, parameters=params or {})
        return list(r.column_names), [tuple(x) for x in r.result_rows]

    def t(self, dest: str) -> str:
        return f"{q(self.schema)}.{q(self.ident(dest))}"

    def c(self, name: str) -> str:
        return f"{q(self.control)}.{q(name)}"

    def stage_name(self, stream: str, dest: str) -> str:
        h = hashlib.sha1(stream.encode()).hexdigest()[:8]
        return f"stg_{self.schema}_{self.ident(dest)}_{h}"

    # ------------------------------------------------------------------ DDL
    def bootstrap(self) -> None:
        for db in (self.schema, self.control):
            self._cmd(f"CREATE DATABASE IF NOT EXISTS {q(db)}")
        self._cmd(f"CREATE TABLE IF NOT EXISTS {self.c('checkpoint')} (stream String, seq UInt64, position String, "
                  "batch_id String, updated_at DateTime64(6, 'UTC'), watermarks String DEFAULT '') "
                  "ENGINE = ReplacingMergeTree(seq) ORDER BY stream")
        self._cmd(f"ALTER TABLE {self.c('checkpoint')} ADD COLUMN IF NOT EXISTS watermarks String DEFAULT ''")
        self._cmd(f"ALTER TABLE {self.c('checkpoint')} ADD COLUMN IF NOT EXISTS ver UInt64 DEFAULT 0")
        self._cmd(f"CREATE TABLE IF NOT EXISTS {self.c('version')} (k String, ver UInt64) "
                  "ENGINE = ReplacingMergeTree(ver) ORDER BY k")
        self._cmd(f"CREATE TABLE IF NOT EXISTS {self.c('writer')} (stream String, writer String, epoch UInt64, "
                  "claimed_at DateTime64(6, 'UTC')) ENGINE = ReplacingMergeTree(epoch) ORDER BY stream")
        self._cmd(f"CREATE TABLE IF NOT EXISTS {self.c('audit_batch')} (batch_id String, stream String, seq UInt64, "
                  "from_position Nullable(String), to_position String, rows_by_table String, total_rows UInt64, "
                  "duration_ms UInt64, status String, applied_at DateTime64(6, 'UTC')) "
                  "ENGINE = ReplacingMergeTree ORDER BY (stream, seq)")

    def _col_defs(self, table: TableInfo) -> tuple[list[str], list[str]]:
        keys = [self.ident(k) for k in self.key_names(table)]
        keyless = not table.key_columns()
        defs = [f"{q(self.ident(c.name))} {ch_type(c)}" for c in self.data_cols(table, keyless)]
        defs += [f"{q(self.c_synced)} DateTime64(6, 'UTC')", f"{q(self.c_deleted)} Bool DEFAULT false",
                 "`_alelt_ver` UInt64", "`_alelt_is_deleted` UInt8 DEFAULT 0"]
        return defs, keys

    def ensure_table(self, table: TableInfo, dest_name: str, golden_ddl: str | None) -> None:
        if golden_ddl:
            self._cmd(golden_ddl)                       # trusted config (R8)
        self._cmd(f"CREATE DATABASE IF NOT EXISTS {q(self.schema)}")
        defs, keys = self._col_defs(table)
        self._cmd(f"CREATE TABLE IF NOT EXISTS {self.t(dest_name)} ({', '.join(defs)}) "
                  f"ENGINE = ReplacingMergeTree(_alelt_ver, _alelt_is_deleted) ORDER BY ({', '.join(map(q, keys))})")
        self._cmd(f"CREATE VIEW IF NOT EXISTS {q(self.schema)}.{q(self.ident(dest_name) + '_current')} AS "
                  f"SELECT * EXCEPT (_alelt_ver, _alelt_is_deleted) FROM {self.t(dest_name)} FINAL "
                  "WHERE _alelt_is_deleted = 0")
        for c in table.columns:                        # additive drift
            self.add_column(dest_name, c)
        self._widen(table, dest_name)
        self._tables[self.ident(dest_name)] = table

    def _widen(self, table: TableInfo, dest: str) -> None:
        """Type widening on drift (DST-kit-04): Decimal / integer columns MODIFY'ed in place (String is unbounded)."""
        from alelt_dst_kit.widen import widen_to

        _, rows = self._rows("SELECT name, type FROM system.columns WHERE database = {d:String} AND table = {t:String}",
                             {"d": self.schema, "t": self.ident(dest)})
        have = {n: t for n, t in rows}
        for c in table.columns:
            cur = have.get(self.ident(c.name))
            if not cur:
                continue
            base = cur[len("Nullable("):-1] if cur.startswith("Nullable(") else cur
            target = physical_type(c, type_map("clickhouse", _FALLBACK))
            to = widen_to(base.replace(", ", ","), target)
            if to:
                typ = f"Nullable({to})" if cur.startswith("Nullable(") else to
                self._cmd(f"ALTER TABLE {self.t(dest)} MODIFY COLUMN {q(self.ident(c.name))} {typ}")
                for (name,) in self._rows("SELECT name FROM system.tables WHERE database = {d:String} AND "
                                          "startsWith(name, {p:String})",
                                          {"d": self.control, "p": f"stg_{self.schema}_{self.ident(dest)}_"})[1]:
                    self._cmd(f"ALTER TABLE {self.c(name)} MODIFY COLUMN {q(self.ident(c.name))} {typ}")

    def add_column(self, dest_name: str, col: ColumnInfo) -> None:
        self._cmd(f"ALTER TABLE {self.t(dest_name)} ADD COLUMN IF NOT EXISTS {q(self.ident(col.name))} {ch_type(col)}")
        for (name,) in self._rows("SELECT name FROM system.tables WHERE database = {d:String} AND "
                                  "startsWith(name, {p:String})",
                                  {"d": self.control, "p": f"stg_{self.schema}_{self.ident(dest_name)}_"})[1]:
            self._cmd(f"ALTER TABLE {self.c(name)} ADD COLUMN IF NOT EXISTS {q(self.ident(col.name))} {ch_type(col)}")

    def _ensure_stage(self, stream: str, dest: str) -> str:
        name = self.stage_name(stream, dest)
        self._cmd(f"CREATE TABLE IF NOT EXISTS {self.c(name)} AS {self.t(dest)} ENGINE = MergeTree ORDER BY tuple() "
                  f"COMMENT {_lit(self.ident(dest))}")
        self._cmd(f"ALTER TABLE {self.c(name)} ADD COLUMN IF NOT EXISTS _alelt_stream String, "
                  "ADD COLUMN IF NOT EXISTS _alelt_seq UInt64")
        # DST-clickhouse-05: a stage created before a golden-DDL / drifted target gets every target column
        _, tcols = self._rows("SELECT name, type FROM system.columns WHERE database = {d:String} AND "
                              "table = {t:String} ORDER BY position", {"d": self.schema, "t": self.ident(dest)})
        _, scols = self._rows("SELECT name FROM system.columns WHERE database = {d:String} AND table = {t:String}",
                              {"d": self.control, "t": name})
        have = {r[0] for r in scols}
        for n, typ in tcols:
            if n not in have:
                self._cmd(f"ALTER TABLE {self.c(name)} ADD COLUMN IF NOT EXISTS {q(n)} {typ}")
        return name

    # ------------------------------------------------------------------ control
    def delete_checkpoint(self, stream: str) -> None:
        self._cmd(f"DELETE FROM {self.c('checkpoint')} WHERE stream = {_lit(stream)}")

    def _read_state(self, stream: str) -> ControlState:
        _, rows = self._rows(f"SELECT seq, position, batch_id, watermarks FROM {self.c('checkpoint')} FINAL "
                             "WHERE stream = {s:String}", {"s": stream})
        if not rows:
            return ControlState()
        seq, pos, bid, wm = rows[0]
        return ControlState(pos or None, int(seq), bid or None, wm_loads(wm))

    def control_state(self, stream: str) -> ControlState:
        st = self._read_state(stream)
        if st.seq:
            self._recover(stream, st, discard=False)     # DST-clickhouse-02: committed rows become visible now
        return st

    def _target_cols(self, dest: str) -> list[str]:
        _, rows = self._rows("SELECT name FROM system.columns WHERE database = {d:String} AND table = {t:String} "
                             "ORDER BY position", {"d": self.schema, "t": self.ident(dest)})
        return [r[0] for r in rows]

    def _recover(self, stream: str, prev: ControlState, discard: bool = True) -> None:
        """Roll committed stage rows (seq <= committed) forward; discard=True also drops uncommitted ones (only by
        the writer about to stage its own batch)."""
        h = hashlib.sha1(stream.encode()).hexdigest()[:8]
        _, stages = self._rows("SELECT name, comment FROM system.tables WHERE database = {d:String} AND "
                               "startsWith(name, {p:String}) AND endsWith(name, {h:String})",
                               {"d": self.control, "p": f"stg_{self.schema}_", "h": f"_{h}"})
        for name, target in stages:
            _, r = self._rows(f"SELECT count(), max(_alelt_seq) FROM {self.c(name)}")
            n, mx = r[0]
            if not n:
                continue
            if int(mx) <= prev.seq:
                log.warning("rolling forward committed stage %s (seq %s)", name, mx)
                self._move(name, target)
                self._cmd(f"TRUNCATE TABLE {self.c(name)}")
            elif discard:
                self._cmd(f"TRUNCATE TABLE {self.c(name)}")

    def _move(self, stage: str, dest: str) -> None:
        _, sc = self._rows("SELECT name FROM system.columns WHERE database = {d:String} AND table = {t:String}",
                           {"d": self.control, "t": stage})
        have = {r[0] for r in sc}
        cols = ", ".join(q(c) for c in self._target_cols(dest) if c in have)    # target defaults for the rest
        self._cmd(f"INSERT INTO {self.t(dest)} ({cols}) SELECT {cols} FROM {self.c(stage)}")

    # ------------------------------------------------------------------ apply
    def lookup(self, plan: TablePlan, keys: list[tuple[Any, ...]]) -> dict[tuple[Any, ...], dict[str, Any]]:
        if not keys:
            return {}
        from alelt_loader.loader import _canon

        kc = [self.ident(k) for k in plan.keys]
        cols = [self.ident(c) for c in plan.columns]
        out: dict[tuple[Any, ...], dict[str, Any]] = {}
        types = {c.name: c.canonical_type for c in plan.info.columns}
        for i in range(0, len(keys), 1000):
            chunk = keys[i:i + 1000]
            kts = [types.get(c) for c in plan.keys]
            lits = ", ".join("(" + ", ".join(_lit(_typed_key(v, t)) for v, t in zip(k, kts)) + ")" for k in chunk)
            names, rows = self._rows(self.lookup_sql(plan.dest, cols, kc, lits))
            for r in rows:
                if r[-1]:                                       # newest version is a hard delete
                    continue
                rec = {src: _from_ch(v, types.get(src)) for src, v in zip(plan.columns, r[:-1])}
                out[tuple(_canon(rec[k]) for k in plan.keys)] = rec
        return out

    def lookup_sql(self, dest: str, cols: list[str], kc: list[str], lits: str) -> str:
        """Current row per key WITHOUT `FINAL` (DST-clickhouse-06): the key filter prunes by the primary key, then
        the newest version per key wins (`ORDER BY _alelt_ver DESC LIMIT 1 BY key`) - only the granules of the
        touched keys are read, never a merge-on-read of every part. Last column = _alelt_is_deleted."""
        where = (f"tuple({', '.join(map(q, kc))}) IN ({lits})" if len(kc) > 1 else f"{q(kc[0])} IN ({lits})")
        return (f"SELECT {', '.join(map(q, cols))}, _alelt_is_deleted FROM {self.t(dest)} WHERE {where} "
                f"ORDER BY _alelt_ver DESC LIMIT 1 BY {', '.join(map(q, kc))}")

    def commit_batch(self, stream: str | None, plans: list[TablePlan], to_pos: Position | None, batch_id: str,
                     prev: ControlState, ts: datetime, stats: dict[str, ApplyStats]) -> None:
        if stream is None:                                  # snapshot: direct insert, version above all commits
            base = self._next_ver()                         # DST-clickhouse-08: one version line for all streams
            self.client.insert(self.c("version"), [["v", base]], column_names=["k", "ver"])
            for pl in plans:
                self._insert(self.t(pl.dest), pl, self._records(pl, base, ts), None)
                self.count(pl, stats[pl.dest])
            return
        assert to_pos is not None
        epoch = self._claim_writer(stream)                                # single writer (DST-clickhouse-01)
        self.cas(stream, prev, self._read_state(stream), to_pos)          # CAS before staging anything
        self._recover(stream, prev)
        seq = prev.seq + 1
        ver = self._next_ver()                # row versions: above every commit/snapshot of ANY stream (replays)
        stages: list[tuple[str, str]] = []
        for pl in plans:
            name = self._ensure_stage(stream, pl.dest)
            stages.append((name, pl.dest))
            if pl.truncate:
                self._stage_truncate(name, pl, seq, ts, stream, ver)
            self._insert(self.c(name), pl, self._records(pl, ver, ts), (stream, seq))
            self.count(pl, stats[pl.dest])
        self._hook("after_data")
        self._hook("before_commit")
        try:
            self._check_writer(stream, epoch)                             # still the fenced writer?
            self.cas(stream, prev, self._read_state(stream), to_pos)      # re-check right before the commit point
        except BaseException:
            for name, _ in stages:                                        # loser: never leave rows to roll forward
                self._cmd(f"TRUNCATE TABLE {self.c(name)}")
            raise
        audit = self.audit_record(stream, prev, to_pos, batch_id, stats)
        self.client.insert(self.c("checkpoint"), [[stream, seq, to_pos.text, batch_id, ts, self.wm_text(), ver]],
                           column_names=["stream", "seq", "position", "batch_id", "updated_at", "watermarks", "ver"],
                           settings={"insert_deduplication_token": f"{stream}:{seq}:{to_pos.text}:{batch_id}"})
        _, same = self._rows(f"SELECT DISTINCT position, batch_id FROM {self.c('checkpoint')} "
                             "WHERE stream = {s:String} AND seq = {q:UInt64}", {"s": stream, "q": seq})
        if len(same) > 1:                                                 # another writer committed this seq too
            for name, _ in stages:
                self._cmd(f"TRUNCATE TABLE {self.c(name)}")
            raise CheckpointConflictError(stream, prev.position, ",".join(str(x[0]) for x in same),
                                          f"two writers committed seq {seq} (version-based idempotence only)")
        self.client.insert(self.c("audit_batch"), [[audit["batch_id"], stream, seq, audit["from_position"],
                                                    audit["to_position"], audit["rows_by_table"], audit["total_rows"],
                                                    0, "APPLIED", ts]],
                           column_names=["batch_id", "stream", "seq", "from_position", "to_position", "rows_by_table",
                                         "total_rows", "duration_ms", "status", "applied_at"])
        for name, dest in stages:
            self._move(name, dest)
            self._cmd(f"TRUNCATE TABLE {self.c(name)}")

    def _next_ver(self) -> int:
        """Next row-version line (DST-clickhouse-08): versions are compared across streams for the same key (a
        catch-up / re-read stream writes a table after the main stream did), so they come from ONE counter: above
        every committed batch of every stream (`ver`, or `seq` for rows of older releases) and every snapshot."""
        _, r = self._rows(f"SELECT max(greatest(ver, seq)) FROM {self.c('checkpoint')} FINAL")
        _, v = self._rows(f"SELECT max(ver) FROM {self.c('version')}")
        return max(int(r[0][0] or 0), int(v[0][0] or 0)) + 1

    # ------------------------------------------------------------------ single writer
    def _writer_row(self, stream: str) -> tuple[str, int] | None:
        _, rows = self._rows(f"SELECT writer, epoch FROM {self.c('writer')} FINAL WHERE stream = {{s:String}}",
                             {"s": stream})
        return (str(rows[0][0]), int(rows[0][1])) if rows else None

    def _claim_writer(self, stream: str) -> int:
        """Prove the hub lock, then claim the stream (epoch + 1) if another writer held it. Returns our epoch."""
        if not self.single_writer:
            return 0
        require_writer(stream)
        cur = self._writer_row(stream)
        if cur is not None and cur[0] == self.writer_id:
            return cur[1]
        epoch = (cur[1] if cur else 0) + 1
        self.client.insert(self.c("writer"), [[stream, self.writer_id, epoch, datetime.now(timezone.utc)]],
                           column_names=["stream", "writer", "epoch", "claimed_at"])
        if cur is not None:
            log.warning("clickhouse stream %s: writer %s (epoch %s) taken over by %s (epoch %s)", stream, cur[0],
                        cur[1], self.writer_id, epoch)
        return epoch

    def _check_writer(self, stream: str, epoch: int) -> None:
        if not self.single_writer:
            return
        require_writer(stream)
        cur = self._writer_row(stream)
        if cur is None or cur[0] != self.writer_id:
            raise WriterFenceError(f"stream {stream!r}: fenced out at the commit point (writer now "
                                   f"{cur[0] if cur else None!r}, epoch {cur[1] if cur else None}; ours {epoch})")

    def _records(self, pl: TablePlan, seq: int, ts: datetime) -> list[dict[str, Any]]:
        if len(pl.ops) >= (1 << ORD_BITS):
            raise ValueError(f"{pl.dest}: more than {1 << ORD_BITS} rows in one batch")
        types = {c.name: c.canonical_type for c in pl.info.columns}
        out = []
        for i, op in enumerate(pl.ops.values(), start=1):
            r = {self.ident(c): _to_ch(op.row.get(c), types.get(c)) for c in pl.columns}
            r[self.c_synced] = ts
            r[self.c_deleted] = op.op == "D"
            r["_alelt_ver"] = (seq << ORD_BITS) | i
            r["_alelt_is_deleted"] = 1 if (op.op == "D" and self.hard) else 0
            out.append(r)
        return out

    def _insert(self, table: str, pl: TablePlan, recs: list[dict[str, Any]], tag: tuple[str, int] | None) -> None:
        if not recs:
            return
        names = list(recs[0])
        data = [[r[n] for n in names] for r in recs]
        if tag is not None:
            names += ["_alelt_stream", "_alelt_seq"]
            data = [[*d, tag[0], tag[1]] for d in data]
        self.client.insert(table, data, column_names=names)

    def _stage_truncate(self, stage: str, pl: TablePlan, seq: int, ts: datetime, stream: str,
                        ver: int | None = None) -> None:
        cols = [c for c in self._target_cols(pl.dest) if c not in (self.c_synced, self.c_deleted, "_alelt_ver",
                                                                   "_alelt_is_deleted")]
        sel = ", ".join(map(q, cols))
        hard = 1 if self.hard else 0
        self._cmd(f"INSERT INTO {self.c(stage)} ({sel}, {q(self.c_synced)}, {q(self.c_deleted)}, _alelt_ver, "
                  f"_alelt_is_deleted, _alelt_stream, _alelt_seq) SELECT {sel}, {_lit(ts)}, true, {(ver or seq) << ORD_BITS}, {hard}, "
                  f"{_lit(stream)}, {seq} FROM {self.t(pl.dest)} FINAL WHERE _alelt_is_deleted = 0 "
                  f"AND NOT {q(self.c_deleted)}")

    # ------------------------------------------------------------------ verify / snapshot helpers
    def fetch_rows(self, dest_name: str) -> list[dict[str, Any]]:
        names, rows = self._rows(f"SELECT * EXCEPT (_alelt_ver, _alelt_is_deleted) FROM {self.t(dest_name)} FINAL "
                                 "WHERE _alelt_is_deleted = 0")
        return [dict(zip(names, r)) for r in rows]

    def row_count(self, dest_name: str, include_deleted: bool = False, **kw: Any) -> int:
        w = "" if include_deleted else f" AND NOT {q(self.c_deleted)}"
        fqn = f"{q(kw.get('schema') or self.schema)}.{q(self.ident(dest_name))}"
        _, r = self._rows(f"SELECT count() FROM {fqn} FINAL WHERE _alelt_is_deleted = 0{w}")
        return int(r[0][0])

    def aggregates(self, dest_name: str, columns: list[str]) -> dict[str, Any]:
        parts = ["count()"]
        for c in columns:
            parts += [f"sum({q(self.ident(c))})", f"min({q(self.ident(c))})", f"max({q(self.ident(c))})"]
        _, r = self._rows(f"SELECT {', '.join(parts)} FROM {self.t(dest_name)} FINAL WHERE _alelt_is_deleted = 0 "
                          f"AND NOT {q(self.c_deleted)}")
        out: dict[str, Any] = {"count": int(r[0][0])}
        for i, c in enumerate(columns):
            out[f"sum:{c}"], out[f"min:{c}"], out[f"max:{c}"] = r[0][1 + 3 * i: 4 + 3 * i]
        return out

    def list_tables(self, schema: str | None = None) -> list[str]:
        _, rows = self._rows("SELECT name FROM system.tables WHERE database = {d:String} AND engine NOT LIKE '%View' "
                             "ORDER BY name", {"d": schema or self.schema})
        return [r[0] for r in rows]

    def describe_table(self, dest_name: str, schema: str | None = None) -> list[dict[str, Any]]:
        _, rows = self._rows("SELECT name, type FROM system.columns WHERE database = {d:String} AND table = {t:String} "
                             "ORDER BY position", {"d": schema or self.schema, "t": self.ident(dest_name)})
        return [{"name": n, "type": t.lower(), "nullable": t.startswith("Nullable(")} for n, t in rows
                if n not in ("_alelt_ver", "_alelt_is_deleted")]

    def audit_rows(self, stream: str) -> list[dict[str, Any]]:
        names, rows = self._rows(f"SELECT batch_id, to_position, rows_by_table, total_rows FROM "
                                 f"{self.c('audit_batch')} FINAL WHERE stream = {{s:String}} ORDER BY seq",
                                 {"s": stream})
        return [dict(zip(names, r)) for r in rows]


_MIN_D, _MAX_D = date(1900, 1, 1), date(2299, 12, 31)


def _typed_key(v: Any, ct: CanonicalType | None) -> Any:
    """Canonical key (alelt_loader._canon text form) -> typed value, so lookups compare with the column type
    (Decimal / Date32 / DateTime64 / binary keys never miss on a string comparison)."""
    if not isinstance(v, str):
        return v
    try:
        if ct is CanonicalType.DECIMAL:
            return Decimal(v)
        if ct is CanonicalType.LOCALDATE:
            return date.fromisoformat(v)
        if ct in (CanonicalType.LOCALDATETIME, CanonicalType.INSTANT):
            d = datetime.fromisoformat(v)
            return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
        if ct is CanonicalType.BINARY:
            return bytes.fromhex(v)
    except ValueError:
        return v
    return v


def _to_ch(v: Any, ct: CanonicalType | None) -> Any:
    if v is None:
        return None
    if isinstance(v, (date, datetime)) and ct in (CanonicalType.LOCALDATE, CanonicalType.LOCALDATETIME,
                                                  CanonicalType.INSTANT):
        d = v.date() if isinstance(v, datetime) else v
        if not _MIN_D <= d <= _MAX_D:                 # DST-clickhouse-03: ClickHouse would clamp silently
            raise ValueError(f"date {v} outside the ClickHouse Date32/DateTime64 range 1900-01-01..2299-12-31")
    if ct is CanonicalType.LOCALTIME and isinstance(v, dtime):
        return v.isoformat()
    if ct is CanonicalType.JSON and not isinstance(v, str):
        return json.dumps(v, separators=(",", ":"), default=str)
    if ct in (CanonicalType.INSTANT, CanonicalType.LOCALDATETIME) and isinstance(v, datetime) and v.tzinfo is None:
        # naive wall clock -> tagged UTC: the client would otherwise convert it with the HOST time zone
        # (local date-times are stored as their wall clock in a UTC column, read back naive)
        return v.replace(tzinfo=timezone.utc)
    return v


def _from_ch(v: Any, ct: CanonicalType | None) -> Any:
    if v is None:
        return None
    if ct is CanonicalType.LOCALTIME and isinstance(v, str):
        return dtime.fromisoformat(v)
    if ct is CanonicalType.LOCALDATETIME and isinstance(v, datetime):
        return v.replace(tzinfo=None)
    return v


def _lit(v: Any) -> str:
    """SQL literal for keys / constants (ClickHouse quoting rules)."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float, Decimal)):
        return str(v)
    if isinstance(v, datetime):
        if v.tzinfo is not None:
            v = v.astimezone(timezone.utc).replace(tzinfo=None)
        return f"toDateTime64('{v.isoformat(sep=' ')}', 6, 'UTC')"
    if isinstance(v, date):
        return f"toDate32('{v.isoformat()}')"
    if isinstance(v, (bytes, bytearray)):
        return "unhex('" + bytes(v).hex() + "')"
    s = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"
