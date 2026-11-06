"""SnowflakeDestination (DESIGN.md §6, master spec §8, R6) on the shared destination base (alelt_dst_kit, F-16).

Exactly-once design (F-05): the checkpoint compare-and-set is the FIRST statement of the apply transaction (a
conditional MERGE on ALELT_CONTROL.CHECKPOINT matching the position/batch the batch was planned from); it locks the
control table for the transaction, so a second applier waits and then matches nothing -> ROLLBACK +
CheckpointConflictError (or a no-op when it was the same batch). Verified with mocks and SQL generation only:
no claim of exactly-once on a real Snowflake account until a real-account run passes (X-126)."""
from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import tempfile
import time
import uuid
from collections.abc import Callable, Iterable, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from alelt_common.canonical_jsonl import canonical_snapshot_row, snapshot_types
from alelt_common.models import ChangeSet, ColumnInfo, Position, QuarantineRecord, TableInfo, TableRef
from alelt_common.spi import UNSET, ApplyStats, CheckpointConflictError
from alelt_dst_kit.base import ControlState, DestinationBase, wm_dumps, wm_loads
from alelt_common.util import utcnow

from . import sqlgen as g
from .executor import Executor
from .migrations import MigrationRunner
from .parquet import write_parquet
from .profile import ALELT, OutputProfile
from .rows import TableBatch, fold, surrogate_id
from .types import DEFAULT_MAP, snowflake_type

log = logging.getLogger("alelt_dst_snowflake")

Compare = Callable[[Position, Position], int]
_SAFE = re.compile(r"[^A-Za-z0-9_.-]")


class SnowflakeDestination(DestinationBase):
    """Apply = Parquet -> PUT -> COPY INTO transient -> one transaction (CHECKPOINT CAS + MERGE + AUDIT_BATCH) ->
    COMMIT."""

    engine = "snowflake"

    def __init__(self, executor: Executor, database: str, landing_schema: str, compare: Compare,
                 profile: OutputProfile = ALELT, type_map: dict[str, Any] | None = None,
                 migration_params: dict[str, object] | None = None, work_dir: str | None = None,
                 snapshot_chunk_rows: int = 100_000) -> None:
        self.ex = executor
        self.db = g.check_ident(database)
        self.landing = g.check_ident(landing_schema)
        self.schema, self.control = self.landing, "ALELT_CONTROL"
        from alelt_loader.policy import SystemColumns

        self.sys = SystemColumns(profile.name, profile.synced_column, profile.deleted_column, profile.id_column)
        self.compare = compare
        self.profile = profile
        self.type_map = type_map or DEFAULT_MAP
        self.migration_params = migration_params
        self.work_dir = work_dir
        self.snapshot_chunk_rows = snapshot_chunk_rows

    @classmethod
    def from_hub(cls, url: str, schema: str, output_profile: str | None, compare: Compare,
                 env: dict[str, str] | None = None, executor: Executor | None = None) -> "SnowflakeDestination":
        """Factory used by the hub pipeline (`make_destination`). Identifiers and key come from the environment
        (`ALELT_SNOWFLAKE_ACCOUNT/USER/ROLE/WAREHOUSE/DATABASE`, `..._PRIVATE_KEY_PATH|PRIVATE_KEY[_PASSPHRASE]`, R9);
        `schema` = landing schema from tables.yaml; output profile resolved from the profile files
        (`config/output_profiles.yaml`, extensions, `ALELT_OUTPUT_PROFILES`) and `ALELT_GOLDEN_COLUMNS` for `from_golden`.
        `ALELT_SNOWFLAKE_SOURCE` (default: landing schema) names the migration templates' source; `url` is unused."""
        import os

        from .executor import SnowflakeExecutor, SnowflakeSettings
        from .migrations import default_params
        from .profile import load_profile

        e = dict(os.environ if env is None else env)
        repo = Path(__file__).resolve().parents[4]
        files = [repo / "config" / "output_profiles.yaml"]
        try:
            from alelt_schema.config import profile_files

            files = list(profile_files(repo))
        except ImportError:
            pass
        files += [Path(p) for p in e.get("ALELT_OUTPUT_PROFILES", "").split(os.pathsep) if p]
        golden = e.get("ALELT_GOLDEN_COLUMNS") or str(repo / "config" / "golden" / "columns.csv")
        profile = load_profile([f for f in files if f.exists()], output_profile,
                               golden if Path(golden).exists() else None)
        database = e.get("ALELT_SNOWFLAKE_DATABASE") or ""
        if executor is None:                 # settings read from `e`; os.environ is never mutated (DST-snowflake-09)
            executor = SnowflakeExecutor(SnowflakeSettings.from_env(env=e))
        landing = g.landing_schema_name(schema)
        params = default_params(database, e.get("ALELT_SNOWFLAKE_SOURCE") or landing, LANDING_SCHEMA=landing,
                                WAREHOUSE=e.get("ALELT_SNOWFLAKE_WAREHOUSE", "ALELT_WH"))
        skip = e.get("ALELT_SNOWFLAKE_BOOTSTRAP", "1") == "0"     # objects created beforehand by an admin
        dest = cls(executor, database, landing, compare, profile=profile,
                   migration_params=None if skip else params, work_dir=e.get("ALELT_SNOWFLAKE_WORK_DIR"))
        dest._skip_bootstrap = skip
        return dest

    # ------------------------------------------------------------ naming
    def table_fqn(self, dest_name: str, schema: str | None = None) -> str:
        return g.fq(self.db, schema or self.landing, dest_name)

    def _x(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
        return self.ex.execute(sql, params)

    # ------------------------------------------------------------ bootstrap / DDL
    def bootstrap(self) -> None:
        if self.migration_params is None:
            if getattr(self, "_skip_bootstrap", False):
                return
            raise RuntimeError("bootstrap needs migration_params (see migrations.default_params)")
        MigrationRunner(self.ex, self.migration_params).run()

    def ensure_table(self, table: TableInfo, dest_name: str, golden_ddl: str | None) -> None:
        fqn = self.table_fqn(dest_name)
        sql = g.golden_table_sql(golden_ddl, fqn) if golden_ddl else g.create_table_sql(fqn, table, self.profile, self.type_map)
        self._x(sql)
        rows = self._x(g.columns_sql(self.db), (self.landing, dest_name))
        have = {str(r[0]): r for r in rows}
        if have:                                    # additive drift (DST-snowflake-04): COPY matches by name
            from alelt_dst_kit.widen import widen_to

            for c in table.columns:
                if c.name not in have:
                    self.add_column(dest_name, c)
                    continue
                r = have[c.name]
                if len(r) < 5:
                    continue
                dt = str(r[1]).lower()
                cur = (f"varchar({r[2]})" if "text" in dt or "char" in dt and r[2] else
                       f"number({r[3]},{r[4] or 0})" if dt in ("number", "numeric", "decimal") and r[3] else dt)
                to = widen_to(cur, snowflake_type(c, self.type_map))
                if to:                              # type widening on drift (DST-kit-04)
                    try:
                        self._x(g.alter_type_sql(fqn, c.name, to))
                    except Exception as e:  # noqa: BLE001 - e.g. NUMBER scale change: reported, inserts fail loudly
                        log.warning("cannot widen %s.%s to %s: %s", dest_name, c.name, to, e)

    def add_column(self, dest_name: str, col: ColumnInfo) -> None:
        self._x(g.add_column_sql(self.table_fqn(dest_name), col.name, snowflake_type(col, self.type_map)))

    # ------------------------------------------------------------ checkpoint
    def _state(self, stream: str) -> ControlState:
        rows = self._x(g.read_checkpoint_sql(self.db), (stream,))
        if not rows:
            return ControlState()
        r = rows[0]
        return ControlState(str(r[1]), 0, r[2] if len(r) > 2 else None, wm_loads(r[3]) if len(r) > 3 else {})

    def read_checkpoint(self, stream: str) -> Position | None:
        rows = self._x(g.read_checkpoint_sql(self.db), (stream,))
        return Position(engine=str(rows[0][0]), text=str(rows[0][1])) if rows else None

    def delete_checkpoint(self, stream: str) -> None:
        """Drop the checkpoint row of a transient stream (replay pass, HC-21)."""
        self._x(f"DELETE FROM {g.control(self.db, 'CHECKPOINT')} WHERE STREAM = %s", (stream,))

    def read_watermarks(self, stream: str) -> dict[str, Position]:
        return {t: Position(engine="", text=p) for t, p in self._state(stream).watermarks.items()}

    # ------------------------------------------------------------ apply (R6)
    def apply(self, stream: str, changesets: list[ChangeSet], to_pos: Position, batch_id: str,
              table_map: dict[TableRef, str], tables: dict[TableRef, TableInfo], *, expected: Any = UNSET,
              watermarks: Any = None) -> list[ApplyStats]:
        self.refuse_lossy_values(changesets, tables)
        base = self._state(stream)                  # what the batch is planned from (CAS base)
        if expected is not UNSET:
            exp = expected.text if isinstance(expected, Position) else (expected or None)
            if exp != base.position:
                if self.pos_ge(base.position, to_pos):
                    return []
                raise CheckpointConflictError(stream, exp, base.position, "checkpoint is not the expected base")
        cp = Position(engine=to_pos.engine, text=base.position) if base.position else None
        if cp is not None and self.compare(cp, to_pos) >= 0:
            log.info("batch already applied", extra={"component": "dst_snowflake", "batch_id": batch_id})
            return []
        self._hook("after_read")
        wm = wm_dumps(self.merge_watermarks(base.watermarks, self.batch_watermarks(to_pos, table_map, watermarks),
                                            to_pos.engine))
        t0 = time.monotonic()
        folded = fold(changesets, tables, self.profile)
        order = sorted(folded, key=lambda r: table_map[r])
        work = Path(tempfile.mkdtemp(prefix="alelt_", dir=self.work_dir))
        staged: list[tuple[str, str]] = []
        try:
            temps = self._stage_all(stream, batch_id, [(ref, table_map[ref], tables[ref], folded[ref].rows)
                                                        for ref in order if folded[ref].rows], work, staged)
            self._x("BEGIN TRANSACTION")
            try:
                res = self._x(g.checkpoint_cas_sql(self.db), (stream, to_pos.engine, to_pos.text, batch_id, wm,
                                                              base.position, base.batch_id if base.position else None))
                if res and sum(int(x or 0) for x in res[0]) != 1:      # another writer moved the checkpoint
                    self._x("ROLLBACK")
                    found = self._state(stream)
                    if self.pos_ge(found.position, to_pos):
                        return []
                    raise CheckpointConflictError(stream, base.position, found.position,
                                                  "checkpoint changed since the batch was planned (concurrent writer)")
                for ref in order:
                    tb, dest = folded[ref], table_map[ref]
                    if tb.truncated:
                        self._x(g.truncate_sql(self.table_fqn(dest), self.profile))
                    for tmp, cols in temps.get(ref, []):
                        self._x(self._merge(dest, tables[ref], tmp, cols))
                self._hook("after_data")
                ms = int((time.monotonic() - t0) * 1000)
                audit = g.audit_insert_sql(self.db)
                for ref in order:
                    self._x(audit, self._audit_row(stream, batch_id, cp, to_pos, table_map[ref], folded[ref], ms))
                if not order:
                    self._x(audit, self._audit_row(stream, batch_id, cp, to_pos, None, None, ms))
                self._hook("before_commit")
                self._x("COMMIT")
            except CheckpointConflictError:
                raise
            except BaseException:
                self._x("ROLLBACK")
                raise
            ms = int((time.monotonic() - t0) * 1000)
            return [ApplyStats(table=table_map[r], inserts=folded[r].inserts, updates=folded[r].updates,
                               deletes=folded[r].deletes, duration_ms=ms) for r in order]
        finally:
            self._cleanup(staged, work)

    def _stage_all(self, stream: str, batch_id: str, items: list[tuple[TableRef, str, TableInfo, list[dict[str, Any]]]],
                   work: Path, staged: list[tuple[str, str]]) -> dict[TableRef, list[tuple[str, list[str] | None]]]:
        """Stage every table of the flush with ONE PUT (all Parquet files of the batch folder), then one CREATE AS +
        one COPY per stage table; cleanup = one DROP per stage table + ONE REMOVE (DST-snowflake-06)."""
        folder = g.stage_path(self.db, "PARQUET", _SAFE.sub("_", stream), _SAFE.sub("_", batch_id)) + "/"
        files: list[tuple[TableRef, str, TableInfo, Path, list[str] | None]] = []
        for ref, dest, info, rows in items:
            for gi, (cols, grows) in enumerate(_groups(rows, info)):
                local = work / f"{_SAFE.sub('_', dest)}{f'_g{gi}' if gi else ''}.parquet"
                write_parquet(local, info, self.profile, grows)
                files.append((ref, dest, info, local, cols))
        out: dict[TableRef, list[tuple[str, list[str] | None]]] = {}
        if not files:
            return out
        self._x(g.put_sql(str(work / "*.parquet"), folder))
        for ref, dest, info, local, cols in files:
            h = hashlib.sha1(f"{stream}|{batch_id}|{uuid.uuid4().hex}".encode()).hexdigest()[:16].upper()
            tmp = g.fq(self.db, "ALELT_STAGE", f"TMP_{_SAFE.sub('_', dest).upper()[:200]}_{h}")    # DST-snowflake-03
            staged.append((tmp, folder))
            self._x(g.create_stage_table_sql(tmp, self.table_fqn(dest)))
            names = [c.name.upper() for c in info.columns]
            self._x(g.copy_into_sql(tmp, folder + local.name, case_sensitive=len(set(names)) != len(names)))
            out.setdefault(ref, []).append((tmp, cols))
        return out

    def _stage(self, stream: str, batch_id: str, dest: str, info: TableInfo, rows: list[dict[str, Any]],
               work: Path, staged: list[tuple[str, str]], group: int = 0) -> str:
        """Parquet -> PUT -> transient table -> COPY. Runs before BEGIN (DDL auto-commits in Snowflake)."""
        local = work / f"{_SAFE.sub('_', dest)}{f'_g{group}' if group else ''}.parquet"
        write_parquet(local, info, self.profile, rows)
        folder = g.stage_path(self.db, "PARQUET", _SAFE.sub("_", stream), _SAFE.sub("_", batch_id)) + "/"
        h = hashlib.sha1(f"{stream}|{batch_id}|{uuid.uuid4().hex}".encode()).hexdigest()[:16].upper()
        tmp = g.fq(self.db, "ALELT_STAGE", f"TMP_{_SAFE.sub('_', dest).upper()[:200]}_{h}")    # unique (DST-snowflake-03)
        staged.append((tmp, folder))
        self._x(g.put_sql(str(local), folder))
        self._x(g.create_transient_sql(tmp, self.table_fqn(dest)))
        self._x(g.add_op_columns_sql(tmp))
        names = [c.name.upper() for c in info.columns]
        self._x(g.copy_into_sql(tmp, folder + local.name, case_sensitive=len(set(names)) != len(names)))
        return tmp

    def _merge(self, dest: str, info: TableInfo, tmp: str, cols: list[str] | None = None) -> str:
        data = cols or [c.name for c in sorted(info.columns, key=lambda c: c.ordinal)]
        keys = g.key_names(info, self.profile)
        return g.merge_sql(self.table_fqn(dest), tmp, data, keys, self.profile)

    @staticmethod
    def _audit_row(stream: str, batch_id: str, cp: Position | None, to_pos: Position, dest: str | None,
                   tb: TableBatch | None, ms: int) -> tuple[Any, ...]:
        return (batch_id, stream, dest, cp.text if cp else None, to_pos.text,
                tb.inserts if tb else 0, tb.updates if tb else 0, tb.deletes if tb else 0,
                bool(tb and tb.truncated), ms, "COMMITTED",
                tb.min_commit_ts if tb else None, tb.max_commit_ts if tb else None)

    def _cleanup(self, staged: list[tuple[str, str]], work: Path) -> None:
        sqls = [g.drop_table_sql(tmp) for tmp, _ in staged]
        sqls += [g.remove_sql(f) for f in dict.fromkeys(f for _, f in staged)]      # one REMOVE per folder
        for sql in sqls:
            try:
                self._x(sql)
            except Exception:  # best effort: leftovers are replaced on retry
                log.warning("cleanup failed: %s", sql, extra={"component": "dst_snowflake"})
        shutil.rmtree(work, ignore_errors=True)

    # ------------------------------------------------------------ snapshot
    def load_snapshot(self, dest_name: str, table: TableInfo, rows: Iterable[dict[str, Any]]) -> int:
        """Upsert snapshot rows (idempotent by key); no checkpoint change. Returns rows loaded."""
        data_cols = [c.name for c in sorted(table.columns, key=lambda c: c.ordinal)]
        keyless = not table.key_columns()
        types = snapshot_types(table)
        total, chunk = 0, []
        for r in rows:                         # snapshot values in the CDC forms (JSON = text; dict/list accepted)
            r = canonical_snapshot_row(r, types, strict=True)
            row = {c: r.get(c) for c in data_cols}
            if keyless:
                row[self.profile.id_column] = surrogate_id(row, data_cols)
            row["_op"], row["_seq"] = "U", total + len(chunk) + 1
            chunk.append(row)
            if len(chunk) >= self.snapshot_chunk_rows:
                total += self._load_chunk(dest_name, table, chunk)
                chunk = []
        if chunk:
            total += self._load_chunk(dest_name, table, chunk)
        return total

    def _load_chunk(self, dest: str, info: TableInfo, rows: list[dict[str, Any]]) -> int:
        work = Path(tempfile.mkdtemp(prefix="alelt_snap_", dir=self.work_dir))
        staged: list[tuple[str, str]] = []
        try:
            tmp = self._stage("snapshot", uuid.uuid4().hex, dest, info, rows, work, staged)
            self._x("BEGIN TRANSACTION")
            try:
                self._x(self._merge(dest, info, tmp))
                self._x("COMMIT")
            except Exception:
                self._x("ROLLBACK")
                raise
            return len(rows)
        finally:
            self._cleanup(staged, work)

    # ------------------------------------------------------------ quarantine / raw archive
    def quarantine(self, stream: str, records: Sequence[QuarantineRecord]) -> int:  # type: ignore[override]
        sql = (f"INSERT INTO {g.control(self.db, 'QUARANTINE')} (QUARANTINE_ID, STREAM, TABLE_NAME, POSITION_TEXT, "
               f"REASON, RAW_BATCH_ID, RAW_OFFSET, RAW_LENGTH, RAW_HEX) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
        for r in records:
            self._x(sql, (uuid.uuid4().hex, stream, r.table.fqn if r.table else None,
                          r.position.text if r.position else None, r.reason,
                          str(r.raw_ref.batch_id) if r.raw_ref else None, r.raw_ref.offset if r.raw_ref else None,
                          r.raw_ref.length if r.raw_ref else None, r.raw_hex))
        return len(records)

    def archive_raw(self, connection_id: str, batch_id: str, body: bytes, header: dict[str, Any],
                    day: date | None = None) -> str:
        """PUT raw edge batch (body as shipped, zstd) + header JSON to RAW_ARCHIVE/<conn>/<yyyy>/<mm>/<dd>/."""
        d = day or utcnow().date()
        folder = g.stage_path(self.db, "RAW_ARCHIVE", _SAFE.sub("_", connection_id), f"{d:%Y/%m/%d}") + "/"
        work = Path(tempfile.mkdtemp(prefix="alelt_raw_", dir=self.work_dir))
        try:
            name = _SAFE.sub("_", batch_id)
            (work / f"{name}.zst").write_bytes(body)
            (work / f"{name}.json").write_text(json.dumps(header, sort_keys=True, default=str), encoding="utf-8")
            for f in (f"{name}.zst", f"{name}.json"):
                self._x(g.put_sql(str(work / f), folder))
            return folder + f"{name}.zst"
        finally:
            shutil.rmtree(work, ignore_errors=True)

    def purge_raw_archive(self, connection_id: str, keep_days: int, today: date | None = None) -> list[str]:
        """Remove day folders older than keep_days (retention 30-90 days, ADR-007). Returns removed prefixes."""
        base = g.stage_path(self.db, "RAW_ARCHIVE", _SAFE.sub("_", connection_id)) + "/"
        names = [str(r[0]) for r in self._x(f"LIST '{base}'")]
        removed = expired_day_prefixes(names, keep_days, today or utcnow().date())
        for day in removed:
            self._x(g.remove_sql(base + day + "/"))
        return removed

    # ------------------------------------------------------------ verification helpers (alelt_verify)
    def _vp(self, profile: OutputProfile | None) -> OutputProfile:
        return profile or self.profile

    def row_count(self, dest_name: str, include_deleted: bool = False, *, schema: str | None = None,
                  profile: OutputProfile | None = None, synced_before: datetime | None = None) -> int:
        sql, p = g.row_count_sql(self.table_fqn(dest_name, schema), self._vp(profile), include_deleted, synced_before)
        return int(self._x(sql, p)[0][0])

    def key_bucket_hashes(self, dest_name: str, key_cols: list[str], buckets: int, *, schema: str | None = None,
                          profile: OutputProfile | None = None, synced_before: datetime | None = None) -> dict[int, str]:
        sql, p = g.key_bucket_sql(self.table_fqn(dest_name, schema), key_cols, buckets, self._vp(profile), synced_before)
        return {int(b): f"{int(n)}:{h}" for b, n, h in self._x(sql, p)}

    def column_checksums(self, dest_name: str, key_cols: list[str], columns: list[str], buckets: int, *,
                         schema: str | None = None, profile: OutputProfile | None = None,
                         synced_before: datetime | None = None) -> dict[str, dict[int, str]]:
        sql, p = g.column_checksum_sql(self.table_fqn(dest_name, schema), key_cols, columns, buckets,
                                       self._vp(profile), synced_before)
        out: dict[str, dict[int, str]] = {c: {} for c in columns}
        for row in self._x(sql, p):
            for c, h in zip(columns, row[1:]):
                out[c][int(row[0])] = str(h)
        return out

    def deleted_keys(self, dest_name: str, key_cols: list[str], *, schema: str | None = None,
                     profile: OutputProfile | None = None) -> list[tuple[Any, ...]]:
        return [tuple(r) for r in self._x(g.deleted_keys_sql(self.table_fqn(dest_name, schema), key_cols,
                                                              self._vp(profile)))]

    def sample_rows(self, dest_name: str, key_cols: list[str], columns: list[str], keys: list[tuple[Any, ...]], *,
                    schema: str | None = None) -> dict[tuple[Any, ...], dict[str, Any]]:
        if not keys:
            return {}
        cols = list(dict.fromkeys([*key_cols, *columns]))
        sql = g.sample_rows_sql(self.table_fqn(dest_name, schema), key_cols, columns, len(keys))
        rows = self._x(sql, [v for k in keys for v in k])
        return {tuple(r[: len(key_cols)]): dict(zip(cols, r)) for r in rows}

    def _on_schema(self, schema: str | None) -> "SnowflakeDestination":
        if not schema or schema == self.landing:
            return self
        import copy

        c = copy.copy(self)
        c.landing = c.schema = g.check_ident(schema)
        return c

    def fetch_rows(self, dest_name: str) -> list[dict[str, Any]]:
        """All rows (OBJECT_CONSTRUCT keeps column names; lower-cased). Verify / reference-landing use only."""
        out = []
        for (obj,) in self._x(f"SELECT OBJECT_CONSTRUCT_KEEP_NULL(*) FROM {self.table_fqn(dest_name)}"):
            d = json.loads(obj) if isinstance(obj, str) else dict(obj)
            out.append({str(k).lower(): v for k, v in d.items()})
        return out

    def publish_view(self, view_schema: str, table: str, source_schema: str, columns: list[str]) -> None:
        """Explicit-column view + VIEW_COLUMNS registry (used by SWITCH_SOURCE)."""
        self._x(g.view_sql(g.fq(self.db, view_schema, table), g.fq(self.db, source_schema, table), columns))
        for sql, p in g.view_columns_sql(self.db, table, columns):
            self._x(sql, p)


def _groups(rows: list[dict[str, Any]], info: TableInfo) -> list[tuple[list[str] | None, list[dict[str, Any]]]]:
    """Rows grouped by the column set their image carries: full rows (and deletes) in one group (all columns),
    each partial column set in its own group whose MERGE updates only those columns (DST-snowflake-01)."""
    data = [c.name for c in sorted(info.columns, key=lambda c: c.ordinal)]
    full = frozenset(data)
    out: dict[frozenset[str], list[dict[str, Any]]] = {}
    for r in rows:
        pres = r.get("_present", full)
        out.setdefault(full if r["_op"] == "D" else pres, []).append(r)
    return [(None if k == full else [c for c in data if c in k], v) for k, v in sorted(
        out.items(), key=lambda kv: (kv[0] != full, sorted(kv[0])))]


def expired_day_prefixes(names: Iterable[str], keep_days: int, today: date) -> list[str]:
    """From LIST output names ('raw_archive/<conn>/YYYY/MM/DD/file'), the 'YYYY/MM/DD' folders older than cutoff."""
    cutoff = today - timedelta(days=keep_days)
    days: set[str] = set()
    for n in names:
        m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", n)
        if m and date(int(m.group(1)), int(m.group(2)), int(m.group(3))) < cutoff:
            days.add(f"{m.group(1)}/{m.group(2)}/{m.group(3)}")
    return sorted(days)
