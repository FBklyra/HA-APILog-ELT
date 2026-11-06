"""Db2Source: hub-side Db2 LUW source connector (edge capture via db2ReadLog)."""
from __future__ import annotations

from typing import Any
from uuid import UUID

from alelt_common.models import (
    CanonicalType,
    CatalogSnapshot,
    ChangeEvent,
    ChangeKind,
    ColumnInfo,
    Position,
    QuarantineRecord,
    RawRef,
    TableRef,
)
from alelt_common.completeness import RecordClass, SourceSignal
from alelt_common.spi import DecodeResult, SourceConnector

from . import types as T
from .catalog import specs_from_dict
from .logdecode import TableSpec, decode_buffer
from .native import enable as enable_native
from .native import info as native_info
from .position import Lri, compare_lri

ENGINE = "db2"
# Bump on ANY change of decoded output (rowimage / logdecode / framing / catalog specs): replay cache key (HC-02).
PY_DECODER_VERSION = "db2-py-4"


def decoder_version() -> str:
    from . import rowimage

    nat = native_info() if rowimage._native_cells is not None else {"backend": "python"}
    lib = nat.get("library_id") or ""
    return f"{PY_DECODER_VERSION}+{nat.get('backend')}" + (f"+abi{nat.get('abi')}+{lib}" if lib else "")


def _uuid(batch_id: Any) -> UUID | None:
    if batch_id is None:
        return None
    if isinstance(batch_id, UUID):
        return batch_id
    try:
        return UUID(str(batch_id))
    except ValueError:
        return None


class Db2Source(SourceConnector):
    engine = ENGINE
    capture_mode = "edge"

    @property
    def decoder_version(self) -> str:
        """HC-02 replay cache key: Python decoder version + C ABI + identity of the loaded libaleltdecode (if any)."""
        return decoder_version()

    def __init__(self, native: bool = True) -> None:
        self._spec_cache: tuple[int, dict[tuple[int, int], TableSpec]] | None = None
        self.native = enable_native() if native else False
        # table ids proven out of scope (absent from a catalog valid at a later position); a CREATE TABLE of the id
        # removes it again (id reuse). In memory: after a restart one catalog refresh per foreign id at most.
        self.foreign_ids: set[tuple[int, int]] = set()

    # --- positions
    def parse_position(self, text: str) -> Position:
        return Position(engine=ENGINE, text=Lri.parse(text).text)

    def compare(self, a: Position, b: Position) -> int:
        return compare_lri(a.text, b.text)

    # --- types
    def canonical_type(self, col: ColumnInfo) -> CanonicalType:
        return CanonicalType(T.canonical_name(col.engine_type, col.codepage))

    # --- decoding
    def _specs(self, catalog: CatalogSnapshot) -> dict[tuple[int, int], TableSpec]:
        key = id(catalog)
        if self._spec_cache is None or self._spec_cache[0] != key:
            self._spec_cache = (key, specs_from_dict(catalog.model_dump(by_alias=True)))
        return self._spec_cache[1]

    def decode(self, raw: bytes, catalog: CatalogSnapshot, batch_id: Any) -> DecodeResult:
        """Never raises on bad bytes: undecodable records become QuarantineRecords (R7)."""
        specs = self._specs(catalog)
        self.foreign_ids -= {i for i in self.foreign_ids if i in specs}
        res = decode_buffer(raw, specs, catalog_lri=catalog.position.text if catalog.position else None,
                            foreign=self.foreign_ids)
        self.foreign_ids |= res.foreign_ids
        self.foreign_ids -= res.created_ids
        for (record, cls, reason), n in res.census.items():
            if cls == "unexplained":    # also emitted as an `unsupported` signal by the decoder
                for _ in range(n):
                    self.census.unexplained(f"db2:{record}", reason)
            else:
                self.census.count(f"db2:{record}", cls, reason, n)
        bid = _uuid(batch_id)

        def ref(off: int, ln: int) -> RawRef | None:
            return RawRef(batch_id=bid, offset=off, length=ln) if bid else None

        events: list[ChangeEvent] = []
        for e in res.events:
            pos = Position(engine=ENGINE, text=e.lri)
            tref = TableRef(schema=e.schema, name=e.table) if e.schema and e.table else None
            if e.signal is not None:   # alelt_common.completeness signal event (X-132)
                s = e.signal
                ev = SourceSignal(RecordClass(s.cls), s.reason, tref, s.keys, None, s.record, s.detail).event(pos, e.txid)
                ev.raw_ref = ref(e.offset, e.length)
                events.append(ev)
                continue
            events.append(ChangeEvent(
                position=pos, txid=e.txid, kind=ChangeKind(e.kind), table=tref,
                before=e.before, after=e.after, commit_ts=e.commit_ts, raw_ref=ref(e.offset, e.length),
                note=e.note))
        quarantined = [QuarantineRecord(
            position=Position(engine=ENGINE, text=q.lri) if q.lri else None,
            table=TableRef(schema=q.schema, name=q.table) if q.schema and q.table else None,
            reason=q.reason, raw_ref=ref(q.offset, q.length), raw_hex=q.raw_hex) for q in res.quarantined]
        return DecodeResult(events=events, quarantined=quarantined, skipped=res.skipped)

    # --- record census (generic `census --source db2`, X-132): live log range (Db2 client) or stored raw batches
    def census_scan(self, database: str | None = None, schemas: list[str] | None = None, start: str | None = None,
                    end: str | None = None, files: list[str] | None = None,
                    catalog: CatalogSnapshot | None = None) -> dict[str, Any]:
        """Report shaped like alelt_common.completeness.Census.report() (+ range, signals, quarantine reasons)."""
        from pathlib import Path

        from .census import run_db, run_files

        cat = catalog.model_dump(by_alias=True, mode="json") if catalog is not None else None
        if files:
            if cat is None:
                raise ValueError("census over stored batches needs the catalog snapshot")
            return run_files([Path(f) for f in files], cat)
        if not (database and start):
            raise ValueError("live census needs database and start position")
        return run_db(database, schemas or [], start, end, cat)

    # --- catalog (needs the Db2 client: x64 process on the Db2 host / dev box)
    def read_catalog_from(self, database: str, schemas: list[str], connection_id: str = "db2") -> CatalogSnapshot:
        from .catalog import read_catalog
        from .db2api import Db2Connection

        with Db2Connection(database) as conn:
            return read_catalog(conn, schemas, connection_id)

    display_name = "IBM Db2 LUW"
    connection_fields = [
        {"name": "host", "label": "Host", "required": True},
        {"name": "port", "label": "Port", "required": True, "default": 50000},
        {"name": "database", "label": "Database", "required": True},
        {"name": "user", "label": "User", "required": True},
        {"name": "password", "label": "Password", "secret": True, "required": False},
        {"name": "schemas", "label": "Schemas", "required": False, "default": ""},
    ]

    def prerequisite_checks(self, catalog: CatalogSnapshot | None, probes: dict[str, Any],
                            tables: list[TableRef] | None = None, settings: dict[str, Any] | None = None
                            ) -> list[dict[str, Any]]:
        """Checklist with copy-paste DBA SQL (shown in the console, never executed by ALELT: R1)."""
        st = settings or {}
        db = str(st.get("database") or "<DB>").upper()
        user = str(st.get("user") or "<USER>").upper()
        schemas = sorted({t.schema_ for t in tables or []} or set(str(st.get("schemas") or "").upper().split(",")) - {""})
        flt = ", ".join(f"'{x}'" for x in schemas) or "'<SCHEMA>'"
        want = {t.fqn for t in tables or []}
        out: list[dict[str, Any]] = []

        def q(ident: str) -> str:
            return '"' + ident.replace('"', '""') + '"'

        log = (probes.get("log_config") or {}) if isinstance(probes.get("log_config"), dict) else {}
        arch = str(log.get("logarchmeth1") or log.get("LOGARCHMETH1") or "")
        out.append({"id": "archive_logging", "label": "Archive logging enabled (LOGARCHMETH1 is not OFF)",
                    "status": "UNKNOWN" if not arch else ("FAIL" if arch.upper() == "OFF" else "PASS"),
                    "detail": arch or "checked by the agent (PROBE log_config)",
                    "check_sql": f"db2 get db cfg for {db} | findstr LOGARCHMETH1",
                    "fix_sql": (f"db2 update db cfg for {db} using LOGARCHMETH1 DISK:D:\\db2archive\\\n"
                                f"db2 backup db {db} to D:\\db2backup   -- required once after enabling archive logging")})
        if catalog is not None:
            tabs = [t for t in catalog.tables if not want or t.table.fqn in want]
            no_dcc = [t for t in tabs if not t.capture_enabled]
            out.append({"id": "data_capture", "label": "DATA CAPTURE CHANGES on every captured table",
                        "status": "FAIL" if no_dcc else "PASS",
                        "detail": f"{len(no_dcc)} of {len(tabs)} table(s) need it" if no_dcc else f"{len(tabs)} table(s) ok",
                        "tables": [t.table.fqn for t in no_dcc],
                        "fix_sql": "\n".join(f"ALTER TABLE {q(t.table.schema_)}.{q(t.table.name)} DATA CAPTURE CHANGES;"
                                             for t in no_dcc) or None})
            vcomp = [t for t in tabs if (t.compression or "").upper() in ("V", "B")]
            out.append({"id": "no_value_compression", "label": "No VALUE COMPRESSION on captured tables",
                        "status": "FAIL" if vcomp else "PASS", "detail": f"{len(vcomp)} table(s)" if vcomp else "",
                        "tables": [t.table.fqn for t in vcomp],
                        "fix_sql": "\n".join(f"ALTER TABLE {q(t.table.schema_)}.{q(t.table.name)} DEACTIVATE VALUE "
                                             f"COMPRESSION;\nREORG TABLE {q(t.table.schema_)}.{q(t.table.name)};"
                                             for t in vcomp) or None})
            # SRC-db2-06: every character column's CCSID must map to a codec (else its rows cannot be decoded)
            from .rowimage import _codec

            bad_cp = sorted({f"{t.table.fqn}.{c.name} (CCSID {c.codepage})" for t in tabs for c in t.columns
                             if c.codepage not in (None, 0, 1200) and _codec(int(c.codepage)) is None})
            out.append({"id": "ccsid_supported", "label": "Character columns use a supported CCSID",
                        "status": "FAIL" if bad_cp else "PASS",
                        "detail": f"{len(bad_cp)} column(s): {', '.join(bad_cp[:20])}" if bad_cp else "",
                        "fix_sql": None})
        else:
            out.append({"id": "data_capture", "label": "DATA CAPTURE CHANGES on every captured table",
                        "status": "UNKNOWN", "detail": "checked when the agent sends the catalog",
                        "check_sql": f"SELECT TABSCHEMA, TABNAME FROM SYSCAT.TABLES WHERE TYPE = 'T' AND "
                                     f"DATACAPTURE <> 'Y' AND TABSCHEMA IN ({flt}) WITH UR",
                        "fix_sql": f"-- for each table returned above:\nALTER TABLE <SCHEMA>.<TABLE> DATA CAPTURE CHANGES;"})
        cc = str(log.get("cur_commit") or log.get("CUR_COMMIT") or "")
        out.append({"id": "cur_commit", "label": "Currently committed semantics (CUR_COMMIT ON): snapshot and verify "
                                                 "reads use cursor stability without waiting on writers (F-04)",
                    "status": "UNKNOWN" if not cc else ("PASS" if cc.upper() in ("ON", "AVAILABLE") else "FAIL"),
                    "detail": cc or "checked by the agent (PROBE log_config)",
                    "check_sql": f"db2 get db cfg for {db} | findstr CUR_COMMIT",
                    "fix_sql": f"db2 update db cfg for {db} using CUR_COMMIT ON   -- takes effect on database "
                               "reactivation; without it CS readers wait on uncommitted writers (never dirty reads)"})
        health = probes.get("db_health") if isinstance(probes.get("db_health"), dict) else {}
        dbadm = (health or {}).get("dbadm")
        out.append({"id": "authority", "label": "Capture user holds DBADM (or SYSADM) for db2ReadLog",
                    "status": "UNKNOWN" if dbadm is None else ("PASS" if dbadm else "FAIL"),
                    "detail": "" if dbadm is not None else "checked by the agent (PROBE db_health)",
                    "check_sql": f"SELECT * FROM TABLE(SYSPROC.AUTH_LIST_AUTHORITIES_FOR_AUTHID('{user}', 'U'))",
                    "fix_sql": f"GRANT DBADM ON DATABASE TO USER {user};"})
        out += self._log_shape_checks(catalog, health or {}, want, flt, q)
        return out

    @staticmethod
    def _log_shape_checks(catalog: CatalogSnapshot | None, health: dict[str, Any], want: set[str], flt: str,
                          q: Any) -> list[dict[str, Any]]:
        """Log-format prerequisites (DESIGN.md §9): partitioned database, data-partitioned tables, LONG columns,
        native encryption. fix_sql is shown to the DBA, never run (R1)."""
        out: list[dict[str, Any]] = []
        parts = health.get("partitions")
        out.append({"id": "single_partition", "label": "Single database partition (db2ReadLog returns only the "
                                                         "connected partition's log; pureScale members are merged)",
                    "status": "UNKNOWN" if parts is None else ("PASS" if int(parts) <= 1 else "FAIL"),
                    "detail": "checked by the agent (PROBE db_health)" if parts is None else f"{parts} partition(s)",
                    "check_sql": "SELECT COUNT(DISTINCT DBPARTITIONNUM) FROM SYSCAT.DBPARTITIONGROUPDEF WITH UR",
                    "fix_sql": None})
        enc = health.get("encrypted")
        out.append({"id": "native_encryption", "label": "Native encryption: db2ReadLog returns records through the "
                                                          "engine (not stated in IBM's db2ReadLog docs; census verifies)",
                    "status": "UNKNOWN" if enc is None else "INFO",
                    "detail": "" if enc is None else ("encrypted: run the record census on this database before "
                                                      "certification" if enc else "not encrypted"),
                    "check_sql": "SELECT OBJECT_NAME, ALGORITHM FROM TABLE(SYSPROC.ADMIN_GET_ENCRYPTION_INFO()) "
                                 "WHERE OBJECT_TYPE = 'DATABASE'",
                    "fix_sql": None})
        if catalog is None:
            return out
        tabs = [t for t in catalog.tables if not want or t.table.fqn in want]
        unmapped = [t for t in tabs if (t.engine_ids.get("tbspaceid", 0) < 0 or t.engine_ids.get("tableid", 0) < 0)
                    and not any(k.startswith("part.") for k in t.engine_ids)]
        out.append({"id": "partition_ids", "label": "Data-partitioned tables carry their partition object ids in "
                                                     "the catalog (SYSCAT.DATAPARTITIONS)",
                    "status": "FAIL" if unmapped else "PASS",
                    "detail": f"{len(unmapped)} table(s): catalog from an agent without partition support"
                    if unmapped else "", "tables": [t.table.fqn for t in unmapped], "fix_sql": None})
        longv = [t for t in tabs if any(c.engine_type.upper() in T.LONG_TYPES for c in t.columns)]
        out.append({"id": "long_columns", "label": "LONG VARCHAR / LONG VARGRAPHIC columns: values re-read by key "
                                                    "(DATA CAPTURE CHANGES INCLUDE LONGVAR COLUMNS optional)",
                    "status": "INFO" if longv else "PASS",
                    "detail": f"{len(longv)} table(s) with LONG columns" if longv else "",
                    "tables": [t.table.fqn for t in longv],
                    "check_sql": "SELECT TABSCHEMA, TABNAME, DATACAPTURE FROM SYSCAT.TABLES WHERE TABSCHEMA IN "
                                 f"({flt}) AND DATACAPTURE = 'Y' WITH UR   -- 'L' = INCLUDE LONGVAR COLUMNS",
                    "fix_sql": "\n".join(f"ALTER TABLE {q(t.table.schema_)}.{q(t.table.name)} DATA CAPTURE CHANGES "
                                         "INCLUDE LONGVAR COLUMNS;" for t in longv) or None})
        return out

    def prerequisites(self) -> list[str]:
        return ["LOGARCHMETH1 set (archive logging)", "DATA CAPTURE CHANGES on captured tables",
                "capture user has DBADM or SYSADM (db2ReadLog)",
                "no VALUE COMPRESSION on captured tables (row format not supported)",
                "CUR_COMMIT ON (snapshot / VERIFY_TABLE reads are WITH CS, currently committed)"]
