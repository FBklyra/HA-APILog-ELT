"""Oracle LogMiner prerequisites with copy-paste DBA SQL. ALELT never executes fix_sql (R1)."""
from __future__ import annotations

from typing import Any

from alelt_common.models import CatalogSnapshot, TableRef

from .catalog import q

REQUIRED_PRIVS = ("CREATE SESSION", "LOGMINING", "SELECT ANY TRANSACTION")
V_VIEWS = ("V_$DATABASE", "V_$LOG", "V_$LOGFILE", "V_$ARCHIVED_LOG", "V_$LOGMNR_CONTENTS", "V_$LOGMNR_LOGS",
           "V_$TRANSACTION")


def grants_sql(user: str, tables: list[TableRef], cdb: bool) -> str:
    u = user.upper() if user else "ALELT_CAPTURE"
    lines = []
    if cdb:
        lines.append(f"-- multitenant: run in CDB$ROOT for a common user (C##...), or in the PDB on Oracle 21c+")
    lines += [f"GRANT CREATE SESSION TO {u};", f"GRANT LOGMINING TO {u};", f"GRANT SELECT ANY TRANSACTION TO {u};",
              f"GRANT SELECT_CATALOG_ROLE TO {u};", f"GRANT EXECUTE ON SYS.DBMS_LOGMNR TO {u};"]
    lines += [f"GRANT SELECT ON SYS.{v} TO {u};" for v in V_VIEWS]
    for t in tables or [TableRef(schema="<SCHEMA>", name="<TABLE>")]:
        lines.append(f"GRANT SELECT, FLASHBACK ON {q(t.schema_)}.{q(t.name)} TO {u};")
    return "\n".join(lines)


def checks(conn: Any | None, catalog: CatalogSnapshot | None, tables: list[TableRef] | None,
           settings: dict[str, Any], resume_scn: int | None = None) -> list[dict[str, Any]]:
    user = str(settings.get("user") or "ALELT_CAPTURE").upper()
    want = {t.fqn for t in tables or []}
    out: list[dict[str, Any]] = []
    facts: dict[str, Any] = {}
    privs: set[str] = set()
    roles: set[str] = set()
    if conn is not None:
        cur = conn.cursor()

        def one(sql: str) -> Any:
            try:
                cur.execute(sql)
                r = cur.fetchone()
                return r if r else None
            except Exception as e:  # noqa: BLE001 - missing grant shows as UNKNOWN/FAIL, never raises
                return e

        r = one("SELECT LOG_MODE, SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_ALL, CDB FROM V$DATABASE")
        if isinstance(r, tuple):
            facts.update(log_mode=r[0], supp_min=r[1], supp_all=r[2], cdb=r[3])
        else:
            facts["vdb_error"] = str(r)
        r = one("SELECT SYS_CONTEXT('USERENV','CON_NAME') FROM DUAL")
        if isinstance(r, tuple):
            facts["con_name"] = r[0]
        r = one("SELECT VERSION FROM PRODUCT_COMPONENT_VERSION WHERE PRODUCT LIKE 'Oracle Database%'")
        if isinstance(r, tuple) and r[0]:
            facts["version"] = str(r[0])
        r = one("SELECT MIN(FIRST_CHANGE#) FROM V$ARCHIVED_LOG WHERE DELETED = 'NO' AND NAME IS NOT NULL")
        if isinstance(r, tuple):
            facts["oldest_archived_scn"] = r[0]
        r = one("SELECT MIN(FIRST_CHANGE#) FROM V$LOG")
        if isinstance(r, tuple):
            facts["oldest_online_scn"] = r[0]
        try:
            cur.execute("SELECT PRIVILEGE FROM SESSION_PRIVS")
            privs = {str(x[0]) for x in cur.fetchall()}
            cur.execute("SELECT ROLE FROM SESSION_ROLES")
            roles = {str(x[0]) for x in cur.fetchall()}
        except Exception:  # noqa: BLE001
            pass
    cdb = str(facts.get("cdb", "")).upper() == "YES"
    has = bool(facts) and "vdb_error" not in facts

    def st(ok: bool | None) -> str:
        return "UNKNOWN" if ok is None else ("PASS" if ok else "FAIL")

    lm = facts.get("log_mode")
    out.append({"id": "archivelog", "label": "Database in ARCHIVELOG mode",
                "status": st(None if lm is None else lm == "ARCHIVELOG"),
                "detail": str(lm or facts.get("vdb_error") or "needs SELECT on V$DATABASE"),
                "check_sql": "SELECT LOG_MODE FROM V$DATABASE;",
                "fix_sql": "-- as SYSDBA, needs a short outage:\nSHUTDOWN IMMEDIATE;\nSTARTUP MOUNT;\n"
                           "ALTER DATABASE ARCHIVELOG;\nALTER DATABASE OPEN;"})
    sm = facts.get("supp_min")
    out.append({"id": "supplemental_min", "label": "Minimal supplemental logging (database level)",
                "status": st(None if sm is None else sm in ("YES", "IMPLICIT")), "detail": str(sm or ""),
                "check_sql": "SELECT SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE;",
                "fix_sql": "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;"})
    if catalog is not None:
        tabs = [t for t in catalog.tables if not want or t.table.fqn in want]
        miss = [t for t in tabs if not t.capture_enabled]
        out.append({"id": "supplemental_all", "label": "Supplemental logging (ALL) COLUMNS on every captured table",
                    "status": "FAIL" if miss else "PASS",
                    "detail": f"{len(miss)} of {len(tabs)} table(s) need it" if miss else f"{len(tabs)} table(s) ok",
                    "tables": [t.table.fqn for t in miss],
                    "check_sql": "SELECT OWNER, TABLE_NAME, LOG_GROUP_TYPE FROM ALL_LOG_GROUPS WHERE OWNER IN (...);",
                    "fix_sql": "\n".join(f"ALTER TABLE {q(t.table.schema_)}.{q(t.table.name)} ADD SUPPLEMENTAL LOG "
                                         f"DATA (ALL) COLUMNS;" for t in miss) or None})
        nokey = [t for t in tabs if not t.key_columns()]
        if nokey:
            out.append({"id": "primary_keys", "label": "Tables without primary key (surrogate id used)",
                        "status": "INFO", "detail": ", ".join(t.table.fqn for t in nokey), "fix_sql": None})
        unk = [f"{t.table.fqn}.{c.name} ({c.engine_type})" for t in tabs for c in t.columns
               if c.canonical_type.value == "UNKNOWN"]
        if unk:
            out.append({"id": "unsupported_types", "label": "Columns with unsupported types (quarantined)",
                        "status": "FAIL", "detail": ", ".join(unk[:20]), "fix_sql": None})
    else:
        out.append({"id": "supplemental_all", "label": "Supplemental logging (ALL) COLUMNS on every captured table",
                    "status": "UNKNOWN", "detail": "checked when the catalog is read",
                    "fix_sql": "ALTER TABLE \"<SCHEMA>\".\"<TABLE>\" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;"})
    if conn is not None:
        missing = [p for p in REQUIRED_PRIVS if p not in privs]
        if "SELECT_CATALOG_ROLE" not in roles and "SELECT ANY DICTIONARY" not in privs and not has:
            missing.append("SELECT_CATALOG_ROLE (or SELECT on the V$ views)")
        if "DBA" in roles:
            missing = []
        out.append({"id": "privileges", "label": "Capture user privileges (LogMiner, catalog, flashback)",
                    "status": "PASS" if not missing else "FAIL",
                    "detail": "missing: " + ", ".join(missing) if missing else "ok",
                    "check_sql": "SELECT PRIVILEGE FROM SESSION_PRIVS; SELECT ROLE FROM SESSION_ROLES;",
                    "fix_sql": grants_sql(user, list(tables or []), cdb)})
    else:
        out.append({"id": "privileges", "label": "Capture user privileges (LogMiner, catalog, flashback)",
                    "status": "UNKNOWN", "detail": "checked when the hub connects",
                    "fix_sql": grants_sql(user, list(tables or []), cdb)})
    if cdb:
        con = str(facts.get("con_name") or "")
        major = int(str(facts.get("version") or "0").split(".")[0] or 0)
        # SRC-ora-08: per-PDB LogMiner exists from 21c; on 19c a PDB session cannot mine (START_LOGMNR fails)
        pdb_ok = con == "CDB$ROOT" or major >= 21
        out.append({"id": "multitenant", "label": "Multitenant: mining container",
                    "status": "INFO" if pdb_ok and major else ("PASS" if pdb_ok else "FAIL" if major else "UNKNOWN"),
                    "detail": f"connected to {con}, version {facts.get('version') or '?'}: "
                    + ("CDB root (common user)" if con == "CDB$ROOT" else
                       "PDB-level LogMiner (Oracle 21c+; logs are chosen by Oracle)" if pdb_ok else
                       "LogMiner cannot run inside a PDB before Oracle 21c"),
                    "fix_sql": None if pdb_ok else "-- connect to CDB$ROOT as a common user (C##...) with the grants "
                                                   "above; keep settings.schemas = the PDB schemas"})
    old = facts.get("oldest_archived_scn") or facts.get("oldest_online_scn")
    if resume_scn is not None and old is not None:
        ok = int(old) <= resume_scn
        out.append({"id": "redo_retention", "label": "Redo for the resume position still available",
                    "status": "PASS" if ok else "FAIL",
                    "detail": f"oldest redo SCN {old}, resume SCN {resume_scn}",
                    "fix_sql": None if ok else "-- archived logs were removed: restore them from backup or re-snapshot"
                                               " the tables (ALELT never deletes archived logs)"})
    else:
        out.append({"id": "redo_retention", "label": "Archived log retention covers the capture lag",
                    "status": "INFO", "detail": f"oldest redo SCN {old}" if old else "keep archived logs for at "
                    "least the longest expected hub outage (RMAN deletion policy owned by the DBA)", "fix_sql": None})
    return out
