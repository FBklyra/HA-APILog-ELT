"""SQL Server CDC prerequisites with copy-paste DBA SQL. ALELT never executes fix_sql (R1)."""
from __future__ import annotations

from typing import Any

from alelt_common.models import CatalogSnapshot, TableRef

from .catalog import qi
from .position import increment, lsn_text


def _st(ok: bool | None) -> str:
    return "UNKNOWN" if ok is None else ("PASS" if ok else "FAIL")


def checks(conn: Any | None, catalog: CatalogSnapshot | None, tables: list[TableRef] | None,
           settings: dict[str, Any], resume_lsn: bytes | None = None) -> list[dict[str, Any]]:
    db = str(settings.get("database") or "<database>")
    user = str(settings.get("user") or "<capture_user>")
    dev_scan = bool(settings.get("dev_manual_scan"))
    out: list[dict[str, Any]] = []
    f: dict[str, Any] = {}
    instances: list[tuple[Any, ...]] = []
    if conn is not None:
        cur = conn.cursor()

        def one(sql: str, args: tuple[Any, ...] = ()) -> Any:
            try:
                if args:
                    cur.execute(sql, args)
                else:
                    cur.execute(sql)
                r = cur.fetchone()
                return r
            except Exception as e:  # noqa: BLE001 - missing permission shows as UNKNOWN, never raises
                return e

        r = one("SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(64)), "
                "CAST(SERVERPROPERTY('Edition') AS nvarchar(128)), DB_NAME()")
        if isinstance(r, tuple):
            f.update(version=r[0], edition=r[1], db=r[2])
            db = r[2] or db
        r = one("SELECT is_cdc_enabled, snapshot_isolation_state_desc FROM sys.databases WHERE name = DB_NAME()")
        if isinstance(r, tuple):
            f.update(cdc_db=bool(r[0]), snapshot=r[1])
        r = one("SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE N'SQL Server Agent%'")
        f["agent"] = r[0] if isinstance(r, tuple) else ("NO_ROW" if r is None else f"error: {r}")
        r = one("SELECT COUNT(*) FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = N'capture'")
        f["capture_job"] = int(r[0]) if isinstance(r, tuple) else None
        r = one("SELECT retention FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = N'cleanup'")
        f["retention_min"] = int(r[0]) if isinstance(r, tuple) and r[0] is not None else None
        r = one("SELECT IS_ROLEMEMBER(N'db_owner'), SUSER_SNAME()")
        if isinstance(r, tuple):
            f.update(db_owner=bool(r[0]), login=r[1])
        if f.get("cdc_db"):
            try:
                cur.execute("SELECT ct.capture_instance, OBJECT_SCHEMA_NAME(ct.source_object_id), "
                            "OBJECT_NAME(ct.source_object_id), ct.role_name, "
                            "HAS_PERMS_BY_NAME(QUOTENAME(OBJECT_SCHEMA_NAME(ct.source_object_id)) + '.' + "
                            "QUOTENAME(OBJECT_NAME(ct.source_object_id)), 'OBJECT', 'SELECT'), "
                            "CASE WHEN ct.role_name IS NULL THEN 1 ELSE IS_ROLEMEMBER(ct.role_name) END, "
                            "sys.fn_cdc_get_min_lsn(ct.capture_instance) FROM cdc.change_tables ct")
                instances = list(cur.fetchall())
            except Exception as e:  # noqa: BLE001
                f["instances_error"] = str(e)

    has = bool(f)
    out.append({"id": "server", "label": "SQL Server version / edition", "status": "INFO" if has else "UNKNOWN",
                "detail": f"{f.get('version')} {f.get('edition')} database {db}" if has else "not connected",
                "check_sql": "SELECT SERVERPROPERTY('ProductVersion'), SERVERPROPERTY('Edition');"})
    out.append({"id": "cdc_db", "label": "CDC enabled on the database",
                "status": _st(f.get("cdc_db") if "cdc_db" in f else None),
                "detail": f"sys.databases.is_cdc_enabled = {f.get('cdc_db')}",
                "check_sql": "SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME();",
                "fix_sql": f"USE {qi(db)};\nEXEC sys.sp_cdc_enable_db;   -- sysadmin"})

    agent = str(f.get("agent") or "")
    agent_ok = None if not has or agent.startswith("error") or agent == "NO_ROW" else agent.upper() == "RUNNING"
    detail = f"SQL Server Agent: {agent or 'unknown (needs VIEW SERVER STATE)'}"
    if agent_ok is False:
        detail += (" - the CDC capture job never runs, change tables stay empty."
                   + (" Connector option dev_manual_scan is ON: sys.sp_cdc_scan is called before each read "
                      "(DEV / TEST ONLY, never on a production source)." if dev_scan else ""))
    out.append({"id": "agent", "label": "SQL Server Agent running (runs the CDC capture job)",
                "status": _st(agent_ok), "detail": detail,
                "check_sql": "SELECT servicename, status_desc FROM sys.dm_server_services;",
                "fix_sql": "-- start the SQL Server Agent service and set it to start automatically\n"
                           "-- Windows: services.msc -> SQL Server Agent (MSSQLSERVER) -> Automatic + Start\n"
                           "-- Linux:   sudo /opt/mssql/bin/mssql-conf set sqlagent.enabled true && "
                           "sudo systemctl restart mssql-server"})
    cj = f.get("capture_job")
    out.append({"id": "capture_job", "label": "CDC capture job exists",
                "status": _st(None if cj is None else cj > 0),
                "detail": "msdb.dbo.cdc_jobs capture rows: " + ("unknown (needs SELECT on msdb.dbo.cdc_jobs)"
                                                               if cj is None else str(cj)),
                "check_sql": "SELECT * FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID();",
                "fix_sql": f"USE {qi(db)};\nEXEC sys.sp_cdc_add_job @job_type = N'capture';"})

    want = {t.fqn.upper() for t in tables or []}
    if catalog is not None and not want:
        want = {t.table.fqn.upper() for t in catalog.tables}
    by_tab = {f"{s}.{t}".upper(): (ci, s, t, role, sel, mem, mn) for ci, s, t, role, sel, mem, mn in instances}
    cat_tabs = {t.table.fqn.upper(): t for t in (catalog.tables if catalog else [])}
    for fq in sorted(want):
        ti = cat_tabs.get(fq)
        sch, _, tab = (ti.table.schema_, ".", ti.table.name) if ti else fq.partition(".")
        inst = by_tab.get(fq)
        enable = (f"USE {qi(db)};\nEXEC sys.sp_cdc_enable_table @source_schema = N'{sch}', @source_name = N'{tab}',"
                  " @role_name = NULL, @supports_net_changes = 0;")
        ok: bool | None = None if not has or "instances_error" in f else inst is not None
        out.append({"id": f"cdc_table:{fq}", "label": f"CDC enabled on {sch}.{tab}", "status": _st(ok),
                    "detail": f"capture instance {inst[0]}" if inst else (f.get("instances_error") or "not tracked"),
                    "check_sql": "SELECT capture_instance FROM cdc.change_tables "
                                 f"WHERE source_object_id = OBJECT_ID(N'{sch}.{tab}');",
                    "fix_sql": enable})
        if inst is None:
            continue
        ci, _s, _t, role, sel, mem, _mn = inst
        perm_ok = bool(sel) and bool(mem)
        fix = [f"USE {qi(db)};", f"GRANT SELECT ON {qi(sch)}.{qi(tab)} TO {qi(user)};",
               f"GRANT SELECT ON SCHEMA::cdc TO {qi(user)};"]
        if role:
            fix.append(f"ALTER ROLE {qi(role)} ADD MEMBER {qi(user)};")
        out.append({"id": f"perm:{fq}", "label": f"Capture user can read {sch}.{tab} changes",
                    "status": _st(perm_ok),
                    "detail": f"SELECT={bool(sel)} gating role {role or '(none)'} member={bool(mem)}",
                    "check_sql": f"SELECT HAS_PERMS_BY_NAME(N'{sch}.{tab}', 'OBJECT', 'SELECT');",
                    "fix_sql": "\n".join(fix)})
    snap = f.get("snapshot")
    out.append({"id": "snapshot_isolation", "label": "ALLOW_SNAPSHOT_ISOLATION (consistent initial load)",
                "status": _st(None if snap is None else snap == "ON"),
                "detail": f"snapshot_isolation_state = {snap}; when OFF the initial load reads READ COMMITTED "
                          "chunks (overlap with the change stream is replayed idempotently)",
                "check_sql": "SELECT snapshot_isolation_state_desc FROM sys.databases WHERE name = DB_NAME();",
                "fix_sql": f"ALTER DATABASE {qi(db)} SET ALLOW_SNAPSHOT_ISOLATION ON;"})
    ret = f.get("retention_min")
    out.append({"id": "retention", "label": "CDC cleanup retention covers the longest hub outage",
                "status": "INFO" if ret is not None else "UNKNOWN",
                "detail": f"cleanup retention = {ret} min ({(ret or 0) / 1440:.1f} days)" if ret is not None
                else "unknown (needs SELECT on msdb.dbo.cdc_jobs)",
                "check_sql": "SELECT retention FROM msdb.dbo.cdc_jobs WHERE job_type = N'cleanup';",
                "fix_sql": "EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = 10080;  -- 7 days"})
    if resume_lsn is not None:
        behind = [f"{ci} (min_lsn {lsn_text(bytes(mn))})" for ci, *_r, mn in instances
                  if mn is not None and any(bytes(mn)) and increment(resume_lsn) < bytes(mn)]
        out.append({"id": "resume_retained", "label": "Resume position still retained by CDC",
                    "status": "FAIL" if behind else ("PASS" if instances else "UNKNOWN"),
                    "detail": (f"resume {lsn_text(resume_lsn)} older than " + ", ".join(behind)
                               + ": changes were purged, resync these tables") if behind
                    else f"resume {lsn_text(resume_lsn)} retained",
                    "check_sql": "SELECT capture_instance, sys.fn_cdc_get_min_lsn(capture_instance) "
                                 "FROM cdc.change_tables;",
                    "fix_sql": "-- resync (snapshot) the listed tables from the console, then raise retention:\n"
                               "EXEC sys.sp_cdc_change_job @job_type = N'cleanup', @retention = 10080;"})
    return out
