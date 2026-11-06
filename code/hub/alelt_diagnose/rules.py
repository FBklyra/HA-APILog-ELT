"""Stage A - deterministic signature rules (offline, always on). Each rule reads only the redacted evidence pack.

A rule returns a Finding (root cause, confidence, evidence refs, fix steps, optional suggested action) or None.
Suggested actions are executed only after approval in the console, except SAFE_IDEMPOTENT kinds explicitly enabled
by ALELT_DIAG_AUTO_ACTIONS.
"""
from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from .models import Classification as C
from .models import EvidencePack, Finding, IncidentKind, SuggestedAction
from .signature import normalize

Rule = Callable[[EvidencePack], Finding | None]
RULES: list[tuple[str, Rule]] = []


def rule(rule_id: str) -> Callable[[Rule], Rule]:
    def deco(fn: Rule) -> Rule:
        RULES.append((rule_id, fn))
        return fn
    return deco


def _any(text: str, *needles: str) -> str | None:
    return next((n for n in needles if n in text), None)


def _refs(p: EvidencePack, *extra: str) -> list[str]:
    t = p.trigger
    out = [f"incident.kind={t.kind.value}"]
    if t.job_id is not None:
        out.append(f"job:{t.job_id}")
    if t.batch_id:
        out.append(f"batch:{t.batch_id}")
    if t.table_fqn:
        out.append(f"table:{t.table_fqn}")
    if t.position:
        out.append(f"position:{t.position}")
    return out + [e for e in extra if e]


def _qrefs(p: EvidencePack, needle: str) -> list[str]:
    return [f"quarantine:{q['id']}@{q.get('position')}" for q in p.quarantine
            if needle in str(q.get("reason", "")).lower()][:5]


def _range(p: EvidencePack) -> dict[str, Any]:
    b = p.batch or {}
    first = next((q.get("position") for q in reversed(p.quarantine) if q.get("position")), None)
    last = next((q.get("position") for q in p.quarantine if q.get("position")), None)
    return {"connection_id": p.trigger.connection_id, "table": p.trigger.table_fqn,
            "from_pos": first or b.get("from_pos") or p.trigger.position,
            "to_pos": last or b.get("to_pos") or p.trigger.position}


def _redecode(p: EvidencePack, why: str) -> SuggestedAction | None:
    r = _range(p)
    if not (r["table"] and r["from_pos"] and r["to_pos"]):
        return None
    return SuggestedAction(kind="REDECODE_RANGE", params=r,
                           description=f"Re-decode {r['table']} {r['from_pos']}..{r['to_pos']} from the hub raw "
                                       f"archive ({why}); replay is idempotent (checkpoint), Db2 is not touched.")


# ---------------------------------------------------------------- decoder / catalog
@rule("codepage_mismatch")
def codepage_mismatch(p: EvidencePack) -> Finding | None:
    t = p.text()
    hit = _any(t, "bad_codepage", "codepage mismatch")
    if not hit or re.search(r"codepage \d+", t) is None or "codec can't decode" in t:
        return None                      # invalid bytes in a supported codepage = source data (source_data.py)
    cps = sorted({str(c.get("codepage")) for lay in p.catalog.get("table_layouts", {}).values()
                  for c in lay.get("columns", []) if c.get("codepage") not in (None, 0, 1208)})
    return Finding(
        rule_id="codepage_mismatch", title="Character data in an unexpected codepage",
        root_cause="Column bytes are not valid in the codepage recorded in the catalog "
                   f"({', '.join(cps) if cps else 'database codepage'}); the decoder quarantined the records (R7).",
        explanation="Some text values were written with a different character encoding than the catalog says. "
                    "Nothing was lost: the raw records are kept in quarantine.",
        confidence=0.85, evidence_refs=_refs(p, f"match:{hit}", *_qrefs(p, "codepage")),
        fix_steps=["Compare SYSCAT.COLUMNS.CODEPAGE for the column with the database codepage (catalog evidence).",
                   "If a non-1208 codepage is legitimate, add it to the decoder codepage map with a golden corpus "
                   "case (tests/golden/) and release a decoder build.",
                   "Re-decode the quarantined range from the raw archive after the decoder upgrade."],
        action=_redecode(p, "after decoder codepage fix"))


@rule("catalog_drift")
def catalog_drift(p: EvidencePack) -> Finding | None:
    t = p.text()
    hit = _any(t, "not in catalog", "bad_fixed_len", "bad_var_ref", "bad_null_flag", "layout", "framing")
    if not hit:
        return None
    versions = p.catalog.get("versions", [])
    ddl = [s for s in p.schema_changes if s.get("kind") in ("ADD_COLUMN", "DROP_COLUMN", "ALTER_TYPE", "NEW_TABLE")]
    layouts = p.catalog.get("table_layouts", {})
    differ = len({repr(v.get("columns")) for v in layouts.values()}) > 1
    conf = 0.6 + (0.15 if differ else 0) + (0.1 if ddl else 0) + (0.05 if len(versions) > 1 else 0)
    return Finding(
        rule_id="catalog_drift", title="Record decoded against the wrong table layout version",
        root_cause="The row image does not fit the catalog layout used for the batch "
                   f"({hit}); the table changed (DDL / DROP+CREATE reusing the table id) around this position and the "
                   "record was decoded against another catalog version.",
        explanation="The table structure changed in the source while changes were flowing. The affected records "
                    "were set aside, not dropped; they can be decoded again with the right layout.",
        confidence=min(conf, 0.9),
        evidence_refs=_refs(p, f"match:{hit}", f"catalog_versions:{[v.get('id') for v in versions]}",
                            *(f"schema_change:{s.get('kind')}@{s.get('position')}" for s in ddl[:3])),
        fix_steps=["Check catalog versions around the position: valid_from_pos must precede the first record of the "
                   "new layout (agent SNAPSHOT_CATALOG right after the DDL).",
                   "Request a fresh catalog snapshot (read-only SYSCAT query) if the newest layout is missing.",
                   "Re-decode the quarantined range from the raw archive with the correct version."],
        action=(SuggestedAction(kind="SNAPSHOT_CATALOG", params={"connection_id": p.trigger.connection_id,
                                                                 "agent_id": (p.agent or {}).get("id")},
                                description="Ask the agent for a fresh catalog snapshot (read-only SYSCAT).")
                if not differ and (p.agent or {}).get("id") else _redecode(p, "with the catalog version valid at the "
                                                                              "record position")))


@rule("unsupported_type")
def unsupported_type(p: EvidencePack) -> Finding | None:
    t = p.text()
    m = re.search(r"unsupported_type:?\s*(\S+)?\s*(decfloat|xml|long varchar|long vargraphic|\w+)?", t)
    hit = _any(t, "decfloat", "xml", "long varchar", "long vargraphic", "unsupported_type", "unknown type")
    if not (m or hit) or "value compression" in t:
        return None
    typ = (m.group(2) if m and m.group(2) else hit or "").upper()
    return Finding(
        rule_id="unsupported_type", title=f"Source type not supported by the decoder ({typ or 'unknown'})",
        root_cause=f"A column of type {typ or 'UNKNOWN'} is not decoded (DECFLOAT, XML and LONG types are not "
                   "implemented); affected records are quarantined (R7).",
        explanation="The table has a column type ALELT cannot read yet. Its changes are kept aside safely.",
        confidence=0.9, evidence_refs=_refs(p, f"match:{typ}", *_qrefs(p, "unsupported")),
        fix_steps=["Exclude the column (table config) if it is not needed downstream, or",
                   "implement the type in the decoder (C + Python stage) with a golden corpus case, then",
                   "re-decode the quarantined range from the raw archive."],
        action=None)


@rule("value_compression")
def value_compression(p: EvidencePack) -> Finding | None:
    if "value compression" not in p.text():
        return None
    return Finding(
        rule_id="value_compression", title="Value-compressed row format not supported",
        root_cause="The table uses Db2 value compression (COMPRESSION V/B); its row format is not implemented.",
        explanation="The source table is stored in a compressed format ALELT cannot decode yet.",
        confidence=0.9, evidence_refs=_refs(p, f"compression:{p.catalog.get('compression')}"),
        fix_steps=["Confirm SYSCAT.TABLES.COMPRESSION with the DBA (no change on Db2 by ALELT, R1).",
                   "Keep the table on the legacy pipeline until the decoder supports the format (OQ-04)."])


@rule("out_of_row_lob")
def out_of_row_lob(p: EvidencePack) -> Finding | None:
    t = p.text()
    if not _any(t, "lob_unavailable", "out-of-row lob", "lob manager"):
        return None
    return Finding(
        rule_id="out_of_row_lob", title="LOB value stored out of row",
        root_cause="The LOB is larger than its INLINE LENGTH and is logged separately; the change event was kept "
                   "with the column omitted (destination keeps its previous value).",
        explanation="A large text/binary value was not in the change record. The rest of the row was applied.",
        confidence=0.8, evidence_refs=_refs(p, "note:lob_unavailable"),
        fix_steps=["If the column must be exact, schedule a re-sync of the table (snapshot reads the value).",
                   "Ask the DBA whether INLINE LENGTH can cover typical sizes (DBA decision, R1)."],
        action=SuggestedAction(kind="RESYNC_TABLE", params={"table": p.trigger.table_fqn,
                                                            "connection_id": p.trigger.connection_id},
                               description="Re-sync the table to refresh out-of-row LOB values.")
        if p.trigger.table_fqn else None)


@rule("spool_corrupt")
def spool_corrupt(p: EvidencePack) -> Finding | None:
    t = p.text()
    hit = _any(t, "spool corrupt", "sha256/raw_bytes mismatch", "error -3 while decompressing", "incorrect header check",
               "zstd", "invalid block type")
    if not hit:
        return None
    return Finding(
        rule_id="spool_corrupt", title="Batch body corrupt in the hub spool",
        root_cause="The spooled body of the batch no longer matches its sha256 (disk / copy / restore problem); "
                   "the decode job cannot proceed past it.",
        explanation="A stored batch file on the hub is damaged, so replication for this connection is stuck at it.",
        confidence=0.9, evidence_refs=_refs(p, f"match:{hit}", f"spool_batch:{(p.batch or {}).get('id')}"),
        fix_steps=["Check the hub spool volume (disk errors, free space, restored files).",
                   "Restore the batch file from the raw archive stage if present, or",
                   "ask the agent to re-read from the batch from_pos (START_CAPTURE{from_pos}, two-person review).",
                   "Retry the decode job once the file is restored."],
        action=SuggestedAction(kind="RETRY_JOB", params={"job_id": p.trigger.job_id},
                               description="Retry the decode job (after the spool file is restored).")
        if p.trigger.job_id is not None else None)


@rule("decoder_defect")
def decoder_defect(p: EvidencePack) -> Finding | None:
    """Row image rejected although the catalog layout is consistent: most likely a decoder bug -> code fix."""
    t = p.text()
    hit = _any(t, "bad_bcd", "bad_bool", "bad_null_flag", "subrec_type", "not_user_data", "short:", "row image:")
    if not hit or _any(t, "bad_codepage", "unsupported_type", "value compression", "codec can't decode"):
        return None
    layouts = p.catalog.get("table_layouts", {})
    if len({repr(v.get("columns")) for v in layouts.values()}) > 1 or p.schema_changes:
        return None                      # layout changed around the position: catalog_drift explains it better
    return Finding(
        rule_id="decoder_defect", title="Decoder rejected a well-formed row image",
        root_cause=f"The decoder raised {hit.rstrip(':').upper()} on records whose table layout did not change; the "
                   "record format is valid for Db2, so the decoder misreads it (ALELT defect).",
        explanation="ALELT could not read some changes because of a bug in its decoder. The raw records are "
                    "kept; once a fixed version is released they are decoded again from the archive.",
        confidence=0.65, category=C.PLATFORM_DEFECT, evidence_refs=_refs(p, f"match:{hit}", *_qrefs(p, "row image")),
        fix_steps=["Ask the AI assistant for a code-fix proposal (patch + regression case), or reproduce with the "
                   "golden corpus (tests/golden/).", "Review the patch and ship it through the normal PR process.",
                   "After the fixed release, re-decode the affected range from the raw archive."],
        action=_redecode(p, "after the decoder fix is released"))


# ---------------------------------------------------------------- source / log
@rule("archive_log_unavailable")
def archive_log_unavailable(p: EvidencePack) -> Finding | None:
    t = p.text()
    hit = _any(t, "sql1273n", "sql2657n", "sql1265n", "log file not found", "log file cannot be found",
               "log no longer available", "archive log", "logs required", "retention")
    if not hit:
        return None
    return Finding(
        rule_id="archive_log_unavailable", title="Archive log no longer available (retention)",
        root_cause="The position to resume from is older than the archive-log retention; Db2 can no longer return "
                   "the log records (R3: ALELT never touches archive files).",
        explanation="The source database already removed the history ALELT needed. The affected tables must be "
                    "copied again from a fresh snapshot.",
        confidence=0.85, evidence_refs=_refs(p, f"match:{hit}", f"lag_s:{p.positions.get('lag_s')}"),
        fix_steps=["Confirm with the DBA which log files were pruned (LOGARCHMETH / retention policy).",
                   "Re-sync the affected tables (snapshot + catch-up from the current position).",
                   "Raise retention headroom or reduce lag (alert AleltArchiveRetentionHeadroomLow)."],
        action=SuggestedAction(kind="RESYNC_TABLE", params={"table": p.trigger.table_fqn,
                                                            "connection_id": p.trigger.connection_id},
                               description="Re-sync the table from a new snapshot.") if p.trigger.table_fqn else None)


@rule("capture_disabled")
def capture_disabled(p: EvidencePack) -> Finding | None:
    t = p.text()
    off = p.trigger.kind == IncidentKind.CAPTURE_LOST or p.catalog.get("capture_enabled") is False
    if not off and not _any(t, "data capture changes", "capture_disabled"):
        return None
    upg = [s for s in p.schema_changes if s.get("kind") in ("CAPTURE_DISABLED", "NEW_TABLE", "ALTER_TYPE")]
    return Finding(
        rule_id="capture_disabled", title="DATA CAPTURE CHANGES is off on an in-scope table",
        root_cause="The latest catalog shows the table without DATA CAPTURE CHANGES; changes since then are not in the "
                   "log. " + ("A recent schema change (often an application upgrade recreating the table) coincides."
                              if upg else ""),
        explanation="The source table stopped writing full change records, typically after an application upgrade "
                    "recreated it. Changes made since then must be recovered with a re-sync.",
        confidence=0.9 if off else 0.6,
        evidence_refs=_refs(p, f"capture_enabled:{p.catalog.get('capture_enabled')}",
                            *(f"schema_change:{s.get('kind')}@{s.get('at')}" for s in upg[:3])),
        fix_steps=["DBA: ALTER TABLE <t> DATA CAPTURE CHANGES (ALELT never runs DDL on Db2, R1).",
                   "Request a catalog snapshot to confirm, then re-sync the table (changes in between are missing).",
                   "Add the ALTER to the application upgrade checklist."],
        action=SuggestedAction(kind="RESYNC_TABLE", params={"table": p.trigger.table_fqn,
                                                            "connection_id": p.trigger.connection_id},
                               description="Re-sync the table once capture is re-enabled.")
        if p.trigger.table_fqn else None)


@rule("db2_auth_expired")
def db2_auth_expired(p: EvidencePack) -> Finding | None:
    hit = _any(p.text(), "sql30082n", "sql1639n", "password expired", "password has expired", "security reason 1",
               "security processing failed")
    if not hit:
        return None
    return Finding(
        rule_id="db2_auth_expired", title="Db2 connection credentials rejected / expired",
        root_cause="The agent's Db2 user cannot connect (expired or changed password, revoked account).",
        explanation="The source database refused the agent's login, so no new changes are read.",
        confidence=0.85, evidence_refs=_refs(p, f"match:{hit}"),
        fix_steps=["Rotate the Db2 password in the agent secret store (runbooks/rotate-certs-keys.md).",
                   "Restart / RESUME the agent; it resumes from its durable position (no data loss)."])


# ---------------------------------------------------------------- agent / gateway
@rule("agent_auth_rejected")
def agent_auth_rejected(p: EvidencePack) -> Finding | None:
    t = p.text()
    hit = _any(t, "client certificate not allow-listed", "bad agent token", "no agent tokens configured",
               "certificate has expired", "certificate expired", "x-alelt-protocol must be")
    if not hit and p.trigger.kind != IncidentKind.AGENT_AUTH:
        return None
    proto = hit == "x-alelt-protocol must be"
    return Finding(
        rule_id="agent_auth_rejected",
        title="Agent protocol version rejected" if proto else "Agent certificate / token rejected by the hub",
        root_cause=("The agent speaks another protocol version than the hub." if proto else
                    "The hub rejects the agent identity: certificate thumbprint not pinned in ALELT_AGENT_THUMBPRINTS, "
                    "certificate expired, or token not in ALELT_AGENT_TOKENS."),
        explanation="The hub does not recognise the agent, so batches are refused. The agent keeps its spool.",
        confidence=0.9 if hit else 0.7, evidence_refs=_refs(p, f"match:{hit}", f"detail:{p.trigger.detail.get('status')}"),
        fix_steps=(["Upgrade the agent or hub so both speak X-ALELT-Protocol 1."] if proto else
                   ["Compare the agent certificate thumbprint with ALELT_AGENT_THUMBPRINTS (nginx forwards it).",
                    "If the certificate was renewed, pin the new thumbprint (runbooks/rotate-certs-keys.md).",
                    "Check the certificate expiry (alert AleltCertExpiring30d)."]))


@rule("spool_full_backpressure")
def spool_full(p: EvidencePack) -> Finding | None:
    t = p.text()
    hit = _any(t, "spool_full", "spool full", "hub backlog full", "backpressure", "paused_by_governor")
    if not hit and p.trigger.kind not in (IncidentKind.BACKPRESSURE, IncidentKind.GOVERNOR_PAUSE):
        return None
    gov = "governor" in t or p.trigger.kind == IncidentKind.GOVERNOR_PAUSE
    backlog = p.metrics.get("unloaded_batches")
    return Finding(
        rule_id="spool_full_backpressure",
        title="Agent paused by its resource governor" if gov else "Spool full / hub backpressure",
        root_cause=("The agent governor paused reading to respect its CPU / memory limits (Job Object cap)." if gov else
                    "The hub refuses new batches (backlog over ALELT_MAX_UNLOADED_BATCHES) or the agent spool is full; "
                    "the agent pauses reading and never drops data."),
        explanation="Replication is slowed down on purpose to protect the source server or the hub. It resumes "
                    "automatically when the backlog drains.",
        confidence=0.8, evidence_refs=_refs(p, f"match:{hit}", f"unloaded_batches:{backlog}",
                                            f"agent_limits:{(p.agent or {}).get('limits')}"),
        fix_steps=["Check why the worker is not draining (failed decode job, destination down) - see other incidents.",
                   "If the load is legitimate, raise SET_LIMITS (agent) or ALELT_MAX_UNLOADED_BATCHES (hub).",
                   "Watch alelt_spool_bytes; the agent resumes by itself."],
        action=SuggestedAction(kind="RESUME_AGENT", params={"agent_id": (p.agent or {}).get("id")},
                               description="RESUME the agent after the cause is cleared.")
        if gov and (p.agent or {}).get("id") else None)


@rule("position_gap")
def position_gap(p: EvidencePack) -> Finding | None:
    t = p.text()
    hit = _any(t, "position gap", "gap/overlap", "expected_from")
    behind = p.positions.get("destination_vs_mirror")
    if not hit and p.trigger.kind != IncidentKind.POSITION_GAP and behind not in (-1,):
        return None
    if behind == -1 and not hit:
        return Finding(
            rule_id="position_mismatch", title="Destination checkpoint behind the hub position",
            root_cause="The destination checkpoint is older than the hub mirror (destination restored from backup or "
                       "checkpoint rolled back); the hub replays from the destination checkpoint.",
            explanation="The destination went back in time; ALELT will re-apply the missing range from its "
                        "archive.",
            confidence=0.75, evidence_refs=_refs(p, f"destination:{p.positions.get('destination_checkpoint')}",
                                                 f"mirror:{p.positions.get('mirror_position')}"),
            fix_steps=["Confirm whether the destination was restored.",
                       "Re-decode the range between the two positions from the raw archive (idempotent)."],
            action=SuggestedAction(kind="REDECODE_RANGE", params={
                "connection_id": p.trigger.connection_id, "table": p.trigger.table_fqn,
                "from_pos": p.positions.get("destination_checkpoint"), "to_pos": p.positions.get("mirror_position")},
                description="Re-decode the missing range from the raw archive."))
    d = p.trigger.detail
    return Finding(
        rule_id="position_gap", title="Gap / overlap in batch positions",
        root_cause="The agent sent a batch whose from_pos is not the previous next_pos "
                   f"(expected {d.get('expected_from')}, got {d.get('got') or p.trigger.position}); the hub refused it "
                   "(409) and the agent keeps it in its spool.",
        explanation="The agent and hub disagree on where reading should continue. Nothing is lost; the batch waits.",
        confidence=0.85, evidence_refs=_refs(p, f"expected:{d.get('expected_from')}", f"got:{d.get('got')}",
                                             f"agent_durable:{(p.agent or {}).get('durable_pos')}"),
        fix_steps=["Compare the agent durable position with the hub's last batch next_pos.",
                   "If the agent was reset, issue START_CAPTURE{from_pos=<hub next_pos>} (reposition is audited).",
                   "Never skip the range: a real gap requires a re-sync of the affected tables."])


@rule("clock_skew")
def clock_skew(p: EvidencePack) -> Finding | None:
    skews = [abs(float(e["clock_skew_s"])) for e in p.agent_events if e.get("clock_skew_s") is not None]
    skews += [abs(float(v["receive_skew_s"])) for v in p.catalog.get("versions", [])
              if v.get("receive_skew_s") is not None]
    hit = _any(p.text(), "immaturesignatureerror", "token is not yet valid", "certificate is not yet valid",
               "clock skew")
    big = max(skews, default=0.0)
    if big < 300 and not hit:
        return None
    return Finding(
        rule_id="clock_skew", title="Clock skew between agent host and hub",
        root_cause=f"Timestamps sent by the agent differ from hub time by up to {big:.0f} s"
                   + (f" ({hit})" if hit else "") + "; certificate/token validation and lag figures are affected.",
        explanation="The clocks of the source server and the hub disagree, which breaks security checks and timing.",
        confidence=0.75 if big >= 300 else 0.6, evidence_refs=_refs(p, f"max_skew_s:{big:.0f}"),
        fix_steps=["Check NTP / w32time on the Db2 server and the hub VM.",
                   "Lag metrics are hub-clock based; re-check alerts after the clocks converge."])


# ---------------------------------------------------------------- destination
@rule("warehouse_suspended")
def warehouse_suspended(p: EvidencePack) -> Finding | None:
    t = p.text()
    hit = _any(t, "000606", "no active warehouse", "resource monitor", "credit quota", "warehouse suspended",
               "is suspended")
    if not hit or ("warehouse" not in t and "quota" not in t and hit != "000606"):
        return None
    return Finding(
        rule_id="warehouse_suspended", title="Destination warehouse suspended / credit quota reached",
        root_cause="The destination warehouse is suspended or its resource monitor quota is exhausted; MERGE cannot "
                   "run. The checkpoint did not move, so nothing was partially applied (R6).",
        explanation="The destination has no compute available (paused or out of credits). Data waits on the hub.",
        confidence=0.85, evidence_refs=_refs(p, f"match:{hit}"),
        fix_steps=["Check the warehouse state and resource monitor (platform team).",
                   "Resume the warehouse or raise the quota; the worker retries automatically with backoff."])


@rule("permission_denied")
def permission_denied(p: EvidencePack) -> Finding | None:
    hit = _any(p.text(), "permission denied", "insufficient privileges", "not authorized", "sql0551n", "42501",
               "access denied", "insufficientprivilege")
    if not hit:
        return None
    return Finding(
        rule_id="permission_denied", title="Permission denied",
        root_cause="The service role lacks a privilege on the object named in the error (grant revoked, new schema "
                   "or table created by another role).",
        explanation="ALELT's account is not allowed to perform one operation it needs.",
        confidence=0.85, evidence_refs=_refs(p, f"match:{hit}"),
        fix_steps=["Identify the object and operation from the error text.",
                   "Re-apply the role grants from the destination migrations (ALELT_LOADER role).",
                   "The worker retries automatically; no replay needed (checkpoint unchanged)."])


@rule("destination_auth")
def destination_auth(p: EvidencePack) -> Finding | None:
    hit = _any(p.text(), "password authentication failed", "authentication failed", "invalid credentials",
               "jwt token is invalid", "incorrect username or password", "390100", "390144", "key pair")
    if not hit:
        return None
    return Finding(
        rule_id="destination_auth", title="Destination authentication failed",
        root_cause="The destination rejected the loader credentials (rotated password / key pair, expired secret).",
        explanation="The destination no longer accepts ALELT's login. Data waits on the hub.",
        confidence=0.85, evidence_refs=_refs(p, f"match:{hit}"),
        fix_steps=["Rotate / re-publish the destination credential in the secret store (R9).",
                   "Restart the worker to pick up the new secret; it resumes from the checkpoint."])


@rule("network_tls")
def network_tls(p: EvidencePack) -> Finding | None:
    hit = _any(p.text(), "connection refused", "could not connect", "timed out", "timeout expired",
               "name or service not known", "getaddrinfo failed", "no route to host", "connection reset",
               "certificate verify failed", "sslerror", "ssl error", "tls handshake", "server closed the connection", "network is unreachable",
               "destination checkpoint read timed out")
    if not hit:
        return None
    tls = hit in ("certificate verify failed", "sslerror", "ssl error", "tls handshake")
    return Finding(
        rule_id="network_tls", title="TLS handshake failure" if tls else "Network: endpoint unreachable",
        root_cause=("TLS validation failed (untrusted / expired certificate, proxy interception)." if tls else
                    "The endpoint could not be reached (service down, DNS, firewall)."),
        explanation="ALELT cannot reach a system it depends on. It retries automatically; nothing is lost.",
        confidence=0.75, evidence_refs=_refs(p, f"match:{hit}", f"destination:{p.positions.get('destination_error')}"),
        fix_steps=["Check the endpoint is up and reachable from the hub (DNS, firewall, proxy).",
                   "For TLS: check certificate chain and expiry, and the trust store of the hub image.",
                   "Retry the job once reachable (automatic backoff, max 5 min)."],
        action=SuggestedAction(kind="RETRY_JOB", params={"job_id": p.trigger.job_id},
                               description="Retry the job now instead of waiting for backoff.")
        if p.trigger.job_id is not None else None)


@rule("merge_failure")
def merge_failure(p: EvidencePack) -> Finding | None:
    hit = _any(p.text(), "numeric field overflow", "value too long", "out of range", "invalid input syntax",
               "duplicate key", "unique constraint", "merge into", "merge failed", "does not exist",
               "datatype mismatch", "cannot be cast", "undefinedcolumn", "undefinedtable")
    if not hit or p.trigger.kind not in (IncidentKind.DESTINATION_ERROR, IncidentKind.JOB_FAILED):
        return None
    return Finding(
        rule_id="merge_failure", title="MERGE / load failed at the destination",
        root_cause=f"The destination rejected the apply ({hit}): the landing table no longer matches the contract "
                   "(type width / missing column / key) - destination type mismatch vs golden DDL or schema drift not "
                   "applied. The checkpoint did not move (R6).",
        explanation="The destination table does not accept the incoming rows as they are; the batch is retried "
                    "after the table definition is fixed.",
        confidence=0.7, evidence_refs=_refs(p, f"match:{hit}", *(f"schema_change:{s.get('kind')}"
                                                                 for s in p.schema_changes[:3])),
        fix_steps=["Compare the landing table with config/golden DDL for the table (types, lengths, keys).",
                   "Approve / apply pending schema changes (console: Schema changes).",
                   "Retry the load job; replay is idempotent."],
        action=SuggestedAction(kind="RETRY_JOB", params={"job_id": p.trigger.job_id},
                               description="Retry the load job after the destination table is fixed.")
        if p.trigger.job_id is not None else None)


# ---------------------------------------------------------------- symptoms without a cause yet
@rule("agent_offline")
def agent_offline(p: EvidencePack) -> Finding | None:
    if p.trigger.kind != IncidentKind.AGENT_OFFLINE:
        return None
    age = (p.agent or {}).get("heartbeat_age_s")
    return Finding(
        rule_id="agent_offline", title="Agent silent (no heartbeat)",
        root_cause=f"No heartbeat for {age} s: the service ALELTAgent is stopped, the host is down, or the "
                   "outbound HTTPS path to the hub is blocked (R4: the agent always connects out).",
        explanation="The hub has not heard from the source-side agent. Changes accumulate in the Db2 log meanwhile.",
        confidence=0.6, evidence_refs=_refs(p, f"heartbeat_age_s:{age}", f"status:{(p.agent or {}).get('status')}"),
        fix_steps=["Check the Windows service ALELTAgent and its log on the Db2 server.",
                   "Check outbound HTTPS from the Db2 server to the hub (proxy / firewall / certificate).",
                   "Watch archive-retention headroom while the agent is down."])


@rule("lag_above_target")
def lag_above_target(p: EvidencePack) -> Finding | None:
    if p.trigger.kind != IncidentKind.LAG_ABOVE_TARGET:
        return None
    backlog = p.metrics.get("unloaded_batches", 0)
    failed = p.metrics.get("jobs_failed", 0)
    cause = ("the worker is blocked by failing jobs" if failed else
             "batches arrive faster than they are applied" if backlog else
             "the source is quiet or the agent is not sending")
    return Finding(
        rule_id="lag_above_target", title="Capture lag above target",
        root_cause=f"Lag {p.positions.get('lag_s')} s exceeds the target: {cause}.",
        explanation="Changes reach the destination later than the agreed target.",
        confidence=0.55, evidence_refs=_refs(p, f"unloaded_batches:{backlog}", f"jobs_failed:{failed}"),
        fix_steps=["Resolve failing-job incidents first.", "Check the destination merge duration "
                   "(alelt_merge_duration_seconds) and warehouse size.", "Check the agent heartbeat and limits."])


@rule("completion_stalled")
def completion_stalled(p: EvidencePack) -> Finding | None:
    if p.trigger.kind != IncidentKind.COMPLETION_STALLED:
        return None
    return Finding(
        rule_id="completion_stalled", title="Out-of-row values not fetched",
        root_cause="Keys whose out-of-row columns (LOB / long text) must be read by key were not answered within "
                   "ALELT_COMPLETION_STALL_S: the agent is offline, paused, refuses VERIFY_TABLE for the table "
                   "(allow-list) or the source read fails. The rest of the row is applied; the stream keeps flowing.",
        explanation="The destination rows exist but some large column values are still missing for the listed keys.",
        confidence=0.6, evidence_refs=_refs(p, f"table:{p.trigger.table_fqn}"),
        fix_steps=["Check the agent heartbeat and its log for VERIFY_TABLE errors (NOT_ALLOWED, SOURCE_ERROR).",
                   "Check that the table is in the agent's snapshot.allowed_tables (SCHEMA.TABLE or SCHEMA.*).",
                   "The incident resolves itself when the table's pending completions drain."])


@rule("completion_oversize")
def completion_oversize(p: EvidencePack) -> Finding | None:
    if p.trigger.kind != IncidentKind.COMPLETION_OVERSIZE:
        return None
    return Finding(
        rule_id="completion_oversize", title="Out-of-row value larger than the edge result cap",
        root_cause="A single row's out-of-row columns exceed the 8 MiB VERIFY_TABLE result cap; the value cannot be "
                   "fetched by key. The fetch is retried hourly.",
        explanation="One row keeps an old (or empty) large value until the table is re-synchronised.",
        confidence=0.7, evidence_refs=_refs(p, f"table:{p.trigger.table_fqn}"),
        fix_steps=["Re-sync the table (the snapshot reads the value).",
                   "Or exclude the column from replication in tables.yaml."],
        action=SuggestedAction(kind="RESYNC_TABLE", params={"table": p.trigger.table_fqn,
                                                            "connection_id": p.trigger.connection_id},
                               description="Re-sync the table to read the large value.")
        if p.trigger.table_fqn else None)


@rule("recon_mismatch")
def recon_mismatch(p: EvidencePack) -> Finding | None:
    if p.trigger.kind != IncidentKind.RECON_MISMATCH:
        return None
    q = sum(p.quarantine_reasons.values())
    return Finding(
        rule_id="recon_mismatch", title="Reconciliation mismatch",
        root_cause="Source and destination differ for the table" + (f"; {q} quarantined record(s) on this connection "
                                                                    "are the likely difference." if q else "."),
        explanation="A data check found differences between the source and the destination for this table.",
        confidence=0.6 if q else 0.4, evidence_refs=_refs(p, f"quarantined:{q}"),
        fix_steps=["Open the recon drill-down (first offending key / position).",
                   "Resolve quarantine for the table, then re-decode the range or re-sync the table.",
                   "Re-run the check."],
        action=_redecode(p, "to repair the mismatching range") if q else None)


@dataclass(frozen=True)
class KnownIssue:
    id: str
    kind: str
    pattern: str
    title: str
    root_cause: str
    fix_steps: list[str]
    category: str = "UNKNOWN"


def match_known(p: EvidencePack, known: Sequence[KnownIssue]) -> list[Finding]:
    """Learning loop: human-confirmed causes are matched first (normalized error text, same incident kind)."""
    norm = normalize(p.trigger.error)
    out = []
    for k in known:
        if k.kind == p.trigger.kind.value and k.pattern and (k.pattern == norm or (len(k.pattern) > 12
                                                                                   and k.pattern in p.text())):
            out.append(Finding(rule_id=f"known:{k.id}", title=k.title, root_cause=k.root_cause,
                               explanation=f"Seen before and confirmed by an operator: {k.root_cause}",
                               confidence=0.95, evidence_refs=_refs(p, f"known_issue:{k.id}"),
                               category=C(k.category) if k.category in C.__members__ else C.UNKNOWN,
                               fix_steps=list(k.fix_steps)))
    return out


CATEGORY: dict[str, C] = {
    "codepage_mismatch": C.PLATFORM_DEFECT, "unsupported_type": C.PLATFORM_DEFECT, "value_compression": C.PLATFORM_DEFECT,
    "out_of_row_lob": C.PLATFORM_DEFECT, "catalog_drift": C.PLATFORM_DEFECT, "decoder_defect": C.PLATFORM_DEFECT, "spool_corrupt": C.INFRASTRUCTURE,
    "archive_log_unavailable": C.INFRASTRUCTURE, "capture_disabled": C.CONFIGURATION, "db2_auth_expired": C.CONFIGURATION,
    "agent_auth_rejected": C.CONFIGURATION, "spool_full_backpressure": C.INFRASTRUCTURE, "position_gap": C.INFRASTRUCTURE,
    "position_mismatch": C.DESTINATION, "clock_skew": C.INFRASTRUCTURE, "warehouse_suspended": C.DESTINATION,
    "permission_denied": C.DESTINATION, "destination_auth": C.CONFIGURATION, "network_tls": C.INFRASTRUCTURE,
    "merge_failure": C.DESTINATION, "agent_offline": C.INFRASTRUCTURE,
}


def evaluate(p: EvidencePack, known: Sequence[KnownIssue] = ()) -> list[Finding]:
    """All matching findings, best first. Never raises (a broken rule is skipped)."""
    from .source_data import source_finding

    out = match_known(p, known)
    for rid, fn in [*RULES, ("source_data", source_finding)]:
        try:
            f = fn(p)
        except Exception:  # noqa: BLE001
            continue
        if f is not None:
            if f.category == C.UNKNOWN:
                f.category = CATEGORY.get(f.rule_id, C.UNKNOWN)
            if f.rule_id == "catalog_drift" and f.action and f.action.kind == "SNAPSHOT_CATALOG":
                f.category = C.CONFIGURATION       # missing catalog snapshot, not a decoder defect
            out.append(f)
    out.sort(key=lambda f: f.confidence, reverse=True)
    if not out:
        out.append(Finding(
            rule_id="unclassified", title="No known signature matched",
            root_cause="No deterministic rule matched this failure.",
            explanation="ALELT could not classify this error automatically. The evidence pack lists what was "
                        "collected; ask the AI assistant (if enabled) or follow the incident runbook.",
            confidence=0.1, evidence_refs=_refs(p),
            fix_steps=["Read the error and traceback in the evidence pack.", "Follow runbooks/README.md#incidents.",
                       "Once understood, confirm the root cause so the next occurrence is matched automatically."]))
    return out
