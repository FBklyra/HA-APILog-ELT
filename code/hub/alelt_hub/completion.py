"""Out-of-row completion in the data path (RC1, ADR-013).

A row image whose out-of-row columns (Db2 LOB / long VARCHAR, Postgres TOAST, MySQL minimal images, SQL Server update
masks, Oracle LOB ops) are absent is a correct, partial change: the main stream applies it at once and this module
completes the missing columns by key, in batches, through the main stream's own flushes. It is not an integrity event:
no table hold, no incident for the normal case. Sources only decide WHAT to signal (`needs_keyed_reread` with keys +
`detail.columns`); nothing here knows LOBs, TOAST or update masks.

Entries (`table_completion`, one per (connection, table, key)):
  PENDING  -> INFLIGHT (fetch issued: VERIFY_TABLE{rows} through the agent, or SourceConnector.keyed_reread for remote
              sources in the control lane) -> deleted once a fetch read at R >= last_pos was flushed;
  CANCELLED (a DELETE of the key committed after the newest signal) -> deleted at the next step.
Folding: a new signal extends a PENDING entry (last_pos = max, columns union); an INFLIGHT entry gets last_pos beyond
its read position, so it goes back to PENDING after the current result is applied (one more round).

Positions: signals are queued when their transaction COMMITS (commit position), so a fetch issued after it reads the
committed value; R (`position_before` of the edge read) >= last_pos means the read saw the newest signalled value.

Apply: results become `ChangeSet{txid="completion:<command>", commit_position=R}` of UPDATE events `after = key + the
fetched out-of-row columns only`, inserted into the main-stream buffer before the first buffered change committed
after R (applied "at R": every later buffered change wins), flushed with the main stream (same transaction as the
checkpoint, ADR-005). Tables held by the schedule are completed too (only the signalled columns are written; the
signals come from ranges already applied); tables frozen by an integrity / schema / snapshot hold or not STREAMING
are not fetched. When the main stream has nothing to flush (idle source), completion rows are applied through a
transient stream `<cid>#completion:<id>` whose checkpoint row is dropped after use (FIX-IMPL-NOTES D-03).

Completion floor (ADR-005 amendment): main-stream flushes carry `{"<cid>#t:<table>": F_t}` with F_t = min(first_pos)
of the table's open entries; `rebuild()` recreates the entries from the spool after a metadata loss (DR) and
`alelt_hub.retention` keeps the spool from min(first_seq).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from sqlalchemy import Engine, delete, func, select, update

from alelt_common.canonical_jsonl import decode_value, encode_value
from alelt_common.models import VERIFY_MAX_KEY_VALUES, ChangeEvent, ChangeKind, ChangeSet, Position, TableRef
from alelt_common.util import utcnow

from . import jobs
from .db import agent_command, table_completion
from .dialect import insert
from .metrics import REGISTRY

log = logging.getLogger("alelt_hub.completion")
PENDING, INFLIGHT, CANCELLED = "PENDING", "INFLIGHT", "CANCELLED"
TXID_PREFIX = "completion:"
REMOTE_PREFIX = "remote:"
FROZEN_KINDS = ("integrity", "schema", "snapshot", "ddl")   # holds that freeze a table (no completion)

for _n, _t in {
    "alelt_completion_pending": "out-of-row completion entries waiting per table",
    "alelt_completion_oldest_seconds": "age of the oldest open completion entry per connection",
    "alelt_completion_commands_total": "keyed fetch commands issued for out-of-row completion",
    "alelt_completion_rows_total": "completion results (completed / missing / requeued / cancelled / truncated)",
    "alelt_completion_fetch_seconds": "issue -> answer time of a completion fetch",
    "alelt_signals_deferred_total": "out-of-row signals not queued by the main stream (table held / not streaming)",
}.items():
    REGISTRY.describe(_n, _t)


def _env(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)) or default)
    except ValueError:
        return default


def keys_per_cmd() -> int:
    return max(1, min(int(_env("ALELT_COMPLETION_KEYS_PER_CMD", 1000)), VERIFY_MAX_KEY_VALUES))


def cmd_bytes() -> int:
    return int(_env("ALELT_COMPLETION_CMD_BYTES", 48 * 1024))


def inflight_cmds() -> int:
    return max(1, int(_env("ALELT_COMPLETION_INFLIGHT_CMDS", 4)))


def stall_s() -> float:
    return _env("ALELT_COMPLETION_STALL_S", 600)


def pending_max() -> int:
    return int(_env("ALELT_COMPLETION_PENDING_MAX", 200_000))


def cmd_timeout_s() -> float:
    return _env("ALELT_COMPLETION_CMD_TIMEOUT_S", 360)


def poll_s() -> float:
    return _env("ALELT_COMPLETION_POLL_S", 1)


def _ref(fqn: str) -> TableRef:
    s, _, n = fqn.partition(".")
    return TableRef(schema=s, name=n)


def encode_key(key: dict[str, Any]) -> dict[str, Any]:
    return {k: encode_value(v) for k, v in key.items()}


def key_id(enc: dict[str, Any]) -> str:
    """sha256[:32] of the canonical JSON of the (tagged) key dict."""
    return hashlib.sha256(json.dumps(enc, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()[:32]


@dataclass
class Signal:
    """One needs_keyed_reread key of a committed transaction."""

    fqn: str
    key: dict[str, Any]                  # canonical (decoded) values
    columns: list[str]
    pos: str                             # commit position of the transaction
    seq: int | None = None               # batch seq of the commit


@dataclass
class Injected:
    """A completion result put into the main-stream buffer (in memory until flushed)."""

    command_id: str
    read_pos: str | None
    entries: dict[tuple[str, str], str] = field(default_factory=dict)   # (fqn, key_id) -> last_pos when collected


# ------------------------------------------------------------------ signals -> entries
def signals_of(events: list[ChangeEvent]) -> dict[str, list[tuple[dict[str, Any], list[str]]]]:
    """txid -> [(key, columns)] of needs_keyed_reread signals WITH keys in `events` (key ranges / keyless signals stay
    integrity: the caller routes them)."""
    from alelt_common.completeness import RecordClass, signal_of

    out: dict[str, list[tuple[Any, list[str]]]] = {}
    for e in events:
        sig = signal_of(e)
        if sig is None or sig.cls is not RecordClass.NEEDS_KEYED_REREAD or not sig.keys or sig.table is None:
            continue
        cols = [str(c) for c in (sig.detail.get("columns") or [])]
        out.setdefault(e.txid, []).extend((sig.table.fqn, k, cols) for k in sig.keys)   # type: ignore[misc]
    return out  # type: ignore[return-value]


def is_completion_signal(q: Any) -> bool:
    """A quarantine-equivalent signal record that completion handles (keys known)."""
    return (q.reason or "").startswith("signal needs_keyed_reread")


class SignalTracker:
    """Signals of open transactions of one stream: released with the COMMIT (dropped on ROLLBACK), so an entry is only
    queued for committed changes and carries the commit position."""

    def __init__(self) -> None:
        self.open: dict[str, list[tuple[str, dict[str, Any], list[str]]]] = {}

    def reset(self) -> None:
        self.open.clear()

    def feed(self, events: list[ChangeEvent]) -> None:
        for txid, items in signals_of(events).items():
            self.open.setdefault(txid, []).extend(items)   # type: ignore[arg-type]
        for e in events:
            if e.kind is ChangeKind.ROLLBACK:
                self.open.pop(e.txid, None)

    def committed(self, sets: list[ChangeSet], seq: int) -> list[Signal]:
        out: list[Signal] = []
        for cs in sets:
            for fqn, key, cols in self.open.pop(cs.txid, []):
                out.append(Signal(fqn, key, cols, cs.commit_position.text, seq))
        return out


def enqueue_signals(engine: Engine, p: Any, sigs: list[Signal]) -> int:
    """Fold signals into `table_completion` (one entry per (table, key)). Returns entries created or extended."""
    if not sigs:
        return 0
    cmp = p.source.compare
    eng = p.engine_name

    def later(a: str | None, b: str | None) -> bool:   # a > b
        if a is None:
            return False
        if b is None:
            return True
        try:
            return cmp(Position(engine=eng, text=a), Position(engine=eng, text=b)) > 0
        except Exception:  # noqa: BLE001
            return a > b
    folded: dict[tuple[str, str], dict[str, Any]] = {}
    for s in sigs:
        enc = encode_key(s.key)
        k = (s.fqn, key_id(enc))
        f = folded.get(k)
        if f is None:
            folded[k] = {"key": enc, "columns": sorted(set(s.columns)), "first": s.pos, "last": s.pos, "seq": s.seq}
        else:
            f["columns"] = sorted(set(f["columns"]) | set(s.columns))
            if later(s.pos, f["last"]):
                f["last"] = s.pos
            if later(f["first"], s.pos):
                f["first"], f["seq"] = s.pos, s.seq
    n = 0
    now = utcnow()
    by_table: dict[str, list[str]] = {}
    for fqn, kid in folded:
        by_table.setdefault(fqn, []).append(kid)
    with engine.begin() as c:
        for fqn, kids in by_table.items():
            cur: dict[str, dict[str, Any]] = {}
            for i in range(0, len(kids), 500):
                for r in c.execute(select(table_completion).where(
                        table_completion.c.connection_id == p.cid, table_completion.c.table_fqn == fqn,
                        table_completion.c.key_id.in_(kids[i:i + 500]))).mappings():
                    cur[str(r["key_id"])] = dict(r)
            for kid in kids:
                f = folded[(fqn, kid)]
                r = cur.get(kid)
                if r is None or r["status"] == CANCELLED:
                    vals = dict(key=f["key"], columns=f["columns"], first_pos=f["first"], last_pos=f["last"],
                                first_seq=f["seq"], status=PENDING, command_id=None, read_pos=None, attempts=0,
                                next_attempt_at=now, last_error=None, updated_at=now)
                    c.execute(insert(table_completion).values(connection_id=p.cid, table_fqn=fqn, key_id=kid, **vals)
                              .on_conflict_do_update(index_elements=["connection_id", "table_fqn", "key_id"], set_=vals))
                else:
                    cols = sorted(set(r["columns"] or []) | set(f["columns"]))
                    vals = {"columns": cols, "updated_at": now}
                    if later(f["last"], r["last_pos"]):
                        vals["last_pos"] = f["last"]
                    c.execute(update(table_completion).where(
                        table_completion.c.connection_id == p.cid, table_completion.c.table_fqn == fqn,
                        table_completion.c.key_id == kid).values(**vals))
                n += 1
    _overflow_check(engine, p, list(by_table))
    with engine.begin() as c:
        jobs.enqueue(c, "completion", {"connection_id": p.cid}, dedup_key=jobs.marker_key("completion", p.cid),
                     delay_s=poll_s())
    return n


def _overflow_check(engine: Engine, p: Any, fqns: list[str]) -> None:
    """More than ALELT_COMPLETION_PENDING_MAX entries for one table: the table is resynced instead (the snapshot replaces
    every value; entries are dropped when the resync snapshot is taken)."""
    cap = pending_max()
    with engine.connect() as c:
        over = [str(f) for f, n in c.execute(select(table_completion.c.table_fqn, func.count()).where(
            table_completion.c.connection_id == p.cid, table_completion.c.table_fqn.in_(fqns))
            .group_by(table_completion.c.table_fqn)).all() if int(n) > cap]
    for fqn in over:
        from . import integrity

        integrity.completion_unavailable(p, fqn, f"more than {cap} pending out-of-row completions: resync")


def cancel_deleted(engine: Engine, p: Any, sets: list[ChangeSet]) -> int:
    """A DELETE committed after an entry's newest signal cancels it (results of a cancelled entry are dropped)."""
    tabs = tables_with_entries(engine, p)
    if not tabs:
        return 0
    want: dict[str, dict[str, str]] = {}
    for cs in sets:
        for e in cs.events:
            if e.kind is not ChangeKind.DELETE or e.table is None or e.table.fqn not in tabs:
                continue
            info = p.tables.get(e.table)
            img = e.before or e.after or {}
            if info is None or not info.key_columns():
                continue
            key = {c.name: img.get(c.name) for c in info.key_columns()}
            want.setdefault(e.table.fqn, {})[key_id(encode_key(key))] = cs.commit_position.text
    n = 0
    if not want:
        return 0
    cmp, eng = p.source.compare, p.engine_name
    with engine.begin() as c:
        for fqn, kids in want.items():
            rows = c.execute(select(table_completion.c.key_id, table_completion.c.last_pos, table_completion.c.status)
                             .where(table_completion.c.connection_id == p.cid, table_completion.c.table_fqn == fqn,
                                    table_completion.c.key_id.in_(list(kids)))).all()
            for kid, last, st in rows:
                if st == CANCELLED:
                    continue
                if last is None or cmp(Position(engine=eng, text=kids[str(kid)]), Position(engine=eng, text=last)) > 0:
                    c.execute(update(table_completion).where(
                        table_completion.c.connection_id == p.cid, table_completion.c.table_fqn == fqn,
                        table_completion.c.key_id == kid).values(status=CANCELLED, updated_at=utcnow()))
                    n += 1
    if n:
        REGISTRY.inc("alelt_completion_rows_total", float(n), connection_id=p.cid, result="cancelled")
        with engine.begin() as c:
            jobs.enqueue(c, "completion", {"connection_id": p.cid}, dedup_key=jobs.marker_key("completion", p.cid),
                         delay_s=poll_s())
    return n


def tables_with_entries(engine: Engine, p: Any) -> set[str]:
    with engine.connect() as c:
        return {str(f) for f, in c.execute(select(table_completion.c.table_fqn).where(
            table_completion.c.connection_id == p.cid).group_by(table_completion.c.table_fqn))}


def pending_counts(engine: Engine, cid: str | None = None) -> dict[tuple[str, str], int]:
    q = select(table_completion.c.connection_id, table_completion.c.table_fqn, func.count()).where(
        table_completion.c.status != CANCELLED)
    if cid is not None:
        q = q.where(table_completion.c.connection_id == cid)
    with engine.connect() as c:
        return {(str(a), str(b)): int(n) for a, b, n in c.execute(
            q.group_by(table_completion.c.connection_id, table_completion.c.table_fqn)).all()}


def floors(engine: Engine, p: Any) -> dict[str, str]:
    """Completion floor per table: min(first_pos) of open entries -> {"<cid>#t:<fqn>": F_t}."""
    cmp, eng = p.source.compare, p.engine_name
    best: dict[str, str] = {}
    with engine.connect() as c:
        for fqn, pos in c.execute(select(table_completion.c.table_fqn, table_completion.c.first_pos).where(
                table_completion.c.connection_id == p.cid, table_completion.c.status != CANCELLED,
                table_completion.c.first_pos.is_not(None))):
            cur = best.get(fqn)
            if cur is None or cmp(Position(engine=eng, text=pos), Position(engine=eng, text=cur)) < 0:
                best[str(fqn)] = str(pos)
    from .replay import wm_stream

    return {wm_stream(p.cid, f): v for f, v in best.items()}


# ------------------------------------------------------------------ fetchers
class EdgeFetcher:
    """VERIFY_TABLE{checks:[rows], key_values, columns} through the connection's agent (whitelisted, R5)."""

    def available(self, engine: Engine, p: Any) -> str | None:
        """None when fetching is possible now; else why not ('no_capability' is terminal for the table)."""
        from .edge_verify import AgentUnavailable, agent_ready

        try:
            agent_ready(engine, p.cid)
        except AgentUnavailable as e:
            return "no_capability" if "does not support" in str(e) else f"agent: {e}"
        return None

    def issue(self, engine: Engine, p: Any, fqn: str, key_cols: list[str], keys: list[dict[str, Any]],
              columns: list[str]) -> str:
        from alelt_verify.defaults import canon

        from .edge_verify import issue

        ref = _ref(fqn)
        args = {"table": {"schema": ref.schema_, "name": ref.name}, "verify_id": str(uuid.uuid4()),
                "checks": ["rows"], "key_values": [[canon(k.get(c)) or "" for c in key_cols] for k in keys],
                "columns": columns, "timeout_s": 300}
        return issue(engine, p.cid, args, f"out-of-row completion {fqn} ({len(keys)} key(s))")

    def poll(self, engine: Engine, command_id: str) -> tuple[str, dict[str, Any] | None]:
        """('pending' | 'ok' | 'failed' | 'expired', result)."""
        with engine.connect() as c:
            r = c.execute(select(agent_command.c.status, agent_command.c.result, agent_command.c.issued_at)
                          .where(agent_command.c.id == uuid.UUID(command_id))).first()
        if r is None:
            return "failed", {"code": "NOT_FOUND", "message": "command vanished"}
        if r[0] == "ACKED":
            return "ok", r[1] or {}
        if r[0] == "FAILED":
            return "failed", r[1] or {}
        if r[0] == "EXPIRED":
            return "expired", r[1]
        issued = r[2] if r[2] is None or r[2].tzinfo else r[2].replace(tzinfo=utcnow().tzinfo)
        if issued is not None and utcnow() - issued > timedelta(seconds=cmd_timeout_s()):
            from .edge_verify import expire

            expire(engine, command_id)
            return "expired", None
        return "pending", None


def _remote(p: Any) -> bool:
    return getattr(p.source, "capture_mode", "edge") == "remote"


# ------------------------------------------------------------------ the step (data lane, slice start)
def step(p: Any) -> int:
    """Collect answered fetches into the main-stream buffer, then issue new fetches. Cheap when idle. Returns the
    number of completion rows put into the buffer."""
    engine = p.engine
    with engine.connect() as c:
        open_n = c.execute(select(func.count()).select_from(table_completion).where(
            table_completion.c.connection_id == p.cid)).scalar_one()
    if not open_n:
        p.completion_tables = set()
        return 0
    with engine.begin() as c:                          # cancelled entries are dropped at the next step
        n = c.execute(delete(table_completion).where(table_completion.c.connection_id == p.cid,
                                                     table_completion.c.status == CANCELLED)).rowcount or 0
    p.completion_tables = tables_with_entries(engine, p)
    if n:
        p.completion_tables = tables_with_entries(engine, p)
    rows = collect(p)
    issue_due(p)
    _gauges(p)
    return rows


def frozen(p: Any) -> set[str]:
    """Tables a completion must not touch now: integrity / schema / snapshot holds, lifecycle not STREAMING."""
    from . import holds

    out = {r["table_fqn"] for r in holds.rows(p.engine, p.cid) if r["held"] and r["kind"] in FROZEN_KINDS}
    try:
        from . import lifecycle

        out |= lifecycle.not_streaming(p.engine, p.cid)
    except Exception:  # noqa: BLE001 - lifecycle unavailable: holds only
        pass
    return out


def _parse_rows(p: Any, fqn: str, res: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None,
                                                               bool]:
    """(rows, missing keys, R, truncated) of an edge VERIFY_TABLE{rows} result, typed by the catalog."""
    from alelt_common.models import VerifyTableResult
    from alelt_common.verify_edge import parse_text

    info = p.tables.get(_ref(fqn)) or _info(p, fqn)
    types = {c.name: c.canonical_type for c in info.columns}
    kc = [c.name for c in info.key_columns()]
    r = VerifyTableResult.model_validate(res)
    rows = [{k: parse_text(v, types.get(k)) for k, v in x.items()} for x in r.rows]
    missing = [{c: parse_text(v, types.get(c)) for c, v in zip(kc, m)} for m in r.missing]
    return rows, missing, r.position_before, bool(r.truncated)


def _info(p: Any, fqn: str) -> Any:
    from . import store

    with p.engine.connect() as c:
        vid = store.latest_catalog_id(c, p.cid)
        info = store.table_infos(store.load_catalog(c, vid)).get(_ref(fqn)) if vid else None
    if info is None:
        raise LookupError(f"{fqn} not in the catalog of {p.cid}")
    return info


def collect(p: Any) -> int:
    """Step 1: answered commands -> completion change sets in the buffer (arrival order, placed at R)."""
    engine = p.engine
    injected: dict[str, Injected] = p.completion_injected
    with engine.connect() as c:
        rows = [dict(r) for r in c.execute(select(table_completion).where(
            table_completion.c.connection_id == p.cid, table_completion.c.status == INFLIGHT,
            table_completion.c.command_id.is_not(None)).order_by(table_completion.c.updated_at)).mappings()]
    by_cmd: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        if str(r["command_id"]) not in injected:
            by_cmd.setdefault(str(r["command_id"]), []).append(r)
    fetcher = EdgeFetcher()
    put = 0
    for cmd, ents in by_cmd.items():
        fqn = str(ents[0]["table_fqn"])
        if cmd.startswith(REMOTE_PREFIX):
            res = _remote_result(engine, ents)
            if res is None:
                continue
            st, payload = res
        else:
            st, payload = fetcher.poll(engine, cmd)
        if st == "pending":
            continue
        if st != "ok":
            _failed(p, ents, payload, st)
            continue
        try:
            if cmd.startswith(REMOTE_PREFIX):
                frows, missing, rpos, trunc = payload["rows"], payload["missing"], payload.get("read_pos"), False
            else:
                frows, missing, rpos, trunc = _parse_rows(p, fqn, payload or {})
        except Exception as e:  # noqa: BLE001 - malformed result: retried like a failure
            _failed(p, ents, {"code": "SOURCE_ERROR", "message": f"bad result: {type(e).__name__}: {e}"}, "failed")
            continue
        if trunc:
            _truncated(p, ents)
            continue
        put += _inject(p, cmd, fqn, ents, frows, rpos)
        REGISTRY.inc("alelt_completion_rows_total", float(len(missing)), connection_id=p.cid, result="missing")
    return put


def _inject(p: Any, cmd: str, fqn: str, ents: list[dict[str, Any]], frows: list[dict[str, Any]],
            rpos: str | None) -> int:
    info = p.tables.get(_ref(fqn)) or _info(p, fqn)
    kc = [c.name for c in info.key_columns()]
    known = {c.name for c in info.columns}
    by_key: dict[str, dict[str, Any]] = {}
    for r in frows:
        by_key[key_id(encode_key({c: r.get(c) for c in kc}))] = r
    eng = p.engine_name
    if rpos is None:                                  # agent did not report its read position: newest known signal
        rpos = max((str(e["last_pos"]) for e in ents if e["last_pos"]),
                   key=lambda x: _sort_key(p, x), default=p.flushed_position())
    at = Position(engine=eng, text=rpos or "")
    events: list[ChangeEvent] = []
    inj = Injected(cmd, rpos)
    for e in ents:
        r = by_key.get(str(e["key_id"]))
        inj.entries[(fqn, str(e["key_id"]))] = str(e["last_pos"] or "")
        if r is None:
            continue                                   # gone at R: the DELETE is in the log (applied by the stream)
        cols = [c for c in (e["columns"] or []) if c in known and c not in kc] or \
            [c for c in r if c in known and c not in kc]
        after = {**{c: r.get(c) for c in kc}, **{c: r.get(c) for c in cols if c in r}}
        events.append(ChangeEvent(position=at, txid=f"{TXID_PREFIX}{cmd}", kind=ChangeKind.UPDATE, table=_ref(fqn),
                                  before=None, after=after, note=f"completion:{','.join(cols)}"[:500]))
    p.completion_injected[cmd] = inj
    if events:
        p.loader.inject(ChangeSet(txid=f"{TXID_PREFIX}{cmd}", commit_position=at, commit_ts=utcnow(), events=events))
    return len(events)


def _sort_key(p: Any, text: str) -> Any:
    cmp = p.source.compare
    eng = p.engine_name

    class K:
        def __init__(self, t: str) -> None:
            self.t = t

        def __lt__(self, o: "K") -> bool:
            return cmp(Position(engine=eng, text=self.t), Position(engine=eng, text=o.t)) < 0
    return K(text)


def _failed(p: Any, ents: list[dict[str, Any]], res: dict[str, Any] | None, st: str) -> None:
    code = str((res or {}).get("code") or ("TIMEOUT" if st == "expired" else "SOURCE_ERROR"))
    msg = str((res or {}).get("message") or (res or {}).get("error") or st)[:300]
    now = utcnow()
    with p.engine.begin() as c:
        for e in ents:
            att = int(e["attempts"] or 0) + 1
            c.execute(update(table_completion).where(
                table_completion.c.connection_id == p.cid, table_completion.c.table_fqn == e["table_fqn"],
                table_completion.c.key_id == e["key_id"], table_completion.c.command_id == e["command_id"]).values(
                status=PENDING, command_id=None, attempts=att, last_error=f"{code}: {msg}",
                next_attempt_at=now + timedelta(seconds=min(300, 2 ** min(att, 9))), updated_at=now))
    REGISTRY.inc("alelt_completion_rows_total", float(len(ents)), connection_id=p.cid, result="requeued")
    log.warning("completion fetch failed", extra={"component": "alelt_hub", "table": ents[0]["table_fqn"],
                                                  "correlation_id": f"{code}: {msg}"})
    if code == "UNSUPPORTED":
        from . import integrity

        integrity.completion_unavailable(p, str(ents[0]["table_fqn"]), f"completion unavailable: {msg}")


def _truncated(p: Any, ents: list[dict[str, Any]]) -> None:
    """Edge size cap hit: re-issue with half the keys; a single key that still truncates is oversize."""
    now = utcnow()
    n = len(ents)
    with p.engine.begin() as c:
        for e in ents:
            vals: dict[str, Any] = dict(status=PENDING, command_id=None, updated_at=now,
                                        last_error=f"truncated:{max(1, n // 2)}")
            if n == 1:
                vals.update(last_error="oversize", next_attempt_at=now + timedelta(hours=1),
                            attempts=int(e["attempts"] or 0) + 1)
            c.execute(update(table_completion).where(
                table_completion.c.connection_id == p.cid, table_completion.c.table_fqn == e["table_fqn"],
                table_completion.c.key_id == e["key_id"]).values(**vals))
    REGISTRY.inc("alelt_completion_rows_total", float(n), connection_id=p.cid, result="truncated")
    if n == 1:
        _incident(p, str(ents[0]["table_fqn"]), "COMPLETION_OVERSIZE",
                  "one row's out-of-row values exceed the edge result cap (8 MiB); retried hourly")


def issue_due(p: Any) -> int:
    """Step 4: due PENDING entries -> fetch commands (per table, oldest first, cut by keys and bytes)."""
    engine = p.engine
    now = utcnow()
    with engine.connect() as c:
        inflight = c.execute(select(func.count(func.distinct(table_completion.c.command_id))).where(
            table_completion.c.connection_id == p.cid, table_completion.c.status == INFLIGHT)).scalar_one()
    slots = inflight_cmds() - int(inflight)
    if slots <= 0:
        return 0
    stop = frozen(p)
    per = keys_per_cmd()
    with engine.connect() as c:
        due = [dict(r) for r in c.execute(select(table_completion).where(
            table_completion.c.connection_id == p.cid, table_completion.c.status == PENDING,
            table_completion.c.next_attempt_at <= now).order_by(table_completion.c.created_at)
            .limit(slots * per * 2)).mappings()]
    due = [d for d in due if d["table_fqn"] not in stop]
    if not due:
        return 0
    remote = _remote(p)
    fetcher = EdgeFetcher()
    if not remote:
        why = fetcher.available(engine, p)
        if why == "no_capability":
            for fqn in sorted({d["table_fqn"] for d in due}):
                from . import integrity

                integrity.completion_unavailable(p, fqn, "completion unavailable: agent lacks verify_table")
            return 0
        if why is not None:                          # agent offline: look again in 10 s (stall incident later)
            log.info("completion fetch deferred: %s", why, extra={"component": "alelt_hub", "correlation_id": p.cid})
            with engine.begin() as c:
                c.execute(update(table_completion).where(
                    table_completion.c.connection_id == p.cid, table_completion.c.status == PENDING,
                    table_completion.c.next_attempt_at <= now).values(
                    next_attempt_at=now + timedelta(seconds=10), last_error=f"deferred: {why}"[:300]))
            _stall_check(p)
            return 0
    tables: dict[str, list[dict[str, Any]]] = {}
    for d in due:
        tables.setdefault(str(d["table_fqn"]), []).append(d)
    issued = 0
    for fqn, ents in tables.items():
        if issued >= slots:
            break
        try:
            info = p.tables.get(_ref(fqn)) or _info(p, fqn)
        except LookupError:
            continue
        kc = [c.name for c in info.key_columns()]
        if not kc:
            from . import integrity

            integrity.completion_unavailable(p, fqn, "completion unavailable: table has no key")
            continue
        half = min((int(str(e["last_error"]).split(":")[1]) for e in ents
                    if str(e.get("last_error") or "").startswith("truncated:")), default=per)
        cap = max(1, min(per, half))
        for chunk in _chunks(ents, kc, cap):
            if issued >= slots:
                break
            keys = [{k: decode_value(v) for k, v in (e["key"] or {}).items()} for e in chunk]
            cols = sorted({c for e in chunk for c in (e["columns"] or [])} - set(kc))
            known = {c.name for c in info.columns}
            cols = [c for c in cols if c in known] or [c.name for c in sorted(info.columns, key=lambda x: x.ordinal)
                                                       if c.name not in kc]
            try:
                if remote:
                    cmd = f"{REMOTE_PREFIX}{uuid.uuid4()}"
                else:
                    cmd = fetcher.issue(engine, p, fqn, kc, keys, kc + cols)
            except Exception as e:  # noqa: BLE001 - agent unavailable / refused at issue: retried later
                _failed(p, [{**x, "command_id": None} for x in chunk], {"code": "SOURCE_ERROR",
                                                                         "message": f"{type(e).__name__}: {e}"},
                        "failed")
                continue
            with engine.begin() as c:
                for e in chunk:
                    c.execute(update(table_completion).where(
                        table_completion.c.connection_id == p.cid, table_completion.c.table_fqn == fqn,
                        table_completion.c.key_id == e["key_id"], table_completion.c.status == PENDING).values(
                        status=INFLIGHT, command_id=cmd, updated_at=utcnow()))
                if remote:
                    jobs.enqueue(c, "completion", {"connection_id": p.cid, "fetch": cmd, "table": fqn},
                                 dedup_key=f"completion-fetch:{cmd}")
            issued += 1
            REGISTRY.inc("alelt_completion_commands_total", connection_id=p.cid)
    if issued:
        with engine.begin() as c:
            jobs.enqueue(c, "completion", {"connection_id": p.cid}, dedup_key=jobs.marker_key("completion", p.cid),
                         delay_s=poll_s())
    _stall_check(p)
    return issued


def _chunks(ents: list[dict[str, Any]], kc: list[str], cap: int) -> list[list[dict[str, Any]]]:
    """Cut by key count and by serialized argument size (ALELT_COMPLETION_CMD_BYTES)."""
    limit = cmd_bytes()
    out: list[list[dict[str, Any]]] = []
    cur: list[dict[str, Any]] = []
    size = 0
    for e in ents:
        n = len(json.dumps([str((e["key"] or {}).get(c)) for c in kc])) + 4
        if cur and (len(cur) >= cap or size + n > limit):
            out.append(cur)
            cur, size = [], 0
        cur.append(e)
        size += n
    if cur:
        out.append(cur)
    return out


# ------------------------------------------------------------------ after a flush
def after_flush(p: Any, done: dict[str, Injected], held: set[str]) -> None:
    """Metadata step of a successful flush that carried `done`: entries whose fetch read at R >= last_pos are deleted;
    entries with a newer signal (or whose table was held at flush time) go back to PENDING."""
    if not done:
        return
    cmp, eng = p.source.compare, p.engine_name
    completed = requeued = 0
    with p.engine.begin() as c:
        for inj in done.values():
            for (fqn, kid), _lp in inj.entries.items():
                r = c.execute(select(table_completion.c.last_pos, table_completion.c.status,
                                     table_completion.c.command_id).where(
                    table_completion.c.connection_id == p.cid, table_completion.c.table_fqn == fqn,
                    table_completion.c.key_id == kid)).first()
                if r is None or r[2] != inj.command_id:
                    continue
                if r[1] == CANCELLED:
                    continue
                fresh = inj.read_pos is not None and r[0] is not None and \
                    cmp(Position(engine=eng, text=inj.read_pos), Position(engine=eng, text=r[0])) >= 0
                if inj.read_pos is not None and r[0] is None:
                    fresh = True
                if fqn not in held and fresh:
                    c.execute(delete(table_completion).where(
                        table_completion.c.connection_id == p.cid, table_completion.c.table_fqn == fqn,
                        table_completion.c.key_id == kid, table_completion.c.command_id == inj.command_id))
                    completed += 1
                else:
                    c.execute(update(table_completion).where(
                        table_completion.c.connection_id == p.cid, table_completion.c.table_fqn == fqn,
                        table_completion.c.key_id == kid).values(status=PENDING, command_id=None,
                                                                 read_pos=inj.read_pos, next_attempt_at=utcnow(),
                                                                 updated_at=utcnow()))
                    requeued += 1
    for inj in done.values():
        _forget_remote(p.engine, inj.command_id)
    if completed:
        REGISTRY.inc("alelt_completion_rows_total", float(completed), connection_id=p.cid, result="completed")
    if requeued:
        REGISTRY.inc("alelt_completion_rows_total", float(requeued), connection_id=p.cid, result="requeued")
        with p.engine.begin() as c:
            jobs.mark_dirty(c, p.cid, delay_s=poll_s())
    _resolve_drained(p)


def flush_idle(p: Any) -> bool:
    """The main stream has nothing to flush but completion rows wait in its buffer (idle source): apply them through a
    transient stream `<cid>#completion:<id>` (exactly-once per flush, idempotent re-fetch on crash), then drop it."""
    from alelt_loader import Loader

    sets = p.loader.take_injected()
    if not sets:
        return False
    stream = f"{p.cid}#completion:{uuid.uuid4().hex[:12]}"
    held = set(p.held)
    ld = Loader(p.dest, p.policy, stream, p.source.compare, max_rows=10**12, max_latency_s=10**9,
                hold=p.loader.hold, frozen=p.loader.frozen)
    last = sets[-1].commit_position
    for cs in sets:
        ld.add([cs], cs.commit_position)
    ld.buf.to_pos = last
    done = {k: v for k, v in p.completion_injected.items() if any(cs.txid == f"{TXID_PREFIX}{k}" for cs in sets)}
    fl = floors(p.engine, p)
    mainpos = p.flushed_position()
    wm: dict[Any, Any] = {k: v for k, v in fl.items()}
    for cs in sets:                                     # the tables' main watermark stays the main stream's position
        for e in cs.events:
            if e.table is not None:
                wm.setdefault(e.table, mainpos)
    _register_transient(p, stream)
    ld.flush(p.tables, batch_id=f"{stream}", watermarks=wm or None)
    for k in done:
        p.completion_injected.pop(k, None)
    after_flush(p, done, held)
    from .replay import _drop_checkpoint

    _drop_checkpoint(p, stream)
    _unregister_transient(p, stream)
    return True


# ------------------------------------------------------------------ transient stream list (retention retries drops)
def _register_transient(p: Any, stream: str) -> None:
    from .db import hub_setting

    key = f"transient:{p.cid}"
    with p.engine.begin() as c:
        cur = c.execute(select(hub_setting.c.value).where(hub_setting.c.key == key)).scalar() or []
        vals = {"value": sorted(set(cur) | {stream})[-500:], "updated_at": utcnow()}
        c.execute(insert(hub_setting).values(key=key, **vals).on_conflict_do_update(index_elements=["key"], set_=vals))


def _unregister_transient(p: Any, stream: str) -> None:
    from .db import hub_setting

    key = f"transient:{p.cid}"
    with p.engine.begin() as c:
        cur = c.execute(select(hub_setting.c.value).where(hub_setting.c.key == key)).scalar() or []
        c.execute(update(hub_setting).where(hub_setting.c.key == key).values(
            value=[s for s in cur if s != stream], updated_at=utcnow()))


def drop_transient(p: Any) -> int:
    """Retention: retry the checkpoint drop of transient streams left by a crash (one list per connection)."""
    from .db import hub_setting
    from .replay import _drop_checkpoint

    key = f"transient:{p.cid}"
    with p.engine.connect() as c:
        cur = c.execute(select(hub_setting.c.value).where(hub_setting.c.key == key)).scalar() or []
    for s in cur:
        _drop_checkpoint(p, s)
        _unregister_transient(p, s)
    return len(cur)


# ------------------------------------------------------------------ control lane: polling + remote fetches
def control(engine: Engine, worker: Any, payload: dict[str, Any]) -> str:
    """`completion` job: run a remote keyed fetch, or poll answered edge commands and wake the stream (the data lane
    collects them under the pipeline lock). Re-enqueues itself while fetches are in flight."""
    cid = str(payload["connection_id"])
    if payload.get("fetch"):
        return _remote_fetch(engine, worker, cid, str(payload["fetch"]), str(payload.get("table") or ""))
    with engine.begin() as c:       # cancelled entries: dropped (a late result finds no entry and is ignored)
        c.execute(delete(table_completion).where(table_completion.c.connection_id == cid,
                                                 table_completion.c.status == CANCELLED))
    with engine.connect() as c:
        rows = c.execute(select(table_completion.c.command_id, table_completion.c.status,
                                table_completion.c.next_attempt_at).where(
            table_completion.c.connection_id == cid, table_completion.c.status.in_((PENDING, INFLIGHT)))).all()
    if not rows:
        return "idle"
    now = utcnow()
    cmds = {str(r[0]) for r in rows if r[1] == INFLIGHT and r[0]}
    answered = False
    f = EdgeFetcher()
    for cmd in cmds:
        if cmd.startswith(REMOTE_PREFIX):
            continue
        st, _ = f.poll(engine, cmd)
        if st != "pending":
            answered = True
            break
    due = any(r[1] == PENDING and (r[2] is None or _aware(r[2]) <= now) for r in rows)
    with engine.begin() as c:
        if answered or due:
            jobs.mark_dirty(c, cid)
        nxt = poll_s() if cmds else max(poll_s(), min(
            [(_aware(r[2]) - now).total_seconds() for r in rows if r[1] == PENDING and r[2] is not None] or [poll_s()]))
        jobs.enqueue(c, "completion", {"connection_id": cid}, dedup_key=jobs.marker_key("completion", cid),
                     delay_s=nxt)
    return "answered" if answered else ("due" if due else "waiting")


def _aware(t: Any) -> Any:
    return t if t is None or t.tzinfo else t.replace(tzinfo=utcnow().tzinfo)


def _remote_result(engine: Engine, ents: list[dict[str, Any]]) -> tuple[str, Any] | None:
    from .db import hub_setting

    key = f"completion-result:{ents[0]['command_id']}"
    with engine.connect() as c:
        v = c.execute(select(hub_setting.c.value).where(hub_setting.c.key == key)).scalar()
    if v is None:
        return None
    if v.get("error"):
        _forget_remote(engine, str(ents[0]["command_id"]))
        return "failed", {"code": v.get("code") or "SOURCE_ERROR", "message": v["error"]}
    return "ok", {"rows": [{k: decode_value(x) for k, x in r.items()} for r in v.get("rows") or []],
                  "missing": [{k: decode_value(x) for k, x in r.items()} for r in v.get("missing") or []],
                  "read_pos": v.get("read_pos")}


def _forget_remote(engine: Engine, cmd: str) -> None:
    """A remote result is kept until the flush that carried it (a lost buffer re-collects it)."""
    from .db import hub_setting

    if cmd.startswith(REMOTE_PREFIX):
        with engine.begin() as c:
            c.execute(delete(hub_setting).where(hub_setting.c.key == f"completion-result:{cmd}"))


def _remote_fetch(engine: Engine, worker: Any, cid: str, cmd: str, fqn: str) -> str:
    """Remote source: SourceConnector.keyed_reread on a live connector (control lane, synchronous); the result is
    stored like an agent ack (hub_setting `completion-result:<cmd>`) and collected by the data lane."""
    from alelt_capture.config import connections

    from .db import hub_setting

    with engine.connect() as c:
        ents = [dict(r) for r in c.execute(select(table_completion).where(
            table_completion.c.connection_id == cid, table_completion.c.command_id == cmd)).mappings()]
    if not ents:
        return "nothing"
    p = worker.pipeline(cid)
    out: dict[str, Any]
    try:
        info = p.tables.get(_ref(fqn)) or _info(p, fqn)
        keys = [{k: decode_value(v) for k, v in (e["key"] or {}).items()} for e in ents]
        rc = connections(engine).get(cid)
        if rc is None:
            raise LookupError(f"{cid}: no live source settings")
        src = rc.factory()()
        try:
            res = src.keyed_reread(info, keys)
        finally:
            src.close()
        out = {"rows": [encode_key(r) for r in res.rows], "missing": [encode_key(r) for r in res.missing],
               "read_pos": res.read_from.text if res.read_from else None}
    except NotImplementedError:
        out = {"error": "source has no keyed_reread", "code": "UNSUPPORTED"}
    except Exception as e:  # noqa: BLE001 - retried with backoff by the collect step
        out = {"error": f"{type(e).__name__}: {e}"[:500], "code": "SOURCE_ERROR"}
    key = f"completion-result:{cmd}"
    vals = {"value": out, "updated_at": utcnow()}
    with engine.begin() as c:
        c.execute(insert(hub_setting).values(key=key, **vals).on_conflict_do_update(index_elements=["key"], set_=vals))
        jobs.mark_dirty(c, cid)
    return "fetched"


# ------------------------------------------------------------------ stall / incidents / gauges
def _incident(p: Any, fqn: str, kind: str, error: str) -> None:
    try:
        from alelt_diagnose.incidents import open_incident
        from alelt_diagnose.models import IncidentKind, Trigger

        open_incident(t=Trigger(kind=IncidentKind(kind), connection_id=p.cid, table_fqn=fqn, stage="completion",
                                error=error[:500], detail={"table": fqn}), engine=p.engine, diagnose=False)
    except Exception:  # noqa: BLE001 - diagnosis never breaks the data path
        log.exception("completion incident failed", extra={"component": "alelt_hub"})


def _stall_check(p: Any) -> None:
    """COMPLETION_STALLED per table when its oldest open entry waits longer than ALELT_COMPLETION_STALL_S."""
    lim = utcnow() - timedelta(seconds=stall_s())
    with p.engine.connect() as c:
        old = [str(f) for f, in c.execute(select(table_completion.c.table_fqn).where(
            table_completion.c.connection_id == p.cid, table_completion.c.status != CANCELLED,
            table_completion.c.created_at < lim).group_by(table_completion.c.table_fqn))]
    for fqn in old:
        _incident(p, fqn, "COMPLETION_STALLED",
                  f"out-of-row completion of {fqn} unanswered for more than {stall_s():.0f}s")


def _resolve_drained(p: Any) -> None:
    """Auto-resolve COMPLETION_STALLED incidents of tables that drained."""
    try:
        from alelt_diagnose.tables import incident

        left = {f for (_c, f) in pending_counts(p.engine, p.cid)}
        with p.engine.begin() as c:
            rows = c.execute(select(incident.c.id, incident.c.table_fqn).where(
                incident.c.connection_id == p.cid, incident.c.kind == "COMPLETION_STALLED",
                incident.c.status.in_(("OPEN", "DIAGNOSED", "ACKNOWLEDGED")))).all()
            for iid, fqn in rows:
                if fqn not in left:
                    c.execute(update(incident).where(incident.c.id == iid).values(
                        status="RESOLVED", resolved_by="system:completion", resolved_at=utcnow(),
                        confirmed_cause="pending completions drained"))
    except Exception:  # noqa: BLE001
        log.exception("completion incident resolve failed", extra={"component": "alelt_hub"})


def _gauges(p: Any) -> None:
    counts = pending_counts(p.engine, p.cid)
    for (_c, fqn), n in counts.items():
        REGISTRY.set("alelt_completion_pending", n, connection_id=p.cid, table=fqn)
    for fqn in getattr(p, "_gauged", set()) - {f for (_c, f) in counts}:
        REGISTRY.set("alelt_completion_pending", 0, connection_id=p.cid, table=fqn)
    p._gauged = {f for (_c, f) in counts}
    with p.engine.connect() as c:
        oldest = c.execute(select(func.min(table_completion.c.created_at)).where(
            table_completion.c.connection_id == p.cid)).scalar()
    REGISTRY.set("alelt_completion_oldest_seconds",
                 (utcnow() - _aware(oldest)).total_seconds() if oldest is not None else 0, connection_id=p.cid)


def drop_table(engine: Engine, cid: str, fqn: str) -> int:
    """A resync snapshot replaces every value of the table: its entries are dropped."""
    with engine.begin() as c:
        return c.execute(delete(table_completion).where(table_completion.c.connection_id == cid,
                                                        table_completion.c.table_fqn == fqn)).rowcount or 0


def blocking(engine: Engine, cid: str, fqn: str) -> str | None:
    """Certification / switch / settled verification gate (soft gate, never a hold)."""
    n = pending_counts(engine, cid).get((cid, fqn), 0)
    return f"{n} out-of-row completion(s) pending" if n else None


# ------------------------------------------------------------------ DR: rebuild entries from the spool
def rebuild(p: Any) -> int:
    """`alelt completion rebuild <cid>` (metadata lost): re-scan the spool from each table's watermark for out-of-row
    signals and recreate the entries. Without a known watermark the whole retained spool is scanned (entries are
    idempotent: a fetch writes the current value)."""
    from .db import batch

    with p.engine.connect() as c:
        rows = [dict(r) for r in c.execute(select(batch).where(batch.c.connection_id == p.cid)
                                           .order_by(batch.c.seq)).mappings()]
    tracker = SignalTracker()
    from alelt_txn import TxnAssembler

    asm = TxnAssembler(p.s.txn_byte_budget, p.s.spool_dir / "txn-spill" / f"{p.cid}-rebuild-{uuid.uuid4().hex[:8]}")
    sigs: list[Signal] = []
    try:
        for b in rows:
            vid = p.catalog_vid_for(b)
            res = p.cache.decode(b, p._catalog(vid), vid)
            tracker.feed(res.events)
            sets = asm.feed(res.events, int(b["seq"]))
            sigs += tracker.committed(sets, int(b["seq"]))
    finally:
        asm.reset()
    return enqueue_signals(p.engine, p, sigs)


__all__ = ["CANCELLED", "INFLIGHT", "PENDING", "EdgeFetcher", "Injected", "Signal", "SignalTracker", "after_flush",
           "blocking", "cancel_deleted", "collect", "control", "drop_table", "enqueue_signals", "floors",
           "flush_idle", "issue_due", "key_id", "pending_counts", "rebuild", "step"]
