import type { TransportRequest, TransportResponse, Transport } from './client';
import type {
  AgentCommandRequest, AuditEvent, DestructiveMutation, EndpointKey, GroupMappingsUpdate, Me, Mutation, Overview,
  ReconRunRequest, ReprocessRequest, RetentionUpdate, Role, SchemaDecision, SwitchRequest, SwitchRequestCreate,
  TableConfigUpdate, TableDetail, TableSummary, TopologyPlanRequest, WaveCreate,
} from './contract';
import { COMMAND_NAMES, DEFAULT_SYSTEM_COLUMNS, ROLES, expectedConfirmText } from './contract';
import { createMockDb, type MockDb } from './fixtures';
import { ENDPOINT_PERMISSION, approvalBlock } from '../lib/endpointPermissions';
import { can } from '../lib/rbac';
import { toCsv } from '../lib/format';
import { importHandlers } from './mockImport';
import { diagnoseHandlers } from './mockDiagnose';
import { planTopology } from '../lib/topology';

/**
 * In-browser mock of the hub `/api/v1`. Enforces the same RBAC, reason/confirm-text and two-person rules the hub must,
 * so UI behaviour (and Playwright specs) are meaningful without a hub. Dev identity: X-ALELT-Dev-Role (+ optional X-ALELT-Dev-User).
 */
class Fail extends Error {
  constructor(readonly status: number, readonly code: string, message: string) {
    super(message);
  }
}

interface Ctx {
  user: string;
  roles: Role[];
  id: string;
  query: URLSearchParams;
  body: unknown;
  correlationId: string | null;
}
type Handler = (db: MockDb, c: Ctx) => unknown;

const ROUTES: { key: EndpointKey; method: string; re: RegExp }[] = (Object.keys(ENDPOINT_PERMISSION) as EndpointKey[]).map((key) => {
  const [method, path] = key.split(' ') as [string, string];
  const re = new RegExp('^' + path.replace(/[.*+?^$()|[\]\\]/g, '\\$&').replace('\\{id\\}', '([^/]+)').replace('{id}', '([^/]+)') + '$');
  return { key, method, re };
});

function nowIso(): string {
  return new Date().toISOString();
}

function requireReason(body: unknown): string {
  const reason = (body as Partial<Mutation> | undefined)?.reason?.trim();
  if (!reason) throw new Fail(400, 'REASON_REQUIRED', 'reason is required for mutating calls');
  return reason;
}
function requireConfirm(body: unknown, expected: string): void {
  if ((body as Partial<DestructiveMutation> | undefined)?.confirm_text !== expected) {
    throw new Fail(400, 'CONFIRM_MISMATCH', `confirm_text must be "${expected}"`);
  }
}
function audit(db: MockDb, c: Ctx, action: string, target: string, detail: Record<string, unknown> = {}): void {
  const ev: AuditEvent = {
    id: `a-${++db.seq}`, at: nowIso(), actor: c.user, role: c.roles[0] ?? 'Viewer', action, target, detail,
    correlation_id: c.correlationId,
  };
  db.audit.unshift(ev);
}
function tableById(db: MockDb, id: string): TableDetail {
  const t = db.tables.find((x) => x.id === id);
  if (!t) throw new Fail(404, 'NOT_FOUND', `table ${id} not found`);
  return t;
}
const fqn = (t: { table: { schema: string; name: string } }) => `${t.table.schema}.${t.table.name}`;
function summary(t: TableDetail): TableSummary {
  const { revision: _r, columns: _c, schema_history: _s, recon: _x, checkpoint: _k, ...rest } = t;
  return rest;
}
const page = <T,>(items: T[]) => ({ items, total: items.length, next_cursor: null });

function executeSwitch(db: MockDb, req: SwitchRequest): void {
  const ids = 'table_id' in req.scope ? [req.scope.table_id] : (db.waves.find((w) => 'wave_id' in req.scope && w.id === req.scope.wave_id)?.table_ids ?? []);
  for (const id of ids) {
    const t = tableById(db, id);
    t.active_source = req.target;
    const cert = db.certification.find((c) => c.table_id === id);
    if (cert) cert.active_source = req.target;
  }
  if ('wave_id' in req.scope) {
    const w = db.waves.find((x) => 'wave_id' in req.scope && x.id === req.scope.wave_id);
    if (w) w.status = req.target === 'ALELT' ? 'SWITCHED' : 'ROLLED_BACK';
  }
}

const HANDLERS: Record<EndpointKey, Handler> = {
  ...(importHandlers<MockDb>({ Fail, requireReason, audit, state: (db) => db.importer }) as Record<EndpointKey, Handler>),
  ...(diagnoseHandlers<MockDb>({ Fail, requireReason, audit }) as Record<EndpointKey, Handler>),
  'GET /me': (_db, c): Me => ({ oid: `dev-${c.user}`, name: c.user, email: c.user, roles: c.roles, dev: true }),
  'GET /overview': (db): Overview => {
    const enabled = db.tables.filter((t) => t.enabled);
    const agent = db.agents[0];
    return {
      pipeline_health: agent?.state === 'CAPTURING' ? 'OK' : 'DEGRADED',
      capture_lag_seconds: 42, capture_lag_lri_distance: 31,
      last_batch: db.batches[0] ? { batch_id: db.batches[0].batch_id, received_at: db.batches[0].received_at, to_pos: db.batches[0].to_pos } : null,
      latency_p95_seconds: 74, latency_target_seconds: 120,
      open_alerts: agent?.state === 'PAUSED'
        ? [{ id: 'al-p', name: 'AleltAgentPaused', severity: 'warning', summary: 'Capture paused by kill switch', since: nowIso() }]
        : [{ id: 'al-1', name: 'AleltCertExpirySoon', severity: 'warning', summary: 'Agent client certificate expires in 21 days', since: nowIso() }],
      parity_score: 0.92, tables_total: enabled.length, tables_in_sync: enabled.filter((t) => t.state === 'IN_SYNC').length,
      output_profile: 'alelt', system_columns: { ...DEFAULT_SYSTEM_COLUMNS }, ladder: db.ladder,
    };
  },
  'GET /ladder': (db) => db.ladder,
  'GET /tables': (db) => page(db.tables.map(summary)),
  'GET /tables/{id}': (db, c) => tableById(db, c.id),
  'PUT /tables/{id}/config': (db, c) => {
    const reason = requireReason(c.body);
    const t = tableById(db, c.id);
    const b = c.body as TableConfigUpdate;
    if (b.enabled !== undefined) {
      if (b.enabled && !t.capture_enabled) throw new Fail(409, 'CAPTURE_DISABLED', 'DATA CAPTURE CHANGES is not enabled on the source table');
      t.enabled = b.enabled;
      t.state = b.enabled ? (t.state === 'DISABLED' ? 'NEW' : t.state) : 'DISABLED';
    }
    if (b.sync_mode) t.sync_mode = b.sync_mode;
    for (const u of b.columns ?? []) {
      const cc = t.columns.find((x) => x.name === u.name);
      if (!cc) throw new Fail(400, 'UNKNOWN_COLUMN', u.name);
      if (cc.key_seq && !u.enabled) throw new Fail(409, 'KEY_COLUMN', `key column ${u.name} cannot be disabled`);
      cc.enabled = u.enabled;
      cc.hashed = u.hashed;
    }
    db.revisions.forEach((r) => (r.current = false));
    const id = Math.max(...db.revisions.map((r) => r.id)) + 1;
    db.revisions.push({ id, author: c.user, created_at: nowIso(), reason, source: 'console', current: true });
    t.revision = id;
    audit(db, c, 'table.config.update', fqn(t), { revision: id, reason });
    return t;
  },
  'POST /tables/{id}/resync': (db, c) => {
    const reason = requireReason(c.body);
    const t = tableById(db, c.id);
    requireConfirm(c.body, expectedConfirmText('resync', fqn(t)));
    t.state = 'SNAPSHOTTING';
    audit(db, c, 'table.resync', fqn(t), { reason });
    return { job_id: `job-${++db.seq}` };
  },
  'POST /tables/{id}/reprocess': (db, c) => {
    const reason = requireReason(c.body);
    const t = tableById(db, c.id);
    requireConfirm(c.body, expectedConfirmText('reprocess', fqn(t)));
    const b = c.body as ReprocessRequest;
    if (!b.from_pos || !b.to_pos) throw new Fail(400, 'RANGE_REQUIRED', 'from_pos and to_pos are required');
    audit(db, c, 'table.reprocess', fqn(t), { reason, from_pos: b.from_pos, to_pos: b.to_pos });
    return { job_id: `job-${++db.seq}` };
  },
  'GET /batches': (db) => page(db.batches),
  'GET /quarantine': (db) => page(db.quarantine),
  'GET /agents': (db) => db.agents,
  'POST /agents/{id}/commands': (db, c) => {
    const reason = requireReason(c.body);
    const a = db.agents.find((x) => x.id === c.id);
    if (!a) throw new Fail(404, 'NOT_FOUND', c.id);
    const b = c.body as AgentCommandRequest;
    if (!(COMMAND_NAMES as readonly string[]).includes(b.name)) throw new Fail(400, 'COMMAND_NOT_WHITELISTED', String(b.name));
    if (b.name === 'PAUSE') requireConfirm(c.body, expectedConfirmText('pause', a.id));
    if (b.name === 'SHUTDOWN') requireConfirm(c.body, expectedConfirmText('shutdown', a.id));
    if (b.name === 'START_CAPTURE') {
      // REQ-10: {resume: true} (sent to the agent as {}) or a non-empty from_pos string
      const args = b.args ?? {};
      const resume = args.resume === true && !('from_pos' in args);
      if (!resume && (typeof args.from_pos !== 'string' || !args.from_pos.trim())) {
        throw new Fail(400, 'INVALID_ARGS', 'START_CAPTURE needs args.from_pos (string) or args.resume=true');
      }
      a.state = 'CAPTURING';
    }
    if (b.name === 'PAUSE') a.state = 'PAUSED';
    if (b.name === 'RESUME') a.state = 'CAPTURING';
    if (b.name === 'SET_LIMITS') a.limits = { ...a.limits, ...(b.args as Partial<typeof a.limits>) };
    audit(db, c, `agent.command.${b.name}`, a.id, { reason, args: b.args });
    return { command_id: `cmd-${++db.seq}`, status: 'SENT' };
  },
  'GET /revisions': (db) => [...db.revisions].reverse(),
  'GET /revisions/{id}/diff': (db, c) => {
    const id = Number(c.id);
    if (!db.revisions.some((r) => r.id === id)) throw new Fail(404, 'NOT_FOUND', c.id);
    return id === 2
      ? [{ path: 'tables.APPSCHEMA.DRIVER.columns.LICENSE_NO.hashed', op: 'change', before: false, after: true }]
      : id === 1
        ? [{ path: 'tables.APPSCHEMA.DRIVER.enabled', op: 'change', before: false, after: true }]
        : [];
  },
  'POST /revisions/{id}/rollback': (db, c) => {
    const reason = requireReason(c.body);
    requireConfirm(c.body, expectedConfirmText('revision_rollback', c.id));
    db.revisions.forEach((r) => (r.current = false));
    const rev = { id: Math.max(...db.revisions.map((r) => r.id)) + 1, author: c.user, created_at: nowIso(), reason: `${reason} (rollback to r${c.id})`, source: 'rollback' as const, current: true };
    db.revisions.push(rev);
    audit(db, c, 'revision.rollback', `r${c.id}`, { reason, new_revision: rev.id });
    return rev;
  },
  'GET /revisions/{id}/export': (db, c) => {
    const tablesYaml = db.tables.map((t) => `  - source: ${fqn(t)}\n    enabled: ${t.enabled}\n    sync_mode: ${t.sync_mode}`).join('\n');
    return `# ALELT config revision ${c.id}\nconnection: erp-prod\noutput_profile: alelt\ntables:\n${tablesYaml}\n`;
  },
  'POST /import/runs': (db, c) => {
    const reason = requireReason(c.body);
    const run = {
      id: `imp-${++db.seq}`, started_at: nowIso(), finished_at: nowIso(), status: 'DONE' as const, source_ref: 'legacy-landing',
      diff: [
        { path: 'tables.APPSCHEMA.ORDER_STATUS.columns.COMMENTS.enabled', op: 'change' as const, before: true, after: false },
        { path: 'tables.APPSCHEMA.ORDERS.columns.CHARGES.hashed', op: 'change' as const, before: false, after: false },
      ].filter((d) => d.before !== d.after),
      message: null,
    };
    db.imports.unshift(run);
    audit(db, c, 'import.run', run.id, { reason });
    return run;
  },
  'GET /import/runs': (db) => db.imports,
  'POST /import/runs/{id}/accept': (db, c) => {
    const reason = requireReason(c.body);
    const run = db.imports.find((r) => r.id === c.id);
    if (!run) throw new Fail(404, 'NOT_FOUND', c.id);
    if (run.status !== 'DONE') throw new Fail(409, 'NOT_ACCEPTABLE', `import run is ${run.status}`);
    run.status = 'ACCEPTED';
    db.revisions.forEach((r) => (r.current = false));
    const rev = { id: Math.max(...db.revisions.map((r) => r.id)) + 1, author: c.user, created_at: nowIso(), reason, source: 'alelt_import' as const, current: true };
    db.revisions.push(rev);
    audit(db, c, 'import.accept', run.id, { reason, revision: rev.id });
    return rev;
  },
  'GET /schema-changes': (db) => db.schemaChanges,
  'POST /schema-changes/{id}/decision': (db, c) => {
    const reason = requireReason(c.body);
    const sc = db.schemaChanges.find((s) => s.id === c.id);
    if (!sc) throw new Fail(404, 'NOT_FOUND', c.id);
    if (sc.status !== 'PENDING') throw new Fail(409, 'NOT_PENDING', sc.status);
    sc.status = (c.body as SchemaDecision).decision === 'APPROVE' ? 'APPROVED' : 'REJECTED';
    audit(db, c, `schema.${sc.status.toLowerCase()}`, `${sc.table.schema}.${sc.table.name}`, { reason, change: sc.detail });
    return sc;
  },
  'GET /recon': (db) => db.recon,
  'POST /recon/runs': (db, c) => {
    const reason = requireReason(c.body);
    const b = c.body as ReconRunRequest;
    const id = `rr-${++db.seq}`;
    const run = {
      id, check_kind: b.check_kind, started_at: nowIso(), finished_at: nowIso(),
      results: b.table_ids.map((tid) => {
        const t = tableById(db, tid);
        return { id: `r-${++db.seq}`, run_id: id, table_id: tid, table: t.table, check_kind: b.check_kind, outcome: 'MATCH' as const, detail: 'mock: match', at: nowIso(), drill: null };
      }),
    };
    db.recon.unshift(run);
    audit(db, c, 'recon.run', b.check_kind, { reason, tables: b.table_ids });
    return run;
  },
  'GET /certification': (db) => db.certification,
  'POST /certification/{id}/signoff': (db, c) => {
    const reason = requireReason(c.body);
    const cert = db.certification.find((x) => x.table_id === c.id);
    if (!cert) throw new Fail(404, 'NOT_FOUND', c.id);
    if (cert.criteria.some((k) => k.status !== 'PASS')) throw new Fail(409, 'CRITERIA_NOT_MET', 'all §10 criteria must pass');
    cert.signoffs.push({ id: `s-${++db.seq}`, user: c.user, signed_at: nowIso(), comment: reason });
    cert.status = 'CERTIFIED';
    tableById(db, c.id).certification = 'CERTIFIED';
    audit(db, c, 'certification.signoff', `${cert.table.schema}.${cert.table.name}`, { reason });
    return cert;
  },
  'GET /certification/waves': (db) => db.waves,
  'POST /certification/waves': (db, c) => {
    const reason = requireReason(c.body);
    const b = c.body as WaveCreate;
    if (!b.name.trim() || b.table_ids.length === 0) throw new Fail(400, 'INVALID_WAVE', 'name and at least one table required');
    const w = { id: `w-${++db.seq}`, name: b.name.trim(), table_ids: b.table_ids, planned_at: b.planned_at, status: 'PLANNED' as const };
    db.waves.push(w);
    audit(db, c, 'wave.plan', w.name, { reason, tables: b.table_ids });
    return w;
  },
  'POST /switch': (db, c) => {
    const reason = requireReason(c.body);
    const b = c.body as SwitchRequestCreate;
    let label: string;
    if ('table_id' in b.scope) {
      const t = tableById(db, b.scope.table_id);
      label = fqn(t);
      const cert = db.certification.find((x) => x.table_id === t.id);
      if (b.target === 'ALELT' && cert?.status !== 'CERTIFIED') throw new Fail(409, 'NOT_CERTIFIED', `${label} is not certified`);
    } else {
      const wid = b.scope.wave_id;
      const w = db.waves.find((x) => x.id === wid);
      if (!w) throw new Fail(404, 'NOT_FOUND', wid);
      label = w.name;
      if (b.target === 'ALELT') {
        const uncertified = w.table_ids.filter((id) => db.certification.find((x) => x.table_id === id)?.status !== 'CERTIFIED');
        if (uncertified.length) throw new Fail(409, 'NOT_CERTIFIED', `uncertified tables in wave: ${uncertified.join(', ')}`);
      }
      w.status = 'PENDING_APPROVAL';
    }
    requireConfirm(c.body, expectedConfirmText(b.target === 'LEGACY' ? 'rollback' : 'switch', label));
    const req: SwitchRequest = {
      id: `sw-${++db.seq}`, scope: b.scope, scope_label: label, target: b.target, requested_by: c.user, requested_at: nowIso(),
      reason, status: 'PENDING_APPROVAL', approved_by: null, executed_at: null, expires_at: new Date(Date.now() + 4 * 3600_000).toISOString(),
    };
    db.switchRequests.unshift(req);
    audit(db, c, b.target === 'LEGACY' ? 'switch.rollback.request' : 'switch.request', label, { reason, request: req.id });
    return req;
  },
  'GET /switch/requests': (db) => db.switchRequests,
  'POST /switch/requests/{id}/approve': (db, c) => {
    const reason = requireReason(c.body);
    const req = db.switchRequests.find((r) => r.id === c.id);
    if (!req) throw new Fail(404, 'NOT_FOUND', c.id);
    const block = approvalBlock(req, c.user, c.roles);
    if (block === 'self') throw new Fail(409, 'TWO_PERSON_RULE', 'approver must differ from requester');
    if (block) throw new Fail(409, `NOT_APPROVABLE_${block.toUpperCase()}`, block);
    requireConfirm(c.body, expectedConfirmText(req.target === 'LEGACY' ? 'rollback' : 'switch', req.scope_label));
    req.status = 'EXECUTED';
    req.approved_by = c.user;
    req.executed_at = nowIso();
    executeSwitch(db, req);
    audit(db, c, 'switch.approve', req.scope_label, { reason, request: req.id, target: req.target });
    return req;
  },
  'POST /switch/requests/{id}/reject': (db, c) => {
    const reason = requireReason(c.body);
    const req = db.switchRequests.find((r) => r.id === c.id);
    if (!req) throw new Fail(404, 'NOT_FOUND', c.id);
    if (req.status !== 'PENDING_APPROVAL') throw new Fail(409, 'NOT_PENDING', req.status);
    req.status = 'REJECTED';
    audit(db, c, 'switch.reject', req.scope_label, { reason, request: req.id });
    return req;
  },
  'GET /hub/health': (db) => db.hub,
  'GET /topology': (db) => db.topology,
  'POST /topology/plan': (db, c) => {
    const b = c.body as Partial<TopologyPlanRequest> | undefined;
    if (b?.to !== 'ha' && b?.to !== 'standalone') throw new Fail(400, 'BAD_REQUEST', "to must be 'ha' or 'standalone'");
    return planTopology(db.topology, { to: b.to, db_url: b.db_url, spool_url: b.spool_url });
  },
  'POST /topology/connections/{id}/release-lease': (db, c) => {
    const reason = requireReason(c.body);
    const conn = db.topology.connections.find((x) => x.connection_id === c.id);
    if (!conn) throw new Fail(404, 'NOT_FOUND', c.id);
    if (conn.agent_mode !== 'active_passive' || !conn.holder) throw new Fail(409, 'NO_LEASE', 'connection has no capture lease');
    const res = { released: true, holder: conn.holder, epoch: conn.epoch };
    // simulate the standby taking over: new holder, epoch + 1 (fencing token)
    const standby = conn.agents.find((a) => a.role === 'standby');
    for (const a of conn.agents) {
      if (a.agent_id === conn.holder) Object.assign(a, { role: 'standby', state: 'STANDBY' });
    }
    if (standby) {
      Object.assign(standby, { role: 'active', state: 'CAPTURING' });
      conn.holder = standby.agent_id;
      conn.epoch += 1;
      conn.expires_at = new Date(Date.now() + 30_000).toISOString();
    } else {
      conn.holder = null;
      conn.expires_at = nowIso();
    }
    audit(db, c, 'lease.release', c.id, { reason, holder: res.holder, epoch: res.epoch });
    return res;
  },
  'GET /retention': (db) => db.retention,
  'PUT /retention': (db, c) => {
    const reason = requireReason(c.body);
    const b = c.body as RetentionUpdate;
    if (b.raw_archive_days !== undefined && (b.raw_archive_days < 30 || b.raw_archive_days > 90)) {
      throw new Fail(400, 'OUT_OF_RANGE', 'raw_archive_days must be 30..90 (ADR-007)');
    }
    if (b.spool_cap_bytes !== undefined && b.spool_cap_bytes <= 0) throw new Fail(400, 'OUT_OF_RANGE', 'spool_cap_bytes > 0');
    Object.assign(db.retention, { ...(b.raw_archive_days !== undefined ? { raw_archive_days: b.raw_archive_days } : {}), ...(b.spool_cap_bytes !== undefined ? { spool_cap_bytes: b.spool_cap_bytes } : {}) });
    audit(db, c, 'retention.update', 'hub', { ...b, reason });
    return db.retention;
  },
  'GET /users': (db) => db.users,
  'PUT /users/group-mappings': (db, c) => {
    const reason = requireReason(c.body);
    const b = c.body as GroupMappingsUpdate;
    if (!b.group_mappings.some((g) => g.role === 'PlatformAdmin')) throw new Fail(409, 'LOCKOUT', 'at least one PlatformAdmin group mapping is required');
    db.users.group_mappings = b.group_mappings;
    audit(db, c, 'users.group_mappings.update', 'rbac', { reason, count: b.group_mappings.length });
    return db.users;
  },
  'GET /audit': (db, c) => {
    const q = (c.query.get('q') ?? '').toLowerCase();
    return page(q ? db.audit.filter((a) => a.action.toLowerCase().includes(q) || a.actor.toLowerCase().includes(q)) : db.audit);
  },
  'GET /audit/export': (db, c) => (c.query.get('format') === 'csv'
    ? toCsv(db.audit as unknown as Record<string, unknown>[], ['at', 'actor', 'role', 'action', 'target', 'correlation_id', 'detail'])
    : JSON.stringify(db.audit, null, 2)),
};

function header(h: Record<string, string>, name: string): string | undefined {
  const k = Object.keys(h).find((x) => x.toLowerCase() === name.toLowerCase());
  return k ? h[k] : undefined;
}

export function createMockTransport(db: MockDb = createMockDb(), latencyMs = 0): Transport {
  return async (req: TransportRequest): Promise<TransportResponse> => {
    if (latencyMs) await new Promise((r) => setTimeout(r, latencyMs));
    const [pathOnly, qs] = req.path.split('?') as [string, string | undefined];
    const correlationId = header(req.headers, 'X-ALELT-Correlation-Id') ?? null;
    const err = (status: number, code: string, message: string): TransportResponse => ({ status, body: { error: { code, message, correlation_id: correlationId } } });
    const role = header(req.headers, 'X-ALELT-Dev-Role');
    if (!role || !(ROLES as readonly string[]).includes(role)) return err(401, 'UNAUTHENTICATED', 'missing or invalid X-ALELT-Dev-Role');
    const roles = [role as Role];
    const user = header(req.headers, 'X-ALELT-Dev-User') || `${role.toLowerCase()}@dev.local`;
    for (const r of ROUTES) {
      if (r.method !== req.method) continue;
      const m = r.re.exec(pathOnly);
      if (!m) continue;
      const perm = ENDPOINT_PERMISSION[r.key];
      if (perm && !can(roles, perm)) return err(403, 'FORBIDDEN', `${role} lacks ${perm}`);
      try {
        const body = HANDLERS[r.key](db, { user, roles, id: decodeURIComponent(m[1] ?? ''), query: new URLSearchParams(qs ?? ''), body: req.body, correlationId });
        return { status: 200, body: structuredClone(body) };
      } catch (e) {
        if (e instanceof Fail) return err(e.status, e.code, e.message);
        throw e;
      }
    }
    return err(404, 'NO_ROUTE', `${req.method} ${pathOnly}`);
  };
}
