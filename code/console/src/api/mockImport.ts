import type {
  AgentPackage, ApplyRequest, ApplyResponse, CheckItem, DefaultTable, DenyList, DenyListUpdate, DestTestRequest,
  DestTestResult, DiscoverFilesRequest, DiscoverRequest, EndpointKey, HostMap, HostMapUpdate, ImportSession, MigrationPlan,
  Pipeline,
  PipelineCreate, PipelineCreated, PlanEdits, PluginInfo, Progress, Role, SourceTestRequest, SourceTestResult,
  WizardDefaults, WizardSource,
} from './contract';

/** In-browser mock of the importer / pipeline wizard / guard endpoints (hub/alelt_hub/importer.py). */
export interface ImportMockState {
  sessions: Record<string, { s: ImportSession; base: MigrationPlan | null }>;
  pipelines: Pipeline[];
  progress: Record<string, number>;
  denylist: string[];
  /** import run id -> stored prod host -> test host mapping (PUT /import/runs/{id}/host-map) */
  hostmaps: Record<string, Record<string, string>>;
}

export function createImportMock(): ImportMockState {
  return { sessions: {}, pipelines: [], progress: {}, hostmaps: {}, denylist: ['db2prod.corp.local', 'xy12345.canada-central'] };
}

interface Ctx { user: string; roles: Role[]; id: string; query: URLSearchParams; body: unknown }
type Fail = new (status: number, code: string, message: string) => Error;
interface Helpers<Db> {
  Fail: Fail;
  requireReason: (body: unknown) => string;
  audit: (db: Db, c: Ctx & { correlationId: string | null }, action: string, target: string, detail?: Record<string, unknown>) => void;
  state: (db: Db) => ImportMockState;
}

const PLUGINS: PluginInfo[] = [
  { name: 'legacy_etl', display_name: 'Legacy ETL (files)', description: 'Mock plugin: saved API responses or a live read-only key.',
    credential_fields: [
      { name: 'api_key', label: 'API key', secret: false, required: true, multiline: false, help: 'Read-only key, used for this run only.' },
      { name: 'api_secret', label: 'API secret', secret: true, required: true, multiline: false, help: '' }],
    read_only: true, accepts_files: true, file_hint: 'Saved API responses (JSON or ZIP) + landing DDL (.sql)', live: true },
  { name: 'yaml_manifest', display_name: 'YAML / JSON manifest', description: 'Describe an existing pipeline estate in a file.',
    credential_fields: [{ name: 'manifest', label: 'Manifest (YAML or JSON)', secret: false, required: true, multiline: true, help: '' }],
    read_only: true, accepts_files: true, file_hint: 'manifest.yaml', live: false },
];

const col = (pk = false, enabled = true, hashed = false) => ({ enabled, hashed, is_primary_key: pk });
function cannedPlan(platform: string): MigrationPlan {
  const sched = { frequency_minutes: 15, daily_time: null, timezone: null, paused: false, schedule_type: 'auto' };
  return {
    plugin: 'legacy_etl', platform, generated_at: new Date().toISOString(),
    connections: [{
      legacy_id: 'conn_erp', legacy_name: 'legacy_erp', connection_id: 'legacy_erp', include: true, supported: true,
      source_engine: 'db2', destination_engine: 'snowflake', capture_mode: 'edge',
      source: { engine: 'db2', service: 'db2', settings: { host: 'db2prod.corp.local', port: 50000, database: 'ERPPROD', user: 'ETL_RO', password: '********' }, secrets: { password: 'src_db2_erp_password' } },
      destination: { engine: 'snowflake', legacy_id: 'grp_wh', name: 'Warehouse', settings: { host: 'xy12345.canada-central.azure.snowflakecomputing.com', database: 'RAW' }, secrets: { private_key: 'dst_grp_wh_private_key' } },
      tables: {
        connection_id: 'legacy_erp', source_engine: 'db2', destination_engine: 'snowflake', output_profile: 'golden_spec',
        schema_change_handling: 'ALLOW_COLUMNS', source_name: 'LEGACY_ERP', legacy_schema: 'LEGACY_ERP_APPSCHEMA',
        schemas: { APPSCHEMA: { enabled: true, name_in_destination: 'APPSCHEMA', tables: {
          CUSTOMER: { enabled: true, name_in_destination: 'CUSTOMER', sync_mode: 'SOFT_DELETE', columns: { CUSTOMER_ID: col(true), NOTES: col(false, false), SIN: col(false, true, true) } },
          ORDERS: { enabled: true, name_in_destination: 'ORDERS', sync_mode: 'HISTORY', columns: { ORDER_ID: col(true) } },
          ORDER_STATUS: { enabled: true, name_in_destination: 'ORDER_STATUS', sync_mode: 'SOFT_DELETE', columns: {} },
          DRIVER_BAK: { enabled: false, name_in_destination: 'DRIVER_BAK', sync_mode: 'SOFT_DELETE', columns: {} },
        } } },
      },
      latency_target_s: 900, schedule: sched, paused: false, legacy_schema: 'LEGACY_ERP_APPSCHEMA', enabled_tables: 3, total_tables: 4,
    }, {
      legacy_id: 'conn_pg', legacy_name: 'billing_pg', connection_id: 'billing_pg', include: false, supported: false,
      source_engine: 'postgres', destination_engine: 'snowflake', capture_mode: null,
      source: { engine: 'postgres', service: 'postgres_rds', settings: { host: 'pg.corp.local' }, secrets: {} },
      destination: { engine: 'snowflake', legacy_id: 'grp_wh', name: 'Warehouse', settings: {}, secrets: {} },
      tables: { connection_id: 'billing_pg', source_engine: 'postgres', destination_engine: 'snowflake', output_profile: 'golden_spec',
        schema_change_handling: 'ALLOW_ALL', source_name: 'BILLING_PG', legacy_schema: 'BILLING_PG_PUBLIC',
        schemas: { public: { enabled: true, name_in_destination: 'PUBLIC', tables: { invoice: { enabled: true, name_in_destination: 'INVOICE', sync_mode: 'SOFT_DELETE', columns: {} } } } } },
      latency_target_s: 3600, schedule: { ...sched, frequency_minutes: 60 }, paused: false, legacy_schema: 'BILLING_PG_PUBLIC', enabled_tables: 1, total_tables: 1,
    }],
    triggers: [
      { legacy_id: 'wh_1', kind: 'webhook', name: 'webhook-wh_1', include: true, target: 'https://hooks.corp.local/etl', events: ['load_completed', 'pipeline_error'], after_connections: ['legacy_erp'], settings: {} },
      { legacy_id: 'tr_dbt', kind: 'dbt_job', name: 'erp_models', include: true, target: 'proj_tm', events: ['load_completed'], after_connections: ['legacy_erp'], settings: { steps: ['dbt run --select erp'] } },
    ],
    roles: [
      { email: 'ana@example.test', name: 'Ana', roles: ['Viewer'], legacy_role: 'Analyst', include: true },
      { email: 'ops@example.test', name: 'Olga', roles: ['PlatformAdmin', 'DataOperator'], legacy_role: 'Account Administrator', include: false },
    ],
    secrets: [
      { key: 'dst_grp_wh_private_key', label: 'Warehouse: private_key', owner: 'destination:grp_wh', field: 'private_key', where: 'hub', used_by: ['legacy_erp'] },
      { key: 'src_db2_erp_password', label: 'legacy_erp: password', owner: 'connection:legacy_erp', field: 'password', where: 'agent', used_by: ['legacy_erp'] },
    ],
    warnings: [
      { code: 'SOURCE_PLANNED', severity: 'warning', message: "billing_pg: source type 'postgres_rds' is not supported yet (planned)", ref: 'connection:conn_pg' },
      { code: 'HASHED_COLUMNS', severity: 'warning', message: 'legacy_erp: 1 hashed column(s); verify hash parity during the parallel run', ref: 'connection:conn_erp' },
      { code: 'HISTORY_MODE', severity: 'warning', message: 'legacy_erp: 1 table(s) in history mode', ref: 'connection:conn_erp' },
      { code: 'NOT_EXPORTABLE', severity: 'info', message: 'credentials cannot be exported; enter secrets once', ref: null },
    ],
    summary: {},
  };
}

function summarize(p: MigrationPlan): MigrationPlan {
  const inc = p.connections.filter((c) => c.include);
  p.summary = {
    connections: p.connections.length, connections_included: inc.length,
    tables_enabled: inc.reduce((n, c) => n + c.enabled_tables, 0), triggers: p.triggers.filter((t) => t.include).length,
    roles: p.roles.filter((r) => r.include).length, warnings: p.warnings.filter((w) => w.severity !== 'info').length,
  };
  return p;
}

/** Same keys as the hub (alelt_import.guard.HOST_KEYS). */
const HOST_KEYS = /^(host|hostname|server|address|account|account_identifier|url|endpoint|base_url|dsn|api_host|tunnel_host|ssh_host)$/i;
const hostIn = (v: string) => v.trim().toLowerCase().replace(/^[a-z][a-z0-9+.-]*:\/\//, '').replace(/^[^@/]*@/, '').split('/')[0]!.split(':')[0]!;
/** Hosts referenced by a settings mapping (recursive), lower case, unique, in order. */
export function hostsOf(settings: unknown): string[] {
  const out: string[] = [];
  const walk = (o: unknown): void => {
    if (Array.isArray(o)) { o.forEach(walk); return; }
    if (!o || typeof o !== 'object') return;
    for (const [k, v] of Object.entries(o as Record<string, unknown>)) {
      if (typeof v === 'string' && HOST_KEYS.test(k) && v.trim()) { const h = hostIn(v); if (h && !out.includes(h)) out.push(h); }
      else if (v && typeof v === 'object') walk(v);
    }
  };
  walk(settings);
  return out;
}
/** Copy of `settings` with every mapped host replaced (ports, paths, credentials kept). */
function applyMap(settings: unknown, mapping: Record<string, string>): unknown {
  if (Array.isArray(settings)) return settings.map((v) => applyMap(v, mapping));
  if (!settings || typeof settings !== 'object') return settings;
  return Object.fromEntries(Object.entries(settings as Record<string, unknown>).map(([k, v]) => {
    if (typeof v === 'string' && HOST_KEYS.test(k) && v.trim()) {
      const h = hostIn(v);
      const i = v.toLowerCase().indexOf(h);
      return [k, h in mapping && i >= 0 ? v.slice(0, i) + mapping[h] + v.slice(i + h.length) : v];
    }
    return [k, v && typeof v === 'object' ? applyMap(v, mapping) : v];
  }));
}
const denyMatch = (list: string[], h: string): string | null => {
  const x = h.trim().toLowerCase();
  return list.find((e) => x === e || x.startsWith(`${e}.`)) ?? null;
};
export function deniedBy(list: string[], settings: Record<string, unknown>): string | null {
  for (const h of hostsOf(settings)) { const e = denyMatch(list, h); if (e) return `${h} (${e})`; }
  return null;
}
function normalizeMap(m: Record<string, string> | undefined, F: Fail): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(m ?? {})) {
    const k2 = String(k).trim().toLowerCase(); const v2 = String(v).trim();
    if (!k2 || !v2 || /[\s/@?#]/.test(v2)) throw new F(400, 'BAD_HOST_MAP', `invalid host mapping ${k} -> ${v} (target must be a host name or address)`);
    out[k2] = v2;
  }
  return out;
}
type PlanConn = MigrationPlan['connections'][number];
type HostEntry = HostMap['hosts'][number];
function requiredHosts(conns: PlanConn[]): HostEntry[] {
  const acc = new Map<string, HostEntry>();
  for (const pc of conns) {
    for (const side of ['source', 'destination'] as const) {
      for (const h of hostsOf(pc[side].settings)) {
        const e = acc.get(h) ?? { host: h, connections: [], sides: [], prod_denied: false, test_host: null, test_denied: false };
        if (!e.connections.includes(pc.connection_id)) e.connections.push(pc.connection_id);
        if (!e.sides.includes(side)) e.sides.push(side);
        acc.set(h, e);
      }
    }
  }
  return [...acc.keys()].sort().map((h) => acc.get(h)!);
}

const CATALOG: { fqn: string; pk: boolean; cap: boolean }[] = [
  { fqn: 'APPSCHEMA.CUSTOMER', pk: true, cap: true }, { fqn: 'APPSCHEMA.ORDERS', pk: true, cap: true },
  { fqn: 'APPSCHEMA.ORDER_STATUS', pk: false, cap: true }, { fqn: 'APPSCHEMA.DRIVER', pk: true, cap: false },
  { fqn: 'APPSCHEMA.DRIVER_BAK', pk: true, cap: true },
];

function checks(withCatalog: boolean): CheckItem[] {
  return [
    { id: 'archive_logging', label: 'Archive logging enabled (LOGARCHMETH1 is not OFF)', status: 'UNKNOWN', detail: 'checked by the agent',
      check_sql: 'db2 get db cfg for ERPTEST | findstr LOGARCHMETH1', fix_sql: 'db2 update db cfg for ERPTEST using LOGARCHMETH1 DISK:D:\\db2archive\\' },
    withCatalog
      ? { id: 'data_capture', label: 'DATA CAPTURE CHANGES on every captured table', status: 'FAIL', detail: '1 of 5 table(s) need it', tables: ['APPSCHEMA.DRIVER'], fix_sql: 'ALTER TABLE "APPSCHEMA"."DRIVER" DATA CAPTURE CHANGES;' }
      : { id: 'data_capture', label: 'DATA CAPTURE CHANGES on every captured table', status: 'UNKNOWN', detail: 'checked when the agent sends the catalog',
          check_sql: "SELECT TABSCHEMA, TABNAME FROM SYSCAT.TABLES WHERE TYPE = 'T' AND DATACAPTURE <> 'Y' AND TABSCHEMA IN ('APPSCHEMA') WITH UR",
          fix_sql: 'ALTER TABLE <SCHEMA>.<TABLE> DATA CAPTURE CHANGES;' },
    { id: 'authority', label: 'Capture user holds DBADM (or SYSADM) for db2ReadLog', status: 'UNKNOWN', detail: '', fix_sql: 'GRANT DBADM ON DATABASE TO USER ALELT_CAPTURE;' },
  ];
}

export function importHandlers<Db>(h: Helpers<Db>): Partial<Record<EndpointKey, (db: Db, c: Ctx & { correlationId: string | null }) => unknown>> {
  const { Fail, requireReason, audit, state } = h;
  const session = (db: Db, id: string) => {
    const s = state(db).sessions[id];
    if (!s) throw new Fail(404, 'NOT_FOUND', 'unknown import run');
    return s;
  };
  const newSession = (db: Db, c: Ctx, plugin: string, mode: 'live' | 'files', plan: MigrationPlan | null,
    files: { name: string; kind: string }[], golden: string[] | null, message: string | null): ImportSession => {
    const st = state(db);
    const id = `imp-${Object.keys(st.sessions).length + 1}`;
    const p = plan ? summarize(plan) : null;
    const s: ImportSession = {
      id, plugin, platform: PLUGINS.find((x) => x.name === plugin)?.display_name ?? plugin, mode,
      status: p ? 'DISCOVERED' : golden ? 'GOLDEN_ONLY' : 'FAILED', started_at: new Date().toISOString(), finished_at: new Date().toISOString(),
      started_by: c.user, files, message, inventory: p ? { connections: p.connections.length } : null, plan: p, warnings: p?.warnings ?? [],
      diff: p ? p.connections.filter((x) => x.include).map((x) => ({ path: x.connection_id, op: 'add' as const, before: null, after: { tables: x.enabled_tables } })) : [],
      needs_approval: p ? p.connections.some((x) => x.include && st.pipelines.some((q) => q.connection_id === x.connection_id)) : false,
      golden: golden ? { tables: golden, columns: golden.length * 4 } : null, requested_by: null, approved_by: null, applied_revision: null, result: null,
    };
    st.sessions[id] = { s, base: p ? structuredClone(p) : null };
    return s;
  };
  const pkg = (cid: string, deny: string[]): AgentPackage => ({
    token: `alelte_mock_${cid}_${Math.random().toString(36).slice(2, 14)}`, token_hint: 'alelte_mock...', agent_id: `${cid}-agent`,
    expires_at: new Date(Date.now() + 72 * 3600_000).toISOString(), hub_url: 'https://hub.example.test',
    aleltagent_json: { agent_id: `${cid}-agent`, connection_id: cid, hub: { url: 'https://hub.example.test', mode: 'token', token_env: 'ALELT_AGENT_TOKEN' }, deny_hosts: deny, environment: 'TEST', capture: { autostart: true } },
    devedge_json: { hub_url: 'https://hub.example.test', agent_id: `${cid}-agent`, connection_id: cid, token_env: 'ALELT_AGENT_TOKEN', deny_hosts: deny, environment: 'TEST', start: 'current' },
  });

  const hostMapView = (db: Db, s: ImportSession): HostMap => {
    const st = state(db);
    const stored = st.hostmaps[s.id] ?? {};
    const hosts = requiredHosts((s.plan?.connections ?? []).filter((x) => x.include)).map((e) => {
      const test_host = stored[e.host] ?? null;
      return { ...e, prod_denied: denyMatch(st.denylist, e.host) !== null, test_host, test_denied: !!test_host && denyMatch(st.denylist, test_host) !== null };
    });
    return { hosts, complete: hosts.every((e) => !!e.test_host && !e.test_denied) };
  };

  return {
    'GET /import/plugins': () => PLUGINS,
    'POST /import/discover': (db, c) => {
      const reason = requireReason(c.body);
      const b = c.body as DiscoverRequest;
      const p = PLUGINS.find((x) => x.name === b.plugin);
      if (!p) throw new Fail(404, 'UNKNOWN_PLUGIN', b.plugin);
      if (!p.live) throw new Fail(400, 'NOT_LIVE', `${p.display_name} supports file import only`);
      const ok = Object.values(b.credentials ?? {}).every((v) => v.trim() !== '');
      audit(db, c, 'import.discover', b.plugin, { reason, mode: 'live', ok });
      return newSession(db, c, b.plugin, 'live', ok ? cannedPlan(p.display_name) : null, [], null, ok ? null : 'API key and secret are required');
    },
    'POST /import/discover/files': (db, c) => {
      const reason = requireReason(c.body);
      const b = c.body as DiscoverFilesRequest;
      if (!b.files?.length) throw new Fail(400, 'BAD_FILE', 'no files');
      const kinds = b.files.map((f) => ({ name: f.name, kind: /\.sql$/i.test(f.name) ? 'ddl' : /\.csv$/i.test(f.name) ? 'columns' : /\.(json|zip|ya?ml)$/i.test(f.name) ? 'json' : 'unknown' }));
      const golden = kinds.some((k) => k.kind === 'ddl' || k.kind === 'columns') ? ['CUSTOMER', 'ORDERS', 'ORDER_STATUS'] : null;
      const defs = kinds.some((k) => k.kind === 'json');
      audit(db, c, 'import.discover', b.plugin ?? 'auto', { reason, mode: 'files', files: kinds.map((k) => k.name) });
      return newSession(db, c, b.plugin ?? 'legacy_etl', 'files', defs ? cannedPlan('Legacy ETL (offline export)') : null, kinds, golden,
        defs ? null : golden ? 'only a golden spec was found (DDL / columns); no pipeline definitions' : 'no recognised file');
    },
    'GET /import/runs/{id}': (db, c) => session(db, c.id).s,
    'PUT /import/runs/{id}/plan': (db, c) => {
      const entry = session(db, c.id);
      if (!entry.base) throw new Fail(404, 'NOT_FOUND', 'no plan');
      if (entry.s.status === 'APPLIED') throw new Fail(409, 'ALREADY_APPLIED', 'already applied');
      const edits = (c.body as { edits: PlanEdits }).edits;
      const p = structuredClone(entry.base);
      for (const [lid, e] of Object.entries(edits.connections ?? {})) {
        const pc = p.connections.find((x) => x.legacy_id === lid);
        if (!pc) throw new Fail(404, 'NOT_FOUND', lid);
        if (e.include !== undefined) {
          if (e.include && !pc.supported) throw new Fail(409, 'UNSUPPORTED', `${pc.legacy_name} cannot be included`);
          pc.include = e.include;
        }
        if (e.latency_target_s !== undefined) pc.latency_target_s = e.latency_target_s;
        for (const [fqn, te] of Object.entries(e.tables ?? {})) {
          const [s, t] = fqn.split('.') as [string, string];
          const td = pc.tables.schemas[s]?.tables[t];
          if (!td) throw new Fail(404, 'NOT_FOUND', fqn);
          if (te.enabled !== undefined) td.enabled = te.enabled;
          for (const [cn, ce] of Object.entries(te.columns ?? {})) {
            const cur = td.columns[cn] ?? { enabled: true, hashed: false, is_primary_key: false };
            if (cur.is_primary_key && (ce.enabled === false || ce.hashed)) throw new Fail(409, 'KEY_COLUMN', `key column ${fqn}.${cn}`);
            if (ce.enabled !== undefined) cur.enabled = ce.enabled;
            if (ce.hashed !== undefined) cur.hashed = ce.hashed;
            td.columns[cn] = cur;
          }
        }
        pc.enabled_tables = Object.values(pc.tables.schemas).flatMap((sd) => Object.values(sd.tables)).filter((x) => x.enabled).length;
      }
      for (const t of p.triggers) if (t.legacy_id in (edits.triggers ?? {})) t.include = edits.triggers[t.legacy_id]!;
      for (const r of p.roles) if (r.email in (edits.roles ?? {})) r.include = edits.roles[r.email]!;
      entry.s.plan = summarize(p);
      entry.s.requested_by = null;
      return entry.s;
    },
    'POST /import/runs/{id}/apply': (db, c): ApplyResponse => {
      const reason = requireReason(c.body);
      const b = c.body as ApplyRequest;
      const entry = session(db, c.id);
      const s = entry.s;
      if (s.status === 'APPLIED') throw new Fail(409, 'ALREADY_APPLIED', 'already applied');
      if (!s.plan) throw new Fail(409, 'NO_PLAN', s.message ?? 'nothing to apply');
      const st = state(db);
      const selected = s.plan.connections.filter((x) => x.include).map((pc) => ({ ...pc,
        source: { ...pc.source, settings: { ...pc.source.settings, ...(b.source_overrides?.[pc.legacy_id] ?? {}) } },
        destination: { ...pc.destination, settings: { ...pc.destination.settings, ...(b.destination_overrides?.[pc.legacy_id] ?? {}) } } }));
      // X-117: required prod -> test host mapping (stored + inline); targets never on the deny list
      const mapping = { ...(st.hostmaps[s.id] ?? {}), ...normalizeMap(b.host_map, Fail) };
      const need = requiredHosts(selected).map((e) => e.host);
      const unmapped = need.filter((x) => !(x in mapping));
      if (unmapped.length) throw new Fail(409, 'HOST_MAP_REQUIRED', `map every production host to its test host before apply: ${unmapped.join(', ')}`);
      const denied = [...new Set(need.map((x) => mapping[x]!).filter((x) => denyMatch(st.denylist, x)))].sort();
      if (denied.length) throw new Fail(409, 'PROD_TARGET', `mapping targets on the production deny list: ${denied.join(', ')}`);
      for (const pc of selected) {
        const d = deniedBy(st.denylist, applyMap(pc.source.settings, mapping) as Record<string, unknown>)
          ?? deniedBy(st.denylist, applyMap(pc.destination.settings, mapping) as Record<string, unknown>);
        if (d) throw new Fail(409, 'PROD_TARGET', `${pc.legacy_name}: ${d} matches the production deny list`);
      }
      if (s.needs_approval) {
        if (!s.requested_by) {
          s.requested_by = c.user;
          s.status = 'PENDING_APPROVAL';
          audit(db, c, 'import.apply.request', s.id, { reason });
          return { id: s.id, status: s.status, result: null, requested_by: c.user, applied_revision: null };
        }
        if (s.requested_by.toLowerCase() === c.user.toLowerCase()) throw new Fail(409, 'TWO_PERSON_RULE', 'a second DataOperator must approve this import');
        s.approved_by = c.user;
      }
      const conns = s.plan.connections.filter((x) => x.include);
      const saved = Object.keys(b.secrets ?? {}).filter((k) => b.secrets[k]?.value || b.secrets[k]?.ref);
      const result = {
        revision: 100 + Object.keys(st.sessions).length,
        connections: conns.map((pc) => {
          const missing = s.plan!.secrets.filter((x) => x.where === 'hub' && x.used_by.includes(pc.connection_id) && !saved.includes(x.key)).map((x) => x.key);
          return { connection_id: pc.connection_id, status: missing.length ? 'NEEDS_SECRETS' : 'SHADOW', config_file: `.alelt/pipelines/${pc.connection_id}.yaml`, missing_secrets: missing,
            ...(s.golden ? { golden: { covered: s.golden.tables, missing: [] } } : {}) };
        }),
        triggers: s.plan.triggers.filter((x) => x.include).length, roles_granted: s.plan.roles.filter((r) => r.include).flatMap((r) => r.roles.map((x) => `${r.email}:${x}`)), secrets_saved: saved,
      };
      for (const pc of conns) {
        st.pipelines = st.pipelines.filter((q) => q.connection_id !== pc.connection_id);
        st.pipelines.unshift({ connection_id: pc.connection_id, environment: b.environment, origin: `import:${s.id}`, status: 'SHADOW', active_source: 'LEGACY',
          source_engine: pc.source_engine ?? '', destination_engine: pc.destination_engine ?? '', latency_target_s: pc.latency_target_s, revision: result.revision,
          legacy_ref: pc.legacy_id, tables: pc.enabled_tables, source_host: String(pc.source.settings.host ?? ''), source_database: String(pc.source.settings.database ?? ''),
          created_at: new Date().toISOString(), created_by: c.user });
      }
      s.status = 'APPLIED';
      s.applied_revision = result.revision;
      s.result = result;
      audit(db, c, 'import.apply', s.id, { reason, revision: result.revision, environment: b.environment, host_map: mapping });
      return { id: s.id, status: 'APPLIED', result, requested_by: s.requested_by, applied_revision: result.revision };
    },
    'GET /import/runs/{id}/host-map': (db, c): HostMap => hostMapView(db, session(db, c.id).s),
    'PUT /import/runs/{id}/host-map': (db, c): HostMap => {
      const body = c.body as HostMapUpdate;
      const reason = body.reason?.trim() || 'map production hosts to test hosts';
      const s = session(db, c.id).s;
      const st = state(db);
      const m = normalizeMap(body.mappings, Fail);
      const bad = Object.values(m).filter((x) => denyMatch(st.denylist, x)).sort();
      if (bad.length) throw new Fail(409, 'PROD_TARGET', `test hosts on the production deny list: ${bad.join(', ')}`);
      st.hostmaps[s.id] = { ...(st.hostmaps[s.id] ?? {}), ...m };
      audit(db, c, 'import.hostmap', s.id, { mappings: m, reason });
      return hostMapView(db, s);
    },
    'GET /wizard/sources': (): WizardSource[] => [
      { engine: 'db2', display_name: 'IBM Db2 LUW', capture_mode: 'edge', available: true, fields: [
        { name: 'host', label: 'Host', required: true }, { name: 'port', label: 'Port', required: true, default: 50000 },
        { name: 'database', label: 'Database', required: true }, { name: 'user', label: 'User', required: true },
        { name: 'password', label: 'Password', secret: true }, { name: 'schemas', label: 'Schemas', default: '' }] },
      { engine: 'postgres', display_name: 'PostgreSQL', capture_mode: 'remote', available: false, fields: [] },
      { engine: 'sqlserver', display_name: 'SQL Server', capture_mode: 'remote', available: false, fields: [] },
    ],
    'POST /wizard/source/test': (db, c): SourceTestResult => {
      const b = c.body as SourceTestRequest;
      const d = deniedBy(state(db).denylist, b.settings ?? {});
      if (d) return { ok: false, steps: [{ step: 'guard', ok: false, detail: `'${d}' matches the production deny list: ALELT will not connect to it.` }], checks: [], denied: true };
      if (b.environment === 'PROD') return { ok: false, steps: [{ step: 'environment', ok: false, detail: 'parallel tests run against TEST sources only' }], checks: [], denied: true };
      const known = !!b.connection_id && state(db).pipelines.some((p) => p.connection_id === b.connection_id);
      return { ok: true, denied: false, catalog_tables: known ? CATALOG.length : null, checks: checks(known), steps: [
        { step: 'guard', ok: true, detail: 'not on the production deny list' },
        { step: 'network', ok: true, detail: `${String(b.settings.host ?? 'localhost')}:${String(b.settings.port ?? 50000)} reachable`, elapsed_ms: 12 },
        { step: 'catalog', ok: true, pending: !known, detail: known ? `${CATALOG.length} tables` : 'read on the source host by the agent after it connects (outbound only)' }] };
    },
    'POST /wizard/destination/test': (db, c): DestTestResult => {
      const b = c.body as DestTestRequest;
      const d = deniedBy(state(db).denylist, b.settings ?? {});
      if (d) return { ok: false, denied: true, steps: [{ step: 'guard', ok: false, detail: `'${d}' matches the production deny list` }] };
      const steps = [{ step: 'connect', ok: true, detail: b.engine === 'postgres' ? 'PostgreSQL 16.4' : 'Snowflake 8.40' }];
      if (b.environment === 'PROD') steps.push({ step: 'bootstrap', ok: true, detail: 'skipped: destination labelled PROD is read-only' });
      else if (b.bootstrap) steps.push({ step: 'bootstrap', ok: true, detail: 'control schema ready' });
      return { ok: true, denied: false, environment: b.environment, steps, elapsed_ms: 40 };
    },
    'GET /wizard/defaults': (db, c): WizardDefaults => {
      const sid = c.query.get('session_id');
      const entry = sid ? state(db).sessions[sid] : undefined;
      const pc = entry?.s.plan?.connections.find((x) => x.supported) ?? null;
      const golden = entry?.s.golden?.tables ?? [];
      const imported = pc ? Object.entries(pc.tables.schemas).flatMap(([s, sd]) => Object.entries(sd.tables).map(([t, td]) => [`${s}.${t}`, td.enabled] as const)) : [];
      const rows: DefaultTable[] = CATALOG.map(({ fqn, pk, cap }) => {
        const [schema, name] = fqn.split('.') as [string, string];
        const imp = imported.find(([f]) => f === fqn);
        let selected = cap; let reason: DefaultTable['reason'] = pk ? 'keyed' : 'keyless_surrogate';
        if (imp) { selected = imp[1] && cap; reason = cap ? 'imported' : 'capture_disabled'; }
        else if (!cap) { selected = false; reason = 'capture_disabled'; }
        else if (/_BAK$/.test(name)) { selected = false; reason = 'noise_name'; }
        else if (golden.length && !golden.includes(name)) { selected = false; reason = 'not_in_golden'; }
        return { fqn, schema, name, has_pk: pk, capture_enabled: cap, columns: 6, selected, reason, churn_per_day: name === 'ORDERS' ? 250_000 : null,
          high_churn: name === 'ORDERS', in_golden: golden.includes(name) };
      }).sort((a, b) => Number(b.selected) - Number(a.selected) || Number(b.has_pk) - Number(a.has_pk) || a.fqn.localeCompare(b.fqn));
      const hot = rows.filter((r) => r.selected && r.high_churn).map((r) => r.fqn);
      return { connection_id: c.query.get('connection_id') || 'erptest', source_name: 'ERPTEST', tables: rows,
        latency: hot.length ? { latency_target_s: 60, why: 'high_churn', tables: hot } : { latency_target_s: pc?.latency_target_s ?? 120, why: pc ? 'legacy_schedule' : 'default', tables: [] },
        output_profile: pc?.tables.output_profile ?? (golden.length ? 'golden_spec' : 'alelt'), schemas: ['APPSCHEMA'], catalog_available: true,
        golden_tables: golden, legacy_schema: pc?.legacy_schema ?? null, legacy_id: pc?.legacy_id ?? null, schedule: pc?.schedule ?? null, checks: checks(true) };
    },
    'POST /wizard/pipelines': (db, c): PipelineCreated => {
      const reason = (c.body as PipelineCreate).reason?.trim() || 'create parallel test';
      const b = c.body as PipelineCreate;
      const st = state(db);
      if (b.environment !== 'TEST') throw new Fail(409, 'PROD_READ_ONLY', 'the parallel-test wizard only creates TEST pipelines');
      const d = deniedBy(st.denylist, b.source ?? {}) ?? deniedBy(st.denylist, b.destination ?? {});
      if (d) throw new Fail(409, 'PROD_TARGET', `${d} matches the production deny list: ALELT will not connect to it`);
      if (!/^[a-z][a-z0-9_]{0,62}$/.test(b.connection_id)) throw new Fail(422, 'INVALID', 'connection_id');
      const tables = b.tables ?? CATALOG.filter((x) => x.cap).map((x) => x.fqn);
      st.pipelines = st.pipelines.filter((q) => q.connection_id !== b.connection_id);
      st.pipelines.unshift({ connection_id: b.connection_id, environment: 'TEST', origin: b.session_id ? `wizard+import:${b.session_id}` : 'wizard', status: 'SHADOW',
        active_source: 'LEGACY', source_engine: b.source_engine, destination_engine: b.destination_engine, latency_target_s: b.latency_target_s ?? 120, revision: 200,
        legacy_ref: b.legacy_id ?? null, tables: tables.length, source_host: String(b.source.host ?? ''), source_database: String(b.source.database ?? ''),
        created_at: new Date().toISOString(), created_by: c.user });
      st.progress[b.connection_id] = 0;
      audit(db, c, 'wizard.pipeline.create', b.connection_id, { reason, tables: tables.length });
      return { connection_id: b.connection_id, status: 'SHADOW', config_file: `.alelt/pipelines/${b.connection_id}.yaml`, revision: 200, tables,
        latency_target_s: b.latency_target_s ?? 120, output_profile: b.output_profile ?? 'alelt', agent: pkg(b.connection_id, st.denylist),
        kind: b.agent_kind ?? 'service', contract: null };
    },
    'POST /wizard/pipelines/{id}/agent': (db, c) => {
      requireReason(c.body);
      const p = state(db).pipelines.find((x) => x.connection_id === c.id);
      if (!p) throw new Fail(404, 'NOT_FOUND', 'unknown pipeline');
      if (p.environment !== 'TEST') throw new Fail(409, 'PROD_READ_ONLY', 'TEST pipelines only');
      return pkg(c.id, state(db).denylist);
    },
    'GET /wizard/pipelines/{id}/progress': (db, c): Progress => {
      const st = state(db);
      const p = st.pipelines.find((x) => x.connection_id === c.id);
      if (!p) throw new Fail(404, 'NOT_FOUND', 'unknown pipeline');
      const n = (st.progress[c.id] = (st.progress[c.id] ?? 0) + 1);
      const at = (k: number) => (n > k + 1 ? 'DONE' : n === k + 1 ? 'RUNNING' : 'PENDING') as 'DONE' | 'RUNNING' | 'PENDING';
      const tabs = CATALOG.filter((x) => x.cap).map((x, i) => ({ fqn: x.fqn, snapshot_rows: n > 2 ? 1000 * (i + 1) : 0, snapshot_done: n > 3, parity: n > 6 ? 'MATCH' : null }));
      return { connection_id: c.id, environment: p.environment, status: p.status, active_source: 'LEGACY', tables: tabs, steps: [
        { id: 'agent', status: n > 1 ? 'DONE' : 'PENDING', detail: n > 1 ? `${c.id}-agent` : 'waiting for the agent' },
        { id: 'prerequisites', status: n > 1 ? 'DONE' : 'PENDING', detail: n > 1 ? `${CATALOG.length} tables in catalog` : 'waiting for the catalog' },
        { id: 'initial_load', status: at(2), detail: `${n > 3 ? tabs.length : 0}/${tabs.length} tables` },
        { id: 'cdc', status: n > 4 ? 'RUNNING' : 'PENDING', detail: `${Math.max(0, n - 4) * 3} batches loaded` },
        { id: 'contract', status: at(4), detail: `${tabs.length}/${tabs.length} tables covered by the golden spec` },
        { id: 'parity', status: n > 6 ? 'DONE' : 'PENDING', detail: `${n > 6 ? tabs.length : 0}/${tabs.length} tables match` }] };
    },
    'GET /pipelines': (db) => state(db).pipelines,
    'GET /pipelines/{id}/tables.yaml': (db, c) => {
      const p = state(db).pipelines.find((x) => x.connection_id === c.id);
      if (!p) throw new Fail(404, 'NOT_FOUND', 'unknown pipeline');
      return `connection_id: ${p.connection_id}\nsource_engine: ${p.source_engine}\noutput_profile: alelt\nschemas: {}\n`;
    },
    'GET /guard/denylist': (db, c): DenyList => {
      if (!c.roles.some((r) => r === 'DataOperator' || r === 'PlatformAdmin')) throw new Fail(403, 'FORBIDDEN', 'requires table.configure or users.manage');
      const l = state(db).denylist;
      return { entries: l.map((entry) => ({ entry, source: 'console' as const })), effective: [...l] };
    },
    'PUT /guard/denylist': (db, c): DenyList => {
      const reason = requireReason(c.body);
      const b = c.body as DenyListUpdate;
      const bad = b.entries.find((e) => !/^[A-Za-z0-9*._:/[\]-]{1,253}$/.test(e.trim()));
      if (bad !== undefined) throw new Fail(400, 'BAD_ENTRY', `invalid deny list entry ${bad}`);
      const st = state(db);
      st.denylist = [...new Set(b.entries.map((e) => e.trim().toLowerCase()).filter(Boolean))].sort();
      audit(db, c, 'guard.denylist.update', 'prod_denylist', { reason, count: st.denylist.length });
      return { entries: st.denylist.map((entry) => ({ entry, source: 'console' as const })), effective: [...st.denylist] };
    },
    'POST /guard/check': (db, c) => (c.body as { targets: string[] }).targets.map((t) => ({ target: t, denied_by: denyMatch(state(db).denylist, hostIn(t)) })),
  };
}
