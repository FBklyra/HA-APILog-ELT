import type { Role } from './contract';
import type {
  CodeFix, DiagnoseEndpoints, ExplainRequest, ExplainResult, IncidentAction, IncidentDetail, LlmSettings,
  LlmSettingsUpdate,
} from './diagnose';
import { can } from '../lib/rbac';

/** In-browser mock of the incidents / diagnosis endpoints (hub/alelt_hub/diagnose_api.py). Same rules as the hub:
 * reason on mutations, LLM only with incident.act and when configured, API key write-only. */
interface DiagState {
  incidents: IncidentDetail[];
  llm: LlmSettings;
  keySet: boolean;
  values: Record<string, { column: string; position: string | null; key: Record<string, string>; value: string }[]>;
  seq: number;
}
interface Ctx { user: string; roles: Role[]; id: string; query: URLSearchParams; body: unknown; correlationId: string | null }
type Fail = new (status: number, code: string, message: string) => Error;
interface Helpers<Db> {
  Fail: Fail;
  requireReason: (body: unknown) => string;
  audit: (db: Db, c: Ctx, action: string, target: string, detail?: Record<string, unknown>) => void;
}

const now = () => new Date().toISOString();
const PATCH = `diff --git a/connectors/sources/db2/alelt_src_db2/rowimage.py b/connectors/sources/db2/alelt_src_db2/rowimage.py
--- a/connectors/sources/db2/alelt_src_db2/rowimage.py
+++ b/connectors/sources/db2/alelt_src_db2/rowimage.py
@@ -86,5 +86,5 @@ def bcd(data: bytes, digits: int, scale: int) -> Decimal:
     sign = data[-1] & 0x0F
-    if sign not in (0xC, 0xD):
+    if sign not in (0xA, 0xB, 0xC, 0xD, 0xE, 0xF):
         raise RowImageError(Err.BAD_BCD, f"sign nibble {sign:X}")
diff --git a/tests/golden/db2/bcd_sign_f/case.json b/tests/golden/db2/bcd_sign_f/case.json
new file mode 100644
--- /dev/null
+++ b/tests/golden/db2/bcd_sign_f/case.json
@@ -0,0 +1 @@
+{"table": "APPSCHEMA.ORDERS", "column": "AMT", "raw_hex": "...", "expected": "12.50"}
`;

function base(id: string, o: Partial<IncidentDetail>): IncidentDetail {
  return {
    id, kind: 'QUARANTINE', severity: 'warning', status: 'DIAGNOSED', title: '', connection_id: 'erpprod', table: null,
    batch_id: null, job_id: null, agent_id: null, position: null, error: null, count: 1, first_seen: now(), last_seen: now(),
    rule_id: null, root_cause: null, explanation: null, confidence: null, fix_steps: [], classification: 'UNKNOWN',
    source_data: null, resolved_by: null, resolved_at: null, confirmed_cause: null, timeline: [], actions: [], findings: [],
    ai: [], code_fixes: [], sensitive_samples: 0, ...o,
  };
}

function action(id: string, kind: IncidentAction['kind'], description: string, safe: boolean, params: Record<string, unknown> = {}): IncidentAction {
  return { id, kind, params, description, safe, rule_id: null, status: 'PROPOSED', created_at: now(), decided_by: null, decided_at: null, reason: null, result: null };
}

export function createDiagnoseMock(): DiagState {
  const codeFix: CodeFix = {
    id: 'cf-1', incident_id: 'inc-1', status: 'PROPOSED', summary: 'Accept every positive BCD sign nibble (A, C, E, F)',
    explanation: 'Db2 writes 0xF as the sign of some packed decimals; the decoder only accepted 0xC/0xD.',
    files: ['connectors/sources/db2/alelt_src_db2/rowimage.py'], patch: PATCH, test_path: 'tests/golden/db2/bcd_sign_f/case.json',
    validation: { paths: ['connectors/sources/db2/alelt_src_db2/rowimage.py', 'tests/golden/db2/bcd_sign_f/case.json'], paths_ok: true, disallowed: [], has_test: true, apply_check: 'ok', message: '' },
    regression_case: { path: 'tests/golden/db2/incident-inc1/case.json', content: { expected: null } }, model: 'mock-model', confidence: 0.7,
    created_at: now(), created_by: 'dataoperator@dev.local', reviewed_by: null, reviewed_at: null,
  };
  return {
    seq: 0,
    keySet: false,
    values: { 'inc-2': [{ column: 'SHIP_TS', position: '1:00000000000A1F00', key: { ORDER_ID: '48213' }, value: '2087-02-30 00:00:00' }] },
    llm: { enabled: false, provider: 'anthropic', url: 'https://api.anthropic.com', model: 'claude-sonnet-5-5', timeout_s: 30, max_tokens_day: 200000, max_output_tokens: 1500, redaction: 'strict', key_set: false, configured: false, tokens_used_today: 0, source: { provider: 'default', model: 'default' } },
    incidents: [
      base('inc-1', {
        severity: 'critical', title: 'QUARANTINE (decode) - APPSCHEMA.ORDERS - row image: BAD_BCD: AMT sign nibble F', table: 'APPSCHEMA.ORDERS',
        position: '1:00000000000A0F00', error: 'row image: BAD_BCD: AMT sign nibble F', count: 14, rule_id: 'decoder_defect',
        root_cause: 'The decoder raised BAD_BCD on records whose table layout did not change (ALELT defect).',
        explanation: 'ALELT could not read some changes because of a bug in its decoder. The raw records are kept.',
        confidence: 0.65, classification: 'PLATFORM_DEFECT',
        fix_steps: ['Ask the AI assistant for a code-fix proposal.', 'Review the patch.', 'Re-decode the range after the release.'],
        actions: [action('act-1', 'REDECODE_RANGE', 'Re-decode APPSCHEMA.ORDERS from the raw archive after the decoder fix is released.', true,
          { table: 'APPSCHEMA.ORDERS', from_pos: '1:00000000000A0F00', to_pos: '1:00000000000A0FFF' })],
        timeline: [{ at: now(), kind: 'OPENED', actor: 'alelt_diagnose', detail: {} }, { at: now(), kind: 'DIAGNOSED', actor: 'alelt_diagnose', detail: { rule_id: 'decoder_defect' } }],
        code_fixes: [codeFix],
      }),
      base('inc-2', {
        kind: 'SOURCE_DATA', title: 'SOURCE_DATA (decode) - APPSCHEMA.ORDERS - source data future_timestamp in APPSCHEMA.ORDERS.SHIP_TS',
        table: 'APPSCHEMA.ORDERS', classification: 'SOURCE_DATA', rule_id: 'source_future_timestamp', confidence: 0.9, count: 3,
        root_cause: 'The source rows contain future timestamp values in column SHIP_TS; this is a data quality issue in the application data.',
        explanation: 'Column SHIP_TS contains dates/times in the future. This usually comes from a wrong clock or a placeholder value in the application.',
        source_data: {
          issue: 'FUTURE_TIMESTAMP', table: 'APPSCHEMA.ORDERS', column: 'SHIP_TS', key_columns: ['ORDER_ID'], positions: ['1:00000000000A1F00'], count: 3,
          explanation: 'Column SHIP_TS contains dates/times in the future.', handling_options: ['QUARANTINE_SKIP', 'COERCE', 'ACCEPT_AS_IS'],
          recommended: 'ACCEPT_AS_IS',
          dba_sql: 'SELECT "ORDER_ID", "SHIP_TS" FROM "APPSCHEMA"."ORDERS" WHERE "SHIP_TS" > CURRENT TIMESTAMP + 1 DAY FETCH FIRST 1000 ROWS ONLY WITH UR',
        },
        sensitive_samples: 1,
        actions: [action('act-2', 'SOURCE_DATA_POLICY', "Record handling 'ACCEPT_AS_IS' for FUTURE_TIMESTAMP on APPSCHEMA.ORDERS.SHIP_TS.", false,
          { table: 'APPSCHEMA.ORDERS', column: 'SHIP_TS', issue: 'FUTURE_TIMESTAMP', option: 'ACCEPT_AS_IS' })],
      }),
      base('inc-3', {
        kind: 'DESTINATION_ERROR', severity: 'critical', title: 'DESTINATION_ERROR (load) - erpprod - OperationalError: connection refused',
        error: 'OperationalError: connection refused', classification: 'INFRASTRUCTURE', rule_id: 'network_tls', confidence: 0.75, job_id: 42,
        root_cause: 'The endpoint could not be reached (service down, DNS, firewall).',
        explanation: 'ALELT cannot reach a system it depends on. It retries automatically; nothing is lost.',
        fix_steps: ['Check the endpoint is up and reachable from the hub.', 'Retry the job once reachable.'],
        actions: [action('act-3', 'RETRY_JOB', 'Retry the job now instead of waiting for backoff.', true, { job_id: 42 })],
      }),
    ],
  };
}

const STATES = new WeakMap<object, DiagState>();
const stateOf = (db: object): DiagState => {
  let s = STATES.get(db);
  if (!s) STATES.set(db, (s = createDiagnoseMock()));
  return s;
};

type H<Db> = Record<keyof DiagnoseEndpoints, (db: Db, c: Ctx) => unknown>;

export function diagnoseHandlers<Db extends object>(h: Helpers<Db>): H<Db> {
  const find = (db: Db, id: string): IncidentDetail => {
    const i = stateOf(db).incidents.find((x) => x.id === id);
    if (!i) throw new h.Fail(404, 'NOT_FOUND', `incident ${id}`);
    return i;
  };
  const summary = ({ timeline: _t, actions: _a, findings: _f, ai: _i, code_fixes: _c, sensitive_samples: _s, ...rest }: IncidentDetail) => rest;
  const fix = (db: Db, id: string): [IncidentDetail, CodeFix] => {
    for (const i of stateOf(db).incidents) {
      const f = i.code_fixes.find((x) => x.id === id);
      if (f) return [i, f];
    }
    throw new h.Fail(404, 'NOT_FOUND', `code fix ${id}`);
  };
  const explain = (db: Db, c: Ctx): ExplainResult => {
    const b = c.body as ExplainRequest;
    const st = stateOf(db);
    const inc = b.kind === 'incident' ? find(db, b.ref_id) : null;
    const useLlm = (b.use_llm ?? true) && st.llm.configured && can(c.roles, 'incident.act');
    return {
      kind: b.kind, ref_id: b.ref_id, source: useLlm ? 'llm' : 'rules', rule_id: inc?.rule_id ?? 'unclassified',
      root_cause: inc?.root_cause ?? (b.kind === 'quarantine' ? 'The record could not be decoded; it is kept in quarantine with its raw bytes.' : 'No deterministic rule matched this failure.'),
      explanation: useLlm ? 'AI summary: the evidence points to the rule finding above (mock).' : (inc?.explanation ?? 'ALELT could not classify this error automatically.'),
      fix_steps: inc?.fix_steps.length ? inc.fix_steps : ['Read the error in the evidence pack.', 'Follow the incident runbook.'],
      confidence: inc?.confidence ?? 0.1, needs_human: true, llm_configured: st.llm.configured, llm_error: null,
      classification: inc?.classification ?? 'UNKNOWN',
    };
  };
  return {
    'GET /meta': (db) => {
      const l = stateOf(db).llm;
      return { llm: { configured: l.configured, enabled: l.enabled, provider: l.provider, model: l.model }, diagnose: { rules: 27, auto_actions: [] } };
    },
    'GET /incidents': (db, c) => {
      const st = c.query.get('status');
      return stateOf(db).incidents.filter((i) => !st || (st === 'ACTIVE' ? i.status !== 'RESOLVED' : i.status === st)).map(summary);
    },
    'GET /incidents/summary': (db) => {
      const open = stateOf(db).incidents.filter((i) => i.status !== 'RESOLVED');
      const n = (s: string) => open.filter((i) => i.severity === s).length;
      return { open: open.length, critical: n('critical'), warning: n('warning'), info: n('info'), by_kind: {} };
    },
    'GET /incidents/{id}': (db, c) => find(db, c.id),
    'GET /incidents/{id}/evidence': (db, c) => ({ trigger: { kind: find(db, c.id).kind }, redaction: 'strict', quarantine: [], logs: [] }),
    'POST /incidents/{id}/diagnose': (db, c) => {
      const i = find(db, c.id);
      const b = (c.body ?? {}) as { use_llm?: boolean };
      if (b.use_llm) {
        if (!can(c.roles, 'incident.act')) throw new h.Fail(403, 'FORBIDDEN', 'requires incident.act');
        const reason = h.requireReason(c.body);
        const st = stateOf(db);
        if (!st.llm.configured) {
          i.ai.unshift({ id: `ai-${++st.seq}`, at: now(), actor: c.user, provider: st.llm.provider, model: st.llm.model, prompt_hash: null, answer: null, tokens: null, error: 'AI diagnosis not configured' });
        } else {
          i.ai.unshift({ id: `ai-${++st.seq}`, at: now(), actor: c.user, provider: st.llm.provider, model: st.llm.model, prompt_hash: 'f'.repeat(64),
            answer: { root_cause: `AI: ${i.root_cause ?? 'unknown'}`, confidence: 0.6, reasoning_summary: 'Mock answer based on the redacted evidence.', fix_steps: i.fix_steps, risk: 'medium', needs_human: true },
            tokens: 1200, error: null });
        }
        h.audit(db, c, 'incident.diagnose.ai', i.id, { reason });
      }
      i.timeline.push({ at: now(), kind: 'DIAGNOSED', actor: c.user, detail: { stage: b.use_llm ? 'B' : 'A' } });
      return i;
    },
    'POST /incident-actions/{id}/decision': (db, c) => {
      const reason = h.requireReason(c.body);
      const b = c.body as { decision: 'APPROVE' | 'REJECT' };
      for (const i of stateOf(db).incidents) {
        const a = i.actions.find((x) => x.id === c.id);
        if (!a) continue;
        if (a.status !== 'PROPOSED') throw new h.Fail(409, 'ACTION_STATE', `action is ${a.status}`);
        a.status = b.decision === 'APPROVE' ? 'EXECUTED' : 'REJECTED';
        Object.assign(a, { decided_by: c.user, decided_at: now(), reason });
        i.timeline.push({ at: now(), kind: `ACTION_${a.status}`, actor: c.user, detail: { action_id: a.id } });
        h.audit(db, c, `incident.action.${b.decision === 'APPROVE' ? 'approve' : 'reject'}`, i.id, { action_id: a.id, reason });
        return { id: a.id, status: a.status };
      }
      throw new h.Fail(404, 'NOT_FOUND', c.id);
    },
    'POST /incidents/{id}/resolve': (db, c) => {
      const reason = h.requireReason(c.body);
      const i = find(db, c.id);
      if (i.status === 'RESOLVED') throw new h.Fail(409, 'INCIDENT_STATE', 'incident already resolved');
      const b = c.body as { confirmed_root_cause: string | null };
      Object.assign(i, { status: 'RESOLVED', resolved_by: c.user, resolved_at: now(), confirmed_cause: b.confirmed_root_cause });
      i.timeline.push({ at: now(), kind: 'RESOLVED', actor: c.user, detail: { reason } });
      h.audit(db, c, 'incident.resolve', i.id, { reason, confirmed_cause: b.confirmed_root_cause });
      return i;
    },
    'GET /incidents/{id}/regression-note': (db, c) => `# Regression case - incident ${find(db, c.id).id}\n`,
    'POST /incidents/{id}/code-fix': (db, c) => {
      const reason = h.requireReason(c.body);
      const i = find(db, c.id);
      const st = stateOf(db);
      if (i.classification !== 'PLATFORM_DEFECT') throw new h.Fail(409, 'CODE_FIX_UNAVAILABLE', `code fix only for PLATFORM_DEFECT incidents (this one is ${i.classification})`);
      if (!st.llm.configured) throw new h.Fail(409, 'CODE_FIX_UNAVAILABLE', 'AI diagnosis not configured');
      const f: CodeFix = { ...i.code_fixes[0]!, id: `cf-${++st.seq}`, status: 'PROPOSED', created_at: now(), created_by: c.user, reviewed_by: null, reviewed_at: null };
      i.code_fixes.push(f);
      h.audit(db, c, 'incident.code_fix.propose', i.id, { reason });
      return f;
    },
    'POST /code-fixes/{id}/review': (db, c) => {
      const reason = h.requireReason(c.body);
      const [i, f] = fix(db, c.id);
      if (f.status !== 'PROPOSED') throw new h.Fail(409, 'CODE_FIX_STATE', `code fix is ${f.status}`);
      const ok = (c.body as { decision: string }).decision === 'APPROVE';
      Object.assign(f, { status: ok ? 'REVIEWED' : 'REJECTED', reviewed_by: c.user, reviewed_at: now() });
      i.timeline.push({ at: now(), kind: `CODE_FIX_${f.status}`, actor: c.user, detail: { reason } });
      h.audit(db, c, ok ? 'incident.code_fix.review' : 'incident.code_fix.reject', i.id, { reason });
      return f;
    },
    'GET /code-fixes/{id}/patch': (db, c) => fix(db, c.id)[1].patch,
    'POST /incidents/{id}/source-values': (db, c) => {
      const reason = h.requireReason(c.body);
      find(db, c.id);
      h.audit(db, c, 'incident.source_values.reveal', c.id, { reason });
      return stateOf(db).values[c.id] ?? [];
    },
    'POST /diagnose/explain': explain,
    'POST /diagnose/scan': () => ({ triggers: 0 }),
    'GET /admin/llm': (db) => stateOf(db).llm,
    'PUT /admin/llm': (db, c) => {
      const reason = h.requireReason(c.body);
      const st = stateOf(db);
      const { reason: _r, api_key, ...rest } = c.body as LlmSettingsUpdate;
      if (api_key !== undefined) st.keySet = api_key !== '';
      Object.assign(st.llm, Object.fromEntries(Object.entries(rest).filter(([, v]) => v !== undefined)));
      st.llm.key_set = st.keySet;
      st.llm.configured = st.llm.enabled && !!st.llm.model && !!st.llm.url && (st.keySet || st.llm.provider === 'ollama');
      h.audit(db, c, 'llm.settings.update', 'diag.llm', { reason, api_key: api_key ? '***' : undefined });
      return st.llm;
    },
    'POST /admin/llm/test': (db, c) => {
      const b = (c.body ?? {}) as LlmSettingsUpdate;
      const st = stateOf(db);
      const key = (b.api_key ?? '') !== '' || st.keySet || (b.provider ?? st.llm.provider) === 'ollama';
      return key ? { ok: true, latency_ms: 420, model: b.model ?? st.llm.model, error: null }
        : { ok: false, latency_ms: 3, model: b.model ?? st.llm.model, error: 'anthropic: HTTP 401: missing API key' };
    },
  };
}
