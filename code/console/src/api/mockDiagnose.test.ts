import { beforeEach, describe, expect, it } from 'vitest';
import { ApiClient } from './client';
import type { Role } from './contract';
import { diffLines } from './diagnose';
import { createMockDb, type MockDb } from './fixtures';
import { createMockTransport } from './mock';
import { ENDPOINT_PERMISSION } from '../lib/endpointPermissions';
import { can } from '../lib/rbac';

let db: MockDb;
const as = (role: Role, user?: string) =>
  new ApiClient(createMockTransport(db), async (): Promise<Record<string, string>> => ({ 'X-ALELT-Dev-Role': role, ...(user ? { 'X-ALELT-Dev-User': user } : {}) }));

beforeEach(() => {
  db = createMockDb();
});

describe('incidents & self-diagnosis (mock hub)', () => {
  it('every role lists incidents; only DataOperator/PlatformAdmin act; only DataOperator reveals raw values', async () => {
    for (const r of ['Viewer', 'DataOperator', 'PlatformAdmin', 'Auditor'] as Role[]) expect((await as(r).call('GET /incidents')).length).toBeGreaterThan(0);
    await expect(as('Viewer').call('POST /incident-actions/{id}/decision', { id: 'act-3', body: { reason: 'r', decision: 'APPROVE' } })).rejects.toMatchObject({ status: 403 });
    await expect(as('PlatformAdmin').call('POST /incidents/{id}/source-values', { id: 'inc-2', body: { reason: 'r' } })).rejects.toMatchObject({ status: 403 });
    const rows = await as('DataOperator').call('POST /incidents/{id}/source-values', { id: 'inc-2', body: { reason: 'owner asked' } });
    expect(rows[0]?.value).toBe('2087-02-30 00:00:00');
    expect(db.audit[0]).toMatchObject({ action: 'incident.source_values.reveal', target: 'inc-2' });
    expect(can(['Auditor'], 'incident.act')).toBe(false);
    expect(ENDPOINT_PERMISSION['PUT /admin/llm']).toBe('llm.configure');
  });

  it('requires a reason, approves once, resolves with a confirmed cause', async () => {
    const op = as('DataOperator', 'op@x.test');
    await expect(op.call('POST /incident-actions/{id}/decision', { id: 'act-3', body: { reason: ' ', decision: 'APPROVE' } })).rejects.toMatchObject({ code: 'REASON_REQUIRED' });
    expect((await op.call('POST /incident-actions/{id}/decision', { id: 'act-3', body: { reason: 'endpoint back', decision: 'APPROVE' } })).status).toBe('EXECUTED');
    await expect(op.call('POST /incident-actions/{id}/decision', { id: 'act-3', body: { reason: 'again', decision: 'APPROVE' } })).rejects.toMatchObject({ status: 409 });
    const r = await op.call('POST /incidents/{id}/resolve', { id: 'inc-3', body: { reason: 'fixed', confirmed_root_cause: 'firewall change' } });
    expect(r).toMatchObject({ status: 'RESOLVED', confirmed_cause: 'firewall change' });
    expect((await op.call('GET /incidents/summary')).open).toBe(2);
  });

  it('explains with rules when AI is not configured; Ask AI and code fix need a configured model', async () => {
    const op = as('DataOperator');
    const ex = await op.call('POST /diagnose/explain', { body: { kind: 'quarantine', ref_id: 'q-1' } });
    expect(ex).toMatchObject({ source: 'rules', llm_configured: false });
    expect((await op.call('GET /meta')).llm.configured).toBe(false);
    await expect(op.call('POST /incidents/{id}/code-fix', { id: 'inc-1', body: { reason: 'r' } })).rejects.toMatchObject({ code: 'CODE_FIX_UNAVAILABLE' });
    await expect(op.call('POST /incidents/{id}/code-fix', { id: 'inc-3', body: { reason: 'r' } })).rejects.toMatchObject({ code: 'CODE_FIX_UNAVAILABLE' });
  });

  it('AI assistant settings: PlatformAdmin only, key write-only, then LLM answers', async () => {
    await expect(as('DataOperator').call('GET /admin/llm')).rejects.toMatchObject({ status: 403 });
    const adm = as('PlatformAdmin');
    expect((await adm.call('POST /admin/llm/test', { body: {} })).ok).toBe(false);
    const s = await adm.call('PUT /admin/llm', { body: { reason: 'enable', enabled: true, api_key: 'sk-test-123' } });
    expect(s).toMatchObject({ key_set: true, configured: true });
    expect(JSON.stringify(s)).not.toContain('sk-test');
    expect(JSON.stringify(db.audit)).not.toContain('sk-test');
    const op = as('DataOperator');
    expect((await op.call('POST /diagnose/explain', { body: { kind: 'incident', ref_id: 'inc-1' } })).source).toBe('llm');
    expect((await as('Viewer').call('POST /diagnose/explain', { body: { kind: 'incident', ref_id: 'inc-1' } })).source).toBe('rules');
    const d = await op.call('POST /incidents/{id}/diagnose', { id: 'inc-1', body: { use_llm: true, reason: 'second opinion' } });
    expect(d.ai[0]?.answer?.root_cause).toMatch(/^AI:/);
  });

  it('code fix: review flow and diff rendering', async () => {
    const op = as('DataOperator');
    const f = await op.call('POST /code-fixes/{id}/review', { id: 'cf-1', body: { reason: 'looks right', decision: 'APPROVE' } });
    expect(f.status).toBe('REVIEWED');
    await expect(op.call('POST /code-fixes/{id}/review', { id: 'cf-1', body: { reason: 'x', decision: 'APPROVE' } })).rejects.toMatchObject({ status: 409 });
    const lines = diffLines(await op.call('GET /code-fixes/{id}/patch', { id: 'cf-1' }));
    expect(lines.some((l) => l.kind === 'add')).toBe(true);
    expect(lines.some((l) => l.kind === 'del')).toBe(true);
    expect(lines[0]?.kind).toBe('file');
    const sd = (await op.call('GET /incidents/{id}', { id: 'inc-2' })).source_data;
    expect(sd?.dba_sql).toMatch(/^SELECT .* WITH UR$/);
  });
});
