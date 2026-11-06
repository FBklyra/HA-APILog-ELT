import { describe, expect, it } from 'vitest';
import { ROLES } from '../api/contract';
import { NAV, PERMISSIONS, ROLE_PERMISSIONS, can, navFor } from './rbac';

const MUTATING = PERMISSIONS.filter((p) => !p.startsWith('view.') && p !== 'agent.view' && p !== 'hub.view' && p !== 'audit.read' && p !== 'audit.export');

describe('permissions matrix (prompt 10)', () => {
  it('Viewer has no mutating permission', () => {
    for (const p of MUTATING) expect(can(['Viewer'], p), p).toBe(false);
  });
  it('Auditor can read/export audit and nothing mutating', () => {
    expect(can(['Auditor'], 'audit.read')).toBe(true);
    expect(can(['Auditor'], 'audit.export')).toBe(true);
    for (const p of MUTATING) expect(can(['Auditor'], p), p).toBe(false);
  });
  it('DataOperator owns table lifecycle but not the platform', () => {
    for (const p of ['table.configure', 'table.resync', 'import.run', 'cert.signoff', 'switch.request', 'switch.approve'] as const) {
      expect(can(['DataOperator'], p), p).toBe(true);
    }
    for (const p of ['agent.command', 'users.manage', 'retention.edit', 'audit.read'] as const) expect(can(['DataOperator'], p), p).toBe(false);
  });
  it('PlatformAdmin runs agent/hub/users but cannot switch or certify', () => {
    for (const p of ['agent.command', 'users.manage', 'retention.edit', 'audit.read'] as const) expect(can(['PlatformAdmin'], p), p).toBe(true);
    for (const p of ['switch.request', 'switch.approve', 'cert.signoff', 'table.configure'] as const) expect(can(['PlatformAdmin'], p), p).toBe(false);
  });
  it('every role sees Overview; nav only lists permitted screens', () => {
    for (const r of ROLES) {
      const nav = navFor([r]);
      expect(nav[0]?.path).toBe('/');
      for (const n of nav) expect(ROLE_PERMISSIONS[r]).toContain(n.requires);
    }
    expect(navFor(['Auditor']).map((n) => n.path)).toEqual(['/', '/incidents', '/pipelines', '/certification', '/audit', '/about']);
    expect(navFor(['PlatformAdmin']).map((n) => n.path)).not.toContain('/lab');
    expect(navFor(['PlatformAdmin'], true).map((n) => n.path)).toContain('/lab');
    expect(navFor(['DataOperator'], true).map((n) => n.path)).not.toContain('/lab');
    expect(NAV).toHaveLength(22);
  });
  it('multiple roles union their permissions', () => {
    expect(can(['Viewer', 'Auditor'], 'audit.read')).toBe(true);
    expect(can([], 'view.dashboard')).toBe(false);
  });
});
