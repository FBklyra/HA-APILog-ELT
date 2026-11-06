import type { EndpointKey, Role, SwitchRequest } from '../api/contract';
import type { Permission } from './rbac';
import { can } from './rbac';
import { DIAG_ENDPOINT_PERMISSION } from './diagnosePermissions';

/** Server-side permission per endpoint (the hub enforces this matrix; the mock API mirrors it). `null` = any authenticated user. */
export const ENDPOINT_PERMISSION: Record<EndpointKey, Permission | null> = {
  ...DIAG_ENDPOINT_PERMISSION,
  'GET /me': null,
  'GET /overview': 'view.dashboard',
  'GET /ladder': 'view.dashboard',
  'GET /tables': 'view.tables',
  'GET /tables/{id}': 'view.tables',
  'PUT /tables/{id}/config': 'table.configure',
  'POST /tables/{id}/resync': 'table.resync',
  'POST /tables/{id}/reprocess': 'table.reprocess',
  'GET /batches': 'view.recon',
  'GET /quarantine': 'view.recon',
  'GET /agents': 'agent.view',
  'POST /agents/{id}/commands': 'agent.command',
  'GET /revisions': 'table.configure',
  'GET /revisions/{id}/diff': 'table.configure',
  'POST /revisions/{id}/rollback': 'revision.rollback',
  'GET /revisions/{id}/export': 'table.configure',
  'POST /import/runs': 'import.run',
  'GET /import/runs': 'import.run',
  'POST /import/runs/{id}/accept': 'import.run',
  'GET /schema-changes': 'schema.decide',
  'POST /schema-changes/{id}/decision': 'schema.decide',
  'GET /recon': 'view.recon',
  'POST /recon/runs': 'recon.run',
  'GET /certification': 'view.certification',
  'POST /certification/{id}/signoff': 'cert.signoff',
  'GET /certification/waves': 'view.certification',
  'POST /certification/waves': 'wave.plan',
  'POST /switch': 'switch.request',
  'GET /switch/requests': 'view.certification',
  'POST /switch/requests/{id}/approve': 'switch.approve',
  'POST /switch/requests/{id}/reject': 'switch.approve',
  'GET /hub/health': 'hub.view',
  'GET /topology': 'hub.view',
  'POST /topology/plan': 'hub.view',
  'POST /topology/connections/{id}/release-lease': 'agent.command',
  'GET /retention': 'retention.edit',
  'PUT /retention': 'retention.edit',
  'GET /users': 'users.manage',
  'PUT /users/group-mappings': 'users.manage',
  'GET /audit': 'audit.read',
  'GET /audit/export': 'audit.export',
  'GET /import/plugins': 'import.run',
  'POST /import/discover': 'import.run',
  'POST /import/discover/files': 'import.run',
  'GET /import/runs/{id}': 'import.run',
  'PUT /import/runs/{id}/plan': 'import.run',
  'POST /import/runs/{id}/apply': 'import.run',
  'GET /import/runs/{id}/host-map': 'import.run',
  'PUT /import/runs/{id}/host-map': 'import.run',
  'GET /wizard/sources': 'table.configure',
  'POST /wizard/source/test': 'table.configure',
  'POST /wizard/destination/test': 'table.configure',
  'GET /wizard/defaults': 'table.configure',
  'POST /wizard/pipelines': 'table.configure',
  'POST /wizard/pipelines/{id}/agent': 'table.configure',
  'GET /wizard/pipelines/{id}/progress': 'view.dashboard',
  'GET /pipelines': 'view.dashboard',
  'GET /pipelines/{id}/tables.yaml': 'table.configure',
  'GET /guard/denylist': null, // table.configure OR users.manage: enforced by the hub (and the mock)
  'PUT /guard/denylist': 'users.manage',
  'POST /guard/check': 'table.configure',
};

export type ApprovalBlock = 'self' | 'role' | 'status' | 'expired';

/** Two-person rule: approver must be a different person holding switch.approve, request pending and not expired. */
export function approvalBlock(req: SwitchRequest, user: string, roles: readonly Role[], now: Date = new Date()): ApprovalBlock | null {
  if (req.status !== 'PENDING_APPROVAL') return 'status';
  if (new Date(req.expires_at).getTime() <= now.getTime()) return 'expired';
  if (!can(roles, 'switch.approve')) return 'role';
  if (req.requested_by.trim().toLowerCase() === user.trim().toLowerCase()) return 'self';
  return null;
}
