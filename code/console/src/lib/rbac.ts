import type { Role } from '../api/contract';

/** Console permissions. The hub enforces the same matrix server-side; the console only hides what a role cannot do. */
export const PERMISSIONS = [
  'view.dashboard',
  'view.tables',
  'view.recon',
  'view.certification',
  'table.configure',
  'table.resync',
  'table.reprocess',
  'import.run',
  'revision.rollback',
  'schema.decide',
  'recon.run',
  'cert.signoff',
  'wave.plan',
  'switch.request',
  'switch.approve',
  'agent.view',
  'agent.command',
  'hub.view',
  'retention.edit',
  'users.manage',
  'audit.read',
  'audit.export',
  'view.incidents',
  'incident.act',
  'incident.reveal',
  'llm.configure',
] as const;
export type Permission = (typeof PERMISSIONS)[number];

const DATA_OPERATOR: Permission[] = [
  'view.dashboard', 'view.tables', 'view.recon', 'view.certification',
  'table.configure', 'table.resync', 'table.reprocess', 'import.run', 'revision.rollback', 'schema.decide',
  'recon.run', 'cert.signoff', 'wave.plan', 'switch.request', 'switch.approve',
  'view.incidents', 'incident.act', 'incident.reveal',
];

export const ROLE_PERMISSIONS: Record<Role, readonly Permission[]> = {
  Viewer: ['view.dashboard', 'view.tables', 'view.recon', 'view.incidents'],
  DataOperator: DATA_OPERATOR,
  PlatformAdmin: [
    'view.dashboard', 'view.tables', 'agent.view', 'agent.command', 'hub.view', 'retention.edit', 'users.manage',
    'audit.read', 'audit.export', 'view.incidents', 'incident.act', 'llm.configure',
  ],
  Auditor: ['view.dashboard', 'view.certification', 'audit.read', 'audit.export', 'view.incidents'],
};

export function can(roles: readonly Role[], p: Permission): boolean {
  return roles.some((r) => ROLE_PERMISSIONS[r].includes(p));
}

export interface NavItem {
  path: string;
  labelKey: NavLabelKey;
  /** Visible if the user holds this permission. */
  requires: Permission;
  /** Only in dev / lab mode (local evaluation install). */
  labOnly?: boolean;
}
export type NavLabelKey =
  | 'nav.overview' | 'nav.tables' | 'nav.import' | 'nav.revisions' | 'nav.schema' | 'nav.recon'
  | 'nav.certification' | 'nav.agent' | 'nav.hub' | 'nav.retention' | 'nav.users' | 'nav.audit'
  | 'nav.setup' | 'nav.topology' | 'nav.wizard' | 'nav.importWizard' | 'nav.pipelines' | 'nav.guard'
  | 'nav.incidents' | 'nav.aiAssistant' | 'nav.lab' | 'nav.about';

/** Screens 1–13 of prompt 10 (table detail is reached from Tables). */
export const NAV: readonly NavItem[] = [
  { path: '/', labelKey: 'nav.overview', requires: 'view.dashboard' },
  { path: '/incidents', labelKey: 'nav.incidents', requires: 'view.incidents' },
  { path: '/pipelines', labelKey: 'nav.pipelines', requires: 'view.dashboard' },
  { path: '/wizard', labelKey: 'nav.wizard', requires: 'table.configure' },
  { path: '/import/new', labelKey: 'nav.importWizard', requires: 'import.run' },
  { path: '/tables', labelKey: 'nav.tables', requires: 'view.tables' },
  { path: '/import', labelKey: 'nav.import', requires: 'import.run' },
  { path: '/revisions', labelKey: 'nav.revisions', requires: 'table.configure' },
  { path: '/schema-changes', labelKey: 'nav.schema', requires: 'schema.decide' },
  { path: '/recon', labelKey: 'nav.recon', requires: 'view.recon' },
  { path: '/certification', labelKey: 'nav.certification', requires: 'view.certification' },
  { path: '/agent', labelKey: 'nav.agent', requires: 'agent.view' },
  { path: '/hub', labelKey: 'nav.hub', requires: 'hub.view' },
  { path: '/topology', labelKey: 'nav.topology', requires: 'hub.view' },
  { path: '/retention', labelKey: 'nav.retention', requires: 'retention.edit' },
  { path: '/users', labelKey: 'nav.users', requires: 'users.manage' },
  { path: '/guard', labelKey: 'nav.guard', requires: 'users.manage' },
  { path: '/ai-assistant', labelKey: 'nav.aiAssistant', requires: 'llm.configure' },
  { path: '/audit', labelKey: 'nav.audit', requires: 'audit.read' },
  { path: '/setup', labelKey: 'nav.setup', requires: 'users.manage' },
  { path: '/lab', labelKey: 'nav.lab', requires: 'users.manage', labOnly: true },
  { path: '/about', labelKey: 'nav.about', requires: 'view.dashboard' },
];

export function navFor(roles: readonly Role[], labMode = false): NavItem[] {
  return NAV.filter((n) => can(roles, n.requires) && (!n.labOnly || labMode));
}
