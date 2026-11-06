import type { TopologyMode, TopologyOperation, TopologyPlan, TopologyPlanRequest, TopologyStatus } from '../api/contract';

/** Seconds until the capture lease expires (0 when expired or unknown). */
export function leaseRemainingSeconds(expiresAt: string | null, now: number = Date.now()): number {
  if (!expiresAt) return 0;
  const t = Date.parse(expiresAt);
  return Number.isNaN(t) ? 0 : Math.max(0, Math.round((t - now) / 1000));
}

/** An operation is "live" while the console should keep polling it. */
export function operationLive(op: TopologyOperation | null): boolean {
  return !!op && (op.status === 'running' || op.status === 'waiting');
}

/** CLI the operator runs to apply a switch (the hub restarts on the new metadata/spool, so it cannot apply itself). */
export function applyCommand(req: TopologyPlanRequest): string {
  const parts = ['alelt', 'topology', 'apply', '--to', req.to];
  if (req.db_url?.trim()) parts.push('--db-url', quote(req.db_url.trim()));
  if (req.spool_url?.trim()) parts.push('--spool-url', quote(req.spool_url.trim()));
  return parts.join(' ');
}

function quote(v: string): string {
  return /^[\w:/.@+=%-]+$/.test(v) ? v : `'${v.replace(/'/g, `'\\''`)}'`;
}

/** Hide a password in a URL before it is shown or logged (user:secret@host -> user:***@host). */
export function redactUrl(url: string): string {
  return url.replace(/(\/\/[^:/@]+:)[^@]*@/, '$1***@');
}

const LAYER = (s: TopologyStatus, id: string) => s.layers.find((l) => l.layer === id);

/**
 * Plain-language dry run, mirroring the hub's step list (X-50). Used by the mock hub; the real hub computes its own
 * plan with live preflight checks.
 */
export function planTopology(s: TopologyStatus, req: TopologyPlanRequest): TopologyPlan {
  const to: TopologyMode = req.to;
  const from = s.mode;
  const blockers: string[] = [];
  const warnings: string[] = [];
  const meta = LAYER(s, 'metadata');
  const spool = LAYER(s, 'spool');
  const db = req.db_url?.trim() ?? '';
  const sp = req.spool_url?.trim() ?? '';
  if (from === to) warnings.push(`The hub already runs in ${to} mode; apply only re-checks and re-writes the generated config.`);
  if (to === 'ha') {
    if (meta && !meta.ha && !db) blockers.push('HA needs a PostgreSQL metadata database: give the target database URL (postgresql+psycopg://...).');
    if (db && !db.startsWith('postgresql')) blockers.push('The target metadata database for HA must be PostgreSQL (SQLite is single-host only).');
    if (spool && !spool.ha && !sp) blockers.push('HA needs a shared spool: give an S3-compatible URL (s3://bucket/prefix).');
    if (sp && !sp.startsWith('s3://')) blockers.push('The HA spool must be an S3-compatible object store (s3://bucket/prefix).');
  } else {
    if (sp && !sp.startsWith('file://')) blockers.push('Standalone uses a local spool directory (file:///var/lib/alelt/spool).');
    if (s.nodes.filter((n) => n.alive).length > 2) warnings.push('More than one API or worker replica is running: scale to one of each after the switch.');
  }
  if (s.connections.some((c) => c.agent_mode === 'active_passive') && to === 'standalone') {
    warnings.push('Capture agents stay active/passive; that is independent of the hub mode (lease-clear to change it).');
  }
  const migrate = !!db;
  const copy = !!sp;
  const steps = [
    { id: 'preflight', title: 'Check the target', detail: 'Target metadata database and spool are reachable and writable; versions match.' },
    { id: 'drain', title: 'Drain', detail: 'Pause every capture agent, wait until agent spools, the decode queue and all batches are loaded. Nothing is lost: agents keep their position.' },
    { id: 'copy_spool', title: 'Copy the spool', detail: copy ? `Copy every raw batch and snapshot chunk to ${redactUrl(sp)} and verify each sha256.` : 'Spool stays where it is.', skip: !copy },
    { id: 'migrate_metadata', title: 'Copy the metadata', detail: migrate ? `Copy every metadata table to ${redactUrl(db)} and compare row counts per table.` : 'Metadata database stays where it is.', skip: !migrate },
    { id: 'write_config', title: 'Generate the config', detail: `Write the ${to} env file and Helm values (no secrets) for the restart.` },
    { id: 'resume', title: 'Resume', detail: 'Queue RESUME for the paused agents in the target metadata; they continue from the hub durable position after the restart (exactly-once kept by the destination checkpoint).' },
  ].map(({ skip, ...st }) => ({ ...st, status: blockers.length ? 'blocked' : skip ? 'skip' : 'todo' }));
  return { from, to, steps, blockers, warnings, command: applyCommand(req) };
}
