import type { HostMapEntry } from '../api/contract';

/** Same rule as the hub (alelt_import.hostmap.normalize): a target is a host name or address, no URL parts. */
export function validTestHost(v: string): boolean {
  const s = v.trim();
  return s !== '' && !/[\s/@?#]/.test(s);
}

export type RowState = 'missing' | 'invalid' | 'checking' | 'denied' | 'ok';

export interface RowView { host: string; target: string; state: RowState; deniedBy: string | null }

/**
 * State of each mapping row from the draft targets and the live deny-list results (`checks`: target -> denied_by,
 * absent = not checked yet). Apply is allowed only when every row is `ok`.
 */
export function hostMapRows(entries: HostMapEntry[], drafts: Record<string, string>, checks: Record<string, string | null>): RowView[] {
  return entries.map((e) => {
    const target = (drafts[e.host] ?? '').trim();
    if (!target) return { host: e.host, target, state: 'missing', deniedBy: null };
    if (!validTestHost(target)) return { host: e.host, target, state: 'invalid', deniedBy: null };
    if (!(target in checks)) return { host: e.host, target, state: 'checking', deniedBy: null };
    const d = checks[target] ?? null;
    return { host: e.host, target, state: d ? 'denied' : 'ok', deniedBy: d };
  });
}

export function hostMapComplete(rows: RowView[]): boolean {
  return rows.every((r) => r.state === 'ok');
}

/** Distinct valid targets that still need a deny-list check. */
export function targetsToCheck(entries: HostMapEntry[], drafts: Record<string, string>, checks: Record<string, string | null>): string[] {
  const out = new Set<string>();
  for (const e of entries) {
    const t = (drafts[e.host] ?? '').trim();
    if (validTestHost(t) && !(t in checks)) out.add(t);
  }
  return [...out].slice(0, 50);
}

/** host -> trimmed target for the PUT / inline apply body. */
export function mappingOf(rows: RowView[]): Record<string, string> {
  return Object.fromEntries(rows.filter((r) => r.target).map((r) => [r.host, r.target]));
}
