/**
 * Client for the host-side lab helper (`lab helper`: small local HTTP service on 127.0.0.1, one-time token).
 * The browser talks to the helper directly; the helper URL and token are never sent to the hub.
 */
import type { LabCatalog } from './labTypes';
import { isLoopbackUrl } from './lab';

export type LabState = 'healthy' | 'running' | 'starting' | 'unhealthy' | 'stopped' | 'absent';
export interface LabStatus {
  profiles: { id: string; state: LabState; image_present?: boolean }[];
  helper: { version: string };
}
export interface LabOp {
  op_id: string;
  action: string;
  profiles: string[];
  state: 'running' | 'done' | 'failed';
  exit_code: number | null;
  log: string[];
}

export class LabHelperError extends Error {
  constructor(readonly status: number, message: string) {
    super(message);
    this.name = 'LabHelperError';
  }
}

type Fetch = (input: string, init?: RequestInit) => Promise<Response>;

export class LabHelperClient {
  readonly base: string;
  constructor(base: string, private readonly token: string, private readonly fetchFn: Fetch = (i, o) => fetch(i, o)) {
    if (!isLoopbackUrl(base)) throw new LabHelperError(0, 'lab helper must run on this machine (127.0.0.1)');
    this.base = base.replace(/\/+$/, '');
  }

  private async req<T>(method: string, path: string, body?: unknown): Promise<T> {
    const res = await this.fetchFn(`${this.base}/lab/v1${path}`, {
      method,
      headers: { Accept: 'application/json', 'X-Lab-Token': this.token, ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}) },
      body: body === undefined ? null : JSON.stringify(body),
      credentials: 'omit',
      mode: 'cors',
    });
    let parsed: unknown = null;
    try { parsed = await res.json(); } catch { /* empty or non-JSON body */ }
    if (!res.ok) {
      const msg = (parsed as { error?: string } | null)?.error ?? `HTTP ${res.status}`;
      throw new LabHelperError(res.status, msg);
    }
    return parsed as T;
  }

  catalog() { return this.req<LabCatalog>('GET', '/catalog'); }
  status() { return this.req<LabStatus>('GET', '/status'); }
  up(profiles: string[]) { return this.req<{ op_id: string }>('POST', '/up', { profiles }); }
  down(profiles?: string[], volumes = false) { return this.req<{ op_id: string }>('POST', '/down', { ...(profiles ? { profiles } : {}), volumes }); }
  pull(profiles: string[]) { return this.req<{ op_id: string }>('POST', '/pull', { profiles }); }
  reset(profiles: string[]) { return this.req<{ op_id: string }>('POST', '/reset', { profiles }); }
  op(id: string) { return this.req<LabOp>('GET', `/ops/${encodeURIComponent(id)}`); }
}

const SESSION_KEY = 'alelt.labHelper';
type SessionStore = Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>;

/** Helper link lives in sessionStorage only (gone when the tab closes). */
export function loadHelperLink(s: SessionStore): { url: string; token: string } | null {
  try {
    const v = JSON.parse(s.getItem(SESSION_KEY) ?? 'null') as { url?: unknown; token?: unknown } | null;
    return v && typeof v.url === 'string' && typeof v.token === 'string' && isLoopbackUrl(v.url) ? { url: v.url, token: v.token } : null;
  } catch {
    return null;
  }
}
export function saveHelperLink(s: SessionStore, link: { url: string; token: string } | null): void {
  try {
    if (link) s.setItem(SESSION_KEY, JSON.stringify(link));
    else s.removeItem(SESSION_KEY);
  } catch { /* storage unavailable */ }
}

/** Last `n` log lines of an operation. */
export const logTail = (op: LabOp | null, n = 40): string[] => (op ? op.log.slice(-n) : []);
