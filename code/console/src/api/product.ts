/** Product endpoints outside the typed console contract: first-run setup, agent enrollment, version, support bundle. */
import { API_BASE_URL, USE_MOCK } from '../auth/config';

export type Headers = () => Promise<Record<string, string>>;

export interface SetupStatus {
  completed: boolean;
  auth_mode: string;
  env: string;
  steps: Record<'metadata' | 'admin' | 'destination' | 'agent', { done: boolean; [k: string]: unknown }>;
  metadata: MetadataInfo;
  destination: { engine: string; plugin: string; url: string };
  connections: string[];
  enrollments: number;
  links: { import: string; wizard: string };
}
export interface MetadataInfo {
  engine: string;
  url: string;
  mode: 'sqlite' | 'bundled_postgres' | 'external_postgres';
  schema_ok?: boolean;
  has_data?: boolean;
  error?: string;
}
export interface MetadataTest { ok: boolean; detail: string; elapsed_ms: number; switching: boolean; warning: string }
export interface DestinationTest { ok: boolean; steps: { step: string; ok: boolean; detail: string }[]; elapsed_ms: number }
export interface Prerequisite { id: string; label: string; check: string }
export interface Enrollment {
  token: string;
  agent_id: string;
  connection_id: string;
  expires_at: string;
  hub_url: string;
  devedge_json: unknown;
  aleltagent_json: unknown;
  prerequisites: Prerequisite[];
}
export interface EnrollmentRow {
  id: string;
  connection_id: string;
  agent_id: string;
  state: string;
  created_at: string;
  expires_at: string;
  claimed_by: string | null;
}

export class ProductError extends Error {}

export async function call<T>(headers: Headers, method: string, path: string, body?: unknown): Promise<T> {
  if (USE_MOCK) throw new ProductError('not available in mock mode');
  const res = await fetch(`${API_BASE_URL}${path}`, {
    method,
    headers: { Accept: 'application/json', ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}), ...(await headers()) },
    body: body === undefined ? null : JSON.stringify(body),
    credentials: 'omit',
  });
  const parsed: unknown = (res.headers.get('content-type') ?? '').includes('json') ? await res.json() : await res.text();
  if (!res.ok) {
    const err = (parsed as { error?: { message?: string } } | null)?.error;
    throw new ProductError(err?.message ?? `HTTP ${res.status}`);
  }
  return parsed as T;
}

/** Download a file served by the hub with the current auth headers. */
export async function download(headers: Headers, path: string, fallbackName: string): Promise<void> {
  const res = await fetch(`${API_BASE_URL}${path}`, { headers: await headers(), credentials: 'omit' });
  if (!res.ok) throw new ProductError(`HTTP ${res.status}`);
  const name = /filename="([^"]+)"/.exec(res.headers.get('content-disposition') ?? '')?.[1] ?? fallbackName;
  saveBlob(await res.blob(), name);
}

export function saveJson(data: unknown, name: string): void {
  saveBlob(new Blob([JSON.stringify(data, null, 2) + '\n'], { type: 'application/json' }), name);
}

function saveBlob(blob: Blob, name: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
