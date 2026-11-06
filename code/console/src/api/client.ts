import type { ApiError, EndpointKey, Endpoints } from './contract';
import { API_BASE } from './contract';

export type ReqOf<K extends EndpointKey> = Endpoints[K] extends { req: infer R } ? R : undefined;
export type ResOf<K extends EndpointKey> = Endpoints[K]['res'];

export interface TransportRequest {
  method: string;
  path: string; // path below API_BASE, with query string
  headers: Record<string, string>;
  body: unknown;
}
export interface TransportResponse {
  status: number;
  body: unknown;
}
export type Transport = (req: TransportRequest) => Promise<TransportResponse>;

export class HttpError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly correlationId: string | null,
  ) {
    super(message);
    this.name = 'HttpError';
  }
}

/** Fetch transport against the hub. Credentials stay in headers; no cookies (CSRF-free). */
export function fetchTransport(base: string = API_BASE): Transport {
  return async ({ method, path, headers, body }) => {
    const res = await fetch(base + path, {
      method,
      headers: { Accept: 'application/json', ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}), ...headers },
      body: body === undefined ? null : JSON.stringify(body),
      credentials: 'omit',
    });
    const ct = res.headers.get('content-type') ?? '';
    const parsed: unknown = ct.includes('json') ? await res.json() : await res.text();
    return { status: res.status, body: parsed };
  };
}

export function buildPath(key: EndpointKey, id?: string, query?: Record<string, string | number | undefined>): { method: string; path: string } {
  const space = key.indexOf(' ');
  const method = key.slice(0, space);
  let path = key.slice(space + 1);
  if (path.includes('{id}')) {
    if (id === undefined || id === '') throw new Error(`${key} requires an id`);
    path = path.replace('{id}', encodeURIComponent(id));
  }
  const qs = Object.entries(query ?? {})
    .filter((e): e is [string, string | number] => e[1] !== undefined && e[1] !== '')
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
    .join('&');
  return { method, path: qs ? `${path}?${qs}` : path };
}

let seq = 0;
function correlationId(): string {
  seq += 1;
  const rnd = typeof crypto !== 'undefined' && 'randomUUID' in crypto ? crypto.randomUUID() : `${Date.now()}-${seq}`;
  return `console-${rnd}`;
}

export type AuthHeaders = () => Promise<Record<string, string>>;

export class ApiClient {
  constructor(
    private readonly transport: Transport,
    private readonly authHeaders: AuthHeaders,
  ) {}

  async call<K extends EndpointKey>(
    key: K,
    opts: { id?: string; query?: Record<string, string | number | undefined>; body?: ReqOf<K> } = {},
  ): Promise<ResOf<K>> {
    const { method, path } = buildPath(key, opts.id, opts.query);
    const headers = { ...(await this.authHeaders()), 'X-ALELT-Correlation-Id': correlationId() };
    const res = await this.transport({ method, path, headers, body: opts.body });
    if (res.status >= 400) {
      const err = (res.body as Partial<ApiError> | null)?.error;
      throw new HttpError(res.status, err?.code ?? `HTTP_${res.status}`, err?.message ?? `HTTP ${res.status}`, err?.correlation_id ?? null);
    }
    return res.body as ResOf<K>;
  }
}
