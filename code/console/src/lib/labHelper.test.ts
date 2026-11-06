// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';
import { LabHelperClient, LabHelperError, loadHelperLink, logTail, saveHelperLink, type LabOp } from './labHelper';

interface Call { url: string; init: RequestInit | undefined }

function fakeFetch(reply: (c: Call) => { status: number; body: unknown }) {
  const calls: Call[] = [];
  const fn = async (url: string, init?: RequestInit): Promise<Response> => {
    const c = { url, init };
    calls.push(c);
    const r = reply(c);
    return new Response(r.body === undefined ? null : JSON.stringify(r.body), { status: r.status, headers: { 'content-type': 'application/json' } });
  };
  return { fn, calls };
}

describe('LabHelperClient', () => {
  it('refuses a helper that is not on this machine', () => {
    expect(() => new LabHelperClient('http://192.168.1.10:47999', 't')).toThrow(LabHelperError);
  });
  it('sends the token header and JSON bodies to /lab/v1', async () => {
    const f = fakeFetch(() => ({ status: 200, body: { op_id: 'op1' } }));
    const c = new LabHelperClient('http://127.0.0.1:47999/', 'tok', f.fn);
    await c.up(['postgres', 'landing']);
    await c.reset(['postgres']);
    await c.down(undefined, true);
    await c.pull(['mysql']);
    const [up, reset, down, pull] = f.calls;
    expect(up?.url).toBe('http://127.0.0.1:47999/lab/v1/up');
    expect(up?.init?.method).toBe('POST');
    expect((up?.init?.headers as Record<string, string>)['X-Lab-Token']).toBe('tok');
    expect(JSON.parse(String(up?.init?.body))).toEqual({ profiles: ['postgres', 'landing'] });
    expect(reset?.url).toBe('http://127.0.0.1:47999/lab/v1/reset');
    expect(JSON.parse(String(down?.init?.body))).toEqual({ volumes: true });
    expect(pull?.url).toBe('http://127.0.0.1:47999/lab/v1/pull');
    expect(up?.init?.credentials).toBe('omit');
  });
  it('reads status and ops, encodes op ids', async () => {
    const f = fakeFetch((c) => c.url.endsWith('/status')
      ? { status: 200, body: { profiles: [{ id: 'postgres', state: 'healthy', image_present: true }], helper: { version: '1' } } }
      : { status: 200, body: { op_id: 'a/b', action: 'up', profiles: [], state: 'done', exit_code: 0, log: ['x'] } });
    const c = new LabHelperClient('http://localhost:47999', 'tok', f.fn);
    expect((await c.status()).profiles[0]?.image_present).toBe(true);
    await c.op('a/b');
    expect(f.calls[1]?.url).toBe('http://localhost:47999/lab/v1/ops/a%2Fb');
    expect(f.calls[1]?.init?.body).toBeNull();
  });
  it('surfaces helper errors (bad token)', async () => {
    const f = fakeFetch(() => ({ status: 401, body: { error: 'bad token' } }));
    const c = new LabHelperClient('http://127.0.0.1:47999', 'wrong', f.fn);
    await expect(c.status()).rejects.toMatchObject({ status: 401, message: 'bad token' });
  });
});

describe('helper link storage + log tail', () => {
  it('stores the link in session storage only when loopback', () => {
    const m = new Map<string, string>();
    const s = { getItem: (k: string) => m.get(k) ?? null, setItem: (k: string, v: string) => void m.set(k, v), removeItem: (k: string) => void m.delete(k) };
    saveHelperLink(s, { url: 'http://127.0.0.1:47999', token: 't' });
    expect(loadHelperLink(s)).toEqual({ url: 'http://127.0.0.1:47999', token: 't' });
    saveHelperLink(s, null);
    expect(loadHelperLink(s)).toBeNull();
    m.set('alelt.labHelper', JSON.stringify({ url: 'http://evil.example', token: 't' }));
    expect(loadHelperLink(s)).toBeNull();
    m.set('alelt.labHelper', '{not json');
    expect(loadHelperLink(s)).toBeNull();
  });
  it('tails the op log', () => {
    const op: LabOp = { op_id: '1', action: 'up', profiles: [], state: 'running', exit_code: null, log: Array.from({ length: 50 }, (_, i) => `l${i}`) };
    expect(logTail(op, 3)).toEqual(['l47', 'l48', 'l49']);
    expect(logTail(null)).toEqual([]);
  });
});
