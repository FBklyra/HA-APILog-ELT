// SPDX-License-Identifier: Apache-2.0
import { useCallback, useEffect, useMemo, useState } from 'react';
import { HttpError } from '../api/client';
import type { DestTestRequest, PipelineCreated } from '../api/contract';
import { DEV_AUTH, USE_MOCK } from '../auth/config';
import { useAuth } from '../auth/AuthProvider';
import { Badge, Card, Page, Table, errorText } from '../components/ui';
import { useI18n, type MessageKey } from '../i18n';
import {
  HUB_HOST, amd64Only, connectionLines, demoPipelineBody, guidanceSettings, isLoopbackUrl, labCommands, labEnvFile,
  orderedSelection, parseHelperFragment, sources, targets,
} from '../lib/lab';
import { LAB_CATALOG } from '../lib/labCatalog.generated';
import { LabHelperClient, loadHelperLink, logTail, saveHelperLink, type LabOp, type LabStatus } from '../lib/labHelper';
import type { LabProfile } from '../lib/labTypes';

/** Lab screen is offered only in dev / lab mode (local evaluation install). */
export const LAB_MODE: boolean = DEV_AUTH || USE_MOCK;

const STATE_BADGE: Record<string, string> = {
  healthy: 'OK', running: 'RUNNING', starting: 'PENDING', unhealthy: 'FAILED', stopped: 'DOWN', absent: 'DOWN',
};
const MATURITY_BADGE: Record<string, string> = { stable: 'OK', beta: 'RUNNING', planned: 'PLANNED' };

function copy(text: string) {
  void navigator.clipboard?.writeText(text).catch(() => undefined);
}

function download(text: string, name: string) {
  const url = URL.createObjectURL(new Blob([text], { type: 'text/plain' }));
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function CodeLines({ lines }: { lines: string[] }) {
  const { t } = useI18n();
  return (
    <div className="sql">
      <pre><code>{lines.join('\n')}</code></pre>
      <button onClick={() => copy(lines.join('\n'))}>{t('lab.copy')}</button>
    </div>
  );
}

/** Cloud-only entry: what it is, sign-up + docs links, objects to create, form values, Test connection. */
function Guidance({ p }: { p: LabProfile }) {
  const { t } = useI18n();
  const { api } = useAuth();
  const g = p.guidance;
  const [values, setValues] = useState<Record<string, string>>(
    () => Object.fromEntries(Object.entries(g?.form_fields ?? {}).map(([k, v]) => [k, String(v)])),
  );
  const [result, setResult] = useState<string | null>(null);
  if (!g) return null;
  const test = async () => {
    setResult('…');
    try {
      const r = await api.call('POST /wizard/destination/test', { body: {
        engine: g.test_engine as DestTestRequest['engine'], environment: 'TEST', settings: guidanceSettings(values),
        bootstrap: false, reason: t('lab.g.reason'),
      } });
      setResult(r.ok ? t('lab.g.testOk', { ms: r.elapsed_ms ?? 0 })
        : t('lab.g.testFail', { error: r.steps.filter((s) => !s.ok).map((s) => `${s.step}: ${s.detail}`).join('; ') || '?' }));
    } catch (e) {
      setResult(e instanceof HttpError && (e.status === 422 || e.status === 404) ? t('lab.g.unsupported') : t('lab.g.testFail', { error: errorText(e) }));
    }
  };
  return (
    <div data-testid={`lab-guidance-${p.id}`}>
      <p>{g.what}</p>
      <p>
        {t('lab.g.signup')}: <a href={g.signup_url} target="_blank" rel="noreferrer noopener">{g.signup_url}</a><br />
        {t('lab.g.docs')}: <a href={g.docs_url} target="_blank" rel="noreferrer noopener">{g.docs_url}</a>
      </p>
      <h3>{t('lab.g.objects')}</h3>
      <ul>{g.objects.map((o) => <li key={o}>{o}</li>)}</ul>
      <h3>{t('lab.g.form')}</h3>
      <p className="hint">{t('lab.g.secretNote')}</p>
      <div className="form-grid">
        {Object.keys(g.form_fields).map((k) => (
          <label key={k} className="field">{k}
            <input value={values[k] ?? ''} onChange={(e) => setValues((v) => ({ ...v, [k]: e.target.value }))} />
          </label>
        ))}
      </div>
      <button data-testid={`lab-test-${p.id}`} onClick={() => void test()}>{t('lab.g.test')}</button>
      {result && <span> {result}</span>}
    </div>
  );
}

/** PlatformAdmin, dev/lab mode only: local test catalog; commands (default, zero risk) or run via the local helper. */
export function Lab() {
  const { t } = useI18n();
  const { api } = useAuth();
  const [sel, setSel] = useState<Set<string>>(() => new Set(['postgres', 'landing']));
  const [link, setLink] = useState<{ url: string; token: string } | null>(() => {
    if (typeof window === 'undefined') return null;
    const fromHash = parseHelperFragment(window.location.hash);
    if (fromHash) {
      saveHelperLink(window.sessionStorage, fromHash);
      window.history.replaceState(null, '', window.location.pathname + window.location.search); // drop token from URL
      return fromHash;
    }
    return loadHelperLink(window.sessionStorage);
  });
  const [urlDraft, setUrlDraft] = useState(`http://127.0.0.1:${LAB_CATALOG.helper.port}`);
  const [tokenDraft, setTokenDraft] = useState('');
  const [status, setStatus] = useState<LabStatus | null>(null);
  const [op, setOp] = useState<LabOp | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [demo, setDemo] = useState<Record<string, string>>({});

  const client = useMemo(() => {
    if (!link) return null;
    try { return new LabHelperClient(link.url, link.token); } catch { return null; }
  }, [link]);
  const ids = orderedSelection(sel);
  const cmds = labCommands(ids);
  const chosen = LAB_CATALOG.profiles.filter((p) => sel.has(p.id) && p.availability === 'image');

  const refresh = useCallback(async () => {
    if (!client) return;
    try { setStatus(await client.status()); setErr(null); } catch (e) { setErr(errorText(e)); setStatus(null); }
  }, [client]);
  useEffect(() => { void refresh(); }, [refresh]);
  useEffect(() => {
    if (!client || !op || op.state !== 'running') return undefined;
    const h = setInterval(() => {
      client.op(op.op_id).then((o) => { setOp(o); if (o.state !== 'running') void refresh(); }, (e: unknown) => setErr(errorText(e)));
    }, 2000);
    return () => clearInterval(h);
  }, [client, op, refresh]);

  if (!LAB_MODE) return <Page title={t('nav.lab')}><p role="alert" className="hint" data-testid="lab-only">{t('lab.labOnly')}</p></Page>;

  const toggle = (id: string) => setSel((s) => { const n = new Set(s); if (n.has(id)) n.delete(id); else n.add(id); return n; });
  const run = async (f: (c: LabHelperClient) => Promise<{ op_id: string }>) => {
    if (!client) return;
    setErr(null);
    try { const { op_id } = await f(client); setOp(await client.op(op_id)); } catch (e) { setErr(errorText(e)); }
  };
  const st = (id: string) => status?.profiles.find((p) => p.id === id) ?? null;
  const isUp = (id: string) => ['healthy', 'running'].includes(st(id)?.state ?? '');
  const landing = LAB_CATALOG.profiles.find((p) => p.id === 'landing');
  const createDemo = async (p: LabProfile) => {
    if (!landing) return;
    setDemo((d) => ({ ...d, [p.id]: '…' }));
    try {
      const r: PipelineCreated = await api.call('POST /wizard/pipelines', { body: demoPipelineBody(p, landing, t('lab.demoReason')) });
      setDemo((d) => ({ ...d, [p.id]: t('lab.demoDone', { id: r.connection_id, revision: r.revision, tables: r.tables.length }) }));
    } catch (e) {
      setDemo((d) => ({ ...d, [p.id]: t('common.error', { message: errorText(e) }) }));
    }
  };
  const connect = () => {
    if (!isLoopbackUrl(urlDraft)) { setErr(t('lab.helper.badUrl')); return; }
    const l = { url: urlDraft.replace(/\/+$/, ''), token: tokenDraft.trim() };
    saveHelperLink(window.sessionStorage, l);
    setLink(l);
    setTokenDraft('');
  };
  const disconnect = () => { saveHelperLink(window.sessionStorage, null); setLink(null); setStatus(null); setOp(null); };

  const entry = (p: LabProfile) => {
    const s = st(p.id);
    const hasImages = (p.images?.length ?? 0) > 0;
    return (
      <li key={p.id} className="card" data-testid={`lab-entry-${p.id}`}>
        <div className="row">
          {p.availability === 'image' && (
            <input type="checkbox" aria-label={p.label} data-testid={`lab-pick-${p.id}`} checked={sel.has(p.id)} onChange={() => toggle(p.id)} />
          )}
          <strong>{p.label}</strong> <code>{p.id}</code>
          <span>{t('lab.maturity')}: <Badge value={MATURITY_BADGE[p.status] ?? 'PLANNED'} /> {t(`lab.status.${p.status}` as MessageKey)}</span>
          {p.availability === 'image'
            ? <span>{t('lab.arch')}: <code>{(p.arch ?? []).join(', ') || '—'}</code></span>
            : <span><Badge value="UNKNOWN" /> {t('lab.cloudOnly')}</span>}
          {status && p.availability === 'image' && (
            <span><Badge value={STATE_BADGE[s?.state ?? 'absent'] ?? 'DOWN'} /> {s?.state ?? t('lab.notStarted')}
              {hasImages && <> · {s?.image_present ? t('lab.image.present') : t('lab.image.missing')}</>}</span>
          )}
        </div>
        {amd64Only(p) && <p className="hint">{t('lab.emulated')}</p>}
        {p.privileged && <p className="hint">{t('lab.privileged')}</p>}
        {p.notes && <p className="hint">{p.notes}</p>}
        {p.availability === 'image' && client && (
          <div className="row">
            {hasImages && <button onClick={() => void run((c) => c.pull([p.id]))}>{t('lab.pull')}</button>}
            <button className="primary" onClick={() => void run((c) => c.up([p.id]))}>{t('lab.start')}</button>
            <button onClick={() => void run((c) => c.down([p.id]))}>{t('lab.stop')}</button>
            <button className="danger" onClick={() => void run((c) => c.reset([p.id]))}>{t('lab.reset')}</button>
            {p.kind === 'source' && p.demo && (
              <button disabled={!isUp(p.id) || !isUp('landing')} title={isUp('landing') ? '' : t('lab.demoNeedsLanding')}
                data-testid={`lab-demo-${p.id}`} onClick={() => void createDemo(p)}>{t('lab.demo')}</button>
            )}
            {demo[p.id] && <span>{demo[p.id]}</span>}
          </div>
        )}
        {p.availability === 'no_image' && <Guidance p={p} />}
      </li>
    );
  };

  return (
    <Page title={t('nav.lab')}>
      <p className="hint">{t('lab.intro')}</p>
      {err && <p role="alert" className="error">{err}</p>}

      <Card title={t('lab.helper.title')}>
        <p className="hint">{t('lab.helper.local')}</p>
        {!client ? (
          <>
            <p>{t('lab.helper.off')}</p>
            <div className="row">
              <label>{t('lab.helper.url')} <input value={urlDraft} onChange={(e) => setUrlDraft(e.target.value)} /></label>
              <label>{t('lab.helper.token')} <input type="password" autoComplete="off" value={tokenDraft} onChange={(e) => setTokenDraft(e.target.value)} /></label>
              <button onClick={connect} disabled={!tokenDraft.trim()}>{t('lab.helper.connect')}</button>
            </div>
          </>
        ) : (
          <>
            <p data-testid="lab-helper-connected">{t('lab.helper.connected', { version: status?.helper.version ?? '?', url: client.base })}{' '}
              <button className="link" onClick={disconnect}>{t('lab.helper.disconnect')}</button></p>
            <div className="row">
              <button onClick={() => void run((c) => c.pull(ids))} disabled={!ids.length}>{t('lab.pullSel')}</button>
              <button className="primary" onClick={() => void run((c) => c.up(ids))} disabled={!ids.length}>{t('lab.up')}</button>
              <button onClick={() => void run((c) => c.down(ids))} disabled={!ids.length}>{t('lab.down')}</button>
              <button className="danger" onClick={() => void run((c) => c.down(undefined, true))}>{t('lab.downAll')}</button>
              <button onClick={() => void refresh()}>{t('common.refresh')}</button>
            </div>
            <p className="hint">{t('lab.demoNote', { host: HUB_HOST })}</p>
            {op && (
              <>
                <p>{t('lab.op', { action: op.action, state: op.state })} <Badge value={op.state === 'done' ? 'DONE' : op.state === 'failed' ? 'FAILED' : 'RUNNING'} /></p>
                <div className="sql"><pre data-testid="lab-log"><code>{logTail(op).join('\n')}</code></pre></div>
              </>
            )}
          </>
        )}
      </Card>

      <Card title={t('lab.sources')}><ul className="checklist">{sources().map(entry)}</ul></Card>
      <Card title={t('lab.targets')}><ul className="checklist">{targets().map(entry)}</ul></Card>

      <Card title={t('lab.safe.title')}>
        <p>{t('lab.safe.body')}</p>
        {ids.length === 0 ? <p>{t('lab.none')}</p> : (
          <>
            <h3>{t('lab.bash')}</h3><CodeLines lines={cmds.sh} />
            <h3>{t('lab.pwsh')}</h3><CodeLines lines={cmds.ps1} />
            <h3>{t('lab.compose')}</h3><CodeLines lines={[cmds.compose]} />
            <button className="primary" data-testid="lab-download" onClick={() => download(labEnvFile(ids), 'lab.env')}>{t('lab.download')}</button>
          </>
        )}
      </Card>

      <Card title={t('lab.conn.title')}>
        <p className="hint">{t('lab.conn.note', { host: HUB_HOST })}</p>
        <Table head={[t('lab.conn.profile'), t('lab.conn.dsn'), t('lab.conn.fields'), '']}>
          {chosen.map((p) => (
            <tr key={p.id}>
              <td>{p.label}</td><td><code>{p.dsn ?? '—'}</code></td>
              <td><code>{connectionLines(p).join(' · ')}</code></td>
              <td>{p.dsn && <button onClick={() => copy(p.dsn ?? '')}>{t('lab.copy')}</button>}</td>
            </tr>
          ))}
        </Table>
      </Card>
    </Page>
  );
}
