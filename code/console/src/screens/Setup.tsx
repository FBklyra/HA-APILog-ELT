import { useCallback, useEffect, useState } from 'react';
import { Link } from 'react-router';
import {
  call, download, saveJson,
  type DestinationTest, type Enrollment, type EnrollmentRow, type MetadataTest, type Prerequisite, type SetupStatus,
} from '../api/product';
import { useAuth } from '../auth/AuthProvider';
import { Badge, Card, Loading, Page, Table, errorText } from '../components/ui';
import { useI18n } from '../i18n';

type MetaMode = 'sqlite' | 'bundled_postgres' | 'external_postgres';

/** First-run setup: metadata database, administrator, destination, agent enrollment; then links to the pipeline flows. */
export function Setup() {
  const { authHeaders, dev } = useAuth();
  const { t } = useI18n();
  const [st, setSt] = useState<SetupStatus | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const reload = useCallback(() => {
    call<SetupStatus>(authHeaders, 'GET', '/setup/status').then(setSt, (e: unknown) => setErr(errorText(e)));
  }, [authHeaders]);
  useEffect(reload, [reload]);
  if (!st) return <Loading error={err} />;
  const finish = () => call(authHeaders, 'POST', '/setup/complete', {}).then(reload, (e: unknown) => setErr(errorText(e)));
  return (
    <Page title={t('setup.title')} actions={st.completed ? <Badge value={t('setup.done')} /> : <button className="primary" onClick={finish}>{t('setup.finish')}</button>}>
      <p>{t('setup.intro')}</p>
      {err && <p role="alert" className="error">{err}</p>}
      <MetadataStep st={st} onDone={reload} />
      <AdminStep st={st} dev={dev} onDone={reload} />
      <DestinationStep st={st} onDone={reload} />
      <AgentStep onDone={reload} />
      <Card title={t('setup.step.next')}>
        <p className="actions">
          <Link className="button" to={st.links.import} data-testid="setup-import">{t('setup.next.import')}</Link>{' '}
          <Link className="button primary" to={st.links.wizard} data-testid="setup-wizard">{t('setup.next.wizard')}</Link>{' '}
          <Link className="button" to="/topology" data-testid="setup-ha">{t('setup.next.ha')}</Link>
        </p>
        <p className="hint">{t('setup.next.haHint')}</p>
      </Card>
    </Page>
  );
}

function Result({ ok, text }: { ok: boolean; text: string }) {
  const { t } = useI18n();
  return <p role="status"><Badge value={ok ? t('setup.ok') : t('setup.failed')} /> {text}</p>;
}

function MetadataStep({ st, onDone }: { st: SetupStatus; onDone: () => void }) {
  const { authHeaders } = useAuth();
  const { t } = useI18n();
  const [mode, setMode] = useState<MetaMode>(st.metadata.mode);
  const [url, setUrl] = useState('');
  const [res, setRes] = useState<MetadataTest | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const test = () => {
    setErr(null);
    call<MetadataTest>(authHeaders, 'POST', '/setup/metadata/test', { mode, url: url || null }).then(setRes, (e: unknown) => setErr(errorText(e)));
  };
  const use = () => call(authHeaders, 'POST', '/setup/metadata/ack').then(onDone, (e: unknown) => setErr(errorText(e)));
  const options: [MetaMode, string][] = [
    ['sqlite', t('setup.meta.sqlite')], ['bundled_postgres', t('setup.meta.bundled')], ['external_postgres', t('setup.meta.external')],
  ];
  return (
    <Card title={`${t('setup.step.metadata')} ${st.steps.metadata.done ? '✓' : ''}`}>
      <p>{t('setup.meta.current', { engine: st.metadata.engine, mode: st.metadata.mode })} · <code>{st.metadata.url}</code></p>
      <fieldset>
        {options.map(([v, label]) => (
          <label key={v} className="radio">
            <input type="radio" name="meta-mode" value={v} checked={mode === v} onChange={() => { setMode(v); setRes(null); }} /> {label}
          </label>
        ))}
      </fieldset>
      {mode === 'external_postgres' && (
        <label>{t('setup.meta.url')} <input value={url} onChange={(e) => setUrl(e.target.value)} placeholder="postgresql+psycopg://user:password@host:5432/alelt" size={60} /></label>
      )}
      {mode !== st.metadata.mode && <p className="hint">{st.metadata.has_data ? t('setup.meta.migration') : t('setup.meta.apply')}</p>}
      <p className="actions">
        <button onClick={test}>{t('setup.test')}</button>{' '}
        {mode === st.metadata.mode && <button className="primary" onClick={use}>{t('setup.confirm')}</button>}
      </p>
      {res && <Result ok={res.ok} text={`${res.detail} (${res.elapsed_ms} ms)`} />}
      {res?.warning && <p role="alert" className="warn">{res.warning}</p>}
      {err && <p role="alert" className="error">{err}</p>}
    </Card>
  );
}

function AdminStep({ st, dev, onDone }: { st: SetupStatus; dev: boolean; onDone: () => void }) {
  const { authHeaders } = useAuth();
  const { t } = useI18n();
  const [email, setEmail] = useState(String(st.steps.admin.email ?? ''));
  const [group, setGroup] = useState('');
  const [err, setErr] = useState<string | null>(null);
  const create = () => call(authHeaders, 'POST', '/setup/admin', { email, group_id: group || null, reason: 'first-run setup' })
    .then(onDone, (e: unknown) => setErr(errorText(e)));
  return (
    <Card title={`${t('setup.step.admin')} ${st.steps.admin.done ? '✓' : ''}`}>
      {dev && <p className="hint">{t('setup.admin.devNote')}</p>}
      <label>{t('setup.admin.email')} <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} placeholder="admin@example.com" /></label>
      {!dev && <label>{t('setup.admin.group')} <input value={group} onChange={(e) => setGroup(e.target.value)} /></label>}
      <p className="actions"><button className="primary" disabled={email.length < 3} onClick={create}>{t('setup.admin.create')}</button></p>
      {err && <p role="alert" className="error">{err}</p>}
    </Card>
  );
}

function DestinationStep({ st, onDone }: { st: SetupStatus; onDone: () => void }) {
  const { authHeaders } = useAuth();
  const { t } = useI18n();
  const [engine, setEngine] = useState<'postgres' | 'snowflake'>(st.destination.engine === 'snowflake' ? 'snowflake' : 'postgres');
  const [url, setUrl] = useState('');
  const [sf, setSf] = useState({ account: '', user: '', role: 'ALELT_LOADER', warehouse: 'ALELT_WH', database: '' });
  const [boot, setBoot] = useState(true);
  const [res, setRes] = useState<DestinationTest | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const test = () => {
    setErr(null);
    const body = engine === 'postgres' ? { engine, url: url || null, bootstrap: boot } : { engine, ...sf, account: sf.account || null, bootstrap: boot };
    call<DestinationTest>(authHeaders, 'POST', '/setup/destination/test', body).then((r) => { setRes(r); if (r.ok) onDone(); }, (e: unknown) => setErr(errorText(e)));
  };
  return (
    <Card title={`${t('setup.step.destination')} ${st.steps.destination.done ? '✓' : ''}`}>
      <p><code>{st.destination.engine}</code> · <code>{st.destination.url}</code></p>
      <label>{t('setup.dest.engine')}{' '}
        <select value={engine} onChange={(e) => setEngine(e.target.value as 'postgres' | 'snowflake')}>
          <option value="postgres">PostgreSQL</option>
          <option value="snowflake">Snowflake</option>
        </select>
      </label>
      {engine === 'postgres' ? (
        <label>{t('setup.dest.pgUrl')} <input value={url} onChange={(e) => setUrl(e.target.value)} size={60} placeholder="postgresql://user:password@host:5432/alelt_landing" /></label>
      ) : (
        <div className="grid">
          <p className="hint">{t('setup.dest.sfNote')}</p>
          {(['account', 'user', 'role', 'warehouse', 'database'] as const).map((k) => (
            <label key={k}>{k} <input value={sf[k]} onChange={(e) => setSf({ ...sf, [k]: e.target.value })} /></label>
          ))}
        </div>
      )}
      <label className="check"><input type="checkbox" checked={boot} onChange={(e) => setBoot(e.target.checked)} /> {t('setup.dest.bootstrap')}</label>
      <p className="hint">{t('setup.dest.secretNote')}</p>
      <p className="actions"><button onClick={test}>{t('setup.test')}</button></p>
      {res && res.steps.map((s) => <Result key={s.step} ok={s.ok} text={`${s.step}: ${s.detail}`} />)}
      {err && <p role="alert" className="error">{err}</p>}
    </Card>
  );
}

function AgentStep({ onDone }: { onDone: () => void }) {
  const { authHeaders } = useAuth();
  const { t, locale } = useI18n();
  const [f, setF] = useState({ connection_id: 'erptest', database: 'ERPTEST', host: '', port: '50000', user: '', schemas: 'APPSCHEMA' });
  const [pre, setPre] = useState<Prerequisite[]>([]);
  const [enr, setEnr] = useState<Enrollment | null>(null);
  const [rows, setRows] = useState<EnrollmentRow[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const list = useCallback(() => {
    call<EnrollmentRow[]>(authHeaders, 'GET', '/setup/agents/enrollments').then(setRows, () => setRows([]));
  }, [authHeaders]);
  useEffect(() => {
    call<{ items: Prerequisite[] }>(authHeaders, 'GET', '/setup/prerequisites?engine=db2').then((r) => setPre(r.items), () => setPre([]));
    list();
  }, [authHeaders, list]);
  const generate = () => {
    setErr(null);
    call<Enrollment>(authHeaders, 'POST', '/setup/agents/enroll', {
      connection_id: f.connection_id, engine: 'db2', database: f.database, host: f.host || null,
      port: f.port ? Number(f.port) : null, user: f.user || null,
      schemas: f.schemas.split(',').map((s) => s.trim()).filter(Boolean), reason: 'agent enrollment',
    }).then((r) => { setEnr(r); list(); onDone(); }, (e: unknown) => setErr(errorText(e)));
  };
  const fields: [keyof typeof f, string][] = [
    ['connection_id', t('setup.agent.connection')], ['database', t('setup.agent.database')], ['host', t('setup.agent.host')],
    ['port', t('setup.agent.port')], ['user', t('setup.agent.user')], ['schemas', t('setup.agent.schemas')],
  ];
  return (
    <Card title={t('setup.step.agent')}>
      <div className="grid">
        {fields.map(([k, label]) => (
          <label key={k}>{label} <input value={f[k]} onChange={(e) => setF({ ...f, [k]: e.target.value })} /></label>
        ))}
      </div>
      <h4>{t('setup.agent.prereq')}</h4>
      <ul className="checklist">
        {pre.map((p) => <li key={p.id}><label><input type="checkbox" /> {p.label}</label> <code>{p.check}</code></li>)}
      </ul>
      <p className="actions"><button className="primary" onClick={generate}>{t('setup.agent.generate')}</button></p>
      {err && <p role="alert" className="error">{err}</p>}
      {enr && (
        <div className="enrollment" data-testid="enrollment">
          <label>{t('setup.agent.token')} <input readOnly value={enr.token} size={60} onFocus={(e) => e.target.select()} /></label>
          <p className="hint">{t('setup.agent.expires', { when: new Date(enr.expires_at).toLocaleString(locale) })}</p>
          <p>{t('setup.agent.howto', { connection: enr.connection_id })}</p>
          <p className="actions">
            <button onClick={() => saveJson(enr.devedge_json, 'devedge.json')}>{t('setup.agent.download', { file: 'devedge.json' })}</button>{' '}
            <button onClick={() => saveJson(enr.aleltagent_json, 'aleltagent.json')}>{t('setup.agent.download', { file: 'aleltagent.json' })}</button>
          </p>
        </div>
      )}
      {rows.length > 0 && (
        <>
          <h4>{t('setup.agent.list')}</h4>
          <Table head={['id', t('setup.agent.connection'), 'agent', 'state', 'expires']}>
            {rows.map((r) => (
              <tr key={r.id}><td><code>{r.id}</code></td><td>{r.connection_id}</td><td>{r.claimed_by ?? r.agent_id}</td>
                <td><Badge value={r.state} /></td><td>{new Date(r.expires_at).toLocaleString(locale)}</td></tr>
            ))}
          </Table>
        </>
      )}
    </Card>
  );
}

/** Support bundle download (PlatformAdmin). */
export function SupportBundleButton() {
  const { authHeaders } = useAuth();
  const { t } = useI18n();
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const go = () => {
    setBusy(true);
    setErr(null);
    download(authHeaders, '/support/bundle', 'alelt-support.zip').catch((e: unknown) => setErr(errorText(e))).finally(() => setBusy(false));
  };
  return (
    <span title={t('support.note')}>
      <button onClick={go} disabled={busy} data-testid="support-bundle">{t('support.bundle')}</button>
      {err && <span role="alert" className="error"> {err}</span>}
    </span>
  );
}
