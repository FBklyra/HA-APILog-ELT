import { useEffect, useState } from 'react';
import { Link, useSearchParams } from 'react-router';
import type {
  DestTestResult, Environment, ImportSession, PipelineCreated, Progress, SourceTestResult, WizardDefaults, WizardSource,
} from '../api/contract';
import { saveJson } from '../api/product';
import { useAuth } from '../auth/AuthProvider';
import { Badge, Card, Loading, Page, Table, errorText, useLoad } from '../components/ui';
import { Checklist, CopyButton, DropZone, EnvBadge, Field, NavButtons, Stepper, Steps, type DroppedFile } from '../components/wizard';
import { useI18n, type MessageKey } from '../i18n';

const SECRET_KEYS = /password|secret|key|token/i;

/** One-flow parallel test: files (optional) -> TEST source -> TEST destination -> review -> create + live progress.
 *  Every field is pre-filled; clicking Next all the way creates a working pipeline. */
export function ParallelTestWizard() {
  const { api } = useAuth();
  const { t } = useI18n();
  const [params] = useSearchParams();
  const sources = useLoad(() => api.call('GET /wizard/sources'), [api]);
  const [step, setStep] = useState(params.get('session') ? 1 : 0);
  const [session, setSession] = useState<string | null>(params.get('session'));
  const [sessionInfo, setSessionInfo] = useState<ImportSession | null>(null);
  const [engine, setEngine] = useState('db2');
  const [src, setSrc] = useState<Record<string, string>>({ host: 'localhost', port: '50000', database: 'ERPTEST', user: 'db2admin', schemas: 'APPSCHEMA' });
  const [password, setPassword] = useState('');
  const [srcTest, setSrcTest] = useState<SourceTestResult | null>(null);
  const [destEngine, setDestEngine] = useState<'postgres' | 'snowflake'>('postgres');
  const [dest, setDest] = useState<Record<string, string>>({ url: '', schema: 'alelt_erptest', account: '', user: '', role: 'ALELT_LOADER', warehouse: 'ALELT_WH', database: '' });
  const [destEnv, setDestEnv] = useState<Environment>('TEST');
  const [destTest, setDestTest] = useState<DestTestResult | null>(null);
  const [defaults, setDefaults] = useState<WizardDefaults | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [cid, setCid] = useState('erptest');
  const [latency, setLatency] = useState(120);
  const [profile, setProfile] = useState('alelt');
  const [created, setCreated] = useState<PipelineCreated | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    if (session && !sessionInfo) api.call('GET /import/runs/{id}', { id: session }).then(setSessionInfo, () => undefined);
  }, [api, session, sessionInfo]);

  if (!sources.data) return <Loading error={sources.error} />;
  const source: WizardSource | undefined = sources.data.find((s) => s.engine === engine);
  const steps = [t('pw.step.files'), t('pw.step.source'), t('pw.step.destination'), t('pw.step.review'), t('pw.step.create')];

  const guard = async <T,>(fn: () => Promise<T>): Promise<T | null> => {
    setBusy(true);
    setErr(null);
    try { return await fn(); } catch (e) { setErr(errorText(e)); return null; } finally { setBusy(false); }
  };
  const sourceSettings = () => ({ ...Object.fromEntries(Object.entries(src).filter(([k]) => !SECRET_KEYS.test(k))), port: Number(src.port) || src.port });
  const testSource = () => guard(async () => {
    const r = await api.call('POST /wizard/source/test', { body: { engine, connection_id: cid, environment: 'TEST', settings: sourceSettings(), ...(password ? { password } : {}) } });
    setSrcTest(r);
    return r;
  });
  const destSettings = () => (destEngine === 'postgres' ? { url: dest.url, schema: dest.schema }
    : { account: dest.account, user: dest.user, role: dest.role, warehouse: dest.warehouse, database: dest.database, schema: dest.schema });
  const testDest = () => guard(async () => {
    const r = await api.call('POST /wizard/destination/test', { body: { engine: destEngine, environment: destEnv, settings: destSettings(), bootstrap: destEnv === 'TEST', reason: t('pw.reason.dest') } });
    setDestTest(r);
    return r;
  });
  const loadDefaults = () => guard(async () => {
    const d = await api.call('GET /wizard/defaults', { query: { session_id: session ?? undefined, connection_id: cid, engine } });
    setDefaults(d);
    setSelected(new Set(d.tables.filter((x) => x.selected).map((x) => x.fqn)));
    setLatency(d.latency.latency_target_s);
    setProfile(d.output_profile);
    return d;
  });
  const create = () => guard(async () => {
    const r = await api.call('POST /wizard/pipelines', { body: {
      reason: t('pw.reason.create'), connection_id: cid, environment: 'TEST', source_engine: engine, source: sourceSettings(),
      ...(password ? { password: { value: password } } : {}), destination_engine: destEngine, destination: destSettings(),
      tables: defaults && defaults.tables.length ? [...selected] : undefined, latency_target_s: latency, output_profile: profile,
      ...(session ? { session_id: session } : {}), ...(defaults?.legacy_id ? { legacy_id: defaults.legacy_id } : {}),
    } });
    setPassword('');
    setCreated(r);
    return r;
  });

  return (
    <Page title={t('pw.title')} actions={<EnvBadge env="TEST" />}>
      <p className="hint">{t('pw.intro')}</p>
      <Stepper steps={steps} current={step} onGo={created ? undefined : setStep} />
      {err && <p role="alert" className="error">{err}</p>}

      {step === 0 && <FilesStep busy={busy} onSkip={() => setStep(1)} onFiles={async (files) => {
        const s = await guard(() => api.call('POST /import/discover/files', { body: { reason: t('pw.reason.files'), files: files.map(({ name, data_base64 }) => ({ name, data_base64 })) } }));
        if (s) {
          setSession(s.id);
          setSessionInfo(s);
          const pc = s.plan?.connections.find((c) => c.supported);
          if (pc) setCid(pc.connection_id.replace(/^legacy_/, '') + '_test');
          setStep(1);
        }
      }} />}

      {step === 1 && (
        <Card title={t('pw.source.title')}>
          {sessionInfo && <p className="hint">{t('pw.fromImport', { platform: sessionInfo.platform, n: sessionInfo.plan?.summary.tables_enabled ?? sessionInfo.golden?.tables.length ?? 0 })}</p>}
          <Field label={t('pw.source.engine')}>
            <select value={engine} onChange={(e) => setEngine(e.target.value)}>
              {sources.data.map((s) => <option key={s.engine} value={s.engine} disabled={!s.available}>{s.display_name}{s.available ? '' : ` (${t('pw.planned')})`}</option>)}
            </select>
          </Field>
          <form autoComplete="off" className="grid2" onSubmit={(e) => e.preventDefault()}>
            {(source?.fields ?? []).map((f) => (
              <Field key={f.name} label={f.label}>
                {f.secret
                  ? <input type="password" autoComplete="new-password" value={password} onChange={(e) => setPassword(e.target.value)} />
                  : <input value={src[f.name] ?? String(f.default ?? '')} data-testid={`src-${f.name}`} onChange={(e) => setSrc({ ...src, [f.name]: e.target.value })} />}
              </Field>
            ))}
          </form>
          <p className="hint">{t('pw.source.secretNote')}</p>
          <button onClick={() => void testSource()} disabled={busy}>{t('pw.test')}</button>
          {srcTest && <><Steps steps={srcTest.steps} /><Checklist checks={srcTest.checks} /></>}
          <NavButtons onBack={() => setStep(0)} busy={busy} onNext={async () => {
            const r = srcTest ?? await testSource();
            if (r && !r.denied) setStep(2);
          }} />
        </Card>
      )}

      {step === 2 && (
        <Card title={t('pw.dest.title')}>
          <div className="row">
            <label><input type="radio" checked={destEngine === 'postgres'} onChange={() => { setDestEngine('postgres'); setDestTest(null); }} /> {t('pw.dest.postgres')}</label>
            <label><input type="radio" checked={destEngine === 'snowflake'} onChange={() => { setDestEngine('snowflake'); setDestTest(null); }} /> {t('pw.dest.snowflake')}</label>
          </div>
          <form autoComplete="off" className="grid2" onSubmit={(e) => e.preventDefault()}>
            {(destEngine === 'postgres' ? ['url', 'schema'] : ['account', 'user', 'role', 'warehouse', 'database', 'schema']).map((k) => (
              <Field key={k} label={t(`pw.dest.${k}` as MessageKey)} help={k === 'url' ? t('pw.dest.urlHelp') : undefined}>
                <input value={dest[k] ?? ''} onChange={(e) => { setDest({ ...dest, [k]: e.target.value }); setDestTest(null); }} />
              </Field>
            ))}
          </form>
          <Field label={t('wiz.environment')} help={t('pw.dest.envHelp')}>
            <select value={destEnv} onChange={(e) => { setDestEnv(e.target.value as Environment); setDestTest(null); }} data-testid="dest-env">
              <option value="TEST">TEST</option><option value="PROD">PROD</option>
            </select>
          </Field>
          {destEnv === 'PROD' && <p role="alert" className="error">{t('pw.dest.prodBlocked')}</p>}
          <button className="primary" onClick={() => void testDest()} disabled={busy}>{destEnv === 'TEST' ? t('pw.dest.testBootstrap') : t('pw.test')}</button>
          {destTest && <Steps steps={destTest.steps} />}
          <NavButtons onBack={() => setStep(1)} busy={busy} nextDisabled={destEnv === 'PROD'} onNext={async () => {
            const r = destTest ?? await testDest();
            if (r?.ok && (await loadDefaults())) setStep(3);
          }} />
        </Card>
      )}

      {step === 3 && defaults && (
        <Card title={t('pw.review.title')}>
          <div className="grid2">
            <Field label={t('wiz.connectionId')}><input value={cid} pattern="[a-z][a-z0-9_]*" onChange={(e) => setCid(e.target.value.toLowerCase().replace(/[^a-z0-9_]/g, '_'))} /></Field>
            <Field label={t('wiz.latency')} help={t(`pw.latency.${defaults.latency.why}` as MessageKey)}>
              <input type="number" min={10} max={86400} value={latency} onChange={(e) => setLatency(Number(e.target.value) || 120)} />
            </Field>
            <Field label={t('pw.profile')} help={t('pw.profile.help')}>
              <select value={profile} onChange={(e) => setProfile(e.target.value)}>
                {[...new Set([defaults.output_profile, 'alelt', 'golden_spec'])].map((p) => <option key={p} value={p}>{p}</option>)}
              </select>
            </Field>
          </div>
          {defaults.golden_tables.length > 0 && <p className="hint">{t('iw.golden', { n: defaults.golden_tables.length })}</p>}
          {!defaults.catalog_available && <p className="hint">{t('pw.review.noCatalog')}</p>}
          <p>{t('pw.review.selected', { n: selected.size, total: defaults.tables.length })}{' '}
            <button className="link" onClick={() => setSelected(new Set(defaults.tables.filter((x) => x.capture_enabled !== false).map((x) => x.fqn)))}>{t('pw.review.all')}</button>{' '}
            <button className="link" onClick={() => setSelected(new Set(defaults.tables.filter((x) => x.selected).map((x) => x.fqn)))}>{t('pw.review.reset')}</button></p>
          <Table head={['', t('common.table'), t('pw.review.why'), 'PK', t('pw.review.churn')]}>
            {defaults.tables.map((r) => (
              <tr key={r.fqn}>
                <td><input type="checkbox" aria-label={r.fqn} checked={selected.has(r.fqn)} onChange={(e) => {
                  const n = new Set(selected);
                  if (e.target.checked) n.add(r.fqn); else n.delete(r.fqn);
                  setSelected(n);
                }} /></td>
                <td>{r.fqn}</td>
                <td><Badge value={t(`pw.why.${r.reason}` as MessageKey)} /></td>
                <td>{r.has_pk === null ? '—' : r.has_pk ? '✓' : '—'}</td>
                <td>{r.high_churn ? <Badge value={t('pw.review.hot')} /> : r.churn_per_day ?? '—'}</td>
              </tr>
            ))}
          </Table>
          <Checklist checks={defaults.checks} />
          <NavButtons onBack={() => setStep(2)} busy={busy} nextLabel={t('pw.create')} onNext={async () => { if (await create()) setStep(4); }} />
        </Card>
      )}

      {step === 4 && created && <CreatedStep created={created} />}
    </Page>
  );
}

function FilesStep({ busy, onSkip, onFiles }: { busy: boolean; onSkip: () => void; onFiles: (f: DroppedFile[]) => void }) {
  const { t } = useI18n();
  const [files, setFiles] = useState<DroppedFile[]>([]);
  return (
    <Card title={t('pw.files.title')}>
      <p className="hint">{t('pw.files.note')}</p>
      <DropZone files={files} onFiles={setFiles} />
      <div className="wizard-nav">
        <button onClick={onSkip} data-testid="wizard-skip">{t('pw.skip')}</button>
        <button className="primary" data-testid="wizard-next" disabled={busy} onClick={() => (files.length ? onFiles(files) : onSkip())}>
          {busy ? t('common.loading') : files.length ? t('iw.analyze') : t('wiz.next')}
        </button>
      </div>
    </Card>
  );
}

const STEP_KEYS = { agent: 'pw.p.agent', prerequisites: 'pw.p.prereq', initial_load: 'pw.p.load', cdc: 'pw.p.cdc', contract: 'pw.p.contract', parity: 'pw.p.parity' } as const;

function CreatedStep({ created }: { created: PipelineCreated }) {
  const { api } = useAuth();
  const { t } = useI18n();
  const [progress, setProgress] = useState<Progress | null>(null);
  const [err, setErr] = useState<string | null>(null);
  useEffect(() => {
    let live = true;
    const tick = () => api.call('GET /wizard/pipelines/{id}/progress', { id: created.connection_id }).then(
      (p) => live && (setProgress(p), setErr(null)), (e: unknown) => live && setErr(errorText(e)));
    void tick();
    const h = setInterval(tick, 3000);
    return () => { live = false; clearInterval(h); };
  }, [api, created.connection_id]);
  const a = created.agent;
  return (
    <>
      <Card title={t('pw.agent.title')}>
        <p>{t('pw.agent.intro', { connection: created.connection_id, n: created.tables.length })}</p>
        <Field label={t('setup.agent.token')}>
          <div className="row"><code data-testid="agent-token">{a.token}</code> <CopyButton text={a.token} /></div>
        </Field>
        <p className="hint">{t('pw.agent.tokenNote', { when: a.expires_at })}</p>
        <div className="row">
          <button className="primary" onClick={() => saveJson(a.aleltagent_json, 'aleltagent.json')}>{t('setup.agent.download', { file: 'aleltagent.json' })}</button>
          <button onClick={() => saveJson(a.devedge_json, 'devedge.json')}>{t('setup.agent.download', { file: 'devedge.json' })}</button>
        </div>
        <ol className="hint">
          <li>{t('pw.agent.h1')}</li>
          <li>{t('pw.agent.h2')}</li>
          <li>{t('pw.agent.h3')}</li>
        </ol>
      </Card>
      <Card title={t('pw.progress.title')}>
        {err && <p role="alert" className="error">{err}</p>}
        {!progress ? <Loading /> : (
          <>
            <p><EnvBadge env={progress.environment} /> <Badge value={progress.status} /> {t('pw.progress.active', { source: progress.active_source })}</p>
            <ol className="progress-steps" data-testid="progress">
              {progress.steps.map((s) => (
                <li key={s.id} className={s.status.toLowerCase()}>
                  <Badge value={s.status} /> <strong>{t(STEP_KEYS[s.id])}</strong> — {s.detail}
                  {s.id === 'prerequisites' && s.status === 'FAIL' && s.checks && <Checklist checks={s.checks.filter((c) => c.status === 'FAIL')} />}
                  {s.missing && s.missing.length > 0 && <div className="hint">{t('pw.p.missing', { tables: s.missing.join(', ') })}</div>}
                </li>
              ))}
            </ol>
            <Table head={[t('common.table'), t('pw.p.rows'), t('pw.p.load'), t('pw.p.parity')]}>
              {progress.tables.map((x) => (
                <tr key={x.fqn}><td>{x.fqn}</td><td>{x.snapshot_rows}</td><td>{x.snapshot_done ? '✓' : '…'}</td><td>{x.parity ? <Badge value={x.parity} /> : '—'}</td></tr>
              ))}
            </Table>
          </>
        )}
        <p><Link to="/pipelines">{t('nav.pipelines')}</Link> · <Link to="/recon">{t('nav.recon')}</Link></p>
      </Card>
    </>
  );
}
