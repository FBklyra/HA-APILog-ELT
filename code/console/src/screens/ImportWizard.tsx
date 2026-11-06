import { useEffect, useState } from 'react';
import { Link } from 'react-router';
import type { ApplyResponse, Environment, HostMap, ImportSession, PlanEdits, PlannedConnection, PluginInfo } from '../api/contract';
import { saveJson } from '../api/product';
import { useAuth } from '../auth/AuthProvider';
import { Badge, Card, Loading, Page, Table, errorText, useLoad } from '../components/ui';
import { DropZone, EnvBadge, Field, NavButtons, Stepper, type DroppedFile } from '../components/wizard';
import { useI18n } from '../i18n';
import { download } from '../lib/format';
import { hostMapComplete, hostMapRows, mappingOf, targetsToCheck } from '../lib/hostMap';

const EMPTY: PlanEdits = { connections: {}, triggers: {}, roles: {} };

/** Choices of the review step carried to the host-mapping step (secret values stay in memory until apply). */
interface ApplyOptions { env: Environment; secrets: Record<string, string> }

/** Import wizard: platform -> data (files or read-only key) -> review plan -> map prod hosts to test hosts -> create. */
export function ImportWizard() {
  const { api } = useAuth();
  const { t } = useI18n();
  const plugins = useLoad(() => api.call('GET /import/plugins'), [api]);
  const [step, setStep] = useState(0);
  const [plugin, setPlugin] = useState<PluginInfo | null>(null);
  const [session, setSession] = useState<ImportSession | null>(null);
  const [result, setResult] = useState<ApplyResponse | null>(null);
  const [opts, setOpts] = useState<ApplyOptions | null>(null);
  if (!plugins.data) return <Loading error={plugins.error} />;
  const steps = [t('iw.step.platform'), t('iw.step.data'), t('iw.step.review'), t('iw.step.hosts'), t('iw.step.create')];
  return (
    <Page title={t('iw.title')}>
      <Stepper steps={steps} current={step} onGo={step < 4 ? setStep : undefined} />
      {step === 0 && (
        <div className="cards">
          {plugins.data.map((p) => (
            <button key={p.name} className={`pick ${plugin?.name === p.name ? 'selected' : ''}`} data-testid={`plugin-${p.name}`}
              onClick={() => { setPlugin(p); setStep(1); }}>
              <strong>{p.display_name}</strong>
              <span>{p.description}</span>
              <small>{[p.accepts_files && t('iw.files'), p.live && t('iw.live'), p.read_only && t('iw.readOnly')].filter(Boolean).join(' · ')}</small>
            </button>
          ))}
        </div>
      )}
      {step === 1 && plugin && <DataStep plugin={plugin} onBack={() => setStep(0)} onDone={(s) => { setSession(s); setStep(2); }} />}
      {step === 2 && session && (
        <ReviewStep session={session} onChange={setSession} onBack={() => setStep(1)} onNext={(o) => { setOpts(o); setStep(3); }} />
      )}
      {step === 3 && session && opts && (
        <HostMapStep session={session} opts={opts} onBack={() => setStep(2)}
          onApplied={(r) => { setOpts(null); setResult(r); setStep(4); }} />
      )}
      {step === 4 && session && result && <ResultStep session={session} result={result} />}
    </Page>
  );
}

function DataStep({ plugin, onBack, onDone }: { plugin: PluginInfo; onBack: () => void; onDone: (s: ImportSession) => void }) {
  const { api } = useAuth();
  const { t } = useI18n();
  const [files, setFiles] = useState<DroppedFile[]>([]);
  const [creds, setCreds] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const run = async (mode: 'files' | 'live') => {
    setBusy(true);
    setErr(null);
    try {
      const reason = t('iw.reason.discover', { platform: plugin.display_name });
      const s = mode === 'files'
        ? await api.call('POST /import/discover/files', { body: { plugin: plugin.name, reason, files: files.map(({ name, data_base64 }) => ({ name, data_base64 })) } })
        : await api.call('POST /import/discover', { body: { plugin: plugin.name, reason, credentials: creds } });
      setCreds({});
      if (s.status === 'FAILED') setErr(s.message ?? t('iw.failed'));
      else onDone(s);
    } catch (e) {
      setErr(errorText(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <>
      {plugin.accepts_files && (
        <Card title={t('iw.files.title')}>
          <p className="hint">{t('iw.files.offline')}</p>
          <DropZone files={files} onFiles={setFiles} hint={plugin.file_hint} />
          <NavButtons onBack={onBack} onNext={() => void run('files')} nextLabel={t('iw.analyze')} nextDisabled={!files.length} busy={busy} />
        </Card>
      )}
      {plugin.live && (
        <Card title={t('iw.live.title')}>
          <p className="hint">{t('iw.live.note')}</p>
          <form autoComplete="off" onSubmit={(e) => { e.preventDefault(); void run('live'); }}>
            {plugin.credential_fields.map((f) => (
              <Field key={f.name} label={f.label} help={f.help}>
                {f.multiline
                  ? <textarea rows={8} value={creds[f.name] ?? ''} onChange={(e) => setCreds({ ...creds, [f.name]: e.target.value })} spellCheck={false} />
                  : <input type={f.secret ? 'password' : 'text'} autoComplete="off" spellCheck={false} value={creds[f.name] ?? ''}
                      data-testid={`cred-${f.name}`} onChange={(e) => setCreds({ ...creds, [f.name]: e.target.value })} />}
              </Field>
            ))}
            <NavButtons onBack={plugin.accepts_files ? undefined : onBack} onNext={() => void run('live')} nextLabel={t('iw.connect')}
              nextDisabled={plugin.credential_fields.some((f) => f.required && !creds[f.name]?.trim())} busy={busy} />
          </form>
        </Card>
      )}
      {err && <p role="alert" className="error">{err}</p>}
    </>
  );
}

function merge(edits: PlanEdits, lid: string, fn: (c: NonNullable<PlanEdits['connections'][string]>) => void): PlanEdits {
  const next: PlanEdits = structuredClone(edits);
  const c = (next.connections[lid] ??= {});
  fn(c);
  return next;
}

function ReviewStep({ session, onChange, onBack, onNext }: {
  session: ImportSession; onChange: (s: ImportSession) => void; onBack: () => void; onNext: (o: ApplyOptions) => void;
}) {
  const { api } = useAuth();
  const { t } = useI18n();
  const [edits, setEdits] = useState<PlanEdits>(EMPTY);
  const [env, setEnv] = useState<Environment>('TEST');
  const [secrets, setSecrets] = useState<Record<string, string>>({});
  const [err, setErr] = useState<string | null>(null);
  const plan = session.plan;
  const edit = async (next: PlanEdits) => {
    setErr(null);
    try {
      onChange(await api.call('PUT /import/runs/{id}/plan', { id: session.id, body: { edits: next, reason: 'review' } }));
      setEdits(next);
    } catch (e) {
      setErr(errorText(e));
    }
  };
  if (!plan) {
    return (
      <Card title={t('iw.goldenOnly')}>
        <p>{session.message}</p>
        {session.golden && <p>{t('iw.golden', { n: session.golden.tables.length })}</p>}
        <Link className="button primary" to={`/wizard?session=${encodeURIComponent(session.id)}`}>{t('iw.next.parallel')}</Link>
      </Card>
    );
  }
  const hubSecrets = plan.secrets.filter((s) => s.where === 'hub');
  const agentSecrets = plan.secrets.filter((s) => s.where === 'agent');
  return (
    <>
      <Card title={t('iw.summary', { platform: session.platform })}>
        <p>{t('iw.summary.line', { c: plan.summary.connections_included ?? 0, t: plan.summary.tables_enabled ?? 0, g: plan.summary.triggers ?? 0, r: plan.summary.roles ?? 0 })}</p>
        {session.golden && <p>{t('iw.golden', { n: session.golden.tables.length })}</p>}
        {session.files.length > 0 && <p className="hint">{session.files.map((f) => `${f.name} (${f.kind})`).join(', ')}</p>}
      </Card>
      {session.warnings.length > 0 && (
        <Card title={t('iw.warnings')}>
          <ul className="checklist">{session.warnings.map((w, i) => <li key={i}><Badge value={w.severity} /> {w.message}</li>)}</ul>
        </Card>
      )}
      <Card title={t('iw.connections')}>
        {plan.connections.map((c) => <ConnectionTree key={c.legacy_id} c={c} edits={edits} onEdit={(n) => void edit(n)} />)}
      </Card>
      {(plan.triggers.length > 0 || plan.roles.length > 0) && (
        <Card title={t('iw.triggersRoles')}>
          {plan.triggers.map((tr) => (
            <label key={tr.legacy_id} className="row">
              <input type="checkbox" checked={tr.include} onChange={(e) => void edit({ ...edits, triggers: { ...edits.triggers, [tr.legacy_id]: e.target.checked } })} />
              <Badge value={tr.kind} /> {tr.name} → {tr.events.join(', ')} {tr.target ? <code>{tr.target}</code> : null}
            </label>
          ))}
          {plan.roles.map((r) => (
            <label key={r.email} className="row">
              <input type="checkbox" checked={r.include} onChange={(e) => void edit({ ...edits, roles: { ...edits.roles, [r.email]: e.target.checked } })} />
              {r.email} — {r.roles.join(', ')} <small>({r.legacy_role ?? '—'})</small>
            </label>
          ))}
        </Card>
      )}
      <Card title={t('iw.diff')}>
        {session.diff.length === 0 ? <p>{t('import.noDiff')}</p> : (
          <Table head={['op', 'path', 'before', 'after']}>
            {session.diff.map((d) => (
              <tr key={d.path} className={`diff-${d.op}`}><td>{d.op}</td><td><code>{d.path}</code></td><td><code>{JSON.stringify(d.before)}</code></td><td><code>{JSON.stringify(d.after)}</code></td></tr>
            ))}
          </Table>
        )}
        {session.needs_approval && <p className="hint">{t('iw.twoPerson')}</p>}
      </Card>
      <Card title={t('iw.secrets')}>
        <p className="hint">{t('iw.secrets.note')}</p>
        <form autoComplete="off" onSubmit={(e) => e.preventDefault()}>
          {hubSecrets.map((s) => (
            <Field key={s.key} label={s.label}>
              <input type="password" autoComplete="new-password" value={secrets[s.key] ?? ''} onChange={(e) => setSecrets({ ...secrets, [s.key]: e.target.value })} />
            </Field>
          ))}
        </form>
        {agentSecrets.map((s) => <p key={s.key} className="hint">{s.label}: {t('iw.secrets.agent')}</p>)}
        <Field label={t('wiz.environment')} help={t('wiz.environment.help')}>
          <select value={env} onChange={(e) => setEnv(e.target.value as Environment)} data-testid="iw-env">
            <option value="TEST">TEST</option><option value="PROD">PROD</option>
          </select>
        </Field>
        <EnvBadge env={env} />
      </Card>
      {err && <p role="alert" className="error">{err}</p>}
      {session.status === 'PENDING_APPROVAL' && <p className="hint">{t('iw.pending', { user: session.requested_by ?? '' })}</p>}
      <NavButtons onBack={onBack} onNext={() => onNext({ env, secrets })} nextLabel={t('iw.hosts.next')}
        nextDisabled={!plan.connections.some((c) => c.include) && !plan.triggers.some((x) => x.include) && !plan.roles.some((x) => x.include)} />
    </>
  );
}

/**
 * Required step (X-117): every production host / account of the selected connections is mapped to its TEST
 * counterpart; each target is checked live against the production deny list (POST /guard/check). Apply stays
 * disabled until every row is mapped and clean; the hub enforces the same rule (409 HOST_MAP_REQUIRED / PROD_TARGET).
 */
function HostMapStep({ session, opts, onBack, onApplied }: {
  session: ImportSession; opts: ApplyOptions; onBack: () => void; onApplied: (r: ApplyResponse) => void;
}) {
  const { api } = useAuth();
  const { t } = useI18n();
  const hm = useLoad(() => api.call('GET /import/runs/{id}/host-map', { id: session.id }), [api, session.id]);
  const [drafts, setDrafts] = useState<Record<string, string>>({});
  const [checks, setChecks] = useState<Record<string, string | null>>({});
  const [checkErr, setCheckErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [refusal, setRefusal] = useState<string | null>(null);
  const hosts: HostMap['hosts'] = hm.data?.hosts ?? [];
  useEffect(() => {
    if (hm.data) setDrafts(Object.fromEntries(hm.data.hosts.map((e) => [e.host, e.test_host ?? ''])));
  }, [hm.data]);
  const pending = targetsToCheck(hosts, drafts, checks);
  const pendingKey = pending.join('\n');
  useEffect(() => {
    if (!pendingKey) return;
    const targets = pendingKey.split('\n');
    let live = true;
    const timer = setTimeout(() => {
      api.call('POST /guard/check', { body: { targets } })
        .then((res) => { if (live) { setCheckErr(null); setChecks((c) => ({ ...c, ...Object.fromEntries(res.map((r) => [r.target, r.denied_by])) })); } })
        .catch((e: unknown) => { if (live) setCheckErr(errorText(e)); });
    }, 300);
    return () => { live = false; clearTimeout(timer); };
  }, [api, pendingKey]);
  if (!hm.data) return <Loading error={hm.error} />;
  const rows = hostMapRows(hosts, drafts, checks);
  const ready = hostMapComplete(rows) && !busy;
  const apply = async () => {
    setBusy(true);
    setRefusal(null);
    try {
      const host_map = mappingOf(rows);
      if (Object.keys(host_map).length) {
        await api.call('PUT /import/runs/{id}/host-map', { id: session.id, body: { mappings: host_map, reason: t('iw.reason.hosts') } });
      }
      const body = { reason: t('iw.reason.apply', { platform: session.platform }), environment: opts.env, start_shadow: true, host_map,
        secrets: Object.fromEntries(Object.entries(opts.secrets).filter(([, v]) => v).map(([k, v]) => [k, { value: v }])) };
      onApplied(await api.call('POST /import/runs/{id}/apply', { id: session.id, body }));
    } catch (e) {
      setRefusal(errorText(e));
    } finally {
      setBusy(false);
    }
  };
  const status = (r: (typeof rows)[number]) =>
    r.state === 'ok' ? <Badge value={t('iw.hosts.clean')} />
      : r.state === 'denied' ? <span className="error" data-testid={`host-denied-${r.host}`}>{t('iw.hosts.denied', { entry: r.deniedBy ?? '' })}</span>
        : r.state === 'invalid' ? <span className="error">{t('iw.hosts.invalid')}</span>
          : r.state === 'checking' ? <span aria-busy="true">{t('iw.hosts.checking')}</span>
            : <span className="hint">{t('iw.hosts.missing')}</span>;
  return (
    <>
      <Card title={t('iw.hosts.title')}>
        <p className="hint">{t('iw.hosts.intro')}</p>
        {rows.length === 0 ? <p>{t('iw.hosts.none')}</p> : (
          <Table head={[t('iw.hosts.prod'), t('iw.hosts.usedBy'), t('iw.hosts.test'), t('common.status')]}>
            {hosts.map((e, i) => (
              <tr key={e.host} data-testid={`host-row-${e.host}`}>
                <td><code>{e.host}</code> {e.prod_denied && <Badge value="PROD" />}</td>
                <td>{e.connections.join(', ')} <small>({e.sides.join(', ')})</small></td>
                <td><input aria-label={t('iw.hosts.testFor', { host: e.host })} value={drafts[e.host] ?? ''} spellCheck={false} autoComplete="off"
                  data-testid={`host-map-${e.host}`} onChange={(ev) => setDrafts({ ...drafts, [e.host]: ev.target.value })} /></td>
                <td>{status(rows[i]!)}</td>
              </tr>
            ))}
          </Table>
        )}
        {checkErr && <p role="alert" className="error">{checkErr}</p>}
      </Card>
      {refusal && <p role="alert" className="error" data-testid="hub-refusal">{t('iw.hosts.refused', { message: refusal })}</p>}
      {session.status === 'PENDING_APPROVAL' && <p className="hint">{t('iw.pending', { user: session.requested_by ?? '' })}</p>}
      <NavButtons onBack={onBack} onNext={() => void apply()} busy={busy} nextDisabled={!ready}
        nextLabel={session.status === 'PENDING_APPROVAL' ? t('iw.approve') : t('iw.create')} />
    </>
  );
}

function ConnectionTree({ c, edits, onEdit }: { c: PlannedConnection; edits: PlanEdits; onEdit: (e: PlanEdits) => void }) {
  const { t } = useI18n();
  const [open, setOpen] = useState(c.include);
  const sched = c.schedule.daily_time ? t('iw.sched.daily', { time: c.schedule.daily_time })
    : c.schedule.frequency_minutes ? t('iw.sched.every', { n: c.schedule.frequency_minutes }) : '—';
  return (
    <div className="tree">
      <div className="row">
        <input type="checkbox" aria-label={t('iw.include')} checked={c.include} disabled={!c.supported}
          onChange={(e) => onEdit(merge(edits, c.legacy_id, (x) => { x.include = e.target.checked; }))} />
        <button className="link" onClick={() => setOpen(!open)}>{open ? '▾' : '▸'} <strong>{c.legacy_name}</strong></button>
        <code>{c.connection_id}</code> <Badge value={c.source_engine ?? '?'} /> {!c.supported && <Badge value="unsupported" />}
        <span>{t('iw.tables', { n: c.enabled_tables, total: c.total_tables })}</span>
        <span>{t('iw.legacySchedule')}: {sched}{c.paused ? ` (${t('iw.paused')})` : ''}</span>
        <label>{t('wiz.latency')} <input type="number" min={10} max={86400} className="narrow" value={c.latency_target_s}
          onChange={(e) => onEdit(merge(edits, c.legacy_id, (x) => { x.latency_target_s = Math.max(10, Number(e.target.value) || 120); }))} /> s</label>
      </div>
      {open && Object.entries(c.tables.schemas).map(([s, sd]) => (
        <ul key={s}>
          <li><strong>{s}</strong>
            <ul>
              {Object.entries(sd.tables).map(([tn, td]) => (
                <TableNode key={tn} fqn={`${s}.${tn}`} enabled={td.enabled} syncMode={td.sync_mode} columns={td.columns}
                  onTable={(v) => onEdit(merge(edits, c.legacy_id, (x) => { (x.tables ??= {})[`${s}.${tn}`] = { ...(x.tables?.[`${s}.${tn}`] ?? {}), enabled: v }; }))}
                  onColumn={(col, patch) => onEdit(merge(edits, c.legacy_id, (x) => {
                    const te = ((x.tables ??= {})[`${s}.${tn}`] ??= {});
                    (te.columns ??= {})[col] = { ...(te.columns[col] ?? {}), ...patch };
                  }))} />
              ))}
            </ul>
          </li>
        </ul>
      ))}
    </div>
  );
}

function TableNode({ fqn, enabled, syncMode, columns, onTable, onColumn }: {
  fqn: string; enabled: boolean; syncMode: string; columns: Record<string, { enabled: boolean; hashed: boolean; is_primary_key: boolean }>;
  onTable: (v: boolean) => void; onColumn: (col: string, patch: { enabled?: boolean; hashed?: boolean }) => void;
}) {
  const { t } = useI18n();
  const [open, setOpen] = useState(false);
  const cols = Object.entries(columns);
  return (
    <li>
      <input type="checkbox" aria-label={fqn} checked={enabled} onChange={(e) => onTable(e.target.checked)} />{' '}
      <button className="link" onClick={() => setOpen(!open)} disabled={!cols.length}>{cols.length ? (open ? '▾' : '▸') : '·'} {fqn.split('.')[1]}</button>{' '}
      {syncMode !== 'SOFT_DELETE' && <Badge value={syncMode} />}
      {cols.some(([, v]) => v.hashed) && <Badge value={t('iw.hashed')} />}
      {open && (
        <ul>
          {cols.map(([cn, cv]) => (
            <li key={cn}>
              <code>{cn}</code> {cv.is_primary_key && <Badge value="PK" />}
              <label><input type="checkbox" checked={cv.enabled} disabled={cv.is_primary_key} onChange={(e) => onColumn(cn, { enabled: e.target.checked })} /> {t('iw.col.enabled')}</label>
              <label><input type="checkbox" checked={cv.hashed} disabled={cv.is_primary_key} onChange={(e) => onColumn(cn, { hashed: e.target.checked })} /> {t('iw.col.hashed')}</label>
            </li>
          ))}
        </ul>
      )}
    </li>
  );
}

function ResultStep({ session, result }: { session: ImportSession; result: ApplyResponse }) {
  const { api } = useAuth();
  const { t } = useI18n();
  const [err, setErr] = useState<string | null>(null);
  if (result.status === 'PENDING_APPROVAL') {
    return <Card title={t('iw.pendingTitle')}><p>{t('iw.pending', { user: result.requested_by ?? '' })}</p></Card>;
  }
  const r = result.result;
  return (
    <Card title={t('iw.doneTitle', { rev: r?.revision ?? '' })}>
      <Table head={[t('wiz.connectionId'), t('common.status'), t('iw.missingSecrets'), t('common.actions')]}>
        {(r?.connections ?? []).map((c) => (
          <tr key={c.connection_id}>
            <td><code>{c.connection_id}</code></td><td><Badge value={c.status} /></td>
            <td>{c.missing_secrets.join(', ') || '—'}</td>
            <td className="actions">
              <Link className="button primary" to={`/wizard?session=${encodeURIComponent(session.id)}`}>{t('iw.next.parallel')}</Link>
              <button onClick={async () => {
                try { download(`${c.connection_id}.tables.yaml`, await api.call('GET /pipelines/{id}/tables.yaml', { id: c.connection_id }), 'text/yaml'); } catch (e) { setErr(errorText(e)); }
              }}>{t('iw.next.yaml')}</button>
              <button onClick={async () => {
                try {
                  const pkg = await api.call('POST /wizard/pipelines/{id}/agent', { id: c.connection_id, body: { reason: t('iw.reason.agent') } });
                  saveJson(pkg.aleltagent_json, 'aleltagent.json');
                } catch (e) { setErr(errorText(e)); }
              }}>{t('iw.next.agent')}</button>
            </td>
          </tr>
        ))}
      </Table>
      <p>{t('iw.done.line', { g: r?.triggers ?? 0, r: r?.roles_granted.length ?? 0 })}</p>
      {err && <p role="alert" className="error">{err}</p>}
    </Card>
  );
}
