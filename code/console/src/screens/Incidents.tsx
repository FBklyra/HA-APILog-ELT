import { useState } from 'react';
import { Link, useParams } from 'react-router';
import type { CodeFix, HandlingOption, IncidentAction, SourceDataIssue } from '../api/diagnose';
import { diffLines } from '../api/diagnose';
import { useAuth } from '../auth/AuthProvider';
import { ExplainButton } from '../components/ExplainButton';
import { IfCan } from '../components/Layout';
import { Badge, Card, ConfirmDialog, Loading, Notice, Page, Table, errorText, useLoad } from '../components/ui';
import { useI18n, type MessageKey } from '../i18n';
import { dateTime, download, pct } from '../lib/format';

const classKey = (c: string) => `inc.class.${c}` as MessageKey;
const optKey = (o: string) => `src.opt.${o}` as MessageKey;

// ----------------------------------------------------------------- list
export function Incidents() {
  const { api } = useAuth();
  const { t, locale } = useI18n();
  const [status, setStatus] = useState('ACTIVE');
  const list = useLoad(() => api.call('GET /incidents', { query: { status: status === 'ALL' ? undefined : status } }), [api, status]);
  const meta = useLoad(() => api.call('GET /meta'), [api]);
  return (
    <Page title={t('nav.incidents')} actions={
      <>
        <select aria-label={t('common.status')} value={status} onChange={(e) => setStatus(e.target.value)}>
          <option value="ACTIVE">{t('inc.filterActive')}</option>
          <option value="RESOLVED">{t('inc.filterResolved')}</option>
          <option value="ALL">{t('inc.filterAll')}</option>
        </select>
        <button onClick={list.reload}>{t('common.refresh')}</button>
      </>
    }>
      {meta.data && !meta.data.llm.configured && <p className="muted" data-testid="ai-off">{t('inc.aiNotConfigured')}</p>}
      {!list.data ? <Loading error={list.error} /> : list.data.length === 0 ? <p>{t('inc.none')}</p> : (
        <Table label={t('nav.incidents')} head={[t('inc.severity'), t('inc.incident'), t('inc.classification'), t('inc.rootCause'), t('inc.count'), t('inc.lastSeen'), t('common.status')]}>
          {list.data.map((i) => (
            <tr key={i.id}>
              <td><Badge value={i.severity} /></td>
              <td><Link to={`/incidents/${encodeURIComponent(i.id)}`}>{i.title}</Link></td>
              <td>{t(classKey(i.classification))}</td>
              <td>{i.root_cause ?? '—'} {i.confidence !== null && <small>({pct(i.confidence, locale)})</small>}</td>
              <td className="num">{i.count}</td>
              <td>{dateTime(i.last_seen, locale)}</td>
              <td><Badge value={i.status} /></td>
            </tr>
          ))}
        </Table>
      )}
    </Page>
  );
}

// ----------------------------------------------------------------- detail
type Pending =
  | { kind: 'action'; action: IncidentAction; decision: 'APPROVE' | 'REJECT' }
  | { kind: 'ai' }
  | { kind: 'resolve' }
  | { kind: 'codefix' }
  | { kind: 'review'; fix: CodeFix; decision: 'APPROVE' | 'REJECT' }
  | { kind: 'reveal' }
  | null;

export function IncidentDetailScreen() {
  const { id = '' } = useParams();
  const { api } = useAuth();
  const { t, locale } = useI18n();
  const inc = useLoad(() => api.call('GET /incidents/{id}', { id }), [api, id]);
  const meta = useLoad(() => api.call('GET /meta'), [api]);
  const [pending, setPending] = useState<Pending>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [cause, setCause] = useState('');
  const [saveKnown, setSaveKnown] = useState(true);
  const [revealed, setRevealed] = useState<{ column: string; position: string | null; key: Record<string, string>; value: string }[] | null>(null);
  if (!inc.data) return <Loading error={inc.error} />;
  const i = inc.data;
  const aiOn = meta.data?.llm.configured ?? false;
  const done = () => {
    setPending(null);
    setNotice(t('common.done'));
    inc.reload();
  };
  const evidence = async () => {
    try {
      download(`evidence-${i.id}.json`, JSON.stringify(await api.call('GET /incidents/{id}/evidence', { id: i.id }), null, 2), 'application/json');
    } catch (e) {
      setNotice(errorText(e));
    }
  };
  const note = async () => download(`regression-${i.id}.md`, await api.call('GET /incidents/{id}/regression-note', { id: i.id }), 'text/markdown');

  return (
    <Page title={i.title} actions={
      <>
        <button onClick={async () => { await api.call('POST /incidents/{id}/diagnose', { id: i.id, body: {} }); inc.reload(); }}>{t('inc.rerun')}</button>
        <IfCan p="incident.act">
          <button className="primary" disabled={!aiOn} title={aiOn ? '' : t('inc.aiNotConfigured')} onClick={() => setPending({ kind: 'ai' })} data-testid="ask-ai">{t('inc.askAi')}</button>
        </IfCan>
        <button onClick={evidence}>{t('inc.downloadEvidence')}</button>
        <button onClick={note}>{t('inc.regressionNote')}</button>
      </>
    }>
      <Notice text={notice} onClose={() => setNotice(null)} />
      {!aiOn && <p className="muted">{t('inc.aiNotConfigured')}</p>}
      <Card title={t('inc.rootCause')}>
        <dl className="kv">
          <dt>{t('common.status')}</dt><dd><Badge value={i.status} /> <Badge value={i.severity} /> {i.kind} · ×{i.count}</dd>
          <dt>{t('inc.classification')}</dt><dd data-testid="classification"><Badge value={t(classKey(i.classification))} /></dd>
          <dt>{t('inc.rootCause')}</dt><dd><strong>{i.root_cause ?? '—'}</strong> {i.rule_id && <code>{i.rule_id}</code>}</dd>
          <dt>{t('inc.confidence')}</dt><dd>{i.confidence !== null ? pct(i.confidence, locale) : '—'}</dd>
          <dt>{t('common.detail')}</dt><dd>{i.explanation ?? '—'}</dd>
          {i.error && <><dt>{t('inc.error')}</dt><dd><code>{i.error}</code> <ExplainButton kind="incident" refId={i.id} /></dd></>}
          <dt>{t('inc.links')}</dt><dd>{[i.table, i.position, i.batch_id && `batch ${i.batch_id.slice(0, 8)}`, i.job_id !== null && `job ${i.job_id}`, i.agent_id].filter(Boolean).join(' · ') || '—'}</dd>
          {i.confirmed_cause && <><dt>{t('inc.confirmedCause')}</dt><dd>{i.confirmed_cause}</dd></>}
        </dl>
        {i.fix_steps.length > 0 && (<><h3>{t('inc.fixSteps')}</h3><ol>{i.fix_steps.map((s, n) => <li key={n}>{s}</li>)}</ol></>)}
      </Card>

      {i.source_data && <SourceData sd={i.source_data} samples={i.sensitive_samples} revealed={revealed} onReveal={() => setPending({ kind: 'reveal' })} />}

      {i.classification === 'PLATFORM_DEFECT' && (
        <Card title={t('fix.title')}>
          <IfCan p="incident.act">
            <button disabled={!aiOn} onClick={() => setPending({ kind: 'codefix' })} data-testid="propose-fix">{t('fix.propose')}</button>
          </IfCan>
          {i.code_fixes.length === 0 && <p>{t('common.none')}</p>}
          {i.code_fixes.map((f) => <CodeFixView key={f.id} fix={f} onReview={(decision) => setPending({ kind: 'review', fix: f, decision })}
            onDownload={async () => download(`incident-${f.id.slice(0, 8)}.patch`, await api.call('GET /code-fixes/{id}/patch', { id: f.id }), 'text/x-diff')} />)}
        </Card>
      )}

      <Card title={t('inc.actions')}>
        {i.actions.length === 0 ? <p>{t('common.none')}</p> : (
          <Table head={[t('inc.action'), t('common.detail'), t('common.status'), t('common.actions')]}>
            {i.actions.map((a) => (
              <tr key={a.id}>
                <td><code>{a.kind}</code> {a.safe && <small>({t('inc.safe')})</small>}</td>
                <td>{a.description}{a.decided_by && <><br /><small>{a.decided_by} · {dateTime(a.decided_at, locale)}</small></>}</td>
                <td><Badge value={a.status} /></td>
                <td>{a.status === 'PROPOSED' && (
                  <IfCan p="incident.act">
                    <button className="primary" onClick={() => setPending({ kind: 'action', action: a, decision: 'APPROVE' })}>{t('inc.approve')}</button>{' '}
                    <button onClick={() => setPending({ kind: 'action', action: a, decision: 'REJECT' })}>{t('inc.reject')}</button>
                  </IfCan>)}
                </td>
              </tr>
            ))}
          </Table>
        )}
      </Card>

      {i.ai.length > 0 && (
        <Card title={t('inc.aiAnswer')}>
          <p className="muted">{t('inc.aiNeverActs')}</p>
          {i.ai.map((r) => (
            <div key={r.id} className="ai-run">
              <small>{dateTime(r.at, locale)} · {r.provider} {r.model} · {r.actor}{r.prompt_hash && <> · <code title={r.prompt_hash}>{r.prompt_hash.slice(0, 12)}</code></>}</small>
              {r.error ? <p className="error">{r.error}</p> : r.answer && (
                <>
                  <p><strong>{r.answer.root_cause}</strong> ({pct(r.answer.confidence, locale)}, {r.answer.risk})</p>
                  <p>{r.answer.reasoning_summary}</p>
                  <ol>{r.answer.fix_steps.map((s, n) => <li key={n}>{s}</li>)}</ol>
                </>
              )}
            </div>
          ))}
        </Card>
      )}

      {i.findings.length > 1 && (
        <Card title={t('inc.findings')}>
          <ul>{i.findings.map((f) => <li key={f.rule_id}><code>{f.rule_id}</code> {f.title} — {pct(f.confidence, locale)} <small>{f.evidence_refs.join(', ')}</small></li>)}</ul>
        </Card>
      )}

      <Card title={t('inc.timeline')}>
        <ul className="timeline">{i.timeline.map((e, n) => <li key={n}><small>{dateTime(e.at, locale)}</small> <strong>{e.kind}</strong> · {e.actor}</li>)}</ul>
      </Card>

      {i.status !== 'RESOLVED' && (
        <IfCan p="incident.act">
          <Card title={t('inc.confirmCause')}>
            <label htmlFor="confirmed-cause">{t('inc.confirmedCause')}</label>
            <textarea id="confirmed-cause" rows={2} value={cause} onChange={(e) => setCause(e.target.value)} placeholder={i.root_cause ?? ''} />
            <label className="row"><input type="checkbox" checked={saveKnown} onChange={(e) => setSaveKnown(e.target.checked)} /> {t('inc.saveKnown')}</label>
            <button className="primary" onClick={() => setPending({ kind: 'resolve' })} data-testid="resolve">{t('inc.confirmCause')}</button>
          </Card>
        </IfCan>
      )}

      <ConfirmDialog open={pending !== null} title={pendingTitle(pending, t)} onCancel={() => setPending(null)}
        onConfirm={async (reason) => {
          if (!pending) return;
          if (pending.kind === 'action') await api.call('POST /incident-actions/{id}/decision', { id: pending.action.id, body: { reason, decision: pending.decision } });
          else if (pending.kind === 'ai') await api.call('POST /incidents/{id}/diagnose', { id: i.id, body: { use_llm: true, reason } });
          else if (pending.kind === 'resolve') await api.call('POST /incidents/{id}/resolve', { id: i.id, body: { reason, confirmed_root_cause: cause.trim() || i.root_cause, save_known_issue: saveKnown } });
          else if (pending.kind === 'codefix') await api.call('POST /incidents/{id}/code-fix', { id: i.id, body: { reason } });
          else if (pending.kind === 'review') await api.call('POST /code-fixes/{id}/review', { id: pending.fix.id, body: { reason, decision: pending.decision } });
          else if (pending.kind === 'reveal') setRevealed(await api.call('POST /incidents/{id}/source-values', { id: i.id, body: { reason } }));
          done();
        }}>
        {pending?.kind === 'action' && <p>{pending.action.description}</p>}
        {pending?.kind === 'reveal' && <p>{t('src.revealNote')}</p>}
      </ConfirmDialog>
    </Page>
  );
}

function pendingTitle(p: Pending, t: (k: MessageKey) => string): string {
  if (!p) return '';
  if (p.kind === 'action' || p.kind === 'review') return `${t(p.decision === 'APPROVE' ? 'inc.approve' : 'inc.reject')}: ${p.kind === 'action' ? p.action.kind : p.fix.summary}`;
  return t(({ ai: 'inc.askAi', resolve: 'inc.confirmCause', codefix: 'fix.propose', reveal: 'src.reveal' } as const)[p.kind]);
}

function SourceData({ sd, samples, revealed, onReveal }: {
  sd: SourceDataIssue; samples: number; onReveal: () => void;
  revealed: { column: string; position: string | null; key: Record<string, string>; value: string }[] | null;
}) {
  const { t } = useI18n();
  const [copied, setCopied] = useState(false);
  return (
    <Card title={t('src.title')}>
      <dl className="kv">
        <dt>{t('src.issue')}</dt><dd><code>{sd.issue}</code> · ×{sd.count}</dd>
        <dt>{t('common.table')}</dt><dd>{sd.table ?? '—'}</dd>
        <dt>{t('src.column')}</dt><dd>{sd.column ?? '—'}</dd>
        <dt>{t('src.keys')}</dt><dd>{sd.key_columns.join(', ') || '—'}</dd>
        <dt>{t('src.positions')}</dt><dd><code>{sd.positions.slice(0, 5).join(', ') || '—'}</code></dd>
        <dt>{t('src.forOwner')}</dt><dd>{sd.explanation}</dd>
        <dt>{t('src.handling')}</dt>
        <dd>{sd.handling_options.map((o: HandlingOption) => <span key={o} className="opt">{t(optKey(o))}{o === sd.recommended && <> <Badge value={t('src.recommended')} /></>}</span>)}</dd>
      </dl>
      {sd.dba_sql && (
        <div className="sql">
          <p>{t('src.sql')}</p>
          <div className="row"><pre data-testid="dba-sql">{sd.dba_sql}</pre>
            <button onClick={async () => { await navigator.clipboard?.writeText(sd.dba_sql); setCopied(true); }}>{copied ? t('src.copied') : t('src.copy')}</button>
          </div>
        </div>
      )}
      {samples > 0 && (
        <p>{t('src.samples', { count: samples })}{' '}
          <IfCan p="incident.reveal"><button onClick={onReveal} data-testid="reveal">{t('src.reveal')}</button></IfCan>
        </p>
      )}
      {revealed && (
        <Table head={[t('src.column'), t('src.positions'), t('src.keys'), t('src.value')]}>
          {revealed.map((r, n) => <tr key={n}><td>{r.column}</td><td><code>{r.position ?? '—'}</code></td><td><code>{JSON.stringify(r.key)}</code></td><td><code>{r.value}</code></td></tr>)}
        </Table>
      )}
    </Card>
  );
}

function CodeFixView({ fix, onReview, onDownload }: { fix: CodeFix; onReview: (d: 'APPROVE' | 'REJECT') => void; onDownload: () => void }) {
  const { t, locale } = useI18n();
  const v = fix.validation;
  return (
    <div className="code-fix" data-testid={`code-fix-${fix.id}`}>
      <p><Badge value={fix.status} /> <strong>{fix.summary}</strong> <small>{fix.model} · {dateTime(fix.created_at, locale)}</small></p>
      {fix.explanation && <p>{fix.explanation}</p>}
      <dl className="kv">
        <dt>{t('fix.files')}</dt><dd><code>{fix.files.join(', ')}</code></dd>
        <dt>{t('fix.validation')}</dt>
        <dd>
          <Badge value={v.paths_ok ? 'OK' : 'FAIL'} /> {t('fix.pathsOk')}{v.disallowed.length > 0 && <code> {v.disallowed.join(', ')}</code>}<br />
          <Badge value={v.has_test ? 'OK' : 'FAIL'} /> {t('fix.hasTest')}<br />
          <Badge value={v.apply_check === 'ok' ? 'OK' : v.apply_check === 'failed' ? 'FAIL' : 'PENDING'} /> {t('fix.applies', { status: v.apply_check })}
        </dd>
      </dl>
      <pre className="diff" aria-label="patch">{diffLines(fix.patch).map((l, n) => <span key={n} className={`diff-${l.kind}`}>{l.text}{'\n'}</span>)}</pre>
      <div className="row">
        <button onClick={onDownload}>{t('fix.download')}</button>
        {fix.status === 'PROPOSED' && (
          <IfCan p="incident.act">
            <button className="primary" onClick={() => onReview('APPROVE')}>{t('fix.review')}</button>
            <button onClick={() => onReview('REJECT')}>{t('inc.reject')}</button>
          </IfCan>
        )}
      </div>
      <p className="muted">{t('fix.neverApplied', { id: fix.incident_id })} {fix.status !== 'PROPOSED' && fix.status !== 'REJECTED' && t('fix.after')}</p>
    </div>
  );
}


