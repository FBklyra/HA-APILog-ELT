import { useState } from 'react';
import { Link } from 'react-router';
import type { DiffEntry, ImportRun, SchemaChange } from '../api/contract';
import { expectedConfirmText } from '../api/contract';
import { useAuth } from '../auth/AuthProvider';
import { IfCan } from '../components/Layout';
import { Badge, Card, ConfirmDialog, Loading, Notice, Page, Table, useLoad } from '../components/ui';
import { useI18n } from '../i18n';
import { dateTime, download } from '../lib/format';

function Diff({ entries }: { entries: DiffEntry[] }) {
  const { t } = useI18n();
  if (entries.length === 0) return <p>{t('import.noDiff')}</p>;
  return (
    <Table head={['op', 'path', 'before', 'after']}>
      {entries.map((e) => (
        <tr key={e.path} className={`diff-${e.op}`}>
          <td>{e.op}</td><td><code>{e.path}</code></td><td><code>{JSON.stringify(e.before)}</code></td><td><code>{JSON.stringify(e.after)}</code></td>
        </tr>
      ))}
    </Table>
  );
}

// ----------------------------------------------------------------- screen 4
export function SchemaImport() {
  const { api } = useAuth();
  const { t, locale } = useI18n();
  const runs = useLoad(() => api.call('GET /import/runs'), [api]);
  const [dialog, setDialog] = useState<'run' | ImportRun | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  if (!runs.data) return <Loading error={runs.error} />;
  const latest = runs.data[0];
  return (
    <Page title={t('nav.import')} actions={<><Link className="button primary" to="/import/new">{t('nav.importWizard')}</Link>{' '}<button onClick={() => setDialog('run')}>{t('import.run')}</button></>}>
      <Notice text={notice} onClose={() => setNotice(null)} />
      {latest && (
        <Card title={`${t('import.diff')} — ${latest.id} (${dateTime(latest.finished_at, locale)})`}>
          <p><Badge value={latest.status} /> {latest.source_ref}</p>
          <Diff entries={latest.diff} />
          {latest.status === 'DONE' && <button className="primary" onClick={() => setDialog(latest)}>{t('import.accept')}</button>}
        </Card>
      )}
      <ConfirmDialog open={dialog === 'run'} title={t('import.run')} onCancel={() => setDialog(null)}
        onConfirm={async (reason) => {
          await api.call('POST /import/runs', { body: { reason } });
          setDialog(null);
          runs.reload();
        }} />
      <ConfirmDialog open={typeof dialog === 'object' && dialog !== null} title={t('import.accept')} onCancel={() => setDialog(null)}
        onConfirm={async (reason) => {
          if (typeof dialog !== 'object' || !dialog) return;
          const rev = await api.call('POST /import/runs/{id}/accept', { id: dialog.id, body: { reason } });
          setDialog(null);
          setNotice(`${t('common.done')} r${rev.id}`);
          runs.reload();
        }} />
    </Page>
  );
}

// ----------------------------------------------------------------- screen 5
export function Revisions() {
  const { api } = useAuth();
  const { t, locale } = useI18n();
  const revs = useLoad(() => api.call('GET /revisions'), [api]);
  const [diff, setDiff] = useState<{ id: number; entries: DiffEntry[] } | null>(null);
  const [rollback, setRollback] = useState<number | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  if (!revs.data) return <Loading error={revs.error} />;
  return (
    <Page title={t('nav.revisions')}>
      <Notice text={notice} onClose={() => setNotice(null)} />
      <Table label={t('nav.revisions')} head={['#', t('common.at'), t('revisions.author'), t('revisions.source'), t('recon.reason'), t('common.actions')]}>
        {revs.data.map((r) => (
          <tr key={r.id}>
            <td>r{r.id} {r.current && <Badge value="OK" />}</td>
            <td>{dateTime(r.created_at, locale)}</td><td>{r.author}</td><td>{r.source}</td><td>{r.reason}</td>
            <td className="actions">
              <button onClick={async () => setDiff({ id: r.id, entries: await api.call('GET /revisions/{id}/diff', { id: String(r.id) }) })}>{t('revisions.diff')}</button>
              <button onClick={async () => download(`alelt-r${r.id}.yaml`, await api.call('GET /revisions/{id}/export', { id: String(r.id) }), 'text/yaml')}>{t('revisions.export')}</button>
              {!r.current && <IfCan p="revision.rollback"><button className="danger" onClick={() => setRollback(r.id)}>{t('revisions.rollback')}</button></IfCan>}
            </td>
          </tr>
        ))}
      </Table>
      {diff && <Card title={`${t('revisions.diff')} — r${diff.id}`}><Diff entries={diff.entries} /></Card>}
      <ConfirmDialog open={rollback !== null} title={`${t('revisions.rollback')} r${rollback}`} confirmText={expectedConfirmText('revision_rollback', String(rollback))}
        onCancel={() => setRollback(null)}
        onConfirm={async (reason, confirm_text) => {
          const rev = await api.call('POST /revisions/{id}/rollback', { id: String(rollback), body: { reason, confirm_text } });
          setRollback(null);
          setNotice(`${t('common.done')} r${rev.id}`);
          revs.reload();
        }} />
    </Page>
  );
}

// ----------------------------------------------------------------- screen 6
export function SchemaChanges() {
  const { api } = useAuth();
  const { t, locale } = useI18n();
  const list = useLoad(() => api.call('GET /schema-changes'), [api]);
  const [pending, setPending] = useState<{ sc: SchemaChange; decision: 'APPROVE' | 'REJECT' } | null>(null);
  if (!list.data) return <Loading error={list.error} />;
  return (
    <Page title={t('nav.schema')}>
      <Table label={t('nav.schema')} head={[t('common.at'), t('common.table'), t('schema.kind'), t('common.detail'), 'LRI', t('schema.policy'), t('common.status'), t('common.actions')]}>
        {list.data.map((s) => (
          <tr key={s.id}>
            <td>{dateTime(s.detected_at, locale)}</td><td>{s.table.schema}.{s.table.name}</td><td>{s.kind}</td><td>{s.detail}</td>
            <td><code>{s.position?.text ?? '—'}</code></td><td>{s.policy}</td><td><Badge value={s.status} /></td>
            <td className="actions">
              {s.status === 'PENDING' && (
                <>
                  <button className="primary" onClick={() => setPending({ sc: s, decision: 'APPROVE' })}>{t('schema.approve')}</button>
                  <button onClick={() => setPending({ sc: s, decision: 'REJECT' })}>{t('schema.reject')}</button>
                </>
              )}
            </td>
          </tr>
        ))}
      </Table>
      <ConfirmDialog open={pending !== null} title={`${pending?.decision ?? ''}: ${pending?.sc.detail ?? ''}`} onCancel={() => setPending(null)}
        onConfirm={async (reason) => {
          if (!pending) return;
          await api.call('POST /schema-changes/{id}/decision', { id: pending.sc.id, body: { reason, decision: pending.decision } });
          setPending(null);
          list.reload();
        }} />
    </Page>
  );
}
