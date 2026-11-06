import { useState } from 'react';
import { Link } from 'react-router';
import type { Progress } from '../api/contract';
import { useAuth } from '../auth/AuthProvider';
import { IfCan } from '../components/Layout';
import { Badge, Card, Loading, Page, Table, errorText, useLoad } from '../components/ui';
import { EnvBadge } from '../components/wizard';
import { useI18n } from '../i18n';
import { dateTime } from '../lib/format';

/** Pipelines created by the import or the parallel-test wizard, with their environment label. */
export function Pipelines() {
  const { api } = useAuth();
  const { t, locale } = useI18n();
  const list = useLoad(() => api.call('GET /pipelines'), [api]);
  const [progress, setProgress] = useState<Progress | null>(null);
  const [err, setErr] = useState<string | null>(null);
  if (!list.data) return <Loading error={list.error} />;
  return (
    <Page title={t('nav.pipelines')} actions={
      <IfCan p="table.configure"><Link className="button primary" to="/wizard">{t('nav.wizard')}</Link></IfCan>
    }>
      {list.data.length === 0 ? <p>{t('pl.empty')}</p> : (
        <Table label={t('nav.pipelines')} head={[t('wiz.connectionId'), t('wiz.environment'), t('common.status'), t('pl.source'), t('pl.tables'), t('wiz.latency'), t('pl.active'), t('common.at'), '']}>
          {list.data.map((p) => (
            <tr key={p.connection_id}>
              <td><code>{p.connection_id}</code></td>
              <td><EnvBadge env={p.environment} /></td>
              <td><Badge value={p.status} /></td>
              <td>{p.source_engine} {p.source_host ? `· ${p.source_host}` : ''} {p.source_database ? `/ ${p.source_database}` : ''}</td>
              <td>{p.tables}</td>
              <td>{p.latency_target_s} s</td>
              <td><Badge value={p.active_source} /></td>
              <td>{dateTime(p.created_at, locale)}</td>
              <td><button onClick={async () => {
                try { setProgress(await api.call('GET /wizard/pipelines/{id}/progress', { id: p.connection_id })); } catch (e) { setErr(errorText(e)); }
              }}>{t('pl.progress')}</button></td>
            </tr>
          ))}
        </Table>
      )}
      {err && <p role="alert" className="error">{err}</p>}
      {progress && (
        <Card title={`${t('pw.progress.title')} — ${progress.connection_id}`}>
          <p><EnvBadge env={progress.environment} /></p>
          <ul className="checklist">{progress.steps.map((s) => <li key={s.id}><Badge value={s.status} /> {s.id}: {s.detail}</li>)}</ul>
        </Card>
      )}
    </Page>
  );
}
