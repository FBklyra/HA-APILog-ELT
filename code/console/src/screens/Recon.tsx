import { useState } from 'react';
import type { ReconCheck, ReconResult } from '../api/contract';
import { useAuth } from '../auth/AuthProvider';
import { ExplainButton } from '../components/ExplainButton';
import { IfCan } from '../components/Layout';
import { Badge, Card, ConfirmDialog, Loading, Page, Table, useLoad } from '../components/ui';
import { useI18n } from '../i18n';
import { bytes, dateTime } from '../lib/format';

const CHECKS: ReconCheck[] = ['ROW_COUNT', 'KEY_SET', 'COLUMN_CHECKSUM', 'DELETES', 'LEGACY_PARITY'];

export function Recon() {
  const { api } = useAuth();
  const { t, locale } = useI18n();
  const runs = useLoad(() => api.call('GET /recon'), [api]);
  const quarantine = useLoad(() => api.call('GET /quarantine'), [api]);
  const batches = useLoad(() => api.call('GET /batches'), [api]);
  const tables = useLoad(() => api.call('GET /tables'), [api]);
  const [check, setCheck] = useState<ReconCheck>('ROW_COUNT');
  const [running, setRunning] = useState(false);
  const [drill, setDrill] = useState<ReconResult | null>(null);
  if (!runs.data) return <Loading error={runs.error} />;
  const enabledIds = (tables.data?.items ?? []).filter((x) => x.enabled).map((x) => x.id);

  return (
    <Page title={t('nav.recon')} actions={
      <IfCan p="recon.run">
        <select aria-label={t('recon.check')} value={check} onChange={(e) => setCheck(e.target.value as ReconCheck)}>
          {CHECKS.map((c) => <option key={c}>{c}</option>)}
        </select>
        <button className="primary" onClick={() => setRunning(true)} disabled={enabledIds.length === 0}>{t('recon.run')}</button>
      </IfCan>
    }>
      {runs.data.map((run) => (
        <Card key={run.id} title={`${run.check_kind} — ${dateTime(run.started_at, locale)}`}>
          <Table head={[t('common.table'), t('recon.outcome'), t('common.detail'), t('recon.drill')]}>
            {run.results.map((r) => (
              <tr key={r.id}>
                <td>{r.table.schema}.{r.table.name}</td><td><Badge value={r.outcome} /></td><td>{r.detail}</td>
                <td>{r.drill && <button className="link" onClick={() => setDrill(r)}>{t('recon.drill')}</button>}</td>
              </tr>
            ))}
          </Table>
        </Card>
      ))}
      {drill?.drill && (
        <Card title={`${t('recon.drill')} — ${drill.table.schema}.${drill.table.name}`}>
          <dl className="kv">
            <dt>key</dt><dd><code>{JSON.stringify(drill.drill.key)}</code></dd>
            <dt>column</dt><dd>{drill.drill.column ?? '—'}</dd>
            <dt>LRI</dt><dd><code>{drill.drill.position?.text ?? '—'}</code></dd>
          </dl>
        </Card>
      )}
      <Card title={t('recon.quarantine')}>
        {!quarantine.data ? <Loading error={quarantine.error} /> : (
          <Table head={[t('common.at'), t('common.table'), 'LRI', t('recon.reason'), 'raw_ref', t('recon.rawHex'), t('common.status')]}>
            {quarantine.data.items.map((q) => (
              <tr key={q.id}>
                <td>{dateTime(q.at, locale)}</td><td>{q.table ? `${q.table.schema}.${q.table.name}` : '—'}</td><td><code>{q.position?.text ?? '—'}</code></td>
                <td>{q.reason}</td><td><code>{q.raw_ref ? `${q.raw_ref.batch_id.slice(0, 8)}@${q.raw_ref.offset}+${q.raw_ref.length}` : '—'}</code></td>
                <td><code>{q.raw_hex ?? ''}</code></td><td><Badge value={q.status} /> <ExplainButton kind="quarantine" refId={q.id} /></td>
              </tr>
            ))}
          </Table>
        )}
      </Card>
      <Card title="Batches">
        {!batches.data ? <Loading error={batches.error} /> : (
          <Table head={['batch_id', 'from → to', 'recs', 'bytes', 'sha256', t('common.status'), t('common.at')]}>
            {batches.data.items.map((b) => (
              <tr key={b.batch_id}>
                <td><code>{b.batch_id.slice(0, 8)}</code></td><td><code>{b.from_pos.text} → {b.to_pos.text}</code></td><td className="num">{b.rec_count}</td>
                <td className="num">{bytes(b.raw_bytes, locale)}</td><td><code title={b.sha256}>{b.sha256.slice(0, 12)}…</code></td><td><Badge value={b.status} />{b.status !== 'LOADED' && <> <ExplainButton kind="batch" refId={b.batch_id} /></>}</td><td>{dateTime(b.received_at, locale)}</td>
              </tr>
            ))}
          </Table>
        )}
      </Card>
      <ConfirmDialog open={running} title={`${t('recon.run')}: ${check}`} onCancel={() => setRunning(false)}
        onConfirm={async (reason) => {
          await api.call('POST /recon/runs', { body: { reason, check_kind: check, table_ids: enabledIds } });
          setRunning(false);
          runs.reload();
        }} />
    </Page>
  );
}
