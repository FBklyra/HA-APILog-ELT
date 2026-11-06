import { useEffect, useState } from 'react';
import { useParams } from 'react-router';
import type { ColumnConfig } from '../api/contract';
import { expectedConfirmText } from '../api/contract';
import { useAuth } from '../auth/AuthProvider';
import { IfCan } from '../components/Layout';
import { Badge, Card, ConfirmDialog, Loading, Notice, Page, Table, useLoad } from '../components/ui';
import { useI18n } from '../i18n';
import { can } from '../lib/rbac';
import { dateTime } from '../lib/format';

type Dialog = 'save' | 'resync' | 'reprocess' | 'enable' | null;

export function TableDetail() {
  const { id = '' } = useParams();
  const { api, me } = useAuth();
  const { t, locale } = useI18n();
  const { data: d, error, reload } = useLoad(() => api.call('GET /tables/{id}', { id }), [api, id]);
  const [cols, setCols] = useState<ColumnConfig[]>([]);
  const [dialog, setDialog] = useState<Dialog>(null);
  const [range, setRange] = useState({ from: '', to: '' });
  const [notice, setNotice] = useState<string | null>(null);
  useEffect(() => setCols(d?.columns ?? []), [d]);
  if (!d) return <Loading error={error} />;

  const fqn = `${d.table.schema}.${d.table.name}`;
  const editable = !!me && can(me.roles, 'table.configure');
  const dirty = cols.some((c, i) => c.enabled !== d.columns[i]?.enabled || c.hashed !== d.columns[i]?.hashed);
  const done = () => {
    setDialog(null);
    setNotice(t('common.done'));
    reload();
  };
  const patch = (i: number, k: 'enabled' | 'hashed', v: boolean) => setCols(cols.map((c, j) => (j === i ? { ...c, [k]: v } : c)));

  return (
    <Page
      title={fqn}
      actions={
        <>
          <Badge value={d.state} /> <Badge value={d.active_source} />
          <IfCan p="table.configure">
            <button onClick={() => setDialog('enable')} disabled={!d.capture_enabled && !d.enabled}>{d.enabled ? `${t('tables.enabled')} ✓` : `${t('tables.enabled')} ✗`}</button>
          </IfCan>
          <IfCan p="table.resync"><button className="danger" onClick={() => setDialog('resync')}>{t('table.resync')}</button></IfCan>
        </>
      }
    >
      <Notice text={notice} onClose={() => setNotice(null)} />
      <p>{t('table.checkpoint')}: <code>{d.checkpoint?.text ?? '—'}</code> · r{d.revision} · {d.sync_mode}</p>
      <Card title={t('table.columns')}>
        <Table label={t('table.columns')} head={['#', t('table.column'), t('table.engineType'), t('table.canonicalType'), t('table.key'), t('tables.enabled'), t('table.hashed')]}>
          {cols.map((c, i) => (
            <tr key={c.name}>
              <td className="num">{c.ordinal}</td>
              <td><code>{c.name}</code></td>
              <td>{c.engine_type}{c.length ? `(${c.length}${c.scale ? `,${c.scale}` : ''})` : ''}{c.nullable ? '' : ' NOT NULL'}</td>
              <td>{c.canonical_type === 'UNKNOWN' ? <Badge value="FAIL" /> : c.canonical_type}</td>
              <td>{c.key_seq ?? ''}</td>
              <td><input type="checkbox" aria-label={`${c.name} enabled`} checked={c.enabled} disabled={!editable || !!c.key_seq} onChange={(e) => patch(i, 'enabled', e.target.checked)} /></td>
              <td><input type="checkbox" aria-label={`${c.name} hashed`} checked={c.hashed} disabled={!editable || !!c.key_seq} onChange={(e) => patch(i, 'hashed', e.target.checked)} /></td>
            </tr>
          ))}
        </Table>
        <IfCan p="table.configure"><button className="primary" disabled={!dirty} onClick={() => setDialog('save')}>{t('table.saveConfig')}</button></IfCan>
      </Card>
      <IfCan p="table.reprocess">
        <Card title={t('table.reprocess')}>
          <div className="row">
            <label>{t('table.from')} <input value={range.from} onChange={(e) => setRange({ ...range, from: e.target.value })} placeholder="1:00000000000A3000" /></label>
            <label>{t('table.to')} <input value={range.to} onChange={(e) => setRange({ ...range, to: e.target.value })} placeholder="1:00000000000A3FFF" /></label>
            <button className="danger" disabled={!range.from || !range.to} onClick={() => setDialog('reprocess')}>{t('table.reprocess')}</button>
          </div>
        </Card>
      </IfCan>
      <Card title={t('table.schemaHistory')}>
        <Table head={[t('common.at'), t('schema.kind'), t('common.detail'), t('common.status')]}>
          {d.schema_history.map((s) => <tr key={s.id}><td>{dateTime(s.detected_at, locale)}</td><td>{s.kind}</td><td>{s.detail}</td><td><Badge value={s.status} /></td></tr>)}
        </Table>
      </Card>
      <Card title={t('table.recon')}>
        <Table head={[t('common.at'), t('recon.check'), t('recon.outcome'), t('common.detail')]}>
          {d.recon.map((r) => <tr key={r.id}><td>{dateTime(r.at, locale)}</td><td>{r.check_kind}</td><td><Badge value={r.outcome} /></td><td>{r.detail}</td></tr>)}
        </Table>
      </Card>

      <ConfirmDialog open={dialog === 'save'} title={t('table.saveConfig')} onCancel={() => setDialog(null)}
        onConfirm={async (reason) => {
          await api.call('PUT /tables/{id}/config', { id, body: { reason, columns: cols.map((c) => ({ name: c.name, enabled: c.enabled, hashed: c.hashed })) } });
          done();
        }} />
      <ConfirmDialog open={dialog === 'enable'} title={`${fqn}: ${d.enabled ? 'disable' : 'enable'}`} onCancel={() => setDialog(null)}
        onConfirm={async (reason) => {
          await api.call('PUT /tables/{id}/config', { id, body: { reason, enabled: !d.enabled } });
          done();
        }} />
      <ConfirmDialog open={dialog === 'resync'} title={t('table.resync')} confirmText={expectedConfirmText('resync', fqn)} onCancel={() => setDialog(null)}
        onConfirm={async (reason, confirm_text) => {
          await api.call('POST /tables/{id}/resync', { id, body: { reason, confirm_text } });
          done();
        }} />
      <ConfirmDialog open={dialog === 'reprocess'} title={`${t('table.reprocess')}: ${range.from} → ${range.to}`} confirmText={expectedConfirmText('reprocess', fqn)} onCancel={() => setDialog(null)}
        onConfirm={async (reason, confirm_text) => {
          await api.call('POST /tables/{id}/reprocess', { id, body: { reason, confirm_text, from_pos: range.from, to_pos: range.to } });
          done();
        }} />
    </Page>
  );
}
