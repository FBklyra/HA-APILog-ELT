import { useState } from 'react';
import type { ActiveSource, SwitchRequest, SwitchScope, TableCertification } from '../api/contract';
import { expectedConfirmText } from '../api/contract';
import { useAuth } from '../auth/AuthProvider';
import { IfCan } from '../components/Layout';
import { Badge, Card, ConfirmDialog, Loading, Notice, Page, Table, useLoad } from '../components/ui';
import { useI18n } from '../i18n';
import { approvalBlock } from '../lib/endpointPermissions';
import { dateTime } from '../lib/format';

type Dialog =
  | { kind: 'signoff'; cert: TableCertification }
  | { kind: 'switch'; scope: SwitchScope; label: string; target: ActiveSource }
  | { kind: 'approve' | 'reject'; req: SwitchRequest }
  | { kind: 'wave' }
  | null;

export function Certification() {
  const { api, me } = useAuth();
  const { t, locale } = useI18n();
  const certs = useLoad(() => api.call('GET /certification'), [api]);
  const waves = useLoad(() => api.call('GET /certification/waves'), [api]);
  const reqs = useLoad(() => api.call('GET /switch/requests'), [api]);
  const [dialog, setDialog] = useState<Dialog>(null);
  const [waveName, setWaveName] = useState('');
  const [waveTables, setWaveTables] = useState<string[]>([]);
  const [notice, setNotice] = useState<string | null>(null);
  if (!certs.data) return <Loading error={certs.error} />;
  const reloadAll = () => {
    certs.reload();
    waves.reload();
    reqs.reload();
  };
  const close = () => {
    setDialog(null);
    setNotice(t('common.done'));
    reloadAll();
  };
  const fqn = (c: TableCertification) => `${c.table.schema}.${c.table.name}`;
  const confirmFor = (target: ActiveSource, label: string) => expectedConfirmText(target === 'LEGACY' ? 'rollback' : 'switch', label);

  return (
    <Page title={t('nav.certification')}>
      <Notice text={notice} onClose={() => setNotice(null)} />
      <Card title={t('cert.criteria')}>
        <Table label={t('cert.criteria')} head={[t('common.table'), t('common.status'), ...certs.data[0]?.criteria.map((c) => c.criterion) ?? [], t('cert.cleanDays'), t('tables.activeSource'), t('common.actions')]}>
          {certs.data.map((c) => {
            const allPass = c.criteria.every((k) => k.status === 'PASS');
            return (
              <tr key={c.table_id} data-testid={`cert-${c.table.name}`}>
                <td>{fqn(c)}</td>
                <td><Badge value={c.status} /></td>
                {c.criteria.map((k) => <td key={k.criterion} title={k.value}><Badge value={k.status} /></td>)}
                <td className="num">{c.clean_days}</td>
                <td><Badge value={c.active_source} /></td>
                <td className="actions">
                  <IfCan p="cert.signoff">
                    {c.status !== 'CERTIFIED' && (
                      <button disabled={!allPass} title={allPass ? '' : t('cert.signoffBlocked')} onClick={() => setDialog({ kind: 'signoff', cert: c })}>{t('cert.signoff')}</button>
                    )}
                  </IfCan>
                  <IfCan p="switch.request">
                    {c.active_source === 'LEGACY' && c.status === 'CERTIFIED' && (
                      <button className="danger" onClick={() => setDialog({ kind: 'switch', scope: { table_id: c.table_id }, label: fqn(c), target: 'ALELT' })}>{t('cert.switch')}</button>
                    )}
                    {c.active_source === 'ALELT' && (
                      <button className="danger" onClick={() => setDialog({ kind: 'switch', scope: { table_id: c.table_id }, label: fqn(c), target: 'LEGACY' })}>{t('cert.rollback')}</button>
                    )}
                  </IfCan>
                </td>
              </tr>
            );
          })}
        </Table>
      </Card>

      <Card title={t('cert.waves')}>
        <Table head={[t('cert.waveName'), t('nav.tables'), t('common.at'), t('common.status'), t('common.actions')]}>
          {(waves.data ?? []).map((w) => (
            <tr key={w.id}>
              <td>{w.name}</td>
              <td>{w.table_ids.map((id) => certs.data?.find((c) => c.table_id === id)?.table.name ?? id).join(', ')}</td>
              <td>{dateTime(w.planned_at, locale)}</td>
              <td><Badge value={w.status} /></td>
              <td className="actions">
                <IfCan p="switch.request">
                  {w.status === 'PLANNED' && <button className="danger" onClick={() => setDialog({ kind: 'switch', scope: { wave_id: w.id }, label: w.name, target: 'ALELT' })}>{t('cert.switch')}</button>}
                  {w.status === 'SWITCHED' && <button className="danger" onClick={() => setDialog({ kind: 'switch', scope: { wave_id: w.id }, label: w.name, target: 'LEGACY' })}>{t('cert.rollback')}</button>}
                </IfCan>
              </td>
            </tr>
          ))}
        </Table>
        <IfCan p="wave.plan">
          <div className="row">
            <input aria-label={t('cert.waveName')} placeholder={t('cert.waveName')} value={waveName} onChange={(e) => setWaveName(e.target.value)} />
            {certs.data.map((c) => (
              <label key={c.table_id} className="chk">
                <input type="checkbox" checked={waveTables.includes(c.table_id)}
                  onChange={(e) => setWaveTables(e.target.checked ? [...waveTables, c.table_id] : waveTables.filter((x) => x !== c.table_id))} />
                {c.table.name}
              </label>
            ))}
            <button disabled={!waveName.trim() || waveTables.length === 0} onClick={() => setDialog({ kind: 'wave' })}>{t('cert.newWave')}</button>
          </div>
        </IfCan>
      </Card>

      <Card title={t('cert.requests')}>
        <Table label={t('cert.requests')} head={['#', 'scope', 'target', t('cert.requestedBy'), t('common.at'), t('common.status'), t('common.actions')]}>
          {(reqs.data ?? []).map((r) => {
            const block = me ? approvalBlock(r, me.email, me.roles) : 'role';
            return (
              <tr key={r.id} data-testid={`switch-${r.id}`}>
                <td>{r.id}</td><td>{r.scope_label}</td><td><Badge value={r.target} /></td><td>{r.requested_by}</td>
                <td>{dateTime(r.requested_at, locale)}</td><td><Badge value={r.status} />{r.approved_by ? ` · ${r.approved_by}` : ''}</td>
                <td className="actions">
                  {r.status === 'PENDING_APPROVAL' && (
                    <IfCan p="switch.approve">
                      <button className="danger" disabled={block !== null} title={block === 'self' ? t('cert.selfApprove') : ''} onClick={() => setDialog({ kind: 'approve', req: r })}>{t('cert.approve')}</button>
                      <button onClick={() => setDialog({ kind: 'reject', req: r })}>{t('cert.rejectReq')}</button>
                      {block === 'self' && <small className="hint">{t('cert.selfApprove')}</small>}
                    </IfCan>
                  )}
                </td>
              </tr>
            );
          })}
        </Table>
      </Card>

      <ConfirmDialog open={dialog?.kind === 'signoff'} title={dialog?.kind === 'signoff' ? `${t('cert.signoff')}: ${fqn(dialog.cert)}` : ''} onCancel={() => setDialog(null)}
        onConfirm={async (reason) => {
          if (dialog?.kind !== 'signoff') return;
          await api.call('POST /certification/{id}/signoff', { id: dialog.cert.table_id, body: { reason } });
          close();
        }} />
      <ConfirmDialog open={dialog?.kind === 'switch'} title={dialog?.kind === 'switch' ? `${dialog.target === 'LEGACY' ? t('cert.rollback') : t('cert.switch')}: ${dialog.label}` : ''}
        confirmText={dialog?.kind === 'switch' ? confirmFor(dialog.target, dialog.label) : ''} onCancel={() => setDialog(null)}
        onConfirm={async (reason, confirm_text) => {
          if (dialog?.kind !== 'switch') return;
          await api.call('POST /switch', { body: { reason, confirm_text, scope: dialog.scope, target: dialog.target } });
          close();
        }} />
      <ConfirmDialog open={dialog?.kind === 'approve'} title={dialog?.kind === 'approve' ? `${t('cert.approve')}: ${dialog.req.scope_label} → ${dialog.req.target}` : ''}
        confirmText={dialog?.kind === 'approve' ? confirmFor(dialog.req.target, dialog.req.scope_label) : ''} onCancel={() => setDialog(null)}
        onConfirm={async (reason, confirm_text) => {
          if (dialog?.kind !== 'approve') return;
          await api.call('POST /switch/requests/{id}/approve', { id: dialog.req.id, body: { reason, confirm_text } });
          close();
        }} />
      <ConfirmDialog open={dialog?.kind === 'reject'} title={t('cert.rejectReq')} onCancel={() => setDialog(null)}
        onConfirm={async (reason) => {
          if (dialog?.kind !== 'reject') return;
          await api.call('POST /switch/requests/{id}/reject', { id: dialog.req.id, body: { reason } });
          close();
        }} />
      <ConfirmDialog open={dialog?.kind === 'wave'} title={`${t('cert.newWave')}: ${waveName}`} onCancel={() => setDialog(null)}
        onConfirm={async (reason) => {
          await api.call('POST /certification/waves', { body: { reason, name: waveName, table_ids: waveTables, planned_at: null } });
          setWaveName('');
          setWaveTables([]);
          close();
        }} />
    </Page>
  );
}
