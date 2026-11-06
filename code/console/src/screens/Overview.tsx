import { Link } from 'react-router';
import { useAuth } from '../auth/AuthProvider';
import { ExplainButton } from '../components/ExplainButton';
import { IfCan } from '../components/Layout';
import { Badge, Card, Loading, Page, Stat, Table, useLoad } from '../components/ui';
import { useI18n } from '../i18n';
import { dateTime, duration, pct } from '../lib/format';

export function Overview() {
  const { api } = useAuth();
  const { t, locale } = useI18n();
  const { data: o, error, reload } = useLoad(() => api.call('GET /overview'), [api]);
  const inc = useLoad(() => api.call('GET /incidents/summary'), [api]);
  if (!o) return <Loading error={error} />;
  const latTone = o.latency_p95_seconds === null ? 'warn' : o.latency_p95_seconds <= o.latency_target_seconds ? 'ok' : 'bad';
  const sys = o.system_columns;
  return (
    <Page title={t('nav.overview')} actions={<button onClick={reload}>{t('common.refresh')}</button>}>
      <div className="stats">
        {inc.data && (
          <Stat label={t('ov.incidents')} tone={inc.data.critical ? 'bad' : inc.data.open ? 'warn' : 'ok'}
            value={<IfCan p="view.incidents"><Link to="/incidents" data-testid="incident-badge">{t('inc.badge', { count: inc.data.open, critical: inc.data.critical })}</Link></IfCan>} />
        )}
        <Stat label={t('overview.health')} value={<Badge value={o.pipeline_health} />} />
        <Stat label={t('overview.lag')} value={`${duration(o.capture_lag_seconds)}${o.capture_lag_lri_distance !== null ? ` · ${o.capture_lag_lri_distance} LRI` : ''}`} />
        <Stat label={t('overview.latency')} value={`${o.latency_p95_seconds === null ? '—' : duration(o.latency_p95_seconds)} / ${duration(o.latency_target_seconds)}`} tone={latTone} />
        <Stat label={t('overview.parity')} value={pct(o.parity_score, locale)} tone={o.parity_score >= 0.99 ? 'ok' : 'warn'} />
        <Stat label={t('overview.tablesInSync')} value={`${o.tables_in_sync} / ${o.tables_total}`} />
        <Stat label={t('overview.lastBatch')} value={o.last_batch ? <><code>{o.last_batch.to_pos.text}</code><br /><small>{dateTime(o.last_batch.received_at, locale)}</small></> : '—'} />
        <Stat label={t('overview.profile')} value={<><code>{o.output_profile}</code><br /><small>{sys.synced} · {sys.deleted}</small></>} />
      </div>
      <Card title={t('overview.alerts')}>
        {o.open_alerts.length === 0 ? <p>{t('common.none')}</p> : (
          <ul className="alerts">
            {o.open_alerts.map((a) => (
              <li key={a.id}><Badge value={a.severity} /> <strong>{a.name}</strong> — {a.summary} <small>({dateTime(a.since, locale)})</small> <ExplainButton kind="alert" refId={a.name} /></li>
            ))}
          </ul>
        )}
      </Card>
      <Card title={t('overview.ladder')}>
        <Table head={['Level', 'Name', t('common.status'), 'Env', t('common.at')]} label={t('overview.ladder')}>
          {o.ladder.map((l) => (
            <tr key={l.level}>
              <td>{l.level}</td><td>{l.name}</td><td><Badge value={l.status} />{l.stale ? <small> (stale)</small> : null}</td><td>{l.environment}</td><td>{dateTime(l.finished_at, locale)}</td>
            </tr>
          ))}
        </Table>
      </Card>
    </Page>
  );
}
