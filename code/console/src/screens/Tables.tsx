import { useState } from 'react';
import { Link } from 'react-router';
import { useAuth } from '../auth/AuthProvider';
import { Badge, Loading, Page, Table, useLoad } from '../components/ui';
import { useI18n } from '../i18n';
import { duration } from '../lib/format';

export function Tables() {
  const { api } = useAuth();
  const { t, locale } = useI18n();
  const [filter, setFilter] = useState('');
  const { data, error } = useLoad(() => api.call('GET /tables'), [api]);
  if (!data) return <Loading error={error} />;
  const rows = data.items.filter((x) => `${x.table.schema}.${x.table.name}`.toLowerCase().includes(filter.toLowerCase()));
  const nf = new Intl.NumberFormat(locale);
  return (
    <Page title={t('nav.tables')} actions={<input aria-label="filter" placeholder="APPSCHEMA.…" value={filter} onChange={(e) => setFilter(e.target.value)} />}>
      <Table label={t('nav.tables')} head={[t('common.table'), t('tables.enabled'), t('tables.syncMode'), t('tables.state'), t('tables.lag'), t('tables.rowsPerDay'), t('tables.activeSource'), t('tables.certification'), t('tables.capture')]}>
        {rows.map((x) => (
          <tr key={x.id}>
            <td><Link to={`/tables/${encodeURIComponent(x.id)}`}>{x.table.schema}.{x.table.name}</Link></td>
            <td>{x.enabled ? t('common.yes') : t('common.no')}</td>
            <td>{x.sync_mode}</td>
            <td>
              <Badge value={x.phase && x.phase !== 'STREAMING' ? x.phase : x.state} />
              {x.pending_completions ? <span title="pending out-of-row completions"> +{nf.format(x.pending_completions)}</span> : null}
            </td>
            <td>{duration(x.lag_seconds)}</td>
            <td className="num">{nf.format(x.rows_per_day)}</td>
            <td><Badge value={x.active_source} /></td>
            <td><Badge value={x.certification} /></td>
            <td>{x.capture_enabled ? '✓' : <Badge value="FAIL" />}</td>
          </tr>
        ))}
      </Table>
    </Page>
  );
}
