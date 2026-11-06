import { useEffect, useState } from 'react';
import { useAuth } from '../auth/AuthProvider';
import { Badge, Card, ConfirmDialog, Loading, Notice, Page, Table, useLoad } from '../components/ui';
import { useI18n } from '../i18n';

/** Production safety guard (PlatformAdmin): endpoints ALELT and its agents must never contact. */
export function DenyListScreen() {
  const { api } = useAuth();
  const { t } = useI18n();
  const list = useLoad(() => api.call('GET /guard/denylist'), [api]);
  const [text, setText] = useState('');
  const [open, setOpen] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  useEffect(() => {
    if (list.data) setText(list.data.entries.filter((e) => e.source === 'console').map((e) => e.entry).join('\n'));
  }, [list.data]);
  if (!list.data) return <Loading error={list.error} />;
  const fixed = list.data.entries.filter((e) => e.source === 'settings');
  return (
    <Page title={t('nav.guard')}>
      <Notice text={notice} onClose={() => setNotice(null)} />
      <Card title={t('guard.what')}>
        <p>{t('guard.intro')}</p>
        <Table head={[t('guard.format'), t('guard.example')]}>
          <tr><td>{t('guard.f.host')}</td><td><code>db2prod.corp.local</code>, <code>*.prod.corp.local</code></td></tr>
          <tr><td>{t('guard.f.ip')}</td><td><code>10.20.0.15</code>, <code>10.20.0.0/16</code></td></tr>
          <tr><td>{t('guard.f.port')}</td><td><code>db2prod.corp.local:50000</code></td></tr>
          <tr><td>{t('guard.f.account')}</td><td><code>xy12345.canada-central</code></td></tr>
          <tr><td>{t('guard.f.url')}</td><td><code>https://api.legacy.example</code></td></tr>
        </Table>
      </Card>
      <Card title={t('guard.entries')}>
        {fixed.length > 0 && <p>{t('guard.fromSettings')} {fixed.map((e) => <Badge key={e.entry} value={e.entry} />)}</p>}
        <label htmlFor="deny-entries">{t('guard.editHelp')}</label>
        <textarea id="deny-entries" data-testid="deny-entries" rows={10} value={text} onChange={(e) => setText(e.target.value)} spellCheck={false} />
        <p className="hint">{t('guard.agentNote')}</p>
        <button className="primary" onClick={() => setOpen(true)}>{t('common.save')}</button>
      </Card>
      <ConfirmDialog open={open} title={t('guard.save')} onCancel={() => setOpen(false)} onConfirm={async (reason) => {
        await api.call('PUT /guard/denylist', { body: { reason, entries: text.split(/[\n,]+/).map((x) => x.trim()).filter(Boolean) } });
        setOpen(false);
        setNotice(t('common.done'));
        list.reload();
      }} />
    </Page>
  );
}
