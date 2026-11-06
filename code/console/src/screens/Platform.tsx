import { useEffect, useState } from 'react';
import type { Agent as AgentT, CommandName, GroupMapping, Limits, Role } from '../api/contract';
import { PROBES, ROLES, expectedConfirmText } from '../api/contract';
import { useAuth } from '../auth/AuthProvider';
import { IfCan } from '../components/Layout';
import { Badge, Card, ConfirmDialog, Loading, Notice, Page, Stat, Table, useLoad } from '../components/ui';
import { useI18n } from '../i18n';
import { SupportBundleButton } from './Setup';
import { bytes, dateTime, daysUntil, download, pct, toCsv } from '../lib/format';
import { positionExample, positionHintKey, startCaptureArgs, type StartMode } from '../lib/startCapture';

// ----------------------------------------------------------------- screen 9 — Agent + kill switch
type Cmd = { name: CommandName; args: Record<string, unknown>; agent: AgentT; destructive: boolean } | null;

export function Agent() {
  const { api } = useAuth();
  const { t, locale } = useI18n();
  const agents = useLoad(() => api.call('GET /agents'), [api]);
  const [cmd, setCmd] = useState<Cmd>(null);
  const [limits, setLimits] = useState<Limits | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [startMode, setStartMode] = useState<StartMode>('resume');
  const [fromPos, setFromPos] = useState('');
  const a = agents.data?.[0];
  useEffect(() => setLimits(a ? a.limits : null), [a]);
  if (!agents.data) return <Loading error={agents.error} />;
  if (!a || !limits) return <Page title={t('nav.agent')}><p>{t('common.none')}</p></Page>;
  const certDays = daysUntil(a.cert_expires_at);
  const spoolRatio = a.spool_capacity_bytes ? a.spool_bytes / a.spool_capacity_bytes : 0;
  const paused = a.state === 'PAUSED';
  const startArgs = startCaptureArgs(startMode, fromPos);
  const confirmFor = (c: NonNullable<Cmd>) =>
    c.name === 'PAUSE' ? expectedConfirmText('pause', c.agent.id) : c.name === 'SHUTDOWN' ? expectedConfirmText('shutdown', c.agent.id) : undefined;

  return (
    <Page title={`${t('nav.agent')} — ${a.hostname}`} actions={
      <IfCan p="agent.command">
        {paused
          ? <button className="primary" data-testid="resume" onClick={() => setCmd({ name: 'RESUME', args: {}, agent: a, destructive: false })}>{t('agent.resume')}</button>
          : <button className="danger kill" data-testid="kill-switch" onClick={() => setCmd({ name: 'PAUSE', args: {}, agent: a, destructive: true })}>{t('agent.killSwitch')}</button>}
      </IfCan>
    }>
      <Notice text={notice} onClose={() => setNotice(null)} />
      <div className="stats">
        <Stat label={t('agent.state')} value={<span data-testid="agent-state"><Badge value={a.state} /></span>} />
        <Stat label={t('agent.version')} value={`${a.version} · ${a.engine} · ${a.database}`} />
        <Stat label={t('agent.heartbeat')} value={dateTime(a.last_heartbeat_at, locale)} />
        <Stat label={t('agent.certExpiry')} value={`${dateTime(a.cert_expires_at, locale)} (${certDays} d)`} tone={certDays < 30 ? 'warn' : 'ok'} />
        <Stat label={t('agent.spool')} value={`${bytes(a.spool_bytes, locale)} / ${bytes(a.spool_capacity_bytes, locale)} (${pct(spoolRatio, locale)})`} tone={spoolRatio > 0.7 ? 'bad' : 'ok'} />
        <Stat label={`${t('agent.cpu')} / ${t('agent.mem')}`} value={`${a.cpu_pct.toFixed(1)} % / ${Math.round(a.mem_mb)} MB`} />
        <Stat label={t('agent.positions')} value={<code>{a.last_read_pos?.text ?? '—'} / {a.durable_pos?.text ?? '—'}</code>} />
      </div>
      <p><small>cert {a.cert_thumbprint}</small></p>
      <IfCan p="agent.command">
        <Card title={t('agent.start.title')}>
          <fieldset className="row" data-testid="start-capture">
            <label>
              <input type="radio" name="start-mode" checked={startMode === 'resume'} onChange={() => setStartMode('resume')} data-testid="start-resume" />{' '}
              {t('agent.start.resume')}
            </label>
            <label>
              <input type="radio" name="start-mode" checked={startMode === 'from_pos'} onChange={() => setStartMode('from_pos')} data-testid="start-from" />{' '}
              {t('agent.start.from')}
            </label>
            {startMode === 'from_pos' && (
              <label>{t('agent.start.position')}{' '}
                <input value={fromPos} onChange={(e) => setFromPos(e.target.value)} spellCheck={false} autoComplete="off"
                  placeholder={positionExample(a.durable_pos)} data-testid="start-from-pos" />
              </label>
            )}
            <button className="primary" disabled={!startArgs} data-testid="start-capture-btn"
              onClick={() => startArgs && setCmd({ name: 'START_CAPTURE', args: { ...startArgs }, agent: a, destructive: false })}>{t('agent.start.button')}</button>
          </fieldset>
          {startMode === 'from_pos' && (
            <p className="hint">{t(positionHintKey(a.engine), { example: positionExample(a.durable_pos) })} {t('agent.start.fromWarn')}</p>
          )}
        </Card>
        <Card title={t('agent.limits')}>
          <div className="row">
            {(Object.keys(limits) as (keyof Limits)[]).map((k) => (
              <label key={k}>{k} <input type="number" min={0} value={limits[k]} onChange={(e) => setLimits({ ...limits, [k]: Number(e.target.value) })} /></label>
            ))}
            <button onClick={() => setCmd({ name: 'SET_LIMITS', args: { ...limits }, agent: a, destructive: false })}>{t('agent.setLimits')}</button>
          </div>
        </Card>
        <Card title={t('agent.probe')}>
          <div className="row">
            {PROBES.map((p) => <button key={p} onClick={() => setCmd({ name: 'PROBE', args: { name: p }, agent: a, destructive: false })}>{p}</button>)}
            <button onClick={() => setCmd({ name: 'SNAPSHOT_CATALOG', args: { schemas: ['APPSCHEMA'] }, agent: a, destructive: false })}>SNAPSHOT_CATALOG</button>
          </div>
        </Card>
      </IfCan>
      <Card title={t('agent.governor')}>
        <Table head={[t('common.at'), 'kind', t('common.detail')]}>
          {a.governor_events.map((g, i) => <tr key={i}><td>{dateTime(g.at, locale)}</td><td>{g.kind}</td><td>{g.detail}</td></tr>)}
        </Table>
      </Card>
      <ConfirmDialog open={cmd !== null} title={cmd ? `${cmd.name} → ${cmd.agent.id}` : ''} confirmText={cmd ? confirmFor(cmd) : undefined}
        onCancel={() => setCmd(null)}
        children={cmd?.name === 'START_CAPTURE' ? (
          <p data-testid="start-capture-summary">{typeof cmd.args.from_pos === 'string'
            ? t('agent.start.confirmFrom', { position: cmd.args.from_pos }) : t('agent.start.confirmResume')}</p>
        ) : undefined}
        onConfirm={async (reason, confirm_text) => {
          if (!cmd) return;
          const res = await api.call('POST /agents/{id}/commands', { id: cmd.agent.id, body: { reason, name: cmd.name, args: cmd.args, ...(cmd.destructive ? { confirm_text } : {}) } });
          setCmd(null);
          setNotice(`${t('common.done')} ${res.command_id} ${res.status}`);
          agents.reload();
        }} />
    </Page>
  );
}

// ----------------------------------------------------------------- screen 10 — Hub & destination
export function Hub() {
  const { api } = useAuth();
  const { t, locale } = useI18n();
  const h = useLoad(() => api.call('GET /hub/health'), [api]);
  if (!h.data) return <Loading error={h.error} />;
  const d = h.data;
  const spool = d.spool_used_bytes / d.spool_capacity_bytes;
  return (
    <Page title={t('nav.hub')} actions={<><SupportBundleButton /> <button onClick={h.reload}>{t('common.refresh')}</button></>}>
      <div className="stats">
        <Stat label={t('hub.spool')} value={`${bytes(d.spool_used_bytes, locale)} / ${bytes(d.spool_capacity_bytes, locale)}`} tone={spool > 0.7 ? 'bad' : 'ok'} />
        <Stat label={t('hub.destination')} value={`${d.destination.engine} · ${d.destination.account} · ${d.destination.warehouse}`} />
        <Stat label={t('hub.credits')} value={d.destination.credits_used_month === null ? 'UNKNOWN'
          : `${d.destination.credits_used_today ?? '-'} / ${d.destination.credits_used_month} / ${d.destination.credit_quota_month ?? '-'}`}
          tone={d.destination.credits_used_month !== null && d.destination.credit_quota_month
            && d.destination.credits_used_month / d.destination.credit_quota_month > 0.8 ? 'warn' : 'ok'} />
      </div>
      <Card title={t('hub.services')}>
        <Table head={['service', t('common.status'), t('common.detail')]}>
          {d.services.map((s) => <tr key={s.name}><td>{s.name}</td><td><Badge value={s.status} /></td><td>{s.detail}</td></tr>)}
        </Table>
      </Card>
      <Card title={t('hub.queues')}>
        <Table head={['queue', 'depth', 'oldest (s)']}>
          {d.queues.map((q) => <tr key={q.name}><td>{q.name}</td><td className="num">{q.depth}</td><td className="num">{q.oldest_age_seconds}</td></tr>)}
        </Table>
      </Card>
      <Card title={t('hub.credentials')}>
        <Table head={['name', 'kind', t('hub.rotated'), t('hub.expires')]}>
          {d.credentials.map((c) => {
            const days = c.expires_at ? daysUntil(c.expires_at) : null;
            return (
              <tr key={c.name}>
                <td>{c.name}</td><td>{c.kind}</td><td>{dateTime(c.rotated_at, locale)}</td>
                <td>{dateTime(c.expires_at, locale)} {days !== null && days < 30 && <Badge value="warning" />}</td>
              </tr>
            );
          })}
        </Table>
      </Card>
    </Page>
  );
}

// ----------------------------------------------------------------- screen 11 — Retention
export function RetentionScreen() {
  const { api } = useAuth();
  const { t, locale } = useI18n();
  const r = useLoad(() => api.call('GET /retention'), [api]);
  const [draft, setDraft] = useState({ spoolGb: 0, days: 0 });
  const [open, setOpen] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  useEffect(() => {
    if (r.data) setDraft({ spoolGb: Math.round(r.data.spool_cap_bytes / 1024 ** 3), days: r.data.raw_archive_days });
  }, [r.data]);
  if (!r.data) return <Loading error={r.error} />;
  const valid = draft.days >= 30 && draft.days <= 90 && draft.spoolGb > 0;
  return (
    <Page title={t('nav.retention')}>
      <Notice text={notice} onClose={() => setNotice(null)} />
      <Card title={t('nav.retention')}>
        <div className="row">
          <label>{t('retention.spoolCap')} <input type="number" min={1} value={draft.spoolGb} onChange={(e) => setDraft({ ...draft, spoolGb: Number(e.target.value) })} /></label>
          <label>{t('retention.archiveDays')} <input type="number" min={30} max={90} value={draft.days} onChange={(e) => setDraft({ ...draft, days: Number(e.target.value) })} /></label>
          <button className="primary" disabled={!valid} onClick={() => setOpen(true)}>{t('common.save')}</button>
        </div>
      </Card>
      <Card title={t('retention.backups')}>
        <p>{t('retention.wal')}: <Badge value={r.data.wal_archiving_ok === null ? 'UNKNOWN' : r.data.wal_archiving_ok ? 'OK' : 'FAIL'} /></p>
        <Table head={[t('common.at'), 'kind', 'size', t('common.status')]}>
          {(r.data.postgres_backups ?? []).map((b) => <tr key={b.at + b.kind}><td>{dateTime(b.at, locale)}</td><td>{b.kind}</td><td>{bytes(b.size_bytes, locale)}</td><td><Badge value={b.status} /></td></tr>)}
        </Table>
      </Card>
      <ConfirmDialog open={open} title={t('nav.retention')} onCancel={() => setOpen(false)}
        onConfirm={async (reason) => {
          await api.call('PUT /retention', { body: { reason, spool_cap_bytes: draft.spoolGb * 1024 ** 3, raw_archive_days: draft.days } });
          setOpen(false);
          setNotice(t('common.done'));
          r.reload();
        }} />
    </Page>
  );
}

// ----------------------------------------------------------------- screen 12 — Users & roles
export function Users() {
  const { api } = useAuth();
  const { t, locale } = useI18n();
  const u = useLoad(() => api.call('GET /users'), [api]);
  const [maps, setMaps] = useState<GroupMapping[]>([]);
  const [open, setOpen] = useState(false);
  useEffect(() => setMaps(u.data?.group_mappings ?? []), [u.data]);
  if (!u.data) return <Loading error={u.error} />;
  return (
    <Page title={t('nav.users')}>
      <Card title={t('users.groups')}>
        <Table head={[t('users.group'), 'object id', t('users.role'), '']}>
          {maps.map((m, i) => (
            <tr key={i}>
              <td><input aria-label="group name" value={m.group_name} onChange={(e) => setMaps(maps.map((x, j) => (j === i ? { ...x, group_name: e.target.value } : x)))} /></td>
              <td><input aria-label="group id" value={m.group_id} onChange={(e) => setMaps(maps.map((x, j) => (j === i ? { ...x, group_id: e.target.value } : x)))} /></td>
              <td>
                <select aria-label={t('users.role')} value={m.role} onChange={(e) => setMaps(maps.map((x, j) => (j === i ? { ...x, role: e.target.value as Role } : x)))}>
                  {ROLES.map((r) => <option key={r}>{r}</option>)}
                </select>
              </td>
              <td><button className="link" onClick={() => setMaps(maps.filter((_, j) => j !== i))}>×</button></td>
            </tr>
          ))}
        </Table>
        <div className="row">
          <button onClick={() => setMaps([...maps, { group_id: '', group_name: '', role: 'Viewer' }])}>+</button>
          <button className="primary" onClick={() => setOpen(true)}>{t('common.save')}</button>
        </div>
      </Card>
      <Card title={t('users.users')}>
        <Table head={['email', 'name', t('users.role'), t('users.lastSeen')]}>
          {u.data.users.map((x) => <tr key={x.id}><td>{x.email}</td><td>{x.name}</td><td>{x.roles.join(', ')}</td><td>{dateTime(x.last_seen, locale)}</td></tr>)}
        </Table>
      </Card>
      <ConfirmDialog open={open} title={t('users.groups')} onCancel={() => setOpen(false)}
        onConfirm={async (reason) => {
          await api.call('PUT /users/group-mappings', { body: { reason, group_mappings: maps.filter((m) => m.group_id.trim()) } });
          setOpen(false);
          u.reload();
        }} />
    </Page>
  );
}

// ----------------------------------------------------------------- screen 13 — Audit
export function Audit() {
  const { api } = useAuth();
  const { t, locale } = useI18n();
  const [q, setQ] = useState('');
  const a = useLoad(() => api.call('GET /audit', { query: { q } }), [api, q]);
  const cols = ['at', 'actor', 'role', 'action', 'target', 'correlation_id', 'detail'];
  return (
    <Page title={t('nav.audit')} actions={
      <>
        <input aria-label={t('audit.filter')} placeholder={t('audit.filter')} value={q} onChange={(e) => setQ(e.target.value)} />
        <IfCan p="audit.export">
          <button disabled={!a.data} onClick={() => a.data && download('alelt-audit.csv', toCsv(a.data.items as unknown as Record<string, unknown>[], cols), 'text/csv')}>{t('audit.exportCsv')}</button>
          <button disabled={!a.data} onClick={() => a.data && download('alelt-audit.json', JSON.stringify(a.data.items, null, 2), 'application/json')}>{t('audit.exportJson')}</button>
        </IfCan>
      </>
    }>
      {!a.data ? <Loading error={a.error} /> : (
        <Table label={t('nav.audit')} head={[t('common.at'), t('audit.actor'), t('users.role'), t('audit.action'), t('audit.target'), t('common.detail')]}>
          {a.data.items.map((e) => (
            <tr key={e.id}>
              <td>{dateTime(e.at, locale)}</td><td>{e.actor}</td><td>{e.role}</td><td><code>{e.action}</code></td><td>{e.target}</td>
              <td><code>{JSON.stringify(e.detail)}</code></td>
            </tr>
          ))}
        </Table>
      )}
    </Page>
  );
}
