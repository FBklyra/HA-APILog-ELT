import { useEffect, useState } from 'react';
import type { CaptureConnection, TopologyMode, TopologyPlan, TopologyStatus } from '../api/contract';
import { useAuth } from '../auth/AuthProvider';
import { IfCan } from '../components/Layout';
import { Badge, Card, ConfirmDialog, Loading, Notice, Page, Stat, Table, errorText, useLoad } from '../components/ui';
import { useI18n, type MessageKey } from '../i18n';
import { dateTime } from '../lib/format';
import { leaseRemainingSeconds, operationLive } from '../lib/topology';

const yes = (ok: boolean) => <Badge value={ok ? 'OK' : 'DEGRADED'} />;
const STEP_TONE: Record<string, string> = { done: 'DONE', running: 'RUNNING', failed: 'FAILED', todo: 'PENDING', skipped: 'SKIPPED', skip: 'SKIPPED', blocked: 'FAILED' };

/** PlatformAdmin screen "Topology & HA": per-layer HA status, replicas, capture leases, switch plan (X-50). */
export function Topology() {
  const { api } = useAuth();
  const { t, locale } = useI18n();
  const s = useLoad(() => api.call('GET /topology'), [api]);
  const [failover, setFailover] = useState<CaptureConnection | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const live = operationLive(s.data?.operation ?? null);
  const { reload } = s;

  useEffect(() => {
    const tick = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(tick);
  }, []);
  useEffect(() => {
    if (!live) return undefined;
    const poll = setInterval(reload, 5000);
    return () => clearInterval(poll);
  }, [live, reload]);

  if (!s.data) return <Loading error={s.error} />;
  const d = s.data;
  return (
    <Page title={t('nav.topology')} actions={<button onClick={reload}>{t('common.refresh')}</button>}>
      <Notice text={notice} onClose={() => setNotice(null)} />
      <div className="stats">
        <Stat label={t('topo.mode')} value={<span data-testid="topology-mode"><Badge value={d.mode.toUpperCase()} /> {t(`topo.mode.${d.mode}` as MessageKey)}</span>}
          tone={d.mode === 'ha' ? 'ok' : 'warn'} />
        <Stat label={t('topo.nodes')} value={`${d.nodes.filter((n) => n.alive).length} / ${d.nodes.length}`} />
      </div>
      <Layers d={d} />
      <Card title={t('topo.nodes')}>
        <Table head={[t('topo.node'), t('topo.role'), t('topo.host'), t('topo.lastSeen'), t('common.status')]}>
          {d.nodes.map((n) => (
            <tr key={n.node_id}>
              <td><code>{n.node_id}</code></td><td>{n.role}</td><td>{n.host}</td><td>{dateTime(n.last_seen, locale)}</td>
              <td><Badge value={n.alive ? 'OK' : 'DOWN'} /> {n.alive ? t('topo.alive') : t('topo.down')}</td>
            </tr>
          ))}
        </Table>
      </Card>
      <Card title={t('topo.connections')}>
        {d.connections.length === 0 ? <p>{t('common.none')}</p> : d.connections.map((c) => {
          const left = leaseRemainingSeconds(c.expires_at, now);
          const ap = c.agent_mode === 'active_passive';
          return (
            <div key={c.connection_id} className="card" data-testid={`conn-${c.connection_id}`}>
              <div className="row">
                <strong>{c.connection_id}</strong>
                <span>{t('topo.agentMode')}: {t(`topo.agentMode.${c.agent_mode}` as MessageKey)}</span>
                {ap && <span>{t('topo.holder')}: <code>{c.holder ?? '—'}</code></span>}
                {ap && <span>{t('topo.epoch')}: <code>{c.epoch}</code></span>}
                {ap && <span>{left > 0 ? t('topo.expiresIn', { seconds: left }) : <Badge value="warning" />} {left > 0 ? '' : t('topo.expired')}</span>}
                {ap && c.holder && (
                  <IfCan p="agent.command">
                    <button className="danger" data-testid={`failover-${c.connection_id}`} onClick={() => setFailover(c)}>{t('topo.failover')}</button>
                  </IfCan>
                )}
              </div>
              <Table head={[t('topo.agents'), t('topo.host'), t('topo.role'), t('common.status'), t('topo.lastSeen')]}>
                {c.agents.map((a) => (
                  <tr key={a.agent_id}>
                    <td><code>{a.agent_id}</code></td><td>{a.hostname ?? '—'}</td>
                    <td><Badge value={a.role === 'active' ? 'OK' : a.role.toUpperCase()} /> {a.role}</td>
                    <td>{a.state ? <Badge value={a.state} /> : '—'}</td><td>{dateTime(a.last_heartbeat, locale)}</td>
                  </tr>
                ))}
              </Table>
            </div>
          );
        })}
      </Card>
      <SwitchPlanner current={d.mode} />
      <Operation d={d} live={live} />
      <Card title={t('topo.install')}>
        <Table head={[t('topo.mode'), t('topo.install.when'), t('topo.install.command'), t('topo.install.doc')]}>
          {d.install_options.map((o) => (
            <tr key={o.mode}>
              <td><strong>{o.title}</strong></td><td>{o.when}</td><td><code>{o.command}</code></td><td><code>{o.doc}</code></td>
            </tr>
          ))}
        </Table>
      </Card>
      <ConfirmDialog open={failover !== null} title={failover ? t('topo.failoverTitle', { connection: failover.connection_id }) : ''}
        onCancel={() => setFailover(null)}
        onConfirm={async (reason) => {
          if (!failover) return;
          const r = await api.call('POST /topology/connections/{id}/release-lease', { id: failover.connection_id, body: { reason } });
          setFailover(null);
          setNotice(t('topo.failoverDone', { holder: r.holder ?? '—', epoch: r.epoch }));
          reload();
        }}>
        <p>{t('topo.failoverHelp')}</p>
      </ConfirmDialog>
    </Page>
  );
}

function Layers({ d }: { d: TopologyStatus }) {
  const { t } = useI18n();
  return (
    <Card title={t('topo.layers')}>
      <Table head={[t('topo.layer'), t('topo.ha'), t('topo.kind'), t('common.detail'), t('topo.advice')]} label={t('topo.layers')}>
        {d.layers.map((l) => (
          <tr key={l.layer} data-testid={`layer-${l.layer}`}>
            <td>{l.layer}</td><td>{yes(l.ha)} {l.ha ? t('common.yes') : t('common.no')}</td><td>{l.kind}</td><td><code>{l.detail}</code></td><td>{l.advice ?? '—'}</td>
          </tr>
        ))}
      </Table>
    </Card>
  );
}

function SwitchPlanner({ current }: { current: TopologyStatus['mode'] }) {
  const { api } = useAuth();
  const { t } = useI18n();
  const [to, setTo] = useState<TopologyMode>(current === 'ha' ? 'standalone' : 'ha');
  const [dbUrl, setDbUrl] = useState('');
  const [spoolUrl, setSpoolUrl] = useState('');
  const [plan, setPlan] = useState<TopologyPlan | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [copied, setCopied] = useState(false);
  const run = async () => {
    setBusy(true);
    setErr(null);
    setCopied(false);
    try {
      setPlan(await api.call('POST /topology/plan', {
        body: { to, ...(dbUrl.trim() ? { db_url: dbUrl.trim() } : {}), ...(spoolUrl.trim() ? { spool_url: spoolUrl.trim() } : {}) },
      }));
    } catch (e) {
      setErr(errorText(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <Card title={t('topo.switch')}>
      <fieldset className="row">
        <legend>{t('topo.target')}</legend>
        {(['ha', 'standalone'] as const).map((m) => (
          <label key={m}><input type="radio" name="topo-to" value={m} checked={to === m} onChange={() => setTo(m)} /> {t(`topo.mode.${m}`)}</label>
        ))}
      </fieldset>
      <div className="row">
        <label>{t('topo.dbUrl')} <input value={dbUrl} onChange={(e) => setDbUrl(e.target.value)} spellCheck={false} autoComplete="off"
          placeholder={to === 'ha' ? 'postgresql+psycopg://alelt@pg-ha:5432/alelt' : 'sqlite:////data/alelt.db'} /></label>
        <label>{t('topo.spoolUrl')} <input value={spoolUrl} onChange={(e) => setSpoolUrl(e.target.value)} spellCheck={false} autoComplete="off"
          placeholder={to === 'ha' ? 's3://alelt-spool/hub' : 'file:///var/lib/alelt/spool'} /></label>
        <button className="primary" disabled={busy} onClick={run} data-testid="topology-plan">{t('topo.plan')}</button>
      </div>
      {err && <p role="alert" className="error">{err}</p>}
      {plan && (
        <div data-testid="topology-plan-result">
          <p><strong>{t('topo.planFrom', { from: plan.from, to: plan.to })}</strong></p>
          {plan.blockers.length > 0 && (
            <div role="alert"><h3>{t('topo.blockers')}</h3><ul>{plan.blockers.map((b) => <li key={b} className="error">{b}</li>)}</ul></div>
          )}
          {plan.warnings.length > 0 && <div><h3>{t('topo.warnings')}</h3><ul>{plan.warnings.map((w) => <li key={w}>{w}</li>)}</ul></div>}
          <h3>{t('topo.steps')}</h3>
          <ol>
            {plan.steps.map((st) => (
              <li key={st.id}><Badge value={STEP_TONE[st.status] ?? st.status.toUpperCase()} /> <strong>{st.title}</strong> — {st.detail}</li>
            ))}
          </ol>
          <h3>{t('topo.command')}</h3>
          <div className="row">
            <code data-testid="topology-command">{plan.command}</code>
            <button onClick={() => { void navigator.clipboard?.writeText(plan.command); setCopied(true); }}>{copied ? t('topo.copied') : t('topo.copy')}</button>
          </div>
          <p><small>{t('topo.cliWhy')}</small></p>
        </div>
      )}
    </Card>
  );
}

function Operation({ d, live }: { d: TopologyStatus; live: boolean }) {
  const { t, locale } = useI18n();
  const op = d.operation;
  return (
    <Card title={t('topo.operation')}>
      {!op ? <p>{t('topo.noOperation')}</p> : (
        <>
          <p>{t('topo.opHeader', { to: op.to, status: op.status, started: dateTime(op.started_at, locale) })} {live && <small>{t('topo.autoRefresh')}</small>}</p>
          <Table head={[t('topo.step'), t('common.status'), t('common.at'), t('common.detail')]}>
            {op.steps.map((st) => (
              <tr key={st.id}>
                <td>{st.id}</td><td><Badge value={STEP_TONE[st.status] ?? st.status.toUpperCase()} /></td>
                <td>{dateTime(st.at, locale)}</td><td>{st.detail}</td>
              </tr>
            ))}
          </Table>
        </>
      )}
    </Card>
  );
}
