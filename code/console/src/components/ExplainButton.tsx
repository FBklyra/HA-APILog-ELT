import { useState } from 'react';
import type { ExplainKind, ExplainResult } from '../api/diagnose';
import { useAuth } from '../auth/AuthProvider';
import { useI18n, type MessageKey } from '../i18n';
import { can } from '../lib/rbac';
import { pct } from '../lib/format';
import { Badge, errorText } from './ui';

/** "Explain with AI" on any operational error (incident, failed job/batch, quarantine record, agent event, alert).
 * Calls POST /diagnose/explain; the hub falls back to Stage-A rules when the LLM is off or the role may not use it. */
export function ExplainButton({ kind, refId, labels }: { kind: ExplainKind; refId: string; labels?: Record<string, string> }) {
  const { api, me } = useAuth();
  const { t, locale } = useI18n();
  const [res, setRes] = useState<ExplainResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  if (!me || !can(me.roles, 'view.incidents')) return null;
  const run = async () => {
    setBusy(true);
    setErr(null);
    try {
      setRes(await api.call('POST /diagnose/explain', { body: { kind, ref_id: refId, labels } }));
    } catch (e) {
      setErr(errorText(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <>
      <button className="link" onClick={run} disabled={busy} data-testid={`explain-${kind}-${refId}`}>{t('explain.button')}</button>
      {err && <span role="alert" className="error"> {err}</span>}
      {res && (
        <div className="modal-backdrop" role="presentation">
          <div className="modal" role="dialog" aria-modal="true" aria-label={t('explain.title')}>
            <h2>{t('explain.title')}</h2>
            <p className="modal-sub">
              <Badge value={res.source === 'llm' ? t('explain.ai') : t('explain.rules')} /> <Badge value={t(`inc.class.${res.classification}` as MessageKey)} />{' '}
              {t('inc.confidence')}: {pct(res.confidence, locale)}
            </p>
            {!res.llm_configured && <p className="muted" data-testid="rules-only">{t('inc.aiNotConfigured')}</p>}
            {res.llm_error && <p className="error">{res.llm_error}</p>}
            <p><strong>{res.root_cause}</strong></p>
            <p>{res.explanation}</p>
            {res.fix_steps.length > 0 && (
              <>
                <h3>{t('inc.fixSteps')}</h3>
                <ol>{res.fix_steps.map((s, i) => <li key={i}>{s}</li>)}</ol>
              </>
            )}
            <p className="muted">{t('inc.aiNeverActs')}</p>
            <div className="modal-actions"><button onClick={() => setRes(null)}>{t('explain.close')}</button></div>
          </div>
        </div>
      )}
    </>
  );
}
