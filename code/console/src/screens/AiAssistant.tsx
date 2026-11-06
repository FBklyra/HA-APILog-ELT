import { useEffect, useState } from 'react';
import type { LlmProvider, LlmSettings, LlmTestResult } from '../api/diagnose';
import { useAuth } from '../auth/AuthProvider';
import { Badge, Card, ConfirmDialog, Loading, Notice, Page, errorText, useLoad } from '../components/ui';
import { useI18n } from '../i18n';

const PROVIDERS: LlmProvider[] = ['anthropic', 'openai_compatible', 'ollama'];
type Draft = Pick<LlmSettings, 'enabled' | 'provider' | 'url' | 'model' | 'timeout_s' | 'max_tokens_day' | 'max_output_tokens' | 'redaction'>;

/** Admin screen (PlatformAdmin, llm.configure): runtime LLM endpoint for Stage-B diagnosis. Env vars are defaults. */
export function AiAssistant() {
  const { api } = useAuth();
  const { t } = useI18n();
  const s = useLoad(() => api.call('GET /admin/llm'), [api]);
  const [d, setD] = useState<Draft | null>(null);
  const [key, setKey] = useState('');
  const [clearKey, setClearKey] = useState(false);
  const [saving, setSaving] = useState(false);
  const [test, setTest] = useState<LlmTestResult | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  useEffect(() => {
    if (s.data) {
      const { enabled, provider, url, model, timeout_s, max_tokens_day, max_output_tokens, redaction } = s.data;
      setD({ enabled, provider, url, model, timeout_s, max_tokens_day, max_output_tokens, redaction });
    }
  }, [s.data]);
  if (!s.data || !d) return <Loading error={s.error} />;
  const src = (f: string) => <small className="muted"> {t('ai.source', { source: s.data?.source[f] ?? 'default' })}</small>;
  const keyField = clearKey ? { api_key: '' } : key ? { api_key: key } : {};
  return (
    <Page title={t('nav.aiAssistant')}>
      <Notice text={notice} onClose={() => setNotice(null)} />
      <p className="muted">{t('ai.privacy')}</p>
      <Card title={t('ai.status')}>
        <p><Badge value={s.data.configured ? 'OK' : 'PENDING'} /> {s.data.configured ? t('ai.configured') : t('inc.aiNotConfigured')} · {t('ai.usage', { n: s.data.tokens_used_today })}</p>
      </Card>
      <Card title={t('nav.aiAssistant')}>
        <div className="form-grid">
          <label><input type="checkbox" checked={d.enabled} onChange={(e) => setD({ ...d, enabled: e.target.checked })} data-testid="ai-enabled" /> {t('ai.enabled')}</label>
          <label>{t('ai.provider')}{src('provider')}
            <select value={d.provider} onChange={(e) => setD({ ...d, provider: e.target.value as LlmProvider })}>{PROVIDERS.map((p) => <option key={p}>{p}</option>)}</select>
          </label>
          <label>{t('ai.url')}{src('url')}<input value={d.url} onChange={(e) => setD({ ...d, url: e.target.value })} spellCheck={false} /></label>
          <label>{t('ai.model')}{src('model')}<input value={d.model} onChange={(e) => setD({ ...d, model: e.target.value })} spellCheck={false} data-testid="ai-model" /></label>
          <label>{t('ai.apiKey')}
            <input type="password" autoComplete="new-password" value={key} disabled={clearKey} onChange={(e) => setKey(e.target.value)} data-testid="ai-key" />
            <small className="muted">{s.data.key_set ? t('ai.keySet') : t('ai.keyNone')}</small>
          </label>
          {s.data.key_set && <label><input type="checkbox" checked={clearKey} onChange={(e) => setClearKey(e.target.checked)} /> {t('ai.clearKey')}</label>}
          <label>{t('ai.timeout')}<input type="number" min={1} max={300} value={d.timeout_s} onChange={(e) => setD({ ...d, timeout_s: Number(e.target.value) })} /></label>
          <label>{t('ai.budget')}<input type="number" min={0} value={d.max_tokens_day} onChange={(e) => setD({ ...d, max_tokens_day: Number(e.target.value) })} /></label>
          <label>{t('ai.maxOut')}<input type="number" min={128} value={d.max_output_tokens} onChange={(e) => setD({ ...d, max_output_tokens: Number(e.target.value) })} /></label>
          <label>{t('ai.redaction')}
            <select value={d.redaction} onChange={(e) => setD({ ...d, redaction: e.target.value as Draft['redaction'] })}>
              <option value="strict">{t('ai.strict')}</option><option value="standard">{t('ai.standard')}</option>
            </select>
          </label>
        </div>
        <div className="row">
          <button onClick={async () => {
            try { setTest(await api.call('POST /admin/llm/test', { body: { ...d, ...(key ? { api_key: key } : {}) } })); } catch (e) { setNotice(errorText(e)); }
          }} data-testid="ai-test">{t('ai.test')}</button>
          <button className="primary" onClick={() => setSaving(true)} data-testid="ai-save">{t('common.save')}</button>
        </div>
        {test && <p role="status" data-testid="ai-test-result" className={test.ok ? '' : 'error'}>
          {test.ok ? t('ai.testOk', { model: test.model, ms: test.latency_ms }) : t('ai.testFail', { error: test.error ?? '' })}</p>}
      </Card>
      <ConfirmDialog open={saving} title={t('nav.aiAssistant')} onCancel={() => setSaving(false)}
        onConfirm={async (reason) => {
          await api.call('PUT /admin/llm', { body: { reason, ...d, ...keyField } });
          setSaving(false);
          setKey('');
          setClearKey(false);
          setNotice(t('common.done'));
          s.reload();
        }} />
    </Page>
  );
}
