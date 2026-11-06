import { useCallback, useEffect, useId, useState, type ReactNode } from 'react';
import { HttpError } from '../api/client';
import { useI18n } from '../i18n';

/** Load data; `reload()` refetches. Errors are surfaced, never swallowed. */
export function useLoad<T>(fn: () => Promise<T>, deps: unknown[]): { data: T | null; error: string | null; reload: () => void } {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [tick, setTick] = useState(0);
  useEffect(() => {
    let live = true;
    fn().then(
      (d) => live && (setData(d), setError(null)),
      (e: unknown) => live && setError(errorText(e)),
    );
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, tick]);
  const reload = useCallback(() => setTick((x) => x + 1), []);
  return { data, error, reload };
}

export function errorText(e: unknown): string {
  if (e instanceof HttpError) return `${e.code}: ${e.message}${e.correlationId ? ` [${e.correlationId}]` : ''}`;
  return e instanceof Error ? e.message : String(e);
}

export function Loading({ error }: { error?: string | null }) {
  const { t } = useI18n();
  return error ? <p role="alert" className="error">{t('common.error', { message: error })}</p> : <p aria-busy="true">{t('common.loading')}</p>;
}

export function Page({ title, children, actions }: { title: string; children: ReactNode; actions?: ReactNode }) {
  return (
    <section className="page">
      <header className="page-head">
        <h1>{title}</h1>
        {actions && <div className="page-actions">{actions}</div>}
      </header>
      {children}
    </section>
  );
}

export function Card({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="card">
      <h2>{title}</h2>
      {children}
    </div>
  );
}

const TONE: Record<string, 'ok' | 'warn' | 'bad' | 'neutral'> = {
  OK: 'ok', PASS: 'ok', MATCH: 'ok', IN_SYNC: 'ok', CERTIFIED: 'ok', CAPTURING: 'ok', LOADED: 'ok', EXECUTED: 'ok', APPLIED: 'ok', APPROVED: 'ok', SWITCHED: 'ok', DONE: 'ok', ACCEPTED: 'ok', ALELT: 'ok', TEST: 'ok', SHADOW: 'ok', READY: 'ok', DISCOVERED: 'ok',
  DEGRADED: 'warn', warning: 'warn', PENDING: 'warn', PENDING_APPROVAL: 'warn', CATCHING_UP: 'warn', SNAPSHOTTING: 'warn', IN_PROGRESS: 'warn', PAUSED: 'warn', PAUSED_BY_GOVERNOR: 'warn', RECEIVED: 'warn', NEW: 'warn', OPEN: 'warn', PLANNED: 'warn', RUNNING: 'warn',
  DOWN: 'bad', critical: 'bad', FAIL: 'bad', MISMATCH: 'bad', ERROR: 'bad', FAILED: 'bad', REJECTED: 'bad', SPOOL_FULL: 'bad', OFFLINE: 'bad', REVOKED: 'bad', ROLLED_BACK: 'bad', PROD: 'bad', blocker: 'bad', NEEDS_SECRETS: 'warn', GOLDEN_ONLY: 'warn', UNKNOWN: 'warn',
};
export function Badge({ value }: { value: string }) {
  return <span className={`badge badge-${TONE[value] ?? 'neutral'}`}>{value}</span>;
}

export function Stat({ label, value, tone }: { label: string; value: ReactNode; tone?: 'ok' | 'warn' | 'bad' }) {
  return (
    <div className={`stat ${tone ? `stat-${tone}` : ''}`}>
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
    </div>
  );
}

export function Table({ head, children, label }: { head: ReactNode[]; children: ReactNode; label?: string }) {
  return (
    <div className="table-wrap">
      <table aria-label={label}>
        <thead>
          <tr>{head.map((h, i) => <th key={i} scope="col">{h}</th>)}</tr>
        </thead>
        <tbody>{children}</tbody>
      </table>
    </div>
  );
}

export function Notice({ text, onClose }: { text: string | null; onClose: () => void }) {
  if (!text) return null;
  return (
    <div role="status" className="notice">
      {text} <button className="link" onClick={onClose} aria-label="close">×</button>
    </div>
  );
}

/**
 * Confirmation dialog for mutating actions. Always asks for a reason (audit); destructive actions additionally
 * require typing `confirmText` exactly (same rule the hub validates).
 */
export function ConfirmDialog(props: {
  open: boolean;
  title: string;
  confirmText?: string;
  children?: ReactNode;
  onCancel: () => void;
  onConfirm: (reason: string, typed: string) => Promise<void>;
}) {
  const { t } = useI18n();
  const [reason, setReason] = useState('');
  const [typed, setTyped] = useState('');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const rid = useId();
  const cid = useId();
  useEffect(() => {
    if (props.open) {
      setReason('');
      setTyped('');
      setErr(null);
    }
  }, [props.open]);
  if (!props.open) return null;
  const textOk = !props.confirmText || typed === props.confirmText;
  const ok = reason.trim().length > 0 && textOk && !busy;
  return (
    <div className="modal-backdrop" role="presentation">
      <div className="modal" role="dialog" aria-modal="true" aria-labelledby={`${rid}-title`}>
        <h2 id={`${rid}-title`}>{props.confirmText ? t('confirm.title') : props.title}</h2>
        {props.confirmText && <p className="modal-sub">{props.title}</p>}
        {props.children}
        <label htmlFor={rid}>{t('common.reason')}</label>
        <textarea id={rid} value={reason} onChange={(e) => setReason(e.target.value)} rows={2} required />
        {props.confirmText && (
          <>
            <label htmlFor={cid}>{t('confirm.typeToConfirm', { text: props.confirmText })}</label>
            <input id={cid} value={typed} onChange={(e) => setTyped(e.target.value)} autoComplete="off" spellCheck={false} data-testid="confirm-text" />
            {typed && !textOk && <p className="error">{t('confirm.mismatch')}</p>}
          </>
        )}
        {err && <p role="alert" className="error">{err}</p>}
        <div className="modal-actions">
          <button onClick={props.onCancel}>{t('common.cancel')}</button>
          <button
            className={props.confirmText ? 'danger' : 'primary'}
            disabled={!ok}
            onClick={async () => {
              setBusy(true);
              setErr(null);
              try {
                await props.onConfirm(reason.trim(), typed);
              } catch (e) {
                setErr(errorText(e));
              } finally {
                setBusy(false);
              }
            }}
          >
            {t('common.confirm')}
          </button>
        </div>
      </div>
    </div>
  );
}
