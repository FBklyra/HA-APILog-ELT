import { useRef, useState, type ReactNode } from 'react';
import type { CheckItem, Environment, TestStep } from '../api/contract';
import { useI18n } from '../i18n';
import { Badge } from './ui';

/** Step indicator; completed steps are clickable to go back. */
export function Stepper({ steps, current, onGo }: { steps: string[]; current: number; onGo?: (i: number) => void }) {
  return (
    <ol className="stepper" aria-label="steps">
      {steps.map((s, i) => (
        <li key={s} className={i === current ? 'current' : i < current ? 'done' : ''} aria-current={i === current ? 'step' : undefined}>
          {i < current && onGo ? <button className="link" onClick={() => onGo(i)}>{i + 1}. {s}</button> : <span>{i + 1}. {s}</span>}
        </li>
      ))}
    </ol>
  );
}

export function EnvBadge({ env }: { env: Environment }) {
  return <span data-testid="env-badge"><Badge value={env} /></span>;
}

export interface DroppedFile { name: string; data_base64: string; size: number }

function readBase64(f: File): Promise<DroppedFile> {
  return new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onerror = () => reject(r.error ?? new Error('read failed'));
    r.onload = () => {
      const url = String(r.result ?? '');
      resolve({ name: f.name, data_base64: url.slice(url.indexOf(',') + 1), size: f.size });
    };
    r.readAsDataURL(f);
  });
}

/** Drag-and-drop zone (also click to browse). Files are read in the browser as base64; nothing is uploaded until the caller sends them. */
export function DropZone({ files, onFiles, hint }: { files: DroppedFile[]; onFiles: (f: DroppedFile[]) => void; hint?: string }) {
  const { t } = useI18n();
  const input = useRef<HTMLInputElement>(null);
  const [over, setOver] = useState(false);
  const add = async (list: FileList | null) => {
    if (!list?.length) return;
    const read = await Promise.all([...list].map(readBase64));
    const byName = new Map(files.map((f) => [f.name, f]));
    for (const f of read) byName.set(f.name, f);
    onFiles([...byName.values()]);
  };
  return (
    <div>
      <div className={`dropzone ${over ? 'over' : ''}`} role="button" tabIndex={0} data-testid="dropzone"
        onClick={() => input.current?.click()} onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') input.current?.click(); }}
        onDragOver={(e) => { e.preventDefault(); setOver(true); }} onDragLeave={() => setOver(false)}
        onDrop={(e) => { e.preventDefault(); setOver(false); void add(e.dataTransfer.files); }}>
        <strong>{t('wiz.drop.title')}</strong>
        <p>{hint || t('wiz.drop.hint')}</p>
        <input ref={input} type="file" multiple hidden accept=".json,.zip,.sql,.ddl,.csv,.yaml,.yml,.txt" onChange={(e) => void add(e.target.files)} />
      </div>
      {files.length > 0 && (
        <ul className="file-list">
          {files.map((f) => (
            <li key={f.name}>
              <code>{f.name}</code> ({Math.ceil(f.size / 1024)} KB){' '}
              <button className="link" onClick={() => onFiles(files.filter((x) => x.name !== f.name))} aria-label={t('wiz.drop.remove')}>×</button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export function CopyButton({ text }: { text: string }) {
  const { t } = useI18n();
  const [done, setDone] = useState(false);
  return (
    <button onClick={async () => {
      try { await navigator.clipboard.writeText(text); setDone(true); setTimeout(() => setDone(false), 1500); } catch { /* clipboard unavailable */ }
    }}>{done ? t('wiz.copied') : t('wiz.copy')}</button>
  );
}

export function Steps({ steps }: { steps: TestStep[] }) {
  return (
    <ul className="checklist">
      {steps.map((s) => (
        <li key={s.step} className={s.ok ? (s.pending ? 'pending' : 'ok') : 'bad'}>
          <Badge value={s.ok ? (s.pending ? 'PENDING' : 'OK') : 'FAIL'} /> <strong>{s.step}</strong>: {s.detail}
        </li>
      ))}
    </ul>
  );
}

/** Prerequisite checklist. SQL is displayed for the DBA to run; ALELT never executes it (R1). */
export function Checklist({ checks }: { checks: CheckItem[] }) {
  const { t } = useI18n();
  if (!checks.length) return null;
  return (
    <div>
      <p className="hint">{t('wiz.prereq.note')}</p>
      <ul className="checklist">
        {checks.map((c) => (
          <li key={c.id} data-testid={`check-${c.id}`}>
            <Badge value={c.status === 'UNKNOWN' ? 'PENDING' : c.status} /> {c.label}{c.detail ? ` — ${c.detail}` : ''}
            {c.check_sql && (
              <div className="sql"><span>{t('wiz.prereq.check')}</span><pre>{c.check_sql}</pre><CopyButton text={c.check_sql} /></div>
            )}
            {c.fix_sql && c.status !== 'PASS' && (
              <div className="sql"><span>{t('wiz.prereq.fix')}</span><pre>{c.fix_sql}</pre><CopyButton text={c.fix_sql} /></div>
            )}
          </li>
        ))}
      </ul>
    </div>
  );
}

export function Field({ label, children, help }: { label: string; children: ReactNode; help?: string }) {
  return (
    <label className="field">
      <span>{label}</span>
      {children}
      {help && <small className="hint">{help}</small>}
    </label>
  );
}

export function NavButtons({ onBack, onNext, nextLabel, nextDisabled, busy }: {
  onBack?: () => void; onNext?: () => void; nextLabel?: string; nextDisabled?: boolean; busy?: boolean;
}) {
  const { t } = useI18n();
  return (
    <div className="wizard-nav">
      {onBack && <button onClick={onBack}>{t('wiz.back')}</button>}
      {onNext && <button className="primary" data-testid="wizard-next" disabled={nextDisabled || busy} onClick={onNext}>{busy ? t('common.loading') : nextLabel ?? t('wiz.next')}</button>}
    </div>
  );
}
