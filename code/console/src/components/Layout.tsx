import { useEffect, useState, type ReactNode } from 'react';
import { NavLink } from 'react-router';
import { ROLES, type Role } from '../api/contract';
import { useAuth } from '../auth/AuthProvider';
import { isLang, LANGS, useI18n } from '../i18n';
import { DEV_AUTH, USE_MOCK } from '../auth/config';
import { can, navFor, type Permission } from '../lib/rbac';

export type Theme = 'light' | 'dark';
function initialTheme(): Theme {
  const s = localStorage.getItem('alelt.theme');
  if (s === 'light' || s === 'dark') return s;
  return window.matchMedia?.('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

export function Layout({ children }: { children: ReactNode }) {
  const { t, lang, setLang } = useI18n();
  const { me, dev, devRole, devUser, setDevIdentity, signOut } = useAuth();
  const [theme, setTheme] = useState<Theme>(initialTheme);
  const [userDraft, setUserDraft] = useState(devUser);
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem('alelt.theme', theme);
  }, [theme]);

  const roles = me?.roles ?? [];
  return (
    <div className="shell">
      <a className="skip" href="#main">Skip to content</a>
      <header className="topbar">
        <div className="brand">
          <strong>{t('app.title')}</strong>
          <span className="tagline">{t('app.tagline')}</span>
        </div>
        <div className="topbar-actions">
          {dev && (
            <div className="dev-picker" title={t('auth.devWarning')}>
              <label htmlFor="dev-role">{t('auth.devMode')}</label>
              <select id="dev-role" data-testid="dev-role" value={devRole} onChange={(e) => setDevIdentity(e.target.value as Role, userDraft)}>
                {ROLES.map((r) => <option key={r} value={r}>{r}</option>)}
              </select>
              <input aria-label="dev user" data-testid="dev-user" placeholder="user@dev.local" value={userDraft}
                onChange={(e) => setUserDraft(e.target.value)} onBlur={() => setDevIdentity(devRole, userDraft.trim())} />
            </div>
          )}
          <button onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')} aria-label={t('theme.toggle')} title={t('theme.toggle')}>
            {theme === 'dark' ? '☀' : '☾'}
          </button>
          <select aria-label={t('lang.toggle')} title={t('lang.toggle')} data-testid="lang-picker" value={lang}
            onChange={(e) => { if (isLang(e.target.value)) setLang(e.target.value); }}>
            {LANGS.map((l) => <option key={l.code} value={l.code} lang={l.locale}>{l.label}</option>)}
          </select>
          {me && <span className="whoami" data-testid="whoami">{me.email} · {me.roles.join(', ')}</span>}
          {me && !dev && <button onClick={signOut}>{t('auth.signOut')}</button>}
        </div>
      </header>
      <div className="body">
        <nav aria-label="main" className="sidenav">
          <ul>
            {navFor(roles, DEV_AUTH || USE_MOCK).map((n) => (
              <li key={n.path}>
                <NavLink to={n.path} end={n.path === '/'}>{t(n.labelKey)}</NavLink>
              </li>
            ))}
          </ul>
        </nav>
        <main id="main">{children}</main>
      </div>
    </div>
  );
}

/** Route guard: renders children only if the current user holds `p`. */
export function Guard({ p, children }: { p: Permission; children: ReactNode }) {
  const { me } = useAuth();
  const { t } = useI18n();
  if (!me) return null;
  return can(me.roles, p) ? <>{children}</> : <p role="alert" className="error" data-testid="forbidden">{t('common.forbidden')}</p>;
}

/** Renders children only when the user holds `p` (for buttons/actions). */
export function IfCan({ p, children }: { p: Permission; children: ReactNode }) {
  const { me } = useAuth();
  return me && can(me.roles, p) ? <>{children}</> : null;
}
