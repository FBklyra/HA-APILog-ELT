import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from 'react';
import { en, type MessageKey } from './en';
import { es } from './es';
import { format } from './format';
import { fr } from './fr';
import { ptBR } from './pt-BR';

export { format };

export type Lang = 'en' | 'fr' | 'es' | 'pt-BR';
export type { MessageKey };
export const DICTS: Record<Lang, Record<MessageKey, string>> = { en, fr, es, 'pt-BR': ptBR };

/** Supported languages, in picker order. `label` is the native name (never translated); `locale` drives Intl and <html lang>. */
export const LANGS: readonly { code: Lang; label: string; locale: string }[] = [
  { code: 'en', label: 'English', locale: 'en-CA' },
  { code: 'fr', label: 'Français', locale: 'fr-CA' },
  { code: 'es', label: 'Español', locale: 'es-419' },
  { code: 'pt-BR', label: 'Português (Brasil)', locale: 'pt-BR' },
];

export const isLang = (v: unknown): v is Lang => LANGS.some((l) => l.code === v);
export const localeFor = (lang: Lang): string => LANGS.find((l) => l.code === lang)?.locale ?? 'en-CA';

export function translate(lang: Lang, key: MessageKey, vars?: Record<string, string | number>): string {
  return format(DICTS[lang][key] || en[key], vars);
}

/** Map browser language tags (e.g. 'es-MX', 'pt-PT', 'fr-FR') to a supported language; EN if none match. */
export function detectLang(tags: readonly string[]): Lang {
  for (const tag of tags) {
    const base = tag.toLowerCase().split('-')[0];
    if (base === 'en') return 'en';
    if (base === 'fr') return 'fr';
    if (base === 'es') return 'es';
    if (base === 'pt') return 'pt-BR';
  }
  return 'en';
}

interface I18nCtx {
  lang: Lang;
  setLang: (l: Lang) => void;
  t: (key: MessageKey, vars?: Record<string, string | number>) => string;
  locale: string;
}
const Ctx = createContext<I18nCtx | null>(null);
export const STORAGE_KEY = 'alelt.lang';
type LangStorage = Pick<Storage, 'getItem' | 'setItem'>;

/**
 * Saved choice wins; otherwise the browser language (first visit); otherwise EN.
 * The hub has no user-preference endpoint (GET /me only), so the choice lives in localStorage.
 */
export function resolveInitialLang(storage: LangStorage, browserTags: readonly string[]): Lang {
  let saved: string | null = null;
  try { saved = storage.getItem(STORAGE_KEY); } catch { /* storage unavailable */ }
  return isLang(saved) ? saved : detectLang(browserTags);
}

export function persistLang(storage: LangStorage, lang: Lang): void {
  try { storage.setItem(STORAGE_KEY, lang); } catch { /* storage unavailable: choice lasts for the session */ }
}

const browserTags = (): readonly string[] =>
  navigator.languages?.length ? navigator.languages : [navigator.language ?? ''];

export function I18nProvider({ children }: { children: ReactNode }) {
  const [lang, setLangState] = useState<Lang>(() => resolveInitialLang(localStorage, browserTags()));
  const setLang = useCallback((l: Lang) => {
    persistLang(localStorage, l);
    setLangState(l);
  }, []);
  const locale = localeFor(lang);
  document.documentElement.lang = locale;
  const value = useMemo<I18nCtx>(
    () => ({ lang, setLang, t: (k, v) => translate(lang, k, v), locale }),
    [lang, setLang, locale],
  );
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useI18n(): I18nCtx {
  const c = useContext(Ctx);
  if (!c) throw new Error('useI18n outside I18nProvider');
  return c;
}
