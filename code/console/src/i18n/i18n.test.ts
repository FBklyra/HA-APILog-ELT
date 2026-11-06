import { describe, expect, it } from 'vitest';
import { en } from './en';
import { format } from './format';
import { DICTS, detectLang, LANGS, localeFor, persistLang, resolveInitialLang, STORAGE_KEY, translate, type Lang } from './index';

const placeholders = (s: string) => [...s.matchAll(/\{(\w+)\}/g)].map((m) => m[1]).sort();
const langs = (Object.keys(DICTS) as Lang[]).sort();
const others = langs.filter((l) => l !== 'en');

describe('i18n EN/FR/ES/PT-BR', () => {
  it('registers all four languages in the picker', () => {
    expect(langs).toEqual(['en', 'es', 'fr', 'pt-BR']);
    expect(LANGS.map((l) => l.code).sort()).toEqual(langs);
    expect(LANGS.find((l) => l.code === 'es')?.label).toBe('Español');
    expect(LANGS.find((l) => l.code === 'pt-BR')?.label).toBe('Português (Brasil)');
  });
  it.each(others)('%s defines exactly the EN keys, none empty', (lang) => {
    const dict = DICTS[lang];
    expect(Object.keys(dict).sort()).toEqual(Object.keys(en).sort());
    for (const [k, v] of Object.entries(dict)) expect(v.trim(), `${lang}:${k}`).not.toBe('');
  });
  it.each(others)('%s preserves EN placeholders', (lang) => {
    for (const k of Object.keys(en) as (keyof typeof en)[]) expect(placeholders(DICTS[lang][k]), `${lang}:${k}`).toEqual(placeholders(en[k]));
  });
  it('stays vendor-neutral in every language (no clone wording, no third-party pipeline names)', () => {
    const banned = new RegExp(['clon', 'five' + 'tran'].join('|'));
    for (const lang of langs) for (const [k, v] of Object.entries(DICTS[lang])) expect(v.toLowerCase(), `${lang}:${k}`).not.toMatch(banned);
  });
  it('detects browser language with EN fallback', () => {
    expect(detectLang(['es-MX'])).toBe('es');
    expect(detectLang(['pt-PT', 'en'])).toBe('pt-BR');
    expect(detectLang(['fr-FR'])).toBe('fr');
    expect(detectLang(['de-DE', 'ja'])).toBe('en');
    expect(detectLang(['de-DE', 'pt-BR'])).toBe('pt-BR');
    expect(detectLang([])).toBe('en');
  });
  it('first visit defaults to the browser language, switch persists across reload', () => {
    const store = new Map<string, string>();
    const storage = { getItem: (k: string) => store.get(k) ?? null, setItem: (k: string, v: string) => void store.set(k, v) };
    // first visit: nothing saved -> browser language
    expect(resolveInitialLang(storage, ['pt-BR', 'en'])).toBe('pt-BR');
    expect(translate('pt-BR', 'nav.overview')).toBe('Visão geral');
    // user switches to Spanish
    persistLang(storage, 'es');
    expect(store.get(STORAGE_KEY)).toBe('es');
    expect(translate('es', 'nav.overview')).toBe('Resumen');
    // reload: saved choice wins over browser language
    expect(resolveInitialLang(storage, ['pt-BR', 'en'])).toBe('es');
    // garbage in storage -> browser language; unsupported browser language -> EN
    store.set(STORAGE_KEY, 'xx');
    expect(resolveInitialLang(storage, ['de-DE'])).toBe('en');
    // storage that throws (privacy mode) still resolves
    const broken = { getItem: () => { throw new Error('denied'); }, setItem: () => { throw new Error('denied'); } };
    expect(resolveInitialLang(broken, ['fr-CA'])).toBe('fr');
    expect(() => persistLang(broken, 'en')).not.toThrow();
  });
  it('formats numbers via Intl for the active locale', () => {
    const n = (lang: Lang) => new Intl.NumberFormat(localeFor(lang)).format(1234.5);
    expect(n('en')).toBe('1,234.5');
    expect(n('pt-BR')).toBe('1.234,5');
    expect(localeFor('es')).toBe('es-419');
  });
  it('format substitutes and leaves unknown placeholders', () => {
    expect(format('Type {text} now {x}', { text: 'PAUSE a' })).toBe('Type PAUSE a now {x}');
  });
});
