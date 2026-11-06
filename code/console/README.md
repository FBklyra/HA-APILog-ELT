# alelt-console

React 19 + TypeScript (strict) + Vite. Served as static files by the hub's nginx (`deploy/hub/nginx`); talks to `/api/v1`.

| Command | What |
|---|---|
| `npm ci` | install (lockfile) |
| `npm run dev` | dev server :5173 — mock API + dev role picker by default |
| `npm run typecheck` / `npm test` / `npm run build` | tsc strict · Vitest · `dist/` |
| `npm run e2e` | Playwright specs in `e2e/` against the mock (installs `@playwright/test` on demand; `npx playwright install chromium` once) |

| Env (`.env.example`) | Meaning |
|---|---|
| `VITE_ALELT_MOCK` | `1` in-browser mock hub (`src/api/mock.ts`), `0` real hub (dev server proxies `/api` to `ALELT_HUB_URL`, default `http://localhost:8080`) |
| `VITE_ALELT_DEV_AUTH` | `1` role picker → `X-ALELT-Dev-Role`/`X-ALELT-Dev-User` (hub accepts only with `ALELT_DEV_AUTH=1`); `0` Entra ID OIDC (auth code + PKCE) |
| `VITE_ENTRA_TENANT_ID`, `VITE_ENTRA_CLIENT_ID`, `VITE_ENTRA_API_SCOPE` | public identifiers of the SPA registration (no secrets) |

Layout: `src/api` (contract, client, mock, fixtures) · `src/auth` (OIDC config/client, provider) · `src/lib` (RBAC, two-person rule, formatting) · `src/i18n` (EN, FR-CA, ES, PT-BR) · `src/screens` (13 screens of prompt 10) · `e2e/` (Playwright per role, switch two-person rule, kill switch).
API contract for the hub: [`API_CONTRACT.md`](API_CONTRACT.md).

## Languages

| Code | Picker label | Intl locale | Dictionary |
|---|---|---|---|
| `en` | English | `en-CA` | `src/i18n/en.ts` (source of truth for keys) |
| `fr` | Français | `fr-CA` | `src/i18n/fr.ts` |
| `es` | Español | `es-419` (neutral Latin-American) | `src/i18n/es.ts` |
| `pt-BR` | Português (Brasil) | `pt-BR` | `src/i18n/pt-BR.ts` |

- Picker is always in the header; switching re-renders instantly (no reload). Numbers/dates use `Intl` with the active locale; `<html lang>` follows it.
- First visit: first supported browser language (`navigator.languages`; any `pt-*` → `pt-BR`), else EN. The choice is saved in `localStorage` (`alelt.lang`); the hub has no user-preference endpoint, so it is not sent server-side.
- Adding a language: new dictionary typed `Record<MessageKey, string>`, entry in `DICTS` and `LANGS` (`src/i18n/index.tsx`). `src/i18n/i18n.test.ts` enforces for every language: same keys as EN, no empty strings, same `{placeholders}`, vendor-neutral wording.

Note (Windows): npm `.cmd` shims break when the repo path contains `&` (e.g. `C:\src\R&D\…`). Run tools directly there: `node node_modules/typescript/bin/tsc --noEmit -p tsconfig.json`, `node node_modules/vitest/vitest.mjs run`, `node node_modules/vite/bin/vite.js build`.
