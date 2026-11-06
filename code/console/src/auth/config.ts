/**
 * Entra ID (OIDC) configuration. Same shape as an MSAL `Configuration` so `@azure/msal-browser` can be dropped in
 * later without changing call sites; the built-in client below implements auth-code + PKCE with no extra dependency.
 * Client/tenant ids are public identifiers, not secrets. The SPA never holds a client secret.
 */
export interface MsalLikeConfig {
  auth: { clientId: string; authority: string; redirectUri: string; postLogoutRedirectUri: string };
  cache: { cacheLocation: 'sessionStorage' };
  scopes: string[];
}

const env = import.meta.env;

/** Runtime settings served by the hub at /config.js (window.__ALELT_CONFIG__); they override build-time defaults so
 *  one image serves dev and production. Absent (dev server, tests) = build-time env only. */
interface RuntimeConfig {
  devAuth?: boolean;
  mock?: boolean;
  apiBase?: string;
  entraTenantId?: string;
  entraClientId?: string;
  entraApiScope?: string;
}
const rt: RuntimeConfig =
  (typeof window !== 'undefined' ? (window as unknown as { __ALELT_CONFIG__?: RuntimeConfig }).__ALELT_CONFIG__ : undefined) ?? {};

export const DEV_AUTH: boolean = rt.devAuth ?? (env.VITE_ALELT_DEV_AUTH === '1' || (env.DEV && env.VITE_ALELT_DEV_AUTH !== '0'));
export const USE_MOCK: boolean = rt.mock ?? (env.VITE_ALELT_MOCK === '1' || (env.DEV && env.VITE_ALELT_MOCK !== '0'));
export const API_BASE_URL: string = rt.apiBase ?? (env.VITE_ALELT_API_BASE as string | undefined) ?? '/api/v1';

export function msalConfig(origin: string = window.location.origin): MsalLikeConfig {
  const tenant = rt.entraTenantId || ((env.VITE_ENTRA_TENANT_ID as string | undefined) ?? 'organizations');
  return {
    auth: {
      clientId: rt.entraClientId || ((env.VITE_ENTRA_CLIENT_ID as string | undefined) ?? ''),
      authority: `https://login.microsoftonline.com/${tenant}`,
      redirectUri: `${origin}/auth/callback`,
      postLogoutRedirectUri: origin,
    },
    cache: { cacheLocation: 'sessionStorage' },
    scopes: ['openid', 'profile', rt.entraApiScope || ((env.VITE_ENTRA_API_SCOPE as string | undefined) ?? 'api://alelt-hub/Console.Access')],
  };
}
