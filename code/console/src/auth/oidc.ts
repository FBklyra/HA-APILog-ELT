import type { MsalLikeConfig } from './config';

/** Minimal OIDC auth-code + PKCE client for Entra ID (SPA platform redirect URI). Tokens live in sessionStorage only. */
const K = { verifier: 'alelt.pkce.verifier', state: 'alelt.pkce.state', token: 'alelt.token', exp: 'alelt.token.exp' } as const;

function b64url(bytes: Uint8Array): string {
  let s = '';
  bytes.forEach((b) => (s += String.fromCharCode(b)));
  return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}
function randomString(n = 32): string {
  return b64url(crypto.getRandomValues(new Uint8Array(n)));
}
export async function pkceChallenge(verifier: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier));
  return b64url(new Uint8Array(digest));
}

export async function signIn(cfg: MsalLikeConfig): Promise<void> {
  const verifier = randomString(48);
  const state = randomString(16);
  sessionStorage.setItem(K.verifier, verifier);
  sessionStorage.setItem(K.state, state);
  const p = new URLSearchParams({
    client_id: cfg.auth.clientId,
    response_type: 'code',
    redirect_uri: cfg.auth.redirectUri,
    scope: cfg.scopes.join(' '),
    state,
    code_challenge: await pkceChallenge(verifier),
    code_challenge_method: 'S256',
    response_mode: 'query',
  });
  window.location.assign(`${cfg.auth.authority}/oauth2/v2.0/authorize?${p.toString()}`);
}

/** Completes the redirect on /auth/callback. Returns true when a token was obtained. */
export async function handleCallback(cfg: MsalLikeConfig, url: URL = new URL(window.location.href)): Promise<boolean> {
  const code = url.searchParams.get('code');
  if (!code) return false;
  const state = url.searchParams.get('state');
  const verifier = sessionStorage.getItem(K.verifier);
  if (!verifier || state !== sessionStorage.getItem(K.state)) throw new Error('OIDC state mismatch');
  sessionStorage.removeItem(K.verifier);
  sessionStorage.removeItem(K.state);
  const res = await fetch(`${cfg.auth.authority}/oauth2/v2.0/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: cfg.auth.clientId,
      grant_type: 'authorization_code',
      code,
      redirect_uri: cfg.auth.redirectUri,
      code_verifier: verifier,
      scope: cfg.scopes.join(' '),
    }),
  });
  if (!res.ok) throw new Error(`token endpoint ${res.status}`);
  const tok = (await res.json()) as { access_token: string; expires_in: number };
  sessionStorage.setItem(K.token, tok.access_token);
  sessionStorage.setItem(K.exp, String(Date.now() + (tok.expires_in - 60) * 1000));
  window.history.replaceState(null, '', '/');
  return true;
}

export function accessToken(): string | null {
  const exp = Number(sessionStorage.getItem(K.exp) ?? 0);
  return exp > Date.now() ? sessionStorage.getItem(K.token) : null;
}

export function signOut(cfg: MsalLikeConfig): void {
  sessionStorage.removeItem(K.token);
  sessionStorage.removeItem(K.exp);
  window.location.assign(`${cfg.auth.authority}/oauth2/v2.0/logout?post_logout_redirect_uri=${encodeURIComponent(cfg.auth.postLogoutRedirectUri)}`);
}
