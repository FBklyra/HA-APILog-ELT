import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import { ApiClient, fetchTransport } from '../api/client';
import type { Me, Role } from '../api/contract';
import { ROLES } from '../api/contract';
import { createMockTransport } from '../api/mock';
import { API_BASE_URL, DEV_AUTH, USE_MOCK, msalConfig } from './config';
import { accessToken, handleCallback, signIn, signOut } from './oidc';

interface AuthCtx {
  api: ApiClient;
  /** Auth headers for calls outside the typed contract (setup wizard, support bundle download). */
  authHeaders: () => Promise<Record<string, string>>;
  me: Me | null;
  error: string | null;
  dev: boolean;
  devRole: Role;
  devUser: string;
  setDevIdentity: (role: Role, user: string) => void;
  signIn: () => void;
  signOut: () => void;
}
const Ctx = createContext<AuthCtx | null>(null);

const transport = USE_MOCK ? createMockTransport(undefined, 150) : fetchTransport(API_BASE_URL);

function loadDevRole(): Role {
  const r = localStorage.getItem('alelt.dev.role');
  return (ROLES as readonly string[]).includes(r ?? '') ? (r as Role) : 'Viewer';
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [devRole, setDevRole] = useState<Role>(loadDevRole);
  const [devUser, setDevUser] = useState<string>(() => localStorage.getItem('alelt.dev.user') ?? '');
  const [me, setMe] = useState<Me | null>(null);
  const [error, setError] = useState<string | null>(null);
  const cfg = useMemo(() => msalConfig(), []);

  const authHeaders = useCallback(async (): Promise<Record<string, string>> => {
    if (DEV_AUTH) return { 'X-ALELT-Dev-Role': devRole, ...(devUser ? { 'X-ALELT-Dev-User': devUser } : {}) };
    const tok = accessToken();
    return tok ? { Authorization: `Bearer ${tok}` } : {};
  }, [devRole, devUser]);
  const api = useMemo(() => new ApiClient(transport, authHeaders), [authHeaders]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        if (!DEV_AUTH) {
          await handleCallback(cfg);
          if (!accessToken()) {
            setMe(null);
            return;
          }
        }
        const m = await api.call('GET /me');
        if (!cancelled) {
          setMe(m);
          setError(null);
        }
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [api, cfg]);

  const setDevIdentity = useCallback((role: Role, user: string) => {
    localStorage.setItem('alelt.dev.role', role);
    localStorage.setItem('alelt.dev.user', user);
    setDevRole(role);
    setDevUser(user);
  }, []);

  const value: AuthCtx = {
    api, authHeaders, me, error, dev: DEV_AUTH, devRole, devUser, setDevIdentity,
    signIn: () => void signIn(cfg),
    signOut: () => (DEV_AUTH ? setMe(null) : signOut(cfg)),
  };
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useAuth(): AuthCtx {
  const c = useContext(Ctx);
  if (!c) throw new Error('useAuth outside AuthProvider');
  return c;
}
