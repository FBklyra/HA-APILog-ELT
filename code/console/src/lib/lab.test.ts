// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';
import {
  HUB_HOST, amd64Only, demoPipelineBody, guidanceSettings, isLoopbackUrl, labCommands, labEnvFile, orderedSelection,
  parseHelperFragment,
} from './lab';
import { LAB_CATALOG } from './labCatalog.generated';

const byId = (id: string) => {
  const p = LAB_CATALOG.profiles.find((x) => x.id === id);
  if (!p) throw new Error(`no profile ${id}`);
  return p;
};

describe('lab catalog (generated from lab/catalog.yaml)', () => {
  it('has unique ids, the landing target and guidance for every no_image entry', () => {
    const ids = LAB_CATALOG.profiles.map((p) => p.id);
    expect(new Set(ids).size).toBe(ids.length);
    expect(byId('landing').kind).toBe('target');
    for (const p of LAB_CATALOG.profiles) {
      expect(['stable', 'beta', 'planned']).toContain(p.status);
      if (p.availability === 'no_image') {
        expect(p.guidance, p.id).toBeTruthy();
        expect(p.guidance?.signup_url, p.id).toMatch(/^https:\/\//);
        expect(p.guidance?.docs_url, p.id).toMatch(/^https:\/\//);
        expect(p.guidance?.objects.length, p.id).toBeGreaterThan(0);
        expect(p.guidance?.test_engine, p.id).toBeTruthy();
      } else {
        expect(p.arch?.length, p.id).toBeGreaterThan(0);
      }
    }
  });
  it('uses only lab ports outside the usual Windows reserved range 54714-55513', () => {
    for (const p of LAB_CATALOG.profiles) for (const port of Object.values(p.ports ?? {})) {
      expect(port < 54714 || port > 55513, `${p.id}:${port}`).toBe(true);
    }
  });
  it('names the project and network alelt-lab', () => {
    expect(LAB_CATALOG.project).toBe('alelt-lab');
    expect(LAB_CATALOG.network).toBe('alelt-lab');
  });
});

describe('lab commands and lab.env', () => {
  it('keeps catalog order, drops unknown ids and cloud-only entries', () => {
    expect(orderedSelection(['landing', 'nope', 'postgres', 'postgres'])).toEqual(['postgres', 'landing']);
    const noImage = LAB_CATALOG.profiles.find((p) => p.availability === 'no_image');
    const c = labCommands(['landing', 'postgres', ...(noImage ? [noImage.id] : [])]);
    expect(c.sh).toEqual([
      './lab/lab.sh pull postgres landing', './lab/lab.sh up postgres landing', './lab/lab.sh status',
      './lab/lab.sh down postgres landing', './lab/lab.sh reset postgres landing',
    ]);
    expect(c.ps1[1]).toBe('.\\lab\\lab.ps1 up postgres landing');
    expect(c.compose).toBe('docker compose -p alelt-lab -f lab/docker-compose.lab.yml --env-file lab.env up -d');
  });
  it('generates lab.env with Compose profiles', () => {
    const env = labEnvFile(['mysql', 'postgres'], LAB_CATALOG, new Date('2026-11-04T00:00:00Z'));
    expect(env).toContain('COMPOSE_PROFILES=postgres,mysql\n');
    expect(env).toContain('COMPOSE_PROJECT_NAME=alelt-lab\n');
    expect(env.split('\n').filter((l) => l && !l.startsWith('#'))).toEqual(['COMPOSE_PROJECT_NAME=alelt-lab', 'COMPOSE_PROFILES=postgres,mysql']);
    expect(env.toLowerCase()).not.toContain('five' + 'tran');
  });
  it('flags amd64-only images', () => {
    expect(amd64Only(byId('postgres'))).toBe(false);
    const amd = LAB_CATALOG.profiles.find((p) => p.availability === 'image' && (p.images?.length ?? 0) > 0 && !(p.arch ?? []).includes('arm64'));
    if (amd) expect(amd64Only(amd)).toBe(true);
  });
});

describe('demo pipeline + guidance', () => {
  it('builds a TEST wizard body into the landing Postgres via host.docker.internal', () => {
    const b = demoPipelineBody(byId('postgres'), byId('landing'));
    expect(b.environment).toBe('TEST');
    expect(b.connection_id).toBe('lab_postgres');
    expect(b.connection_id).toMatch(/^[a-z][a-z0-9_]{0,62}$/);
    expect(b.source.host).toBe(HUB_HOST);
    expect(b.source).not.toHaveProperty('password');
    expect(b.password?.value).toBeTruthy();
    expect(b.destination_engine).toBe('postgres');
    expect(String(b.destination.url)).toContain(`@${HUB_HOST}:`);
  });
  it('drops placeholders and empty values from the guidance form', () => {
    expect(guidanceSettings({ account: '<org>-<account>', user: 'ALELT_SVC', port: '5439', empty: ' ' }))
      .toEqual({ user: 'ALELT_SVC', port: 5439 });
  });
});

describe('helper link', () => {
  it('accepts only loopback helpers from the fragment', () => {
    expect(parseHelperFragment('#helper=http://127.0.0.1:47999/&token=abc')).toEqual({ url: 'http://127.0.0.1:47999', token: 'abc' });
    expect(parseHelperFragment('#helper=http://evil.example:47999&token=abc')).toBeNull();
    expect(parseHelperFragment('#token=abc')).toBeNull();
    expect(isLoopbackUrl('http://localhost:47999')).toBe(true);
    expect(isLoopbackUrl('http://10.0.0.5:47999')).toBe(false);
    expect(isLoopbackUrl('file:///etc/passwd')).toBe(false);
  });
});
