// Permissions matrix per role (prompt 10). Mirrors src/lib/rbac.ts; the hub enforces the same matrix server-side.
import { expect, test } from '@playwright/test';
import { loginAs, type Role } from './helpers';

const ALL = ['Overview', 'Tables', 'Import existing schema', 'Config revisions', 'Schema changes', 'Reconciliation',
  'Certification & cutover', 'Agent', 'Hub & destination', 'Retention & storage', 'Users & roles', 'Audit'];

const VISIBLE: Record<Role, string[]> = {
  Viewer: ['Overview', 'Tables', 'Reconciliation'],
  DataOperator: ['Overview', 'Tables', 'Import existing schema', 'Config revisions', 'Schema changes', 'Reconciliation', 'Certification & cutover'],
  PlatformAdmin: ['Overview', 'Tables', 'Agent', 'Hub & destination', 'Retention & storage', 'Users & roles', 'Audit'],
  Auditor: ['Overview', 'Certification & cutover', 'Audit'],
};

for (const role of Object.keys(VISIBLE) as Role[]) {
  test.describe(`role ${role}`, () => {
    test('navigation shows exactly the permitted screens', async ({ page }) => {
      await loginAs(page, role);
      const nav = page.getByRole('navigation', { name: 'main' });
      for (const label of ALL) {
        const link = nav.getByRole('link', { name: label, exact: true });
        if (VISIBLE[role].includes(label)) await expect(link).toBeVisible();
        else await expect(link).toHaveCount(0);
      }
    });

    test('direct URL to a forbidden screen is refused', async ({ page }) => {
      await loginAs(page, role);
      const forbidden = role === 'PlatformAdmin' ? '/certification' : '/agent';
      await page.goto(forbidden);
      await expect(page.getByTestId('forbidden')).toBeVisible();
    });
  });
}

test('Viewer sees tables read-only (no config controls)', async ({ page }) => {
  await loginAs(page, 'Viewer');
  await page.goto('/tables/t-client');
  await expect(page.getByRole('button', { name: 'Save as new revision' })).toHaveCount(0);
  await expect(page.getByRole('button', { name: 'Re-sync table' })).toHaveCount(0);
});

test('Auditor exports audit CSV', async ({ page }) => {
  await loginAs(page, 'Auditor');
  await page.goto('/audit');
  const [download] = await Promise.all([page.waitForEvent('download'), page.getByRole('button', { name: 'Export CSV' }).click()]);
  expect(download.suggestedFilename()).toBe('alelt-audit.csv');
});

test('French UI strings (Quebec)', async ({ page }) => {
  await loginAs(page, 'DataOperator');
  await page.getByTestId('lang-picker').selectOption('fr');
  await expect(page.getByRole('link', { name: 'Vue d’ensemble' })).toBeVisible();
  await expect(page.locator('html')).toHaveAttribute('lang', 'fr-CA');
});

test.describe('language picker', () => {
  test.use({ locale: 'es-MX' });
  test('defaults to browser language, switches without reload, persists across reload', async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.setItem('alelt.dev.role', 'Viewer');
      localStorage.setItem('alelt.dev.user', 'viewer@dev.local');
    });
    await page.goto('/');
    await expect(page.getByRole('link', { name: 'Resumen' })).toBeVisible();
    await expect(page.locator('html')).toHaveAttribute('lang', 'es-419');
    const marker = await page.evaluate(() => ((window as unknown as { __aleltMarker: number }).__aleltMarker = Date.now()));
    await page.getByTestId('lang-picker').selectOption('pt-BR');
    await expect(page.getByRole('link', { name: 'Visão geral' })).toBeVisible();
    expect(await page.evaluate(() => (window as unknown as { __aleltMarker?: number }).__aleltMarker)).toBe(marker);
    await page.reload();
    await expect(page.getByRole('link', { name: 'Visão geral' })).toBeVisible();
    await expect(page.getByTestId('lang-picker')).toHaveValue('pt-BR');
  });
});

test.fixme('accessibility: no serious axe violations on every permitted screen', async () => {
  // Requires @axe-core/playwright (not a default dependency): new AxeBuilder({ page }).analyze() per screen,
  // assert violations.filter(v => ['serious','critical'].includes(v.impact)) is empty.
});
