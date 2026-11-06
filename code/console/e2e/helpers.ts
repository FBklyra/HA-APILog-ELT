import { expect, type Page } from '@playwright/test';

export type Role = 'Viewer' | 'DataOperator' | 'PlatformAdmin' | 'Auditor';

/** Select the dev identity (sent as X-ALELT-Dev-Role / X-ALELT-Dev-User to the mock hub). */
export async function loginAs(page: Page, role: Role, user = `${role.toLowerCase()}@dev.local`): Promise<void> {
  await page.addInitScript(([r, u]) => {
    localStorage.setItem('alelt.dev.role', r);
    localStorage.setItem('alelt.dev.user', u);
    localStorage.setItem('alelt.lang', 'en');
  }, [role, user] as const);
  await page.goto('/');
  await expect(page.getByTestId('whoami')).toContainText(role);
}

/** Fill a ConfirmDialog: reason, then the expected confirmation text when present. */
export async function confirm(page: Page, reason: string, confirmText?: string): Promise<void> {
  const dialog = page.getByRole('dialog');
  await dialog.getByLabel(/Reason/).fill(reason);
  if (confirmText) await dialog.getByTestId('confirm-text').fill(confirmText);
  await dialog.getByRole('button', { name: 'Confirm' }).click();
  await expect(dialog).toBeHidden();
}
