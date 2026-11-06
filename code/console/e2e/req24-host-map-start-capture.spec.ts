// REQ-24 (skeleton): import wizard host-mapping step before apply, and START_CAPTURE resume / from position.
import { expect, test } from '@playwright/test';
import { confirm, loginAs } from './helpers';

test('import: Apply stays disabled until every production host maps to a clean test host', async ({ page }) => {
  await loginAs(page, 'DataOperator');
  await page.goto('/import/new');
  await page.getByTestId('plugin-legacy_etl').click();
  await page.locator('input[type=file]').setInputFiles([
    { name: 'export.json', mimeType: 'application/json', buffer: Buffer.from('{}') },
  ]);
  await page.getByTestId('wizard-next').click();            // analyze files
  await page.getByTestId('wizard-next').click();            // review -> map hosts
  const apply = page.getByTestId('wizard-next');
  await expect(apply).toBeDisabled();

  await page.getByTestId('host-map-db2prod.corp.local').fill('db2prod.corp.local');
  await expect(page.getByTestId('host-denied-db2prod.corp.local')).toBeVisible();
  await expect(apply).toBeDisabled();

  await page.getByTestId('host-map-db2prod.corp.local').fill('db2test.corp.local');
  await page.getByTestId('host-map-xy12345.canada-central.azure.snowflakecomputing.com').fill('test-acct.snowflakecomputing.com');
  await expect(apply).toBeEnabled();
  await apply.click();
  await expect(page.getByText(/r\d+/)).toBeVisible();
  // TODO: hub refusal path (409 HOST_MAP_REQUIRED / PROD_TARGET) shown in data-testid=hub-refusal
});

test('agent: START_CAPTURE resume (default) and from an explicit position', async ({ page }) => {
  await loginAs(page, 'PlatformAdmin');
  await page.goto('/agent');
  await expect(page.getByTestId('start-resume')).toBeChecked();
  await page.getByTestId('start-capture-btn').click();
  await expect(page.getByTestId('start-capture-summary')).toBeVisible();
  await confirm(page, 'restart after maintenance');

  await page.getByTestId('start-from').check();
  await expect(page.getByTestId('start-capture-btn')).toBeDisabled();
  await page.getByTestId('start-from-pos').fill('1:00000000000A3F21');
  await page.getByTestId('start-capture-btn').click();
  await expect(page.getByTestId('start-capture-summary')).toContainText('1:00000000000A3F21');
  await confirm(page, 'replay from a known position');
  // TODO: assert agent.command.START_CAPTURE in /audit with args {resume: true} and {from_pos}
});
