// Playwright E2E for the console against the in-browser mock API (VITE_ALELT_MOCK=1, VITE_ALELT_DEV_AUTH=1).
// Run: npm run e2e   (first time: npx playwright install chromium). Not part of `npm test`.
import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: '.',
  testMatch: '*.spec.ts',
  fullyParallel: true,
  retries: process.env.CI ? 1 : 0,
  reporter: [['list'], ['html', { open: 'never', outputFolder: '../playwright-report' }]],
  use: { baseURL: 'http://localhost:5173', trace: 'retain-on-failure' },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer: {
    command: 'npx vite --port 5173 --strictPort',
    cwd: '..',
    url: 'http://localhost:5173',
    reuseExistingServer: !process.env.CI,
    env: { VITE_ALELT_MOCK: '1', VITE_ALELT_DEV_AUTH: '1' },
  },
});
