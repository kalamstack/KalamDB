import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { test, expect } from '@playwright/test';

const exampleRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

test.beforeAll(() => {
  execFileSync('./setup.sh', [], {
    cwd: exampleRoot,
    stdio: 'inherit',
    env: {
      ...process.env,
      KALAMDB_URL: process.env.KALAMDB_URL ?? 'http://127.0.0.1:8080',
    },
  });
});

test('two tabs stay in sync through the live SQL subscription', async ({ browser, baseURL }) => {
  const context = await browser.newContext();
  const pageOne = await context.newPage();
  const pageTwo = await context.newPage();
  const uniqueMessage = `playwright-${Date.now()}`;

  await pageOne.goto(baseURL!);
  await pageTwo.goto(baseURL!);

  await expect(pageOne.getByTestId('connection-status')).toContainText('Live');
  await expect(pageTwo.getByTestId('connection-status')).toContainText('Live');

  await pageOne.getByLabel('Service').fill('shipping');
  await pageOne.getByLabel('Level').selectOption('critical');
  await pageOne.getByLabel('Actor').fill('playwright');
  await pageOne.getByLabel('Message').fill(uniqueMessage);
  await pageOne.getByRole('button', { name: 'Broadcast event' }).click();

  await expect(pageOne.getByTestId('feed-list')).toContainText(uniqueMessage);
  await expect(pageTwo.getByTestId('feed-list')).toContainText(uniqueMessage);
});