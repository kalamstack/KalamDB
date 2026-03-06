import { execFileSync, spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { test, expect } from '@playwright/test';

const exampleRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const serverUrl = process.env.KALAMDB_URL ?? 'http://127.0.0.1:8080';
const room = process.env.CHAT_TEST_ROOM ?? 'playwright-room-fallback';

let agentProcess;
const testGroup = `chat-demo-test-${Date.now()}`;

async function login(username, password) {
  const response = await fetch(`${serverUrl}/v1/api/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  });

  if (!response.ok) {
    throw new Error(`Login failed for ${username}: ${response.status}`);
  }

  const body = await response.json();
  return body.access_token;
}

async function executeSql(token, sql) {
  const response = await fetch(`${serverUrl}/v1/api/sql`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({ sql }),
  });

  if (!response.ok) {
    throw new Error(`SQL failed: ${response.status} ${await response.text()}`);
  }
}

async function seedChatHistory() {
  const token = await login('admin', 'kalamdb123');
  const values = Array.from({ length: 48 }, (_, index) => (
    `('${room}', 'assistant', 'KalamDB Copilot', 'admin', 'seed history ${index}-${Date.now()}')`
  )).join(', ');

  await executeSql(
    token,
    `INSERT INTO chat_demo.messages (room, role, author, sender_username, content) VALUES ${values}`,
  );
}

async function waitForOutput(process, expected) {
  await new Promise((resolve, reject) => {
    let stderr = '';
    const timer = setTimeout(() => reject(new Error(`Timed out waiting for: ${expected}`)), 20_000);
    const onData = (chunk) => {
      if (chunk.toString().includes(expected)) {
        clearTimeout(timer);
        process.stdout.off('data', onData);
        resolve();
      }
    };
    process.stdout.on('data', onData);
    process.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });
    process.on('exit', (code) => {
      clearTimeout(timer);
      reject(new Error(`Agent exited early with code ${code}${stderr ? `: ${stderr}` : ''}`));
    });
  });
}

test.beforeAll(async () => {
  execFileSync('./setup.sh', [], {
    cwd: exampleRoot,
    stdio: 'inherit',
    env: {
      ...process.env,
      KALAMDB_URL: process.env.KALAMDB_URL ?? 'http://127.0.0.1:8080',
    },
  });

  await seedChatHistory();

  agentProcess = spawn(process.execPath, ['--import', 'tsx', 'src/agent.ts'], {
    cwd: exampleRoot,
    stdio: ['ignore', 'pipe', 'pipe'],
    env: {
      ...process.env,
      KALAMDB_GROUP: testGroup,
      KALAMDB_START: 'latest',
    },
  });

  await waitForOutput(agentProcess, 'chat-demo-agent ready');
});

test.afterAll(() => {
  agentProcess?.kill('SIGTERM');
});

test('message insert streams a live draft and syncs the committed reply to both tabs', async ({ browser, baseURL }) => {
  const uniqueMessage = `latency spike ${Date.now()}`;
  const context = await browser.newContext();
  const pageOne = await context.newPage();
  const pageTwo = await context.newPage();

  await pageOne.goto(baseURL);
  await pageTwo.goto(baseURL);

  await expect(pageOne.getByTestId('chat-status')).toContainText('Live');
  await expect(pageTwo.getByTestId('chat-status')).toContainText('Live');

  await pageOne.getByLabel('Message').fill(uniqueMessage);
  await pageOne.getByRole('button', { name: 'Send through KalamDB' }).click();

  await expect(pageOne.getByTestId('chat-thread')).toContainText(uniqueMessage);
  await expect(pageOne.locator('.bubble-user').last().locator('header span')).toHaveText(/\d{1,2}:\d{2}(:\d{2})?\s?(AM|PM)?/);
  await expect(pageTwo.getByTestId('chat-thread')).toContainText(uniqueMessage);
  await expect(pageOne.getByTestId('stream-preview')).toContainText('KalamDB Copilot', { timeout: 15000 });
  await expect(pageOne.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });
  await expect(pageTwo.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });
  await expect(pageOne.getByTestId('agent-events')).toContainText('thinking', { timeout: 15000 });
  await expect(pageOne.getByTestId('agent-events')).toContainText('Assistant reply committed', { timeout: 15000 });
  await expect(pageOne.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 15000 });
  await expect(pageTwo.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 15000 });
  await expect(pageOne.getByTestId('stream-preview')).toHaveCount(0, { timeout: 15000 });
  await expect(pageTwo.getByTestId('stream-preview')).toHaveCount(0, { timeout: 15000 });

  const pageOneScroll = await pageOne.getByTestId('chat-thread').evaluate((node) => ({
    scrollTop: node.scrollTop,
    maxScrollTop: node.scrollHeight - node.clientHeight,
  }));
  const pageTwoScroll = await pageTwo.getByTestId('chat-thread').evaluate((node) => ({
    scrollTop: node.scrollTop,
    maxScrollTop: node.scrollHeight - node.clientHeight,
  }));

  expect(pageOneScroll.maxScrollTop - pageOneScroll.scrollTop).toBeLessThan(24);
  expect(pageTwoScroll.maxScrollTop - pageTwoScroll.scrollTop).toBeLessThan(24);
});