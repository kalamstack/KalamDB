import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { gunzipSync } from 'node:zlib';
import { Auth, createClient } from '@kalamdb/client';
import { test, expect } from '@playwright/test';

const exampleRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const serverUrl = process.env.KALAMDB_URL ?? 'http://127.0.0.1:8080';
const room = process.env.CHAT_TEST_ROOM ?? 'playwright-room-fallback';
const adminUsername = 'admin';
const adminPassword = 'kalamdb123';
const rootUsername = 'root';
const rootPassword = process.env.KALAMDB_ROOT_PASSWORD ?? 'kalamdb123';

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

function sqlLiteral(value) {
  return `'${value.replace(/'/g, "''")}'`;
}

function uniqueName(prefix) {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(condition, {
  timeoutMs = 15_000,
  intervalMs = 50,
  description = 'condition',
} = {}) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const result = await condition();
    if (result) {
      return result;
    }
    await sleep(intervalMs);
  }

  throw new Error(`Timed out waiting for ${description}`);
}

function createSdkClient() {
  return createClient({
    url: serverUrl,
    authProvider: async () => Auth.basic(adminUsername, adminPassword),
    disableCompression: true,
  });
}

async function closeClient(client) {
  if (!client) {
    return;
  }

  if (typeof client.shutdown === 'function') {
    await client.shutdown();
    return;
  }

  await client.disconnect();
}

function webSocketUrl(url) {
  return `${url.replace(/^http/, 'ws')}/v1/ws`;
}

function decodeWebSocketBuffer(buffer) {
  if (buffer.length >= 2 && buffer[0] === 0x1f && buffer[1] === 0x8b) {
    return gunzipSync(buffer).toString('utf8');
  }

  return new TextDecoder().decode(buffer);
}

async function readWebSocketText(data) {
  if (typeof data === 'string') {
    return data;
  }

  if (data instanceof Blob) {
    return decodeWebSocketBuffer(Buffer.from(await data.arrayBuffer()));
  }

  if (data instanceof ArrayBuffer) {
    return decodeWebSocketBuffer(Buffer.from(data));
  }

  if (ArrayBuffer.isView(data)) {
    return decodeWebSocketBuffer(Buffer.from(data.buffer, data.byteOffset, data.byteLength));
  }

  return String(data);
}

function parseWebSocketMessage(payload) {
  const lastSeqId = payload.match(/"last_seq_id":(-?\d+)/)?.[1];
  const message = JSON.parse(payload);

  if (lastSeqId && message.batch_control) {
    message.batch_control.last_seq_id = lastSeqId;
  }

  return message;
}

async function openAuthenticatedWebSocket(token, onMessage) {
  const ws = new WebSocket(webSocketUrl(serverUrl));
  const messages = [];
  let socketError = null;

  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Timed out opening raw WebSocket connection')), 5_000);

    ws.addEventListener('open', () => {
      clearTimeout(timer);
      resolve();
    }, { once: true });

    ws.addEventListener('error', (event) => {
      clearTimeout(timer);
      reject(event.error ?? new Error('WebSocket connection error'));
    }, { once: true });
  });

  ws.addEventListener('message', (event) => {
    void (async () => {
      try {
        const message = parseWebSocketMessage(await readWebSocketText(event.data));
        messages.push(message);
        if (onMessage) {
          await onMessage(message, ws);
        }
      } catch (error) {
        socketError = error instanceof Error ? error : new Error(String(error));
      }
    })();
  });

  ws.addEventListener('error', (event) => {
    socketError = event.error instanceof Error ? event.error : new Error('WebSocket error');
  });

  ws.send(JSON.stringify({
    type: 'authenticate',
    method: 'jwt',
    token,
    protocol: { serialization: 'json', compression: 'none' },
  }));

  await waitFor(() => {
    if (socketError) {
      throw socketError;
    }
    return messages.some((message) => message.type === 'auth_success');
  }, { description: 'raw websocket authentication' });

  return {
    ws,
    messages,
    getError: () => socketError,
  };
}

async function closeWebSocket(ws) {
  if (!ws || ws.readyState === WebSocket.CLOSED) {
    return;
  }

  await new Promise((resolve) => {
    const timer = setTimeout(resolve, 5_000);
    ws.addEventListener('close', () => {
      clearTimeout(timer);
      resolve();
    }, { once: true });
    ws.close();
  });
}

function extractRows(events) {
  const rows = [];

  for (const event of events) {
    if (
      (event.type === 'change' || event.type === 'initial_data_batch')
      && Array.isArray(event.rows)
    ) {
      rows.push(...event.rows);
    }
  }

  return rows;
}

function hasRowContent(events, expectedContent) {
  return extractRows(events).some((row) => row.content?.asString?.() === expectedContent);
}

function assertRowsStrictlyAfter(events, from, context) {
  for (const row of extractRows(events)) {
    const seq = row._seq?.asSeqId?.();
    if (!seq) {
      continue;
    }

    assert.ok(
      seq.compareTo(from) > 0,
      `${context}: received stale row with _seq=${seq.toString()} at/before from=${from.toString()}`,
    );
  }
}

function assertNoDuplicateSeqRows(events, context) {
  const seen = new Set();

  for (const row of extractRows(events)) {
    const seq = row._seq?.asSeqId?.();
    if (!seq) {
      continue;
    }

    const key = seq.toString();
    assert.ok(!seen.has(key), `${context}: duplicate _seq replayed: ${key}`);
    seen.add(key);
  }
}

async function createUser(username) {
  const token = await login(rootUsername, rootPassword);
  await executeSql(token, `CREATE USER ${sqlLiteral(username)} WITH PASSWORD ${sqlLiteral(adminPassword)} ROLE user`);
}

async function insertAssistantMessages(roomName, contents, username = adminUsername) {
  const values = contents.map((content) => (
    `(${sqlLiteral(roomName)}, 'assistant', 'KalamDB Copilot', ${sqlLiteral(username)}, ${sqlLiteral(content)})`
  )).join(', ');
  const statement = `INSERT INTO chat_demo.messages (room, role, author, sender_username, content) VALUES ${values}`;

  if (username === adminUsername) {
    const token = await login(adminUsername, adminPassword);
    await executeSql(token, statement);
    return;
  }

  const token = await login(rootUsername, rootPassword);
  await executeSql(token, `EXECUTE AS USER ${sqlLiteral(username)} (${statement})`);
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
  const userMessageContent = pageOne.getByTestId('chat-thread').getByText(uniqueMessage, { exact: true });
  await expect(userMessageContent).toBeVisible();
  const userMessage = userMessageContent.locator('xpath=ancestor::article[1]');
  await expect(userMessage.locator('header strong')).toHaveText('admin');
  await expect(userMessage.locator('header span')).toHaveText(/\d{1,2}:\d{2}(:\d{2})?\s?(AM|PM)?/);
  await expect(pageTwo.getByTestId('chat-thread')).toContainText(uniqueMessage);
  await expect(pageOne.getByTestId('stream-preview')).toContainText('KalamDB Copilot', { timeout: 15000 });
  await expect(pageOne.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });
  await expect(pageTwo.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });
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

test('a tab that joins mid-stream catches the active draft and final reply', async ({ browser, baseURL }) => {
  const uniqueMessage = `latency spike ${'x'.repeat(1200)} ${Date.now()}`;
  const context = await browser.newContext();
  const pageOne = await context.newPage();

  await pageOne.goto(baseURL);
  await expect(pageOne.getByTestId('chat-status')).toContainText('Live');

  await pageOne.getByLabel('Message').fill(uniqueMessage);
  await pageOne.getByRole('button', { name: 'Send through KalamDB' }).click();

  await expect(pageOne.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });

  const pageTwo = await context.newPage();
  await pageTwo.goto(baseURL);
  await expect(pageTwo.getByTestId('chat-status')).toContainText('Live');
  await expect(pageTwo.getByTestId('chat-thread')).toContainText(uniqueMessage, { timeout: 15000 });
  await expect(pageTwo.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });

  await pageTwo.reload();
  await expect(pageTwo.getByTestId('chat-status')).toContainText('Live');
  await expect(pageTwo.getByTestId('chat-thread')).toContainText(uniqueMessage, { timeout: 15000 });

  await expect(pageOne.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 20000 });
  await expect(pageTwo.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 20000 });
});

test('a third tab that joins mid-stream keeps receiving later streams without a refresh', async ({ browser, baseURL }) => {
  const firstMessage = `multi-tab first ${'x'.repeat(800)} ${Date.now()}`;
  const secondMessage = `multi-tab second ${Date.now()}`;
  const context = await browser.newContext();
  const pageOne = await context.newPage();
  const pageTwo = await context.newPage();

  await pageOne.goto(baseURL);
  await pageTwo.goto(baseURL);

  await expect(pageOne.getByTestId('chat-status')).toContainText('Live');
  await expect(pageTwo.getByTestId('chat-status')).toContainText('Live');

  await pageOne.getByLabel('Message').fill(firstMessage);
  await pageOne.getByRole('button', { name: 'Send through KalamDB' }).click();

  await expect(pageOne.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });
  await expect(pageTwo.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });

  const pageThree = await context.newPage();
  await pageThree.goto(baseURL);
  await expect(pageThree.getByTestId('chat-status')).toContainText('Live');
  await expect(pageThree.getByTestId('chat-thread')).toContainText(firstMessage, { timeout: 15000 });
  await expect(pageThree.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });

  await expect(pageOne.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 20000 });
  await expect(pageTwo.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 20000 });
  await expect(pageThree.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 20000 });

  await pageOne.getByLabel('Message').fill(secondMessage);
  await pageOne.getByRole('button', { name: 'Send through KalamDB' }).click();

  await expect(pageTwo.getByTestId('chat-thread')).toContainText(secondMessage, { timeout: 15000 });
  await expect(pageThree.getByTestId('chat-thread')).toContainText(secondMessage, { timeout: 15000 });
  await expect(pageTwo.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });
  await expect(pageThree.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });
  await expect(pageTwo.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 20000 });
  await expect(pageThree.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 20000 });
});

test('three open tabs stay in sync when one tab reloads mid-stream and then receives the next reply', async ({ browser, baseURL }) => {
  const firstMessage = `reconnect first ${'x'.repeat(1800)} ${Date.now()}`;
  const secondMessage = `reconnect second ${Date.now()}`;
  const context = await browser.newContext();
  const pageOne = await context.newPage();
  const pageTwo = await context.newPage();
  const pageThree = await context.newPage();

  await pageOne.goto(baseURL);
  await pageTwo.goto(baseURL);
  await pageThree.goto(baseURL);

  await expect(pageOne.getByTestId('chat-status')).toContainText('Live');
  await expect(pageTwo.getByTestId('chat-status')).toContainText('Live');
  await expect(pageThree.getByTestId('chat-status')).toContainText('Live');

  await pageOne.getByLabel('Message').fill(firstMessage);
  await pageOne.getByRole('button', { name: 'Send through KalamDB' }).click();

  await expect(pageOne.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });
  await expect(pageThree.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });

  await pageTwo.reload();
  await expect(pageTwo.getByTestId('chat-status')).toContainText('Live');
  await expect(pageTwo.getByTestId('chat-thread')).toContainText(firstMessage, { timeout: 15000 });
  await expect(pageTwo.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });

  await expect(pageOne.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 20000 });
  await expect(pageTwo.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 20000 });
  await expect(pageThree.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 20000 });

  await pageThree.getByLabel('Message').fill(secondMessage);
  await pageThree.getByRole('button', { name: 'Send through KalamDB' }).click();

  await expect(pageOne.getByTestId('chat-thread')).toContainText(secondMessage, { timeout: 15000 });
  await expect(pageTwo.getByTestId('chat-thread')).toContainText(secondMessage, { timeout: 15000 });
  await expect(pageOne.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });
  await expect(pageTwo.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });
  await expect(pageThree.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });
  await expect(pageOne.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 20000 });
  await expect(pageTwo.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 20000 });
  await expect(pageThree.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 20000 });
});

test('initial live subscription can deliver three startup batches while staying scoped to the current user', async () => {
  const batchRoom = uniqueName('batch-room');
  const subscriberUser = uniqueName('chat-subscriber');
  const foreignUser = uniqueName('chat-batch-user');
  const contents = Array.from({ length: 6 }, (_, index) => `${batchRoom}-seed-${index}`);
  const foreignContent = `${batchRoom}-foreign-user-row`;
  const sql = `SELECT id, room, role, content FROM chat_demo.messages WHERE room = ${sqlLiteral(batchRoom)}`;
  const subscriptionId = uniqueName('chat-batch-sub');
  let socket;
  let messages = [];
  let getSocketError = () => null;

  try {
    await createUser(subscriberUser);
    await createUser(foreignUser);
    await insertAssistantMessages(batchRoom, contents, subscriberUser);
    await insertAssistantMessages(batchRoom, [foreignContent], foreignUser);

    const subscriberToken = await login(subscriberUser, adminPassword);

    const connection = await openAuthenticatedWebSocket(subscriberToken, async (message, ws) => {
      if (message.type === 'initial_data_batch' && message.batch_control?.has_more) {
        const nextBatchPayload = message.batch_control.last_seq_id
          ? `{"type":"next_batch","subscription_id":${JSON.stringify(message.subscription_id)},"last_seq_id":${message.batch_control.last_seq_id}}`
          : `{"type":"next_batch","subscription_id":${JSON.stringify(message.subscription_id)}}`;
        ws.send(nextBatchPayload);
      }
    });
    socket = connection.ws;
    messages = connection.messages;
    getSocketError = connection.getError;

    socket.send(JSON.stringify({
      type: 'subscribe',
      subscription: {
        id: subscriptionId,
        sql,
        options: { batch_size: 2 },
      },
    }));

    await waitFor(() => {
      const socketError = getSocketError();
      if (socketError) {
        throw socketError;
      }
      const initialBatches = messages.filter((event) => event.type === 'initial_data_batch');
      return initialBatches.length === 3 && initialBatches.at(-1)?.batch_control?.has_more === false;
    }, { description: 'three raw websocket initial data batches for the chat example subscription' });

    await sleep(250);

    const ack = messages.find((event) => event.type === 'subscription_ack' && event.subscription_id === subscriptionId);
    const initialBatches = messages.filter((event) => event.type === 'initial_data_batch');
    const snapshotContents = initialBatches
      .flatMap((event) => event.rows ?? [])
      .map((row) => row.content)
      .filter(Boolean);

    expect(ack?.batch_control?.has_more).toBe(true);
    expect(initialBatches).toHaveLength(3);
    expect(initialBatches.map((event) => event.rows?.length ?? 0)).toEqual([2, 2, 2]);
    expect(initialBatches.at(-1)?.batch_control?.has_more).toBe(false);
    expect(snapshotContents).not.toContain(foreignContent);
    for (const content of contents) {
      expect(snapshotContents).toContain(content);
    }
  } finally {
    await closeWebSocket(socket);
  }
});

test('subscription can disconnect and resume from the saved checkpoint without replaying older rows', async () => {
  const client = createSdkClient();
  const resumeRoom = uniqueName('resume-room');
  const sql = `SELECT id, content FROM chat_demo.messages WHERE room = ${sqlLiteral(resumeRoom)}`;
  const preContent = `${resumeRoom}-before`;
  const gapContent = `${resumeRoom}-gap`;
  const liveContent = `${resumeRoom}-live`;
  const preEvents = [];
  const resumedEvents = [];
  let initialUnsubscribe;
  let resumedUnsubscribe;

  try {
    initialUnsubscribe = await client.subscribeWithSql(
      sql,
      (event) => preEvents.push(event),
      { last_rows: 0 },
    );

    await waitFor(
      () => preEvents.some((event) => event.type === 'subscription_ack'),
      { description: 'initial resume subscription ack' },
    );

    await insertAssistantMessages(resumeRoom, [preContent]);
    await waitFor(
      () => hasRowContent(preEvents, preContent),
      { description: 'pre-disconnect row on the initial subscription' },
    );

    const checkpoint = await waitFor(() => {
      const subscription = client.getSubscriptions().find((entry) => entry.tableName === sql);
      return subscription?.lastSeqId;
    }, { description: 'resume checkpoint in subscription metadata' });

    await initialUnsubscribe();
    initialUnsubscribe = undefined;

    await client.disconnect();
    expect(client.isConnected()).toBe(false);

    await insertAssistantMessages(resumeRoom, [gapContent]);

    resumedUnsubscribe = await client.subscribeWithSql(
      sql,
      (event) => resumedEvents.push(event),
      { from: checkpoint, last_rows: 0 },
    );

    expect(client.isConnected()).toBe(true);
    await waitFor(
      () => resumedEvents.some((event) => event.type === 'subscription_ack'),
      { description: 'resumed subscription ack' },
    );

    await insertAssistantMessages(resumeRoom, [liveContent]);
    await waitFor(
      () => hasRowContent(resumedEvents, gapContent) && hasRowContent(resumedEvents, liveContent),
      { description: 'gap and live rows after resume' },
    );

    expect(hasRowContent(resumedEvents, preContent)).toBe(false);
    assertRowsStrictlyAfter(resumedEvents, checkpoint, 'chat example resume');
    assertNoDuplicateSeqRows(resumedEvents, 'chat example resume');
  } finally {
    if (resumedUnsubscribe) {
      await resumedUnsubscribe();
    }
    if (initialUnsubscribe) {
      await initialUnsubscribe();
    }
    await closeClient(client);
  }
});

test('streaming still works when the user message contains an unmatched parenthesis', async ({ browser, baseURL }) => {
  const uniqueMessage = `manual repro ( ${Date.now()}`;
  const context = await browser.newContext();
  const pageOne = await context.newPage();
  const pageTwo = await context.newPage();

  await pageOne.goto(baseURL);
  await pageTwo.goto(baseURL);

  await expect(pageOne.getByTestId('chat-status')).toContainText('Live');
  await expect(pageTwo.getByTestId('chat-status')).toContainText('Live');

  await pageOne.getByLabel('Message').fill(uniqueMessage);
  await pageOne.getByRole('button', { name: 'Send through KalamDB' }).click();

  await expect(pageOne.getByTestId('chat-thread')).toContainText(uniqueMessage, { timeout: 15000 });
  await expect(pageTwo.getByTestId('chat-thread')).toContainText(uniqueMessage, { timeout: 15000 });
  await expect(pageOne.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });
  await expect(pageTwo.getByTestId('stream-preview')).toContainText('AI reply:', { timeout: 15000 });
  await expect(pageOne.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 20000 });
  await expect(pageTwo.getByTestId('chat-thread')).toContainText('AI reply: KalamDB stored', { timeout: 20000 });
});