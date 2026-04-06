import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { buildReply } from './agent.js';
import { Auth, createClient } from '@kalamdb/client';
import { createConsumerClient, runAgent } from '@kalamdb/consumer';

const serverUrl = process.env.KALAMDB_URL ?? 'http://127.0.0.1:8080';
const username = process.env.KALAMDB_USERNAME ?? 'admin';
const password = process.env.KALAMDB_PASSWORD ?? 'kalamdb123';

function createAuthedClient() {
  return createClient({
    url: serverUrl,
    authProvider: async () => Auth.basic(username, password),
  });
}

function createWorkerClient() {
  return createConsumerClient({
    url: serverUrl,
    authProvider: async () => Auth.basic(username, password),
  });
}

async function isServerAvailable() {
  try {
    const response = await fetch(`${serverUrl}/health`);
    return response.ok;
  } catch {
    return false;
  }
}

async function waitFor(condition, timeoutMs = 15_000, intervalMs = 50) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const value = await condition();
    if (value) {
      return value;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }

  throw new Error(`Timed out after ${timeoutMs}ms`);
}

async function getAccessToken() {
  const response = await fetch(`${serverUrl}/v1/api/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  });

  if (!response.ok) {
    throw new Error(`Login failed: ${response.status}`);
  }

  const body = await response.json();
  if (!body.access_token) {
    throw new Error('Login response did not include an access token');
  }

  return body.access_token;
}

async function waitForTopicReady(client, topicId) {
  const accessToken = await getAccessToken();

  await waitFor(async () => {
    const response = await fetch(`${serverUrl}/v1/api/sql`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
      },
      body: JSON.stringify({
        sql: `SELECT routes FROM system.topics WHERE topic_id = '${topicId}'`,
      }),
    });

    if (!response.ok) {
      return false;
    }

    const body = await response.json();
    const routesRaw = body?.results?.[0]?.rows?.[0]?.[0];
    if (typeof routesRaw !== 'string') {
      return false;
    }

    try {
      const routes = JSON.parse(routesRaw);
      return Array.isArray(routes) && routes.length >= 1;
    } catch {
      return false;
    }
  }, 30_000, 100);

  await new Promise((resolve) => setTimeout(resolve, 100));
}

async function stopAgent(controller, task) {
  controller.abort();
  await Promise.race([
    task,
    new Promise((_, reject) => setTimeout(() => reject(new Error('Timed out waiting for agent shutdown')), 5_000)),
  ]);
}

function parseChatRow(message) {
  const payload = message.value;
  if (!payload || typeof payload !== 'object') {
    return null;
  }

  const raw = (payload.row ?? payload);
  if (!raw || typeof raw !== 'object') {
    return null;
  }

  const id = String(raw.id ?? '').trim();
  const room = String(raw.room ?? '').trim();
  const role = String(raw.role ?? '').trim();
  const sender_username = String(raw.sender_username ?? '').trim();
  const content = String(raw.content ?? '').trim();

  if (!id || !room || !role || !sender_username || !content) {
    return null;
  }

  return { id, room, role, sender_username, content };
}

async function insertUserMessage(client, tableName, room, messageId, content) {
  const sql = [
    `INSERT INTO ${tableName} (id, room, role, sender_username, content)`,
    `VALUES ('${messageId}', '${room}', 'user', '${username}', '${content.replace(/'/g, "''")}')`,
  ].join(' ');
  await client.query(sql);
}

test('buildReply generates a contextual assistant response', () => {
  const reply = buildReply('latency spike');
  assert.ok(reply.includes('AI reply:'), 'should start with AI reply prefix');
  assert.ok(reply.includes('latency spike'), 'should echo the user message');
  assert.ok(reply.includes('slowest route'), 'should include latency-specific advice');
});

test('buildReply handles deploy keyword', () => {
  const reply = buildReply('deploy issue');
  assert.ok(reply.includes('deploy-shaped'), 'should include deploy-specific advice');
});

test('buildReply handles queue keyword', () => {
  const reply = buildReply('queue growth');
  assert.ok(reply.includes('downstream dependency'), 'should include queue-specific advice');
});

test('buildReply uses default advice for generic messages', () => {
  const reply = buildReply('hello world');
  assert.ok(reply.includes('runAgent()'), 'should mention runAgent in default advice');
});

test('two agents in the same group avoid duplicates and fail over to the standby worker', { timeout: 30_000 }, async (t) => {
  if (!(await isServerAvailable())) {
    t.skip(`KalamDB server is not reachable at ${serverUrl}`);
    return;
  }

  const adminClient = createAuthedClient();
  const namespace = `chat_agent_failover_${Date.now()}`;
  const tableName = `${namespace}.messages`;
  const topicName = `${namespace}.agent_inbox`;
  const groupId = `chat-agent-group-${randomUUID()}`;
  const room = `room-${randomUUID()}`;
  const firstMessageId = `msg-${randomUUID()}`;
  const secondMessageId = `msg-${randomUUID()}`;

  await adminClient.query(`CREATE NAMESPACE ${namespace}`);
  await adminClient.query(
    `CREATE TABLE ${tableName} (id TEXT PRIMARY KEY, room TEXT, role TEXT, sender_username TEXT, content TEXT)`,
  );
  await adminClient.query(`CREATE TOPIC ${topicName}`);
  await adminClient.query(`ALTER TOPIC ${topicName} ADD SOURCE ${tableName} ON INSERT`);
  await waitForTopicReady(adminClient, topicName);

  const processed = new Map([
    ['agent-a', []],
    ['agent-b', []],
  ]);

  const agentAClient = createWorkerClient();
  const agentBClient = createWorkerClient();
  const controllerA = new AbortController();
  const controllerB = new AbortController();

  const startAgent = (name, client, stopSignal) => runAgent({
    client,
    name,
    topic: topicName,
    groupId,
    start: 'earliest',
    batchSize: 1,
    timeoutSeconds: 2,
    stopSignal,
    rowParser: parseChatRow,
    onRow: async (_ctx, row) => {
      if (row.role !== 'user' || row.room !== room) {
        return;
      }
      processed.get(name).push(row.id);
    },
  });

  const agentATask = startAgent('agent-a', agentAClient, controllerA.signal);
  const agentBTask = startAgent('agent-b', agentBClient, controllerB.signal);

  try {
    await new Promise((resolve) => setTimeout(resolve, 1_000));

    await insertUserMessage(adminClient, tableName, room, firstMessageId, 'first failover message');

    const firstProcessor = await waitFor(() => {
      if (processed.get('agent-a').includes(firstMessageId)) {
        return 'agent-a';
      }
      if (processed.get('agent-b').includes(firstMessageId)) {
        return 'agent-b';
      }
      return null;
    });

    const standbyProcessor = firstProcessor === 'agent-a' ? 'agent-b' : 'agent-a';
    const firstCount = processed.get('agent-a').filter((id) => id === firstMessageId).length
      + processed.get('agent-b').filter((id) => id === firstMessageId).length;
    assert.equal(firstCount, 1, 'the first message should be processed exactly once across the group');

    if (firstProcessor === 'agent-a') {
      await stopAgent(controllerA, agentATask);
    } else {
      await stopAgent(controllerB, agentBTask);
    }

    await new Promise((resolve) => setTimeout(resolve, 250));
    await insertUserMessage(adminClient, tableName, room, secondMessageId, 'second failover message');

    await waitFor(() => processed.get(standbyProcessor).includes(secondMessageId));

    const secondCount = processed.get('agent-a').filter((id) => id === secondMessageId).length
      + processed.get('agent-b').filter((id) => id === secondMessageId).length;
    assert.equal(secondCount, 1, 'the second message should be processed exactly once across the group');
    assert.ok(
      processed.get(standbyProcessor).includes(secondMessageId),
      'the standby agent should take over after the active agent stops',
    );
    assert.ok(
      !processed.get(firstProcessor).includes(secondMessageId),
      'the stopped agent must not process new messages after failover',
    );
  } finally {
    if (!controllerA.signal.aborted) {
      await stopAgent(controllerA, agentATask).catch(() => {});
    }
    if (!controllerB.signal.aborted) {
      await stopAgent(controllerB, agentBTask).catch(() => {});
    }

    await agentAClient.disconnect().catch(() => {});
    await agentBClient.disconnect().catch(() => {});

    await adminClient.query(`DROP TOPIC ${topicName}`).catch(() => {});
    await adminClient.query(`DROP TABLE ${tableName}`).catch(() => {});
    await adminClient.query(`DROP NAMESPACE ${namespace}`).catch(() => {});
  }
});