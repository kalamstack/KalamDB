import assert from 'node:assert/strict';
import test from 'node:test';

import {
  Auth,
  SeqId,
  createClient,
  runAgent,
} from '../dist/src/index.js';

function createReadmeWasmClient() {
  let connected = false;
  let nextSubscriptionId = 0;
  const callbacks = new Map();
  const subscriptions = [];

  return {
    connectCalls: 0,
    lastLiveQueryOptionsJson: undefined,
    queryCalls: [],
    queryWithParamsCalls: [],
    setAuthProvider() {},
    setWsLazyConnect() {},
    setAutoReconnect() {},
    setReconnectDelay() {},
    setMaxReconnectAttempts() {},
    getReconnectAttempts() { return 0; },
    isReconnecting() { return false; },
    onConnect() {},
    onDisconnect() {},
    onError() {},
    onReceive() {},
    onSend() {},
    isConnected() {
      return connected;
    },
    async connect() {
      this.connectCalls += 1;
      connected = true;
    },
    async query(sql) {
      this.queryCalls.push({ sql });
      return JSON.stringify({ status: 'success', results: [] });
    },
    async queryWithParams(sql, paramsJson) {
      this.queryWithParamsCalls.push({ sql, paramsJson });
      return JSON.stringify({ status: 'success', results: [] });
    },
    async liveQueryRowsWithSql(sql, optionsJson, callback) {
      nextSubscriptionId += 1;
      const subscriptionId = `sub-${nextSubscriptionId}`;
      this.lastLiveQueryOptionsJson = optionsJson;
      callbacks.set(subscriptionId, callback);
      subscriptions.push({ id: subscriptionId, query: sql, lastSeqId: undefined });
      return subscriptionId;
    },
    async unsubscribe(subscriptionId) {
      callbacks.delete(subscriptionId);
    },
    getLastSeqId(subscriptionId) {
      return subscriptions.find((sub) => sub.id === subscriptionId)?.lastSeqId;
    },
    getSubscriptions() {
      return JSON.stringify(subscriptions);
    },
    emitRows(subscriptionId, rows, lastSeqId) {
      const sub = subscriptions.find((entry) => entry.id === subscriptionId);
      if (!sub) {
        throw new Error(`Unknown subscription ${subscriptionId}`);
      }
      if (lastSeqId !== undefined) {
        sub.lastSeqId = String(lastSeqId);
      }
      const callback = callbacks.get(subscriptionId);
      if (!callback) {
        throw new Error(`Missing callback for ${subscriptionId}`);
      }
      callback(JSON.stringify({
        type: 'rows',
        subscription_id: subscriptionId,
        rows,
      }));
    },
  };
}

function createReadmeAgentClient(messages) {
  const state = {
    ackedOffsets: [],
    executeAsUserCalls: [],
    consumerOptions: [],
  };

  const client = {
    query: async () => ({ status: 'success', results: [] }),
    queryOne: async () => null,
    queryAll: async () => [],
    executeAsUser: async (sql, username, params) => {
      state.executeAsUserCalls.push({ sql, username, params });
      return { status: 'success', results: [] };
    },
    consumer: (consumeOptions) => {
      state.consumerOptions.push(consumeOptions);
      return {
        run: async (handler) => {
          for (const message of messages) {
            await handler({
              username: 'alice',
              message,
              ack: async () => {
                state.ackedOffsets.push(message.offset);
              },
            });
          }
        },
        stop: () => {},
      };
    },
  };

  return { client, state };
}

test('README live resume example passes options and exposes typed checkpoints', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.none(),
  });

  const fakeWasmClient = createReadmeWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  const inboxSql = `
  SELECT id, room, role, body, created_at
  FROM support.inbox
`;
  const renderedSnapshots = [];
  const checkpoints = [];
  const startFrom = SeqId.from('42');

  const stop = await client.live(
    inboxSql,
    (rows) => {
      renderedSnapshots.push(rows.map((row) => ({
        id: row.id.asString(),
        role: row.role.asString(),
        body: row.body.asString(),
      })));

      const active = client.getSubscriptions().find((sub) => sub.tableName === inboxSql);
      checkpoints.push(active?.lastSeqId?.toString());
    },
    {
      subscriptionOptions: {
        last_rows: 200,
        from: startFrom,
      },
    },
  );

  assert.deepEqual(JSON.parse(fakeWasmClient.lastLiveQueryOptionsJson), {
    subscription_options: {
      last_rows: 200,
      from: '42',
    },
  });

  fakeWasmClient.emitRows('sub-1', [
    { id: '1', room: 'main', role: 'user', body: 'Need help' },
  ], 101);

  assert.deepEqual(renderedSnapshots[0], [
    { id: '1', role: 'user', body: 'Need help' },
  ]);
  assert.equal(checkpoints[0], '101');

  await stop();
});

test('README executeAsUser example wraps SQL for tenant-safe writes', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.none(),
  });

  const fakeWasmClient = createReadmeWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  await client.executeAsUser(
    'INSERT INTO support.inbox (room, role, body) VALUES ($1, $2, $3);',
    "alice'o",
    ['main', 'assistant', 'Your billing issue is being reviewed'],
  );

  assert.equal(fakeWasmClient.queryWithParamsCalls.length, 1);
  assert.equal(
    fakeWasmClient.queryWithParamsCalls[0].sql,
    "EXECUTE AS USER 'alice''o' (INSERT INTO support.inbox (room, role, body) VALUES ($1, $2, $3))",
  );
  assert.equal(
    fakeWasmClient.queryWithParamsCalls[0].paramsJson,
    JSON.stringify(['main', 'assistant', 'Your billing issue is being reviewed']),
  );
});

test('README queryWithFiles example posts multipart data with auth header', async () => {
  const originalFetch = globalThis.fetch;
  const originalFile = globalThis.File;

  class TestFile extends Blob {
    constructor(parts, name, options) {
      super(parts, options);
      this.name = name;
    }
  }

  globalThis.File = TestFile;

  let fetchCall;
  globalThis.fetch = async (url, options) => {
    fetchCall = { url, options };
    return {
      ok: true,
      async json() {
        return { status: 'success', results: [] };
      },
    };
  };

  try {
    const client = createClient({
      url: 'http://127.0.0.1:8080',
      authProvider: async () => Auth.none(),
    });

    client.initialized = true;
    client.auth = Auth.jwt('token-123');

    await client.queryWithFiles(
      'INSERT INTO support.attachments (id, file_data) VALUES ($1, FILE("upload"))',
      { upload: new TestFile(['hello world'], 'note.txt', { type: 'text/plain' }) },
      ['att_1'],
    );

    assert.equal(fetchCall.url, 'http://127.0.0.1:8080/v1/api/sql');
    assert.equal(fetchCall.options.method, 'POST');
    assert.equal(fetchCall.options.headers.Authorization, 'Bearer token-123');
    assert.equal(fetchCall.options.body.get('sql'), 'INSERT INTO support.attachments (id, file_data) VALUES ($1, FILE("upload"))');
    assert.equal(fetchCall.options.body.get('params'), JSON.stringify(['att_1']));

    const uploaded = fetchCall.options.body.get('file:upload');
    assert.ok(uploaded instanceof TestFile);
    assert.equal(uploaded.name, 'note.txt');
  } finally {
    globalThis.fetch = originalFetch;
    globalThis.File = originalFile;
  }
});

test('README runAgent example writes back through executeAsUser inside the user tenant', async () => {
  const message = {
    offset: 7,
    partition_id: 0,
    topic: 'support.inbox_events',
    group_id: 'support-summary-agent',
    value: {
      row: {
        body: 'Please summarize this support thread',
      },
    },
  };

  const { client, state } = createReadmeAgentClient([message]);

  await runAgent({
    client,
    name: 'support-summary-agent',
    topic: 'support.inbox_events',
    groupId: 'support-summary-agent',
    retry: {
      maxAttempts: 1,
      initialBackoffMs: 0,
      maxBackoffMs: 0,
    },
    onRow: async (ctx, row) => {
      const username = String(ctx.username ?? '').trim();
      const body = String(row.body ?? '').trim();
      if (!username || !body) {
        return;
      }

      const summary = `Support summary: ${body.slice(0, 120)}`;
      await client.executeAsUser(
        'INSERT INTO support.inbox (room, role, body) VALUES ($1, $2, $3)',
        username,
        ['main', 'assistant', summary],
      );
    },
  });

  assert.equal(state.executeAsUserCalls.length, 1);
  assert.equal(state.executeAsUserCalls[0].username, 'alice');
  assert.deepEqual(state.executeAsUserCalls[0].params, [
    'main',
    'assistant',
    'Support summary: Please summarize this support thread',
  ]);
  assert.deepEqual(state.ackedOffsets, [7]);
});
