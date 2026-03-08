import assert from 'node:assert/strict';
import test from 'node:test';

import {
  Auth,
  SeqId,
  createClient,
} from '../dist/src/index.js';

function createRuntimeCoverageWasmClient() {
  let connected = false;
  let nextSubscriptionId = 0;
  let reconnectAttempts = 0;
  const subscriptionCallbacks = new Map();
  const liveCallbacks = new Map();
  const subscriptions = [];
  const consumeResponses = [];

  return {
    queryCalls: [],
    queryWithParamsCalls: [],
    insertCalls: [],
    deleteCalls: [],
    ackCalls: [],
    consumeCalls: [],
    subscribeCalls: [],
    liveSubscribeCalls: [],
    reconnectConfig: {
      autoReconnect: undefined,
      initialDelayMs: undefined,
      maxDelayMs: undefined,
      maxReconnectAttempts: undefined,
    },
    setAuthProvider() {},
    setWsLazyConnect() {},
    onConnect() {},
    onDisconnect() {},
    onError() {},
    onReceive() {},
    onSend() {},
    isConnected() {
      return connected;
    },
    async connect() {
      connected = true;
    },
    async disconnect() {
      connected = false;
    },
    async query(sql) {
      this.queryCalls.push(sql);
      return JSON.stringify({ status: 'success', results: [] });
    },
    async queryWithParams(sql, paramsJson) {
      this.queryWithParamsCalls.push({ sql, paramsJson });
      return JSON.stringify({
        status: 'success',
        results: [{
          row_count: 2,
          named_rows: [
            {
              id: 1,
              title: 'alpha',
              done: true,
              score: 9.5,
              created_at: '2026-03-08T10:00:00.000Z',
            },
            {
              id: 2,
              title: 'beta',
              done: false,
              score: 7.25,
              created_at: '2026-03-08T11:00:00.000Z',
            },
          ],
        }],
      });
    },
    async insert(tableName, dataJson) {
      this.insertCalls.push({ tableName, dataJson });
      return JSON.stringify({ status: 'success', results: [] });
    },
    async delete(tableName, rowId) {
      this.deleteCalls.push({ tableName, rowId });
    },
    async login() {
      return {
        access_token: 'jwt-123',
        refresh_token: 'refresh-123',
        token_type: 'Bearer',
        expires_in: 3600,
        user: { username: 'alice', role: 'user' },
      };
    },
    async refresh_access_token(refreshToken) {
      return {
        access_token: `jwt-for-${refreshToken}`,
        refresh_token: 'refresh-456',
        token_type: 'Bearer',
        expires_in: 3600,
        user: { username: 'alice', role: 'user' },
      };
    },
    setAutoReconnect(enabled) {
      this.reconnectConfig.autoReconnect = enabled;
    },
    setReconnectDelay(initialDelayMs, maxDelayMs) {
      this.reconnectConfig.initialDelayMs = initialDelayMs;
      this.reconnectConfig.maxDelayMs = maxDelayMs;
    },
    setMaxReconnectAttempts(maxAttempts) {
      this.reconnectConfig.maxReconnectAttempts = maxAttempts;
    },
    getReconnectAttempts() {
      return reconnectAttempts;
    },
    isReconnecting() {
      return reconnectAttempts > 0;
    },
    getLastSeqId(subscriptionId) {
      return subscriptions.find((sub) => sub.id === subscriptionId)?.lastSeqId;
    },
    getSubscriptions() {
      return JSON.stringify(subscriptions.map((sub) => ({
        id: sub.id,
        query: sub.query,
        lastSeqId: sub.lastSeqId,
      })));
    },
    async subscribeWithSql(sql, optionsJson, callback) {
      nextSubscriptionId += 1;
      const id = `sub-${nextSubscriptionId}`;
      this.subscribeCalls.push({ sql, optionsJson });
      subscriptionCallbacks.set(id, callback);
      subscriptions.push({ id, query: sql, lastSeqId: undefined });
      return id;
    },
    async liveQueryRowsWithSql(sql, optionsJson, callback) {
      nextSubscriptionId += 1;
      const id = `live-${nextSubscriptionId}`;
      this.liveSubscribeCalls.push({ sql, optionsJson });
      liveCallbacks.set(id, callback);
      subscriptions.push({ id, query: sql, lastSeqId: undefined });
      return id;
    },
    async unsubscribe(subscriptionId) {
      subscriptionCallbacks.delete(subscriptionId);
      liveCallbacks.delete(subscriptionId);
      const index = subscriptions.findIndex((sub) => sub.id === subscriptionId);
      if (index >= 0) {
        subscriptions.splice(index, 1);
      }
    },
    emitSubscription(subscriptionId, event, lastSeqId) {
      const callback = subscriptionCallbacks.get(subscriptionId);
      if (!callback) {
        throw new Error(`Missing subscription callback for ${subscriptionId}`);
      }
      const sub = subscriptions.find((entry) => entry.id === subscriptionId);
      if (sub && lastSeqId !== undefined) {
        sub.lastSeqId = String(lastSeqId);
      }
      callback(JSON.stringify(event));
    },
    emitLiveRows(subscriptionId, rows, lastSeqId) {
      const callback = liveCallbacks.get(subscriptionId);
      if (!callback) {
        throw new Error(`Missing live callback for ${subscriptionId}`);
      }
      const sub = subscriptions.find((entry) => entry.id === subscriptionId);
      if (sub && lastSeqId !== undefined) {
        sub.lastSeqId = String(lastSeqId);
      }
      callback(JSON.stringify({ type: 'rows', subscription_id: subscriptionId, rows }));
    },
    queueConsumeResponse(response) {
      consumeResponses.push(response);
    },
    async consume(options) {
      this.consumeCalls.push(options);
      return consumeResponses.shift() ?? { messages: [], next_offset: 0, has_more: true };
    },
    async ack(topic, groupId, partitionId, offset) {
      this.ackCalls.push({ topic, groupId, partitionId, offset });
      return { success: true, acknowledged_offset: Number(offset) };
    },
    setReconnectAttempts(value) {
      reconnectAttempts = value;
    },
  };
}

test('queryRows wraps named_rows into KalamRow with typed cell access', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.none(),
  });
  const fakeWasmClient = createRuntimeCoverageWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  const rows = await client.queryRows(
    'SELECT id, title, done, score, created_at FROM demo.tasks WHERE id > $1',
    'demo.tasks',
    [0],
  );

  assert.equal(rows.length, 2);
  assert.equal(rows[0].typedData.title.asString(), 'alpha');
  assert.equal(rows[0].cell('id').asInt(), 1);
  assert.equal(rows[0].cell('done').asBool(), true);
  assert.equal(rows[0].cell('score').asFloat(), 9.5);
  assert.equal(rows[0].cell('created_at').asDate().toISOString(), '2026-03-08T10:00:00.000Z');
});

test('subscribeRows wraps change rows and oldValues as KalamRow instances', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.none(),
  });
  const fakeWasmClient = createRuntimeCoverageWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  const changes = [];
  const unsub = await client.subscribeRows('demo.tasks', (change) => {
    changes.push(change);
  });

  fakeWasmClient.emitSubscription('sub-1', {
    type: 'change',
    change_type: 'update',
    subscription_id: 'sub-1',
    rows: [{ id: 7, title: 'after' }],
    old_values: [{ id: 7, title: 'before' }],
  }, 77);

  assert.equal(changes.length, 1);
  assert.equal(changes[0].rows[0].cell('id').asInt(), 7);
  assert.equal(changes[0].rows[0].typedData.title.asString(), 'after');
  assert.equal(changes[0].oldValues[0].typedData.title.asString(), 'before');

  await unsub();
});

test('liveTableRows delegates to live using SELECT * sugar', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.none(),
  });
  const fakeWasmClient = createRuntimeCoverageWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  const snapshots = [];
  const unsub = await client.liveTableRows('demo.tasks', (rows) => {
    snapshots.push(rows.map((row) => row.id.asInt()));
  }, {
    subscriptionOptions: { last_rows: 5, from_seq_id: SeqId.from('10').toJSON() },
  });

  assert.equal(fakeWasmClient.liveSubscribeCalls[0].sql, 'SELECT * FROM demo.tasks');
  assert.deepEqual(JSON.parse(fakeWasmClient.liveSubscribeCalls[0].optionsJson), {
    subscription_options: { last_rows: 5, from_seq_id: 10 },
  });

  fakeWasmClient.emitLiveRows('live-1', [{ id: 1 }, { id: 2 }], 12);
  assert.deepEqual(snapshots[0], [1, 2]);

  await unsub();
});

test('login refresh and reconnect helpers delegate to wasm client', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.basic('alice', 'secret'),
  });
  const fakeWasmClient = createRuntimeCoverageWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;
  client.auth = Auth.basic('alice', 'secret');

  const login = await client.login();
  assert.equal(login.access_token, 'jwt-123');
  assert.equal(client.getAuthType(), 'jwt');

  const refreshed = await client.refreshToken('refresh-123');
  assert.equal(refreshed.access_token, 'jwt-for-refresh-123');

  client.setAutoReconnect(true);
  client.setReconnectDelay(100, 2000);
  client.setMaxReconnectAttempts(5);
  fakeWasmClient.setReconnectAttempts(2);

  assert.equal(fakeWasmClient.reconnectConfig.autoReconnect, true);
  assert.equal(fakeWasmClient.reconnectConfig.initialDelayMs, 100n);
  assert.equal(fakeWasmClient.reconnectConfig.maxDelayMs, 2000n);
  assert.equal(fakeWasmClient.reconnectConfig.maxReconnectAttempts, 5);
  assert.equal(client.getReconnectAttempts(), 2);
  assert.equal(client.isReconnecting(), true);
});

test('consumer one-shot batch and ack preserve all consume options', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.none(),
  });
  const fakeWasmClient = createRuntimeCoverageWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  fakeWasmClient.queueConsumeResponse({
    messages: [{
      offset: 9,
      partition_id: 2,
      topic: 'orders',
      group_id: 'billing',
      value: { id: 9 },
    }],
    next_offset: 10,
    has_more: false,
  });

  const batch = await client.consumeBatch({
    topic: 'orders',
    group_id: 'billing',
    start: 'earliest',
    batch_size: 25,
    partition_id: 2,
    timeout_seconds: 15,
    auto_ack: false,
    concurrency_per_partition: 3,
  });

  assert.equal(batch.messages.length, 1);
  assert.deepEqual(fakeWasmClient.consumeCalls[0], {
    topic: 'orders',
    group_id: 'billing',
    start: 'earliest',
    batch_size: 25,
    partition_id: 2,
    timeout_seconds: 15,
    auto_ack: false,
    concurrency_per_partition: 3,
  });

  const ack = await client.ack('orders', 'billing', 2, 9);
  assert.equal(ack.success, true);
  assert.equal(ack.acknowledged_offset, 9);
  assert.equal(fakeWasmClient.ackCalls[0].offset, 9n);
});

test('consumer run supports latest start manual ack and auto ack flows', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.none(),
  });
  const fakeWasmClient = createRuntimeCoverageWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  fakeWasmClient.queueConsumeResponse({
    messages: [{
      offset: 20,
      partition_id: 0,
      topic: 'events',
      group_id: 'latest-reader',
      value: { status: 'new' },
    }],
    next_offset: 21,
    has_more: true,
  });

  const manualHandle = client.consumer({
    topic: 'events',
    group_id: 'latest-reader',
    start: 'latest',
    batch_size: 1,
  });

  let manualCalls = 0;
  await manualHandle.run(async (ctx) => {
    manualCalls += 1;
    assert.equal(ctx.message.offset, 20);
    await ctx.ack();
    manualHandle.stop();
  });

  assert.equal(manualCalls, 1);
  assert.equal(fakeWasmClient.consumeCalls[0].start, 'latest');
  assert.equal(fakeWasmClient.ackCalls.length, 1);

  fakeWasmClient.queueConsumeResponse({
    messages: [{
      offset: 30,
      partition_id: 1,
      topic: 'events',
      group_id: 'auto-reader',
      value: { status: 'auto' },
    }],
    next_offset: 31,
    has_more: true,
  });

  const autoHandle = client.consumer({
    topic: 'events',
    group_id: 'auto-reader',
    auto_ack: true,
    partition_id: 1,
  });

  let autoCalls = 0;
  await autoHandle.run(async (ctx) => {
    autoCalls += 1;
    assert.equal(ctx.message.partition_id, 1);
    autoHandle.stop();
  });

  assert.equal(autoCalls, 1);
  assert.equal(fakeWasmClient.ackCalls.length, 2);
});

test('documented topic publish path uses SQL query calls only', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.none(),
  });
  const fakeWasmClient = createRuntimeCoverageWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  await client.query('CREATE TOPIC support.inbox_events');
  await client.query(`
    ALTER TOPIC support.inbox_events
    ADD SOURCE support.inbox
    ON INSERT
    WITH (payload = 'full')
  `);
  await client.query(
    'INSERT INTO support.inbox (room, role, body) VALUES ($1, $2, $3)',
    ['main', 'user', 'published via source table'],
  );

  assert.equal(fakeWasmClient.queryCalls[0], 'CREATE TOPIC support.inbox_events');
  assert.match(fakeWasmClient.queryCalls[1], /ALTER TOPIC support\.inbox_events/);
  assert.equal(fakeWasmClient.queryWithParamsCalls[0].sql, 'INSERT INTO support.inbox (room, role, body) VALUES ($1, $2, $3)');
});