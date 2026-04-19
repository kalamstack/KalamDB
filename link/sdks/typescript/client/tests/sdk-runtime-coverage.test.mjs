import assert from 'node:assert/strict';
import test from 'node:test';

import {
  Auth,
  LogLevel,
  SeqId,
  createClient,
} from '../dist/src/index.js';
import { KalamClient as WasmKalamClient } from '../dist/wasm/kalam_client.js';

function createRuntimeCoverageWasmClient() {
  let connected = false;
  let nextSubscriptionId = 0;
  let reconnectAttempts = 0;
  const subscriptionCallbacks = new Map();
  const liveCallbacks = new Map();
  const subscriptions = [];

  return {
    queryCalls: [],
    queryWithParamsCalls: [],
    insertCalls: [],
    deleteCalls: [],
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
        expires_at: '2026-04-17T12:00:00Z',
        admin_ui_access: true,
        user: { id: 'alice', role: 'user' },
      };
    },
    async refresh_access_token(refreshToken) {
      return {
        access_token: `jwt-for-${refreshToken}`,
        refresh_token: 'refresh-456',
        expires_at: '2026-04-17T12:00:00Z',
        admin_ui_access: true,
        user: { id: 'alice', role: 'user' },
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
    emitSubscriptionRaw(subscriptionId, payload) {
      const callback = subscriptionCallbacks.get(subscriptionId);
      if (!callback) {
        throw new Error(`Missing subscription callback for ${subscriptionId}`);
      }
      callback(payload);
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
    emitLiveError(subscriptionId, code, message) {
      const callback = liveCallbacks.get(subscriptionId);
      if (!callback) {
        throw new Error(`Missing live callback for ${subscriptionId}`);
      }
      callback(JSON.stringify({
        type: 'error',
        subscription_id: subscriptionId,
        ...(code !== undefined ? { code } : {}),
        ...(message !== undefined ? { message } : {}),
      }));
    },
    setReconnectAttempts(value) {
      reconnectAttempts = value;
    },
  };
}

test('queryRows wraps named_rows into KalamRow with typed cell access', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.jwt('coverage-token'),
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
    authProvider: async () => Auth.jwt('coverage-token'),
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
    authProvider: async () => Auth.jwt('coverage-token'),
  });
  const fakeWasmClient = createRuntimeCoverageWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  const snapshots = [];
  const unsub = await client.liveTableRows('demo.tasks', (rows) => {
    snapshots.push(rows.map((row) => row.id.asInt()));
  }, {
    subscriptionOptions: { last_rows: 5, from: SeqId.from('10') },
  });

  assert.equal(fakeWasmClient.liveSubscribeCalls[0].sql, 'SELECT * FROM demo.tasks');
  assert.deepEqual(JSON.parse(fakeWasmClient.liveSubscribeCalls[0].optionsJson), {
    subscription_options: { last_rows: 5, from: '10' },
  });

  fakeWasmClient.emitLiveRows('live-1', [{ id: 1 }, { id: 2 }], 12);
  assert.deepEqual(snapshots[0], [1, 2]);

  await unsub();
});

test('live passes key columns through to Rust materialization', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.jwt('coverage-token'),
  });
  const fakeWasmClient = createRuntimeCoverageWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  const snapshots = [];
  const unsub = await client.live('SELECT * FROM demo.messages', (rows) => {
    snapshots.push(rows.map((row) => row.message_id.asString()));
  }, {
    keyColumn: ['room_id', 'message_id'],
    subscriptionOptions: { last_rows: 10 },
  });

  assert.deepEqual(JSON.parse(fakeWasmClient.liveSubscribeCalls[0].optionsJson), {
    key_columns: ['room_id', 'message_id'],
    subscription_options: { last_rows: 10 },
  });

  fakeWasmClient.emitLiveRows('live-1', [
    { room_id: 'room-1', message_id: 'm-1' },
    { room_id: 'room-1', message_id: 'm-2' },
  ], 21);
  assert.deepEqual(snapshots[0], ['m-1', 'm-2']);

  await unsub();
});

test('login refresh and reconnect helpers delegate to wasm client', async () => {
  const originalFetch = globalThis.fetch;
  const originalWithJwt = WasmKalamClient.withJwt;
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.basic('alice', 'secret'),
  });
  const fakeWasmClient = createRuntimeCoverageWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;
  client.auth = Auth.basic('alice', 'secret');

  globalThis.fetch = async () => ({
    ok: true,
    async json() {
      return {
        access_token: 'jwt-123',
        refresh_token: 'refresh-123',
        expires_at: '2026-04-17T12:00:00Z',
        admin_ui_access: true,
        user: { id: 'alice', role: 'user' },
      };
    },
  });
  WasmKalamClient.withJwt = () => fakeWasmClient;

  try {
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
  } finally {
    globalThis.fetch = originalFetch;
    WasmKalamClient.withJwt = originalWithJwt;
  }
});

test('live uses subscribe fallback when getKey is provided and handles updates/deletes/errors', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.jwt('coverage-token'),
  });
  const fakeWasmClient = createRuntimeCoverageWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  const snapshots = [];
  const errors = [];
  const unsub = await client.live('SELECT * FROM demo.items', (rows) => {
    snapshots.push(rows.map((row) => `${row.id}:${row.title}`));
  }, {
    getKey: (row) => row.id,
    mapRow: (row) => ({
      id: row.id.asString(),
      title: row.title.asString(),
    }),
    subscriptionOptions: { last_rows: 3 },
    onError: (event) => errors.push(`${event.code}:${event.message}`),
  });

  assert.equal(fakeWasmClient.subscribeCalls.length, 1, 'fallback should use subscribeWithSql');
  assert.equal(fakeWasmClient.liveSubscribeCalls.length, 0, 'fallback must not call liveQueryRowsWithSql');
  assert.deepEqual(JSON.parse(fakeWasmClient.subscribeCalls[0].optionsJson), {
    last_rows: 3,
  });

  fakeWasmClient.emitSubscription('sub-1', {
    type: 'initial_data_batch',
    subscription_id: 'sub-1',
    rows: [
      { id: '1', title: 'one' },
      { id: '2', title: 'two' },
    ],
    batch_control: { batch_num: 0, has_more: false, status: 'ready', last_seq_id: null },
  }, 11);

  fakeWasmClient.emitSubscription('sub-1', {
    type: 'change',
    change_type: 'update',
    subscription_id: 'sub-1',
    rows: [{ id: '2', title: 'two-updated' }],
    old_values: [{ id: '2', title: 'two' }],
  }, 12);

  fakeWasmClient.emitSubscription('sub-1', {
    type: 'change',
    change_type: 'delete',
    subscription_id: 'sub-1',
    rows: [],
    old_values: [{ id: '1', title: 'one' }],
  }, 13);

  fakeWasmClient.emitSubscription('sub-1', {
    type: 'error',
    subscription_id: 'sub-1',
    code: 'TEST_ERR',
    message: 'fallback error path',
  }, 14);

  assert.deepEqual(snapshots, [
    ['1:one', '2:two'],
    ['1:one', '2:two-updated'],
    ['2:two-updated'],
  ]);
  assert.deepEqual(errors, ['TEST_ERR:fallback error path']);

  await unsub();
});

test('log listener receives messages at configured level and parse errors are logged', async () => {
  const entries = [];
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.jwt('coverage-token'),
    logLevel: LogLevel.Debug,
    logListener: (entry) => entries.push(entry),
  });
  const fakeWasmClient = createRuntimeCoverageWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  const unsub = await client.subscribeWithSql('SELECT * FROM demo.logs', () => {});
  assert.ok(
    entries.some((entry) => entry.tag === 'subscription' && entry.message.includes('Subscribing to:')),
    'debug subscription start log should be emitted',
  );
  assert.ok(
    entries.some((entry) => entry.tag === 'subscription' && entry.message.includes('Subscribed: id=')),
    'subscription success log should be emitted',
  );

  entries.length = 0;
  client.setLogLevel(LogLevel.Error);
  fakeWasmClient.emitSubscriptionRaw('sub-1', '{not-json');

  assert.ok(entries.length >= 1, 'parse failure should produce an error log entry');
  assert.ok(entries.every((entry) => entry.level === LogLevel.Error));
  assert.ok(entries.some((entry) => entry.message.includes('Failed to parse callback payload')));

  await unsub();
});

test('live normalizes keyColumns and reports default onError fields for live-row errors', async () => {
  const errors = [];
  const snapshots = [];
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.jwt('coverage-token'),
  });
  const fakeWasmClient = createRuntimeCoverageWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  const unsub = await client.live('SELECT * FROM demo.messages', (rows) => {
    snapshots.push(rows.length);
  }, {
    limit: 50,
    keyColumns: [' room_id ', '', 'room_id', 'message_id'],
    onError: (event) => errors.push(event),
  });

  assert.deepEqual(JSON.parse(fakeWasmClient.liveSubscribeCalls[0].optionsJson), {
    limit: 50,
    key_columns: ['room_id', 'message_id'],
  });

  fakeWasmClient.emitLiveError('live-1');

  assert.equal(snapshots.length, 0, 'rows callback should not run for live error events');
  assert.equal(errors.length, 1, 'onError should receive live error events');
  assert.equal(errors[0].code, 'unknown');
  assert.equal(errors[0].message, 'Live query failed');
  assert.equal(errors[0].subscription_id, 'live-1');

  await unsub();
});

