import assert from 'node:assert/strict';
import test from 'node:test';

import { Auth, createClient } from '../dist/src/index.js';

function createFakeWasmClient({ subscribeError, disconnectError } = {}) {
  let connected = false;
  let nextSubscriptionId = 0;
  const callbacks = new Map();
  const subscriptions = [];

  return {
    connectCalls: 0,
    subscribeCalls: 0,
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
      await new Promise((resolve) => setTimeout(resolve, 10));
      connected = true;
    },
    async subscribeWithSql(_sql, _optionsJson, callback) {
      this.subscribeCalls += 1;
      if (subscribeError) {
        throw new Error(subscribeError);
      }
      nextSubscriptionId += 1;
      const subscriptionId = `sub-${nextSubscriptionId}`;
      callbacks.set(subscriptionId, callback);
      subscriptions.push({ id: subscriptionId, query: _sql });
      return subscriptionId;
    },
    async liveQueryRowsWithSql(_sql, _optionsJson, callback) {
      this.subscribeCalls += 1;
      if (subscribeError) {
        throw new Error(subscribeError);
      }
      nextSubscriptionId += 1;
      const subscriptionId = `sub-${nextSubscriptionId}`;
      callbacks.set(subscriptionId, callback);
      subscriptions.push({ id: subscriptionId, query: _sql });
      return subscriptionId;
    },
    async unsubscribe(subscriptionId) {
      callbacks.delete(subscriptionId);
      const index = subscriptions.findIndex((sub) => sub.id === subscriptionId);
      if (index >= 0) {
        subscriptions.splice(index, 1);
      }
    },
    async disconnect() {
      connected = false;
      subscriptions.splice(0, subscriptions.length);
      if (disconnectError) {
        throw new Error(disconnectError);
      }
    },
    emit(subscriptionId, event) {
      const callback = callbacks.get(subscriptionId);
      if (!callback) {
        throw new Error(`No callback registered for ${subscriptionId}`);
      }
      callback(JSON.stringify(event));
    },
    getSubscriptions() {
      return JSON.stringify(subscriptions);
    },
    dropRemoteSubscription(subscriptionId) {
      const index = subscriptions.findIndex((sub) => sub.id === subscriptionId);
      if (index >= 0) {
        subscriptions.splice(index, 1);
      }
    },
  };
}

test('multiple subscriptions on one client share one websocket connection', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.jwt('fixture-token'),
  });

  const fakeWasmClient = createFakeWasmClient();

  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  const [unsubscribeMessages, unsubscribeEvents] = await Promise.all([
    client.subscribeWithSql('SELECT * FROM chat_demo.messages', () => {}),
    client.subscribeWithSql('SELECT * FROM chat_demo.agent_events', () => {}),
  ]);

  assert.equal(fakeWasmClient.connectCalls, 1);
  assert.equal(fakeWasmClient.subscribeCalls, 2);
  assert.equal(client.getSubscriptionCount(), 2);

  await unsubscribeMessages();
  await unsubscribeEvents();

  assert.equal(client.getSubscriptionCount(), 0);
});

test('failed subscriptions do not leak local subscription state', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.jwt('fixture-token'),
  });

  const fakeWasmClient = createFakeWasmClient({
    subscribeError: 'Subscription failed (NOT_FOUND): table missing',
  });

  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  await assert.rejects(
    client.subscribeWithSql('SELECT * FROM missing.table', () => {}),
    /Subscription failed \(NOT_FOUND\): table missing/,
  );

  assert.equal(fakeWasmClient.connectCalls, 1);
  assert.equal(fakeWasmClient.subscribeCalls, 1);
  assert.equal(client.getSubscriptionCount(), 0);
});

test('getSubscriptions trusts wasm empty snapshots over stale local metadata', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.jwt('fixture-token'),
  });

  const fakeWasmClient = createFakeWasmClient();

  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  await client.subscribeWithSql('SELECT * FROM chat_demo.messages', () => {});
  assert.equal(client.getSubscriptionCount(), 1);

  fakeWasmClient.dropRemoteSubscription('sub-1');
  assert.equal(client.getSubscriptionCount(), 0);
});

test('disconnect clears local subscription metadata even when wasm disconnect fails', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.jwt('fixture-token'),
  });

  const fakeWasmClient = createFakeWasmClient({
    disconnectError: 'disconnect failed',
  });

  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  await client.subscribeWithSql('SELECT * FROM chat_demo.messages', () => {});
  await client.subscribeWithSql('SELECT * FROM chat_demo.agent_events', () => {});
  assert.equal(client.getSubscriptionCount(), 2);

  await assert.rejects(client.disconnect(), /disconnect failed/);
  assert.equal(client.getSubscriptionCount(), 0);
});

test('subscribeWithSql normalizes websocket rows into RowData cells', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.jwt('fixture-token'),
  });

  const fakeWasmClient = createFakeWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  const events = [];
  const unsubscribe = await client.subscribeWithSql('SELECT * FROM chat_demo.messages', (event) => {
    events.push(event);
  });

  fakeWasmClient.emit('sub-1', {
    type: 'initial_data_batch',
    subscription_id: 'sub-1',
    rows: [{ id: '1', content: 'hello', created_at: '123' }],
    batch_control: { batch_num: 1, has_more: false, status: 'ready', last_seq_id: null },
  });

  assert.equal(events.length, 1);
  assert.equal(events[0].rows[0].id.asString(), '1');
  assert.equal(events[0].rows[0].content.asString(), 'hello');

  await unsubscribe();
});

test('live delegates materialized rows to the Rust/WASM layer', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.jwt('fixture-token'),
  });

  const fakeWasmClient = createFakeWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  const snapshots = [];
  const unsubscribe = await client.live(
    'SELECT * FROM chat_demo.messages',
    (rows) => {
      snapshots.push(rows.map((row) => ({
        id: row.id.asString(),
        content: row.content.asString(),
      })));
    },
  );

  fakeWasmClient.emit('sub-1', {
    type: 'rows',
    subscription_id: 'sub-1',
    rows: [{ id: '1', content: 'one' }],
  });
  fakeWasmClient.emit('sub-1', {
    type: 'rows',
    subscription_id: 'sub-1',
    rows: [
      { id: '1', content: 'one' },
      { id: '2', content: 'two' },
    ],
  });
  fakeWasmClient.emit('sub-1', {
    type: 'rows',
    subscription_id: 'sub-1',
    rows: [
      { id: '1', content: 'one' },
      { id: '2', content: 'two-updated' },
    ],
  });
  fakeWasmClient.emit('sub-1', {
    type: 'rows',
    subscription_id: 'sub-1',
    rows: [
      { id: '2', content: 'two-updated' },
      { id: '3', content: 'three' },
    ],
  });
  fakeWasmClient.emit('sub-1', {
    type: 'rows',
    subscription_id: 'sub-1',
    rows: [{ id: '3', content: 'three' }],
  });

  assert.deepEqual(snapshots[0], [{ id: '1', content: 'one' }]);
  assert.deepEqual(snapshots[1], [
    { id: '1', content: 'one' },
    { id: '2', content: 'two' },
  ]);
  assert.deepEqual(snapshots[2], [
    { id: '1', content: 'one' },
    { id: '2', content: 'two-updated' },
  ]);
  assert.deepEqual(snapshots[3], [
    { id: '2', content: 'two-updated' },
    { id: '3', content: 'three' },
  ]);
  assert.deepEqual(snapshots[4], [{ id: '3', content: 'three' }]);

  await unsubscribe();
});

test('parallel subscribe storms connect once and keep sibling subscriptions isolated', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.jwt('fixture-token'),
  });

  const fakeWasmClient = createFakeWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  const eventBatches = Array.from({ length: 12 }, () => []);
  const unsubs = await Promise.all(
    eventBatches.map((events, index) =>
      client.subscribeWithSql(
        `SELECT * FROM chat_demo.messages WHERE id = ${index + 1}`,
        (event) => {
          events.push(event);
        },
      )),
  );

  assert.equal(fakeWasmClient.connectCalls, 1);
  assert.equal(fakeWasmClient.subscribeCalls, 12);
  assert.equal(client.getSubscriptionCount(), 12);

  fakeWasmClient.emit('sub-5', {
    type: 'change',
    change_type: 'insert',
    subscription_id: 'sub-5',
    rows: [{ id: 5, content: 'five' }],
    old_values: [],
  });
  fakeWasmClient.emit('sub-9', {
    type: 'change',
    change_type: 'insert',
    subscription_id: 'sub-9',
    rows: [{ id: 9, content: 'nine' }],
    old_values: [],
  });

  assert.equal(eventBatches[4].length, 1);
  assert.equal(eventBatches[8].length, 1);
  assert.equal(eventBatches[4][0].rows[0].id.asInt(), 5);
  assert.equal(eventBatches[8][0].rows[0].id.asInt(), 9);

  await unsubs[4]();
  assert.equal(client.getSubscriptionCount(), 11);

  fakeWasmClient.emit('sub-9', {
    type: 'change',
    change_type: 'insert',
    subscription_id: 'sub-9',
    rows: [{ id: 9, content: 'nine-again' }],
    old_values: [],
  });

  assert.equal(eventBatches[4].length, 1);
  assert.equal(eventBatches[8].length, 2);
  assert.equal(eventBatches[8][1].rows[0].content.asString(), 'nine-again');

  await Promise.all(unsubs.filter((_, index) => index !== 4).map((unsub) => unsub()));
  assert.equal(client.getSubscriptionCount(), 0);
});

// ---------------------------------------------------------------------------
// Cross-subscription isolation: change events must only reach the matching
// subscription callback, even when multiple subscriptions share one connection.
// This is a regression test for the bug where events keyed by the server's
// subscription_id were routed via `ends_with()` and could match the wrong
// client-side subscription, causing cross-subscription event leakage.
// ---------------------------------------------------------------------------
test('change events are delivered only to the matching subscription (cross-subscription isolation)', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.jwt('fixture-token'),
  });

  const fakeWasmClient = createFakeWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  const eventsA = [];
  const eventsB = [];

  const [unsubA, unsubB] = await Promise.all([
    client.subscribeWithSql('SELECT * FROM ns.table_a', (event) => eventsA.push(event)),
    client.subscribeWithSql('SELECT * FROM ns.table_b', (event) => eventsB.push(event)),
  ]);

  // The fake WASM client assigns sub-1 to table_a and sub-2 to table_b.
  // Emit a change only on sub-1 (table_a).
  fakeWasmClient.emit('sub-1', {
    type: 'change',
    change_type: 'insert',
    subscription_id: 'sub-1',
    rows: [{ id: '10', value: 'row-for-a' }],
    old_values: [],
  });

  // Only eventsA should receive the event; eventsB must remain empty.
  assert.equal(eventsA.length, 1, 'subscription A should receive its own change event');
  assert.equal(eventsB.length, 0, 'subscription B must NOT receive events targeted at subscription A');
  assert.equal(eventsA[0].rows[0].id.asString(), '10');

  // Now emit a change only on sub-2 (table_b).
  fakeWasmClient.emit('sub-2', {
    type: 'change',
    change_type: 'insert',
    subscription_id: 'sub-2',
    rows: [{ id: '20', value: 'row-for-b' }],
    old_values: [],
  });

  // eventsA must still have exactly one event; eventsB gets its own event.
  assert.equal(eventsA.length, 1, 'subscription A must NOT receive events targeted at subscription B');
  assert.equal(eventsB.length, 1, 'subscription B should receive its own change event');
  assert.equal(eventsB[0].rows[0].id.asString(), '20');

  await unsubA();
  await unsubB();
  assert.equal(client.getSubscriptionCount(), 0);
});

test('same SQL subscribed twice keeps events isolated by subscription_id', async () => {
  const client = createClient({
    url: 'http://127.0.0.1:8080',
    authProvider: async () => Auth.jwt('fixture-token'),
  });

  const fakeWasmClient = createFakeWasmClient();
  client.initialized = true;
  client.wasmClient = fakeWasmClient;

  const sql = 'SELECT * FROM dba.favorites';
  const eventsFirst = [];
  const eventsSecond = [];

  const [unsubFirst, unsubSecond] = await Promise.all([
    client.subscribeWithSql(sql, (event) => eventsFirst.push(event)),
    client.subscribeWithSql(sql, (event) => eventsSecond.push(event)),
  ]);

  assert.equal(fakeWasmClient.connectCalls, 1);
  assert.equal(fakeWasmClient.subscribeCalls, 2);
  assert.equal(client.getSubscriptionCount(), 2);

  // Emit only for the first subscription id.
  fakeWasmClient.emit('sub-1', {
    type: 'change',
    change_type: 'update',
    subscription_id: 'sub-1',
    rows: [{ id: 'sql-studio-state:admin:workspace', payload: '{"v":1}' }],
    old_values: [{ id: 'sql-studio-state:admin:workspace', payload: '{"v":0}' }],
  });

  assert.equal(eventsFirst.length, 1, 'first subscription should receive sub-1 events');
  assert.equal(eventsSecond.length, 0, 'second subscription must not receive sub-1 events');
  assert.equal(eventsFirst[0].subscription_id, 'sub-1');

  // Emit only for the second subscription id.
  fakeWasmClient.emit('sub-2', {
    type: 'change',
    change_type: 'update',
    subscription_id: 'sub-2',
    rows: [{ id: 'sql-studio-state:admin:workspace', payload: '{"v":2}' }],
    old_values: [{ id: 'sql-studio-state:admin:workspace', payload: '{"v":1}' }],
  });

  assert.equal(eventsFirst.length, 1, 'first subscription must not receive sub-2 events');
  assert.equal(eventsSecond.length, 1, 'second subscription should receive sub-2 events');
  assert.equal(eventsSecond[0].subscription_id, 'sub-2');

  await unsubFirst();
  await unsubSecond();
  assert.equal(client.getSubscriptionCount(), 0);
});
