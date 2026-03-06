/**
 * Type safety tests for KalamDB TypeScript SDK
 * Ensures TypeScript definitions are correct
 * Run with: npx tsc --noEmit tests/types.test.ts
 */

import {
  Auth,
  KalamDBClient,
  MessageType,
  ChangeType,
  createClient,
  type BatchStatus,
  type QueryResponse,
  type ServerMessage,
  type SubscriptionOptions,
  type ConsumeRequest,
  type ConsumeResponse,
  type ConsumeMessage,
  type AckResponse,
  type ConsumerHandle,
  type ConsumerHandler,
  type ConsumeContext,
  type Unsubscribe,
  type LoginResponse,
} from '../src/index';

// Test: Constructor with Auth.basic
const client1 = new KalamDBClient({
  url: 'http://localhost:8080',
  authProvider: async () => Auth.basic('user', 'pass'),
});

// Test: Constructor with Auth.jwt
const client2 = new KalamDBClient({
  url: 'http://localhost:8080',
  authProvider: async () => Auth.jwt('eyJhbGci...'),
});

// Test: Constructor with Auth.none
const client3 = new KalamDBClient({
  url: 'http://localhost:8080',
  authProvider: async () => Auth.none(),
});

// Test: Factory function
const client4 = createClient({
  url: 'http://localhost:8080',
  authProvider: async () => Auth.basic('admin', 'admin'),
});

// Test: Factory function with wsLazyConnect
const client5 = createClient({
  url: 'http://localhost:8080',
  authProvider: async () => Auth.basic('admin', 'admin'),
  wsLazyConnect: true,
});

// Test: wsLazyConnect defaults to true — connection is managed internally
const client6 = createClient({
  url: 'http://localhost:8080',
  authProvider: async () => Auth.basic('admin', 'admin'),
  wsLazyConnect: false,
});

// Test: getAuthType
const authType: 'basic' | 'jwt' | 'none' = client1.getAuthType();

// Test: Async methods return promises
async function testMethods() {
  const client = createClient({
    url: 'http://localhost:8080',
    authProvider: async () => Auth.basic('user', 'pass'),
  });

  // Connection methods
  const disconnectResult: void = await client.disconnect();
  const isConnected: boolean = client.isConnected();

  // Query methods
  const queryResult: QueryResponse = await client.query('SELECT 1');
  const insertResult: QueryResponse = await client.insert('table', { id: 1 });
  const deleteResult: void = await client.delete('table', '123');

  // Subscription methods
  const unsub: Unsubscribe = await client.subscribe('table', (event: ServerMessage) => {
    if (event.type === 'change') {
      const ct = event.change_type;
      const rows = event.rows;
    } else if (event.type === 'initial_data_batch') {
      const rows = event.rows;
      const bc = event.batch_control;
    } else if (event.type === 'error') {
      const code: string = event.code;
      const message: string = event.message;
    }
  });

  // Subscription with options
  const opts: SubscriptionOptions = { batch_size: 50, last_rows: 100 };
  const unsub2 = await client.subscribeWithSql(
    'SELECT * FROM chat.messages',
    (event) => {},
    opts,
  );

  await unsub();
  await unsub2();

  // Subscription management
  const count: number = client.getSubscriptionCount();
  const subbed: boolean = client.isSubscribedTo('table');
  await client.unsubscribeAll();

  // Reconnection
  client.setAutoReconnect(true);
  client.setReconnectDelay(1000, 30000);
  client.setMaxReconnectAttempts(5);

  // Login returns LoginResponse
  const loginResponse: LoginResponse = await client.login();
  const accessToken: string = loginResponse.access_token;

  // -------- Consumer API --------

  // Test: ConsumeRequest is type-safe (all fields)
  const consumeReq: ConsumeRequest = {
    topic: 'orders',
    group_id: 'billing',
    start: 'latest',
    batch_size: 10,
    partition_id: 0,
    auto_ack: false,
    concurrency_per_partition: 1,
  };

  // Test: ConsumeRequest with minimal fields (topic + group_id required)
  const minimalReq: ConsumeRequest = {
    topic: 'orders',
    group_id: 'billing',
  };

  // Test: consumer() returns ConsumerHandle with run() and stop()
  const handle: ConsumerHandle = client.consumer({
    topic: 'orders',
    group_id: 'billing',
    auto_ack: true,
    batch_size: 10,
  });

  // Test: ConsumerHandler type signature
  const handler: ConsumerHandler = async (ctx: ConsumeContext) => {
    const msg = ctx.message;
    // Access typed message fields
    const offset: number = msg.offset;
    const partition: number = msg.partition_id;
    const topic: string = msg.topic;
    const groupId: string = msg.group_id;
    const value = msg.value; // JsonValue (any)
    const messageId: string | undefined = msg.message_id;
    const sourceTable: string | undefined = msg.source_table;
    const op: string | undefined = msg.op;
    const timestampMs: number | undefined = msg.timestamp_ms;
    // Access username from context
    const username = ctx.username;

    // Manual ack via context
    await ctx.ack();
  };

  // Test: run() with auto-ack (handler doesn't need ctx)
  const autoAckHandle = client.consumer({
    topic: 'chat.events',
    group_id: 'processor',
    auto_ack: true,
  });
  // autoAckHandle.run(async (ctx) => { console.log(ctx.message.value); });

  // Test: run() with manual ack (handler uses ctx.ack())
  const manualAckHandle = client.consumer({
    topic: 'chat.events',
    group_id: 'processor',
  });
  // manualAckHandle.run(async (ctx) => { await processMsg(ctx.message); await ctx.ack(); });

  // Test: stop() is synchronous
  handle.stop();

  // Test: consumeBatch() one-shot API
  const batchResult: ConsumeResponse = await client.consumeBatch({
    topic: 'orders',
    group_id: 'billing',
    batch_size: 5,
  });
  const messages: ConsumeMessage[] = batchResult.messages;
  const nextOffset: number = batchResult.next_offset;
  const hasMore: boolean = batchResult.has_more;

  // Test: ack() low-level API
  const ackResult: AckResponse = await client.ack('orders', 'billing', 0, 42);
  const success: boolean = ackResult.success;
  const ackedOffset: number = ackResult.acknowledged_offset;
}

// Test: QueryResponse structure
const response: QueryResponse = {
  status: 'success',
  results: [
    {
      schema: [
        { name: 'id', data_type: 'BigInt', index: 0 },
        { name: 'name', data_type: 'Text', index: 1 },
      ],
      rows: [[1, 'test']],
      row_count: 1,
    },
  ],
  took: 15.5,
};

// Test: Error response
const errorResponse: QueryResponse = {
  status: 'error',
  results: [],
  error: {
    code: 'ERR_TABLE_NOT_FOUND',
    message: 'Table does not exist',
  },
};

// Test: Enum values
const _mt: MessageType = MessageType.Change;
const _ct: ChangeType = ChangeType.Insert;
// BatchStatus is a type-only export from WASM (not a runtime value)
const _bs: BatchStatus = 'ready';

console.log('✅ Type definitions are valid');
