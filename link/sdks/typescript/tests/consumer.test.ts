/**
 * Consumer API integration tests for KalamDB TypeScript SDK
 *
 * Tests the ergonomic consumer builder pattern:
 *   client.consumer({ topic, group_id, auto_ack }).run(async (msg, ctx) => { ... });
 *
 * Run type checks with: npx tsc --noEmit tests/consumer.test.ts
 */

import {
  Auth,
  createClient,
  type ConsumeRequest,
  type ConsumeResponse,
  type ConsumeMessage,
  type AckResponse,
  type ConsumerHandle,
  type ConsumerHandler,
  type ConsumeContext,
} from '../src/index';

/* ================================================================== */
/*  Type-safety compile-time tests                                   */
/* ================================================================== */

// ConsumeRequest: required fields only
const minimalRequest: ConsumeRequest = {
  topic: 'orders',
  group_id: 'billing',
};

// ConsumeRequest: all fields
const fullRequest: ConsumeRequest = {
  topic: 'chat.events',
  group_id: 'processor',
  start: 'earliest',
  batch_size: 50,
  partition_id: 2,
  timeout_seconds: 30,
  auto_ack: true,
  concurrency_per_partition: 4,
};

// ConsumeMessage: access all typed fields
function inspectMessage(msg: ConsumeMessage): void {
  const offset: number = msg.offset;
  const partition: number = msg.partition_id;
  const topic: string = msg.topic;
  const groupId: string = msg.group_id;
  const value = msg.value; // JsonValue

  // Optional fields
  const messageId: string | undefined = msg.message_id;
  const sourceTable: string | undefined = msg.source_table;
  const op: string | undefined = msg.op;
  const timestampMs: number | undefined = msg.timestamp_ms;
}

// ConsumeResponse: access all typed fields
function inspectResponse(resp: ConsumeResponse): void {
  const messages: ConsumeMessage[] = resp.messages;
  const nextOffset: number = resp.next_offset;
  const hasMore: boolean = resp.has_more;
}

// AckResponse: access all typed fields
function inspectAckResponse(resp: AckResponse): void {
  const success: boolean = resp.success;
  const ackedOffset: number = resp.acknowledged_offset;
}

/* ================================================================== */
/*  Consumer builder pattern tests                                   */
/* ================================================================== */

async function testConsumerAutoAck() {
  const client = createClient({
    url: 'http://localhost:8080',
    authProvider: async () => Auth.basic('admin', 'kalamdb123'),
  });

  // Auto-ack consumer: messages are acknowledged automatically after handler
  const handle: ConsumerHandle = client.consumer({
    topic: 'orders',
    group_id: 'billing',
    auto_ack: true,
    batch_size: 10,
  });

  // Handler receives typed message, no need to ack manually
  // (though ctx is still available if needed)
  const processed: ConsumeMessage[] = [];
  setTimeout(() => handle.stop(), 5000); // stop after 5s for testing

  await handle.run(async (msg: ConsumeMessage) => {
    processed.push(msg);
    console.log(`[auto-ack] Processed order at offset ${msg.offset}:`, msg.value);
  });

  console.log(`✅ Auto-ack consumer processed ${processed.length} messages`);
}

async function testConsumerManualAck() {
  const client = createClient({
    url: 'http://localhost:8080',
    authProvider: async () => Auth.basic('admin', 'kalamdb123'),
  });

  // Manual-ack consumer: user must call ctx.ack() to acknowledge
  const handle = client.consumer({
    topic: 'orders',
    group_id: 'billing-manual',
    batch_size: 5,
  });

  const processed: ConsumeMessage[] = [];
  setTimeout(() => handle.stop(), 5000);

  await handle.run(async (msg: ConsumeMessage, ctx: ConsumeContext) => {
    // Process the message first
    processed.push(msg);
    console.log(`[manual-ack] Processing order at offset ${msg.offset}...`);

    // Then explicitly acknowledge
    await ctx.ack();
    console.log(`[manual-ack] Acknowledged offset ${msg.offset}`);
  });

  console.log(`✅ Manual-ack consumer processed ${processed.length} messages`);
}

async function testConsumeBatch() {
  const client = createClient({
    url: 'http://localhost:8080',
    authProvider: async () => Auth.basic('admin', 'kalamdb123'),
  });

  // One-shot batch consume (no loop)
  const response: ConsumeResponse = await client.consumeBatch({
    topic: 'orders',
    group_id: 'batch-reader',
    batch_size: 20,
    start: 'earliest',
  });

  console.log(`Got ${response.messages.length} messages`);
  console.log(`Next offset: ${response.next_offset}`);
  console.log(`Has more: ${response.has_more}`);

  for (const msg of response.messages) {
    inspectMessage(msg);
  }

  // Explicitly ack the batch
  if (response.messages.length > 0) {
    const lastMsg = response.messages[response.messages.length - 1];
    const ackResult: AckResponse = await client.ack(
      lastMsg.topic,
      lastMsg.group_id,
      lastMsg.partition_id,
      lastMsg.offset,
    );
    console.log(`Ack success: ${ackResult.success}, offset: ${ackResult.acknowledged_offset}`);
  }

  console.log('✅ consumeBatch test passed');
}

async function testConsumerStop() {
  const client = createClient({
    url: 'http://localhost:8080',
    authProvider: async () => Auth.basic('admin', 'kalamdb123'),
  });

  const handle = client.consumer({
    topic: 'orders',
    group_id: 'stop-test',
    auto_ack: true,
  });

  let messageCount = 0;

  // Stop after processing 3 messages
  const runPromise = handle.run(async (ctx) => {
    messageCount++;
    console.log(`[stop-test] Message #${messageCount}`);
    if (messageCount >= 3) {
      handle.stop();
    }
  });

  await runPromise;
  console.log(`✅ Consumer stopped after ${messageCount} messages`);
}

async function testConsumerHandlerTypeSafety() {
  const client = createClient({
    url: 'http://localhost:8080',
    authProvider: async () => Auth.basic('admin', 'kalamdb123'),
  });

  // Demonstrate typed handler with specific business logic
  const handler: ConsumerHandler = async (ctx) => {
    const msg = ctx.message;
    // TypeScript knows these types at compile time
    if (msg.op === 'insert') {
      console.log(`New record in ${msg.source_table} by ${ctx.username}:`, msg.value);
    } else if (msg.op === 'update') {
      console.log(`Updated record at offset ${msg.offset}`);
    } else if (msg.op === 'delete') {
      console.log(`Deleted record, timestamp: ${msg.timestamp_ms}`);
    }

    // ctx.ack() is properly typed as () => Promise<void>
    await ctx.ack();
  };

  const handle = client.consumer({
    topic: 'user-events',
    group_id: 'audit-log',
    partition_id: 0,
  });

  // handler type-checks correctly with .run()
  setTimeout(() => handle.stop(), 1000);
  await handle.run(handler);

  console.log('✅ Handler type safety test passed');
}

/* ================================================================== */
/*  Negative type-safety tests (would fail compilation)              */
/* ================================================================== */

// These are commented out but can be uncommented to verify they cause type errors:

// Missing required 'topic':
// const badReq1: ConsumeRequest = { group_id: 'billing' };

// Missing required 'group_id':
// const badReq2: ConsumeRequest = { topic: 'orders' };

// offset is number, not string:
// function badMessageAccess(msg: ConsumeMessage) { const x: string = msg.offset; }

// success is boolean, not string:
// function badAckAccess(resp: AckResponse) { const x: string = resp.success; }

console.log('✅ All consumer type definitions are valid');
