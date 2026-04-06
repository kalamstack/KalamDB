import assert from 'node:assert/strict';
import test from 'node:test';

import {
  createLangChainAdapter,
  runAgent,
  runConsumer,
} from '../dist/src/index.js';

function makeMessage(overrides = {}) {
  return {
    offset: 7,
    partition_id: 0,
    topic: 'blog.summarizer',
    group_id: 'blog-summarizer-agent',
    value: { row: { blog_id: '42', content: 'hello world' } },
    ...overrides,
  };
}

function createMockClient(messages, options = {}) {
  const state = {
    consumerOptions: [],
    ackedOffsets: [],
    queryCalls: [],
  };

  const client = {
    query: async (sql, params) => {
      state.queryCalls.push({ sql, params });
      return { status: 'success', results: [] };
    },
    queryOne: async () => null,
    queryAll: async () => [],
    consumer: (consumeOptions) => {
      state.consumerOptions.push(consumeOptions);
      let stopped = false;

      return {
        run: async (handler) => {
          for (const message of messages) {
            if (stopped) {
              break;
            }

            await handler({
              username: undefined,
              message,
              ack: async () => {
                if (options.ackShouldThrow) {
                  throw new Error('ack failed');
                }
                state.ackedOffsets.push(message.offset);
              },
            });
          }
        },
        stop: () => {
          stopped = true;
        },
      };
    },
  };

  return { client, state };
}

test('runAgent retries and acks once after success', async () => {
  const message = makeMessage();
  const { client, state } = createMockClient([message]);

  let attempts = 0;
  const retries = [];

  await runAgent({
    client,
    name: 'summarizer-agent',
    topic: message.topic,
    groupId: message.group_id,
    retry: {
      maxAttempts: 3,
      initialBackoffMs: 0,
      maxBackoffMs: 0,
    },
    onRetry: (event) => retries.push(event.attempt),
    onRow: async () => {
      attempts += 1;
      if (attempts < 3) {
        throw new Error('transient failure');
      }
    },
  });

  assert.equal(attempts, 3);
  assert.deepEqual(retries, [1, 2]);
  assert.deepEqual(state.ackedOffsets, [7]);
  assert.equal(state.consumerOptions[0].auto_ack, false);
});

test('runAgent calls onFailed and then acks when configured', async () => {
  const message = makeMessage({ offset: 9 });
  const { client, state } = createMockClient([message]);

  let onFailedCalls = 0;
  let failedRunKey = '';

  await runAgent({
    client,
    name: 'summarizer-agent',
    topic: message.topic,
    groupId: message.group_id,
    retry: {
      maxAttempts: 2,
      initialBackoffMs: 0,
      maxBackoffMs: 0,
    },
    onRow: async () => {
      throw new Error('permanent failure');
    },
    onFailed: async (ctx) => {
      onFailedCalls += 1;
      failedRunKey = ctx.runKey;
      await ctx.sql('INSERT INTO blog.summary_failures VALUES ($1)', [ctx.runKey]);
    },
    ackOnFailed: true,
  });

  assert.equal(onFailedCalls, 1);
  assert.match(failedRunKey, /^summarizer-agent:/);
  assert.deepEqual(state.ackedOffsets, [9]);
  assert.equal(state.queryCalls.length, 1);
});

test('runAgent does not ack when onFailed throws', async () => {
  const message = makeMessage({ offset: 11 });
  const { client, state } = createMockClient([message]);

  const errors = [];

  await runAgent({
    client,
    name: 'summarizer-agent',
    topic: message.topic,
    groupId: message.group_id,
    retry: {
      maxAttempts: 1,
      initialBackoffMs: 0,
      maxBackoffMs: 0,
    },
    onRow: async () => {
      throw new Error('always fail');
    },
    onFailed: async () => {
      throw new Error('failed sink write');
    },
    onError: (event) => {
      errors.push(String(event.error));
    },
  });

  assert.deepEqual(state.ackedOffsets, []);
  assert.equal(errors.length, 1);
  assert.match(errors[0], /failed sink write/);
});

test('runAgent exposes llm context with system prompt metadata', async () => {
  const message = makeMessage({
    offset: 13,
    value: { row: { blog_id: '13', content: 'A long blog body' } },
  });
  const { client, state } = createMockClient([message]);

  const llmInputs = [];

  await runAgent({
    client,
    name: 'summarizer-agent',
    topic: message.topic,
    groupId: message.group_id,
    systemPrompt: 'system prompt',
    llm: {
      complete: async (input) => {
        llmInputs.push(input);
        return 'summary';
      },
    },
    onRow: async (ctx) => {
      const result = await ctx.llm.complete('summarize');
      assert.equal(result, 'summary');
      assert.ok(ctx.runKey.includes(':13'));
    },
  });

  assert.equal(llmInputs.length, 1);
  assert.equal(llmInputs[0].systemPrompt, 'system prompt');
  assert.equal(llmInputs[0].prompt, 'summarize');
  assert.deepEqual(state.ackedOffsets, [13]);
});

test('runConsumer delegates to runAgent and processes messages', async () => {
  const message = makeMessage({ offset: 21 });
  const { client, state } = createMockClient([message]);

  let calls = 0;

  await runConsumer({
    client,
    name: 'consumer-runtime',
    topic: message.topic,
    groupId: message.group_id,
    retry: {
      maxAttempts: 1,
      initialBackoffMs: 0,
      maxBackoffMs: 0,
    },
    onMessage: async (ctx) => {
      calls += 1;
      assert.equal(ctx.message.offset, 21);
    },
  });

  assert.equal(calls, 1);
  assert.deepEqual(state.ackedOffsets, [21]);
});

test('createLangChainAdapter normalizes completion and stream outputs', async () => {
  const invokeInputs = [];
  const streamInputs = [];

  const adapter = createLangChainAdapter({
    invoke: async (input) => {
      invokeInputs.push(input);
      return { content: [{ text: 'abc' }] };
    },
    stream: async function* (input) {
      streamInputs.push(input);
      yield { content: [{ text: 'x' }] };
      yield { content: [{ text: 'y' }] };
    },
  });

  const completed = await adapter.complete({
    systemPrompt: 'sys',
    prompt: 'hello',
  });
  assert.equal(completed, 'abc');

  const streamed = [];
  for await (const chunk of adapter.stream({ prompt: 'hello' })) {
    streamed.push(chunk);
  }
  assert.deepEqual(streamed, ['x', 'y']);

  assert.equal(invokeInputs.length, 1);
  assert.equal(streamInputs.length, 1);
});