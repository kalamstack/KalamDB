import assert from 'node:assert/strict';
import test from 'node:test';

import { runAgent } from '../dist/src/index.js';

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
    executeAsUser: async (sql, user, params) => {
      state.executeAsUserCalls.push({ sql, user, params });
      return { status: 'success', results: [] };
    },
    consumer: (consumeOptions) => {
      state.consumerOptions.push(consumeOptions);
      return {
        run: async (handler) => {
          for (const message of messages) {
            await handler({
              user: 'alice',
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

test('README runAgent example writes back through executeAsUser inside the user tenant', async () => {
  const message = {
    offset: 7,
    partition_id: 0,
    topic: 'support.inbox_events',
    group_id: 'support-summary-agent',
    payload: {
      body: 'Please summarize this support thread',
      _table: 'support.inbox',
    },
    value: {
      body: 'Please summarize this support thread',
      _table: 'support.inbox',
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
      const user = String(ctx.user ?? '').trim();
      const body = String(row.body ?? '').trim();
      if (!user || !body) {
        return;
      }

      const summary = `Support summary: ${body.slice(0, 120)}`;
      await client.executeAsUser(
        'INSERT INTO support.inbox (room, role, body) VALUES ($1, $2, $3)',
        user,
        ['main', 'assistant', summary],
      );
    },
  });

  assert.equal(state.executeAsUserCalls.length, 1);
  assert.equal(state.executeAsUserCalls[0].user, 'alice');
  assert.deepEqual(state.executeAsUserCalls[0].params, [
    'main',
    'assistant',
    'Support summary: Please summarize this support thread',
  ]);
  assert.deepEqual(state.ackedOffsets, [7]);
});