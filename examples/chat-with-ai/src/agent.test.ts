import test from 'node:test';
import assert from 'node:assert/strict';
import { toPendingMessage } from './agent.js';

test('toPendingMessage unwraps query rows into a replyable message', () => {
  const row = toPendingMessage({
    id: { asString: () => 'msg-1' },
    role: { asString: () => 'user' },
    room: { asString: () => 'qa-room' },
    sender_username: { asString: () => 'admin' },
    content: { asString: () => 'latency spike' },
    created_at: { asInt: () => 1234 },
  });

  assert.deepEqual(row, {
    id: 'msg-1',
    room: 'qa-room',
    senderUsername: 'admin',
    content: 'latency spike',
  });
});