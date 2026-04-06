import assert from 'node:assert/strict';
import test from 'node:test';

import {
  isLikelyTransientAuthProviderError,
  resolveAuthProviderWithRetry,
} from '../dist/src/helpers/auth_provider_retry.js';

test('resolveAuthProviderWithRetry retries transient errors then succeeds', async () => {
  let attempts = 0;
  const sleeps = [];

  const creds = await resolveAuthProviderWithRetry(
    async () => {
      attempts += 1;
      if (attempts < 3) {
        throw new Error('network timeout');
      }
      return { type: 'jwt', token: 'token-123' };
    },
    {
      maxAttempts: 4,
      initialBackoffMs: 25,
      maxBackoffMs: 100,
      sleep: async (ms) => {
        sleeps.push(ms);
      },
    },
  );

  assert.equal(attempts, 3);
  assert.equal(creds.type, 'jwt');
  assert.equal(creds.token, 'token-123');
  assert.deepEqual(sleeps, [25, 50]);
});

test('resolveAuthProviderWithRetry fails fast for non-transient errors', async () => {
  let attempts = 0;
  const sleeps = [];

  await assert.rejects(
    () =>
      resolveAuthProviderWithRetry(
        async () => {
          attempts += 1;
          throw new Error('invalid credentials');
        },
        {
          maxAttempts: 5,
          initialBackoffMs: 25,
          sleep: async (ms) => {
            sleeps.push(ms);
          },
        },
      ),
    /invalid credentials/,
  );

  assert.equal(attempts, 1);
  assert.deepEqual(sleeps, []);
});

test('isLikelyTransientAuthProviderError detects network-like messages', () => {
  assert.equal(isLikelyTransientAuthProviderError(new Error('network-request-failed')), true);
  assert.equal(isLikelyTransientAuthProviderError(new Error('503 Service Unavailable')), true);
  assert.equal(isLikelyTransientAuthProviderError(new Error('invalid credentials')), false);
});
