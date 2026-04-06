/**
 * Basic tests for KalamDB TypeScript SDK
 * Run with: node --test tests/basic.test.js
 */

import { test } from 'node:test';
import assert from 'node:assert';
import { KalamDBClient } from '../dist/index.js';

test('KalamDBClient - constructor validation', async (t) => {
  await t.test('should throw error for empty URL', () => {
    assert.throws(
      () => new KalamDBClient('', 'user', 'pass'),
      /url parameter is required/
    );
  });

  await t.test('should throw error for empty username', () => {
    assert.throws(
      () => new KalamDBClient('http://localhost:8080', '', 'pass'),
      /username parameter is required/
    );
  });

  await t.test('should throw error for empty password', () => {
    assert.throws(
      () => new KalamDBClient('http://localhost:8080', 'user', ''),
      /password parameter is required/
    );
  });

  await t.test('should create client with valid parameters', () => {
    const client = new KalamDBClient('http://localhost:8080', 'user', 'pass');
    assert.ok(client instanceof KalamDBClient);
  });
});

test('KalamDBClient - connection state', async (t) => {
  const client = new KalamDBClient('http://localhost:8080', 'testuser', 'testpass');

  await t.test('should be disconnected initially', () => {
    assert.strictEqual(client.isConnected(), false);
  });
});
