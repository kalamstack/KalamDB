/**
 * Client lifecycle e2e tests — initialize, disconnect, callbacks, disableCompression.
 *
 * Run: node --test tests/e2e/lifecycle/lifecycle.test.mjs
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import {
  SERVER_URL,
  connectJwtClient,
  jwtAuthProvider,
  sleep,
} from '../helpers.mjs';
import { createClient, Auth } from '../../../dist/src/index.js';

describe('Client Lifecycle', { timeout: 30_000 }, () => {
  // -----------------------------------------------------------------------
  // Connect / disconnect
  // -----------------------------------------------------------------------
  test('eager initialize then disconnect toggles isConnected', async () => {
    const client = await connectJwtClient();
    assert.equal(client.isConnected(), true);

    await client.disconnect();
    assert.equal(client.isConnected(), false);
  });

  // -----------------------------------------------------------------------
  // Reconnect on disconnect
  // -----------------------------------------------------------------------
  test('setAutoReconnect / setReconnectDelay / setMaxReconnectAttempts', async () => {
    const client = await connectJwtClient();

    // These should not throw
    client.setAutoReconnect(true);
    client.setReconnectDelay(500, 5000);
    client.setMaxReconnectAttempts(3);

    assert.equal(client.getReconnectAttempts(), 0);
    assert.equal(client.isReconnecting(), false);

    await client.disconnect();
  });

  // -----------------------------------------------------------------------
  // disableCompression passes ?compress=false
  // -----------------------------------------------------------------------
  test('disableCompression: true still connects and queries', async () => {
    const client = createClient({
      url: SERVER_URL,
      authProvider: jwtAuthProvider(),
      disableCompression: true,
      wsLazyConnect: false,
    });

    await client.initialize();
    assert.ok(client.isConnected());

    const resp = await client.query("SELECT 'no-compress' AS val");
    assert.ok(resp.results?.length > 0);

    await client.disconnect();
  });

  // -----------------------------------------------------------------------
  // wsLazyConnect
  // -----------------------------------------------------------------------
  test('wsLazyConnect: true keeps query-only usage disconnected', async () => {
    const client = createClient({
      url: SERVER_URL,
      authProvider: jwtAuthProvider(),
      wsLazyConnect: true,
    });

    // Not connected yet.
    assert.equal(client.isConnected(), false);

    // Query uses HTTP only and should not force a WebSocket connection.
    const resp = await client.query('SELECT 1 AS n');
    assert.ok(resp.results?.length > 0);
    assert.equal(client.isConnected(), false);

    await client.disconnect();
  });

  // -----------------------------------------------------------------------
  // Connection event callbacks
  // -----------------------------------------------------------------------
  test('onConnect callback fires', async () => {
    let connectFired = false;

    const client = createClient({
      url: SERVER_URL,
      authProvider: jwtAuthProvider(),
      wsLazyConnect: false,
      onConnect: () => {
        connectFired = true;
      },
    });

    await client.initialize();
    await sleep(500);

    assert.ok(connectFired, 'onConnect should fire');
    await client.disconnect();
  });

  // -----------------------------------------------------------------------
  // Multiple connect calls are idempotent
  // -----------------------------------------------------------------------
  test('calling initialize() twice is safe', async () => {
    const client = createClient({
      url: SERVER_URL,
      authProvider: jwtAuthProvider(),
      wsLazyConnect: false,
    });

    await client.initialize();
    await client.initialize(); // should not throw
    assert.ok(client.isConnected());

    await client.disconnect();
  });
});
