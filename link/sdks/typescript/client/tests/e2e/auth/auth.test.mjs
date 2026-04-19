/**
 * Auth e2e tests — login, refresh, authProvider, JWT lifecycle.
 *
 * Run: node --test tests/e2e/auth/auth.test.mjs
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import {
  SERVER_URL,
  ADMIN_USER,
  ADMIN_PASS,
  connectJwtClient,
  connectWithAuthProvider,
} from '../helpers.mjs';
import { createClient, Auth } from '../../../dist/src/index.js';

describe('Auth', { timeout: 30_000 }, () => {
  // -----------------------------------------------------------------------
  // Basic login
  // -----------------------------------------------------------------------
  test('login with basic auth returns tokens and user info', async () => {
    const client = createClient({
      url: SERVER_URL,
      authProvider: async () => Auth.basic(ADMIN_USER, ADMIN_PASS),
    });
    const resp = await client.login();

    assert.ok(resp.access_token, 'expected access_token');
    assert.ok(resp.refresh_token, 'expected refresh_token');
    assert.ok(resp.user, 'expected user info');
    assert.equal(typeof resp.user.id, 'string');
    assert.equal(typeof resp.user.role, 'string');
    assert.equal(typeof resp.admin_ui_access, 'boolean');

    await client.disconnect();
  });

  // -----------------------------------------------------------------------
  // Refresh token
  // -----------------------------------------------------------------------
  test('refreshToken returns a valid access token', async () => {
    const client = createClient({
      url: SERVER_URL,
      authProvider: async () => Auth.basic(ADMIN_USER, ADMIN_PASS),
    });
    const login = await client.login();
    const refreshed = await client.refreshToken(login.refresh_token);

    assert.ok(refreshed.access_token, 'expected new access_token');
    assert.ok(refreshed.user, 'expected user info in refreshed response');
    assert.equal(typeof refreshed.access_token, 'string');
    assert.ok(refreshed.access_token.length > 10, 'token should be non-trivial');

    await client.disconnect();
  });

  // -----------------------------------------------------------------------
  // JWT auth
  // -----------------------------------------------------------------------
  test('JWT-only client can query after login', async () => {
    // First, get a token
    const bootstrap = createClient({
      url: SERVER_URL,
      authProvider: async () => Auth.basic(ADMIN_USER, ADMIN_PASS),
    });
    const login = await bootstrap.login();
    await bootstrap.disconnect();

    // Now connect with JWT
    const jwtClient = createClient({
      url: SERVER_URL,
      authProvider: async () => Auth.jwt(login.access_token),
    });
    const resp = await jwtClient.query('SELECT 1 AS val');
    assert.ok(resp.results?.length > 0);
    await jwtClient.disconnect();
  });

  // -----------------------------------------------------------------------
  // authProvider
  // -----------------------------------------------------------------------
  test('authProvider callback is used for authentication', async () => {
    let providerCallCount = 0;

    // Get a JWT first via basic auth
    const bootstrap = createClient({
      url: SERVER_URL,
      authProvider: async () => Auth.basic(ADMIN_USER, ADMIN_PASS),
    });
    const login = await bootstrap.login();
    await bootstrap.disconnect();

    const client = createClient({
      url: SERVER_URL,
      wsLazyConnect: false,
      authProvider: async () => {
        providerCallCount++;
        return Auth.jwt(login.access_token);
      },
    });

    await client.initialize();
    assert.ok(providerCallCount >= 1, 'authProvider should have been called at least once');
    assert.ok(client.isConnected(), 'client should be connected via authProvider');
    await client.disconnect();
  });

  // -----------------------------------------------------------------------
  // Initial auth state
  // -----------------------------------------------------------------------
  test('auth type is unresolved before initialize()', async () => {
    const client = createClient({
      url: SERVER_URL,
      authProvider: async () => Auth.jwt('bootstrap-token'),
    });
    assert.equal(client.getAuthType(), null);
  });

  // -----------------------------------------------------------------------
  // Invalid credentials
  // -----------------------------------------------------------------------
  test('wrong password rejects login', async () => {
    const client = createClient({
      url: SERVER_URL,
      authProvider: async () => Auth.basic(ADMIN_USER, 'wrong-password-xyz'),
    });
    await assert.rejects(
      () => client.login(),
      'login with wrong password should reject',
    );
  });

  // -----------------------------------------------------------------------
  // Validation
  // -----------------------------------------------------------------------
  test('constructor requires url', () => {
    assert.throws(() => createClient({ url: '', authProvider: async () => Auth.jwt('bootstrap-token') }));
  });

  test('constructor requires authProvider', () => {
    assert.throws(() => createClient({ url: SERVER_URL }));
  });
});
