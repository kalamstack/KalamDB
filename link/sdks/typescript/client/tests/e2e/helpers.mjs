/**
 * Shared test helpers for KalamDB TypeScript SDK e2e tests.
 *
 * Requires a running KalamDB server.
 * Configure via env vars:
 *   KALAMDB_URL      (default: http://localhost:8080)
 *   KALAMDB_USER     (default: admin)
 *   KALAMDB_PASSWORD (default: kalamdb123)
 */

import { createClient, Auth, KalamDBClient } from '../../dist/src/index.js';

export const SERVER_URL = process.env.KALAMDB_URL || 'http://localhost:8080';
export const ADMIN_USER = process.env.KALAMDB_USER || 'admin';
export const ADMIN_PASS = process.env.KALAMDB_PASSWORD || 'kalamdb123';

let sharedJwtPromise = null;

/** Generate a unique name for test isolation (namespace or table). */
export function uniqueName(prefix) {
  const ts = Date.now();
  const rand = Math.random().toString(36).slice(2, 8);
  return `${prefix}_${ts}_${rand}`;
}

async function fetchSharedJwt() {
  const bootstrapClient = createClient({
    url: SERVER_URL,
    authProvider: async () => Auth.basic(ADMIN_USER, ADMIN_PASS),
  });

  try {
    const loginResp = await bootstrapClient.login();
    return loginResp.access_token;
  } finally {
    await bootstrapClient.disconnect().catch(() => {});
  }
}

export async function getSharedJwt() {
  if (!sharedJwtPromise) {
    sharedJwtPromise = fetchSharedJwt().catch((error) => {
      sharedJwtPromise = null;
      throw error;
    });
  }

  return sharedJwtPromise;
}

export function jwtAuthProvider() {
  return async () => Auth.jwt(await getSharedJwt());
}

/**
 * Create a client with eager WebSocket connection enabled.
 *
 * Uses a shared JWT authProvider so the e2e suite does not hammer the login
 * endpoint when many eager clients are created.
 */
export async function connectJwtClient() {
  const client = createClient({
    url: SERVER_URL,
    authProvider: jwtAuthProvider(),
    wsLazyConnect: false,
  });
  await client.initialize();
  return client;
}

/**
 * Create a client with authProvider using the shared cached JWT.
 */
export async function connectWithAuthProvider() {
  const client = createClient({
    url: SERVER_URL,
    wsLazyConnect: false,
    authProvider: jwtAuthProvider(),
  });
  await client.initialize();
  return client;
}

/**
 * Ensure a test namespace exists, return its name.
 */
export async function ensureNamespace(client, name) {
  await client.query(`CREATE NAMESPACE IF NOT EXISTS ${name}`);
  return name;
}

/**
 * Drop a table, ignoring errors.
 */
export async function dropTable(client, fullTable) {
  try {
    await client.query(`DROP TABLE IF EXISTS ${fullTable}`);
  } catch (_) {
    // ignore
  }
}

/** Sleep for ms. */
export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
