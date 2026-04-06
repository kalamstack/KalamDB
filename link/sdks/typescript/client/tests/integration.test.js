/**
 * Integration tests for KalamDB TypeScript SDK
 * Requires running KalamDB server on localhost:8080
 * Run with: node --test tests/integration.test.js
 */

import { test } from 'node:test';
import assert from 'node:assert';
import { KalamDBClient } from '../dist/index.js';

// Skip if NO_SERVER environment variable is set
const SKIP_INTEGRATION = process.env.NO_SERVER === 'true';

if (SKIP_INTEGRATION) {
  console.log('⏭️  Skipping integration tests (NO_SERVER=true)');
  process.exit(0);
}

const SERVER_URL = process.env.KALAMDB_URL || 'http://localhost:8080';
const USERNAME = process.env.KALAMDB_USER || 'admin';
const PASSWORD = process.env.KALAMDB_PASSWORD || 'kalamdb123';

test('KalamDB Integration Tests', async (t) => {
  let client;

  t.beforeEach(async () => {
    client = new KalamDBClient(SERVER_URL, USERNAME, PASSWORD);
  });

  t.afterEach(async () => {
    if (client && client.isConnected()) {
      await client.disconnect();
    }
  });

  await t.test('should connect to server', async () => {
    await client.connect();
    assert.strictEqual(client.isConnected(), true);
  });

  await t.test('should disconnect from server', async () => {
    await client.connect();
    await client.disconnect();
    assert.strictEqual(client.isConnected(), false);
  });

  await t.test('should execute simple SELECT query', async () => {
    await client.connect();
    const result = await client.query('SELECT 1 as number, \'hello\' as text');
    
    assert.strictEqual(result.status, 'success');
    assert.ok(Array.isArray(result.results));
    assert.ok(result.results.length > 0);
    
    const firstResult = result.results[0];
    assert.ok(firstResult.rows);
    assert.strictEqual(firstResult.rows[0].number, 1);
    assert.strictEqual(firstResult.rows[0].text, 'hello');
  });

  await t.test('should create namespace', async () => {
    await client.connect();
    const result = await client.query('CREATE NAMESPACE IF NOT EXISTS test_sdk');
    assert.strictEqual(result.status, 'success');
  });

  await t.test('should create table', async () => {
    await client.connect();
    await client.query('CREATE NAMESPACE IF NOT EXISTS test_sdk');
    
    const result = await client.query(`
      CREATE TABLE IF NOT EXISTS test_sdk.users (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        name TEXT NOT NULL,
        email TEXT
      ) WITH (TYPE='SHARED', FLUSH_POLICY='rows:1000')
    `);
    
    assert.strictEqual(result.status, 'success');
  });

  await t.test('should insert data using query method', async () => {
    await client.connect();
    await client.query('CREATE NAMESPACE IF NOT EXISTS test_sdk');
    await client.query(`
      CREATE TABLE IF NOT EXISTS test_sdk.test_insert (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        message TEXT
      ) WITH (TYPE='SHARED')
    `);
    
    const result = await client.query(
      "INSERT INTO test_sdk.test_insert (message) VALUES ('Hello from SDK')"
    );
    
    assert.strictEqual(result.status, 'success');
  });

  await t.test('should query inserted data', async () => {
    await client.connect();
    await client.query('CREATE NAMESPACE IF NOT EXISTS test_sdk');
    await client.query(`
      CREATE TABLE IF NOT EXISTS test_sdk.test_query (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        value INT
      ) WITH (TYPE='SHARED')
    `);
    
    await client.query("INSERT INTO test_sdk.test_query (value) VALUES (42)");
    
    const result = await client.query('SELECT * FROM test_sdk.test_query');
    
    assert.strictEqual(result.status, 'success');
    assert.ok(result.results[0].rows.length > 0);
    assert.strictEqual(result.results[0].rows[0].value, 42);
  });

  await t.test('should handle query errors gracefully', async () => {
    await client.connect();
    
    await assert.rejects(
      async () => await client.query('SELECT * FROM nonexistent.table'),
      /error/i
    );
  });

  await t.test('should subscribe to table changes', { timeout: 10000 }, async () => {
    await client.connect();
    await client.query('CREATE NAMESPACE IF NOT EXISTS test_sdk');
    await client.query(`
      CREATE TABLE IF NOT EXISTS test_sdk.test_subscribe (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        message TEXT
      ) WITH (TYPE='SHARED')
    `);
    
    let receivedEvents = [];
    
    const subId = await client.subscribe('test_sdk.test_subscribe', (event) => {
      receivedEvents.push(event);
    });
    
    assert.ok(subId, 'Should receive subscription ID');
    
    // Give subscription time to establish
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    // Insert data to trigger change event
    await client.query(
      "INSERT INTO test_sdk.test_subscribe (message) VALUES ('Test subscription')"
    );
    
    // Wait for event
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    // Should have received at least subscription_ack
    assert.ok(receivedEvents.length > 0, 'Should receive subscription events');
    
    await client.unsubscribe(subId);
  });
});
