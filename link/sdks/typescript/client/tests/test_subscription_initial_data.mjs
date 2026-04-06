#!/usr/bin/env node
// @ts-check

/**
 * Simple test to verify @kalamdb/client WASM subscriptions with subscribeWithSql
 * Tests initial_data_batch reception like the UI would use
 */

import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { readFile } from 'fs/promises';

// Add WebSocket polyfill for Node.js
import { WebSocket } from 'ws';
// @ts-ignore
globalThis.WebSocket = WebSocket;

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const sdkPath = join(__dirname, '..');

const HTTP_URL = 'http://localhost:8080';
const USERNAME = 'root';
const PASSWORD = 'admin123';

/**
 * @param {number} ms
 * @returns {Promise<void>}
 */
async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Helper to execute SQL via HTTP
 * @param {string} sql
 * @returns {Promise<any>}
 */
async function executeSQL(sql) {
  const credentials = Buffer.from(`${USERNAME}:${PASSWORD}`).toString('base64');
  const response = await fetch(`${HTTP_URL}/v1/api/sql`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Basic ${credentials}`
    },
    body: JSON.stringify({ sql })
  });
  
  const text = await response.text();
  try {
    return JSON.parse(text);
  } catch {
    return { status: response.ok ? 'success' : 'error', raw: text };
  }
}

async function runTest() {
  console.log('🧪 WASM subscribeWithSql Subscription Test\n');

  // Initialize WASM (like websocket.test.mjs does)
  let KalamClient;
  try {
    const { default: init, KalamClient: Client } = await import(join(sdkPath, 'kalam_client.js'));
    const wasmBuffer = await readFile(join(sdkPath, 'kalam_client_bg.wasm'));
    await init(wasmBuffer);
    KalamClient = Client;
    console.log('✓ WASM initialized');
  } catch (error) {
    console.error('✗ WASM initialization failed:', error);
    process.exit(1);
  }

  // Check server
  try {
    const response = await fetch(`${HTTP_URL}/v1/api/healthcheck`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    console.log('✓ Server is running');
  } catch {
    console.error('✗ Server not running. Start with: cd backend && cargo run');
    process.exit(1);
  }

  // Create test namespace and table (using plain names, no quotes)
  const namespace = `test_wasm_${Date.now()}`;
  const table = `${namespace}.messages`;
  
  console.log(`\n📋 Setup: Creating ${table}`);
  await executeSQL(`CREATE NAMESPACE ${namespace}`);
  await executeSQL(`CREATE TABLE ${table} (id INT PRIMARY KEY, msg TEXT) WITH (TYPE='USER')`);
  
  // Insert initial data BEFORE subscribing
  await executeSQL(`INSERT INTO ${table} (id, msg) VALUES (1, 'First message')`);
  await executeSQL(`INSERT INTO ${table} (id, msg) VALUES (2, 'Second message')`);
  console.log('✓ Inserted 2 initial messages');

  // Create WASM client directly (with Basic Auth)
  console.log('\n📋 Test: Create WASM client');
  const client = new KalamClient(HTTP_URL, USERNAME, PASSWORD);
  console.log('✓ Client created');

  // Connect WebSocket
  console.log('\n📋 Test: Connect WebSocket');
  await client.connect();
  console.log('✓ WebSocket connected');
  
  // Track received messages
  /** @type {any[]} */
  const receivedEvents = [];

  // Subscribe using subscribeWithSql (like the UI does, but WASM receives JSON string)
  console.log('\n📋 Test: Subscribe with SQL (using subscribeWithSql)');
  const sql = `SELECT * FROM ${table}`;
  console.log(`  SQL: ${sql}`);
  
  // Note: WASM subscribeWithSql callback receives JSON STRING, not parsed object
  // The TypeScript KalamDBClient wrapper parses it, but we're using raw WASM
  const subscriptionId = await client.subscribeWithSql(sql, null, (/** @type {string} */ eventJson) => {
    // Parse JSON string (like TypeScript wrapper does)
    let event;
    try {
      event = JSON.parse(eventJson);
    } catch (e) {
      console.error('Failed to parse event JSON:', e);
      return;
    }
    
    console.log(`\n📨 Received event:`);
    console.log(`  Type: ${typeof event}`);
    console.log(`  event.type: ${event?.type}`);
    
    if (event?.type === 'subscription_ack') {
      console.log(`  ✓ subscription_ack - total_rows: ${event.total_rows}`);
    } else if (event?.type === 'initial_data_batch') {
      console.log(`  ✓ initial_data_batch - rows: ${event.rows?.length}`);
      if (event.rows) {
        event.rows.forEach((/** @type {any} */ row, /** @type {number} */ i) => {
          console.log(`    Row ${i + 1}:`, JSON.stringify(row).substring(0, 100));
        });
      }
    } else if (event?.type === 'change') {
      console.log(`  ✓ change - type: ${event.change_type}, rows: ${event.rows?.length}`);
    } else if (event?.type === 'error') {
      console.log(`  ✗ error: ${event.message}`);
    }
    
    receivedEvents.push(event);
  });
  console.log(`✓ Subscribed with ID: ${subscriptionId}`);

  // Wait for initial data
  console.log('\n⏳ Waiting for initial data (3 seconds)...');
  await sleep(500);

  // Insert new data (should trigger live change)
  console.log('\n📋 Test: Insert new row (live change)');
  await executeSQL(`INSERT INTO ${table} (id, msg) VALUES (3, 'Live message!')`);
  console.log('✓ Inserted new message');

  // Wait for live change
  console.log('\n⏳ Waiting for live change (2 seconds)...');
  await sleep(300);

  // Summary
  console.log('\n════════════════════════════════════════════════════════════');
  console.log('📊 SUMMARY');
  console.log('════════════════════════════════════════════════════════════');
  console.log(`Total events received: ${receivedEvents.length}`);
  
  const ackCount = receivedEvents.filter(e => e?.type === 'subscription_ack').length;
  const batchCount = receivedEvents.filter(e => e?.type === 'initial_data_batch').length;
  const changeCount = receivedEvents.filter(e => e?.type === 'change').length;
  const errorCount = receivedEvents.filter(e => e?.type === 'error').length;
  
  console.log(`  - subscription_ack: ${ackCount}`);
  console.log(`  - initial_data_batch: ${batchCount}`);
  console.log(`  - change: ${changeCount}`);
  console.log(`  - error: ${errorCount}`);
  console.log('════════════════════════════════════════════════════════════');

  // Cleanup
  await client.unsubscribe(subscriptionId);
  await client.disconnect();
  await executeSQL(`DROP NAMESPACE ${namespace} CASCADE`);
  console.log('\n✓ Cleanup completed');

  // Verify results
  if (batchCount > 0 && ackCount > 0) {
    console.log('\n✅ TEST PASSED: Initial data received correctly');
  } else {
    console.log('\n❌ TEST FAILED: Missing expected events');
    process.exit(1);
  }
}

runTest().catch(console.error);
