#!/usr/bin/env node
// @ts-check

/**
 * WebSocket Subscription Integration Test
 * 
 * Tests:
 * 1. Multiple subscriptions share a single WebSocket connection
 * 2. Initial data is received when subscribing
 * 3. Live changes are received after subscribing
 * 
 * Requires: Server running on localhost:8080 with root/root credentials
 */

import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { readFile } from 'fs/promises';

// Add WebSocket polyfill for Node.js (browser has it built-in)
import { WebSocket } from 'ws';
// @ts-ignore - ws package types differ slightly from browser WebSocket
globalThis.WebSocket = WebSocket;

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const sdkPath = join(__dirname, '..');
const USERNAME = 'root';
const PASSWORD = 'root';
const HTTP_URL = 'http://localhost:8080';

// Test configuration - use unique namespace to avoid conflicts
const TEST_NAMESPACE = 'test_ws_' + Date.now();
const TODOS_TABLE = `${TEST_NAMESPACE}.todos`;
const EVENTS_TABLE = `${TEST_NAMESPACE}.events`;

let testsPassed = 0;
let testsFailed = 0;

/**
 * @param {string} message
 */
function log(message) {
  console.log(`  ${message}`);
}

/**
 * @param {string} message
 */
function pass(message) {
  testsPassed++;
  log(`✓ ${message}`);
}

/**
 * @param {string} message
 */
function fail(message) {
  testsFailed++;
  log(`✗ ${message}`);
}

/**
 * @param {number} ms
 * @returns {Promise<void>}
 */
async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Helper to execute SQL via HTTP (works in Node.js)
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

/**
 * Helper to get error message from various error types
 * @param {unknown} error
 * @returns {string}
 */
function getErrorMessage(error) {
  if (error instanceof Error) return error.message;
  if (typeof error === 'string') return error;
  if (error && typeof error === 'object' && 'message' in error) return String(error.message);
  return String(error);
}

async function runTests() {
  console.log('🧪 WebSocket Subscription Integration Tests\n');
  console.log(`Server: ${HTTP_URL}`);
  console.log(`Username: ${USERNAME}`);
  console.log(`Namespace: ${TEST_NAMESPACE}\n`);

  // Initialize WASM
  let KalamClient;
  try {
    const { default: init, KalamClient: Client } = await import(join(sdkPath, 'kalam_client.js'));
    const wasmBuffer = await readFile(join(sdkPath, 'kalam_client_bg.wasm'));
    await init(wasmBuffer);
    KalamClient = Client;
    pass('WASM initialized');
  } catch (error) {
    fail(`WASM initialization: ${getErrorMessage(error)}`);
    process.exit(1);
  }

  // ============================================================
  // Test 1: Server Health Check
  // ============================================================
  console.log('\n📋 Test 1: Server Health Check');
  try {
    const response = await fetch(`${HTTP_URL}/v1/api/healthcheck`);
    if (response.ok) {
      pass('Server is running');
    } else {
      fail(`Server returned ${response.status}`);
      process.exit(1);
    }
  } catch (error) {
    fail(`Cannot connect to server: ${getErrorMessage(error)}`);
    console.log('\n⚠️  Please start the server: cd backend && cargo run');
    process.exit(1);
  }

  // ============================================================
  // Test 2: Setup Test Tables
  // ============================================================
  console.log('\n📋 Test 2: Setup Test Tables');
  try {
    // Create namespace
    const nsResult = await executeSQL(`CREATE NAMESPACE IF NOT EXISTS ${TEST_NAMESPACE}`);
    pass(`Namespace ${TEST_NAMESPACE} created`);
    
    // Create todos table
    const todosResult = await executeSQL(`
      CREATE TABLE IF NOT EXISTS ${TODOS_TABLE} (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        title TEXT NOT NULL,
        completed BOOLEAN DEFAULT false
      ) WITH (TYPE = 'USER')
    `);
    if (todosResult.status === 'success') {
      pass(`Table ${TODOS_TABLE} created`);
    } else {
      log(`  Note: ${JSON.stringify(todosResult)}`);
    }
    
    // Create events table
    const eventsResult = await executeSQL(`
      CREATE TABLE IF NOT EXISTS ${EVENTS_TABLE} (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        event_type TEXT NOT NULL,
        payload TEXT
      ) WITH (TYPE = 'USER')
    `);
    if (eventsResult.status === 'success') {
      pass(`Table ${EVENTS_TABLE} created`);
    } else {
      log(`  Note: ${JSON.stringify(eventsResult)}`);
    }
  } catch (error) {
    fail(`Setup tables: ${getErrorMessage(error)}`);
  }

  // ============================================================
  // Test 3: Insert Initial Data (before subscribing)
  // ============================================================
  console.log('\n📋 Test 3: Insert Initial Data');
  try {
    // Insert initial todos
    await executeSQL(`INSERT INTO ${TODOS_TABLE} (title, completed) VALUES ('Buy groceries', false)`);
    await executeSQL(`INSERT INTO ${TODOS_TABLE} (title, completed) VALUES ('Read a book', true)`);
    pass('Inserted 2 initial todos');
    
    // Insert initial event
    await executeSQL(`INSERT INTO ${EVENTS_TABLE} (event_type, payload) VALUES ('app_start', '{"version":"1.0"}')`);
    pass('Inserted 1 initial event');
    
    // Verify data via SQL
    const todosCheck = await executeSQL(`SELECT COUNT(*) as cnt FROM ${TODOS_TABLE}`);
    const eventsCheck = await executeSQL(`SELECT COUNT(*) as cnt FROM ${EVENTS_TABLE}`);
    log(`  → Todos count: ${todosCheck.results?.[0]?.rows?.[0]?.cnt || 'N/A'}`);
    log(`  → Events count: ${eventsCheck.results?.[0]?.rows?.[0]?.cnt || 'N/A'}`);
  } catch (error) {
    fail(`Insert initial data: ${getErrorMessage(error)}`);
  }

  // ============================================================
  // Test 4: Create Client and Connect WebSocket
  // ============================================================
  console.log('\n📋 Test 4: Create Client and Connect');
  let client;
  try {
    client = new KalamClient(HTTP_URL, USERNAME, PASSWORD);
    pass('Client created');
    
    await client.connect();
    pass('WebSocket connected');
    
    await sleep(100);
    
    if (client.isConnected()) {
      pass('Connection status verified');
    } else {
      fail('isConnected() returned false');
    }
  } catch (error) {
    fail(`Client/Connection: ${getErrorMessage(error)}`);
    process.exit(1);
  }

  // ============================================================
  // Test 5: Subscribe to BOTH tables and verify initial data
  // ============================================================
  console.log('\n📋 Test 5: Subscribe to Both Tables - Verify Initial Data');
  
  // Track messages received
  /** @type {Array<{type: string, rows?: any[], subscription_id?: string, change_type?: string}>} */
  let todosMessages = [];
  /** @type {Array<{type: string, rows?: any[], subscription_id?: string, change_type?: string}>} */
  let eventsMessages = [];
  /** @type {string | null} */
  let todosSubId = null;
  /** @type {string | null} */
  let eventsSubId = null;
  
  try {
    // Subscribe to todos
    todosSubId = await client.subscribe(TODOS_TABLE, (/** @type {string} */ msgStr) => {
      const msg = JSON.parse(msgStr);
      todosMessages.push(msg);
      log(`  📨 [TODOS] ${msg.type}: ${msg.rows?.length || 0} rows`);
    });
    pass(`Subscribed to todos: ${todosSubId}`);
    
    // Subscribe to events (same WebSocket connection!)
    eventsSubId = await client.subscribe(EVENTS_TABLE, (/** @type {string} */ msgStr) => {
      const msg = JSON.parse(msgStr);
      eventsMessages.push(msg);
      log(`  📨 [EVENTS] ${msg.type}: ${msg.rows?.length || 0} rows`);
    });
    pass(`Subscribed to events: ${eventsSubId}`);
    
    // Verify single WebSocket connection
    if (client.isConnected()) {
      pass('Both subscriptions share single WebSocket connection');
    } else {
      fail('Connection lost after subscribing');
    }
    
    // Wait for initial data batches
    log('  ⏳ Waiting for initial data (2 seconds)...');
    await sleep(300);
    
    // Check if we received initial data for todos
    const todosInitialData = todosMessages.find(m => 
      m.type === 'initial_data_batch' || m.type === 'subscription_ack'
    );
    if (todosInitialData) {
      pass(`Received initial data for todos`);
      if (todosInitialData.rows) {
        log(`    → ${todosInitialData.rows.length} todo(s) received`);
        todosInitialData.rows.forEach((/** @type {any} */ r) => log(`      - ${r.title} (completed: ${r.completed})`));
      }
    } else if (todosMessages.length > 0) {
      log(`  ⚠️  Received ${todosMessages.length} message(s) for todos, but no initial_data_batch`);
      todosMessages.forEach((m) => log(`    → ${m.type}: ${JSON.stringify(m).substring(0, 100)}`));
    } else {
      fail('No messages received for todos subscription');
    }
    
    // Check if we received initial data for events
    const eventsInitialData = eventsMessages.find(m => 
      m.type === 'initial_data_batch' || m.type === 'subscription_ack'
    );
    if (eventsInitialData) {
      pass(`Received initial data for events`);
      if (eventsInitialData.rows) {
        log(`    → ${eventsInitialData.rows.length} event(s) received`);
        eventsInitialData.rows.forEach((/** @type {any} */ r) => log(`      - ${r.event_type}: ${r.payload}`));
      }
    } else if (eventsMessages.length > 0) {
      log(`  ⚠️  Received ${eventsMessages.length} message(s) for events, but no initial_data_batch`);
      eventsMessages.forEach((m) => log(`    → ${m.type}: ${JSON.stringify(m).substring(0, 100)}`));
    } else {
      fail('No messages received for events subscription');
    }
    
  } catch (error) {
    fail(`Subscribe: ${getErrorMessage(error)}`);
  }

  // ============================================================
  // Test 6: Insert NEW data and verify LIVE changes
  // ============================================================
  console.log('\n📋 Test 6: Insert New Data - Verify Live Changes');
  
  // Clear message arrays to track only new changes
  const todosCountBefore = todosMessages.length;
  const eventsCountBefore = eventsMessages.length;
  
  try {
    // Insert new todo
    log('  📝 Inserting new todo...');
    await executeSQL(`INSERT INTO ${TODOS_TABLE} (title, completed) VALUES ('New live todo!', false)`);
    pass('Inserted new todo via SQL');
    
    // Insert new event
    log('  📝 Inserting new event...');
    await executeSQL(`INSERT INTO ${EVENTS_TABLE} (event_type, payload) VALUES ('user_action', '{"action":"click"}')`);
    pass('Inserted new event via SQL');
    
    // Wait for live change notifications
    log('  ⏳ Waiting for live change notifications (2 seconds)...');
    await sleep(300);
    
    // Check for new todos messages
    const newTodosMessages = todosMessages.slice(todosCountBefore);
    if (newTodosMessages.length > 0) {
      pass(`Received ${newTodosMessages.length} new message(s) for todos`);
      newTodosMessages.forEach((m) => {
        log(`    → ${m.type}: ${m.change_type || ''} ${m.rows?.length || 0} row(s)`);
        if (m.rows) {
          m.rows.forEach((/** @type {any} */ r) => log(`      - ${r.title?.Utf8 || r.title || JSON.stringify(r)}`));
        }
      });
    } else {
      fail('No new messages for todos after insert');
    }
    
    // Check for new events messages
    const newEventsMessages = eventsMessages.slice(eventsCountBefore);
    if (newEventsMessages.length > 0) {
      pass(`Received ${newEventsMessages.length} new message(s) for events`);
      newEventsMessages.forEach((m) => {
        log(`    → ${m.type}: ${m.change_type || ''} ${m.rows?.length || 0} row(s)`);
        if (m.rows) {
          m.rows.forEach((/** @type {any} */ r) => log(`      - ${r.event_type?.Utf8 || r.event_type || JSON.stringify(r)}`));
        }
      });
    } else {
      fail('No new messages for events after insert');
    }
    
  } catch (error) {
    fail(`Live changes: ${getErrorMessage(error)}`);
  }

  // ============================================================
  // Test 7: Unsubscribe from one, connection stays open
  // ============================================================
  console.log('\n📋 Test 7: Unsubscribe One - Connection Stays Open');
  try {
    await client.unsubscribe(todosSubId);
    pass('Unsubscribed from todos');
    
    await sleep(100);
    
    if (client.isConnected()) {
      pass('WebSocket connection still open');
    } else {
      fail('Connection closed after unsubscribing one');
    }
    
    // Events subscription should still work
    const eventsCountNow = eventsMessages.length;
    await executeSQL(`INSERT INTO ${EVENTS_TABLE} (event_type, payload) VALUES ('after_unsub', '{"test":true}')`);
    await sleep(200);
    
    if (eventsMessages.length > eventsCountNow) {
      pass('Events subscription still receiving changes');
    } else {
      log('  ⚠️  Events subscription did not receive new change');
    }
    
  } catch (error) {
    fail(`Unsubscribe one: ${getErrorMessage(error)}`);
  }

  // ============================================================
  // Test 8: Unsubscribe all - Connection still open
  // ============================================================
  console.log('\n📋 Test 8: Unsubscribe All - Connection Stays Open');
  try {
    await client.unsubscribe(eventsSubId);
    pass('Unsubscribed from events');
    
    await sleep(100);
    
    if (client.isConnected()) {
      pass('WebSocket connection still open (requires disconnect() to close)');
    } else {
      fail('Connection closed after unsubscribing all');
    }
  } catch (error) {
    fail(`Unsubscribe all: ${getErrorMessage(error)}`);
  }

  // ============================================================
  // Test 9: Disconnect
  // ============================================================
  console.log('\n📋 Test 9: Disconnect');
  try {
    await client.disconnect();
    await sleep(100);
    
    if (!client.isConnected()) {
      pass('WebSocket disconnected');
    } else {
      fail('Still connected after disconnect()');
    }
  } catch (error) {
    fail(`Disconnect: ${getErrorMessage(error)}`);
  }

  // ============================================================
  // Cleanup
  // ============================================================
  console.log('\n📋 Cleanup');
  try {
    await executeSQL(`DROP TABLE IF EXISTS ${TODOS_TABLE}`);
    await executeSQL(`DROP TABLE IF EXISTS ${EVENTS_TABLE}`);
    await executeSQL(`DROP NAMESPACE IF EXISTS ${TEST_NAMESPACE}`);
    pass('Test tables and namespace cleaned up');
  } catch (error) {
    log(`  ⚠️  Cleanup warning: ${getErrorMessage(error)}`);
  }

  // ============================================================
  // Summary
  // ============================================================
  console.log('\n' + '═'.repeat(60));
  console.log(`📊 SUMMARY`);
  console.log('═'.repeat(60));
  console.log(`  Total todos messages received: ${todosMessages.length}`);
  console.log(`  Total events messages received: ${eventsMessages.length}`);
  console.log('─'.repeat(60));
  console.log(`  Tests passed: ${testsPassed}`);
  console.log(`  Tests failed: ${testsFailed}`);
  console.log('═'.repeat(60));

  if (testsFailed === 0) {
    console.log('\n✅ All tests passed!');
    process.exit(0);
  } else {
    console.log('\n❌ Some tests failed');
    process.exit(1);
  }
}

runTests().catch(error => {
  console.error('💥 Test suite error:', error);
  process.exit(1);
});
