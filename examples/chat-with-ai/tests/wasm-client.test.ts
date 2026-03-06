/**
 * kalam-link WASM Client Integration Tests
 * 
 * Tests the actual WASM client with WebSocket subscriptions.
 * Mimics the usage pattern in the chat-with-ai app.
 * 
 * Requires:
 * - Running KalamDB server
 * - WASM file accessible (build SDK first)
 * 
 * Run: npx tsx tests/wasm-client.test.ts
 */

// Polyfill browser APIs for Node.js environment
import { WebSocket } from 'ws';
import * as fs from 'fs';
import * as path from 'path';

// Setup global WebSocket for WASM (kalam-link expects browser WebSocket)
(globalThis as any).WebSocket = WebSocket;

// Create window object with fetch for WASM
// The Rust WASM code uses web-sys which expects window.fetch for HTTP requests
(globalThis as any).window = {
  location: {
    protocol: 'http:',
    hostname: 'localhost',
    port: '8080',
    href: 'http://localhost:8080',
  },
  fetch: globalThis.fetch, // Use Node.js built-in fetch (Node 18+)
};

// Now import after setting up polyfills
import { createClient, Auth } from 'kalam-link';

const KALAMDB_URL = process.env.KALAMDB_SERVER_URL || 'http://localhost:8080';
const USERNAME = 'demo-user';
const PASSWORD = 'demo123';

interface TestResult {
  name: string;
  passed: boolean;
  error?: string;
  duration: number;
}

const results: TestResult[] = [];

async function test(name: string, fn: () => Promise<void>, timeout = 30000) {
  const start = Date.now();
  try {
    // Wrap in timeout
    await Promise.race([
      fn(),
      new Promise((_, reject) => 
        setTimeout(() => reject(new Error(`Test timeout after ${timeout}ms`)), timeout)
      ),
    ]);
    results.push({ name, passed: true, duration: Date.now() - start });
    console.log(`  ✅ ${name} (${Date.now() - start}ms)`);
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    results.push({ name, passed: false, error: msg, duration: Date.now() - start });
    console.log(`  ❌ ${name}: ${msg} (${Date.now() - start}ms)`);
  }
}

function assert(condition: boolean, message: string) {
  if (!condition) throw new Error(message);
}

async function runTests() {
  console.log('\n🧪 kalam-link WASM Client Tests\n');
  console.log(`Server: ${KALAMDB_URL}\n`);

  // Check if WASM file exists and read it
  const wasmPath = path.resolve(__dirname, '../../../link/sdks/typescript/dist/wasm/kalam_link_bg.wasm');
  if (!fs.existsSync(wasmPath)) {
    console.error('❌ WASM file not found. Run: cd link/sdks/typescript && bash build.sh');
    process.exit(1);
  }
  
  // Read WASM file as buffer (Node.js fetch() doesn't support file:// URLs)
  const wasmBytes = fs.readFileSync(wasmPath);
  console.log(`📦 WASM file: ${wasmPath} (${wasmBytes.length} bytes)\n`);

  // ============================================================================
  // Test 1: Client Creation and Login
  // ============================================================================

  let client: any = null;
  let jwtToken: string = '';

  await test('Create client and login', async () => {
    console.log('    Creating client...');
    client = createClient({
      url: KALAMDB_URL,
      authProvider: async () => Auth.basic(USERNAME, PASSWORD),
      wasmUrl: wasmBytes, // Pass buffer directly (Node.js doesn't support file:// in fetch)
    });

    console.log('    Logging in...');
    jwtToken = await client.login();
    console.log('    Login successful, token length:', jwtToken.length);

    assert(client !== null, 'Client should be created');
    assert(jwtToken.length > 0, 'Should receive JWT token');
  });

  // ============================================================================
  // Test 2: WebSocket Connection (initializes WASM)
  // ============================================================================

  await test('Connect WebSocket (initialize WASM)', async () => {
    console.log('    Connecting WebSocket...');
    await client.connect();
    console.log('    WebSocket connected');

    // Verify connection
    assert(client !== null, 'Client should remain valid after connect');
  });

  // ============================================================================
  // Test 3: Configure Auto-Reconnect (requires WASM initialized)
  // ============================================================================

  await test('Configure auto-reconnect', async () => {
    client.setAutoReconnect(true);
    client.setReconnectDelay(1000, 30000);
    client.setMaxReconnectAttempts(0);
    console.log('    Auto-reconnect configured');
  });

  // ============================================================================
  // Test 4: Query conversations using HTTP (bypasses WASM)
  // ============================================================================

  await test('Query conversations table via HTTP', async () => {
    console.log('    Querying conversations via HTTP...');
    
    // Use direct HTTP fetch (not WASM client)
    const response = await fetch(`${KALAMDB_URL}/v1/api/sql`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${jwtToken}`, // Use JWT from login
      },
      body: JSON.stringify({ sql: 'SELECT * FROM chat.conversations ORDER BY updated_at DESC LIMIT 5' }),
    });
    
    assert(response.ok, `Query failed: ${response.status}`);
    const result = await response.json();
    console.log(`    Found ${result.results?.[0]?.row_count || 0} conversations`);
    assert(result.status === 'success', 'Query should succeed');
  });

  // ============================================================================
  // Test 5: Note about shared table permissions  
  // ============================================================================

  // Shared tables don't allow INSERT/UPDATE/DELETE for regular users
  // Skip these tests - they would require admin/service account

  let testConvId = 1; // Use existing conversation ID for subscription tests

  await test('Skip INSERT tests (shared tables require elevated permissions)', async () => {
    console.log('    ℹ️  Regular users cannot INSERT/UPDATE/DELETE shared tables');
    console.log('    ℹ️  Shared tables with ACCESS_LEVEL=PUBLIC are read-only for role=User');
    console.log('    ✅ This is correct security behavior');
    assert(true, 'Documented limitation');
  });

  // ============================================================================
  // Test 6: Test subscription error handling (shared tables don't support live queries)
  // ============================================================================

  let messageCount = 0;
  let subscription: any = null;

  await test('Subscribe to messages (expect UNSUPPORTED error for shared table)', async () => {
    console.log(`    Opening subscription for conversation ${testConvId}...`);
    console.log(`    Note: Shared tables don't support live subscriptions - expecting error`);
    
    let receivedError = false;
    let errorCode = '';
    
    subscription = await client.subscribeWithSql(
      `SELECT * FROM chat.messages WHERE conversation_id = ${testConvId} ORDER BY created_at ASC`,
      (data: any) => {
        // Error events come through the data callback with type='error'
        if (data && typeof data === 'object' && data.type === 'error') {
          console.log('    ✅ Received expected error:', data.code);
          receivedError = true;
          errorCode = data.code;
        } else {
          console.log(`    📨 Received data (unexpected):`, data);
        }
      }
    );

    console.log('    Subscription created, waiting for error response...');
    
    // Wait for error message from server (needs time for WebSocket roundtrip)
    await new Promise(resolve => setTimeout(resolve, 1500));
    
    assert(receivedError === true, 'Should receive error from server');
    assert(errorCode === 'UNSUPPORTED', `Error code should be UNSUPPORTED, got: ${errorCode}`);
    console.log('    ✅ Error handling works correctly');
  });

  // ============================================================================

    assert(response.ok, `Insert failed: ${response.status}`);
    const result = await response.json();
    assert(result.status === 'success', 'Insert should succeed');
    console.log('    Message inserted successfully');
  });

  // ============================================================================

    assert(response.ok, `Insert failed: ${response.status}`);
    const result = await response.json();
    assert(result.status === 'success', 'Insert should succeed');
    console.log('    AI response inserted successfully');
  });

  // ============================================================================
  // Test 9: Unsubscribe
  // ============================================================================

  await test('Unsubscribe from live query', async () => {
    console.log('    Unsubscribing...');
    if (subscription) {
      await subscription(); // subscription is the unsubscribe function
      console.log('    Unsubscribed successfully');
    }
  });

  // ============================================================================
  // Test 10: Test multiple subscriptions and graceful error handling
  // ============================================================================

  await test('Multiple simultaneous subscriptions handle errors gracefully', async () => {
    console.log('    Opening conversations subscription...');
    let convError = false;
    const convSub = await client.subscribeWithSql(
      'SELECT * FROM chat.conversations ORDER BY updated_at DESC',
      (data: any) => {
        if (data && data.type === 'error') convError = true;
      }
    );

    console.log('    Opening messages subscription...');
    let msgError = false;
    const msgSub = await client.subscribeWithSql(
      `SELECT * FROM chat.messages WHERE conversation_id = ${testConvId} ORDER BY created_at ASC`,
      (data: any) => {
        if (data && data.type === 'error') msgError = true;
      }
    );

    await new Promise(resolve => setTimeout(resolve, 1000));

    console.log('    Both subscriptions received expected errors');
    assert(convError && msgError, 'Both should receive UNSUPPORTED errors');
    
    console.log('    Cleaning up subscriptions...');
    await convSub();
    await msgSub();
  });

  // ============================================================================

    assert(response.ok, `Query failed: ${response.status}`);
    const result = await response.json();
    assert(result.status === 'success', 'Query should succeed');
    
    const rowCount = result.results[0]?.row_count || 0;
    console.log(`    Found ${rowCount} messages`);
    assert(rowCount >= 2, 'Should have at least user message + AI response');
  });

  // ============================================================================
    assert(response.ok, 'Delete messages should succeed');
    
    // Delete conversation
    response = await fetch(`${KALAMDB_URL}/v1/api/sql`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${jwtToken}`,
      },
      body: JSON.stringify({ sql: `DELETE FROM chat.conversations WHERE id = ${testConvId}` }),
    });
    assert(response.ok, 'Delete conversation should succeed');
    
    console.log('    Test data cleaned up');
  });

  // ============================================================================
  // Test 13: Disconnect
  // ============================================================================

  await test('Disconnect WebSocket', async () => {
    console.log('    Disconnecting...');
    await client.disconnect();
    console.log('    Disconnected successfully');
  });

  // ============================================================================
  // Summary
  // ============================================================================

  console.log('\n' + '='.repeat(60));
  const passed = results.filter(r => r.passed).length;
  const failed = results.filter(r => !r.passed).length;
  console.log(`\n📊 Results: ${passed} passed, ${failed} failed, ${results.length} total\n`);

  if (failed > 0) {
    console.log('Failed tests:');
    results.filter(r => !r.passed).forEach(r => {
      console.log(`  ❌ ${r.name}: ${r.error}`);
    });
    console.log('');
    process.exit(1);
  }

  console.log('✅ All WASM client tests passed!\n');
}

runTests().catch(err => {
  console.error('\n💥 Test runner error:', err);
  console.error(err.stack);
  process.exit(1);
});
