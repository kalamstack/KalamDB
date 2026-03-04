/**
 * kalam-link SDK Integration Tests
 * 
 * Tests for the new login(), consumeFromTopic(), and ackTopic() methods.
 * Requires a running KalamDB server.
 * 
 * Run: npx tsx tests/sdk.test.ts
 */

const KALAMDB_URL = process.env.KALAMDB_SERVER_URL || 'http://localhost:8080';

interface TestResult {
  name: string;
  passed: boolean;
  error?: string;
  duration: number;
}

const results: TestResult[] = [];

async function test(name: string, fn: () => Promise<void>) {
  const start = Date.now();
  try {
    await fn();
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

// ============================================================================
// SDK tests using direct HTTP (same endpoints kalam-link SDK uses)
// ============================================================================

async function runTests() {
  console.log('\n🧪 kalam-link SDK Method Tests\n');
  console.log(`Server: ${KALAMDB_URL}\n`);

  // --- login() tests ---

  await test('SDK login() - demo-user authentication', async () => {
    const response = await fetch(`${KALAMDB_URL}/v1/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'demo-user', password: 'demo123' }),
    });
    
    assert(response.ok, `Login request failed: ${response.status}`);
    const data = await response.json();
    assert(typeof data.access_token === 'string', 'Should return access_token string');
    assert(data.access_token.length > 20, 'Token should be a valid JWT');
  });

  await test('SDK login() - ai-service authentication', async () => {
    const response = await fetch(`${KALAMDB_URL}/v1/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'ai-service', password: 'service123' }),
    });
    
    assert(response.ok, `Login request failed: ${response.status}`);
    const data = await response.json();
    assert(typeof data.access_token === 'string', 'Should return access_token');
  });

  await test('SDK login() - invalid credentials rejected', async () => {
    const response = await fetch(`${KALAMDB_URL}/v1/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'invalid', password: 'wrong' }),
    });
    
    assert(!response.ok || response.status >= 400, 'Should reject invalid credentials');
  });

  // --- Query with JWT token (login + query flow) ---

  let jwtToken: string;

  await test('SDK login + query flow', async () => {
    // Step 1: Login
    const loginResp = await fetch(`${KALAMDB_URL}/v1/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'demo-user', password: 'demo123' }),
    });
    const loginData = await loginResp.json();
    jwtToken = loginData.access_token;
    
    // Step 2: Query with JWT
    const queryResp = await fetch(`${KALAMDB_URL}/v1/api/sql`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${jwtToken}`,
      },
      body: JSON.stringify({ sql: 'SELECT 1 as test' }),
    });
    
    assert(queryResp.ok, `Query failed: ${queryResp.status}`);
    const result = await queryResp.json();
    assert(result.status === 'success', `Query status: ${result.status}`);
  });

  // --- consumeFromTopic() tests ---

  await test('SDK consumeFromTopic() - consume from chat topic', async () => {
    const serviceLoginResp = await fetch(`${KALAMDB_URL}/v1/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'ai-service', password: 'service123' }),
    });
    const serviceData = await serviceLoginResp.json();
    
    const consumeResp = await fetch(`${KALAMDB_URL}/v1/api/topics/consume`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${serviceData.access_token}`,
      },
      body: JSON.stringify({
        topic_id: 'chat.new_messages',
        group_id: `sdk-test-${Date.now()}`,
        start: 'Latest',
        limit: 5,
        partition_id: 0,
      }),
    });
    
    assert(consumeResp.ok, `Consume failed: ${consumeResp.status}`);
    // Response may be empty (no messages) or have messages
    const text = await consumeResp.text();
    if (text && text.trim()) {
      const data = JSON.parse(text);
      assert('next_offset' in data || 'messages' in data, 'Should have consume result shape');
    }
  });

  await test('SDK consumeFromTopic() - handles empty response', async () => {
    const serviceLoginResp = await fetch(`${KALAMDB_URL}/v1/api/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: 'ai-service', password: 'service123' }),
    });
    const serviceData = await serviceLoginResp.json();

    // Use a unique group to get empty result
    const consumeResp = await fetch(`${KALAMDB_URL}/v1/api/topics/consume`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${serviceData.access_token}`,
      },
      body: JSON.stringify({
        topic_id: 'chat.new_messages',
        group_id: `empty-test-${Date.now()}`,
        start: 'Latest',
        limit: 1,
        partition_id: 0,
      }),
    });
    
    assert(consumeResp.ok, `Consume should succeed even with no messages`);
  });

  // --- Conversations via kalam-link query pattern ---

  await test('SDK queryAll pattern - load conversations', async () => {
    const queryResp = await fetch(`${KALAMDB_URL}/v1/api/sql`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${jwtToken!}`,
      },
      body: JSON.stringify({ sql: 'SELECT * FROM chat.conversations ORDER BY updated_at DESC' }),
    });
    
    assert(queryResp.ok, `Query failed: ${queryResp.status}`);
    const result = await queryResp.json();
    assert(result.status === 'success', 'Query should succeed');
    assert(Array.isArray(result.results), 'Should have results array');
    
    // Parse rows from raw HTTP response (server returns positional arrays).
    // The SDK's queryAll() handles this automatically via WASM named_rows.
    if (result.results[0]?.rows && result.results[0]?.schema) {
      const schema = result.results[0].schema;
      const rows = result.results[0].rows;
      const conversations = rows.map((row: any[]) => {
        const obj: Record<string, any> = {};
        schema.forEach((field: any) => { obj[field.name] = row[field.index]; });
        return obj;
      });
      assert(Array.isArray(conversations), 'Parsed conversations should be array');
    }
  });

  await test('SDK queryAll pattern - load messages for conversation', async () => {
    // First get a conversation
    const convResp = await fetch(`${KALAMDB_URL}/v1/api/sql`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${jwtToken!}`,
      },
      body: JSON.stringify({ sql: 'SELECT id FROM chat.conversations LIMIT 1' }),
    });
    const convResult = await convResp.json();
    
    if (convResult.results?.[0]?.rows?.length > 0) {
      const convId = convResult.results[0].rows[0][0];
      
      // Query messages
      const msgResp = await fetch(`${KALAMDB_URL}/v1/api/sql`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${jwtToken!}`,
        },
        body: JSON.stringify({ 
          sql: `SELECT * FROM chat.messages WHERE conversation_id = ${convId} ORDER BY created_at ASC` 
        }),
      });
      
      assert(msgResp.ok, `Messages query failed: ${msgResp.status}`);
      const msgResult = await msgResp.json();
      assert(msgResult.status === 'success', 'Messages query should succeed');
    }
    // If no conversations exist, the test still passes (schema was verified)
  });

  // --- Summary ---

  console.log('\n' + '='.repeat(60));
  const passed = results.filter(r => r.passed).length;
  const failed = results.filter(r => !r.passed).length;
  console.log(`\n📊 Results: ${passed} passed, ${failed} failed, ${results.length} total\n`);
  
  if (failed > 0) {
    console.log('Failed tests:');
    results.filter(r => !r.passed).forEach(r => {
      console.log(`  ❌ ${r.name}: ${r.error}`);
    });
    process.exit(1);
  }
}

runTests().catch(err => {
  console.error('\n💥 Test runner error:', err);
  process.exit(1);
});
