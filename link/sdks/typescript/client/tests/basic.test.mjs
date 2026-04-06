#!/usr/bin/env node

/**
 * Basic WASM Module Test
 * 
 * Tests that the WASM module loads and initializes correctly
 */

import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { readFile } from 'fs/promises';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Import from parent directory (the SDK root)
const sdkPath = join(__dirname, '..');

// WASM output directory (wasm-pack places files here)
const wasmOutPath = join(sdkPath, 'wasm');

async function runTests() {
  console.log('🧪 Running Basic WASM Module Tests\n');
  
  let passed = 0;
  let failed = 0;

  // Test 1: Module loads
  console.log('Test 1: WASM module loads...');
  try {
    const { default: init, KalamClient } = await import(join(wasmOutPath, 'kalam_client.js'));
    
    if (typeof init === 'function') {
      console.log('  ✓ init function exists');
      passed++;
    } else {
      console.log('  ✗ init is not a function');
      failed++;
    }

    if (typeof KalamClient === 'function') {
      console.log('  ✓ KalamClient class exists');
      passed++;
    } else {
      console.log('  ✗ KalamClient is not a constructor');
      failed++;
    }
  } catch (error) {
    console.log('  ✗ Failed to load module:', error.message);
    failed += 2;
  }

  // Test 2: WASM initialization
  console.log('\nTest 2: WASM initializes...');
  try {
    const { default: init } = await import(join(wasmOutPath, 'kalam_client.js'));
    
    // For Node.js, we need to pass the WASM file path explicitly
    const wasmPath = join(wasmOutPath, 'kalam_client_bg.wasm');
    const wasmBuffer = await readFile(wasmPath);

    await init({ module_or_path: wasmBuffer });
    console.log('  ✓ WASM initialized successfully');
    passed++;
  } catch (error) {
    console.log('  ✗ WASM initialization failed:', error.message);
    failed++;
  }

  // Test 3: Client construction
  console.log('\nTest 3: KalamClient construction...');
  try {
    const { KalamClient } = await import(join(wasmOutPath, 'kalam_client.js'));
    
    const client = new KalamClient('http://localhost:8080', 'admin', 'secret');
    
    if (client) {
      console.log('  ✓ KalamClient instance created');
      passed++;
    } else {
      console.log('  ✗ Failed to create instance');
      failed++;
    }

    // Test isConnected method exists
    if (typeof client.isConnected === 'function') {
      console.log('  ✓ isConnected method exists');
      passed++;
    } else {
      console.log('  ✗ isConnected method missing');
      failed++;
    }

    // Test connect method exists
    if (typeof client.connect === 'function') {
      console.log('  ✓ connect method exists');
      passed++;
    } else {
      console.log('  ✗ connect method missing');
      failed++;
    }

    // Test disconnect method exists
    if (typeof client.disconnect === 'function') {
      console.log('  ✓ disconnect method exists');
      passed++;
    } else {
      console.log('  ✗ disconnect method missing');
      failed++;
    }

    // Test query method exists
    if (typeof client.query === 'function') {
      console.log('  ✓ query method exists');
      passed++;
    } else {
      console.log('  ✗ query method missing');
      failed++;
    }

    // Test insert method exists
    if (typeof client.insert === 'function') {
      console.log('  ✓ insert method exists');
      passed++;
    } else {
      console.log('  ✗ insert method missing');
      failed++;
    }

    // Test delete method exists
    if (typeof client.delete === 'function') {
      console.log('  ✓ delete method exists');
      passed++;
    } else {
      console.log('  ✗ delete method missing');
      failed++;
    }

    // Test subscribe method exists
    if (typeof client.subscribe === 'function') {
      console.log('  ✓ subscribe method exists');
      passed++;
    } else {
      console.log('  ✗ subscribe method missing');
      failed++;
    }

    // Test unsubscribe method exists
    if (typeof client.unsubscribe === 'function') {
      console.log('  ✓ unsubscribe method exists');
      passed++;
    } else {
      console.log('  ✗ unsubscribe method missing');
      failed++;
    }

    // Consumer APIs now live in @kalamdb/consumer, not the main wasm bundle.
    if (typeof client.consume === 'undefined') {
      console.log('  ✓ consume method is absent from main wasm bundle');
      passed++;
    } else {
      console.log('  ✗ consume method should not exist on main wasm bundle');
      failed++;
    }

    if (typeof client.ack === 'undefined') {
      console.log('  ✓ ack method is absent from main wasm bundle');
      passed++;
    } else {
      console.log('  ✗ ack method should not exist on main wasm bundle');
      failed++;
    }

  } catch (error) {
    console.log('  ✗ Client construction failed:', error.message);
    failed += 11;
  }

  // Test 4: Required parameters validation
  console.log('\nTest 4: Constructor parameter validation...');
  try {
    const { KalamClient } = await import(join(wasmOutPath, 'kalam_client.js'));
    
    // Should throw with empty URL
    try {
      new KalamClient('', 'admin', 'secret');
      console.log('  ✗ Empty URL should throw error');
      failed++;
    } catch (error) {
      console.log('  ✓ Empty URL throws error');
      passed++;
    }

    // Should throw with empty username
    try {
      new KalamClient('http://localhost:8080', '', 'secret');
      console.log('  ✗ Empty username should throw error');
      failed++;
    } catch (error) {
      console.log('  ✓ Empty username throws error');
      passed++;
    }

  } catch (error) {
    console.log('  ✗ Parameter validation test failed:', error.message);
    failed += 2;
  }

  // Results
  console.log('\n' + '='.repeat(50));
  console.log(`Results: ${passed} passed, ${failed} failed`);
  console.log('='.repeat(50));

  if (failed === 0) {
    console.log('\n✅ All tests passed!');
    process.exit(0);
  } else {
    console.log('\n❌ Some tests failed');
    process.exit(1);
  }
}

runTests().catch(error => {
  console.error('Test suite error:', error);
  process.exit(1);
});
