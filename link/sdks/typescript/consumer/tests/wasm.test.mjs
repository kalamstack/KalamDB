import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const wasmOutPath = join(__dirname, '..', 'dist', 'wasm');

test('consumer package ships a dedicated low-level wasm module', async () => {
  const wasmModulePath = pathToFileURL(join(wasmOutPath, 'kalam_consumer.js')).href;
  const wasmPath = join(wasmOutPath, 'kalam_consumer_bg.wasm');
  const wasmModule = await import(wasmModulePath);
  const { default: init, KalamConsumerClient } = wasmModule;
  const wasmBuffer = await readFile(wasmPath);

  await init({ module_or_path: wasmBuffer });

  const client = new KalamConsumerClient('http://127.0.0.1:8080');
  assert.equal(typeof client.consume, 'function');
  assert.equal(typeof client.ack, 'function');
  assert.equal(typeof client.connect, 'undefined');
  assert.equal('KalamClient' in wasmModule, false);
});

test('consumer wasm declarations stay worker-only', async () => {
  const dts = await readFile(join(wasmOutPath, 'kalam_consumer.d.ts'), 'utf8');

  assert.equal(dts.includes('export class KalamConsumerClient'), true);
  assert.equal(dts.includes('KalamClient'), false);
  assert.equal(dts.includes('connect(): Promise<void>;'), false);
  assert.equal(dts.includes('query('), false);
  assert.equal(dts.includes('login('), false);
  assert.equal(dts.includes('refresh_access_token'), false);
  assert.equal(dts.includes('subscribe('), false);
});