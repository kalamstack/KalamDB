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
  const { default: init, KalamConsumerClient } = await import(wasmModulePath);
  const wasmBuffer = await readFile(wasmPath);

  await init({ module_or_path: wasmBuffer });

  const client = new KalamConsumerClient('http://127.0.0.1:8080');
  assert.equal(typeof client.consume, 'function');
  assert.equal(typeof client.ack, 'function');
});