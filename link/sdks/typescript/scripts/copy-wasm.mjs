#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

const sdkDirArg = process.argv[2];
if (!sdkDirArg) {
  throw new Error('Usage: node copy-wasm.mjs <sdk-dir>');
}

const sdkDir = path.resolve(process.cwd(), sdkDirArg);
const wasmDir = path.join(sdkDir, 'wasm');
const distWasmDir = path.join(sdkDir, 'dist', 'wasm');

fs.mkdirSync(distWasmDir, { recursive: true });

for (const entry of fs.readdirSync(wasmDir)) {
  if (entry === '.gitignore' || entry === 'package.json' || entry.endsWith('_bg.wasm.d.ts')) {
    continue;
  }

  const sourcePath = path.join(wasmDir, entry);
  if (!fs.statSync(sourcePath).isFile()) {
    continue;
  }

  fs.copyFileSync(sourcePath, path.join(distWasmDir, entry));
}