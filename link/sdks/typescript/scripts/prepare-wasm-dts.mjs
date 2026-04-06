#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

const sdkDirArg = process.argv[2];
if (!sdkDirArg) {
  throw new Error('Usage: node prepare-wasm-dts.mjs <sdk-dir> [--add-json-value]');
}

const sdkDir = path.resolve(process.cwd(), sdkDirArg);
const wasmDir = path.join(sdkDir, 'wasm');
const addJsonValue = process.argv.includes('--add-json-value');

function stripDeclarationComments(content) {
  return content
    .replace(/\/\*\*[\s\S]*?\*\//g, '')
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/^[ \t]*\/\/.*$/gm, '')
    .replace(/\n{3,}/g, '\n\n')
    .trimStart();
}

for (const entry of fs.readdirSync(wasmDir)) {
  if (!entry.endsWith('.d.ts')) {
    continue;
  }

  const filePath = path.join(wasmDir, entry);
  let content = fs.readFileSync(filePath, 'utf8');

  if (addJsonValue && entry === 'kalam_client.d.ts' && !content.includes('type JsonValue')) {
    content = `type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };\n${content}`;
  }

  const stripped = stripDeclarationComments(content);
  fs.writeFileSync(filePath, stripped.endsWith('\n') ? stripped : `${stripped}\n`);
}