#!/usr/bin/env node
import { createClient, Auth } from '@kalamdb/client';
import { generateSchema } from './generate.js';
import { writeFileSync } from 'fs';

const args = process.argv.slice(2);

function getArg(name: string): string | undefined {
  const index = args.indexOf(`--${name}`);
  return index >= 0 ? args[index + 1] : undefined;
}

const url = getArg('url') || process.env.KALAMDB_URL || 'http://localhost:8080';
const user = getArg('user') || process.env.KALAMDB_USER || 'admin';
const password = getArg('password') || process.env.KALAMDB_PASSWORD;
const out = getArg('out') || 'schema.ts';
const includeSystem = args.includes('--include-system');

if (!password) {
  console.error('Usage: kalamdb-orm --url <url> --user <user> --password <pass> --out <file>');
  console.error('');
  console.error('Options:');
  console.error('  --url <url>          KalamDB server URL (default: http://localhost:8080)');
  console.error('  --user <user>        Username (default: admin)');
  console.error('  --password <pass>    Password (required)');
  console.error('  --out <file>         Output file (default: schema.ts)');
  console.error('  --include-system     Include system/dba tables');
  process.exit(1);
}

async function main() {
  const client = createClient({
    url,
    authProvider: async () => Auth.basic(user, password!),
  });
  await client.initialize();

  const options = { includeSystem };
  const schema = await generateSchema(client, options);
  writeFileSync(out, schema);
  console.log(`Generated ${(schema.match(/pgTable/g) || []).length} tables → ${out}`);

  await client.disconnect();
}

main().catch((error) => {
  console.error('Error:', error.message);
  process.exit(1);
});
