import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { generateSchema } from '../dist/index.js';
import { requirePassword, createTestClient } from './helpers.mjs';

requirePassword();

let client;
let fullSchema;

before(async () => {
  client = createTestClient();
  await client.initialize();

  await client.query('CREATE NAMESPACE IF NOT EXISTS test_gen');
  await client.query('CREATE TABLE IF NOT EXISTS test_gen.uploads (id BIGINT PRIMARY KEY, doc FILE)');
  await client.query('CREATE TABLE IF NOT EXISTS test_gen.with_defaults (id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(), name TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW())');
  await client.query("CREATE TABLE IF NOT EXISTS test_gen.with_literal_default (id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(), status TEXT DEFAULT 'pending' NOT NULL)");

  fullSchema = await generateSchema(client, { includeSystem: true });
});

after(async () => {
  await client.query('DROP TABLE IF EXISTS test_gen.uploads');
  await client.query('DROP TABLE IF EXISTS test_gen.with_defaults');
  await client.query('DROP TABLE IF EXISTS test_gen.with_literal_default');
  await client.query('DROP NAMESPACE IF EXISTS test_gen');
  await client?.disconnect();
});

describe('generateSchema', () => {
  it('generates valid TypeScript with pgTable imports', () => {
    assert.ok(fullSchema.includes("import {"));
    assert.ok(fullSchema.includes("from 'drizzle-orm/pg-core'"));
    assert.ok(fullSchema.includes("pgTable"));
  });

  it('includes system tables', () => {
    assert.ok(fullSchema.includes("system_users"));
    assert.ok(fullSchema.includes("system_audit_log"));
    assert.ok(fullSchema.includes("system_jobs"));
    assert.ok(fullSchema.includes("system_storages"));
  });

  it('includes hidden system tables', () => {
    assert.ok(fullSchema.includes("system_live"));
    assert.ok(fullSchema.includes("system_server_logs"));
    assert.ok(fullSchema.includes("system_settings"));
    assert.ok(fullSchema.includes("system_stats"));
    assert.ok(fullSchema.includes("system_cluster"));
  });

  it('includes dba tables', () => {
    assert.ok(fullSchema.includes("dba_"));
  });

  it('uses snake_case for field names', () => {
    assert.ok(fullSchema.includes("user_id:"));
    assert.ok(fullSchema.includes("actor_user_id:"));
    assert.ok(!fullSchema.includes("userId:"));
    assert.ok(!fullSchema.includes("actorUserId:"));
  });

  it('maps timestamps to timestamp with mode string', () => {
    assert.ok(fullSchema.includes("timestamp('created_at', { mode: 'string' })"));
  });

  it('marks non-nullable columns with notNull()', () => {
    assert.ok(fullSchema.includes(".notNull()"));
  });

  it('filters out underscore-prefixed columns', () => {
    assert.ok(!fullSchema.includes("_seq:"));
    assert.ok(!fullSchema.includes("_deleted:"));
  });

  it('maps FILE columns to file() with @kalamdb/orm import', () => {
    assert.ok(fullSchema.includes("file('doc')"));
    assert.ok(fullSchema.includes("from '@kalamdb/orm'"));
  });

  it('marks columns with server defaults as optional via .default()', () => {
    assert.ok(fullSchema.includes(".default(sql``)"));
  });

  it('imports sql from drizzle-orm when defaults are present', () => {
    assert.ok(fullSchema.includes("import { sql } from 'drizzle-orm'"));
  });

  it('marks default columns in user-defined table with SNOWFLAKE_ID and NOW', () => {
    assert.ok(fullSchema.includes("test_gen_with_defaults"));
    const lines = fullSchema.split('\n');
    const tableStart = lines.findIndex((l) => l.includes('test_gen_with_defaults'));
    const tableLines = lines.slice(tableStart, tableStart + 6);
    const idDef = tableLines.find((l) => l.trim().startsWith('id:'));
    const createdDef = tableLines.find((l) => l.trim().startsWith('created_at:'));
    const nameDef = tableLines.find((l) => l.trim().startsWith('name:'));
    assert.ok(idDef.includes(".default(sql``)"), 'id should have default');
    assert.ok(createdDef.includes(".default(sql``)"), 'created_at should have default');
    assert.ok(!nameDef.includes('.default('), 'name should not have default');
  });

  it('maps BIGINT columns to text (KalamDB returns them as strings)', () => {
    const lines = fullSchema.split('\n');
    const tableStart = lines.findIndex((l) => l.includes('test_gen_with_defaults'));
    const tableLines = lines.slice(tableStart, tableStart + 6);
    const idDef = tableLines.find((l) => l.trim().startsWith('id:'));
    assert.ok(idDef.includes("text('id')"), 'BIGINT should be text');
    assert.ok(!idDef.includes("bigint("), 'BIGINT should not use bigint type');
  });

  it('keeps TIMESTAMP as drizzle timestamp with mode string', () => {
    const lines = fullSchema.split('\n');
    const tableStart = lines.findIndex((l) => l.includes('test_gen_with_defaults'));
    const tableLines = lines.slice(tableStart, tableStart + 6);
    const createdDef = tableLines.find((l) => l.trim().startsWith('created_at:'));
    assert.ok(createdDef.includes("timestamp('created_at', { mode: 'string' })"), 'timestamp should use drizzle timestamp');
  });

  it('marks literal default columns', () => {
    assert.ok(fullSchema.includes('test_gen_with_literal_default'));
    const lines = fullSchema.split('\n');
    const tableStart = lines.findIndex((l) => l.includes('test_gen_with_literal_default'));
    const tableLines = lines.slice(tableStart, tableStart + 5);
    const statusDef = tableLines.find((l) => l.trim().startsWith('status:'));
    assert.ok(statusDef.includes(".default(sql``)"), 'status should have default');
  });

  it('does not add .default() to columns without server defaults', () => {
    const uploadLines = fullSchema.split('\n').filter((l) => l.includes("file('doc')"));
    assert.equal(uploadLines.length, 1);
    assert.ok(!uploadLines[0].includes('.default('));
  });

  it('excludes system tables by default', async () => {
    await new Promise((r) => setTimeout(r, 1000));
    const schema = await generateSchema(client);
    assert.ok(!schema.includes("system_users"));
    assert.ok(!schema.includes("system_audit_log"));
    assert.ok(!schema.includes("dba_"));
    assert.ok(schema.includes("test_gen_uploads"));
    assert.ok(schema.includes("test_gen_with_defaults"));
  });

  it('includes system tables when includeSystem is true', async () => {
    await new Promise((r) => setTimeout(r, 1000));
    const schema = await generateSchema(client, { includeSystem: true });
    assert.ok(schema.includes("system_users"));
    assert.ok(schema.includes("system_audit_log"));
    assert.ok(schema.includes("dba_"));
    assert.ok(schema.includes("test_gen_uploads"));
  });

  it('deduplicates tables (no duplicate exports)', () => {
    const exportLines = fullSchema.split('\n').filter((l) => l.startsWith('export const '));
    const tableNames = exportLines.map((l) => l.match(/export const (\w+)/)?.[1]);
    const uniqueNames = new Set(tableNames);
    assert.equal(tableNames.length, uniqueNames.size, 'each table should appear exactly once');
  });
});
