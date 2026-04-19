import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { drizzle } from 'drizzle-orm/pg-proxy';
import { pgTable, text, bigint } from 'drizzle-orm/pg-core';
import { eq, desc } from 'drizzle-orm';
import { kalamDriver, file } from '../dist/index.js';
import { requirePassword, createTestClient } from './helpers.mjs';

requirePassword();

let client;
let db;

const system_users = pgTable('system.users', {
  user_id: text('user_id'),
  username: text('username'),
  role: text('role'),
  email: text('email'),
});

const system_audit_log = pgTable('system.audit_log', {
  audit_id: text('audit_id'),
  actor_username: text('actor_username'),
  action: text('action'),
  target: text('target'),
  timestamp: bigint('timestamp', { mode: 'number' }),
});

before(async () => {
  client = createTestClient();
  await client.initialize();
  db = drizzle(kalamDriver(client));
});

after(async () => {
  await client?.disconnect();
});

describe('kalamDriver', () => {
  it('executes a simple SELECT', async () => {
    const rows = await db.select().from(system_users);
    assert.ok(Array.isArray(rows));
    assert.ok(rows.length > 0);
  });

  it('returns typed fields', async () => {
    const rows = await db.select().from(system_users);
    const first = rows[0];
    assert.ok(typeof first.username === 'string');
    assert.ok(typeof first.role === 'string');
  });

  it('supports WHERE clause', async () => {
    const rows = await db
      .select()
      .from(system_users)
      .where(eq(system_users.role, 'dba'));
    assert.ok(rows.length > 0);
    for (const row of rows) {
      assert.equal(row.role, 'dba');
    }
  });

  it('supports ORDER BY and LIMIT', async () => {
    const rows = await db
      .select()
      .from(system_audit_log)
      .orderBy(desc(system_audit_log.timestamp))
      .limit(5);
    assert.ok(rows.length <= 5);
    if (rows.length >= 2) {
      assert.ok(rows[0].timestamp >= rows[1].timestamp);
    }
  });

  it('supports SELECT with specific columns', async () => {
    const rows = await db
      .select({ username: system_users.username, role: system_users.role })
      .from(system_users);
    assert.ok(rows.length > 0);
    assert.ok('username' in rows[0]);
    assert.ok('role' in rows[0]);
    assert.ok(!('email' in rows[0]));
  });

  it('handles null values in nullable columns', async () => {
    const rows = await db.select().from(system_users);
    const row = rows[0];
    // email is nullable — could be string or null, both are valid
    assert.ok(row.email === null || typeof row.email === 'string');
  });

  it('handles INSERT with server-side DEFAULT (SNOWFLAKE_ID)', async () => {
    await client.query('CREATE NAMESPACE IF NOT EXISTS test_orm_insert');
    await client.query('DROP TABLE IF EXISTS test_orm_insert.items');
    await client.query(`
      CREATE TABLE test_orm_insert.items (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        name TEXT NOT NULL
      )
    `);

    const items = pgTable('test_orm_insert.items', {
      id: bigint('id', { mode: 'number' }),
      name: text('name'),
    });

    await db.insert(items).values({ name: 'drizzle-test' });
    const rows = await db.select().from(items);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].name, 'drizzle-test');
    assert.ok(typeof rows[0].id === 'number', 'id should be auto-generated as number');
    assert.ok(rows[0].id > 0, 'auto-generated id should be positive');

    await client.query('DROP TABLE IF EXISTS test_orm_insert.items');
    await client.query('DROP NAMESPACE IF EXISTS test_orm_insert');
  });

  it('handles INSERT with multiple server-side DEFAULTs', async () => {
    await client.query('CREATE NAMESPACE IF NOT EXISTS test_orm_insert');
    await client.query('DROP TABLE IF EXISTS test_orm_insert.multi_defaults');
    await client.query(`
      CREATE TABLE test_orm_insert.multi_defaults (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        label TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `);

    const table = pgTable('test_orm_insert.multi_defaults', {
      id: bigint('id', { mode: 'number' }),
      label: text('label'),
      created_at: bigint('created_at', { mode: 'number' }),
    });

    await db.insert(table).values({ label: 'multi-default-test' });
    const rows = await db.select().from(table);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].label, 'multi-default-test');
    assert.ok(rows[0].id > 0, 'id should be auto-generated');
    assert.ok(rows[0].created_at > 0, 'created_at should be auto-generated');

    await client.query('DROP TABLE IF EXISTS test_orm_insert.multi_defaults');
    await client.query('DROP NAMESPACE IF EXISTS test_orm_insert');
  });

  it('INSERT with explicit id still works alongside DEFAULT columns', async () => {
    await client.query('CREATE NAMESPACE IF NOT EXISTS test_orm_insert');
    await client.query('DROP TABLE IF EXISTS test_orm_insert.explicit_id');
    await client.query(`
      CREATE TABLE test_orm_insert.explicit_id (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        name TEXT NOT NULL
      )
    `);

    const table = pgTable('test_orm_insert.explicit_id', {
      id: bigint('id', { mode: 'number' }),
      name: text('name'),
    });

    await db.insert(table).values({ id: 99999, name: 'explicit' });
    const rows = await db.select().from(table);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].id, 99999);
    assert.equal(rows[0].name, 'explicit');

    await client.query('DROP TABLE IF EXISTS test_orm_insert.explicit_id');
    await client.query('DROP NAMESPACE IF EXISTS test_orm_insert');
  });

  it('handles multi-row INSERT with server-side DEFAULT', async () => {
    await client.query('CREATE NAMESPACE IF NOT EXISTS test_orm_insert');
    await client.query('DROP TABLE IF EXISTS test_orm_insert.multi_row');
    await client.query(`
      CREATE TABLE test_orm_insert.multi_row (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        name TEXT NOT NULL
      )
    `);

    const table = pgTable('test_orm_insert.multi_row', {
      id: bigint('id', { mode: 'number' }),
      name: text('name'),
    });

    await db.insert(table).values([{ name: 'one' }, { name: 'two' }, { name: 'three' }]);
    const rows = await db.select().from(table);
    assert.equal(rows.length, 3);
    const names = rows.map((r) => r.name).sort();
    assert.deepEqual(names, ['one', 'three', 'two']);
    const ids = new Set(rows.map((r) => r.id));
    assert.equal(ids.size, 3, 'each row should have a unique auto-generated id');

    await client.query('DROP TABLE IF EXISTS test_orm_insert.multi_row');
    await client.query('DROP NAMESPACE IF EXISTS test_orm_insert');
  });

  it('handles UPDATE via Drizzle', async () => {
    await client.query('CREATE NAMESPACE IF NOT EXISTS test_orm_insert');
    await client.query('DROP TABLE IF EXISTS test_orm_insert.updatable');
    await client.query(`
      CREATE TABLE test_orm_insert.updatable (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        name TEXT NOT NULL,
        status TEXT NOT NULL
      )
    `);

    const table = pgTable('test_orm_insert.updatable', {
      id: bigint('id', { mode: 'number' }),
      name: text('name'),
      status: text('status'),
    });

    await db.insert(table).values({ name: 'update-test', status: 'draft' });
    const before = await db.select().from(table);
    assert.equal(before[0].status, 'draft');

    await db.update(table).set({ status: 'published' }).where(eq(table.name, 'update-test'));
    const after = await db.select().from(table);
    assert.equal(after[0].status, 'published');

    await client.query('DROP TABLE IF EXISTS test_orm_insert.updatable');
    await client.query('DROP NAMESPACE IF EXISTS test_orm_insert');
  });

  it('handles DELETE via Drizzle', async () => {
    await client.query('CREATE NAMESPACE IF NOT EXISTS test_orm_insert');
    await client.query('DROP TABLE IF EXISTS test_orm_insert.deletable');
    await client.query(`
      CREATE TABLE test_orm_insert.deletable (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        name TEXT NOT NULL
      )
    `);

    const table = pgTable('test_orm_insert.deletable', {
      id: bigint('id', { mode: 'number' }),
      name: text('name'),
    });

    await db.insert(table).values([{ name: 'keep' }, { name: 'remove' }]);
    const before = await db.select().from(table);
    assert.equal(before.length, 2);

    await db.delete(table).where(eq(table.name, 'remove'));
    const after = await db.select().from(table);
    assert.equal(after.length, 1);
    assert.equal(after[0].name, 'keep');

    await client.query('DROP TABLE IF EXISTS test_orm_insert.deletable');
    await client.query('DROP NAMESPACE IF EXISTS test_orm_insert');
  });

  it('preserves SNOWFLAKE_ID precision when using text type', async () => {
    await client.query('CREATE NAMESPACE IF NOT EXISTS test_orm_insert');
    await client.query('DROP TABLE IF EXISTS test_orm_insert.precision');
    await client.query(`
      CREATE TABLE test_orm_insert.precision (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        name TEXT NOT NULL
      )
    `);

    const table = pgTable('test_orm_insert.precision', {
      id: text('id'),
      name: text('name'),
    });

    await db.insert(table).values({ name: 'precision-test' });
    const rows = await db.select().from(table);
    assert.equal(rows.length, 1);
    assert.equal(typeof rows[0].id, 'string', 'id should be a string');
    assert.ok(rows[0].id.length > 15, 'id should be a long snowflake id');

    const rawResult = await client.query('SELECT id FROM test_orm_insert.precision');
    const rawId = String(rawResult.results[0]?.named_rows[0]?.id);
    assert.equal(rows[0].id, rawId, 'id should match raw query exactly (no precision loss)');

    await client.query('DROP TABLE IF EXISTS test_orm_insert.precision');
    await client.query('DROP NAMESPACE IF EXISTS test_orm_insert');
  });

  it('throws when querying a non-existent table', async () => {
    const fake = pgTable('nonexistent.fake_table', {
      id: bigint('id', { mode: 'number' }),
    });

    await assert.rejects(
      () => db.select().from(fake),
    );
  });

  it('handles FILE column with null value', async () => {
    await client.query('CREATE NAMESPACE IF NOT EXISTS test_orm');
    await client.query(`
      CREATE TABLE IF NOT EXISTS test_orm.nullable_files (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        doc FILE
      )
    `);
    await client.query("INSERT INTO test_orm.nullable_files (id) VALUES (1)");

    const table = pgTable('test_orm.nullable_files', {
      id: bigint('id', { mode: 'number' }),
      doc: file('doc'),
    });

    const rows = await db.select().from(table);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].doc, null);

    await client.query('DROP TABLE IF EXISTS test_orm.nullable_files');
    await client.query('DROP NAMESPACE IF EXISTS test_orm');
  });

  it('handles FILE column type with real data', async () => {
    await client.query('CREATE NAMESPACE IF NOT EXISTS test_orm');
    await client.query(`
      CREATE TABLE IF NOT EXISTS test_orm.docs (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        title TEXT,
        attachment FILE
      )
    `);

    const fakeFile = JSON.stringify({
      id: '99999', sub: 'f0001', name: 'report.pdf',
      size: 8192, mime: 'application/pdf', sha256: 'deadbeef',
    });
    await client.query(`INSERT INTO test_orm.docs (title, attachment) VALUES ('quarterly', '${fakeFile}')`);

    const test_docs = pgTable('test_orm.docs', {
      id: bigint('id', { mode: 'number' }),
      title: text('title'),
      attachment: file('attachment'),
    });

    const rows = await db.select().from(test_docs);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].title, 'quarterly');
    assert.ok(rows[0].attachment);
    assert.equal(rows[0].attachment.name, 'report.pdf');
    assert.equal(rows[0].attachment.size, 8192);
    assert.equal(rows[0].attachment.mime, 'application/pdf');
    assert.ok(rows[0].attachment.getDownloadUrl('http://localhost:8088', 'test_orm', 'docs').includes('/f0001/99999'));

    await client.query('DROP TABLE IF EXISTS test_orm.docs');
    await client.query('DROP NAMESPACE IF EXISTS test_orm');
  });
});
