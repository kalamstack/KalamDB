/**
 * DDL e2e tests — CREATE/DROP NAMESPACE, CREATE/DROP TABLE, schema inspection.
 *
 * Run: node --test tests/e2e/ddl/ddl.test.mjs
 */

import { test, describe, before, after } from 'node:test';
import assert from 'node:assert/strict';
import {
  connectJwtClient,
  uniqueName,
  ensureNamespace,
  dropTable,
} from '../helpers.mjs';

describe('DDL', { timeout: 30_000 }, () => {
  let client;
  const ns = uniqueName('ts_ddl');

  before(async () => {
    client = await connectJwtClient();
    await ensureNamespace(client, ns);
  });

  after(async () => {
    await client.disconnect();
  });

  // -----------------------------------------------------------------------
  // CREATE TABLE
  // -----------------------------------------------------------------------
  test('CREATE TABLE creates a new table', async () => {
    const tbl = `${ns}.${uniqueName('tbl')}`;
    const res = await client.query(
      `CREATE TABLE IF NOT EXISTS ${tbl} (
        id INT PRIMARY KEY,
        name TEXT
      )`,
    );
    assert.ok(res, 'CREATE TABLE should succeed');
    await dropTable(client, tbl);
  });

  // -----------------------------------------------------------------------
  // DROP TABLE
  // -----------------------------------------------------------------------
  test('DROP TABLE IF EXISTS succeeds for existing table', async () => {
    const tbl = `${ns}.${uniqueName('drop_me')}`;
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${tbl} (id INT PRIMARY KEY)`,
    );
    const res = await client.query(`DROP TABLE IF EXISTS ${tbl}`);
    assert.ok(res);
  });

  test('DROP TABLE IF EXISTS succeeds for nonexistent table', async () => {
    const res = await client.query(
      `DROP TABLE IF EXISTS ${ns}.nonexistent_xyz_999`,
    );
    assert.ok(res);
  });

  // -----------------------------------------------------------------------
  // Table with various column types
  // -----------------------------------------------------------------------
  test('table with multiple column types', async () => {
    const tbl = `${ns}.${uniqueName('typed')}`;
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${tbl} (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        name TEXT NOT NULL,
        active BOOLEAN DEFAULT true,
        score DOUBLE,
        count INT
      )`,
    );

    // Insert and verify types survive roundtrip
    await client.query(
      `INSERT INTO ${tbl} (name, active, score, count)
       VALUES ('alice', true, 98.5, 42)`,
    );

    const row = await client.queryOne(
      `SELECT name, active, score, count FROM ${tbl} WHERE name = 'alice'`,
    );
    assert.equal(row.name.asString(), 'alice');
    assert.equal(row.active.asBool(), true);
    assert.equal(row.score.asFloat(), 98.5);
    assert.equal(row.count.asInt(), 42);

    await dropTable(client, tbl);
  });

  // -----------------------------------------------------------------------
  // CREATE NAMESPACE
  // -----------------------------------------------------------------------
  test('CREATE NAMESPACE IF NOT EXISTS is idempotent', async () => {
    const name = uniqueName('ns_idem');
    await client.query(`CREATE NAMESPACE IF NOT EXISTS ${name}`);
    const res = await client.query(`CREATE NAMESPACE IF NOT EXISTS ${name}`);
    assert.ok(res);
  });
});
