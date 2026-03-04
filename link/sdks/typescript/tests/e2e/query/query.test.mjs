/**
 * Query e2e tests — SELECT, INSERT, UPDATE, DELETE, params, errors.
 *
 * Run: node --test tests/e2e/query/query.test.mjs
 */

import { test, describe, before, after } from 'node:test';
import assert from 'node:assert/strict';
import {
  connectJwtClient,
  uniqueName,
  ensureNamespace,
  dropTable,
  sleep,
} from '../helpers.mjs';

describe('Query', { timeout: 30_000 }, () => {
  let client;
  const ns = uniqueName('ts_query');
  const tbl = `${ns}.items`;

  before(async () => {
    client = await connectJwtClient();
    await ensureNamespace(client, ns);
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${tbl} (
        id INT PRIMARY KEY,
        title TEXT NOT NULL,
        done BOOLEAN,
        score DOUBLE
      )`,
    );
  });

  after(async () => {
    await dropTable(client, tbl);
    await client.disconnect();
  });

  // -----------------------------------------------------------------------
  // Simple SELECT
  // -----------------------------------------------------------------------
  test('SELECT literal returns result', async () => {
    const res = await client.query("SELECT 1 AS n, 'hello' AS s");
    assert.ok(res.results?.length > 0);
  });

  // -----------------------------------------------------------------------
  // INSERT + SELECT roundtrip
  // -----------------------------------------------------------------------
  test('INSERT then SELECT returns inserted row', async () => {
    const ins = await client.query(
      `INSERT INTO ${tbl} (id, title, done, score) VALUES (1, 'first', true, 9.5)`,
    );
    assert.ok(ins, 'insert should return response');

    // queryOne transforms raw row arrays into keyed objects
    const row = await client.queryOne(`SELECT * FROM ${tbl} WHERE id = 1`);
    assert.ok(row, 'expected at least one row');
    assert.equal(row.id.asInt(), 1);
    assert.equal(row.title.asString(), 'first');
    assert.equal(row.done.asBool(), true);
    assert.equal(row.score.asFloat(), 9.5);
  });

  // -----------------------------------------------------------------------
  // Parameterised query
  // -----------------------------------------------------------------------
  test('parameterised INSERT + SELECT with $1 $2 $3', async () => {
    await client.query(
      `INSERT INTO ${tbl} (id, title, done, score) VALUES ($1, $2, $3, $4)`,
      [2, 'parameterised', false, 3.14],
    );

    const row = await client.queryOne(
      `SELECT title, done FROM ${tbl} WHERE id = $1`,
      [2],
    );
    assert.equal(row.title.asString(), 'parameterised');
    assert.equal(row.done.asBool(), false);
  });

  // -----------------------------------------------------------------------
  // UPDATE
  // -----------------------------------------------------------------------
  test('UPDATE modifies existing row', async () => {
    await client.query(`UPDATE ${tbl} SET title = 'updated' WHERE id = 1`);

    const row = await client.queryOne(
      `SELECT title FROM ${tbl} WHERE id = 1`,
    );
    assert.equal(row.title.asString(), 'updated');
  });

  // -----------------------------------------------------------------------
  // DELETE
  // -----------------------------------------------------------------------
  test('DELETE removes row', async () => {
    await client.query(
      `INSERT INTO ${tbl} (id, title, done) VALUES (999, 'to-delete', false)`,
    );
    await client.query(`DELETE FROM ${tbl} WHERE id = 999`);

    const rows = await client.queryAll(
      `SELECT * FROM ${tbl} WHERE id = 999`,
    );
    assert.equal(rows.length, 0);
  });

  // -----------------------------------------------------------------------
  // CREATE NAMESPACE
  // -----------------------------------------------------------------------
  test('CREATE NAMESPACE IF NOT EXISTS succeeds', async () => {
    const res = await client.query(
      `CREATE NAMESPACE IF NOT EXISTS ${uniqueName('ns_test')}`,
    );
    assert.ok(res);
  });

  // -----------------------------------------------------------------------
  // Query error for nonexistent table
  // -----------------------------------------------------------------------
  test('SELECT from nonexistent table returns error', async () => {
    await assert.rejects(
      () => client.query('SELECT * FROM nonexistent_ns_xyz.nonexistent_table'),
      'query on nonexistent table should reject',
    );
  });

  // -----------------------------------------------------------------------
  // queryOne / queryAll convenience
  // -----------------------------------------------------------------------
  test('queryOne returns first row or null', async () => {
    const row = await client.queryOne(`SELECT * FROM ${tbl} WHERE id = 1`);
    assert.ok(row);
    assert.equal(row.id.asInt(), 1);

    const missing = await client.queryOne(
      `SELECT * FROM ${tbl} WHERE id = -1`,
    );
    assert.equal(missing, null);
  });

  test('queryAll returns array of rows', async () => {
    const rows = await client.queryAll(`SELECT * FROM ${tbl} ORDER BY id`);
    assert.ok(Array.isArray(rows));
    assert.ok(rows.length >= 1);
  });

  // -----------------------------------------------------------------------
  // Multiple results
  // -----------------------------------------------------------------------
  test('multiple inserts in single call', async () => {
    await client.query(
      `INSERT INTO ${tbl} (id, title, done) VALUES (10, 'a', true)`,
    );
    await client.query(
      `INSERT INTO ${tbl} (id, title, done) VALUES (11, 'b', false)`,
    );

    const rows = await client.queryAll(
      `SELECT id FROM ${tbl} WHERE id IN (10, 11) ORDER BY id`,
    );
    assert.ok(rows.length >= 2);
  });
});
