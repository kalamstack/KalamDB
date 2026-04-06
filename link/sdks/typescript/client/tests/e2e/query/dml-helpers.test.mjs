/**
 * DML convenience method tests — insert(), update(), delete() helpers.
 *
 * Run: node --test tests/e2e/query/dml-helpers.test.mjs
 */

import { test, describe, before, after } from 'node:test';
import assert from 'node:assert/strict';
import {
  connectJwtClient,
  uniqueName,
  ensureNamespace,
  dropTable,
} from '../helpers.mjs';

describe('DML Helpers', { timeout: 30_000 }, () => {
  let client;
  const ns = uniqueName('ts_dml');
  const tbl = `${ns}.tasks`;

  before(async () => {
    client = await connectJwtClient();
    await ensureNamespace(client, ns);
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${tbl} (
        id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
        title TEXT NOT NULL,
        done BOOLEAN DEFAULT false
      )`,
    );
  });

  after(async () => {
    await dropTable(client, tbl);
    await client.disconnect();
  });

  // -----------------------------------------------------------------------
  // insert()
  // -----------------------------------------------------------------------
  test('insert() adds a row and returns response', async () => {
    const res = await client.insert(tbl, { title: 'buy milk', done: false });
    assert.ok(res, 'insert should return response');
  });

  // -----------------------------------------------------------------------
  // update()
  // -----------------------------------------------------------------------
  test('update() modifies an existing row', async () => {
    // Insert a row we know the id of
    await client.query(
      `INSERT INTO ${tbl} (id, title, done) VALUES (100, 'to update', false)`,
    );
    const res = await client.update(tbl, 100, { title: 'updated title', done: true });
    assert.ok(res);

    const row = await client.queryOne(
      `SELECT title, done FROM ${tbl} WHERE id = 100`,
    );
    assert.equal(row.title.asString(), 'updated title');
    assert.equal(row.done.asBool(), true);
  });

  // -----------------------------------------------------------------------
  // delete()
  // -----------------------------------------------------------------------
  test('delete() removes a row by id', async () => {
    await client.query(
      `INSERT INTO ${tbl} (id, title) VALUES (200, 'to delete')`,
    );
    await client.delete(tbl, 200);

    const rows = await client.queryAll(`SELECT * FROM ${tbl} WHERE id = 200`);
    assert.equal(rows.length, 0);
  });
});
