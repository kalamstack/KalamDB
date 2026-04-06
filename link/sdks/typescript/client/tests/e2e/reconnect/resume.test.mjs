/**
 * Proxy-style resume e2e tests — disconnect, insert during gap,
 * reconnect, verify seq-based resume without replaying old rows.
 *
 * These tests verify the same resume semantics as the Rust proxy tests,
 * using the SDK's built-in disconnect/reconnect API instead of a TCP proxy.
 *
 * Run: node --test tests/e2e/reconnect/resume.test.mjs
 */

import { test, describe, before, after } from 'node:test';
import assert from 'node:assert/strict';
import {
  SERVER_URL,
  ADMIN_USER,
  ADMIN_PASS,
  connectJwtClient,
  uniqueName,
  ensureNamespace,
  dropTable,
  sleep,
} from '../helpers.mjs';
import { createClient, Auth } from '../../../dist/src/index.js';

async function waitFor(predicate, timeoutMs = 15_000, intervalMs = 100) {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error('Timed out waiting for condition');
    }
    await sleep(intervalMs);
  }
}

function unwrapCell(value) {
  if (value && typeof value.asInt === 'function') return value.asInt();
  if (value && typeof value.asString === 'function') return value.asString();
  return value;
}

function extractRows(events) {
  const rows = [];
  for (const event of events) {
    if (
      (event.type === 'change' || event.type === 'initial_data_batch') &&
      Array.isArray(event.rows)
    ) {
      rows.push(...event.rows);
    }
  }
  return rows;
}

function hasRowId(events, expectedId) {
  return extractRows(events).some((row) => unwrapCell(row.id) === expectedId);
}

function assertRowsStrictlyAfter(events, from, context) {
  for (const row of extractRows(events)) {
    const seq = row._seq?.asSeqId?.();
    if (!seq) continue;
    assert.ok(
      seq.compareTo(from) > 0,
      `${context}: received stale row with _seq=${seq.toString()} at/before from=${from.toString()}`,
    );
  }
}

function assertNoDuplicateSeqRows(events, context) {
  const seen = new Set();
  for (const row of extractRows(events)) {
    const seq = row._seq?.asSeqId?.();
    if (!seq) continue;
    const key = seq.toString();
    assert.ok(!seen.has(key), `${context}: duplicate _seq replayed: ${key}`);
    seen.add(key);
  }
}

async function shutdownClient(client) {
  if (!client) return;
  if (typeof client.shutdown === 'function') {
    await client.shutdown();
    return;
  }
  await client.disconnect();
}

describe('Resume from checkpoint after disconnect', { timeout: 120_000 }, () => {
  let client;
  const ns = uniqueName('ts_resume');

  before(async () => {
    client = await connectJwtClient();
    await ensureNamespace(client, ns);
  });

  after(async () => {
    try {
      await client.unsubscribeAll();
    } catch (_) {}
    await shutdownClient(client);
  });

  // Live-update resume: insert before, disconnect, insert gap, reconnect, insert live
  test('subscription resumes from checkpoint after disconnect — no replay', async () => {
    const tbl = `${ns}.resume_live`;
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${tbl} (id INT PRIMARY KEY, value TEXT)`,
    );

    const writer = await connectJwtClient();
    const preId = 91001;
    const gapId = 91002;
    const liveId = 91003;

    try {
      // Subscribe and wait for ack
      const preEvents = [];
      const unsub = await client.subscribeWithSql(
        `SELECT id, value FROM ${tbl}`,
        (ev) => preEvents.push(ev),
        { last_rows: 0 },
      );
      await waitFor(() => preEvents.some((e) => e.type === 'subscription_ack'));

      // Insert before-disconnect row
      await writer.query(
        `INSERT INTO ${tbl} (id, value) VALUES (${preId}, 'before')`,
      );
      await waitFor(() => hasRowId(preEvents, preId));

      // Capture checkpoint
      await waitFor(() => {
        const sub = client
          .getSubscriptions()
          .find((s) => s.tableName === `SELECT id, value FROM ${tbl}`);
        return !!sub?.lastSeqId;
      });
      const checkpoint = client
        .getSubscriptions()
        .find((s) => s.tableName === `SELECT id, value FROM ${tbl}`)?.lastSeqId;
      assert.ok(checkpoint, 'checkpoint should exist before disconnect');

      await unsub();

      // Disconnect
      await client.disconnect();
      assert.equal(client.isConnected(), false);

      // Insert gap row during disconnect
      await writer.query(
        `INSERT INTO ${tbl} (id, value) VALUES (${gapId}, 'gap')`,
      );

      // Reconnect with from: checkpoint
      const resumedEvents = [];
      const unsub2 = await client.subscribeWithSql(
        `SELECT id, value FROM ${tbl}`,
        (ev) => resumedEvents.push(ev),
        { from: checkpoint, last_rows: 0 },
      );
      assert.equal(client.isConnected(), true, 'should reconnect on subscribe');

      await waitFor(() =>
        resumedEvents.some((e) => e.type === 'subscription_ack'),
      );

      // Insert live row after reconnect
      await writer.query(
        `INSERT INTO ${tbl} (id, value) VALUES (${liveId}, 'live')`,
      );

      // Wait for both gap and live rows
      await waitFor(() => hasRowId(resumedEvents, gapId) && hasRowId(resumedEvents, liveId));

      // Verify no replay of pre-disconnect row
      assert.ok(
        !hasRowId(resumedEvents, preId),
        'pre-disconnect row must NOT be replayed',
      );
      assertRowsStrictlyAfter(resumedEvents, checkpoint, 'resume-live');
      assertNoDuplicateSeqRows(resumedEvents, 'resume-live');

      await unsub2();
    } finally {
      await dropTable(client, tbl);
      await shutdownClient(writer);
    }
  });

  // Three-subscription resume: all three resume independently
  test('three subscriptions resume from their checkpoints after disconnect', async () => {
    const tblA = `${ns}.resume3_a`;
    const tblB = `${ns}.resume3_b`;
    const tblC = `${ns}.resume3_c`;

    await client.query(`CREATE TABLE IF NOT EXISTS ${tblA} (id INT PRIMARY KEY, value TEXT)`);
    await client.query(`CREATE TABLE IF NOT EXISTS ${tblB} (id INT PRIMARY KEY, value TEXT)`);
    await client.query(`CREATE TABLE IF NOT EXISTS ${tblC} (id INT PRIMARY KEY, value TEXT)`);

    const writer = await connectJwtClient();
    const pre = { a: 81001, b: 82001, c: 83001 };
    const gap = { a: 81002, b: 82002, c: 83002 };
    const live = { a: 81003, b: 82003, c: 83003 };

    try {
      const evA = [], evB = [], evC = [];
      const sqlA = `SELECT id, value FROM ${tblA}`;
      const sqlB = `SELECT id, value FROM ${tblB}`;
      const sqlC = `SELECT id, value FROM ${tblC}`;

      const unsubA = await client.subscribeWithSql(sqlA, (ev) => evA.push(ev), { last_rows: 0 });
      const unsubB = await client.subscribeWithSql(sqlB, (ev) => evB.push(ev), { last_rows: 0 });
      const unsubC = await client.subscribeWithSql(sqlC, (ev) => evC.push(ev), { last_rows: 0 });

      await waitFor(() =>
        evA.some((e) => e.type === 'subscription_ack') &&
        evB.some((e) => e.type === 'subscription_ack') &&
        evC.some((e) => e.type === 'subscription_ack'),
      );

      // Insert pre rows
      await writer.query(`INSERT INTO ${tblA} (id, value) VALUES (${pre.a}, 'pre-a')`);
      await writer.query(`INSERT INTO ${tblB} (id, value) VALUES (${pre.b}, 'pre-b')`);
      await writer.query(`INSERT INTO ${tblC} (id, value) VALUES (${pre.c}, 'pre-c')`);

      await waitFor(() =>
        hasRowId(evA, pre.a) && hasRowId(evB, pre.b) && hasRowId(evC, pre.c),
      );

      // Capture checkpoints
      await waitFor(() => {
        const subs = client.getSubscriptions();
        return (
          subs.find((s) => s.tableName === sqlA)?.lastSeqId &&
          subs.find((s) => s.tableName === sqlB)?.lastSeqId &&
          subs.find((s) => s.tableName === sqlC)?.lastSeqId
        );
      });
      const subs = client.getSubscriptions();
      const cpA = subs.find((s) => s.tableName === sqlA)?.lastSeqId;
      const cpB = subs.find((s) => s.tableName === sqlB)?.lastSeqId;
      const cpC = subs.find((s) => s.tableName === sqlC)?.lastSeqId;

      await unsubA();
      await unsubB();
      await unsubC();

      // Disconnect
      await client.disconnect();

      // Insert gap rows
      await writer.query(`INSERT INTO ${tblA} (id, value) VALUES (${gap.a}, 'gap-a')`);
      await writer.query(`INSERT INTO ${tblB} (id, value) VALUES (${gap.b}, 'gap-b')`);
      await writer.query(`INSERT INTO ${tblC} (id, value) VALUES (${gap.c}, 'gap-c')`);

      // Reconnect from checkpoints
      const rEvA = [], rEvB = [], rEvC = [];
      const rUnsubA = await client.subscribeWithSql(sqlA, (ev) => rEvA.push(ev), { from: cpA, last_rows: 0 });
      const rUnsubB = await client.subscribeWithSql(sqlB, (ev) => rEvB.push(ev), { from: cpB, last_rows: 0 });
      const rUnsubC = await client.subscribeWithSql(sqlC, (ev) => rEvC.push(ev), { from: cpC, last_rows: 0 });

      await waitFor(() =>
        rEvA.some((e) => e.type === 'subscription_ack') &&
        rEvB.some((e) => e.type === 'subscription_ack') &&
        rEvC.some((e) => e.type === 'subscription_ack'),
      );

      // Insert live rows
      await writer.query(`INSERT INTO ${tblA} (id, value) VALUES (${live.a}, 'live-a')`);
      await writer.query(`INSERT INTO ${tblB} (id, value) VALUES (${live.b}, 'live-b')`);
      await writer.query(`INSERT INTO ${tblC} (id, value) VALUES (${live.c}, 'live-c')`);

      // Wait for gap + live rows on all three
      await waitFor(() =>
        hasRowId(rEvA, gap.a) && hasRowId(rEvA, live.a) &&
        hasRowId(rEvB, gap.b) && hasRowId(rEvB, live.b) &&
        hasRowId(rEvC, gap.c) && hasRowId(rEvC, live.c),
      );

      // No replay of pre rows
      assert.ok(!hasRowId(rEvA, pre.a), 'A must NOT replay pre row');
      assert.ok(!hasRowId(rEvB, pre.b), 'B must NOT replay pre row');
      assert.ok(!hasRowId(rEvC, pre.c), 'C must NOT replay pre row');

      assertRowsStrictlyAfter(rEvA, cpA, 'resume3 A');
      assertRowsStrictlyAfter(rEvB, cpB, 'resume3 B');
      assertRowsStrictlyAfter(rEvC, cpC, 'resume3 C');
      assertNoDuplicateSeqRows(rEvA, 'resume3 A');
      assertNoDuplicateSeqRows(rEvB, 'resume3 B');
      assertNoDuplicateSeqRows(rEvC, 'resume3 C');

      await rUnsubA();
      await rUnsubB();
      await rUnsubC();
    } finally {
      await dropTable(client, tblA);
      await dropTable(client, tblB);
      await dropTable(client, tblC);
      await shutdownClient(writer);
    }
  });

  // Double disconnect: disconnect, briefly reconnect, disconnect again, then recover
  test('double disconnect recovers and resumes from checkpoint', async () => {
    const tbl = `${ns}.resume_double`;
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${tbl} (id INT PRIMARY KEY, value TEXT)`,
    );

    const writer = await connectJwtClient();
    const preId = 71001;
    const gapId = 71002;
    const liveId = 71003;
    const sql = `SELECT id, value FROM ${tbl}`;

    try {
      const preEvents = [];
      const unsub = await client.subscribeWithSql(sql, (ev) => preEvents.push(ev), { last_rows: 0 });
      await waitFor(() => preEvents.some((e) => e.type === 'subscription_ack'));

      await writer.query(`INSERT INTO ${tbl} (id, value) VALUES (${preId}, 'pre')`);
      await waitFor(() => hasRowId(preEvents, preId));

      await waitFor(() => {
        const sub = client.getSubscriptions().find((s) => s.tableName === sql);
        return !!sub?.lastSeqId;
      });
      const checkpoint = client.getSubscriptions().find((s) => s.tableName === sql)?.lastSeqId;

      await unsub();

      // First disconnect
      await client.disconnect();

      // Brief reconnect then immediate second disconnect
      const tempEvents = [];
      const tempUnsub = await client.subscribeWithSql(sql, (ev) => tempEvents.push(ev));
      await sleep(300);
      await tempUnsub();
      await client.disconnect();

      // Insert gap row while disconnected
      await writer.query(`INSERT INTO ${tbl} (id, value) VALUES (${gapId}, 'gap')`);

      // Final recovery from checkpoint
      const resumedEvents = [];
      const unsub2 = await client.subscribeWithSql(sql, (ev) => resumedEvents.push(ev), {
        from: checkpoint,
        last_rows: 0,
      });
      await waitFor(() => resumedEvents.some((e) => e.type === 'subscription_ack'));

      await writer.query(`INSERT INTO ${tbl} (id, value) VALUES (${liveId}, 'live')`);

      await waitFor(() => hasRowId(resumedEvents, gapId) && hasRowId(resumedEvents, liveId));

      assert.ok(!hasRowId(resumedEvents, preId), 'pre row must NOT be replayed');
      assertRowsStrictlyAfter(resumedEvents, checkpoint, 'double-disconnect');
      assertNoDuplicateSeqRows(resumedEvents, 'double-disconnect');

      await unsub2();
    } finally {
      await dropTable(client, tbl);
      await shutdownClient(writer);
    }
  });
});
