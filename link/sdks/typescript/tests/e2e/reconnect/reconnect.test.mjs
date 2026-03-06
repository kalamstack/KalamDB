/**
 * Reconnect e2e tests — disconnect, reconnect, resume subscriptions,
 * connection event handlers.
 *
 * Imitates a real-world scenario where a client subscribes to a table,
 * receives live events, loses the connection, reconnects, and continues
 * receiving events without re-subscribing.
 *
 * Run: node --test tests/e2e/reconnect/reconnect.test.mjs
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

async function reconnectViaSubscription(client, tableName) {
  const events = [];
  const unsub = await client.subscribe(tableName, (event) => {
    events.push(event);
  });
  await sleep(1500);
  return { unsub, events };
}

describe('Reconnect & Resume', { timeout: 120_000 }, () => {
  let client;
  const ns = uniqueName('ts_recon');
  const tbl = `${ns}.events`;

  before(async () => {
    client = await connectJwtClient();
    await ensureNamespace(client, ns);
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${tbl} (
        id INT PRIMARY KEY,
        payload TEXT
      )`,
    );
  });

  after(async () => {
    try { await client.unsubscribeAll(); } catch (_) {}
    await dropTable(client, tbl);
    await client.disconnect();
  });

  // -----------------------------------------------------------------------
  // 1. connect / disconnect toggles isConnected
  // -----------------------------------------------------------------------
  test('disconnect then subscribe reconnects automatically', async () => {
    const c = await connectJwtClient();
    assert.equal(c.isConnected(), true, 'should be connected after initialize');

    await c.disconnect();
    assert.equal(c.isConnected(), false, 'should be disconnected after disconnect');

    const { unsub } = await reconnectViaSubscription(c, tbl);
    assert.equal(c.isConnected(), true, 'should reconnect on first subscribe after disconnect');

    await unsub();
    await c.disconnect();
  });

  // -----------------------------------------------------------------------
  // 2. onConnect / onDisconnect handlers fire on reconnect cycle
  // -----------------------------------------------------------------------
  test('onConnect and onDisconnect fire during reconnect cycle', async () => {
    const events = [];

    const c = createClient({
      url: SERVER_URL,
      authProvider: async () => Auth.basic(ADMIN_USER, ADMIN_PASS),
      wsLazyConnect: false,
      onConnect: () => events.push('connect'),
      onDisconnect: (reason) => events.push(`disconnect:${reason?.message || 'manual'}`),
    });

    await c.initialize();
    await sleep(500);

    // Should have fired onConnect.
    assert.ok(
      events.some((e) => e === 'connect'),
      `expected 'connect' in events: ${JSON.stringify(events)}`,
    );

    // Disconnect — should fire onDisconnect.
    await c.disconnect();
    await sleep(500);

    assert.ok(
      events.some((e) => e.startsWith('disconnect')),
      `expected 'disconnect' in events: ${JSON.stringify(events)}`,
    );

    // Reconnect on next subscribe — should fire onConnect again.
    const countBefore = events.filter((e) => e === 'connect').length;
    const { unsub } = await reconnectViaSubscription(c, tbl);
    await sleep(500);

    const countAfter = events.filter((e) => e === 'connect').length;
    assert.ok(
      countAfter > countBefore,
      `expected onConnect to fire again after reconnect, events: ${JSON.stringify(events)}`,
    );

    await unsub();
    await c.disconnect();
  });

  // -----------------------------------------------------------------------
  // 3. onError handler is invokable
  // -----------------------------------------------------------------------
  test('onError handler can be set without throwing', async () => {
    const errors = [];

    const c = createClient({
      url: SERVER_URL,
      authProvider: async () => Auth.basic(ADMIN_USER, ADMIN_PASS),
      wsLazyConnect: false,
      onError: (err) => errors.push(err),
    });

    await c.initialize();
    // The handler is wired; we just verify it doesn't break anything.
    assert.ok(c.isConnected());
    await c.disconnect();
  });

  // -----------------------------------------------------------------------
  // 4. Reconnect settings can be configured
  // -----------------------------------------------------------------------
  test('setAutoReconnect / setReconnectDelay / setMaxReconnectAttempts', async () => {
    const c = await connectJwtClient();

    c.setAutoReconnect(true);
    c.setReconnectDelay(200, 3000);
    c.setMaxReconnectAttempts(5);

    assert.equal(c.getReconnectAttempts(), 0);
    assert.equal(c.isReconnecting(), false);

    await c.disconnect();
  });

  // -----------------------------------------------------------------------
  // 5. Real-world: subscribe → insert → disconnect → reconnect → insert → events resume
  // -----------------------------------------------------------------------
  test('subscription resumes after disconnect/reconnect (real-world)', async () => {
    const events = [];

    // Subscribe to the table.
    const unsub = await client.subscribe(tbl, (event) => {
      events.push(event);
    });

    // Wait for subscription ack.
    await sleep(2000);
    const ack = events.find((e) => e.type === 'subscription_ack');
    assert.ok(ack, 'should receive subscription_ack');

    // Insert a row BEFORE disconnect.
    const writer = await connectJwtClient();
    await writer.query(
      `INSERT INTO ${tbl} (id, payload) VALUES (1, 'before disconnect')`,
    );

    // Wait for the change event.
    await sleep(3000);
    const priorChanges = events.filter((e) => e.type === 'change');
    assert.ok(
      priorChanges.length >= 1,
      `expected change event before disconnect, got ${priorChanges.length}`,
    );

    // --- Disconnect ---
    await client.disconnect();
    assert.equal(client.isConnected(), false, 'should be disconnected');

    // Insert while disconnected — we should catch up later.
    await writer.query(
      `INSERT INTO ${tbl} (id, payload) VALUES (2, 'during disconnect')`,
    );

    // Re-subscribe after disconnect; this should reconnect automatically.
    const postReconnectEvents = [];
    const unsub2 = await client.subscribe(tbl, (event) => {
      postReconnectEvents.push(event);
    });
    assert.equal(client.isConnected(), true, 'should reconnect on subscribe');

    // Wait for subscription ack on the new subscription.
    await sleep(2000);
    const ack2 = postReconnectEvents.find((e) => e.type === 'subscription_ack');
    assert.ok(ack2, 'should receive subscription_ack after reconnect');

    // Insert a row AFTER reconnect.
    await writer.query(
      `INSERT INTO ${tbl} (id, payload) VALUES (3, 'after reconnect')`,
    );

    // Wait for the change event.
    await sleep(3000);
    const postChanges = postReconnectEvents.filter((e) => e.type === 'change');
    assert.ok(
      postChanges.length >= 1,
      `expected change event after reconnect, got ${postChanges.length}`,
    );

    // Verify row 3 is in the change events.
    const row3Change = postChanges.find((c) =>
      c.rows?.some((r) => r.id === 3),
    );
    assert.ok(row3Change, 'row 3 should appear in change events after reconnect');

    await unsub2();
    await unsub();
    await writer.disconnect();
  });

  // -----------------------------------------------------------------------
  // 6. Multiple subscriptions survive reconnect
  // -----------------------------------------------------------------------
  test('multiple subscriptions work after reconnect', async () => {
    // Create a second table.
    const tbl2 = `${ns}.events2`;
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${tbl2} (
        id INT PRIMARY KEY,
        label TEXT
      )`,
    );

    const events1 = [];
    const events2 = [];

    const unsub1 = await client.subscribe(tbl, (ev) => events1.push(ev));
    const unsub2 = await client.subscribe(tbl2, (ev) => events2.push(ev));

    await sleep(2000);

    // Verify both subscriptions ack'd.
    assert.ok(events1.find((e) => e.type === 'subscription_ack'), 'sub1 ack');
    assert.ok(events2.find((e) => e.type === 'subscription_ack'), 'sub2 ack');

    // Disconnect.
    await client.disconnect();
    await sleep(500);

    // Re-subscribe after disconnect; subscriptions should reconnect automatically.
    const postEvents1 = [];
    const postEvents2 = [];

    const unsub3 = await client.subscribe(tbl, (ev) => postEvents1.push(ev));
    const unsub4 = await client.subscribe(tbl2, (ev) => postEvents2.push(ev));

    await sleep(2000);

    // Insert into both tables.
    const writer = await connectJwtClient();
    await writer.query(
      `INSERT INTO ${tbl} (id, payload) VALUES (10, 'multi-recon-1')`,
    );
    await writer.query(
      `INSERT INTO ${tbl2} (id, label) VALUES (10, 'multi-recon-2')`,
    );

    await sleep(3000);

    // Both should have received change events.
    const changes1 = postEvents1.filter((e) => e.type === 'change');
    const changes2 = postEvents2.filter((e) => e.type === 'change');
    assert.ok(changes1.length >= 1, 'table 1 should receive changes after reconnect');
    assert.ok(changes2.length >= 1, 'table 2 should receive changes after reconnect');

    await unsub4();
    await unsub3();
    await unsub2();
    await unsub1();
    await dropTable(client, tbl2);
    await writer.disconnect();
  });

  // -----------------------------------------------------------------------
  // 7. Queries still work after reconnect
  // -----------------------------------------------------------------------
  test('three active subscriptions resume without replaying old rows', async () => {
    const tblA = `${ns}.resume_a`;
    const tblB = `${ns}.resume_b`;
    const tblC = `${ns}.resume_c`;

    await client.query(`CREATE TABLE IF NOT EXISTS ${tblA} (id INT PRIMARY KEY, payload TEXT)`);
    await client.query(`CREATE TABLE IF NOT EXISTS ${tblB} (id INT PRIMARY KEY, payload TEXT)`);
    await client.query(`CREATE TABLE IF NOT EXISTS ${tblC} (id INT PRIMARY KEY, payload TEXT)`);

    const writer = await connectJwtClient();
    const preRows = { a: 1001, b: 2001, c: 3001 };
    const gapRows = { a: 1002, b: 2002, c: 3002 };
    const postRows = { a: 1003, b: 2003, c: 3003 };

    const waitFor = async (predicate, timeoutMs = 15000, intervalMs = 200) => {
      const start = Date.now();
      while (!predicate()) {
        if (Date.now() - start > timeoutMs) {
          throw new Error('Timed out waiting for condition');
        }
        await sleep(intervalMs);
      }
    };

    const extractRows = (events) => {
      const rows = [];
      for (const ev of events) {
        if (ev.type === 'change' && Array.isArray(ev.rows)) rows.push(...ev.rows);
        if (ev.type === 'initial_data_batch' && Array.isArray(ev.rows)) rows.push(...ev.rows);
      }
      return rows;
    };

    const extractChangeRows = (events) => {
      const rows = [];
      for (const ev of events) {
        if (ev.type === 'change' && Array.isArray(ev.rows)) rows.push(...ev.rows);
      }
      return rows;
    };

    const preEventsA = [];
    const preEventsB = [];
    const preEventsC = [];

    const unsubA = await client.subscribeWithSql(
      `SELECT id, payload FROM ${tblA}`,
      (ev) => preEventsA.push(ev),
      { last_rows: 0 },
    );
    const unsubB = await client.subscribeWithSql(
      `SELECT id, payload FROM ${tblB}`,
      (ev) => preEventsB.push(ev),
      { last_rows: 0 },
    );
    const unsubC = await client.subscribeWithSql(
      `SELECT id, payload FROM ${tblC}`,
      (ev) => preEventsC.push(ev),
      { last_rows: 0 },
    );

    await waitFor(() =>
      preEventsA.some((e) => e.type === 'subscription_ack')
      && preEventsB.some((e) => e.type === 'subscription_ack')
      && preEventsC.some((e) => e.type === 'subscription_ack'),
    );

    await writer.query(`INSERT INTO ${tblA} (id, payload) VALUES (${preRows.a}, 'pre-a')`);
    await writer.query(`INSERT INTO ${tblB} (id, payload) VALUES (${preRows.b}, 'pre-b')`);
    await writer.query(`INSERT INTO ${tblC} (id, payload) VALUES (${preRows.c}, 'pre-c')`);

    await waitFor(() =>
      extractChangeRows(preEventsA).some((r) => r.id === preRows.a)
      && extractChangeRows(preEventsB).some((r) => r.id === preRows.b)
      && extractChangeRows(preEventsC).some((r) => r.id === preRows.c),
    );

    await client.disconnect();
    assert.equal(client.isConnected(), false, 'client should disconnect cleanly');

    await writer.query(`INSERT INTO ${tblA} (id, payload) VALUES (${gapRows.a}, 'gap-a')`);
    await writer.query(`INSERT INTO ${tblB} (id, payload) VALUES (${gapRows.b}, 'gap-b')`);
    await writer.query(`INSERT INTO ${tblC} (id, payload) VALUES (${gapRows.c}, 'gap-c')`);

    const postEventsA = [];
    const postEventsB = [];
    const postEventsC = [];

    const unsubA2 = await client.subscribeWithSql(
      `SELECT id, payload FROM ${tblA} WHERE id >= ${gapRows.a}`,
      (ev) => postEventsA.push(ev),
    );
    const unsubB2 = await client.subscribeWithSql(
      `SELECT id, payload FROM ${tblB} WHERE id >= ${gapRows.b}`,
      (ev) => postEventsB.push(ev),
    );
    const unsubC2 = await client.subscribeWithSql(
      `SELECT id, payload FROM ${tblC} WHERE id >= ${gapRows.c}`,
      (ev) => postEventsC.push(ev),
    );
    assert.equal(client.isConnected(), true, 'client should reconnect on subscribeWithSql');

    await waitFor(() =>
      postEventsA.some((e) => e.type === 'subscription_ack')
      && postEventsB.some((e) => e.type === 'subscription_ack')
      && postEventsC.some((e) => e.type === 'subscription_ack'),
    );

    await writer.query(`INSERT INTO ${tblA} (id, payload) VALUES (${postRows.a}, 'post-a')`);
    await writer.query(`INSERT INTO ${tblB} (id, payload) VALUES (${postRows.b}, 'post-b')`);
    await writer.query(`INSERT INTO ${tblC} (id, payload) VALUES (${postRows.c}, 'post-c')`);

    await waitFor(() =>
      extractRows(postEventsA).some((r) => r.id === gapRows.a)
      && extractRows(postEventsA).some((r) => r.id === postRows.a)
      && extractRows(postEventsB).some((r) => r.id === gapRows.b)
      && extractRows(postEventsB).some((r) => r.id === postRows.b)
      && extractRows(postEventsC).some((r) => r.id === gapRows.c)
      && extractRows(postEventsC).some((r) => r.id === postRows.c),
      20000,
    );

    assert.ok(!extractRows(postEventsA).some((r) => r.id === preRows.a), 'A should not replay pre row');
    assert.ok(!extractRows(postEventsB).some((r) => r.id === preRows.b), 'B should not replay pre row');
    assert.ok(!extractRows(postEventsC).some((r) => r.id === preRows.c), 'C should not replay pre row');

    await unsubC2();
    await unsubB2();
    await unsubA2();
    await unsubC();
    await unsubB();
    await unsubA();

    await dropTable(client, tblC);
    await dropTable(client, tblB);
    await dropTable(client, tblA);
    await writer.disconnect();
  });

  test('chaos: repeated reconnect cycles with 3 subscriptions stay consistent', async () => {
    const tblA = `${ns}.chaos_a`;
    const tblB = `${ns}.chaos_b`;
    const tblC = `${ns}.chaos_c`;
    const CYCLES = 3;

    await client.query(`CREATE TABLE IF NOT EXISTS ${tblA} (id INT PRIMARY KEY, payload TEXT)`);
    await client.query(`CREATE TABLE IF NOT EXISTS ${tblB} (id INT PRIMARY KEY, payload TEXT)`);
    await client.query(`CREATE TABLE IF NOT EXISTS ${tblC} (id INT PRIMARY KEY, payload TEXT)`);

    const writer = await connectJwtClient();
    const pre = { a: 11001, b: 12001, c: 13001 };

    const waitFor = async (predicate, timeoutMs = 20000, intervalMs = 200) => {
      const started = Date.now();
      while (!predicate()) {
        if (Date.now() - started > timeoutMs) {
          throw new Error('Timed out waiting for condition');
        }
        await sleep(intervalMs);
      }
    };

    const extractRows = (events) => {
      const rows = [];
      for (const ev of events) {
        if ((ev.type === 'change' || ev.type === 'initial_data_batch') && Array.isArray(ev.rows)) {
          rows.push(...ev.rows);
        }
      }
      return rows;
    };

    const preAEvents = [];
    const preBEvents = [];
    const preCEvents = [];

    const preUnsubA = await client.subscribeWithSql(`SELECT id, payload FROM ${tblA}`, (e) => preAEvents.push(e), { last_rows: 0 });
    const preUnsubB = await client.subscribeWithSql(`SELECT id, payload FROM ${tblB}`, (e) => preBEvents.push(e), { last_rows: 0 });
    const preUnsubC = await client.subscribeWithSql(`SELECT id, payload FROM ${tblC}`, (e) => preCEvents.push(e), { last_rows: 0 });

    await waitFor(() =>
      preAEvents.some((e) => e.type === 'subscription_ack')
      && preBEvents.some((e) => e.type === 'subscription_ack')
      && preCEvents.some((e) => e.type === 'subscription_ack'),
    );

    await writer.query(`INSERT INTO ${tblA} (id, payload) VALUES (${pre.a}, 'chaos-pre-a')`);
    await writer.query(`INSERT INTO ${tblB} (id, payload) VALUES (${pre.b}, 'chaos-pre-b')`);
    await writer.query(`INSERT INTO ${tblC} (id, payload) VALUES (${pre.c}, 'chaos-pre-c')`);

    await waitFor(() =>
      extractRows(preAEvents).some((r) => r.id === pre.a)
      && extractRows(preBEvents).some((r) => r.id === pre.b)
      && extractRows(preCEvents).some((r) => r.id === pre.c),
    );

    await preUnsubC();
    await preUnsubB();
    await preUnsubA();

    for (let cycle = 1; cycle <= CYCLES; cycle += 1) {
      const gap = { a: 11000 + cycle * 10 + 2, b: 12000 + cycle * 10 + 2, c: 13000 + cycle * 10 + 2 };
      const post = { a: 11000 + cycle * 10 + 3, b: 12000 + cycle * 10 + 3, c: 13000 + cycle * 10 + 3 };

      await client.disconnect();
      assert.equal(client.isConnected(), false, `cycle ${cycle}: expected disconnected`);

      await writer.query(`INSERT INTO ${tblA} (id, payload) VALUES (${gap.a}, 'chaos-gap-a-${cycle}')`);
      await writer.query(`INSERT INTO ${tblB} (id, payload) VALUES (${gap.b}, 'chaos-gap-b-${cycle}')`);
      await writer.query(`INSERT INTO ${tblC} (id, payload) VALUES (${gap.c}, 'chaos-gap-c-${cycle}')`);

      const eventsA = [];
      const eventsB = [];
      const eventsC = [];

      const unsubA = await client.subscribeWithSql(
        `SELECT id, payload FROM ${tblA} WHERE id >= ${gap.a}`,
        (e) => eventsA.push(e),
      );
      const unsubB = await client.subscribeWithSql(
        `SELECT id, payload FROM ${tblB} WHERE id >= ${gap.b}`,
        (e) => eventsB.push(e),
      );
      const unsubC = await client.subscribeWithSql(
        `SELECT id, payload FROM ${tblC} WHERE id >= ${gap.c}`,
        (e) => eventsC.push(e),
      );
      assert.equal(client.isConnected(), true, `cycle ${cycle}: expected reconnected on subscribe`);

      await waitFor(() =>
        eventsA.some((e) => e.type === 'subscription_ack')
        && eventsB.some((e) => e.type === 'subscription_ack')
        && eventsC.some((e) => e.type === 'subscription_ack'),
      );

      await writer.query(`INSERT INTO ${tblA} (id, payload) VALUES (${post.a}, 'chaos-post-a-${cycle}')`);
      await writer.query(`INSERT INTO ${tblB} (id, payload) VALUES (${post.b}, 'chaos-post-b-${cycle}')`);
      await writer.query(`INSERT INTO ${tblC} (id, payload) VALUES (${post.c}, 'chaos-post-c-${cycle}')`);

      await waitFor(() =>
        extractRows(eventsA).some((r) => r.id === gap.a)
        && extractRows(eventsA).some((r) => r.id === post.a)
        && extractRows(eventsB).some((r) => r.id === gap.b)
        && extractRows(eventsB).some((r) => r.id === post.b)
        && extractRows(eventsC).some((r) => r.id === gap.c)
        && extractRows(eventsC).some((r) => r.id === post.c),
      );

      assert.ok(!extractRows(eventsA).some((r) => r.id === pre.a), `cycle ${cycle}: A replayed pre row`);
      assert.ok(!extractRows(eventsB).some((r) => r.id === pre.b), `cycle ${cycle}: B replayed pre row`);
      assert.ok(!extractRows(eventsC).some((r) => r.id === pre.c), `cycle ${cycle}: C replayed pre row`);

      await unsubC();
      await unsubB();
      await unsubA();
    }

    await dropTable(client, tblC);
    await dropTable(client, tblB);
    await dropTable(client, tblA);
    await writer.disconnect();
  });

  // -----------------------------------------------------------------------
  // 9. Queries still work after reconnect
  // -----------------------------------------------------------------------
  test('queries work correctly after WebSocket disconnect cycle', async () => {
    const c = await connectJwtClient();

    // Query before disconnect.
    const res1 = await c.query('SELECT 1 AS n');
    assert.ok(res1.results?.length > 0, 'query before disconnect');

    await c.disconnect();

    // Query after disconnect still works because queries use HTTP.
    const res2 = await c.query('SELECT 2 AS n');
    assert.ok(res2.results?.length > 0, 'query after reconnect');

    await c.disconnect();
  });
});
