/**
 * Subscription e2e tests — subscribe, change events, unsubscribe.
 *
 * Run: node --test tests/e2e/subscription/subscription.test.mjs
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

async function waitFor(predicate, timeoutMs = 20_000, intervalMs = 200) {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) {
      throw new Error('Timed out waiting for condition');
    }
    await sleep(intervalMs);
  }
}

function insertedIds(events) {
  const ids = new Set();
  for (const event of events) {
    if (event.type !== 'change' || !Array.isArray(event.rows)) continue;
    for (const row of event.rows) {
      const id = typeof row?.id?.asInt === 'function' ? row.id.asInt() : row?.id;
      if (id !== undefined && id !== null) {
        ids.add(id);
      }
    }
  }
  return ids;
}

async function shutdownClient(client) {
  if (!client) {
    return;
  }

  if (typeof client.shutdown === 'function') {
    await client.shutdown();
    return;
  }

  await client.disconnect();
}

describe('Subscription', { timeout: 150_000 }, () => {
  let client;
  const ns = uniqueName('ts_sub');
  const tbl = `${ns}.messages`;

  before(async () => {
    client = await connectJwtClient();
    await ensureNamespace(client, ns);
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${tbl} (
        id INT PRIMARY KEY,
        body TEXT
      )`,
    );
  });

  after(async () => {
    await client.unsubscribeAll();
    await dropTable(client, tbl);
    await shutdownClient(client);
  });

  // -----------------------------------------------------------------------
  // Basic subscribe / unsubscribe
  // -----------------------------------------------------------------------
  test('subscribe returns unsubscribe function', async () => {
    const events = [];
    const unsub = await client.subscribe(tbl, (event) => {
      events.push(event);
    });
    assert.equal(typeof unsub, 'function');
    assert.ok(client.getSubscriptionCount() >= 1);

    await unsub();
  });

  // -----------------------------------------------------------------------
  // Receives subscription_ack
  // -----------------------------------------------------------------------
  test('subscribe receives subscription_ack event', async () => {
    const events = [];
    const unsub = await client.subscribe(tbl, (event) => {
      events.push(event);
    });

    // Wait for ack event
    await sleep(1500);

    const ackEvent = events.find((e) => e.type === 'subscription_ack');
    assert.ok(ackEvent, 'should receive subscription_ack');
    assert.ok(ackEvent.subscription_id, 'ack should have subscription_id');

    await unsub();
  });

  // -----------------------------------------------------------------------
  // Insert triggers change event
  // -----------------------------------------------------------------------
  test('insert triggers change event on subscriber', async () => {
    const events = [];
    const unsub = await client.subscribe(tbl, (event) => {
      events.push(event);
    });

    // Wait for initial ack
    await sleep(1500);

    // Insert from a second client
    const writer = await connectJwtClient();
    await writer.query(
      `INSERT INTO ${tbl} (id, body) VALUES (500, 'hello from writer')`,
    );

    // Wait for change event
    await sleep(3000);

    const changeEvents = events.filter((e) => e.type === 'change');
    assert.ok(changeEvents.length >= 1, 'should receive at least one change event');

    const rows = changeEvents[0].rows;
    assert.ok(rows, 'change event should have rows');
    assert.ok(
      rows.some((r) => (typeof r.id?.asInt === 'function' ? r.id.asInt() : r.id) === 500),
      'should see inserted row id',
    );

    await unsub();
    await shutdownClient(writer);
  });

  // -----------------------------------------------------------------------
  // subscribeWithSql
  // -----------------------------------------------------------------------
  test('subscribeWithSql with WHERE clause works', async () => {
    const events = [];
    const unsub = await client.subscribeWithSql(
      `SELECT * FROM ${tbl} WHERE id = 600`,
      (event) => events.push(event),
    );

    await sleep(1500);

    const writer = await connectJwtClient();
    await writer.query(
      `INSERT INTO ${tbl} (id, body) VALUES (600, 'targeted')`,
    );

    await sleep(3000);

    const ack = events.find((e) => e.type === 'subscription_ack');
    assert.ok(ack, 'should get ack for sql subscription');

    await unsub();
    await shutdownClient(writer);
  });

  // -----------------------------------------------------------------------
  // Subscription tracking
  // -----------------------------------------------------------------------
  test('getSubscriptions / isSubscribedTo track subscriptions', async () => {
    const unsub = await client.subscribe(tbl, () => {});
    // Wait for subscription ack to register
    await sleep(1500);

    assert.ok(client.getSubscriptionCount() >= 1, 'should have at least 1 subscription');

    const subs = client.getSubscriptions();
    assert.ok(subs.length >= 1, 'getSubscriptions should return subscriptions');
    assert.ok(subs[0].id, 'subscription should have id');

    await unsub();
  });

  // -----------------------------------------------------------------------
  // unsubscribeAll
  // -----------------------------------------------------------------------
  test('unsubscribeAll clears all subscriptions', async () => {
    // Subscribe twice with different queries to ensure separate subscriptions
    await client.subscribe(tbl, () => {});
    await sleep(500);
    await client.subscribeWithSql(`SELECT * FROM ${tbl} WHERE id > 0`, () => {});
    // Wait for both subscriptions to register
    await sleep(1500);

    const count = client.getSubscriptionCount();
    assert.ok(count >= 1, `should have at least 1 subscription, got ${count}`);

    await client.unsubscribeAll();
    assert.equal(client.getSubscriptionCount(), 0);
  });

  test('concurrent writers fan out inserts to every subscriber client', async () => {
    const subscriberClients = await Promise.all(Array.from({ length: 3 }, () => connectJwtClient()));
    const writerClients = await Promise.all(Array.from({ length: 2 }, () => connectJwtClient()));
    const eventBatches = subscriberClients.map(() => []);
    const baseId = (Date.now() % 1_000_000) * 100;
    const ids = Array.from({ length: 8 }, (_, index) => baseId + index + 1);
    const unsubs = [];

    try {
      for (const [index, subscriber] of subscriberClients.entries()) {
        const unsub = await subscriber.subscribeWithSql(
          `SELECT id, body FROM ${tbl} WHERE id >= ${baseId}`,
          (event) => eventBatches[index].push(event),
          { last_rows: 0 },
        );
        unsubs.push(unsub);
      }

      await waitFor(() =>
        eventBatches.every((events) => events.some((event) => event.type === 'subscription_ack')),
      );

      await Promise.all(
        ids.map((id, index) =>
          writerClients[index % writerClients.length].query(
            `INSERT INTO ${tbl} (id, body) VALUES (${id}, 'fanout-${index}')`,
          )),
      );

      await waitFor(
        () => eventBatches.every((events) => ids.every((id) => insertedIds(events).has(id))),
        30_000,
      );

      for (const events of eventBatches) {
        for (const id of ids) {
          assert.ok(insertedIds(events).has(id), `subscriber missed id ${id}`);
        }
      }
    } finally {
      for (const unsub of unsubs.reverse()) {
        await unsub();
      }
      await Promise.all(writerClients.map((writer) => shutdownClient(writer)));
      await Promise.all(subscriberClients.map((subscriber) => shutdownClient(subscriber)));
    }
  });

  test('one client keeps many simultaneous subscriptions isolated', async () => {
    const writer = await connectJwtClient();
    const baseId = (Date.now() % 1_000_000) * 100 + 500;
    const ids = Array.from({ length: 6 }, (_, index) => baseId + index + 1);
    const eventBatches = new Map(ids.map((id) => [id, []]));
    const unsubs = [];

    try {
      for (const id of ids) {
        const unsub = await client.subscribeWithSql(
          `SELECT id, body FROM ${tbl} WHERE id = ${id}`,
          (event) => eventBatches.get(id).push(event),
          { last_rows: 0 },
        );
        unsubs.push(unsub);
      }

      await waitFor(() =>
        ids.every((id) => eventBatches.get(id).some((event) => event.type === 'subscription_ack')),
      );

      const queries = client.getSubscriptions().map((sub) => sub.tableName);
      for (const id of ids) {
        assert.ok(
          queries.some((query) => query.includes(`id = ${id}`)),
          `expected active subscription for id ${id}`,
        );
      }

      await Promise.all(
        ids.map((id) => writer.query(`INSERT INTO ${tbl} (id, body) VALUES (${id}, 'isolated-${id}')`)),
      );

      await waitFor(
        () => ids.every((id) => insertedIds(eventBatches.get(id)).has(id)),
        30_000,
      );

      for (const id of ids) {
        assert.deepEqual([...insertedIds(eventBatches.get(id))], [id]);
      }
    } finally {
      for (const unsub of unsubs.reverse()) {
        await unsub();
      }
      await shutdownClient(writer);
    }
  });
});
