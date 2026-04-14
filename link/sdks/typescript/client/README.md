# @kalamdb/client

Official TypeScript / JavaScript SDK for [KalamDB](https://kalamdb.org) — SQL, realtime rows, and strong tenant isolation in one client.

> Status: **Beta** — the API surface is still evolving.

KalamDB is built for apps where every user or tenant owns a private data space. The same SQL can run for every signed-in customer, while USER tables ensure each query only touches that caller's data. On the frontend, the default realtime API is now `live()`: you get the current materialized row set, not a stream of low-level diff frames that your UI has to reconcile.

→ **[kalamdb.org](https://kalamdb.org)** · [Docs](https://kalamdb.org/docs/sdk/typescript) · [GitHub](https://github.com/kalamstack/KalamDB)

`@kalamdb/client` provides:

- SQL execution over HTTP
- materialized live query rows over WebSocket with `live()` and `liveTableRows()`
- low-level realtime events with `subscribe()` / `subscribeWithSql()` when you need raw frames
- per-user and per-tenant isolation with USER tables
- FILE upload/download helpers

Runtime targets:

- Node.js `>= 18`
- modern browsers

## Installation

```bash
npm i @kalamdb/client
```

## Why `live()` First

Most UIs do not want `subscription_ack`, `initial_data_batch`, `change`, and `error` frames. They want the latest rows.

`live()` gives you exactly that:

- the current row set already reconciled for insert, update, and delete
- one callback shape for initial load and future changes
- shared behavior with the Rust and Dart clients
- simpler React, Vue, Svelte, and plain browser code

Use `subscriptionOptions.last_rows` when you want an initial rewind from the
server. Use `limit` when you want the client to keep the materialized live row
set bounded over time.

The knobs apply at different layers:

- `subscriptionOptions.batch_size` chunks the initial snapshot from the server
- `subscriptionOptions.last_rows` chooses how much history to rewind first
- `limit` caps the materialized live row set the client keeps afterward

Use `subscribeWithSql()` only when you need the raw event protocol.

If your query does not expose an `id` column, prefer declarative `keyColumns`
with `live()` so row reconciliation still stays inside the shared Rust core:

```ts
const stop = await client.live(
  "SELECT room_id, message_id, body FROM support.messages WHERE room_id = 'main'",
  (rows) => {
    console.log(rows.length);
  },
  {
    keyColumns: ['room_id', 'message_id'],
  },
);
```

## Quick Start

Start with a USER table. The SQL stays simple, and KalamDB scopes the data per authenticated user.

```sql
CREATE NAMESPACE IF NOT EXISTS support;

CREATE TABLE support.inbox (
  id         BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  room       TEXT NOT NULL DEFAULT 'main',
  role       TEXT NOT NULL,
  body       TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
) WITH (TYPE = 'USER');
```

```ts
import { Auth, createClient } from '@kalamdb/client';

const client = createClient({
  url: 'http://localhost:8080',
  authProvider: async () => Auth.basic('alice', 'Secret123!'),
});

function renderInbox(rows) {
  console.log(
    rows.map((row) => ({
      id: row.id.asString(),
      role: row.role.asString(),
      body: row.body.asString(),
    })),
  );
}

const inboxSql = `
  SELECT id, room, role, body, created_at
  FROM support.inbox
  WHERE room = 'main'
`;

const stop = await client.live(
  inboxSql,
  (rows) => {
    // `support.inbox` is a USER table.
    // Every signed-in user can run the same SQL text, but KalamDB only returns
    // that caller's rows. No app-side WHERE user_id = ? filter is required.
    renderInbox(rows);
  },
  {
    limit: 200,
    subscriptionOptions: { last_rows: 200 },
    onError: (event) => {
      console.error(event.code, event.message);
    },
  },
);

await client.query(
  'INSERT INTO support.inbox (room, role, body) VALUES ($1, $2, $3)',
  ['main', 'user', 'Need help with billing'],
);

// Later:
await stop();
await client.disconnect();
```

## Resume From a Specific `SeqId`

When you want offline resume or a durable checkpoint, persist the last `SeqId` you applied and feed it back into `subscriptionOptions.from`.

```ts
import { Auth, SeqId, createClient } from '@kalamdb/client';

const client = createClient({
  url: 'http://localhost:8080',
  authProvider: async () => Auth.jwt(await getFreshToken()),
});

function renderInbox(rows) {
  console.log(rows.length);
}

const inboxSql = `
  SELECT id, room, role, body, created_at
  FROM support.inbox
  WHERE room = 'main'
`;

// Start from a specific known sequence ID.
// Replace '42' with a previously persisted checkpoint string when resuming.
const startFrom = SeqId.from('42');
let latestCheckpoint;

const stop = await client.live(
  inboxSql,
  (rows) => {
    renderInbox(rows);

    // Persist the last fully applied server sequence so the next session can
    // continue from that exact point.
    const active = client.getSubscriptions().find((sub) => sub.tableName === inboxSql);
    if (active?.lastSeqId) {
      latestCheckpoint = active.lastSeqId.toString();
    }
  },
  {
    limit: 200,
    subscriptionOptions: {
      last_rows: 200,
      ...(startFrom ? { from: startFrom } : {}),
    },
  },
);

console.log('latest checkpoint', latestCheckpoint);
```

This pattern is useful for chat threads, activity feeds, audit logs, and any UI that wants fast reconnect without replaying everything from zero.

## Preserve Tenant Boundaries in Worker Writes

Background services should keep the same user isolation guarantees as the browser. When a worker needs to write on behalf of a user, use `executeAsUser()`.

```ts
await client.executeAsUser(
  'INSERT INTO support.inbox (room, role, body) VALUES ($1, $2, $3)',
  'alice',
  ['main', 'assistant', 'Your billing issue is being reviewed'],
);
```

That keeps the write inside Alice's USER-table partition instead of leaking service-side writes into the wrong tenant scope.

## Lower-Level Realtime API

If you need raw subscription frames, `subscribeWithSql()` still exists.

```ts
import { ChangeType, MessageType } from '@kalamdb/client';

const stop = await client.subscribeWithSql(
  "SELECT * FROM support.inbox WHERE room = 'main'",
  (event) => {
    // Use this path when you need raw subscription protocol events.
    if (event.type !== MessageType.Change) {
      return;
    }

    if (event.change_type === ChangeType.Insert) {
      console.log('new rows', event.rows);
    }
  },
  { batch_size: 200, last_rows: 200 },
);
```

Use this API for protocol tooling, debugging, or custom reconciliation. For app UI state, prefer `live()`.

## Topics and Workers

Topic workers now live in the separate `@kalamdb/consumer` package so app-only installs keep the main SDK lean.

Install the worker package only when you need topic consumption or the agent runtime:

```bash
npm i @kalamdb/client @kalamdb/consumer
```

Use `@kalamdb/client` for app-facing SQL, live rows, subscriptions, auth, and files. Use `@kalamdb/consumer` for `consumeBatch()`, `ack()`, `consumer()`, `runAgent()`, and `runConsumer()`.

## Files

`queryWithFiles()` sends multipart uploads directly to KalamDB while keeping the same auth flow as the rest of the SDK.

```ts
await client.queryWithFiles(
  'INSERT INTO support.attachments (id, file_data) VALUES ($1, FILE("upload"))',
  { upload: selectedFile },
  ['att_1'],
  (progress) => {
    console.log(progress.file_name, progress.percent);
  },
);
```

## Authentication

`authProvider` is the canonical way to configure the client.

```ts
import { Auth, createClient, type AuthProvider } from '@kalamdb/client';

const authProvider: AuthProvider = async () => {
  const token = await myApp.getOrRefreshJwt();
  return Auth.jwt(token);
};

const client = createClient({
  url: 'http://localhost:8080',
  authProvider,
});
```

The SDK handles:

- WASM initialization
- Basic-auth-to-JWT exchange
- lazy or eager WebSocket connection
- reconnect controls and `SeqId` tracking

## Tested Examples

The npm README examples are backed by SDK tests:

- `tests/readme-examples.test.mjs` covers `live()`, resume-from-`SeqId`, `executeAsUser()`, and `queryWithFiles()`.
- `tests/single-socket-subscriptions.test.mjs` covers shared-socket subscriptions and materialized live rows.

## API Pointers

- `query()`, `queryOne()`, `queryAll()`, `queryRows()` for SQL reads
- `insert()`, `update()`, `delete()` for convenience DML
- `live()` and `liveTableRows()` for materialized realtime rows
- `subscribe()` and `subscribeWithSql()` for low-level subscription frames
- `getSubscriptions()` for active subscriptions and typed `lastSeqId` checkpoints

Full docs: [kalamdb.org/docs/sdk/typescript](https://kalamdb.org/docs/sdk/typescript)
- Issues: [github.com/kalamstack/KalamDB/issues](https://github.com/kalamstack/KalamDB/issues)

---

> Browser and Node.js support is powered by a Rust core compiled to WebAssembly (WASM). See [DEV.md](DEV.md) for build and contribution details.
