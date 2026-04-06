# KalamDB TypeScript SDK Quick Start

Get a working query + realtime flow running with the current `@kalamdb/client` API.

## Prerequisites

- Node.js 18+ or a modern browser runtime
- A running KalamDB server
- Valid credentials for that server

If you are running KalamDB locally from this repo, a common local default is:

- URL: `http://localhost:8080`
- user: `admin`
- password: `kalamdb123`

For server setup and auth flows, see:

- [README.md](README.md)
- [../../../docs/getting-started/authentication.md](../../../docs/getting-started/authentication.md)

## Installation

```bash
npm install @kalamdb/client
```

## 1. Create a Client

The current TypeScript SDK requires `authProvider`.

```typescript
import { Auth, createClient } from '@kalamdb/client';

const client = createClient({
  url: 'http://localhost:8080',
  authProvider: async () => Auth.basic('admin', 'kalamdb123'),
});
```

There is no `auth:` option in the current SDK.

## 2. Execute a Query

HTTP queries work without manually calling `connect()`.

```typescript
const result = await client.query('SELECT CURRENT_USER()');
console.log(result.success, result.rows);
```

There is no public `client.connect()` step in the current TypeScript SDK. The client initializes itself on first use, and realtime connection management is automatic.

## 3. Start Realtime Updates

For UI state, prefer `live()` so the SDK gives you the current materialized row set directly.

```typescript
const stop = await client.live(
  "SELECT * FROM app.messages WHERE room = 'main'",
  (rows) => {
    console.log('rows', rows.length);
  },
  {
    subscriptionOptions: { last_rows: 20 },
  },
);
```

Live SQL should stay in the strict supported form:

- `SELECT ... FROM ... WHERE ...`
- do not put `ORDER BY` or `LIMIT` inside `live()` / `subscribeWithSql()` SQL
- do ordering or capping in application code after rows arrive

## 4. Low-level Subscription API

Use `subscribeWithSql()` only when you need raw subscription protocol events.

```typescript
const stopRaw = await client.subscribeWithSql(
  "SELECT * FROM app.messages WHERE room = 'main'",
  (event) => {
    console.log(event.type);
  },
  { last_rows: 20 },
);

await stopRaw();
```

## 5. Cleanup

```typescript
await stop();
await client.disconnect();
```

## Complete Example

```typescript
import { Auth, createClient } from '@kalamdb/client';

async function main() {
  const client = createClient({
    url: 'http://localhost:8080',
    authProvider: async () => Auth.basic('admin', 'kalamdb123'),
  });

  const me = await client.query('SELECT CURRENT_USER()');
  console.log('current user', me.rows);

  const stop = await client.live(
    "SELECT * FROM app.messages WHERE room = 'main'",
    (rows) => console.log('live rows', rows.length),
    { subscriptionOptions: { last_rows: 10 } },
  );

  try {
    await client.query(
      'INSERT INTO app.messages (room, body) VALUES ($1, $2)',
      ['main', 'hello from quickstart'],
    );
  } finally {
    await stop();
    await client.disconnect();
  }
}

main().catch(console.error);
```

## Next Steps

- Full SDK docs: [README.md](README.md)
- SQL reference: [../../../docs/reference/sql.md](../../../docs/reference/sql.md)
- Workspace examples: [../../../examples](../../../examples)
