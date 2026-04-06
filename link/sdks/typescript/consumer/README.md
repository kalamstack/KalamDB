# @kalamdb/consumer

Topic consumer and agent runtime package for KalamDB.

Use `@kalamdb/client` for app-facing SQL, live rows, subscriptions, and file uploads. Add `@kalamdb/consumer` only when you need topic polling, acknowledgments, or the high-level worker runtime.

`@kalamdb/consumer` ships its own worker-focused WASM bundle and layers it on top of `@kalamdb/client`, so app-only installs can keep using the lighter main client package alone.

> Status: **Beta**.

## Installation

```bash
npm i @kalamdb/client @kalamdb/consumer
```

## What This Package Owns

- `consumeBatch()` for one-shot topic polling
- `ack()` for explicit offset commits
- `consumer().run()` for continuous polling loops
- `runAgent()` and `runConsumer()` for higher-level worker orchestration

Topic HTTP endpoints require bearer authentication and role `service`, `dba`, or `system`.

## Quick Start

```ts
import { Auth } from '@kalamdb/client';
import { createConsumerClient, runAgent } from '@kalamdb/consumer';

const client = createConsumerClient({
  url: 'http://localhost:8080',
  authProvider: async () => Auth.basic('support-worker', 'Secret123!'),
});

await runAgent({
  client,
  name: 'support-summary-agent',
  topic: 'support.inbox_events',
  groupId: 'support-summary-agent',
  retry: {
    maxAttempts: 3,
    initialBackoffMs: 250,
    maxBackoffMs: 2_000,
  },
  onRow: async (ctx, row) => {
    const username = String(ctx.username ?? '').trim();
    const body = String(row.body ?? '').trim();
    if (!username || !body) {
      return;
    }

    const summary = `Support summary: ${body.slice(0, 120)}`;
    await client.executeAsUser(
      'INSERT INTO support.inbox (room, role, body) VALUES ($1, $2, $3)',
      username,
      ['main', 'assistant', summary],
    );
  },
});
```

## Lower-Level Consumer

```ts
import { Auth } from '@kalamdb/client';
import { createConsumerClient } from '@kalamdb/consumer';

const client = createConsumerClient({
  url: 'http://localhost:8080',
  authProvider: async () => Auth.jwt(await getWorkerToken()),
});

const handle = client.consumer({
  topic: 'support.inbox_events',
  group_id: 'support-worker',
  auto_ack: true,
  batch_size: 10,
});

await handle.run(async (ctx) => {
  console.log(ctx.message.topic, ctx.message.offset, ctx.message.value);
});
```

## One-Shot Polling

```ts
const batch = await client.consumeBatch({
  topic: 'support.inbox_events',
  group_id: 'support-worker',
  start: 'earliest',
  batch_size: 25,
});

for (const message of batch.messages) {
  console.log(message.offset, message.value);
}

if (batch.messages.length > 0) {
  const last = batch.messages[batch.messages.length - 1];
  await client.ack(last.topic, last.group_id, last.partition_id, last.offset);
}
```

## Notes

- `Auth.basic(...)` is exchanged to JWT automatically before topic requests.
- Topic payloads are decoded from the HTTP API's base64 payload field.
- When you only need browser/app features, install `@kalamdb/client` alone.
- Low-level worker bindings are also available at `@kalamdb/consumer/wasm`.