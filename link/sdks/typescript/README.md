# kalam-link

Official TypeScript / JavaScript SDK for [KalamDB](https://kalamdb.org) — private, realtime storage for AI agents.

> Status: **Alpha** — the API surface is still evolving and may include breaking changes between releases.

KalamDB is a SQL-first realtime database built in Rust. Every user or tenant gets a private namespace. Subscribe to SQL queries live over WebSocket. Publish and consume events via Topics. Store recent data on local disk (RocksDB) and move cold history to object storage (S3, GCS, Azure, or local FS).

→ **[kalamdb.org](https://kalamdb.org)** · [Docs](https://kalamdb.org/docs) · [GitHub](https://github.com/jamals86/KalamDB)

`kalam-link` provides:

- SQL execution over HTTP
- realtime query subscriptions over WebSocket (`SUBSCRIBE TO` any SQL query)
- per-tenant namespaces — no app-side `WHERE user_id = ?` filters needed
- topic consumer APIs (`consumer`, `consumeBatch`, `ack`) for pub/sub event streams
- auth helpers (`Auth.basic`, `Auth.jwt`, `Auth.none`)
- FILE upload/download helpers

Runtime targets:

- Node.js `>= 18`
- modern browsers

## Installation

```bash
npm i kalam-link
```

## Quick Start

```ts
import { createClient, Auth, MessageType, ChangeType } from 'kalam-link';

const client = createClient({
  url: 'http://localhost:8080',
  auth: Auth.basic('admin', 'AdminPass123!'),
});

// No setup needed — login + WebSocket connect happen automatically
// on the first subscribe call.

const result = await client.query('SELECT CURRENT_USER()');
console.log(result.status, result.results?.[0]?.rows);

const unsubscribe = await client.subscribeWithSql(
  'SELECT * FROM chat.messages ORDER BY created_at DESC',
  (event) => {
    if (event.type === MessageType.Change && event.change_type === ChangeType.Insert) {
      console.log(event.rows);
    }
  },
  { batch_size: 100, last_rows: 20 },
);

await unsubscribe();
await client.disconnect();
```

## Topic Produce + Consume

### Produce to a topic

The high-level SDK does not expose `produce(...)` directly. The documented pattern is:

1. create a topic
2. route table changes into that topic
3. produce by writing rows into the source table

```ts
await client.query('CREATE TOPIC "chat.ai-processing"');

await client.query(`
  ALTER TOPIC "chat.ai-processing"
  ADD SOURCE chat.messages
  ON INSERT
  WITH (payload = 'full')
`);

// Produces a topic message via source-table insert
await client.query(
  'INSERT INTO chat.messages (conversation_id, role, content) VALUES ($1, $2, $3)',
  [42, 'user', 'Hello from producer'],
);
```

### Consume from a topic (continuous worker)

```ts
const handle = client.consumer({
  topic: 'chat.ai-processing',
  group_id: 'ai-worker',
  auto_ack: true,
  batch_size: 10,
});

await handle.run(async (ctx) => {
  console.log(ctx.message.topic, ctx.message.offset, ctx.message.value);
});

// later:
handle.stop();
```

Node.js workers do not need manual WASM/WebSocket bootstrap. `createClient(...)`
auto-configures runtime shims and auto-loads bundled WASM bytes, so the
consumer above is enough.

### Agent Runtime (`runAgent`)

`runAgent` is a higher-level consumer runtime with bounded retries and explicit
ack behavior:

- no ack on handler failure
- retry up to `retry.maxAttempts`
- optional failure sink (`onFailed`)
- ack after failure sink succeeds (`ackOnFailed: true`)

```ts
import { createClient, Auth, runAgent } from 'kalam-link';

await runAgent({
  client: createClient({
    url: 'http://localhost:8080',
    auth: Auth.basic('root', 'kalamdb123'),
  }),
  name: 'blog-summarizer',
  topic: 'blog.summarizer',
  groupId: 'blog-summarizer-agent',
  retry: { maxAttempts: 3 },
  onRow: async (ctx, row) => {
    await ctx.sql(
      'UPDATE blog.blogs SET summary = $1 WHERE blog_id = $2',
      [String((row as any).content ?? '').slice(0, 80), (row as any).blog_id],
    );
  },
  onFailed: async (ctx) => {
    await ctx.sql(
      'INSERT INTO blog.summary_failures (run_key, blog_id, error) VALUES ($1, $2, $3) ON CONFLICT (run_key) DO NOTHING',
      [ctx.runKey, String((ctx.row as any).blog_id ?? 'unknown'), String(ctx.error ?? 'unknown')],
    );
  },
  ackOnFailed: true,
});
```

### Consume one batch + ack manually

```ts
const batch = await client.consumeBatch({
  topic: 'chat.ai-processing',
  group_id: 'ai-worker-manual',
  start: 'earliest',
  batch_size: 20,
});

if (batch.messages.length > 0) {
  const last = batch.messages[batch.messages.length - 1];
  await client.ack(last.topic, last.group_id, last.partition_id, last.offset);
}
```

## Authentication

### `authProvider` — recommended

Use `authProvider` for OAuth flows, refresh tokens, or any credentials that can expire. The callback is invoked before every connection attempt, so tokens are always fresh.

```ts
import { createClient, Auth, type AuthProvider } from 'kalam-link';

const authProvider: AuthProvider = async () => {
  const token = await myApp.getOrRefreshJwt(); // your refresh logic
  return Auth.jwt(token);
};

const client = createClient({
  url: 'http://localhost:8080',
  authProvider,
});
```

### Static auth

```ts
import { createClient, Auth } from 'kalam-link';

// HTTP Basic Auth
const client = createClient({
  url: 'http://localhost:8080',
  auth: Auth.basic('alice', 'Secret123!'),
});

// JWT token
const client = createClient({
  url: 'http://localhost:8080',
  auth: Auth.jwt('<JWT_TOKEN>'),
});

// No authentication
const client = createClient({
  url: 'http://localhost:8080',
  auth: Auth.none(),
});
```

### `login()` and `refreshToken()`

- `login()` requires current auth type = `basic`
- on success, the SDK updates in-memory auth to JWT automatically
- `refreshToken()` also updates in-memory auth to JWT

```ts
const login = await client.login();
console.log(login.access_token, login.refresh_token);

const refreshed = await client.refreshToken(login.refresh_token!);
console.log(refreshed.access_token);
```

## Client Lifecycle

By default (`wsLazyConnect: true`) the client is fully lazy:

- `query()` boots WASM, automatically exchanges Basic auth for JWT when needed, and runs the HTTP request.
- `subscribe()` / `subscribeWithSql()` boot WASM, ensure auth is ready, and open the WebSocket automatically on first use.

### Manual WASM initialization

In most environments WASM is loaded automatically. If you need explicit control (e.g. custom `wasmUrl`, strict CSP, or pre-warming), call `initialize()` before the first query:

```ts
const client = createClient({
  url: 'http://localhost:8080',
  auth: Auth.basic('admin', 'pass'),
  wasmUrl: '/assets/kalam_link_bg.wasm', // custom WASM path
});

await client.initialize(); // load WASM explicitly
```

### Eager WebSocket connection

Set `wsLazyConnect: false` to establish the shared WebSocket immediately:

```ts
const client = createClient({
  url: 'http://localhost:8080',
  auth: Auth.basic('admin', 'pass'),
  wsLazyConnect: false,
});

// The SDK now opens the WebSocket during initialize()/construction flow
// and manages auth + connection lifecycle internally.
console.log(client.isConnected());

client.setAutoReconnect(true);
client.setReconnectDelay(1000, 30000);
client.setMaxReconnectAttempts(0); // 0 = infinite

console.log(client.getReconnectAttempts(), client.isReconnecting());

await client.disconnect();
```

### `using` / `await using` (explicit resource management)

```ts
// Async dispose
async function demo() {
  await using client = createClient({ url, auth });
  await client.query('SELECT 1');
} // disconnect() called automatically
```

## Event Handlers

Hook into connection events for logging, metrics, or UI state. Can be set via `ClientOptions` at construction time or by calling the methods after creation:

```ts
// At construction:
const client = createClient({
  url: 'http://localhost:8080',
  auth: Auth.basic('admin', 'pass'),
  onConnect: () => console.log('Connected'),
  onDisconnect: (reason) => console.log('Disconnected:', reason.message),
  onError: (err) => console.error('Error:', err.message, 'recoverable:', err.recoverable),
  onReceive: (msg) => console.log('[RECV]', msg),
  onSend: (msg) => console.log('[SEND]', msg),
});

// Or after construction:
client.onConnect(() => console.log('Connected'));
client.onDisconnect((reason) => console.log('Disconnected:', reason.message));
client.onError((err) => console.error('Error:', err.message));
client.onReceive((msg) => console.log('[RECV]', msg));
client.onSend((msg) => console.log('[SEND]', msg));
```

## Query API

### `query(sql, params?)`

```ts
const byId = await client.query(
  'SELECT * FROM app.messages WHERE conversation_id = $1 AND is_deleted = $2',
  ['conv_42', false],
);
```

### `queryOne<T>()` and `queryAll<T>()`

```ts
interface MessageRow {
  id: string;
  content: string;
}

const one = await client.queryOne<MessageRow>(
  'SELECT id, content FROM app.messages LIMIT 1',
);

const many = await client.queryAll<MessageRow>(
  'SELECT id, content FROM app.messages',
);
```

### `executeAsUser(sql, username, params?)`

```ts
await client.executeAsUser(
  'INSERT INTO chat.messages (conversation_id, role, content) VALUES ($1, $2, $3)',
  'alice',
  [42, 'assistant', 'Hello Alice'],
);
```

### Convenience DML

```ts
await client.insert('app.messages', { id: 1, content: 'hello' });
await client.update('app.messages', 1, { content: 'updated' });
await client.delete('app.messages', 1);
```

Note: `update()` builds SQL string values directly (with quote escaping for strings). For untrusted/complex input, prefer `query()` with parameter placeholders.

## Realtime Subscriptions

### Table sugar API

```ts
const unsub = await client.subscribe('app.messages', (event) => {
  console.log(event.type);
});
```

### SQL subscription

```ts
const unsub = await client.subscribeWithSql(
  'SELECT * FROM app.messages WHERE conversation_id = 42 ORDER BY created_at ASC',
  (event) => {
    if (event.type === 'change') {
      console.log(event.change_type, event.rows);
    }
  },
  { batch_size: 100, last_rows: 50 },
);
```

### Subscription management helpers

```ts
client.getSubscriptionCount();
client.getSubscriptions();
client.isSubscribedTo('SELECT * FROM app.messages');
client.getLastSeqId('<subscription-id>');

await client.unsubscribeAll();
```

## FILE Uploads + FileRef

### Upload with `queryWithFiles(...)`

```ts
await client.queryWithFiles(
  'INSERT INTO app.docs (id, attachment) VALUES ($1, FILE("attachment"))',
  { attachment: selectedFile },
  ['doc_1'],
  (progress) => {
    console.log(progress.file_name, progress.percent);
  },
);
```

### Parse file refs

```ts
import { parseFileRef } from 'kalam-link';

const ref = parseFileRef(row.attachment);
if (ref) {
  console.log(ref.name, ref.formatSize(), ref.getTypeDescription());
  console.log(ref.getDownloadUrl('http://localhost:8080', 'app', 'docs'));
}
```

## Query Helper Utilities

```ts
import {
  parseRows,
  normalizeQueryResponse,
  sortColumns,
  SYSTEM_TABLES_ORDER,
} from 'kalam-link';
```

- `parseRows<T>(response)` maps schema + row arrays into typed objects
- `normalizeQueryResponse(response, preferredOrder)` reorders columns consistently
- `sortColumns(columns, preferredOrder)` stable column order helper
- `SYSTEM_TABLES_ORDER` canonical ordering for `system.tables`

## Public API (Current)

### Exports from `kalam-link`

```ts
import KalamDBClient, {
  // client
  createClient,
  KalamDBClient,

  // auth
  Auth,
  buildAuthHeader,
  encodeBasicAuth,
  isAuthenticated,
  isBasicAuth,
  isJwtAuth,
  isNoAuth,

  // runtime enums / runtime helper
  MessageType,
  ChangeType,
  Username,

  // query helpers
  parseRows,
  normalizeQueryResponse,
  sortColumns,
  SYSTEM_TABLES_ORDER,

  // file refs
  FileRef,
  parseFileRef,
  parseFileRefs,

  // types
  type AuthCredentials,
  type BasicAuthCredentials,
  type JwtAuthCredentials,
  type NoAuthCredentials,
  type ClientOptions,
  type QueryResponse,
  type QueryResult,
  type SchemaField,
  type ErrorDetail,
  type ResponseStatus,
  type ServerMessage,
  type SubscriptionOptions,
  type SubscriptionCallback,
  type TypedSubscriptionCallback,
  type SubscriptionInfo,
  type Unsubscribe,
  type BatchControl,
  type BatchStatus,
  type SeqId,
  type ChangeTypeRaw,
  type ConsumeRequest,
  type ConsumeResponse,
  type ConsumeMessage,
  type ConsumeContext,
  type ConsumerHandle,
  type ConsumerHandler,
  type AckResponse,
  type UploadProgress,
  type JsonValue,
  type KalamDataType,
  type FieldFlag,
  type FieldFlags,
  type TimestampFormat,
  type HttpVersion,
  type HealthCheckResponse,
  type LoginResponse,
  type LoginUserInfo,
  type FileRefData,
  type WasmKalamClient,
} from 'kalam-link';
```

### `KalamDBClient` methods

```ts
// lifecycle
initialize(): Promise<void>
connect(): Promise<void>
disconnect(): Promise<void>
isConnected(): boolean

// event handlers (also settable via ClientOptions)
onConnect(callback: () => void): void
onDisconnect(callback: (reason: { message: string; code?: number }) => void): void
onError(callback: (error: { message: string; recoverable: boolean }) => void): void
onReceive(callback: (message: string) => void): void
onSend(callback: (message: string) => void): void

// auth/lifecycle helpers
getAuthType(): 'basic' | 'jwt' | 'none'
setAutoReconnect(enabled: boolean): void
setReconnectDelay(initialDelayMs: number, maxDelayMs: number): void
setMaxReconnectAttempts(maxAttempts: number): void
getReconnectAttempts(): number
isReconnecting(): boolean
getLastSeqId(subscriptionId: string): string | undefined

// sql
query(sql: string, params?: unknown[]): Promise<QueryResponse>
queryOne<T extends Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T | null>
queryAll<T extends Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T[]>
executeAsUser(sql: string, username: string, params?: unknown[]): Promise<QueryResponse>
insert(tableName: string, data: Record<string, unknown>): Promise<QueryResponse>
update(tableName: string, rowId: string | number, data: Record<string, unknown>): Promise<QueryResponse>
delete(tableName: string, rowId: string | number): Promise<void>

// files
queryWithFiles(
  sql: string,
  files: Record<string, File | Blob>,
  params?: unknown[],
  onProgress?: (progress: UploadProgress) => void,
): Promise<QueryResponse>

// auth
login(): Promise<LoginResponse>
refreshToken(refreshToken: string): Promise<LoginResponse>

// subscriptions
subscribe(tableName: string, callback: SubscriptionCallback, options?: SubscriptionOptions): Promise<Unsubscribe>
subscribeWithSql(sql: string, callback: SubscriptionCallback, options?: SubscriptionOptions): Promise<Unsubscribe>
unsubscribe(subscriptionId: string): Promise<void>
unsubscribeAll(): Promise<void>
getSubscriptionCount(): number
getSubscriptions(): SubscriptionInfo[]
isSubscribedTo(tableNameOrSql: string): boolean

// topics
consumer(options: ConsumeRequest): ConsumerHandle
consumeBatch(options: ConsumeRequest): Promise<ConsumeResponse>
ack(topic: string, groupId: string, partitionId: number, uptoOffset: number): Promise<AckResponse>
```

## Important Type Shapes

```ts
interface ClientOptions {
  url: string;

  /** @deprecated Use authProvider instead */
  auth?: AuthCredentials;

  /**
   * Async callback invoked before every connection.
   * Recommended for OAuth / refresh-token flows.
   * const authProvider: AuthProvider = async () => Auth.jwt(await getToken());
   */
  authProvider?: AuthProvider;

  /** Custom WASM URL or buffer (default: bundled WASM) */
  wasmUrl?: string | BufferSource;

  /** Set to false to disable lazy connect (default: true) */
  autoConnect?: boolean;

  // Event callbacks (can also be set via client.onConnect(...) etc.)
  onConnect?: () => void;
  onDisconnect?: (reason: { message: string; code?: number }) => void;
  onError?: (error: { message: string; recoverable: boolean }) => void;
  onReceive?: (message: string) => void;
  onSend?: (message: string) => void;
}

interface QueryResponse {
  status: 'success' | 'error';
  results?: QueryResult[];
  took?: number;
  error?: ErrorDetail;
}

interface QueryResult {
  schema?: SchemaField[];
  rows?: unknown[][];
  row_count: number;
  message?: string;
}

interface ConsumeRequest {
  topic: string;
  group_id: string;
  start?: string;
  batch_size?: number;
  partition_id?: number;
  timeout_seconds?: number;
  auto_ack?: boolean;
  concurrency_per_partition?: number;
}
```

## License

Apache-2.0

## Links

- Website: [kalamdb.org](https://kalamdb.org)
- Docs: [kalamdb.org/docs](https://kalamdb.org/docs)
- SDK reference: [kalamdb.org/docs/sdk](https://kalamdb.org/docs/sdk)
- Authentication & token setup: [kalamdb.org/docs/getting-started/authentication](https://kalamdb.org/docs/getting-started/authentication)
- Docker deployment: [kalamdb.org/docs/getting-started/docker](https://kalamdb.org/docs/getting-started/docker)
- Live Query / WebSocket architecture: [kalamdb.org/docs/architecture/live-query](https://kalamdb.org/docs/architecture/live-query)
- WebSocket protocol reference: [kalamdb.org/docs/api/websocket-protocol](https://kalamdb.org/docs/api/websocket-protocol)
- Source: [github.com/jamals86/KalamDB](https://github.com/jamals86/KalamDB)
- Issues: [github.com/jamals86/KalamDB/issues](https://github.com/jamals86/KalamDB/issues)

---

> Browser and Node.js support is powered by a Rust core compiled to WebAssembly (WASM). See [DEV.md](DEV.md) for build and contribution details.
