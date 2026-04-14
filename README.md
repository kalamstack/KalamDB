<p align="center">
  <img src="docs/images/kalamdb_logo.png" alt="KalamDB logo" width="260" />
</p>

<p align="center">
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/rust-1.92%2B-orange.svg" alt="Rust" /></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License" /></a>
  <a href="https://github.com/kalamstack/KalamDB/actions/workflows/ci.yml"><img src="https://github.com/kalamstack/KalamDB/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
  <a href="https://github.com/kalamstack/KalamDB/actions/workflows/ci.yml"><img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/kalamstack/KalamDB/main/.github/badges/tests.json" alt="Overall Tests" /></a>
  <a href="https://github.com/kalamstack/KalamDB/releases"><img src="https://img.shields.io/github/v/release/kalamstack/KalamDB?display_name=tag" alt="Release" /></a>
  <a href="https://hub.docker.com/r/jamals86/kalamdb"><img src="https://img.shields.io/docker/pulls/jamals86/kalamdb" alt="Docker Pulls" /></a>
  <a href="https://hub.docker.com/r/jamals86/pg-kalam"><img src="https://img.shields.io/docker/pulls/jamals86/pg-kalam?label=pg-kalam%20docker" alt="pg-kalam Docker Pulls" /></a>
</p>

<p align="center"><strong>SDKs</strong></p>

<p align="center">
  <a href="https://github.com/kalamstack/KalamDB/actions/workflows/sdks.yml"><img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/kalamstack/KalamDB/main/.github/badges/sdk-typescript-tests.json" alt="TypeScript SDK Tests" /></a>
  <a href="https://github.com/kalamstack/KalamDB/actions/workflows/sdks.yml"><img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/kalamstack/KalamDB/main/.github/badges/sdk-dart-tests.json" alt="Dart SDK Tests" /></a>
  <a href="https://www.npmjs.com/package/@kalamdb/client"><img src="https://img.shields.io/npm/v/%40kalamdb%2Fclient?label=typescript%20sdk" alt="TypeScript SDK" /></a>
  <a href="https://pub.dev/packages/kalam_link"><img src="https://img.shields.io/pub/v/kalam_link?label=dart%20sdk" alt="Dart SDK" /></a>
</p>

KalamDB is a SQL-first realtime state database for AI agents, chat products, and multi-tenant SaaS.
It combines SQL execution, live subscriptions, pub/sub streams, and hot/cold storage in one Rust runtime.

Supported SDKs: [TypeScript/JavaScript](https://www.npmjs.com/package/@kalamdb/client) and [Dart/Flutter](https://pub.dev/packages/kalam_link).

PostgreSQL extension Docker image: [jamals86/pg-kalam](https://hub.docker.com/r/jamals86/pg-kalam) with `pg_kalam` preinstalled.

> Frontend clients can execute SQL directly against KalamDB. This is not only a backend database layer.

## Frontend-First SQL Execution

To avoid confusion: KalamDB is designed for both frontend and backend SQL execution.

- Frontend (web/mobile/desktop): run SQL for user-scoped reads/writes and realtime subscriptions.
- Backend/workers: run automation, cross-tenant system jobs, and service workflows.
- Same SQL model on both sides: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `SUBSCRIBE TO`, `CONSUME`, `ACK`.

This is what lets chat UIs and agent apps read/write state directly and receive live updates without adding separate polling or fanout infrastructure.

## Why KalamDB

- Frontend-direct SQL and realtime subscriptions, so web and mobile apps can read, write, and stay in sync without a separate sync layer.
- Multi-tenant isolation by default, with the same SQL returning user-scoped data safely across frontend and backend clients.
- Built-in authentication and authorization with Basic auth, JWT, OAuth 2.0 (Google Workspace, GitHub, Azure AD), and RBAC.
- Unified application primitives: SQL tables, live queries, pub/sub topics, consumer groups, and file attachments in one runtime.
- Hybrid storage engine tuned for product workloads: RocksDB for fast active writes, Parquet for compressed historical storage and analytics.
- Portable object storage support across filesystem, S3, GCS, and Azure backends.
- Vector embeddings and vector search workflows for semantic retrieval, agent memory, and AI-powered product features.
- Distributed clustering with Multi-Raft replication and failover for resilient production deployments.
- First-party tooling for operators and app teams: Admin UI, `kalam` CLI, and official TypeScript and Dart SDKs.

## Feature & Status

| Feature                     | Status       | Feature                 | Status       |
| :-------------------------- | :----------- | :---------------------- | :----------- |
| **User Tables**             | ✅ Available | **Shared Tables**       | ✅ Available |
| **Streams**                 | ✅ Available | **Pub/Sub Topics**      | ✅ Available |
| **Live Queries**            | ✅ Available | **Consumer Groups**     | ✅ Available |
| **Cluster Replication**     | ✅ Available | **Vector Embeddings**   | ✅ Available |
| **PostgreSQL Extension**    | ✅ Available | **Admin UI**            | ✅ Available |
| **Kalam CLI**               | ✅ Available | **TypeScript SDK**      | ✅ Available |
| **Dart/Flutter SDK**        | ✅ Available | **Object Storage**      | ✅ Available |

## 60-Second Quick Start (Docker)

### Single node

```bash
KALAMDB_JWT_SECRET="$(openssl rand -base64 32)" \
curl -sSL https://raw.githubusercontent.com/kalamstack/KalamDB/main/docker/run/single/docker-compose.yml | docker-compose -f - up -d
```

### 3-node cluster

```bash
KALAMDB_JWT_SECRET="$(openssl rand -base64 32)" \
curl -sSL https://raw.githubusercontent.com/kalamstack/KalamDB/main/docker/run/cluster/docker-compose.yml | docker-compose -f - up -d
```

### Local run

```bash
git clone https://github.com/kalamstack/KalamDB.git
cd KalamDB/backend
cargo run --bin kalamdb-server
```

## Browser/Frontend Client Example

```ts
import { createClient, Auth } from '@kalamdb/client';

const client = createClient({
  url: 'http://localhost:8080',
  authProvider: async () => Auth.jwt('<user-token>'),
});

const threadSql = `
  SELECT id, role, content, created_at
  FROM chat.messages
  WHERE thread_id = 'thread_42'
`;

await client.query(`
  INSERT INTO chat.messages (thread_id, role, content)
  VALUES ('thread_42', 'user', 'hello from frontend');
`);

const unsubscribe = await client.live(
  threadSql,
  (rows) => {
    // If `chat.messages` is a USER table, each signed-in user only sees
    // their own rows even though the SQL text is identical for everyone.
    console.log('live rows', rows);
  },
  {
    subscriptionOptions: { last_rows: 20 },
  },
);

// Later
await unsubscribe();
await client.disconnect();
```

## AI Agent Example (Topic Subscription)

Subscribe an AI agent to a KalamDB topic and process each row with an LLM — fully managed retries, backpressure, and at-least-once delivery via `runAgent`.

```ts
import { createClient, Auth } from '@kalamdb/client';
import { runAgent } from '@kalamdb/consumer';

const client = createClient({
  url: 'http://localhost:8080',
  authProvider: async () => Auth.basic('root', 'kalamdb123'),
});

const abort = new AbortController();
process.on('SIGINT', () => abort.abort());

await runAgent<{ title: string; body: string }>({
  client,
  name: 'summarizer-agent',
  topic: 'blog.posts',       // KalamDB topic to consume
  groupId: 'summarizer-v1',  // consumer group — tracks per-agent offset
  start: 'earliest',
  batchSize: 20,
  timeoutSeconds: 30,
  stopSignal: abort.signal,

  // Called for every row; return value is written back as metadata
  onRow: async ({ row }) => {
    const summary = await myLlm.summarize(row.body);
    console.log(`[${row.title}] →`, summary);
  },

  onFailed: async ({ row, error }) => {
    console.error('failed row', row, error);
  },

  retry: {
    maxAttempts: 3,
    initialBackoffMs: 250,
    maxBackoffMs: 1500,
    multiplier: 2,
  },
  ackOnFailed: true,    // commit offset even on permanent failure
});

await client.disconnect();
```

See [`examples/summarizer-agent/`](examples/summarizer-agent/) for a full working example with Gemini integration.

## SQL Example

```sql
CREATE NAMESPACE IF NOT EXISTS chat;

CREATE TABLE chat.messages (
  id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  thread_id TEXT NOT NULL,
  role TEXT NOT NULL,
  content TEXT NOT NULL,
  attachment FILE,
  created_at TIMESTAMP DEFAULT NOW()
) WITH (TYPE = 'USER');
```

## Architecture Snapshot

- SQL parsing/classification: `kalamdb-dialect`; query execution: `kalamdb-core` + DataFusion/Arrow.
- Hot path: `kalamdb-store` (RocksDB).
- Cold path: `kalamdb-filestore` (Parquet/object storage).
- Orchestration: `kalamdb-core` (DDL/DML, jobs, schema registry).
- API surface: HTTP SQL API + WebSocket realtime + Admin UI + CLI.

## Best-Fit Workloads

- AI chat history and agent memory/state.
- Tool-call logs and human-in-the-loop workflows.
- Live dashboards, notifications, and collaborative feeds.
- Multi-tenant SaaS that needs strong user-level isolation.

## Docs and Links

- Docs: <https://kalamdb.org/docs>
- Quick start: `docs/getting-started/quick-start.md`
- TypeScript SDK: `link/sdks/typescript/client/`
- Docker deployment: `docker/run/`
- PostgreSQL extension Docker image: `jamals86/pg-kalam`
- Website: <https://kalamdb.org>

KalamDB is under active development and evolving quickly.
