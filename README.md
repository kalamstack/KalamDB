<p align="center">
  <img src="docs/images/kalamdb_logo.png" alt="KalamDB logo" width="260" />
</p>

[![Rust](https://img.shields.io/badge/rust-1.92%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![CI](https://github.com/jamals86/KalamDB/actions/workflows/ci.yml/badge.svg)](https://github.com/jamals86/KalamDB/actions/workflows/ci.yml)
[![Tests](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/jamals86/KalamDB/main/.github/badges/tests.json)](https://github.com/jamals86/KalamDB/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/jamals86/KalamDB?display_name=tag)](https://github.com/jamals86/KalamDB/releases)
[![Docker Pulls](https://img.shields.io/docker/pulls/jamals86/kalamdb)](https://hub.docker.com/r/jamals86/kalamdb)

KalamDB is a SQL-first realtime state database for AI agents, chat products, and multi-tenant SaaS.
It combines SQL execution, live subscriptions, pub/sub streams, and hot/cold storage in one Rust runtime.

> Frontend clients can execute SQL directly against KalamDB. This is not only a backend database layer.

## Frontend-First SQL Execution

To avoid confusion: KalamDB is designed for both frontend and backend SQL execution.

- Frontend (web/mobile/desktop): run SQL for user-scoped reads/writes and realtime subscriptions.
- Backend/workers: run automation, cross-tenant system jobs, and service workflows.
- Same SQL model on both sides: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `SUBSCRIBE TO`, `CONSUME`, `ACK`.

This is what lets chat UIs and agent apps read/write state directly and receive live updates without adding separate polling or fanout infrastructure.

## Why KalamDB

- SQL-first developer model with realtime primitives.
- Per-user isolation by default for tenant boundaries and privacy.
- Built-in live subscriptions over WebSocket.
- Topics + pub/sub streams with consumer groups.
- Hot/cold architecture: RocksDB (active writes) + Parquet/object storage (history and analytics).
- Multi-Raft clustering for replication and failover.
- Production tooling: Admin UI (SQL Studio + live streams), `kalam` CLI, official TypeScript SDK.

## 60-Second Quick Start (Docker)

### Single node

```bash
KALAMDB_JWT_SECRET="$(openssl rand -base64 32)" \
curl -sSL https://raw.githubusercontent.com/jamals86/KalamDB/main/docker/run/single/docker-compose.yml | docker-compose -f - up -d
```

### 3-node cluster

```bash
KALAMDB_JWT_SECRET="$(openssl rand -base64 32)" \
curl -sSL https://raw.githubusercontent.com/jamals86/KalamDB/main/docker/run/cluster/docker-compose.yml | docker-compose -f - up -d
```

### Local run

```bash
git clone https://github.com/jamals86/KalamDB.git
cd KalamDB/backend
cargo run --bin kalamdb-server
```

## Browser/Frontend Client Example

```ts
import { createClient, Auth } from 'kalam-link';

const client = createClient({
  url: 'http://localhost:8080',
  authProvider: async () => Auth.jwt('<user-token>'),
});

const threadSql = `
  SELECT id, role, content, created_at
  FROM chat.messages
  WHERE thread_id = 'thread_42'
  ORDER BY created_at ASC
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
import { createClient, Auth, runAgent } from 'kalam-link';

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

- SQL and execution: `kalamdb-sql` + DataFusion/Arrow.
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
- TypeScript SDK: `link/sdks/typescript/`
- Docker deployment: `docker/run/`
- Website: <https://kalamdb.org>

KalamDB is under active development and evolving quickly.
