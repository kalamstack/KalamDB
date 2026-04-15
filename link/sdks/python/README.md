# KalamDB Python SDK

Official Python SDK for [KalamDB](https://github.com/kalamstack/KalamDB) — a SQL-first realtime database for AI agents, chat products, and multi-tenant SaaS.

Built on top of the Rust `kalam-client` core via PyO3, so the networking, reconnection, and protocol logic is shared with the TypeScript and Dart SDKs.

## Installation

```bash
pip install kalamdb
```

## Quick Start

```python
import asyncio
from kalamdb import KalamClient, Auth

async def main():
    async with KalamClient("http://localhost:8080", Auth.basic("admin", "password")) as client:
        # Run a SQL query
        rows = await client.query_rows("SELECT * FROM app.users LIMIT 10")
        for row in rows:
            print(row)

        # Insert a row
        await client.insert("app.messages", {
            "role": "user",
            "content": "hello from python"
        })

        # Subscribe to live changes
        async with await client.subscribe("SELECT * FROM app.messages") as sub:
            async for event in sub:
                print("change:", event)
                if event.get("change_type") == "insert":
                    break

asyncio.run(main())
```

## Authentication

```python
# Basic auth (username/password) — auto-exchanges for JWT on connect
client = KalamClient(url, Auth.basic("admin", "password"))

# JWT token (if you already have one)
client = KalamClient(url, Auth.jwt("eyJhbG..."))
```

## Queries

```python
# Full query response (schema + rows + metadata)
response = await client.query("SELECT id, name FROM app.users")

# Just the rows as a list of dicts
rows = await client.query_rows("SELECT id, name FROM app.users")

# Parameterized queries (recommended for user input)
rows = await client.query_rows(
    "SELECT * FROM app.users WHERE id = $1 AND active = $2",
    [user_id, True],
)

# Insert via convenience method (auto-builds SQL)
await client.insert("app.messages", {
    "role": "user",
    "content": "hello"
})

# Delete by ID
await client.delete("app.messages", 12345)
```

## File Uploads

KalamDB tables can have `FILE` columns. Reference uploaded files in SQL with `FILE("placeholder")`:

```python
with open("avatar.png", "rb") as f:
    data = f.read()

await client.query_with_files(
    "INSERT INTO app.users (name, avatar) VALUES ($1, FILE('avatar'))",
    {"avatar": ("avatar.png", data, "image/png")},
    ["alice"],
)
```

The `files` dict maps each placeholder to a tuple of `(filename, bytes)` or `(filename, bytes, mime_type)`.

## Error Handling

The SDK raises typed exceptions you can catch individually:

```python
from kalamdb import (
    KalamError,            # base class — catches everything
    KalamConnectionError,  # network, websocket, timeout
    KalamAuthError,        # bad credentials, expired token
    KalamServerError,      # server returned an error response
    KalamConfigError,      # invalid client configuration
)

try:
    rows = await client.query_rows("SELECT * FROM nonexistent")
except KalamServerError as e:
    print("server rejected query:", e)
except KalamAuthError:
    print("login failed — check credentials")
except KalamConnectionError:
    print("network unreachable")
```

## Topic Consumers (Pub/Sub)

For Kafka-style topic consumption, use `client.consume()`. Tracks per-group offsets so each consumer group reads independently.

```python
async with await client.consume(
    topic="blog.posts",
    group_id="summarizer-v1",
    start="earliest",  # or "latest"
) as consumer:
    while True:
        records = await consumer.poll()
        if not records:
            await asyncio.sleep(0.5)
            continue

        for record in records:
            try:
                print("got record:", record["offset"], record["payload"])
                await consumer.mark_processed(record)
            except Exception as e:
                print("failed:", e)
                # Don't mark — it'll be redelivered on the next poll.
                break

        await consumer.commit()  # ack all marked records
```

`mark_processed()` is separate from `poll()` so a mid-batch failure doesn't silently advance the offset past unhandled records. For typical AI agent use, prefer `run_agent` below which handles this for you.

### High-level Agent Helper

For most AI agent use cases, use `run_agent` instead — it handles the polling loop, retries with exponential backoff, and commits automatically.

```python
from kalamdb import KalamClient, Auth, run_agent

async with KalamClient(url, Auth.basic("admin", "pass")) as client:
    async def summarize(record):
        summary = await llm.summarize(record["payload"])
        await save_summary(record["message_id"], summary)

    async def on_failed(record, error):
        print("permanent failure:", record["offset"], error)

    await run_agent(
        client=client,
        topic="blog.posts",
        group_id="summarizer-v1",
        on_record=summarize,
        on_failed=on_failed,
        start="earliest",
        max_attempts=3,
        initial_backoff_ms=250,
        max_backoff_ms=2000,
        ack_on_failed=True,  # commit even on permanent failure (so it doesn't replay)
    )
```

Stop the loop with an `asyncio.Event`:

```python
stop = asyncio.Event()
asyncio.create_task(server.serve())  # something else
# later:
stop.set()
```

## Live Subscriptions

```python
sub = await client.subscribe("SELECT * FROM chat.messages WHERE thread_id = 1")

async for event in sub:
    kind = event.get("type")
    if kind == "subscription_ack":
        print("subscribed, total rows:", event["total_rows"])
    elif kind == "initial_data_batch":
        for row in event["rows"]:
            print("initial:", row)
    elif kind == "change":
        print(event["change_type"], event.get("rows"))
    elif kind == "error":
        print("error:", event["message"])
        break

await sub.close()
```

## Building from source

Requirements:
- Rust 1.92+
- Python 3.9+
- [maturin](https://github.com/PyO3/maturin)

```bash
cd link/sdks/python
python -m venv .venv
source .venv/bin/activate  # on Windows: .venv\Scripts\activate
pip install maturin
maturin develop
```

## Running the tests

Tests expect a KalamDB server running at `http://localhost:8088`. Start one with Docker:

```bash
docker compose -f ../../../docker/run/single/docker-compose.yml up -d
```

Then run the tests:

```bash
pip install -e ".[dev]"
pytest
```

Configure the test server via environment variables:

```bash
KALAMDB_TEST_URL=http://localhost:8088 \
KALAMDB_TEST_USER=admin \
KALAMDB_TEST_PASSWORD=yourpass \
  pytest
```

## Architecture

The SDK wraps the existing `kalam-client` Rust crate via [PyO3](https://pyo3.rs) (Python's standard FFI bridge to Rust). The compiled Rust core ships as a platform-specific wheel (`.whl`) on PyPI — no Rust toolchain needed at install time.

```
Python code  →  kalamdb (PyO3)  →  kalam-client (Rust)  →  network
```

This mirrors the architecture of the Dart SDK (also FFI) and differs from the TypeScript SDK (WASM, required by browsers).

## License

Apache-2.0
