# KalamDB Quick Start

Fast path: build, run, and issue your first SQL query.

## 1. Prerequisites

- Git
- Rust 1.92+
- C++ toolchain (build-essential / Xcode CLT / MSVC)

For full setup and troubleshooting, see [Development Setup](../development/development-setup.md).

## 2. Clone and build

```bash
git clone https://github.com/kalamstack/KalamDB.git
cd KalamDB/backend

cargo build --release --bin kalamdb-server
```

## 3. Run the server

```bash
cargo run --release --bin kalamdb-server
```

You should see logs indicating the server is listening on `http://127.0.0.1:8080`.

## 4. Healthcheck

```bash
curl http://127.0.0.1:8080/v1/api/healthcheck
```

Expected:

```json
{"status":"healthy","api_version":"v1"}
```

## 5. Create namespace and table

KalamDB requires an `Authorization` header for `POST /v1/api/sql`.

For local development (localhost / `127.0.0.1`), the default `root` user can authenticate with an empty password, so you can use `curl -u root:`.

```bash
curl -u root: -X POST http://127.0.0.1:8080/v1/api/sql \
  -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{"sql":"CREATE NAMESPACE app;"}
JSON

curl -u root: -X POST http://127.0.0.1:8080/v1/api/sql \
  -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{"sql":"CREATE TABLE app.messages (id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(), content TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW()) WITH (TYPE='USER', FLUSH_POLICY='rows:1000');"}
JSON
```

## 6. Insert and query

```bash
curl -u root: -X POST http://127.0.0.1:8080/v1/api/sql \
  -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{"sql":"INSERT INTO app.messages (content) VALUES ('Hello from KalamDB');"}
JSON

curl -u root: -X POST http://127.0.0.1:8080/v1/api/sql \
  -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{"sql":"SELECT * FROM app.messages ORDER BY created_at DESC LIMIT 10;"}
JSON
```

## 7. Next steps

### Optional: Execute as another user (impersonation)

Use wrapper syntax only:

```bash
curl -u root: -X POST http://127.0.0.1:8080/v1/api/sql \
  -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{"sql":"EXECUTE AS USER 'root' (SELECT * FROM app.messages LIMIT 1);"}
JSON
```

### Optional: Topic/consume and storage maintenance

```bash
curl -u root: -X POST http://127.0.0.1:8080/v1/api/sql \
  -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{"sql":"CREATE TOPIC app.new_messages PARTITIONS 2;"}
JSON

curl -u root: -X POST http://127.0.0.1:8080/v1/api/sql \
  -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{"sql":"CONSUME FROM app.new_messages GROUP 'quickstart-worker' FROM LATEST LIMIT 10;"}
JSON

curl -u root: -X POST http://127.0.0.1:8080/v1/api/sql \
  -H 'Content-Type: application/json' \
  -d @- <<'JSON'
{"sql":"STORAGE FLUSH ALL IN app;"}
JSON
```

- `../reference/sql.md` – more SQL examples
- `../api/api-reference.md` – HTTP API reference
- `../api/websocket-protocol.md` – WebSocket subscriptions
- `../getting-started/cli.md` – `kalam` command-line client
