# KalamDB Backend

Rust-based messaging server with RocksDB storage and REST API.

## 🚀 Quick Start

### Prerequisites

**First time setting up?** See our comprehensive setup guides:

- **[📘 Development Setup Guide](../docs/development/development-setup.md)** - Complete installation for Windows/macOS/Linux
- **[🚀 Quick Start](../docs/getting-started/quick-start.md)** - Get running in 10 minutes

**Requirements:**
- Rust 1.75 or later
- LLVM/Clang (required for RocksDB compilation)
- C++ Compiler (Visual Studio Build Tools on Windows, Xcode CLI on macOS, build-essential on Linux)

### Build

```bash
cd backend
cargo build
```

**Note**: First build takes 10-20 minutes as it compiles RocksDB, Arrow, and Parquet from source.

**Known Issue**: If you encounter chrono/arrow-arith compilation errors, see [KNOWN_ISSUES.md](./KNOWN_ISSUES.md) for the fix.

### Configure

Copy the example configuration and customize it:

```bash
cp server.example.toml server.toml
# Edit server.toml with your settings
```

### Run

```bash
cargo run
```

The server will start on `http://127.0.0.1:8080` by default.

## New Features (Phase 006)

### API Key Authentication

Create users with auto-generated API keys for secure access:

```bash
# Create a user with API key
cargo run --bin kalamdb-server -- create-user --name "demo-user" --role "user"

# Output:
# ✅ User created successfully!
# API Key: 550e8400-e29b-41d4-a716-446655440000
```

Use the API key with the `X-API-KEY` header:

```bash
curl -X POST http://localhost:8080/sql \
  -H "Content-Type: application/json" \
  -H "X-API-KEY: 550e8400-e29b-41d4-a716-446655440000" \
  -d '{"query": "SELECT * FROM todos"}'
```

**Note**: Localhost (127.0.0.1) connections bypass API key requirement for development convenience.

### Soft Delete

User table rows are now soft-deleted (marked as deleted) instead of being physically removed:

```sql
-- Delete row (soft delete)
DELETE FROM my_table WHERE id = 5;

-- Row is hidden from SELECT queries
SELECT * FROM my_table;  -- Won't show id=5

-- Admin can view deleted rows (future feature)
SELECT * FROM my_table INCLUDE DELETED;
```

Benefits:
- Data recovery capability
- Audit trail preservation
- Safer than hard delete

### Docker Deployment

Deploy KalamDB in a production-ready Docker container:

```bash
cd docker/build
docker build -f Dockerfile -t jamals86/kalamdb:latest ../..
cd ../run/single
docker-compose up -d
```

See [docker/README.md](../docker/README.md) for complete deployment guide.

### Environment Variable Configuration

Override any server.toml setting with environment variables:

```bash
KALAMDB_SERVER_PORT=9000 \
KALAMDB_LOG_LEVEL=debug \
cargo run
```

**Format**: `KALAMDB_` + uppercase path with `_` separator
- `KALAMDB_SERVER_PORT` → `[server].port`
- `KALAMDB_LOG_LEVEL` → `[logging].level`
- `KALAMDB_DATA_DIR` → `[storage].data_dir`

### Client SDKs

KalamDB provides official SDKs for multiple languages in `link/sdks/`:

**TypeScript/JavaScript SDK** (`link/sdks/typescript/client/`):
- 📦 Published as `@kalamdb/client` on npm
- 🔧 37 KB WASM module with full TypeScript types
- ✅ 14 passing tests, comprehensive API documentation
- 🌐 Works in browsers and Node.js

**Usage in Examples**:
```json
{
  "dependencies": {
    "@kalamdb/client": "file:../../link/sdks/typescript/client"
  }
}
```

**Important**: Examples MUST use SDKs as dependencies, not create mock implementations. See [SDK Integration Guide](../specs/006-docker-wasm-examples/SDK_INTEGRATION.md) for architecture details.

See [link/README.md](../link/README.md) for complete SDK documentation.

## Development

### Project Structure

```
backend/
├── Cargo.toml                    # Workspace configuration
├── server.example.toml           # Example configuration
├── crates/
│   ├── kalamdb-core/            # Core storage library
│   ├── kalamdb-api/             # REST API library
│   └── kalamdb-server/          # Main server binary
└── tests/                        # Integration tests
```

### Running Tests

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_message_storage

# Run integration tests only
cargo test --test '*'

# Run with output
cargo test -- --nocapture
```

### Code Quality

```bash
# Check for errors
cargo check

# Run clippy (linter)
cargo clippy

# Format code
cargo fmt

# Build optimized release
cargo build --release
```

## Configuration

See `server.example.toml` for all available configuration options:

- **Server**: Host, port, worker threads
- **Storage**: RocksDB path, WAL settings, compression
- **Limits**: Message size, query limits
- **Logging**: Log level, file path, format
- **Performance**: Timeouts, connection limits

## API Endpoints

### POST /api/v1/messages
Insert a new message

**Request:**
```json
{
  "conversation_id": "conv_123",
  "from": "user_alice",
  "content": "Hello, world!",
  "metadata": {"role": "user"}
}
```

**Response:**
```json
{
  "msg_id": 1234567890,
  "status": "stored"
}
```

### POST /api/v1/query
Query messages

**Request:**
```json
{
  "conversation_id": "conv_123",
  "limit": 50
}
```

**Response:**
```json
{
  "messages": [...],
  "count": 50,
  "has_more": false
}
```

### GET /health
Health check endpoint

**Response:**
```json
{
  "status": "healthy",
  "version": "0.1.0"
}
```

## Logging

Logs are written to the path specified in `server.toml` (default: `./logs/app.log`).

Log levels: `error`, `warn`, `info`, `debug`, `trace`

To change log level:
```toml
[logging]
level = "debug"
```

## Architecture

- **kalamdb-core**: Core storage engine with RocksDB wrapper, message models, and ID generation
- **kalamdb-api**: REST API handlers and routes using Actix-web
- **kalamdb-server**: Main binary that ties everything together

## Current Status

**Phase 1 Complete**: Project structure and dependencies set up ✅

**Phase 2 Next**: Implement foundational components (config, logging, models, storage)

See `specs/001-build-a-rust/tasks.md` for the complete task list.

## Known Issues & Troubleshooting

### Arrow 52.2.0 + Chrono 0.4.40+ Conflict (RESOLVED)

If you encounter compilation errors with `arrow-arith` and `chrono::quarter()`, this has been fixed by pinning chrono to 0.4.39.

**Verification**:
```bash
# PowerShell (Windows)
pwsh -File scripts/verify-chrono-version.ps1

# Bash (Linux/macOS)
bash scripts/verify-chrono-version.sh
```

For details, see [KNOWN_ISSUES.md](./KNOWN_ISSUES.md).

## License

MIT OR Apache-2.0
