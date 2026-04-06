# Kalam CLI

Interactive command-line client for KalamDB - a real-time database with WebSocket subscriptions.

## Features

- 🎯 **Interactive SQL Execution** - Execute queries with instant results
- 📊 **Multiple Output Formats** - Table, JSON, and CSV output modes
- 🔄 **Live Query Subscriptions** - Real-time data updates via WebSocket
- 🎨 **Syntax Highlighting** - Beautiful colored SQL syntax
- 📝 **Command History** - Persistent history with arrow key navigation and interactive menu
- ⚡ **Auto-completion** - TAB completion for SQL keywords, tables, and columns
- 🔐 **Authentication** - JWT tokens, username/password, and stored credentials
- 📁 **Batch Execution** - Run SQL scripts from files
- 🎭 **Progress Indicators** - Visual feedback for long-running queries

## Installation

### From Source

```bash
cd cli
cargo build --release
```

The binary will be available at `target/release/kalam`.

#### Using Environment Variables

```bash
# Set custom server URL and root password
export KALAMDB_SERVER_URL="http://127.0.0.1:3000"
export KALAMDB_ROOT_PASSWORD="your-password"

# Run all tests
cd cli
cargo test

# Run smoke tests
cargo test --test smoke -- --nocapture

# Or inline for a single run
KALAMDB_SERVER_URL="http://localhost:3000" \
KALAMDB_ROOT_PASSWORD="mypass" \
cargo test --test smoke -- --nocapture
```

**Environment Variables:**
- `KALAMDB_SERVER_URL` - Server URL (default: `http://127.0.0.1:8080`)
- `KALAMDB_ROOT_PASSWORD` - Root password (default: `""`)

### Cluster Test Shortcuts

```bash
# Run only the dedicated cluster integration target
cd cli
./run-cluster-tests.sh

# Run the follower/replication-focused cluster bundle
./run-cluster-tests.sh --followers --nocapture

# Run a single cluster test function
./run-cluster-tests.sh --test cluster_test_ws_follower_receives_leader_changes --nocapture
```

`run-cluster-tests.sh` delegates to `run-tests.sh`, pins the test target to `cluster`,
and keeps the same environment loading and cluster autodetection behavior.

## Quick Start

### Connect to Server

```bash
# Connect to default localhost:3000
kalam

# Connect to specific host
kalam --host myserver.com

# Connect with authentication
kalam --token YOUR_JWT_TOKEN
```

### Execute SQL

```sql
-- Create namespace
CREATE NAMESPACE app;

-- Create table
CREATE TABLE app.users (
    id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
    username TEXT NOT NULL,
    email TEXT,
    created_at TIMESTAMP DEFAULT NOW()
) WITH (TYPE = 'USER');

-- Insert data
INSERT INTO app.users (username, email) 
VALUES ('alice', 'alice@example.com');

-- Query data
SELECT * FROM app.users;
```

### Live Subscriptions

```sql
-- Subscribe to changes
SUBSCRIBE TO app.messages WHERE user_id = 'user123';

-- Pause subscription
\pause

-- Resume subscription
\continue

-- Stop subscription (Ctrl+C)
```

## Usage

### Command-Line Options

```
kalam [OPTIONS]

CONNECTION:
    -u, --url <URL>                 Server URL (e.g., http://localhost:3000)
    -H, --host <HOST>               Host address (alternative to URL)
    -p, --port <PORT>               Port number (default: 3000)

AUTHENTICATION:
    --token <TOKEN>                 JWT authentication token
    --username <USERNAME>           HTTP Basic Auth username
    --password [PASSWORD]           HTTP Basic Auth password (prompts if flag is present without value)
    --instance <INSTANCE>           Credential instance name (default: local)
    --save-credentials              Save credentials after successful login

EXECUTION:
    -f, --file <FILE>               Execute SQL from file and exit
    -c, --command <SQL>             Execute SQL command and exit
    --subscribe <SQL>               Subscribe to a table or live query
    --unsubscribe <ID>              Unsubscribe from a subscription (non-interactive)
    --list-subscriptions            List active subscriptions (non-interactive)

OUTPUT:
    --format <FORMAT>               Output format: table|json|csv (default: table)
    --json                          Shorthand for --format=json
    --csv                           Shorthand for --format=csv
    --no-color                      Disable colored output
    --no-spinner                    Disable spinners/animations
    --loading-threshold-ms <MS>     Loading indicator threshold (0 to always show)

TIMEOUTS:
    --timeout <SECONDS>             HTTP request timeout (default: 30)
    --connection-timeout <SECONDS>  Connection timeout (default: 10)
    --receive-timeout <SECONDS>     Receive timeout (default: 30)
    --auth-timeout <SECONDS>        WebSocket auth timeout (default: 5)
    --subscription-timeout <SECONDS>Subscription timeout (0 = no timeout, default: 0)
    --initial-data-timeout <SECONDS>Initial data timeout (default: 30)
    --fast-timeouts                 Fast timeout preset (local dev)
    --relaxed-timeouts              Relaxed timeout preset (high latency)

CONFIG:
    --config <PATH>                 Configuration file path (default: ~/.kalam/config.toml)
    -v, --verbose                   Enable verbose logging

CREDENTIALS:
    --show-credentials              Show stored credentials for instance
    --update-credentials            Login and update stored credentials
    --delete-credentials            Delete stored credentials
    --list-instances                List all stored credential instances

INFO:
    --help                          Print help information
    --version                       Print version information
```

### Non-Interactive Examples

```bash
# Run a single SQL command and exit
kalam --command "SELECT * FROM system.tables LIMIT 5"

# Run SQL from a file and exit
kalam --file setup.sql

# Subscribe to a live query (non-interactive)
kalam --subscribe "SUBSCRIBE TO app.messages WHERE user_id = 'alice'"

# Manage stored credentials
kalam --update-credentials --instance local --username root --password ""
kalam --show-credentials --instance local
kalam --list-instances
```

### Interactive Commands

Special commands starting with backslash (`\`):

| Command | Description |
|---------|-------------|
| `\help` / `\?` | Show available commands |
| `\quit` / `\q` | Exit the CLI |
| `\info` / `\session` | Show session info |
| `\format <table\|json\|csv>` | Set output format |
| `\dt` / `\tables` | List tables |
| `\d <table>` / `\describe <table>` | Describe a table |
| `\stats` / `\metrics` | Show system stats |
| `\health` | Check server health |
| `\flush` | Execute STORAGE FLUSH ALL |
| `\pause` | Pause ingestion |
| `\continue` | Resume ingestion |
| `\refresh-tables` / `\refresh` | Refresh autocomplete cache |
| `\subscribe <SQL>` / `\watch <SQL>` | Start live query |
| `\unsubscribe` / `\unwatch` | Cancel live query |
| `\show-credentials` / `\credentials` | Show stored credentials |
| `\update-credentials <u> <p>` | Update stored credentials |
| `\delete-credentials` | Delete stored credentials |
| `\cluster snapshot` | Trigger cluster snapshot |
| `\cluster purge --upto <index>` | Purge cluster logs up to index |
| `\cluster trigger-election` | Trigger cluster election |
| `\cluster transfer-leader <node_id>` | Transfer cluster leadership |
| `\cluster stepdown` | Request leader stepdown |
| `\cluster clear` | Clear old snapshots |
| `\cluster list` / `\cluster ls` | List cluster nodes |
| `\cluster list groups` | List cluster groups |
| `\cluster status` | Cluster status |
| `\cluster join <addr>` | Join a node (not yet implemented) |
| `\cluster leave` | Leave cluster (not yet implemented) |

### Using the CLI (Interactive Mode)

Once the CLI starts, type SQL directly and press **Enter** (or end with `;`) to run it. Use backslash commands (like `\help`) for CLI-specific actions.

Tips:
- Use **Tab** for auto-completion of SQL keywords, namespaces, tables, and columns.
- Use **↑/↓** to navigate command history.
- Use `\format json` or `\format csv` to switch output formats.
- Use `\subscribe <SQL>` for live updates.

### Output Formats

#### Table Format (Default)

```
┌─────────┬──────────┬─────────────────────┬────────────────────────┐
│ id      │ username │ email               │ created_at             │
├─────────┼──────────┼─────────────────────┼────────────────────────┤
│ 1234567 │ alice    │ alice@example.com   │ 2025-10-24 10:30:00    │
│ 1234568 │ bob      │ bob@example.com     │ 2025-10-24 10:31:00    │
└─────────┴──────────┴─────────────────────┴────────────────────────┘
(2 rows)
Took: 5.234 ms
```

#### JSON Format

```bash
kalam --json
```

```json
{
  "status": "success",
  "results": [
    {
      "id": 1234567,
      "username": "alice",
      "email": "alice@example.com",
      "created_at": "2025-10-24T10:30:00Z"
    }
  ],
  "took_ms": 5.234
}
```

#### CSV Format

```bash
kalam --csv
```

```csv
id,username,email,created_at
1234567,alice,alice@example.com,2025-10-24T10:30:00Z
1234568,bob,bob@example.com,2025-10-24T10:31:00Z
```

### Batch Execution

Execute SQL from a file:

```bash
kalam --file setup.sql
```

Example `setup.sql`:

```sql
CREATE NAMESPACE prod;
CREATE TABLE prod.events (
    id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
    event_type TEXT NOT NULL,
    data TEXT,
    created_at TIMESTAMP DEFAULT NOW()
) WITH (TYPE = 'USER');
INSERT INTO prod.events (event_type, data) VALUES ('login', '{"user":"alice"}');
```

### Configuration File

The CLI automatically creates and reads configuration from `~/.kalam/config.toml` on first run:

```toml
[server]
timeout = 30
max_retries = 0
http_version = "http2"         # http1, http2, auto

[connection]
auto_reconnect = true
reconnect_delay_ms = 100
max_reconnect_delay_ms = 30000
max_reconnect_attempts = 10

[ui]
format = "table"             # table, json, csv
color = true
history_size = 1000
timestamp_format = "iso8601" # iso8601, iso8601-date, iso8601-datetime, unix-ms, unix-sec, relative, rfc2822, rfc3339
```

The configuration file is created with default values when you first run the CLI. You can edit it to customize the behavior or override settings with command-line flags.

## Authentication

### Localhost Bypass

When connecting from localhost without credentials, the CLI automatically uses a default user:

```bash
kalam  # Uses default user on localhost
```

### JWT Token

```bash
kalam --token eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

### Username/Password + Stored Credentials

```bash
# Login and store credentials
kalam --username root --password "" --save-credentials

# Update stored credentials explicitly
kalam --update-credentials --instance local --username root --password ""
```

## Advanced Features

### Syntax Highlighting

The CLI provides beautiful SQL syntax highlighting with:
- **Blue bold** keywords (SELECT, INSERT, CREATE, etc.)
- **Magenta** data types (INT, TEXT, TIMESTAMP, etc.)
- **Green** string literals
- **Yellow** numeric literals

### Auto-completion

Press **TAB** to auto-complete:
- SQL keywords (SELECT, INSERT, UPDATE, etc.)
- Table names (loaded from `system.tables`)
- Column names (context-aware after table name)
- SQL types (INT, TEXT, TIMESTAMP, etc.)
- CLI commands (\help, \quit, etc.)

Refresh table/column cache:

```sql
\refresh-tables
```

### Command History

- **↑** arrow (on empty line): Opens interactive history dropdown menu
  - Oldest commands at top, newest at bottom
  - Selection starts at bottom (newest command)
  - **↑** to navigate to older commands (up), **↓** to newer commands (down)
  - **Enter** to select, **Esc** to cancel
  - Type to filter history entries in real-time
  - Selected command appears in your prompt ready to edit
  - Multi-line commands show first 2 lines for easy identification
- **`\history`** or **`\h`**: Alternative way to open the history menu
- History stored in `~/.kalam/history`
- Persistent across sessions
- `\history`, `\h`, `\quit`, and `\q` commands are not saved to history

### Progress Indicators

For queries taking longer than 200ms, the CLI shows a spinner with elapsed time:

```
⠋ Executing query... 1.2s
```

### Live Query Notifications

When subscribed, the CLI displays real-time notifications:

```
[2025-10-24 10:30:15] INSERT: {"id": 1234567, "content": "New message"}
[2025-10-24 10:30:20] UPDATE: {"id": 1234567, "content": "Updated message"}
[2025-10-24 10:30:25] DELETE: {"id": 1234567}
```

## Troubleshooting

### Connection Refused

```
Error: Failed to connect to localhost:3000
```

**Solution:** Ensure the KalamDB server is running:

```bash
cd backend
cargo run --bin kalamdb-server
```

### Authentication Failed

```
Error: Authentication failed (401 Unauthorized)
```

**Solution:** Provide valid credentials:

```bash
kalam --token YOUR_JWT_TOKEN
```

### Table Not Found

```
Error: table 'app.users' not found
```

**Solution:** Create the namespace and table first:

```sql
CREATE NAMESPACE app;
CREATE TABLE app.users (...) WITH (TYPE = 'USER');
```

## Examples

### Create and Query Table

```sql
-- Create namespace
CREATE NAMESPACE myapp;

-- Create table with auto-incrementing ID
CREATE TABLE myapp.users (
    id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
    username TEXT NOT NULL,
    email TEXT,
    created_at TIMESTAMP DEFAULT NOW()
) WITH (TYPE = 'USER');

-- Insert data
INSERT INTO myapp.users (username, email) VALUES 
    ('alice', 'alice@example.com'),
    ('bob', 'bob@example.com');

-- Query all users
SELECT * FROM myapp.users;

-- Query with filter
SELECT username, email FROM myapp.users WHERE username = 'alice';
```

### Live Subscription

```sql
-- Create messages table
CREATE TABLE chat.messages (
    id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
    user_id TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
) WITH (TYPE = 'USER');

-- Subscribe to new messages
SUBSCRIBE TO chat.messages WHERE user_id = 'alice';

-- In another terminal, insert messages:
-- INSERT INTO chat.messages (user_id, content) VALUES ('alice', 'Hello!');
-- The first terminal will show real-time notifications
```

### Batch Processing

Create `import.sql`:

```sql
CREATE NAMESPACE analytics;
CREATE TABLE analytics.events (
    id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
    event_type TEXT NOT NULL,
    user_id TEXT,
    data TEXT,
    created_at TIMESTAMP DEFAULT NOW()
) WITH (TYPE = 'USER');

INSERT INTO analytics.events (event_type, user_id, data) VALUES
    ('page_view', 'user1', '{"page":"/home"}'),
    ('click', 'user1', '{"button":"signup"}'),
    ('conversion', 'user1', '{"amount":99.99}');
```

Execute:

```bash
kalam --file import.sql
```

## Development

### Running Tests

```bash
cd cli
cargo test
```

### Building from Source

```bash
cd cli
cargo build --release
```

## License

Same license as KalamDB main project.

## Contributing

Contributions welcome! Please see the main KalamDB repository for contribution guidelines.

## Related Documentation

- [KalamDB API Reference](../../docs/api/api-reference.md)
- [SQL Syntax Guide](../../docs/reference/sql.md)
- [WebSocket Protocol](../../docs/api/websocket-protocol.md)
- [Quick Start Guide](../../docs/getting-started/quick-start.md)
