# Kalam CLI

Interactive terminal client for KalamDB, built on `kalam-client`.

Binary name: `kalam` (from the `cli` crate).

## Quickstart (first 5 minutes)

1) Start the server (default is `http://localhost:8080`)

```bash
cd backend
cargo run
```

2) Build the CLI

```bash
cd cli
cargo build --release
./target/release/kalam --help
```

3) Connect and run a query

```bash
./target/release/kalam

# inside kalam
\dt
SELECT * FROM system.tables LIMIT 5;
```

4) Optional: start a live query

```bash
# inside kalam
\subscribe SELECT * FROM app.messages WHERE user_id = 'alice';
```

### Install

```bash
cd cli
cargo build --release

./target/release/kalam --help
```

### Connect

```bash
# Default – uses stored credentials/config, otherwise http://localhost:8080
kalam

# Explicit URL
kalam --url http://localhost:8080

# Host/port alternative (note: if you use --host without --port, the default port is 3000)
kalam --host localhost --port 8080

# Basic auth
kalam --username alice --password Secret123!

# JWT
kalam --token "<JWT_TOKEN>"

# Save credentials (stores JWT token for future sessions)
kalam --username alice --password Secret123! --save-credentials --instance dev
```

### Run SQL

```bash
# One command and exit
kalam -c "SELECT * FROM system.tables LIMIT 5;"

# File and exit
kalam -f setup.sql
```

### Important flags

- `--url`, `-u` – server URL
- `--host`, `-H` and `--port`, `-p` – alternative to `--url`
- `--instance` – credential instance name (default: `local`)
- `--token` – JWT bearer token
- `--username` / `--password` – Basic auth
- `--save-credentials` – save JWT token after login
- `--show-credentials` – show stored credentials for instance
- `--update-credentials` – login and update stored credentials
- `--delete-credentials` – delete stored credentials for instance
- `--list-instances` – list stored credential instances
- `--format` – `table` (default) | `json` | `csv`
- `--json` / `--csv` – shorthand for `--format`
- `--no-color` – disable colored output
- `--no-spinner` – disable spinners/animations
- `--loading-threshold-ms` – loading indicator threshold
- `--file`, `-f` – execute SQL file
- `--command`, `-c` – execute a single SQL statement
- `--config` – config path (default `~/.kalam/config.toml`)
- `--verbose`, `-v` – verbose logging
- `--timeout` – HTTP timeout in seconds
- `--connection-timeout` – connection timeout in seconds
- `--receive-timeout` – receive timeout in seconds
- `--auth-timeout` – WebSocket auth timeout in seconds
- `--fast-timeouts` / `--relaxed-timeouts` – timeout presets
- `--subscribe <SQL>` – subscribe (non-interactive) to a live query
- `--subscription-timeout` – subscription idle timeout in seconds (0 = no timeout)
- `--initial-data-timeout` – max seconds to wait for initial data batch
- `--list-subscriptions` – list active subscriptions
- `--unsubscribe <subscription_id>` – unsubscribe by id

### Interactive `\` commands

In interactive mode, meta-commands start with `\`:

| Command                          | Description                               |
|----------------------------------|-------------------------------------------|
| `\help`, `\?`                   | Show help                                 |
| `\quit`, `\q`                   | Exit                                      |
| `\info`, `\session`             | Show session info                         |
| `\dt`, `\tables`               | List tables (`system.tables`)             |
| `\d <table>`, `\describe <table>` | Describe table                         |
| `\stats`, `\metrics`          | Show `system.stats`                       |
| `\health`                       | Server healthcheck                        |
| `\pause` / `\continue`          | Pause/resume ingestion                    |
| `\format table|json|csv`        | Change output format                      |
| `\subscribe <SQL>`, `\watch <SQL>` | Start WebSocket live subscription     |
| `\unsubscribe`, `\unwatch`      | No-op (prints “No active subscription to cancel”) |
| `\cluster ...`                  | Cluster commands (see below)              |
| `\refresh-tables`, `\refresh` | Refresh autocomplete metadata             |
| `\show-credentials`, `\credentials` | Show stored credentials               |
| `\update-credentials <u> <p>`  | Update stored credentials                 |
| `\delete-credentials`          | Delete stored credentials                 |

**Cluster meta-commands**:

- `\cluster snapshot`
- `\cluster purge --upto <index>` (or `\cluster purge <index>`)
- `\cluster trigger-election`
- `\cluster transfer-leader <node_id>`
- `\cluster stepdown`
- `\cluster clear`
- `\cluster list` (alias: `\cluster ls`)
- `\cluster list groups`
- `\cluster status`
- `\cluster join <node_addr>` (not yet implemented)
- `\cluster leave` (not yet implemented)

### Output formats

- `table` – pretty table with row count and latency
- `json` – raw JSON rows
- `csv` – header + rows (good for piping)

### Live subscriptions

Start a live query from interactive mode:

```bash
kalam> \subscribe SELECT * FROM app.messages WHERE user_id = 'alice';
```

Or start one from a non-interactive invocation:

```bash
kalam --subscribe "SELECT * FROM app.messages WHERE user_id = 'alice';"
```

### Multiple Instances

```bash
# Setup credentials for different environments
kalam --update-credentials --instance dev --username dev_user
kalam --update-credentials --instance staging --username staging_user
kalam --update-credentials --instance prod --username prod_admin

# Switch between instances
kalam --instance dev      # Connect to dev
kalam --instance staging  # Connect to staging
kalam --instance prod     # Connect to production
```

### Advanced Queries

```bash
# Complex aggregation with output formatting
kalam --instance prod \
  --command "SELECT country, COUNT(*) as users FROM users GROUP BY country" \
  --format json \
  --no-color > stats.json

# Any SQL supported by the server works here.
```

### File uploads in INSERT/UPDATE

You can upload files directly from the CLI using the `file()` helper in `INSERT` or `UPDATE` statements.

```bash
KalamDB[cluster] root@0.0.0.0:8080 ❯ INSERT INTO chat.uploads (id, name, attachment)
  VALUES ('doc2', 'CLI Doc', file('/Users/user/document1.pdf', 'text/plain'));
Inserted 1 row(s)
Query OK, 1 rows affected
```

```bash
KalamDB[cluster] root@0.0.0.0:8080 ❯ UPDATE chat.uploads
  SET attachment = file('/Users/user/document1.pdf', 'text/plain')
  WHERE id = 'doc2';
```

Selecting the row returns file metadata in the column value.

---

## Smoke Tests

Fast end-to-end checks that your server and CLI are wired correctly. The suite covers:

- User table subscription lifecycle
- Shared table CRUD
- System tables and user lifecycle
- Stream table subscription
- User table row-level security (per-user isolation)

Requirements:

- Server running at http://localhost:8080

Run options:

1) Directly with Cargo

```bash
cargo test -p kalam-cli smoke -- --test-threads=1 --nocapture
```

Run individual tests (examples):

```bash
# User table subscription lifecycle
cargo test -p kalam-cli smoke_user_table_subscription_lifecycle -- --nocapture

# Shared table CRUD
cargo test -p kalam-cli smoke_shared_table_crud -- --nocapture

# System tables + user lifecycle
cargo test -p kalam-cli smoke_system_tables_and_user_lifecycle -- --nocapture

# Stream table subscription
cargo test -p kalam-cli smoke_stream_table_subscription -- --nocapture

# User table RLS (per-user isolation)
cargo test -p kalam-cli smoke_user_table_rls_isolation -- --nocapture
```

Notes:

- Default server URL for tests is http://localhost:8080.

---

## Keyboard Shortcuts

### Line Editing

| Shortcut | Action |
|----------|--------|
| `Ctrl+A` | Move to beginning of line |
| `Ctrl+E` | Move to end of line |
| `Ctrl+K` | Delete from cursor to end of line |
| `Ctrl+U` | Delete from cursor to beginning of line |
| `Ctrl+W` | Delete word before cursor |
| `Alt+D` | Delete word after cursor |

### History Navigation

| Shortcut | Action |
|----------|--------|
| `↑` | Previous command |
| `↓` | Next command |
| `Ctrl+R` | Reverse search history |
| `Ctrl+S` | Forward search history |

### Completion

| Shortcut | Action |
|----------|--------|
| `Tab` | Autocomplete SQL keywords, tables, columns |
| `Tab Tab` | Show all completions |

### Control

| Shortcut | Action |
|----------|--------|
| `Ctrl+C` | Cancel current query/subscription |
| `Ctrl+D` | Exit CLI (alternative to `\quit`) |
| `Ctrl+L` | Clear screen |

---

## Tips & Tricks

### 1. Auto-Completion

The CLI provides intelligent auto-completion:
- **SQL Keywords**: `SEL` + Tab → `SELECT`
- **Table Names**: `FROM us` + Tab → `FROM users`
- **Column Names**: Context-aware completion in SELECT/WHERE clauses

```sql
kalam> SELECT na[Tab]
kalam> SELECT name FROM us[Tab]
kalam> SELECT name FROM users WHERE a[Tab]
```

### 2. Loading Indicator

Queries taking longer than 200ms show a loading spinner:
```
Executing query...
```

### 3. Pretty Tables

Tables automatically adjust to terminal width:
- Columns exceeding 50 characters are truncated with `...`
- Total table width respects terminal size
- Change format with `\format json` or `\format csv`

### 4. Color Output

Disable colors for piping or logging:
```bash
kalam --no-color -c "SELECT * FROM users" > output.txt
```

### 5. Timing Information

Execution time is displayed for all queries:
```
(10 rows)

Took: 245.123 ms
```

### 6. Error Messages

Clear, actionable error messages:
```
ERROR 1001: Table 'users' not found
Details: Available tables: system.tables, system.users, events
```

### 7. Batch Operations

Execute multiple statements from a file:
```sql
-- migration.sql
CREATE TABLE products (id INT, name VARCHAR(100));
INSERT INTO products VALUES (1, 'Laptop'), (2, 'Phone');
SELECT * FROM products;
```

```bash
kalam -f migration.sql
```

### 8. Watch Mode (Real-Time)

Monitor live data changes:
```sql
-- Terminal 1: Start watching
kalam> \subscribe SELECT * FROM orders WHERE status = 'pending'

-- Terminal 2: Insert data
kalam> INSERT INTO orders (id, status) VALUES (1, 'pending');

-- Terminal 1 automatically shows the new row
```

### 9. Quick Health Check

Health check is a CLI meta-command (interactive mode):

```bash
kalam

# inside kalam
\health
```

### 10. System Introspection

```sql
-- Find large tables
SELECT table_name, row_count 
FROM system.tables 
ORDER BY row_count DESC;

-- Monitor active connections
SELECT * FROM system.users WHERE last_seen > NOW() - INTERVAL 5 MINUTES;

-- Check running jobs
SELECT * FROM system.jobs WHERE status = 'running';
```

### 11. Cache Statistics and System Metrics

View server metrics using the `\stats` command (alias: `\metrics`). This runs:

```sql
SELECT * FROM system.stats ORDER BY key;
```

```bash
# Show all cache statistics
kalam> \stats

# Or use the alias
kalam> \metrics
```



---

## Troubleshooting

### Connection Issues

```bash
# Use interactive health check
kalam --url http://localhost:8080

# inside kalam
\health

# Verbose mode for debugging
kalam --verbose --url http://localhost:8080
```

### Authentication Failures

```bash
# Verify stored credentials
kalam --show-credentials --instance local

# Clear and re-enter credentials
kalam --delete-credentials --instance local
kalam --update-credentials --instance local
```

### Performance Issues

```bash
# Reduce timeout for faster failures (CLI flag)
kalam --timeout 10

# Check query execution time
kalam> SELECT * FROM large_table LIMIT 1;
# Took: 1234.567 ms
```

### Display Issues

```bash
# Disable colors if rendering incorrectly
kalam --no-color

# Switch to JSON for machine-readable output
kalam --format json

# Adjust terminal width or use CSV
kalam --csv
```

---

## Related Documentation

- [API Examples (Bruno collection)](../API-Kalam/) - REST API request examples
- [SQL Syntax](../reference/sql.md) - Complete SQL syntax guide
- [WebSocket Protocol](../api/websocket-protocol.md) - Real-time subscription details
- [Development Setup](../development/development-setup.md) - Build and development guide

---

## Support

For issues, questions, or contributions:
- GitHub: [github.com/jamals86/KalamDB](https://github.com/jamals86/KalamDB)
- Documentation: [docs/README.md](../README.md)

---

**Version**: 0.1.3  
**Last Updated**: October 28, 2025
