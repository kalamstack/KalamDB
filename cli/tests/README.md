# CLI Test Organization

This document describes the test organization structure for KalamDB CLI tests.

## Test Categories

Tests are organized into the following categories:

### 1. **usecases/** - Use Case Integration Tests
Combined/integration tests that span multiple features.
- `test_batch_streaming.rs` - Batch streaming operations
- `test_chat_simulation.rs` - Chat application simulation
- `test_datatypes_json.rs` - JSON datatype end-to-end tests
- `test_update_all_types.rs` - UPDATE operations for all data types

**Run with**: `cargo test --test usecases`

### 2. **users/** - User Management Tests
User operations, admin functionality, and multi-user scenarios.
- `test_admin.rs` - Admin operations and permissions
- `test_concurrent_users.rs` - Concurrent user operations and isolation

**Run with**: `cargo test --test users`

### 3. **auth/** - Authentication & Authorization Tests
Authentication flows, JWT tokens, and authorization checks.
- `test_auth.rs` - Authentication and authorization workflows

**Run with**: `cargo test --test auth`

### 4. **cli/** - CLI Interface Tests
Command-line interface functionality and user interactions.
- `test_cli.rs` - General CLI features (connection, help, version, config)
- `test_cli_auth.rs` - Authentication tests (JWT, Basic Auth, sessions)
- `test_cli_auth_admin.rs` - Admin authorization and role-based access

**Run with**: `cargo test --test cli`

### 5. **subscription/** - Live Queries & Subscriptions
WebSocket subscriptions, live queries, and real-time data streaming.
- `test_subscribe.rs` - SUBSCRIBE TO command and live query operations
- `test_subscription_e2e.rs` - End-to-end subscription workflows
- `test_subscription_manual.rs` - Manual subscription verification
- `test_link_subscription_initial_data.rs` - Initial data batch tests
- `subscription_options_tests.rs` - Subscription configuration options
- `live_connection_tests.rs` - Live connection behavior

**Run with**: `cargo test --test subscription`

### 6. **tables/** - Table Management
User tables, shared tables, and table operations.
- `test_user_tables.rs` - User table CRUD and query operations
- `test_shared_tables.rs` - Shared table functionality and access

**Run with**: `cargo test --test tables`

### 7. **flushing/** - Flush Operations
Data flushing from hot (RocksDB) to cold (Parquet) storage.
- `test_flush.rs` - FLUSH command and storage transitions

**Run with**: `cargo test --test flushing`

### 8. **storage/** - Storage Lifecycle
Hot/cold storage transitions and storage management.
- `test_hot_cold_storage.rs` - Storage transition tests
- `test_storage_lifecycle.rs` - Complete storage lifecycle workflows

**Run with**: `cargo test --test storage`

### 9. **smoke/** - Smoke Tests
Quick validation tests organized into subcategories:

#### Subcategories:
- **usecases/** - Integration use case smoke tests
  - `chat_ai_example_smoke.rs`
  - `smoke_test_all_datatypes.rs`
  - `smoke_test_core_operations.rs`
  - `smoke_test_custom_functions.rs`
  - `smoke_test_timing_output.rs`
  - `smoke_test_int64_precision.rs`
  - `smoke_test_batch_control.rs`
  - `smoke_test_schema_history.rs`
  - `smoke_test_websocket_capacity.rs`

- **impersonating/** - AS USER impersonation smoke tests
  - `smoke_test_as_user_impersonation.rs`
  - `smoke_test_as_user_authorization.rs`
  - `smoke_test_as_user_chat_impersonation.rs`

- **subscription/** - Live query smoke tests
  - `smoke_test_stream_subscription.rs`
  - `smoke_test_subscription_advanced.rs`
  - `smoke_test_user_table_subscription.rs`

- **cli/** - CLI command smoke tests
  - `smoke_test_cli_commands.rs`
  - `smoke_test_cluster_operations.rs`

- **tables/** - Table operation smoke tests
  - `smoke_test_shared_table_crud.rs`
  - `smoke_test_user_table_rls.rs`

- **flushing/** - Flush operation smoke tests
  - `smoke_test_flush_manifest.rs`
  - `smoke_test_flush_operations.rs`
  - `smoke_test_flush_pk_integrity.rs`

- **storage/** - Storage template smoke tests
  - `smoke_test_storage_templates.rs`

- **ddl/** - DDL operation smoke tests
  - `smoke_test_ddl_alter.rs`
  - `smoke_test_alter_with_data.rs`

- **dml/** - DML operation smoke tests
  - `smoke_test_dml_extended.rs`
  - `smoke_test_dml_wide_columns.rs`
  - `smoke_test_insert_throughput.rs`

- **query/** - Query performance smoke tests
  - `smoke_test_00_parallel_query_burst.rs`
  - `smoke_test_queries_benchmark.rs`

- **system/** - System table smoke tests
  - `smoke_test_system_and_users.rs`
  - `smoke_test_system_tables_extended.rs`

**Run all smoke tests**: `cargo test --test smoke`
**Run specific category**: `cargo test --test smoke usecases`

### 10. **connection/** - Connection Management
Low-level connection behavior (timeouts, reconnection, etc.)
- `connection_options_tests.rs` - Connection configuration
- `reconnection_tests.rs` - Reconnection behavior
- `resume_seqid_tests.rs` - Sequence ID resumption
- `timeout_tests.rs` - Connection timeout handling

**Run with**: `cargo test --test connection`

### 11. **cluster/** - Cluster Operations
Multi-node cluster functionality (future).
- Various cluster-related tests

**Run with**: `cargo test --test cluster`

### 12. **performance/** - Live Server Performance Guards
Opt-in performance regression tests that measure a running server without auto-starting it.
- `test_server_memory_regression.rs` - Warmed insert/read workload with server RSS and `system.stats` peak/recovery checks against a post-warmup steady baseline

**Run with**: `cargo nextest run --features e2e-tests --test performance --run-ignored ignored-only`

## Running Tests

### Run All Tests
```bash
cargo test -p kalam-cli
```

### Run Specific Category
```bash
cargo test --test usecases
cargo test --test users
cargo test --test auth
cargo test --test cli
cargo test --test subscription
cargo test --test tables
cargo test --test flushing
cargo test --test storage
cargo test --test smoke
cargo nextest run --features e2e-tests --test performance --run-ignored ignored-only
```

### Run Specific Smoke Test Subcategory
```bash
cargo test --test smoke usecases
cargo test --test smoke auth
cargo test --test smoke subscription
cargo test --test smoke flushing
cargo test --test smoke ddl
```

### Run Individual Test
```bash
cargo test --test cli test_cli_connection
cargo test --test subscription test_cli_live_query_basic
cargo test --test usecases test_chat_simulation
```

## Test Requirements

### Server Dependency
Most integration tests require a running KalamDB server. Tests will skip with a warning if the server is not running:
```
⚠️  Server not running. Skipping test.
```

### Starting the Server
```bash
cd backend
cargo run
```

### Smoke Tests
Smoke tests are critical for ensuring basic functionality. **Always ensure smoke tests pass before committing**.

Run smoke tests:
```bash
cd cli
cargo test --test smoke
```

## Adding New Tests

When adding new tests:
1. Choose the appropriate category folder
2. Add the test file to the folder
3. Update the category's aggregator file (e.g., `usecases.rs`, `users.rs`, `auth.rs`)
4. For smoke tests, place in the appropriate subcategory
5. Document the test in this README if adding a new category

## Common Test Utilities

All test categories use the `common` module for shared utilities:
- `is_server_running()` - Check if KalamDB server is available
- `generate_unique_table()` - Generate unique table names
- `execute_sql_as_root_via_cli()` - Execute SQL commands
- Various assertion helpers

Import with:
```rust
use crate::common;  // For qualified access (common::function)
use crate::common::*;  // For wildcard import
```

## Future Migration Note

The test categories in this folder may be moved to a separate repository or testing directory in the future. The current structure is designed to support this migration with minimal changes.
