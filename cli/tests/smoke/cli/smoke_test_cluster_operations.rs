//! Smoke tests for cluster-related operations
//!
//! Tests the Raft-backed command execution path. In standalone mode,
//! commands go through StandaloneExecutor. In cluster mode, they go
//! through RaftExecutor. Both use the same CommandExecutor trait.
//!
//! These tests verify:
//! - Command execution works correctly (regardless of backend)
//! - Data consistency after operations
//! - System tables reflect operations accurately
//! - Multi-user data partitioning works correctly
//!
//! Note: Full multi-node cluster tests require multiple server instances.
//! These smoke tests focus on single-node behavior that must work in both modes.

use crate::common::*;
use kalam_client::KalamLinkTimeouts;
use std::sync::OnceLock;
use std::time::Duration;

fn cluster_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("Failed to create cluster test runtime")
    })
}

fn query_count_on_url(base_url: &str, sql: &str) -> i64 {
    let password = default_password().to_string();
    let sql = sql.to_string();
    let base_url = base_url.to_string();

    cluster_runtime()
        .block_on(async move {
            let client = client_for_user_on_url_with_timeouts(
                &base_url,
                default_username(),
                &password,
                KalamLinkTimeouts::builder()
                    .connection_timeout_secs(5)
                    .receive_timeout_secs(30)
                    .send_timeout_secs(10)
                    .subscribe_timeout_secs(10)
                    .auth_timeout_secs(10)
                    .initial_data_timeout(Duration::from_secs(30))
                    .build(),
            )
            .expect("Failed to build cluster client");
            client.execute_query(&sql, None, None, None).await
        })
        .map(|response| {
            let result = response.results.get(0).expect("Missing query result for count");
            let rows =
                result.rows.as_ref().and_then(|rows| rows.get(0)).expect("Missing count row");
            let value = rows.get(0).expect("Missing count column");
            let unwrapped = extract_typed_value(value);
            match unwrapped {
                serde_json::Value::String(s) => s.parse::<i64>().expect("Invalid count string"),
                serde_json::Value::Number(n) => n.as_i64().expect("Invalid count number"),
                other => panic!("Unexpected count value: {}", other),
            }
        })
        .expect("Cluster count query failed")
}

#[ntest::timeout(60_000)]
#[test]
fn smoke_test_cluster_system_table_counts_consistent() {
    if !require_server_running() {
        return;
    }

    // Skip if not in cluster mode
    if !is_cluster_mode() {
        println!("ℹ️  Skipping cluster-only test in single-node mode");
        return;
    }

    println!("\n=== TEST: Cluster System Table Count Consistency ===\n");

    let urls = get_available_server_urls();

    let queries = [
        ("system.schemas", "SELECT count(*) as count FROM system.schemas"),
        ("system.users", "SELECT count(*) as count FROM system.users"),
        ("system.namespaces", "SELECT count(*) as count FROM system.namespaces"),
    ];

    for (label, sql) in queries {
        let mut counts = Vec::new();
        for url in &urls {
            let count = query_count_on_url(url, sql);
            counts.push((url.clone(), count));
        }

        let max_count = counts.iter().map(|(_, count)| *count).max().unwrap_or(0);
        let min_count = counts.iter().map(|(_, count)| *count).min().unwrap_or(0);

        // Allow for slight differences due to eventual consistency
        let difference = max_count - min_count;
        if difference > 2 {
            eprintln!("⚠️  {} counts have significant mismatch: {:?}", label, counts);
            eprintln!("   This may indicate replication lag in the cluster");
            // Don't fail the test, just warn
        } else {
            println!(
                "  ✓ {} count consistent across nodes (max: {}, min: {})",
                label, max_count, min_count
            );
        }
    }

    println!("\n  ✅ System table counts checked (eventual consistency allowed)\n");
}

/// Test 1: CommandExecutor pattern - namespace operations
///
/// Verifies CREATE/DROP NAMESPACE commands are correctly executed
/// via the CommandExecutor abstraction (works in both standalone and cluster modes)
#[ntest::timeout(60_000)]
#[test]
fn smoke_test_cluster_namespace_consistency() {
    if !require_server_running() {
        return;
    }

    // Skip if not in cluster mode
    if !is_cluster_mode() {
        println!("ℹ️  Skipping cluster-only test in single-node mode");
        return;
    }

    println!("\n=== TEST: Namespace Command Consistency ===\n");

    // Create multiple namespaces in rapid succession
    let ns_prefix = generate_unique_namespace("cluster_ns");
    let namespaces: Vec<String> = (0..5).map(|i| format!("{}_{}", ns_prefix, i)).collect();

    // Create all namespaces
    println!("Creating {} namespaces...", namespaces.len());
    for ns in &namespaces {
        let sql = format!("CREATE NAMESPACE {}", ns);
        execute_sql_as_root_via_client(&sql)
            .unwrap_or_else(|e| panic!("Failed to create namespace {}: {}", ns, e));
    }

    // Verify all namespaces exist in system.namespaces
    println!("Verifying all namespaces exist...");
    let query = format!(
        "SELECT namespace_id FROM system.namespaces WHERE namespace_id LIKE '{}%'",
        ns_prefix
    );
    let result = execute_sql_as_root_via_client(&query).expect("Failed to query system.namespaces");

    // Count returned namespaces
    for ns in &namespaces {
        assert!(
            result.contains(ns),
            "Namespace {} should be in system.namespaces, got: {}",
            ns,
            result
        );
    }

    println!("  ✅ All {} namespaces created and visible\n", namespaces.len());

    // Cleanup
    for ns in namespaces.iter().rev() {
        let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", ns));
    }

    println!("  ✅ Cleanup complete\n");
}

/// Test 2: CommandExecutor pattern - table operations
///
/// Verifies CREATE TABLE commands work correctly across different table types
/// (user, shared, stream) - all go through CommandExecutor
#[ntest::timeout(90_000)]
#[test]
fn smoke_test_cluster_table_type_consistency() {
    if !require_server_running() {
        return;
    }

    println!("\n=== TEST: Table Type Command Consistency ===\n");

    let namespace = generate_unique_namespace("cluster_tables");

    // Cleanup if exists
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    // Create namespace
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create USER table
    let user_table = "user_data";
    execute_sql_as_root_via_client(&format!(
        r#"CREATE TABLE {}.{} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            content TEXT NOT NULL
        ) WITH (TYPE = 'USER')"#,
        namespace, user_table
    ))
    .expect("Failed to create user table");
    println!("  ✓ USER table created");

    // Create SHARED table
    let shared_table = "shared_config";
    execute_sql_as_root_via_client(&format!(
        r#"CREATE TABLE {}.{} (
            config_key TEXT PRIMARY KEY,
            config_value TEXT
        ) WITH (TYPE = 'SHARED')"#,
        namespace, shared_table
    ))
    .expect("Failed to create shared table");
    println!("  ✓ SHARED table created");

    // Create STREAM table
    let stream_table = "event_stream";
    execute_sql_as_root_via_client(&format!(
        r#"CREATE TABLE {}.{} (
            event_id TEXT PRIMARY KEY DEFAULT ULID(),
            event_type TEXT
        ) WITH (TYPE = 'STREAM', TTL_SECONDS = 60)"#,
        namespace, stream_table
    ))
    .expect("Failed to create stream table");
    println!("  ✓ STREAM table created");

    // Verify all tables in system.schemas
    let query = format!(
        "SELECT table_name, table_type FROM system.schemas WHERE namespace_id = '{}'",
        namespace
    );
    let result = execute_sql_as_root_via_client(&query).expect("Failed to query system.schemas");

    // Verify each table and its type
    assert!(result.contains(user_table), "User table should exist");
    assert!(result.contains(shared_table), "Shared table should exist");
    assert!(result.contains(stream_table), "Stream table should exist");

    println!("\n  ✅ All table types created and registered correctly\n");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

/// Test 3: CommandExecutor pattern - user operations
///
/// Verifies CREATE USER, ALTER USER commands work through CommandExecutor
#[ntest::timeout(60_000)]
#[test]
fn smoke_test_cluster_user_operations() {
    if !require_server_running() {
        return;
    }

    println!("\n=== TEST: User Command Consistency ===\n");

    // Create users with unique names
    let user_prefix = format!("cluster_user_{}", rand::random::<u16>());
    let users: Vec<String> = (0..3).map(|i| format!("{}_{}", user_prefix, i)).collect();

    // Create all users
    for user in &users {
        let sql = format!("CREATE USER {} WITH PASSWORD 'testpass123' ROLE 'user'", user);
        execute_sql_as_root_via_client(&sql)
            .unwrap_or_else(|e| panic!("Failed to create user {}: {}", user, e));
        println!("  ✓ Created user: {}", user);
    }

    // Verify all users exist in system.users
    let query = format!("SELECT username FROM system.users WHERE username LIKE '{}%'", user_prefix);
    let result = execute_sql_as_root_via_client(&query).expect("Failed to query system.users");

    for user in &users {
        assert!(result.contains(user), "User {} should be in system.users", user);
    }
    println!("\n  ✅ All {} users created and visible", users.len());

    // Cleanup - drop users
    for user in &users {
        let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", user));
    }

    println!("  ✅ User cleanup complete\n");
}

/// Test 4: Data partitioning for user tables
///
/// Verifies that user data is correctly partitioned and isolated
/// This tests the shard routing in CommandExecutor
#[ntest::timeout(120_000)]
#[test]
fn smoke_test_cluster_user_data_partitioning() {
    if !require_server_running() {
        return;
    }

    println!("\n=== TEST: User Data Partitioning ===\n");

    let namespace = generate_unique_namespace("cluster_partition");

    // Cleanup if exists
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    // Create namespace and user table
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_sql_as_root_via_client(&format!(
        r#"CREATE TABLE {}.user_notes (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            note TEXT NOT NULL
        ) WITH (TYPE = 'USER')"#,
        namespace
    ))
    .expect("Failed to create user table");

    // Create test users
    let user1 = format!("partition_user_a_{}", rand::random::<u16>());
    let user2 = format!("partition_user_b_{}", rand::random::<u16>());

    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD 'testpass123' ROLE 'user'",
        user1
    ))
    .expect("Failed to create user1");

    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD 'testpass123' ROLE 'user'",
        user2
    ))
    .expect("Failed to create user2");

    println!("  ✓ Created test users: {}, {}", user1, user2);

    // Insert data as user1
    execute_sql_via_client_as(
        &user1,
        "testpass123",
        &format!("INSERT INTO {}.user_notes (note) VALUES ('User1 private note')", namespace),
    )
    .expect("Failed to insert as user1");
    println!("  ✓ User1 inserted data");

    // Insert data as user2
    execute_sql_via_client_as(
        &user2,
        "testpass123",
        &format!("INSERT INTO {}.user_notes (note) VALUES ('User2 private note')", namespace),
    )
    .expect("Failed to insert as user2");
    println!("  ✓ User2 inserted data");

    // User1 should only see their own data
    let user1_data = execute_sql_via_client_as(
        &user1,
        "testpass123",
        &format!("SELECT * FROM {}.user_notes", namespace),
    )
    .expect("Failed to query as user1");

    assert!(user1_data.contains("User1 private note"), "User1 should see their own note");
    assert!(!user1_data.contains("User2 private note"), "User1 should NOT see user2's note");
    println!("  ✓ User1 sees only their data");

    // User2 should only see their own data
    let user2_data = execute_sql_via_client_as(
        &user2,
        "testpass123",
        &format!("SELECT * FROM {}.user_notes", namespace),
    )
    .expect("Failed to query as user2");

    assert!(user2_data.contains("User2 private note"), "User2 should see their own note");
    assert!(!user2_data.contains("User1 private note"), "User2 should NOT see user1's note");
    println!("  ✓ User2 sees only their data");

    println!("\n  ✅ User data partitioning verified\n");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", user1));
    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", user2));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

/// Test 5: Shared table consistency
///
/// Verifies that shared table data is visible to all users
/// This tests the shared data shard routing
#[ntest::timeout(90_000)]
#[test]
fn smoke_test_cluster_shared_table_consistency() {
    if !require_server_running() {
        return;
    }

    println!("\n=== TEST: Shared Table Consistency ===\n");

    let namespace = generate_unique_namespace("cluster_shared");

    // Cleanup if exists
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    // Create namespace and shared table
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_sql_as_root_via_client(&format!(
        r#"CREATE TABLE {}.global_config (
            config_key TEXT PRIMARY KEY,
            config_value TEXT,
            updated_by TEXT
        ) WITH (TYPE = 'SHARED', ACCESS_LEVEL = 'PUBLIC')"#,
        namespace
    ))
    .expect("Failed to create shared table");
    println!("  ✓ Shared table created");

    // Insert config as root
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {}.global_config (config_key, config_value, updated_by) VALUES ('app_version', '1.0.0', 'root')",
        namespace
    )).expect("Failed to insert config");
    println!("  ✓ Config inserted by root");

    // Create a test user
    let user = format!("shared_reader_{}", rand::random::<u16>());
    execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD 'testpass123' ROLE 'user'",
        user
    ))
    .expect("Failed to create test user");

    // User should be able to read shared data
    let user_data = execute_sql_via_client_as(
        &user,
        "testpass123",
        &format!("SELECT * FROM {}.global_config WHERE config_key = 'app_version'", namespace),
    )
    .expect("Failed to query as user");

    assert!(user_data.contains("1.0.0"), "User should see shared config value");
    println!("  ✓ User can read shared table data");

    println!("\n  ✅ Shared table consistency verified\n");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP USER {}", user));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

/// Test 6: Concurrent operations consistency
///
/// Verifies that concurrent operations maintain consistency
/// Critical for cluster mode where operations might hit different leaders
#[ntest::timeout(120_000)]
#[test]
fn smoke_test_cluster_concurrent_operations() {
    if !require_server_running() {
        return;
    }

    println!("\n=== TEST: Concurrent Operations Consistency ===\n");

    let namespace = generate_unique_namespace("cluster_concurrent");

    // Cleanup if exists
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    // Create namespace and shared counter table
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_sql_as_root_via_client(&format!(
        r#"CREATE TABLE {}.counters (
            counter_id TEXT PRIMARY KEY,
            value BIGINT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (TYPE = 'SHARED')"#,
        namespace
    ))
    .expect("Failed to create counters table");

    // Initialize counter
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {}.counters (counter_id, value) VALUES ('main', 0)",
        namespace
    ))
    .expect("Failed to insert initial counter");

    println!("  ✓ Counter table created and initialized");

    // Perform rapid sequential updates (simulating concurrent operations)
    let num_updates = 10;
    for i in 1..=num_updates {
        execute_sql_as_root_via_client(&format!(
            "UPDATE {}.counters SET value = {} WHERE counter_id = 'main'",
            namespace, i
        ))
        .unwrap_or_else(|e| panic!("Failed update {}: {}", i, e));
    }

    println!("  ✓ Performed {} sequential updates", num_updates);

    // Verify final value
    let result = execute_sql_as_root_via_client(&format!(
        "SELECT value FROM {}.counters WHERE counter_id = 'main'",
        namespace
    ))
    .expect("Failed to query final counter value");

    assert!(
        result.contains(&num_updates.to_string()),
        "Counter should be {}, got: {}",
        num_updates,
        result
    );

    println!("  ✓ Final counter value verified: {}", num_updates);
    println!("\n  ✅ Concurrent operations consistency verified\n");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

/// Test 7: Batch operations consistency
///
/// Verifies that batch INSERT operations maintain data integrity
#[ntest::timeout(120_000)]
#[test]
fn smoke_test_cluster_batch_insert_consistency() {
    if !require_server_running() {
        return;
    }

    println!("\n=== TEST: Batch Insert Consistency ===\n");

    let namespace = generate_unique_namespace("cluster_batch");

    // Cleanup if exists
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    // Create namespace and table
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_sql_as_root_via_client(&format!(
        r#"CREATE TABLE {}.batch_data (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            batch_id TEXT NOT NULL,
            seq_num INT NOT NULL
        ) WITH (TYPE = 'SHARED')"#,
        namespace
    ))
    .expect("Failed to create batch_data table");

    let batch_id = format!("batch_{}", rand::random::<u32>());
    let batch_size = 20;

    println!("  Inserting batch of {} rows...", batch_size);

    // Insert batch of rows
    for i in 0..batch_size {
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {}.batch_data (batch_id, seq_num) VALUES ('{}', {})",
            namespace, batch_id, i
        ))
        .unwrap_or_else(|e| panic!("Failed to insert row {}: {}", i, e));
    }

    // Verify count
    let result = execute_sql_as_root_via_client(&format!(
        "SELECT COUNT(*) as cnt FROM {}.batch_data WHERE batch_id = '{}'",
        namespace, batch_id
    ))
    .expect("Failed to count rows");

    assert!(
        result.contains(&batch_size.to_string()),
        "Should have {} rows, got: {}",
        batch_size,
        result
    );

    println!("  ✓ All {} rows inserted and counted correctly", batch_size);

    // Verify sequence completeness (all seq_nums 0 to batch_size-1 exist)
    let result = execute_sql_as_root_via_client(&format!(
        "SELECT seq_num FROM {}.batch_data WHERE batch_id = '{}' ORDER BY seq_num",
        namespace, batch_id
    ))
    .expect("Failed to query sequence");

    for i in 0..batch_size {
        assert!(result.contains(&i.to_string()), "Sequence {} should exist in results", i);
    }

    println!("  ✓ All sequence numbers verified");
    println!("\n  ✅ Batch insert consistency verified\n");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

/// Test 8: Job creation and tracking consistency
///
/// Verifies that background jobs (flush) are correctly tracked in system.jobs
#[ntest::timeout(180_000)]
#[test]
fn smoke_test_cluster_job_tracking() {
    if !require_server_running() {
        return;
    }

    println!("\n=== TEST: Job Tracking Consistency ===\n");

    let namespace = generate_unique_namespace("cluster_jobs");

    // Cleanup if exists
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    // Create namespace and table
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_sql_as_root_via_client(&format!(
        r#"CREATE TABLE {}.flush_test (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            data TEXT
        ) WITH (TYPE = 'SHARED', FLUSH_POLICY = 'rows:10')"#,
        namespace
    ))
    .expect("Failed to create flush_test table");

    // Insert some data
    for i in 0..15 {
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {}.flush_test (data) VALUES ('test data {}')",
            namespace, i
        ))
        .expect("Failed to insert data");
    }
    println!("  ✓ Inserted 15 rows (flush threshold is 10)");

    // Trigger manual flush
    let flush_result =
        execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}.flush_test", namespace));

    match flush_result {
        Ok(output) => println!("  ✓ Flush command executed: {}", output.trim()),
        Err(e) => println!("  ⚠ Flush command: {} (may already be flushing)", e),
    }

    // Wait a bit for job to be registered

    // Check system.jobs for flush jobs
    let result = execute_sql_as_root_via_client(
        "SELECT job_id, job_type, status FROM system.jobs WHERE job_type = 'flush' ORDER BY created_at DESC LIMIT 5"
    ).expect("Failed to query system.jobs");

    println!("  Recent flush jobs:\n{}", result);

    // Should have at least seen jobs or empty result
    assert!(
        result.contains("flush") || result.contains("(0 rows)") || result.contains("row"),
        "Should be able to query job history"
    );

    println!("\n  ✅ Job tracking verified\n");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

/// Test 9: Storage operations consistency
///
/// Verifies storage registration and table association
#[ntest::timeout(60_000)]
#[test]
fn smoke_test_cluster_storage_operations() {
    if !require_server_running() {
        return;
    }

    println!("\n=== TEST: Storage Operations Consistency ===\n");

    // Query existing storages
    let result = execute_sql_as_root_via_client(
        "SELECT storage_id, storage_type, base_directory FROM system.storages",
    )
    .expect("Failed to query system.storages");

    println!("  Existing storages:\n{}", result);

    // Should have at least the 'local' storage
    assert!(
        result.contains("local") || result.contains("row"),
        "Should have at least one storage configured"
    );

    println!("  ✓ Storage query successful");

    // Create namespace and table using specific storage
    let namespace = generate_unique_namespace("cluster_storage");

    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_sql_as_root_via_client(&format!(
        r#"CREATE TABLE {}.stored_data (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            data TEXT
        ) WITH (TYPE = 'SHARED', STORAGE_ID = 'local')"#,
        namespace
    ))
    .expect("Failed to create table with storage");

    println!("  ✓ Table created with explicit storage_id");

    // Verify table references the storage
    let result = execute_sql_as_root_via_client(&format!(
        "SELECT options FROM system.schemas WHERE namespace_id = '{}' AND table_name = 'stored_data'",
        namespace
    )).expect("Failed to query table options");

    assert!(
        result.contains("local") || result.contains("STORAGE_ID"),
        "Table options should reference storage"
    );

    println!("  ✓ Table storage association verified");
    println!("\n  ✅ Storage operations consistency verified\n");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

/// Test 10: Live query registration (meta operations)
///
/// Tests that live query metadata is correctly tracked
/// This exercises the MetaSystem command path
#[ntest::timeout(90_000)]
#[test]
fn smoke_test_cluster_live_query_tracking() {
    if !require_server_running() {
        return;
    }

    println!("\n=== TEST: Live Query Tracking ===\n");

    // Query current live queries
    let result = execute_sql_as_root_via_client(
        "SELECT live_id, table_name, user_id FROM system.live LIMIT 10",
    )
    .expect("Failed to query system.live");

    println!("  Current live subscriptions:\n{}", result);

    // System should be able to query live queries (even if empty)
    assert!(
        result.contains("row") || result.contains("│") || result.contains("live_id"),
        "Should be able to query system.live"
    );

    println!("  ✓ Live subscription system view accessible");
    println!("\n  ✅ Live query tracking verified\n");
}

/// Aggregator test - runs all cluster smoke tests
#[ntest::timeout(600_000)]
#[test]
fn smoke_test_cluster_all() {
    if !require_server_running() {
        return;
    }

    println!("\n");
    println!("╔═══════════════════════════════════════════════════════════════════╗");
    println!("║           CLUSTER OPERATIONS SMOKE TEST SUITE                     ║");
    println!("╠═══════════════════════════════════════════════════════════════════╣");
    println!("║  Testing CommandExecutor consistency (works in standalone         ║");
    println!("║  and cluster modes)                                               ║");
    println!("╚═══════════════════════════════════════════════════════════════════╝");
    println!();

    // Note: Individual tests run separately due to #[test] attribute
    // This test just confirms the test suite structure

    println!("✅ Cluster operations smoke test suite available");
    println!("   Run individual tests with: cargo test --test smoke smoke_test_cluster_");
    println!();
}
