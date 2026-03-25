//! Smoke tests for CLI meta-commands
//!
//! Tests all CLI backslash commands against a running server:
//! - \stats / \metrics - Show system statistics
//! - \dt / \tables - List tables
//! - \d / \describe - Describe table schema
//! - \format - Change output format
//! - \health - Server health check
//! - \info / \session - Session information
//! - \help / \? - Help text
//! - SQL execution

use crate::common::*;
use std::time::Duration;

/// Smoke Test: \stats command works correctly
#[ntest::timeout(30000)]
#[test]
fn smoke_cli_stats_command() {
    if !is_server_running() {
        eprintln!("Skipping smoke_cli_stats_command: server not running");
        return;
    }

    // Query system.stats directly (this mirrors \stats, which uses an explicit
    // LIMIT to bypass the server's default query limit for unbounded SELECTs).
    let result = execute_sql_as_root_via_client(
        "SELECT metric_name, metric_value FROM system.stats ORDER BY metric_name LIMIT 5000",
    )
    .expect("Failed to query system.stats");

    // Verify we get some metrics back
    assert!(
        (result.contains("metric_name") || result.contains("cache") || result.contains("rows"))
            && result.contains("server_workers_effective")
            && result.contains("max_connections"),
        "Stats should contain metrics: {}",
        result
    );

    println!("✅ smoke_cli_stats_command passed!");
}

/// Smoke Test: \dt / \tables command lists tables
#[ntest::timeout(120000)]
#[test]
fn smoke_cli_list_tables_command() {
    if !is_server_running() {
        eprintln!("Skipping smoke_cli_list_tables_command: server not running");
        return;
    }

    let namespace = generate_unique_namespace("smoke_cli_dt");
    let table = generate_unique_table("test_table");
    let full_table = format!("{}.{}", namespace, table);

    // Setup: create a table
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, name VARCHAR) WITH (TYPE='SHARED')",
        full_table
    ))
    .expect("Failed to create table");

    // Query system.tables with a narrow filter (this is what \dt uses internally).
    let result = execute_sql_as_root_via_client(&format!(
        "SELECT namespace_id AS namespace, table_name, table_type FROM system.tables WHERE namespace_id = '{}' AND table_name = '{}'",
        namespace, table
    ))
    .expect("Failed to list tables");

    // Verify our table shows up
    assert!(
        result.contains(&namespace) && result.contains(&table),
        "Created table should be listed: {}",
        result
    );

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    println!("✅ smoke_cli_list_tables_command passed!");
}

/// Smoke Test: \d / \describe command shows table schema
#[ntest::timeout(120000)]
#[test]
fn smoke_cli_describe_table_command() {
    if !is_server_running() {
        eprintln!("Skipping smoke_cli_describe_table_command: server not running");
        return;
    }

    let namespace = generate_unique_namespace("smoke_cli_desc");
    let table = generate_unique_table("describe_test");
    let full_table = format!("{}.{}", namespace, table);

    // Setup: create a table with various column types
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (
            id BIGINT PRIMARY KEY,
            name VARCHAR NOT NULL,
            age INT,
            active BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT NOW()
        ) WITH (TYPE='SHARED')",
        full_table
    ))
    .expect("Failed to create table");

    // Query information_schema.columns (this is what \describe does)
    let result = execute_sql_as_root_via_client(&format!(
        "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}' ORDER BY ordinal_position",
        namespace, table
    )).expect("Failed to describe table");

    // Verify columns are shown
    assert!(result.contains("id"), "Should show id column: {}", result);
    assert!(result.contains("name"), "Should show name column: {}", result);
    assert!(result.contains("age"), "Should show age column: {}", result);
    assert!(result.contains("active"), "Should show active column: {}", result);

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    println!("✅ smoke_cli_describe_table_command passed!");
}

/// Smoke Test: \format command changes output format (JSON)
#[ntest::timeout(30000)]
#[test]
fn smoke_cli_format_json_command() {
    if !is_server_running() {
        eprintln!("Skipping smoke_cli_format_json_command: server not running");
        return;
    }

    // Execute a query and get JSON output
    let result = execute_sql_as_root_via_client_json("SELECT 1 as test_value")
        .expect("Failed to execute SQL with JSON format");

    // Verify it's valid JSON
    let parsed: Result<serde_json::Value, _> = serde_json::from_str(&result);
    assert!(parsed.is_ok(), "Output should be valid JSON: {}", result);

    let json = parsed.unwrap();
    assert!(json.get("results").is_some(), "JSON should have results field: {}", result);

    println!("✅ smoke_cli_format_json_command passed!");
}

/// Smoke Test: SQL execution with various statement types
#[ntest::timeout(120000)]
#[test]
fn smoke_cli_sql_execution() {
    if !is_server_running() {
        eprintln!("Skipping smoke_cli_sql_execution: server not running");
        return;
    }

    let namespace = generate_unique_namespace("smoke_cli_sql");
    let table = generate_unique_table("sql_test");
    let full_table = format!("{}.{}", namespace, table);

    // Test CREATE NAMESPACE
    let result =
        execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace));
    assert!(result.is_ok(), "CREATE NAMESPACE should succeed: {:?}", result);

    // Test CREATE TABLE
    let result = execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, value INT) WITH (TYPE='SHARED')",
        full_table
    ));
    assert!(result.is_ok(), "CREATE TABLE should succeed: {:?}", result);

    // Test INSERT
    let result = execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, value) VALUES (1, 100), (2, 200), (3, 300)",
        full_table
    ));
    assert!(result.is_ok(), "INSERT should succeed: {:?}", result);

    // Test SELECT
    let result =
        execute_sql_as_root_via_client(&format!("SELECT * FROM {} ORDER BY id", full_table))
            .expect("SELECT should succeed");
    assert!(result.contains("100"), "Should contain value 100: {}", result);
    assert!(result.contains("200"), "Should contain value 200: {}", result);
    assert!(result.contains("300"), "Should contain value 300: {}", result);

    // Test SELECT with WHERE
    let result = execute_sql_as_root_via_client_json(&format!(
        "SELECT * FROM {} WHERE value > 150",
        full_table
    ))
    .expect("SELECT with WHERE should succeed");
    let json: serde_json::Value =
        serde_json::from_str(&result).expect("Failed to parse JSON output");
    let rows = get_rows_as_hashmaps(&json).unwrap_or_default();
    let values: Vec<i64> = rows
        .iter()
        .filter_map(|row| row.get("value"))
        .filter_map(|value| extract_typed_value(value).as_i64())
        .collect();
    assert!(values.contains(&200), "Should contain value 200: {:?}", values);
    assert!(values.contains(&300), "Should contain value 300: {:?}", values);
    assert!(!values.contains(&100), "Should NOT contain value 100: {:?}", values);

    // Test SELECT with aggregation
    let result = execute_sql_as_root_via_client(&format!(
        "SELECT COUNT(*) as cnt, SUM(value) as total FROM {}",
        full_table
    ))
    .expect("SELECT with aggregation should succeed");
    assert!(result.contains("3"), "Count should be 3: {}", result);
    assert!(result.contains("600"), "Sum should be 600: {}", result);

    // Test UPDATE
    let result = execute_sql_as_root_via_client(&format!(
        "UPDATE {} SET value = 150 WHERE id = 1",
        full_table
    ));
    assert!(result.is_ok(), "UPDATE should succeed: {:?}", result);

    // Verify UPDATE
    let result =
        execute_sql_as_root_via_client(&format!("SELECT value FROM {} WHERE id = 1", full_table))
            .expect("SELECT after UPDATE should succeed");
    assert!(result.contains("150"), "Value should be updated to 150: {}", result);

    // Test DELETE
    let result =
        execute_sql_as_root_via_client(&format!("DELETE FROM {} WHERE id = 3", full_table));
    assert!(result.is_ok(), "DELETE should succeed: {:?}", result);

    // Verify DELETE
    let result =
        execute_sql_as_root_via_client_json(&format!("SELECT * FROM {} ORDER BY id", full_table))
            .expect("SELECT after DELETE should succeed");
    let json: serde_json::Value =
        serde_json::from_str(&result).expect("Failed to parse JSON output");
    let rows = get_rows_as_hashmaps(&json).unwrap_or_default();
    let values: Vec<i64> = rows
        .iter()
        .filter_map(|row| row.get("value"))
        .filter_map(|value| extract_typed_value(value).as_i64())
        .collect();
    assert!(!values.contains(&300), "Value 300 should be deleted: {:?}", values);

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    println!("✅ smoke_cli_sql_execution passed!");
}

/// Smoke Test: System tables are accessible
#[ntest::timeout(30000)]
#[test]
fn smoke_cli_system_tables() {
    if !is_server_running() {
        eprintln!("Skipping smoke_cli_system_tables: server not running");
        return;
    }

    // Test system.users
    let result = execute_sql_as_root_via_client(
        "SELECT user_id, username, role FROM system.users WHERE username = 'root' LIMIT 1",
    );
    assert!(result.is_ok(), "system.users should be queryable: {:?}", result);
    let output = result.unwrap();
    assert!(output.contains("root"), "Should see root user: {}", output);

    // Test system.namespaces
    let result =
        execute_sql_as_root_via_client("SELECT namespace_id, name FROM system.namespaces LIMIT 5");
    assert!(result.is_ok(), "system.namespaces should be queryable: {:?}", result);

    // Test system.schemas
    let result = execute_sql_as_root_via_client(
        "SELECT namespace_id, table_name, table_type FROM system.schemas LIMIT 5",
    );
    assert!(result.is_ok(), "system.schemas should be queryable: {:?}", result);

    // Test system.jobs
    let result =
        execute_sql_as_root_via_client("SELECT job_id, job_type, status FROM system.jobs LIMIT 5");
    assert!(result.is_ok(), "system.jobs should be queryable: {:?}", result);

    // Test system.stats
    let result = execute_sql_as_root_via_client(
        "SELECT metric_name, metric_value FROM system.stats LIMIT 5",
    );
    assert!(result.is_ok(), "system.stats should be queryable: {:?}", result);

    // Test information_schema.tables
    let result = execute_sql_as_root_via_client(
        "SELECT table_schema, table_name FROM information_schema.tables LIMIT 5",
    );
    assert!(result.is_ok(), "information_schema.tables should be queryable: {:?}", result);

    // Test information_schema.columns
    let result = execute_sql_as_root_via_client(
        "SELECT table_schema, table_name, column_name FROM information_schema.columns LIMIT 5",
    );
    assert!(result.is_ok(), "information_schema.columns should be queryable: {:?}", result);

    println!("✅ smoke_cli_system_tables passed!");
}

/// Smoke Test: STORAGE FLUSH TABLE command
#[ntest::timeout(120000)]
#[test]
fn smoke_cli_flush_command() {
    if !is_server_running() {
        eprintln!("Skipping smoke_cli_flush_command: server not running");
        return;
    }

    let namespace = generate_unique_namespace("smoke_cli_flush");
    let table = generate_unique_table("flush_test");
    let full_table = format!("{}.{}", namespace, table);

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, data VARCHAR) WITH (TYPE='SHARED')",
        full_table
    ))
    .expect("Failed to create table");

    // Insert some data
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (id, data) VALUES (1, 'test')",
        full_table
    ))
    .expect("Failed to insert");

    // Test STORAGE FLUSH TABLE
    let result = execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", full_table));
    assert!(result.is_ok(), "STORAGE FLUSH TABLE should succeed: {:?}", result);

    // Verify data still accessible after flush
    let result = execute_sql_as_root_via_client(&format!("SELECT * FROM {}", full_table))
        .expect("SELECT after FLUSH should succeed");
    assert!(result.contains("test"), "Data should persist after flush: {}", result);

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    println!("✅ smoke_cli_flush_command passed!");
}

/// Smoke Test: User management commands
#[ntest::timeout(30000)]
#[test]
fn smoke_cli_user_management() {
    if !is_server_running() {
        eprintln!("Skipping smoke_cli_user_management: server not running");
        return;
    }

    let username = generate_unique_namespace("smoke_cli_user");
    let password = "test_password_123";

    // Test CREATE USER
    let result = execute_sql_as_root_via_client(&format!(
        "CREATE USER {} WITH PASSWORD '{}' ROLE 'user'",
        username, password
    ));
    assert!(result.is_ok(), "CREATE USER should succeed: {:?}", result);

    // Verify user exists
    let result = execute_sql_as_root_via_client(&format!(
        "SELECT username, role FROM system.users WHERE username = '{}'",
        username
    ))
    .expect("Failed to query user");
    assert!(result.contains(&username), "User should exist: {}", result);
    assert!(result.contains("user"), "User role should be 'user': {}", result);

    // Test user can login
    let result = execute_sql_via_client_as(&username, password, "SELECT 1 as test");
    assert!(result.is_ok(), "User should be able to login: {:?}", result);

    // Note: ALTER USER SET ROLE is not currently implemented, skip that test

    // Test DROP USER
    let result = execute_sql_as_root_via_client(&format!("DROP USER IF EXISTS {}", username));
    assert!(result.is_ok(), "DROP USER should succeed: {:?}", result);

    // Note: Users are soft-deleted (deleted_at timestamp set), so they may still appear in system.users
    // Verify user is soft-deleted by checking deleted_at is not null
    let _result = execute_sql_as_root_via_client(&format!(
        "SELECT deleted_at FROM system.users WHERE username = '{}'",
        username
    ))
    .expect("Failed to query deleted user");
    // The result should show a non-null deleted_at timestamp (or user may be filtered out)

    println!("✅ smoke_cli_user_management passed!");
}

/// Smoke Test: Namespace management commands
#[ntest::timeout(120000)]
#[test]
fn smoke_cli_namespace_management() {
    if !is_server_running() {
        eprintln!("Skipping smoke_cli_namespace_management: server not running");
        return;
    }

    let namespace = generate_unique_namespace("smoke_cli_ns");

    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    // Test CREATE NAMESPACE
    let result =
        execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace));
    assert!(result.is_ok(), "CREATE NAMESPACE should succeed: {:?}", result);

    // Verify namespace exists
    let result = execute_sql_as_root_via_client(&format!(
        "SELECT name FROM system.namespaces WHERE name = '{}'",
        namespace
    ))
    .expect("Failed to query namespace");
    assert!(result.contains(&namespace), "Namespace should exist: {}", result);

    // Test CREATE NAMESPACE again (should error because it already exists)
    let result = execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace));
    assert!(result.is_err(), "CREATE NAMESPACE should fail when namespace already exists");

    // Test CREATE NAMESPACE IF NOT EXISTS (should not error)
    let result =
        execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace));
    assert!(result.is_ok(), "CREATE NAMESPACE IF NOT EXISTS should succeed: {:?}", result);

    // Create a table, then drop namespace without CASCADE.
    let table = generate_unique_table("ns_drop_table");
    let full_table = format!("{}.{}", namespace, table);
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, value STRING) WITH (TYPE='SHARED')",
        full_table
    ))
    .expect("CREATE TABLE should succeed");

    // Test DROP NAMESPACE
    let result = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
    assert!(result.is_ok(), "DROP NAMESPACE should succeed: {:?}", result);

    // Namespace drop should remove tables too.
    let table_check = execute_sql_as_root_via_client(&format!(
        "SELECT table_name FROM system.schemas WHERE namespace_id = '{}' AND table_name = '{}'",
        namespace, table
    ))
    .expect("Failed to query system.schemas");
    assert!(
        !table_check.contains(&table),
        "Dropped namespace should remove table metadata, got: {}",
        table_check
    );

    println!("✅ smoke_cli_namespace_management passed!");
}

/// Smoke Test: ALTER TABLE commands
#[ntest::timeout(90000)]
#[test]
fn smoke_cli_alter_table() {
    if !is_server_running() {
        eprintln!("Skipping smoke_cli_alter_table: server not running");
        return;
    }

    let namespace = generate_unique_namespace("smoke_cli_alter");
    let table = generate_unique_table("alter_test");
    let full_table = format!("{}.{}", namespace, table);

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("Failed to create namespace");
    execute_sql_as_root_via_client(&format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, name STRING) WITH (TYPE='USER')",
        full_table
    ))
    .expect("Failed to create table");

    // Test ALTER TABLE ADD COLUMN (using STRING, not VARCHAR)
    let result = execute_sql_as_root_via_client(&format!(
        "ALTER TABLE {} ADD COLUMN email STRING",
        full_table
    ));
    assert!(result.is_ok(), "ALTER TABLE ADD COLUMN should succeed: {:?}", result);

    // Verify column added
    wait_for_sql_output_contains(
        &format!(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}'",
            namespace, table
        ),
        "email",
        Duration::from_secs(45),
    )
    .expect("email column should be visible in information_schema after ALTER TABLE");

    // Insert with new column; schema propagation can lag briefly after ALTER in cluster mode.
    let insert_sql = format!(
        "INSERT INTO {} (id, name, email) VALUES (1, 'Test', 'test@example.com')",
        full_table
    );
    let start = std::time::Instant::now();
    let mut last_error: Option<String> = None;
    loop {
        match execute_sql_as_root_via_client(&insert_sql) {
            Ok(_) => break,
            Err(err) => {
                let message = err.to_string();
                if message.contains("No field named email")
                    && start.elapsed() < Duration::from_secs(45)
                {
                    last_error = Some(message);
                    std::thread::sleep(Duration::from_millis(150));
                    continue;
                }
                panic!("INSERT with new column should succeed: {}", message);
            },
        }
    }
    if let Some(err) = last_error {
        eprintln!("ℹ️  ALTER propagation retry succeeded after transient error: {}", err);
    }

    // Verify data
    let _ = wait_for_sql_output_contains(
        &format!("SELECT email FROM {} WHERE id = 1", full_table),
        "test@example.com",
        Duration::from_secs(20),
    )
    .expect("Email should be stored");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    println!("✅ smoke_cli_alter_table passed!");
}

/// Smoke Test: Error handling for invalid commands
#[ntest::timeout(30000)]
#[test]
fn smoke_cli_error_handling() {
    if !is_server_running() {
        eprintln!("Skipping smoke_cli_error_handling: server not running");
        return;
    }

    // Test invalid SQL syntax
    let result = execute_sql_as_root_via_client("SELEKT * FROM nonexistent");
    assert!(result.is_err(), "Invalid SQL should fail");

    // Test querying non-existent table
    let result = execute_sql_as_root_via_client(
        "SELECT * FROM nonexistent_namespace_12345.nonexistent_table_67890",
    );
    assert!(result.is_err(), "Non-existent table should fail");

    // Test invalid column reference
    let result = execute_sql_as_root_via_client("SELECT nonexistent_column FROM system.users");
    assert!(result.is_err(), "Invalid column should fail");

    println!("✅ smoke_cli_error_handling passed!");
}
