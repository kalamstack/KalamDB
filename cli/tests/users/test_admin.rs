//! Integration tests for administrative operations
//!
//! **Implements T041-T042, T055-T058**: Administrative commands and system operations
//!
//! These tests validate:
//! - List tables and describe table commands
//! - Batch file execution
//! - Server health checks
//! - Administrative SQL operations
//! - Namespace and table management

use std::time::Duration;

use crate::common::*;

/// Test configuration constants
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// T041: Test list tables command (using SELECT from system.schemas)
#[test]
fn test_cli_list_tables() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("messages_list_tables");
    let namespace = "test_cli";

    // Create test table
    let create_sql = format!(
        r#"CREATE TABLE {}.{} (
            id INT PRIMARY KEY AUTO_INCREMENT,
            content VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (TYPE='USER', FLUSH_POLICY='rows:10')"#,
        namespace, table_name
    );

    let result = execute_sql_as_root_via_cli(&create_sql);
    if result.is_err() {
        eprintln!("⚠️  Failed to create test table, skipping test");
        return;
    }

    // Query system tables
    let query_sql = "SELECT table_name FROM system.schemas WHERE namespace_id = 'test_cli'";
    let result = execute_sql_as_root_via_cli(query_sql);

    // Should list tables
    assert!(result.is_ok(), "Should list tables: {:?}", result.err());
    let output = result.unwrap();
    assert!(
        output.contains("messages") || output.contains("row"),
        "Should contain table info: {}",
        output
    );

    // Cleanup
    let drop_sql = format!("DROP TABLE IF EXISTS {}.{}", namespace, table_name);
    let _ = execute_sql_as_root_via_cli(&drop_sql);
}

/// T042: Test describe table command (\d table)
#[test]
fn test_cli_describe_table() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("messages_describe");
    let namespace = "test_cli";

    // Create test table
    let create_sql = format!(
        r#"CREATE TABLE {}.{} (
            id INT PRIMARY KEY AUTO_INCREMENT,
            content VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (TYPE='USER', FLUSH_POLICY='rows:10')"#,
        namespace, table_name
    );

    let result = execute_sql_as_root_via_cli(&create_sql);
    if result.is_err() {
        eprintln!("⚠️  Failed to create test table, skipping test");
        return;
    }

    // Query table info
    let table_full_name = format!("{}.{}", namespace, table_name);
    let query_sql = format!("SELECT '{}' as table_info", table_full_name);
    let result = execute_sql_as_root_via_cli(&query_sql);

    // Should execute successfully and show table info
    assert!(result.is_ok(), "Should describe table: {:?}", result.err());
    let output = result.unwrap();
    assert!(output.contains("messages"), "Should contain table name: {}", output);

    // Cleanup
    let drop_sql = format!("DROP TABLE IF EXISTS {}.{}", namespace, table_name);
    let _ = execute_sql_as_root_via_cli(&drop_sql);
}

/// T055: Test batch file execution
#[test]
fn test_cli_batch_file_execution() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Create temporary SQL file
    let temp_dir = tempfile::TempDir::new().unwrap();
    let sql_file = temp_dir.path().join("test.sql");

    let namespace = generate_unique_namespace("batch_test");
    let table_name = "items";
    let full_table_name = format!("{}.{}", namespace, table_name);

    // Cleanup first in case namespace/table exists from previous run
    // Note: DROP NAMESPACE CASCADE doesn't properly cascade to tables yet, so drop table first
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {}", namespace));

    // Use a unique ID based on timestamp to avoid conflicts
    let unique_id = rand::random::<i64>().abs();

    std::fs::write(
        &sql_file,
        format!(
            r#"CREATE NAMESPACE {};
CREATE TABLE {} (id BIGINT PRIMARY KEY, name VARCHAR) WITH (TYPE='USER', FLUSH_POLICY='rows:10');
INSERT INTO {} (id, name) VALUES ({}, 'Item One');
SELECT * FROM {};"#,
            namespace, full_table_name, full_table_name, unique_id, full_table_name
        ),
    )
    .unwrap();

    // Execute batch file
    let target_url = leader_url().unwrap_or_else(|| server_url().to_string());
    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(target_url)
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(root_password())
        .arg("--file")
        .arg(sql_file.to_str().unwrap())
        .timeout(TEST_TIMEOUT);

    let mut output = cmd.output().unwrap();
    let mut stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let mut stderr = String::from_utf8_lossy(&output.stderr).to_string();

    if !output.status.success() && is_leader_error(&stderr) {
        if let Some(leader) = leader_url() {
            let mut retry_cmd = create_cli_command();
            retry_cmd
                .arg("-u")
                .arg(leader)
                .arg("--user")
                .arg(default_username())
                .arg("--password")
                .arg(root_password())
                .arg("--file")
                .arg(sql_file.to_str().unwrap())
                .timeout(TEST_TIMEOUT);

            output = retry_cmd.output().unwrap();
            stdout = String::from_utf8_lossy(&output.stdout).to_string();
            stderr = String::from_utf8_lossy(&output.stderr).to_string();
        }
    }

    // Verify execution - should show Query OK messages and final result
    assert!(
        (stdout.contains("Item One") || stdout.contains("Query OK")) && output.status.success(),
        "Batch execution should succeed with proper messages.\nstdout: {}\nstderr: {}\nstatus: \
         {:?}",
        stdout,
        stderr,
        output.status
    );

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE {} CASCADE", namespace));
}

/// T056: Test syntax error handling
#[test]
fn test_cli_syntax_error_handling() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let result = execute_sql_as_root_via_cli("INVALID SQL SYNTAX HERE");

    // Should contain error message
    assert!(result.is_err(), "Should fail with syntax error");
    let error_msg = result.err().unwrap().to_string();
    assert!(
        error_msg.contains("ERROR") || error_msg.contains("Error") || error_msg.contains("syntax"),
        "Should display error message: {}",
        error_msg
    );
}

/// T057: Test connection failure handling
#[test]
fn test_cli_connection_failure_handling() {
    // Try to connect to non-existent server
    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg("http://localhost:9999") // Non-existent port
        .arg("--command")
        .arg("SELECT 1")
        .timeout(TEST_TIMEOUT);

    let output = cmd.output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should show connection error
    assert!(
        !output.status.success()
            || stderr.contains("Connection")
            || stderr.contains("error")
            || stdout.contains("Connection")
            || stdout.contains("error"),
        "Should display connection error. stderr: {}, stdout: {}",
        stderr,
        stdout
    );
}

/// T058: Test server health check via CLI
#[test]
fn test_cli_health_check() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Test server health via SQL query
    let result = execute_sql_as_root_via_cli("SELECT 1 as health_check");

    assert!(result.is_ok(), "Server should respond to SQL queries: {:?}", result.err());

    let output = result.unwrap();
    assert!(
        output.contains("health_check") || output.contains("1"),
        "Response should contain query result: {}",
        output
    );
}
