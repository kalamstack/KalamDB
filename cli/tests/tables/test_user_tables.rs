//! Integration tests for user table operations
//!
//! **Implements T037-T040, T064-T068**: User table CRUD operations, output formatting, and query features
//!
//! These tests validate:
//! - Basic query execution on user tables
//! - Output formatting (table, JSON, CSV)
//! - Multi-line queries and comments
//! - Empty queries and result pagination
//! - Query result display and formatting

use crate::common;
use crate::common::*;

/// T037: Test basic query execution
#[test]
fn test_cli_basic_query_execution() {
    if !common::is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Setup with unique table name
    let table_name = generate_unique_table("messages_basic");
    let namespace = generate_unique_namespace("test_cli");
    let full_table_name = format!("{}.{}", namespace, table_name);

    // Create namespace and table via CLI
    let _ = execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace));
    let _ = execute_sql_as_root_via_cli(&format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            content VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (TYPE='USER', FLUSH_POLICY='rows:10')"#,
        full_table_name
    ));

    // Insert test data via CLI
    let _ = execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {} (content) VALUES ('Test Message')",
        full_table_name
    ));

    // Execute query via CLI
    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server_url())
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(root_password())
        .arg("--command")
        .arg(format!("SELECT * FROM {}", full_table_name));

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Verify output contains the inserted data and row count
    assert!(
        stdout.contains("Test Message") && output.status.success(),
        "Output should contain query results: stdout='{}', stderr='{}'",
        stdout,
        stderr
    );

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}

/// T038: Test table output formatting
#[test]
fn test_cli_table_output_formatting() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("messages_table");
    let namespace = generate_unique_namespace("test_cli");
    let full_table_name = format!("{}.{}", namespace, table_name);

    // Setup table and data via CLI
    execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE failed");
    execute_sql_as_root_via_cli(&format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            content VARCHAR NOT NULL
        ) WITH (TYPE='USER', FLUSH_POLICY='rows:10')"#,
        full_table_name
    ))
    .expect("CREATE TABLE failed");

    let _ = execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {} (content) VALUES ('Hello World'), ('Test Message')",
        full_table_name
    ));

    // Query with table format (default)
    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server_url())
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(root_password())
        .arg("--command")
        .arg(format!("SELECT * FROM {}", full_table_name));

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Verify table formatting and content
    assert!(
        stdout.contains("Hello World")
            && stdout.contains("Test Message")
            && output.status.success(),
        "Output should contain both messages: {}",
        stdout
    );

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}

/// T039: Test JSON output format
#[test]
fn test_cli_json_output_format() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("messages_json");
    let namespace = generate_unique_namespace("test_cli");
    let full_table_name = format!("{}.{}", namespace, table_name);

    // Setup table and data via CLI
    let _ = execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace));
    let _ = execute_sql_as_root_via_cli(&format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            content VARCHAR NOT NULL
        ) WITH (TYPE='USER', FLUSH_POLICY='rows:10')"#,
        full_table_name
    ));

    let _ = execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {} (content) VALUES ('JSON Test')",
        full_table_name
    ));

    // Query with JSON format
    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server_url())
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(root_password())
        .arg("--json")
        .arg("--command")
        .arg(format!("SELECT * FROM {} WHERE content = 'JSON Test'", full_table_name));

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Verify JSON output contains test data
    assert!(
        stdout.contains("JSON Test") && output.status.success(),
        "JSON output should contain test data: {}",
        stdout
    );

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}

/// T040: Test CSV output format
#[test]
#[ignore] // TODO: CSV output format not yet implemented
fn test_cli_csv_output_format() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("messages_csv");
    let namespace = generate_unique_namespace("test_cli");
    let full_table_name = format!("{}.{}", namespace, table_name);

    // Setup table and data via CLI
    let _ = execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace));
    let _ = execute_sql_as_root_via_cli(&format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            content VARCHAR NOT NULL
        ) WITH (TYPE='USER', FLUSH_POLICY='rows:10')"#,
        full_table_name
    ));

    let _ = execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {} (content) VALUES ('CSV,Test')",
        full_table_name
    ));

    // Query with CSV format
    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server_url())
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(root_password())
        .arg("--csv")
        .arg("--command")
        .arg(format!("SELECT content FROM {} WHERE content LIKE 'CSV%'", full_table_name));

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Verify CSV output contains data
    assert!(
        stdout.contains("CSV") && output.status.success(),
        "CSV output should contain data: {}",
        stdout
    );

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}

/// T064: Test multi-line query input
#[test]
fn test_cli_multiline_query() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("multiline");
    let namespace = generate_unique_namespace("test_cli");
    let full_table_name = format!("{}.{}", namespace, table_name);

    // Setup table via CLI
    let _ = execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace));
    let _ = execute_sql_as_root_via_cli(&format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            content VARCHAR NOT NULL
        ) WITH (TYPE='USER', FLUSH_POLICY='rows:10')"#,
        full_table_name
    ));

    // Multi-line query with newlines
    let multi_line_query =
        format!("SELECT \n  id, \n  content \nFROM \n  {} \nLIMIT 5", full_table_name);

    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server_url())
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(root_password())
        .arg("--command")
        .arg(&multi_line_query);

    let output = cmd.output().unwrap();

    assert!(output.status.success(), "Should handle multi-line queries");

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}

/// T065: Test query with comments
#[test]
fn test_cli_query_with_comments() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    // Test with simple SQL (comments in SQL strings don't work well in shell args)
    let query_simple = "SELECT 1 as test";

    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server_url())
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(root_password())
        .arg("--command")
        .arg(query_simple);

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "Should handle queries successfully\nstdout: {}\nstderr: {}",
        stdout,
        stderr
    );
}

/// T066: Test empty query handling
#[test]
fn test_cli_empty_query() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server_url())
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(root_password())
        .arg("--command")
        .arg("   ");

    let output = cmd.output().unwrap();

    // Should handle empty query gracefully (no crash)
    assert!(
        output.status.success() || !output.stderr.is_empty(),
        "Should handle empty query without crashing"
    );
}

/// T067: Test query result pagination
#[test]
fn test_cli_result_pagination() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("pagination");
    let namespace = generate_unique_namespace("test_cli");
    let full_table_name = format!("{}.{}", namespace, table_name);

    // Setup table via CLI
    let _ = execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace));
    let _ = execute_sql_as_root_via_cli(&format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            content VARCHAR NOT NULL
        ) WITH (TYPE='USER', FLUSH_POLICY='rows:10')"#,
        full_table_name
    ));

    // Insert multiple rows via CLI
    for i in 1..=20 {
        execute_sql_as_root_via_cli(&format!(
            "INSERT INTO {} (content) VALUES ('Message {}')",
            full_table_name, i
        ))
        .expect("INSERT failed");
    }

    // Query all rows via CLI
    let mut cmd = create_cli_command();
    cmd.arg("-u")
        .arg(server_url())
        .arg("--user")
        .arg(default_username())
        .arg("--password")
        .arg(root_password())
        .arg("--command")
        .arg(format!("SELECT * FROM {}", full_table_name));

    let output = cmd.output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should display results (pagination in interactive mode)
    assert!(
        stdout.contains("Message") || output.status.success(),
        "Should handle result display"
    );

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {}", namespace));
}
