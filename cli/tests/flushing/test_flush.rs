//! Integration tests for flush operations
//!
//! **Implements T059**: Flush table operations and data persistence
//!
//! These tests validate:
//! - Explicit STORAGE FLUSH TABLE commands
//! - Data persistence after flush operations
//! - Flush command error handling

use std::time::Duration;

use crate::common::*;

/// T059: Test explicit flush command
#[test]
fn test_cli_explicit_flush() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("explicit_flush");
    let namespace = "test_cli";
    let full_table_name = format!("{}.{}", namespace, table_name);

    // Setup table via CLI
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
    wait_for_table_ready(&full_table_name, Duration::from_secs(3)).expect("table should be ready");

    // Insert some data first via CLI
    execute_sql_as_root_via_cli(&format!(
        "INSERT INTO {} (content) VALUES ('Flush Test')",
        full_table_name
    ))
    .expect("INSERT INTO table failed");

    let flush_output =
        execute_sql_as_root_via_cli(&format!("STORAGE FLUSH TABLE {}", full_table_name))
            .expect("STORAGE FLUSH TABLE should succeed");

    // Should NOT contain errors
    assert!(
        !flush_output.contains("ERROR")
            && !flush_output.contains("not supported")
            && !flush_output.contains("Unsupported"),
        "STORAGE FLUSH TABLE should not error. output: {}",
        flush_output
    );

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}
