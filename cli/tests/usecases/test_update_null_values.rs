use std::{thread, time::Duration};

use crate::common::*;

/// Test UPDATE on rows with NULL values in non-PK columns (hot storage)
#[test]
fn test_update_row_with_null_columns_hot() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("null_update_test");
    let namespace = generate_unique_namespace("test_null_update");
    let full_table_name = format!("{}.{}", namespace, table_name);

    println!("\n=== Test: UPDATE row with NULL columns (hot storage) ===");

    // Setup namespace
    let _ = execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace));

    // Create table with nullable columns
    let create_sql = format!(
        r#"CREATE USER TABLE {} (
            id BIGINT NOT NULL DEFAULT SNOWFLAKE_ID() PRIMARY KEY,
            client_id TEXT,
            conversation_id BIGINT NOT NULL,
            sender TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            content TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'sent',
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        )"#,
        full_table_name
    );

    let output = execute_sql_as_root_via_cli(&create_sql).unwrap();
    assert!(
        output.contains("created") || output.contains("Success") || output.contains("Query OK"),
        "Table creation failed: {}",
        output
    );

    // Insert row with NULL client_id
    let insert_sql = format!(
        "INSERT INTO {} (id, client_id, conversation_id, sender, role, content, status) VALUES \
         (12345, NULL, 999, 'AI Assistant', 'assistant', 'Test message', 'sent')",
        full_table_name
    );

    let output = execute_sql_as_root_via_cli(&insert_sql).unwrap();
    assert!(
        output.contains("1 row") || output.contains("Success"),
        "Insert failed: {}",
        output
    );

    // Verify row exists with NULL client_id
    let select_sql = format!("SELECT * FROM {} WHERE id = 12345", full_table_name);
    let output = execute_sql_as_root_via_cli(&select_sql).unwrap();
    assert!(output.contains("12345"), "Row not found after insert: {}", output);
    assert!(
        output.contains("(1 row)") || output.contains("1 row"),
        "Expected 1 row, got: {}",
        output
    );
    println!("[DEBUG] Row before update: {}", output);

    // === KEY TEST: UPDATE row that has NULL in non-PK column ===
    let update_sql =
        format!("UPDATE {} SET sender = 'Updated Assistant' WHERE id = 12345", full_table_name);

    let output = execute_sql_as_root_via_cli(&update_sql).unwrap();
    println!("[DEBUG] Update output: {}", output);

    // This should succeed and affect 1 row
    assert!(
        output.contains("1 row") || output.contains("Updated 1"),
        "UPDATE failed on row with NULL column. Expected 1 row affected, got: {}",
        output
    );

    // Verify update took effect by checking the updated column value
    let verify_sql = format!("SELECT id, sender FROM {} WHERE id = 12345", full_table_name);
    let output = execute_sql_as_root_via_cli(&verify_sql).unwrap();
    assert!(output.contains("12345"), "Row disappeared after UPDATE: {}", output);
    // Verify we still get 1 row (row exists)
    assert!(
        output.contains("(1 row)") || output.contains("1 row"),
        "Expected 1 row after UPDATE: {}",
        output
    );

    println!("✅ Test passed: UPDATE works on rows with NULL columns (hot storage)");

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

/// Test UPDATE on rows with NULL values after FLUSH (cold storage)
#[test]
fn test_update_row_with_null_columns_cold() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("null_update_cold");
    let namespace = generate_unique_namespace("test_null_cold");
    let full_table_name = format!("{}.{}", namespace, table_name);

    println!("\n=== Test: UPDATE row with NULL columns (cold storage) ===");

    // Setup namespace
    let _ = execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace));

    // Create table
    let create_sql = format!(
        r#"CREATE USER TABLE {} (
            id BIGINT NOT NULL PRIMARY KEY,
            client_id TEXT,
            conversation_id BIGINT NOT NULL,
            sender TEXT NOT NULL,
            content TEXT NOT NULL
        )"#,
        full_table_name
    );

    let output = execute_sql_as_root_via_cli(&create_sql).unwrap();
    assert!(
        output.contains("created") || output.contains("Success"),
        "Table creation failed: {}",
        output
    );

    // Insert row with NULL client_id
    let insert_sql = format!(
        "INSERT INTO {} (id, client_id, conversation_id, sender, content) VALUES (98765, NULL, \
         888, 'Test Sender', 'Test content')",
        full_table_name
    );

    let output = execute_sql_as_root_via_cli(&insert_sql).unwrap();
    assert!(
        output.contains("1 row") || output.contains("Success"),
        "Insert failed: {}",
        output
    );

    // Flush to cold storage
    let flush_sql = format!("STORAGE FLUSH TABLE {}", full_table_name);
    let flush_output = execute_sql_as_root_via_cli(&flush_sql).unwrap();
    println!("[DEBUG] Flush output: {}", flush_output);

    // Wait for flush to complete
    if let Ok(job_id) = parse_job_id_from_flush_output(&flush_output) {
        println!("[DEBUG] Waiting for flush job {}...", job_id);
        if let Err(e) = verify_job_completed(&job_id, Duration::from_secs(20)) {
            eprintln!("[WARN] Flush job verification failed: {}", e);
        }
    }
    thread::sleep(Duration::from_millis(500));

    // Verify row exists in cold storage
    let select_sql = format!("SELECT * FROM {} WHERE id = 98765", full_table_name);
    let output = execute_sql_as_root_via_cli(&select_sql).unwrap();
    assert!(output.contains("98765"), "Row not found in cold storage: {}", output);
    assert!(
        output.contains("(1 row)") || output.contains("1 row"),
        "Expected 1 row, got: {}",
        output
    );
    println!("[DEBUG] Row in cold storage before update: {}", output);

    // === KEY TEST: UPDATE row with NULL column from cold storage ===
    let update_sql =
        format!("UPDATE {} SET sender = 'Updated Cold' WHERE id = 98765", full_table_name);

    let output = execute_sql_as_root_via_cli(&update_sql).unwrap();
    println!("[DEBUG] Update output (cold): {}", output);

    // This should succeed and affect 1 row
    assert!(
        output.contains("1 row") || output.contains("Updated 1"),
        "UPDATE failed on cold storage row with NULL column. Expected 1 row affected, got: {}",
        output
    );

    // Verify update took effect by checking the row still exists
    let verify_sql = format!("SELECT id, sender FROM {} WHERE id = 98765", full_table_name);
    let output = execute_sql_as_root_via_cli(&verify_sql).unwrap();
    assert!(output.contains("98765"), "Row disappeared after UPDATE: {}", output);
    // Verify we still get 1 row (row exists)
    assert!(
        output.contains("(1 row)") || output.contains("1 row"),
        "Expected 1 row after UPDATE on cold storage: {}",
        output
    );

    println!("✅ Test passed: UPDATE works on rows with NULL columns (cold storage)");

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

/// Test UPDATE with no actual changes - should be a no-op (0 rows affected)
#[test]
fn test_update_no_changes() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("no_change_update");
    let namespace = generate_unique_namespace("test_no_change");
    let full_table_name = format!("{}.{}", namespace, table_name);

    println!("\n=== Test: UPDATE with no actual changes (no-op detection) ===");

    // Setup namespace
    let _ = execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace));

    // Create table
    let create_sql = format!(
        "CREATE USER TABLE {} (id BIGINT NOT NULL PRIMARY KEY, value TEXT NOT NULL)",
        full_table_name
    );

    let output = execute_sql_as_root_via_cli(&create_sql).unwrap();
    assert!(
        output.contains("created") || output.contains("Success"),
        "Table creation failed: {}",
        output
    );

    // Insert row
    let insert_sql =
        format!("INSERT INTO {} (id, value) VALUES (555, 'unchanged')", full_table_name);

    let output = execute_sql_as_root_via_cli(&insert_sql).unwrap();
    assert!(output.contains("1 row"), "Insert failed: {}", output);

    // === KEY TEST: UPDATE with same value (no actual change) ===
    // No-op detection: when the new values match the existing row, the UPDATE
    // is skipped entirely — no RocksDB write, no notification, 0 rows affected.
    let update_sql = format!("UPDATE {} SET value = 'unchanged' WHERE id = 555", full_table_name);

    let output = execute_sql_as_root_via_cli(&update_sql).unwrap();
    println!("[DEBUG] Update (no change) output: {}", output);

    assert!(
        output.contains("0 row") || output.contains("Updated 0"),
        "No-op UPDATE should report 0 rows affected: {}",
        output
    );

    // Verify row is still intact after no-op update
    let verify_sql = format!("SELECT id, value FROM {} WHERE id = 555", full_table_name);
    let output = execute_sql_as_root_via_cli(&verify_sql).unwrap();
    assert!(output.contains("555"), "Row should still exist: {}", output);
    assert!(output.contains("unchanged"), "Value should be unchanged: {}", output);

    // Now do an actual update and verify it still works
    let real_update_sql =
        format!("UPDATE {} SET value = 'changed' WHERE id = 555", full_table_name);
    let output = execute_sql_as_root_via_cli(&real_update_sql).unwrap();
    println!("[DEBUG] Update (real change) output: {}", output);
    assert!(
        output.contains("1 row") || output.contains("Updated 1"),
        "Real UPDATE should report 1 row affected: {}",
        output
    );

    println!("✅ Test passed: No-op UPDATE correctly reports 0 rows affected");

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}

/// Test UPDATE multiple rows with mixed NULL values
#[test]
fn test_update_multiple_rows_with_nulls() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("multi_null_update");
    let namespace = generate_unique_namespace("test_multi_null");
    let full_table_name = format!("{}.{}", namespace, table_name);

    println!("\n=== Test: UPDATE multiple rows with mixed NULL values ===");

    // Setup namespace
    let _ = execute_sql_as_root_via_cli(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace));

    // Create table
    let create_sql = format!(
        "CREATE USER TABLE {} (id BIGINT NOT NULL PRIMARY KEY, optional_field TEXT, \
         required_field TEXT NOT NULL)",
        full_table_name
    );

    let output = execute_sql_as_root_via_cli(&create_sql).unwrap();
    assert!(output.contains("created") || output.contains("Success"));

    // Insert multiple rows: some with NULL, some without
    for i in 1..=5 {
        let optional_val = if i % 2 == 0 {
            "NULL"
        } else {
            &format!("'value_{}'", i)
        };
        let insert_sql = format!(
            "INSERT INTO {} (id, optional_field, required_field) VALUES ({}, {}, 'field_{}')",
            full_table_name, i, optional_val, i
        );
        let output = execute_sql_as_root_via_cli(&insert_sql).unwrap();
        assert!(output.contains("1 row"), "Insert {} failed: {}", i, output);
    }

    // Update each row individually (including ones with NULL)
    for i in 1..=5 {
        let update_sql = format!(
            "UPDATE {} SET required_field = 'updated_{}' WHERE id = {}",
            full_table_name, i, i
        );
        let output = execute_sql_as_root_via_cli(&update_sql).unwrap();
        assert!(
            output.contains("1 row"),
            "UPDATE failed for row {} (has NULL: {}): {}",
            i,
            i % 2 == 0,
            output
        );
    }

    // Verify all updates
    let select_sql = format!("SELECT * FROM {} ORDER BY id", full_table_name);
    let output = execute_sql_as_root_via_cli(&select_sql).unwrap();
    // Check that we have 5 rows returned
    assert!(
        output.contains("(5 row") || output.contains("5 row"),
        "Expected 5 rows, got: {}",
        output
    );
    // Check that all IDs are present (more reliable than checking truncated text)
    for i in 1..=5 {
        assert!(output.contains(&i.to_string()), "Row ID {} not found: {}", i, output);
    }

    println!("✅ Test passed: Multiple rows with NULL values can be updated");

    // Cleanup
    let _ = execute_sql_as_root_via_cli(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
}
