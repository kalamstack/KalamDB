//! Integration tests for hot/cold storage data integrity
//!
//! These tests validate:
//! - INSERT operations work correctly in hot storage (RocksDB)
//! - UPDATE operations work correctly in hot storage
//! - Data persists after FLUSH to cold storage (Parquet)
//! - INSERT/UPDATE operations work with flushed data
//! - Duplicate primary key inserts fail appropriately
//! - UPDATE operations work on both hot and cold storage

use crate::common::*;

// Use client-based execution to avoid authentication issues
fn execute_sql(sql: &str) -> Result<String, Box<dyn std::error::Error>> {
    execute_sql_as_root_via_client(sql)
}

/// Test INSERT → SELECT → UPDATE → SELECT → FLUSH → INSERT → UPDATE → SELECT
/// Validates data integrity across hot (RocksDB) and cold (Parquet) storage
#[test]
fn test_hot_cold_storage_data_integrity() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("hot_cold_integrity");
    // Use a test-unique namespace so concurrent tests cannot interfere via
    // DROP NAMESPACE CASCADE on the shared 'test_storage' namespace.
    let namespace = generate_unique_table("ts_hc");
    let full_table_name = format!("{}.{}", namespace, table_name);

    // === Setup: Create namespace and table ===
    execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE failed");

    execute_sql(&format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY,
            name VARCHAR NOT NULL,
            value INT NOT NULL
        ) WITH (TYPE='USER')"#,
        full_table_name
    ))
    .expect("CREATE TABLE failed");

    // === Phase 1: INSERT into hot storage ===
    execute_sql(&format!(
        "INSERT INTO {} (id, name, value) VALUES (1, 'Alice', 100), (2, 'Bob', 200), (3, \
         'Charlie', 300)",
        full_table_name
    ))
    .expect("Initial INSERT failed");

    // === Phase 2: SELECT from hot storage and verify ===
    let result = execute_sql(&format!("SELECT * FROM {} ORDER BY id", full_table_name))
        .expect("SELECT from hot storage failed");

    assert!(result.contains("Alice"), "Alice should exist in hot storage");
    assert!(result.contains("100"), "Alice's value should be 100");
    assert!(result.contains("Bob"), "Bob should exist in hot storage");
    assert!(result.contains("200"), "Bob's value should be 200");
    assert!(result.contains("Charlie"), "Charlie should exist in hot storage");
    assert!(result.contains("300"), "Charlie's value should be 300");

    // === Phase 3: UPDATE in hot storage ===
    execute_sql(&format!("UPDATE {} SET value = 150 WHERE id = 1", full_table_name))
        .expect("UPDATE in hot storage failed");

    // === Phase 4: SELECT and verify UPDATE in hot storage ===
    let result = execute_sql(&format!("SELECT value FROM {} WHERE id = 1", full_table_name))
        .expect("SELECT after UPDATE failed");

    assert!(result.contains("150"), "Alice's value should be updated to 150 in hot storage");

    // === Phase 5: FLUSH to cold storage ===
    let flush_output = execute_sql(&format!("STORAGE FLUSH TABLE {}", full_table_name))
        .expect("STORAGE FLUSH TABLE failed");
    if let Ok(job_id) = parse_job_id_from_flush_output(&flush_output) {
        let timeout = if is_cluster_mode() {
            std::time::Duration::from_secs(30)
        } else {
            std::time::Duration::from_secs(10)
        };
        verify_job_completed(&job_id, timeout).expect("flush job should complete");
    } else {
    }

    // === Phase 6: SELECT from cold storage and verify all data persisted ===
    let result = wait_for_query_contains_with(
        &format!("SELECT * FROM {} ORDER BY id", full_table_name),
        "Alice",
        std::time::Duration::from_secs(20),
        execute_sql,
    )
    .expect("SELECT from cold storage failed");

    assert!(result.contains("Alice"), "Alice should exist in cold storage after flush");
    assert!(result.contains("150"), "Alice's updated value (150) should persist after flush");
    assert!(result.contains("Bob"), "Bob should exist in cold storage after flush");
    assert!(result.contains("200"), "Bob's value should persist after flush");
    assert!(result.contains("Charlie"), "Charlie should exist in cold storage after flush");
    assert!(result.contains("300"), "Charlie's value should persist after flush");

    // === Phase 7: INSERT new data after flush (hot + cold mix) ===
    execute_sql(&format!(
        "INSERT INTO {} (id, name, value) VALUES (4, 'David', 400)",
        full_table_name
    ))
    .expect("INSERT after flush failed");

    // === Phase 8: SELECT and verify new data appears with old data ===
    let result = execute_sql(&format!("SELECT * FROM {} ORDER BY id", full_table_name))
        .expect("SELECT after post-flush INSERT failed");

    assert!(result.contains("Alice"), "Alice should still exist from cold storage");
    assert!(result.contains("150"), "Alice's value should still be 150");
    assert!(result.contains("David"), "David should exist from hot storage");
    assert!(result.contains("400"), "David's value should be 400");

    // === Phase 9: UPDATE data that exists in cold storage ===
    execute_sql(&format!("UPDATE {} SET value = 250 WHERE id = 2", full_table_name))
        .expect("UPDATE on cold storage row failed");

    // === Phase 10: SELECT and verify UPDATE worked on cold storage data ===
    let result = execute_sql(&format!("SELECT value FROM {} WHERE id = 2", full_table_name))
        .expect("SELECT after UPDATE on cold row failed");

    assert!(
        result.contains("250"),
        "Bob's value should be updated to 250 even though original was in cold storage"
    );

    // === Phase 11: Verify all data in final state ===
    let result = execute_sql(&format!("SELECT * FROM {} ORDER BY id", full_table_name))
        .expect("Final SELECT failed");

    assert!(
        result.contains("Alice") && result.contains("150"),
        "Alice should have value 150"
    );
    assert!(
        result.contains("Bob") && result.contains("250"),
        "Bob should have updated value 250"
    );
    assert!(
        result.contains("Charlie") && result.contains("300"),
        "Charlie should have value 300"
    );
    assert!(
        result.contains("David") && result.contains("400"),
        "David should have value 400"
    );

    // === Cleanup ===
    let _ = execute_sql(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ = execute_sql(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    println!("✅ Hot/cold storage data integrity test passed!");
}

/// Test that inserting duplicate primary key fails appropriately
#[test]
fn test_duplicate_primary_key_insert_fails() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("duplicate_pk");
    // Use a test-unique namespace so concurrent tests cannot interfere via
    // DROP NAMESPACE CASCADE on the shared 'test_storage' namespace.
    let namespace = generate_unique_table("ts_dpk");
    let full_table_name = format!("{}.{}", namespace, table_name);

    // === Setup: Create namespace and table ===
    execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE failed");

    execute_sql(&format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY,
            name VARCHAR NOT NULL
        ) WITH (TYPE='USER')"#,
        full_table_name
    ))
    .expect("CREATE TABLE failed");

    // === Test 1: Insert initial row ===
    let result =
        execute_sql(&format!("INSERT INTO {} (id, name) VALUES (1, 'Alice')", full_table_name));

    assert!(result.is_ok(), "First INSERT should succeed");

    // === Test 2: Attempt to insert duplicate primary key (hot storage) ===
    let result =
        execute_sql(&format!("INSERT INTO {} (id, name) VALUES (1, 'Bob')", full_table_name));

    assert!(result.is_err(), "INSERT with duplicate primary key should fail in hot storage");
    if let Err(e) = result {
        let error_msg = e.to_string().to_lowercase();
        // Should contain some indication of duplicate/constraint violation
        assert!(
            error_msg.contains("duplicate")
                || error_msg.contains("constraint")
                || error_msg.contains("unique")
                || error_msg.contains("already exists"),
            "Error message should indicate duplicate key violation, got: {}",
            e
        );
    }

    // === Test 3: Verify original data is unchanged ===
    let result = execute_sql(&format!("SELECT name FROM {} WHERE id = 1", full_table_name))
        .expect("SELECT after duplicate insert attempt failed");

    assert!(
        result.contains("Alice"),
        "Original data should remain 'Alice', not changed to 'Bob'"
    );

    // === Test 4: FLUSH to cold storage ===
    let flush_output = execute_sql(&format!("STORAGE FLUSH TABLE {}", full_table_name))
        .expect("STORAGE FLUSH TABLE failed");
    if let Ok(job_id) = parse_job_id_from_flush_output(&flush_output) {
        let timeout = if is_cluster_mode() {
            std::time::Duration::from_secs(30)
        } else {
            std::time::Duration::from_secs(10)
        };
        verify_job_completed(&job_id, timeout)
            .expect("flush job should complete before cold-storage duplicate check");
    }

    // === Test 5: Attempt to insert duplicate primary key (cold storage) ===
    let result =
        execute_sql(&format!("INSERT INTO {} (id, name) VALUES (1, 'Charlie')", full_table_name));

    if let Err(e) = result {
        let error_msg = e.to_string().to_lowercase();
        assert!(
            error_msg.contains("duplicate")
                || error_msg.contains("constraint")
                || error_msg.contains("unique")
                || error_msg.contains("already exists"),
            "Error message should indicate duplicate key violation in cold storage, got: {}",
            e
        );
    }

    // === Test 6: Verify UPDATE works on same row ===
    let result =
        execute_sql(&format!("UPDATE {} SET name = 'Alice Updated' WHERE id = 1", full_table_name));

    assert!(result.is_ok(), "UPDATE should work on existing primary key");

    // === Test 7: Verify UPDATE took effect ===
    let result = execute_sql(&format!("SELECT name FROM {} WHERE id = 1", full_table_name))
        .expect("SELECT after UPDATE failed");

    assert!(result.contains("Alice Updated"), "Name should be updated to 'Alice Updated'");

    // === Cleanup ===
    let _ = execute_sql(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ = execute_sql(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    println!("✅ Duplicate primary key constraint test passed!");
}

/// Test UPDATE operations work correctly on both hot and cold storage
#[test]
fn test_update_operations_hot_and_cold() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let table_name = generate_unique_table("update_hot_cold");
    // Use a test-unique namespace so concurrent tests cannot interfere via
    // DROP NAMESPACE CASCADE on the shared 'test_storage' namespace.
    let namespace = generate_unique_table("ts_uhc");
    let full_table_name = format!("{}.{}", namespace, table_name);

    // === Setup ===
    execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE failed");

    execute_sql(&format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY,
            status VARCHAR NOT NULL,
            count INT NOT NULL
        ) WITH (TYPE='USER')"#,
        full_table_name
    ))
    .expect("CREATE TABLE failed");

    // === Insert test data ===
    execute_sql(&format!(
        "INSERT INTO {} (id, status, count) VALUES (1, 'active', 10), (2, 'inactive', 20), (3, \
         'pending', 30)",
        full_table_name
    ))
    .expect("INSERT failed");

    // === Test 1: UPDATE multiple rows in hot storage (one at a time with explicit values) ===
    execute_sql(&format!("UPDATE {} SET count = 15 WHERE id = 1", full_table_name))
        .expect("UPDATE row 1 failed");
    execute_sql(&format!("UPDATE {} SET count = 25 WHERE id = 2", full_table_name))
        .expect("UPDATE row 2 failed");
    execute_sql(&format!("UPDATE {} SET count = 35 WHERE id = 3", full_table_name))
        .expect("UPDATE row 3 failed");

    let result = execute_sql(&format!("SELECT * FROM {} ORDER BY id", full_table_name))
        .expect("SELECT after UPDATE failed");

    assert!(result.contains("15"), "First row count should be 15");
    assert!(result.contains("25"), "Second row count should be 25");
    assert!(result.contains("35"), "Third row count should be 35");

    // === Test 2: FLUSH to cold storage ===
    let flush_output = execute_sql(&format!("STORAGE FLUSH TABLE {}", full_table_name))
        .expect("STORAGE FLUSH TABLE failed");
    if let Ok(job_id) = parse_job_id_from_flush_output(&flush_output) {
        let timeout = if is_cluster_mode() {
            std::time::Duration::from_secs(30)
        } else {
            std::time::Duration::from_secs(10)
        };
        verify_job_completed(&job_id, timeout).expect("flush job should complete");
    }

    // === Test 3: UPDATE specific row in cold storage ===
    execute_sql(&format!(
        "UPDATE {} SET status = 'completed', count = 100 WHERE id = 2",
        full_table_name
    ))
    .expect("UPDATE specific row in cold storage failed");

    let result = execute_sql(&format!("SELECT * FROM {} WHERE id = 2", full_table_name))
        .expect("SELECT after cold storage UPDATE failed");

    assert!(result.contains("completed"), "Status should be updated to 'completed'");
    assert!(result.contains("100"), "Count should be updated to 100");

    // === Test 4: Insert new row and UPDATE specific rows by ID ===
    execute_sql(&format!(
        "INSERT INTO {} (id, status, count) VALUES (4, 'new', 40)",
        full_table_name
    ))
    .expect("INSERT new row failed");

    // Archive rows 3 and 4 explicitly by ID
    execute_sql(&format!("UPDATE {} SET status = 'archived' WHERE id = 3", full_table_name))
        .expect("UPDATE row 3 to archived failed");

    execute_sql(&format!("UPDATE {} SET status = 'archived' WHERE id = 4", full_table_name))
        .expect("UPDATE row 4 to archived failed");

    let result = execute_sql(&format!("SELECT * FROM {} ORDER BY id", full_table_name))
        .expect("SELECT final state failed");

    // Row 1 should be 'active', Row 2 should be 'completed'
    assert!(result.contains("active"), "Row 1 should still be 'active'");
    assert!(result.contains("completed"), "Row 2 should still be 'completed'");
    // Row 3 and 4 should be archived
    let archived_count = result.matches("archived").count();
    assert_eq!(archived_count, 2, "Rows 3 and 4 should be 'archived'");

    // === Cleanup ===
    let _ = execute_sql(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ = execute_sql(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    println!("✅ UPDATE operations on hot and cold storage test passed!");
}
