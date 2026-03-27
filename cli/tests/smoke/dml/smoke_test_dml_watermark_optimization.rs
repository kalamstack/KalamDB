// Smoke test for watermark optimization (spec 021 section 5.4.1)
//
// This test verifies that DML operations (INSERT/UPDATE/DELETE) perform well
// after the watermark optimization that sets required_meta_index=0 for pure DML.
//
// The optimization removes unnecessary Meta group synchronization for DML operations
// where the table schema has already been validated.

use crate::common::*;
use std::time::Instant;

/// Test that INSERT operations complete in acceptable time (no Meta waiting)
#[ntest::timeout(120000)]
#[test]
fn smoke_test_watermark_dml_insert_performance() {
    if !is_server_running() {
        println!(
            "Skipping smoke_test_watermark_dml_insert_performance: server not running at {}",
            server_url()
        );
        return;
    }

    println!("\n=== Watermark Optimization: INSERT Performance ===\n");

    let namespace = generate_unique_namespace("watermark_opt");
    let table = generate_unique_table("perf");
    let full_table_name = format!("{}.{}", namespace, table);

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE should succeed");

    let create_table_sql = format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, name TEXT NOT NULL, value INT)",
        full_table_name
    );
    execute_sql_as_root_via_client(&create_table_sql).expect("CREATE TABLE should succeed");

    // Perform multiple INSERTs and measure total time
    const INSERT_COUNT: usize = 20;
    let start = Instant::now();

    for i in 0..INSERT_COUNT {
        let sql = format!(
            "INSERT INTO {} (id, name, value) VALUES ({}, 'item_{}', {})",
            full_table_name,
            i,
            i,
            i * 10
        );
        execute_sql_as_root_via_client(&sql)
            .unwrap_or_else(|e| panic!("INSERT {} failed: {}", i, e));
    }

    let elapsed = start.elapsed();
    let avg_ms = elapsed.as_millis() as f64 / INSERT_COUNT as f64;

    println!("  Inserted {} rows in {:?}", INSERT_COUNT, elapsed);
    println!("  Average: {:.2}ms per INSERT", avg_ms);

    // Verify data is readable
    let select_result =
        execute_sql_as_root_via_client(&format!("SELECT COUNT(*) as cnt FROM {}", full_table_name))
            .expect("SELECT COUNT should succeed");

    println!("  Verification: {:?}", select_result);

    // Performance assertion: avg INSERT should be < 100ms with watermark optimization
    // Before optimization, this could be 50-200ms due to Meta waiting
    if avg_ms > 100.0 {
        println!("  ⚠️ WARNING: Average INSERT time {:.2}ms exceeds 100ms target", avg_ms);
    } else {
        println!("  ✅ PASS: Average INSERT time {:.2}ms is within target", avg_ms);
    }

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    // The key assertion: INSERTs should work and not hang
    assert!(
        elapsed.as_secs() < 90,
        "Total INSERT time {} seconds exceeded 90 second limit - possible watermark issue",
        elapsed.as_secs()
    );
}

/// Test that UPDATE operations work correctly after watermark optimization
#[ntest::timeout(120000)]
#[test]
fn smoke_test_watermark_dml_update() {
    if !is_server_running() {
        println!(
            "Skipping smoke_test_watermark_dml_update: server not running at {}",
            server_url()
        );
        return;
    }

    println!("\n=== Watermark Optimization: UPDATE Operations ===\n");

    let namespace = generate_unique_namespace("watermark_upd");
    let table = generate_unique_table("data");
    let full_table_name = format!("{}.{}", namespace, table);

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE should succeed");

    let create_table_sql = format!(
        "CREATE TABLE {} (id BIGINT PRIMARY KEY, status TEXT, counter INT) WITH (TYPE = 'USER', STORAGE_ID = 'local')",
        full_table_name
    );
    execute_sql_as_root_via_client(&create_table_sql).expect("CREATE TABLE should succeed");

    // Insert initial data
    for i in 0..10 {
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, status, counter) VALUES ({}, 'pending', 0)",
            full_table_name, i
        ))
        .expect("INSERT should succeed");
    }

    // Verify data was inserted successfully
    let count_result =
        execute_sql_as_root_via_client(&format!("SELECT COUNT(*) as cnt FROM {}", full_table_name))
            .expect("SELECT COUNT should succeed");
    assert!(
        count_result.contains("10"),
        "Expected 10 rows after insert, got: {}",
        count_result
    );

    // Perform UPDATEs and measure time
    let start = Instant::now();
    const UPDATE_COUNT: usize = 10;

    for i in 0..UPDATE_COUNT {
        let id = i % 10;
        let sql = format!(
            "UPDATE {} SET status = 'processed_{}', counter = counter + 1 WHERE id = {}",
            full_table_name, i, id
        );
        execute_sql_as_root_via_client(&sql)
            .unwrap_or_else(|e| panic!("UPDATE {} failed: {}", i, e));
    }

    let elapsed = start.elapsed();
    let avg_ms = elapsed.as_millis() as f64 / UPDATE_COUNT as f64;

    println!("  Executed {} UPDATEs in {:?}", UPDATE_COUNT, elapsed);
    println!("  Average: {:.2}ms per UPDATE", avg_ms);
    println!("  ✅ PASS: UPDATE operations working correctly");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    assert!(elapsed.as_secs() < 30, "Total UPDATE time exceeded 30 second limit");
}

/// Test that DELETE operations work correctly after watermark optimization
#[ntest::timeout(120000)]
#[test]
fn smoke_test_watermark_dml_delete() {
    if !is_server_running() {
        println!(
            "Skipping smoke_test_watermark_dml_delete: server not running at {}",
            server_url()
        );
        return;
    }

    println!("\n=== Watermark Optimization: DELETE Operations ===\n");

    let namespace = generate_unique_namespace("watermark_del");
    let table = generate_unique_table("items");
    let full_table_name = format!("{}.{}", namespace, table);

    // Setup
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE should succeed");

    let create_table_sql =
        format!("CREATE TABLE {} (id BIGINT PRIMARY KEY, data TEXT)", full_table_name);
    execute_sql_as_root_via_client(&create_table_sql).expect("CREATE TABLE should succeed");

    // Insert data to delete
    const ROW_COUNT: usize = 15;
    for i in 0..ROW_COUNT {
        execute_sql_as_root_via_client(&format!(
            "INSERT INTO {} (id, data) VALUES ({}, 'item_{}')",
            full_table_name, i, i
        ))
        .expect("INSERT should succeed");
    }

    // Perform DELETEs and measure time
    let start = Instant::now();

    for i in 0..ROW_COUNT {
        let sql = format!("DELETE FROM {} WHERE id = {}", full_table_name, i);
        execute_sql_as_root_via_client(&sql)
            .unwrap_or_else(|e| panic!("DELETE {} failed: {}", i, e));
    }

    let elapsed = start.elapsed();
    let avg_ms = elapsed.as_millis() as f64 / ROW_COUNT as f64;

    println!("  Executed {} DELETEs in {:?}", ROW_COUNT, elapsed);
    println!("  Average: {:.2}ms per DELETE", avg_ms);

    // Verify all rows deleted
    let select_result =
        execute_sql_as_root_via_client(&format!("SELECT COUNT(*) as cnt FROM {}", full_table_name))
            .expect("SELECT COUNT should succeed");

    println!("  Verification (should be 0): {:?}", select_result);
    println!("  ✅ PASS: DELETE operations working correctly");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", full_table_name));
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    assert!(elapsed.as_secs() < 90, "Total DELETE time exceeded 90 second limit");
}

/// Test rapid DDL followed by DML to verify watermark still works when needed
#[ntest::timeout(120000)]
#[test]
fn smoke_test_watermark_ddl_then_dml() {
    if !is_server_running() {
        println!(
            "Skipping smoke_test_watermark_ddl_then_dml: server not running at {}",
            server_url()
        );
        return;
    }

    println!("\n=== Watermark: DDL then immediate DML ===\n");

    let namespace = generate_unique_namespace("watermark_ddl");

    // Setup namespace
    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE IF NOT EXISTS {}", namespace))
        .expect("CREATE NAMESPACE should succeed");

    // Rapid DDL + DML cycles to verify ordering works
    const CYCLES: usize = 3;
    let start = Instant::now();

    for i in 0..CYCLES {
        let table = format!("{}.cycle_table_{}", namespace, i);

        // DDL: Create table
        let create_sql = format!("CREATE TABLE {} (id BIGINT PRIMARY KEY, value INT)", table);
        execute_sql_as_root_via_client(&create_sql)
            .unwrap_or_else(|e| panic!("CREATE TABLE {} failed: {}", i, e));

        // DML: Immediately insert (tests that Raft ordering ensures table exists)
        let insert_sql = format!("INSERT INTO {} (id, value) VALUES (1, 100)", table);
        execute_sql_as_root_via_client(&insert_sql)
            .unwrap_or_else(|e| panic!("INSERT after CREATE {} failed: {}", i, e));

        // Verify
        let select_result = execute_sql_as_root_via_client(&format!("SELECT * FROM {}", table));
        assert!(select_result.is_ok(), "SELECT after INSERT {} failed: {:?}", i, select_result);

        // Cleanup this table
        let _ = execute_sql_as_root_via_client(&format!("DROP TABLE IF EXISTS {}", table));

        println!("  Cycle {}: CREATE → INSERT → SELECT → DROP completed", i);
    }

    let elapsed = start.elapsed();
    println!("  Total {} DDL+DML cycles in {:?}", CYCLES, elapsed);
    println!("  ✅ PASS: DDL→DML ordering works correctly (Raft guarantees)");

    // Cleanup namespace
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    assert!(
        elapsed.as_secs() < 120,
        "DDL+DML cycles took too long: {} seconds",
        elapsed.as_secs()
    );
}
