//! Smoke tests for flush operations
//!
//! Tests comprehensive flush behavior for both USER and SHARED tables:
//! - Auto-flush with FLUSH ROWS policy
//! - Manual flush with STORAGE FLUSH TABLE command
//! - Job completion verification
//! - Data retrieval from both RocksDB (unflushed) and Parquet (flushed) sources
//!
//! **Test Strategy**:
//! 1. Create table with flush policy (e.g., FLUSH ROWS 50)
//! 2. Insert more rows than the policy (e.g., 200 rows)
//! 3. Manually trigger flush
//! 4. Verify job completes successfully
//! 5. Query all data and verify correct row count from both sources

use crate::common::*;
use std::time::Duration;

const JOB_TIMEOUT: Duration = Duration::from_secs(30);
const FLUSH_POLICY_ROWS: usize = 50;
const INSERT_ROWS: usize = 200;

/// Test USER table flush operations
///
/// Creates a user table with FLUSH ROWS 50 policy, inserts 200 rows,
/// flushes manually, and verifies all data is retrievable.
#[ntest::timeout(180_000)]
#[test]
fn smoke_test_user_table_flush() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("smoke_flush");
    let table_name = generate_unique_table("user_flush");
    let full_table_name = format!("{}.{}", namespace, table_name);

    println!("🧪 Testing USER table flush: {}", full_table_name);

    // Cleanup and setup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create USER table with flush policy
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            content VARCHAR NOT NULL,
            sequence INT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (
            TYPE = 'USER',
            FLUSH_POLICY = 'rows:{}'
        )"#,
        full_table_name, FLUSH_POLICY_ROWS
    );

    execute_sql_as_root_via_client(&create_sql).expect("Failed to create user table");

    println!("✅ Created USER table with FLUSH ROWS {}", FLUSH_POLICY_ROWS);

    // Insert rows in batches using a single multi-row INSERT per batch.
    // This keeps the smoke test fast and reduces flakiness from per-row request overhead.
    println!("📝 Inserting {} rows...", INSERT_ROWS);
    let batch_size = 50;
    for batch in 0..(INSERT_ROWS / batch_size) {
        let start = batch * batch_size;
        let end = (batch + 1) * batch_size;

        let mut values = Vec::with_capacity(batch_size);
        for i in start..end {
            values.push(format!("('Row {}', {})", i, i));
        }

        let insert_sql = format!(
            "INSERT INTO {} (content, sequence) VALUES {}",
            full_table_name,
            values.join(", ")
        );
        execute_sql_as_root_via_client(&insert_sql).expect("Failed to insert batch");

        println!(
            "  Inserted batch {}/{} ({}-{})",
            batch + 1,
            INSERT_ROWS / batch_size,
            start,
            end - 1
        );
    }

    println!("✅ Inserted {} rows", INSERT_ROWS);

    // Manually flush the table
    println!("🚀 Triggering manual STORAGE FLUSH TABLE...");
    let flush_output =
        execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", full_table_name))
            .expect("Failed to flush table");

    println!("Flush output: {}", flush_output);

    // Parse job ID from flush output
    let job_id = parse_job_id_from_flush_output(&flush_output)
        .expect("Failed to parse job ID from flush output");

    println!("📋 Flush job ID: {}", job_id);

    // Verify job completes successfully
    println!("⏳ Waiting for flush job to complete...");
    verify_job_completed(&job_id, JOB_TIMEOUT).expect("Flush job did not complete successfully");

    println!("✅ Flush job completed successfully");

    // Verify flush storage files exist (manifest.json and parquet files)
    assert_flush_storage_files_exist(
        &namespace,
        &table_name,
        true, // is_user_table
        "USER table flush operations test",
    );

    // Query all data to verify retrieval from both RocksDB and Parquet
    println!("🔍 Querying all data...");
    let select_output = execute_sql_as_root_via_client(&format!(
        "SELECT COUNT(*) as total FROM {}",
        full_table_name
    ))
    .expect("Failed to query data");

    println!("Select output:\n{}", select_output);

    // Verify we got all rows back (from both flushed Parquet and unflushed RocksDB)
    assert!(
        select_output.contains(&INSERT_ROWS.to_string()),
        "Expected {} rows in result, but output was: {}",
        INSERT_ROWS,
        select_output
    );

    println!("✅ Verified all {} rows are retrievable", INSERT_ROWS);

    // Verify data integrity by checking sequence numbers
    println!("🔍 Verifying data integrity...");
    let verify_output = execute_sql_as_root_via_client(&format!(
        "SELECT sequence FROM {} WHERE sequence IN (0, 50, 100, 150, 199) ORDER BY sequence",
        full_table_name
    ))
    .expect("Failed to query sequences");

    // Check that we have sequential data (spot check a few values)
    for check_val in [0, 50, 100, 150, 199] {
        assert!(
            verify_output.contains(&check_val.to_string()),
            "Missing sequence value {} in output",
            check_val
        );
    }

    println!("✅ Data integrity verified (sequence values present)");

    // Cleanup
    println!("🧹 Cleaning up...");
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE {}", full_table_name));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("✅ USER table flush smoke test completed successfully!");
}

/// Test SHARED table flush operations
///
/// Creates a shared table with FLUSH ROWS 50 policy, inserts 200 rows,
/// flushes manually, and verifies all data is retrievable.
#[ntest::timeout(180_000)]
#[test]
fn smoke_test_shared_table_flush() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("smoke_flush");
    let table_name = generate_unique_table("shared_flush");
    let full_table_name = format!("{}.{}", namespace, table_name);

    println!("🧪 Testing SHARED table flush: {}", full_table_name);

    // Cleanup and setup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create SHARED table with flush policy
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            content VARCHAR NOT NULL,
            sequence INT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) WITH (
            TYPE = 'SHARED',
            FLUSH_POLICY = 'rows:{}'
        )"#,
        full_table_name, FLUSH_POLICY_ROWS
    );

    execute_sql_as_root_via_client(&create_sql).expect("Failed to create shared table");

    println!("✅ Created SHARED table with FLUSH ROWS {}", FLUSH_POLICY_ROWS);

    // Insert rows in batches using a single multi-row INSERT per batch.
    println!("📝 Inserting {} rows...", INSERT_ROWS);
    let batch_size = 50;
    for batch in 0..(INSERT_ROWS / batch_size) {
        let start = batch * batch_size;
        let end = (batch + 1) * batch_size;

        let mut values = Vec::with_capacity(batch_size);
        for i in start..end {
            values.push(format!("('Shared Row {}', {})", i, i));
        }

        let insert_sql = format!(
            "INSERT INTO {} (content, sequence) VALUES {}",
            full_table_name,
            values.join(", ")
        );
        execute_sql_as_root_via_client(&insert_sql).expect("Failed to insert batch");

        println!(
            "  Inserted batch {}/{} ({}-{})",
            batch + 1,
            INSERT_ROWS / batch_size,
            start,
            end - 1
        );
    }

    println!("✅ Inserted {} rows", INSERT_ROWS);

    // Manually flush the table
    println!("🚀 Triggering manual STORAGE FLUSH TABLE...");
    let flush_output =
        execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", full_table_name))
            .expect("Failed to flush table");

    println!("Flush output: {}", flush_output);

    // Parse job ID from flush output
    let job_id = parse_job_id_from_flush_output(&flush_output)
        .expect("Failed to parse job ID from flush output");

    println!("📋 Flush job ID: {}", job_id);

    // Verify job completes successfully
    println!("⏳ Waiting for flush job to complete...");
    verify_job_completed(&job_id, JOB_TIMEOUT).expect("Flush job did not complete successfully");

    println!("✅ Flush job completed successfully");

    // Verify flush storage files exist (manifest.json and parquet files)
    assert_flush_storage_files_exist(
        &namespace,
        &table_name,
        false, // is_user_table (SHARED)
        "SHARED table flush operations test",
    );

    // Query all data to verify retrieval from both RocksDB and Parquet
    println!("🔍 Querying all data...");
    let select_output = execute_sql_as_root_via_client(&format!(
        "SELECT COUNT(*) as total FROM {}",
        full_table_name
    ))
    .expect("Failed to query data");

    println!("Select output:\n{}", select_output);

    // Verify we got all rows back
    assert!(
        select_output.contains(&INSERT_ROWS.to_string()),
        "Expected {} rows in result, but output was: {}",
        INSERT_ROWS,
        select_output
    );

    println!("✅ Verified all {} rows are retrievable", INSERT_ROWS);

    // Verify data integrity by checking sequence numbers
    println!("🔍 Verifying data integrity...");
    let verify_output = execute_sql_as_root_via_client(&format!(
        "SELECT sequence FROM {} WHERE sequence IN (0, 50, 100, 150, 199) ORDER BY sequence",
        full_table_name
    ))
    .expect("Failed to query sequences");

    // Check that we have sequential data (spot check a few values)
    for check_val in [0, 50, 100, 150, 199] {
        assert!(
            verify_output.contains(&check_val.to_string()),
            "Missing sequence value {} in output",
            check_val
        );
    }

    println!("✅ Data integrity verified (sequence values present)");

    // Cleanup
    println!("🧹 Cleaning up...");
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE {}", full_table_name));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("✅ SHARED table flush smoke test completed successfully!");
}

/// Test that data is correctly merged from both RocksDB and Parquet sources
///
/// This test verifies the query engine properly combines:
/// - Unflushed data still in RocksDB
/// - Flushed data in Parquet files
#[ntest::timeout(180_000)]
#[test]
fn smoke_test_mixed_source_query() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("smoke_flush");
    let table_name = generate_unique_table("mixed_query");
    let full_table_name = format!("{}.{}", namespace, table_name);

    println!("🧪 Testing mixed source (RocksDB + Parquet) query: {}", full_table_name);

    // Setup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create table with small flush policy
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            content VARCHAR NOT NULL,
            sequence INT NOT NULL
        ) WITH (
            TYPE = 'USER',
            FLUSH_POLICY = 'rows:30'
        )"#,
        full_table_name
    );

    execute_sql_as_root_via_client(&create_sql).expect("Failed to create table");

    // Insert first batch (will be flushed)
    println!("📝 Inserting first batch (50 rows - will exceed flush policy)...");
    let mut batch1_values = Vec::with_capacity(50);
    for i in 0..50 {
        batch1_values.push(format!("('Batch1-Row{}', {})", i, i));
    }
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (content, sequence) VALUES {}",
        full_table_name,
        batch1_values.join(", ")
    ))
    .expect("Failed to insert first batch");

    // Manually flush to ensure first batch is in Parquet
    println!("🚀 Flushing first batch...");
    let flush1_output =
        execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", full_table_name))
            .expect("Failed to flush");
    let job1_id = parse_job_id_from_flush_output(&flush1_output).expect("Failed to parse job ID");
    verify_job_completed(&job1_id, JOB_TIMEOUT).expect("First flush failed");
    println!("✅ First batch flushed to Parquet");

    // Verify flush storage files exist (manifest.json and parquet files)
    assert_flush_storage_files_exist(
        &namespace,
        &table_name,
        true, // is_user_table
        "Mixed source query - first flush",
    );

    // Insert second batch (will stay in RocksDB)
    println!("📝 Inserting second batch (20 rows - will stay in RocksDB)...");
    let mut batch2_values = Vec::with_capacity(20);
    for i in 50..70 {
        batch2_values.push(format!("('Batch2-Row{}', {})", i, i));
    }
    execute_sql_as_root_via_client(&format!(
        "INSERT INTO {} (content, sequence) VALUES {}",
        full_table_name,
        batch2_values.join(", ")
    ))
    .expect("Failed to insert second batch");

    // Query all data - should combine from both sources
    println!("🔍 Querying all data (should merge RocksDB + Parquet)...");
    let count_output = execute_sql_as_root_via_client(&format!(
        "SELECT COUNT(*) as total FROM {}",
        full_table_name
    ))
    .expect("Failed to query count");

    println!("Count output:\n{}", count_output);

    // Verify total count (50 from Parquet + 20 from RocksDB = 70)
    assert!(
        count_output.contains("70"),
        "Expected 70 total rows, but output was: {}",
        count_output
    );

    println!("✅ Verified 70 total rows (50 Parquet + 20 RocksDB)");

    // Verify we can query specific ranges spanning both sources
    let range_output = execute_sql_as_root_via_client(&format!(
        "SELECT sequence FROM {} WHERE sequence >= 45 AND sequence <= 55 ORDER BY sequence",
        full_table_name
    ))
    .expect("Failed to query range");

    println!("Range query output (45-55):\n{}", range_output);

    // Should have values from both Parquet (45-49) and RocksDB (50-55)
    for check_val in [45, 49, 50, 55] {
        assert!(
            range_output.contains(&check_val.to_string()),
            "Missing sequence value {} in range query",
            check_val
        );
    }

    println!("✅ Verified data spanning both sources (Parquet and RocksDB)");

    // Cleanup
    let _ = execute_sql_as_root_via_client(&format!("DROP TABLE {}", full_table_name));
    let _ = execute_sql_as_root_via_client(&format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("✅ Mixed source query smoke test completed successfully!");
}
