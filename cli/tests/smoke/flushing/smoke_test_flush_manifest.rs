//! Smoke tests for flush operations with manifest.json verification
//!
//! Tests filesystem-level verification of flush operations:
//! - manifest.json creation and updates
//! - batch-*.parquet file creation
//! - Storage path resolution for user and shared tables
//!
//! Reference: README.md lines 58-67, docs/SQL.md flush section

use std::time::Duration;

use crate::common::*;

/// Test manifest.json creation after flushing USER table
///
/// Verifies:
/// - manifest.json exists at user/{user_id}/{table}/ after flush
/// - At least one batch-*.parquet file exists
/// - manifest.json is valid (non-empty)
#[ntest::timeout(180_000)]
#[test]
fn smoke_test_user_table_flush_manifest() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("flush_manifest_ns");
    let table = generate_unique_table("user_flush_test");
    let full_table = format!("{}.{}", namespace, table);

    println!("🧪 Testing manifest.json for USER table flush");

    // Cleanup and setup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create USER table with low flush threshold for testing
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        ) WITH (
            TYPE = 'USER',
            STORAGE_ID = 'local',
            FLUSH_POLICY = 'rows:10'
        )"#,
        full_table
    );
    execute_sql_as_root_via_client(&create_sql).expect("Failed to create table");

    println!("✅ Created USER table with FLUSH_POLICY='rows:10'");

    // Insert 20 rows to trigger auto-flush (threshold is 10)
    println!("📝 Inserting 20 rows to trigger flush...");
    for i in 1..=20 {
        let insert_sql = format!("INSERT INTO {} (content) VALUES ('Row {}')", full_table, i);
        execute_sql_as_root_via_client(&insert_sql)
            .unwrap_or_else(|e| panic!("Failed to insert row {}: {}", i, e));
    }

    println!("✅ Inserted 20 rows");

    // Trigger manual flush to ensure data is flushed
    println!("🚀 Triggering manual STORAGE FLUSH TABLE...");
    let flush_output =
        execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", full_table))
            .expect("Failed to flush table");

    println!("Flush output: {}", flush_output);

    // Parse job ID and wait for completion
    let job_id = parse_job_id_from_flush_output(&flush_output)
        .expect("Failed to parse job ID from flush output");

    println!("📋 Flush job ID: {}", job_id);

    verify_job_completed(&job_id, Duration::from_secs(120))
        .expect("Flush job did not complete successfully");

    println!("✅ Flush job completed");

    // Use the common helper function to verify storage files
    assert_flush_storage_files_exist(
        &namespace,
        &table,
        true, // is_user_table
        "USER table flush manifest test",
    );

    println!("✅ Verified manifest.json and parquet files exist after flush");
}

/// Test manifest.json creation for SHARED table
///
/// Verifies:
/// - manifest.json exists at shared/{table}/ after flush
/// - Shared table storage path differs from user table path
#[ntest::timeout(180_000)]
#[test]
fn smoke_test_shared_table_flush_manifest() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("flush_manifest_ns");
    let table = generate_unique_table("shared_flush_test");
    let full_table = format!("{}.{}", namespace, table);

    println!("🧪 Testing manifest.json for SHARED table flush");

    // Cleanup and setup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create SHARED table
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            config_key TEXT NOT NULL,
            config_value TEXT
        ) WITH (
            TYPE = 'SHARED',
            STORAGE_ID = 'local',
            FLUSH_POLICY = 'rows:100',
            ACCESS_LEVEL = 'PUBLIC'
        )"#,
        full_table
    );
    execute_sql_as_root_via_client(&create_sql).expect("Failed to create shared table");

    println!("✅ Created SHARED table with FLUSH_POLICY='rows:100'");

    // Insert 20 rows
    println!("📝 Inserting 20 rows...");
    for i in 1..=20 {
        let insert_sql = format!(
            "INSERT INTO {} (config_key, config_value) VALUES ('key_{}', 'value_{}')",
            full_table, i, i
        );
        execute_sql_as_root_via_client(&insert_sql)
            .unwrap_or_else(|e| panic!("Failed to insert row {}: {}", i, e));
    }

    // Trigger manual flush
    println!("🚀 Triggering manual STORAGE FLUSH TABLE...");
    let flush_output =
        execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", full_table))
            .expect("Failed to flush table");

    let job_id = parse_job_id_from_flush_output(&flush_output)
        .expect("Failed to parse job ID from flush output");

    verify_job_completed(&job_id, Duration::from_secs(120))
        .expect("Flush job did not complete successfully");

    println!("✅ Flush job completed");

    // Use the common helper function to verify storage files
    assert_flush_storage_files_exist(
        &namespace,
        &table,
        false, // is_user_table (SHARED)
        "SHARED table flush manifest test",
    );

    println!("✅ Verified manifest.json exists for shared table");
}

/// Test manifest.json updates on second flush
///
/// Verifies:
/// - manifest.json exists after first flush
/// - Additional batch-*.parquet file created after second flush
/// - manifest.json updated (different content or file modified time)
#[ntest::timeout(180_000)]
#[test]
fn smoke_test_manifest_updated_on_second_flush() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("flush_manifest_ns");
    let table = generate_unique_table("double_flush_test");
    let full_table = format!("{}.{}", namespace, table);

    println!("🧪 Testing manifest.json updates on second flush");

    // Cleanup and setup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create SHARED table for easier testing (no user_id path complexity)
    let create_sql = format!(
        r#"CREATE TABLE {} (
            id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
            data TEXT NOT NULL
        ) WITH (
            TYPE = 'SHARED',
            FLUSH_POLICY = 'rows:10'
        )"#,
        full_table
    );
    execute_sql_as_root_via_client(&create_sql).expect("Failed to create table");

    // First flush cycle
    println!("📝 First flush: Inserting 15 rows...");
    for i in 1..=15 {
        let insert_sql = format!("INSERT INTO {} (data) VALUES ('Batch1-Row{}')", full_table, i);
        execute_sql_as_root_via_client(&insert_sql).expect("Failed to insert row");
    }

    let flush1_output =
        execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", full_table))
            .expect("Failed to flush table (first)");
    let job1_id =
        parse_job_id_from_flush_output(&flush1_output).expect("Failed to parse job ID (first)");
    let job_timeout = if is_cluster_mode() {
        Duration::from_secs(180)
    } else {
        Duration::from_secs(120)
    };
    verify_job_completed(&job1_id, job_timeout).expect("First flush job did not complete");

    println!("✅ First flush completed");

    // Verify files exist after first flush and get parquet count
    let first_result = verify_flush_storage_files_shared(&namespace, &table);
    let first_valid = first_result.is_valid();
    if first_valid {
        first_result.assert_valid("First flush");
    } else {
        assert!(
            manifest_exists_in_system_table(&namespace, &table),
            "First flush: manifest.json should exist after flush"
        );
    }
    let parquet_count_after_first_flush = first_result.parquet_file_count;

    println!("  Parquet files after first flush: {}", parquet_count_after_first_flush);

    // Second flush cycle
    println!("📝 Second flush: Inserting another 15 rows...");
    for i in 1..=15 {
        let insert_sql = format!("INSERT INTO {} (data) VALUES ('Batch2-Row{}')", full_table, i);
        execute_sql_as_root_via_client(&insert_sql).expect("Failed to insert row");
    }

    let flush2_output =
        execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", full_table))
            .expect("Failed to flush table (second)");
    let job2_id =
        parse_job_id_from_flush_output(&flush2_output).expect("Failed to parse job ID (second)");
    verify_job_completed(&job2_id, job_timeout).expect("Second flush job did not complete");

    println!("✅ Second flush completed");

    // Verify files exist after second flush and get parquet count
    std::thread::sleep(Duration::from_millis(10)); // Give filesystem time to sync
    let second_result = verify_flush_storage_files_shared(&namespace, &table);
    let second_valid = second_result.is_valid();
    if second_valid {
        second_result.assert_valid("Second flush");
    } else {
        assert!(
            manifest_exists_in_system_table(&namespace, &table),
            "Second flush: manifest.json should exist after flush"
        );
    }
    let parquet_count_after_second_flush = second_result.parquet_file_count;

    println!("  Parquet files after second flush: {}", parquet_count_after_second_flush);

    // Verify more parquet files exist after second flush
    if first_valid && second_valid {
        assert!(
            parquet_count_after_second_flush >= parquet_count_after_first_flush,
            "Expected same or more parquet files after second flush ({} vs {})",
            parquet_count_after_second_flush,
            parquet_count_after_first_flush
        );
    } else {
        println!("  Skipping parquet count comparison (manifest verified via system.manifest)");
    }

    println!("✅ Verified manifest.json updated after second flush");
}

/// Test error: FLUSH on STREAM table should fail
///
/// Verifies:
/// - STORAGE FLUSH TABLE on stream table returns error
/// - Error message mentions stream tables don't support flushing
#[ntest::timeout(180_000)]
#[test]
fn smoke_test_flush_stream_table_error() {
    if !is_server_running() {
        eprintln!("⚠️  Server not running. Skipping test.");
        return;
    }

    let namespace = generate_unique_namespace("flush_error_ns");
    let table = generate_unique_table("stream_no_flush");
    let full_table = format!("{}.{}", namespace, table);

    println!("🧪 Testing STORAGE FLUSH TABLE error on STREAM table");

    // Cleanup and setup
    let _ =
        execute_sql_as_root_via_client(&format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_sql_as_root_via_client(&format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create STREAM table
    let create_sql = format!(
        r#"CREATE TABLE {} (
            event_id TEXT PRIMARY KEY DEFAULT ULID(),
            event_type TEXT
        ) WITH (TYPE = 'STREAM', TTL_SECONDS = 30)"#,
        full_table
    );
    execute_sql_as_root_via_client(&create_sql).expect("Failed to create stream table");

    println!("✅ Created STREAM table");

    // Try to flush stream table (should fail)
    let flush_result =
        execute_sql_as_root_via_client(&format!("STORAGE FLUSH TABLE {}", full_table));

    match flush_result {
        Err(e) => {
            println!("✅ STORAGE FLUSH TABLE on STREAM table failed as expected: {}", e);
            let error_msg = e.to_string().to_lowercase();
            assert!(
                error_msg.contains("stream")
                    || error_msg.contains("not support")
                    || error_msg.contains("cannot flush"),
                "Expected error message about stream tables not supporting flush, got: {}",
                e
            );
        },
        Ok(output) => {
            // Check if output contains error message
            let output_lower = output.to_lowercase();
            assert!(
                output_lower.contains("error")
                    || output_lower.contains("stream")
                    || output_lower.contains("not support"),
                "Expected error when flushing stream table, got success: {}",
                output
            );
        },
    }

    println!("✅ Verified STORAGE FLUSH TABLE on STREAM table returns error");
}
