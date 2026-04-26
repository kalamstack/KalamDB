//! Cluster flush tests
//!
//! Tests that verify flush operations in a Raft cluster environment:
//! - Flush on leader replicates data to followers
//! - Data is readable after flush on all nodes
//! - Flush metadata (manifest) is consistent across nodes

use std::time::Duration;

use crate::{cluster_common::*, common::*};

/// Test: Flush table in cluster and verify data on all nodes
///
/// 1. Create a namespace and shared table on node 0 (leader)
/// 2. Insert test data
/// 3. Execute FLUSH command
/// 4. Verify data is readable from all cluster nodes
/// 5. Verify row counts match on all nodes
#[test]
fn cluster_test_flush_data_consistency() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Cluster Flush Data Consistency ===\n");

    let urls = cluster_urls();
    if urls.len() < 2 {
        println!("  ⏭ Skipping test: need at least 2 nodes");
        return;
    }

    let namespace = generate_unique_namespace("cluster_flush");
    let table_name = "test_data";
    let full_table = format!("{}.{}", namespace, table_name);

    // Cleanup from previous runs
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    // Step 1: Create namespace on first node
    println!("  1. Creating namespace on node 0...");
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Step 2: Create shared table
    println!("  2. Creating shared table...");
    let create_sql =
        format!("CREATE SHARED TABLE {} (id INT PRIMARY KEY, name TEXT, value INT)", full_table);
    execute_on_node(&urls[0], &create_sql).expect("Failed to create table");

    // Wait for table to be visible on all nodes
    println!("  3. Waiting for table to replicate...");
    let mut replicated = false;
    for _ in 0..20 {
        if wait_for_table_on_all_nodes(&namespace, table_name, 500) {
            replicated = true;
            break;
        }
    }
    assert!(replicated, "Table not replicated to all nodes");

    // Step 3: Insert test data
    println!("  4. Inserting test data...");
    let insert_count = 100;
    for i in 0..insert_count {
        let insert_sql = format!(
            "INSERT INTO {} (id, name, value) VALUES ({}, 'item_{}', {})",
            full_table,
            i,
            i,
            i * 10
        );
        execute_on_node(&urls[0], &insert_sql).expect(&format!("Failed to insert row {}", i));
    }

    // Step 4: Execute FLUSH command
    println!("  5. Executing FLUSH command...");
    let flush_sql = format!("STORAGE FLUSH TABLE {}", full_table);
    let flush_result = execute_on_node(&urls[0], &flush_sql);
    assert!(flush_result.is_ok(), "FLUSH command failed: {:?}", flush_result);

    // Wait for flush job to complete
    let job_id = wait_for_latest_job_id_by_type(&urls[0], "Flush", Duration::from_secs(3))
        .expect("Failed to find flush job id");
    assert!(
        wait_for_job_status(&urls[0], &job_id, "Completed", Duration::from_secs(20)),
        "Flush job did not complete in time"
    );

    // Step 5: Verify row count on all nodes
    println!("  6. Verifying row count on all nodes...");
    let count_sql = format!("SELECT count(*) FROM {}", full_table);

    for (idx, url) in urls.iter().enumerate() {
        let count = query_count_on_url(url, &count_sql);
        println!("     Node {}: {} rows", idx, count);
        assert_eq!(
            count, insert_count,
            "Row count mismatch on node {}: expected {}, got {}",
            idx, insert_count, count
        );
    }

    // Step 6: Verify data content matches on all nodes
    println!("  7. Verifying data content on all nodes...");
    let select_sql = format!("SELECT id, name, value FROM {} ORDER BY id LIMIT 5", full_table);

    let baseline_rows =
        fetch_normalized_rows(&urls[0], &select_sql).expect("Failed to fetch baseline rows");

    for (idx, url) in urls.iter().enumerate().skip(1) {
        let node_rows = fetch_normalized_rows(url, &select_sql)
            .expect(&format!("Failed to fetch rows from node {}", idx));
        assert_eq!(baseline_rows, node_rows, "Data mismatch between node 0 and node {}", idx);
        println!("     Node {} matches baseline ✓", idx);
    }

    // Step 7: Verify manifest exists (check system.schemas for latest version)
    println!("  8. Verifying flush metadata...");
    let metadata_sql = format!(
        "SELECT table_name, schema_version FROM system.schemas WHERE namespace_id = '{}' AND \
         table_name = '{}'",
        namespace, table_name
    );

    for (idx, url) in urls.iter().enumerate() {
        let result = execute_on_node(url, &metadata_sql);
        assert!(result.is_ok(), "Failed to query metadata on node {}", idx);
        assert!(
            result.as_ref().unwrap().contains(table_name),
            "Table metadata not found on node {}",
            idx
        );
        println!("     Node {} has table metadata ✓", idx);
    }

    // Cleanup
    println!("  9. Cleaning up...");
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Cluster flush data consistency test passed\n");
}

/// Test: Multiple flushes maintain data integrity
///
/// 1. Create table and insert initial batch
/// 2. Flush
/// 3. Insert more data
/// 4. Flush again
/// 5. Verify all data is present and consistent
#[test]
fn cluster_test_multiple_flushes() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Cluster Multiple Flushes ===\n");

    let urls = cluster_urls();
    if urls.len() < 2 {
        println!("  ⏭ Skipping test: need at least 2 nodes");
        return;
    }

    let namespace = generate_unique_namespace("cluster_mflush");
    let table_name = "multi_flush";
    let full_table = format!("{}.{}", namespace, table_name);

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    // Setup
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!("CREATE SHARED TABLE {} (id INT PRIMARY KEY, batch INT)", full_table),
    )
    .expect("Failed to create table");

    // Wait for table to replicate
    for _ in 0..10 {
        if wait_for_table_on_all_nodes(&namespace, table_name, 500) {
            break;
        }
    }

    // Insert first batch and flush
    println!("  1. Inserting batch 1 (10 rows)...");
    for i in 0..10 {
        execute_on_node(
            &urls[0],
            &format!("INSERT INTO {} (id, batch) VALUES ({}, 1)", full_table, i),
        )
        .expect("Failed to insert batch 1");
    }

    println!("  2. Flushing batch 1...");
    execute_on_node(&urls[0], &format!("STORAGE FLUSH TABLE {}", full_table))
        .expect("Failed to flush batch 1");
    let job_id = wait_for_latest_job_id_by_type(&urls[0], "Flush", Duration::from_secs(3))
        .expect("Failed to find flush job id");
    assert!(
        wait_for_job_status(&urls[0], &job_id, "Completed", Duration::from_secs(30)),
        "Flush job did not complete in time"
    );

    // Insert second batch and flush
    println!("  3. Inserting batch 2 (10 rows)...");
    for i in 10..20 {
        execute_on_node(
            &urls[0],
            &format!("INSERT INTO {} (id, batch) VALUES ({}, 2)", full_table, i),
        )
        .expect("Failed to insert batch 2");
    }

    println!("  4. Flushing batch 2...");
    execute_on_node(&urls[0], &format!("STORAGE FLUSH TABLE {}", full_table))
        .expect("Failed to flush batch 2");
    let job_id = wait_for_latest_job_id_by_type(&urls[0], "Flush", Duration::from_secs(3))
        .expect("Failed to find flush job id");
    assert!(
        wait_for_job_status(&urls[0], &job_id, "Completed", Duration::from_secs(30)),
        "Flush job did not complete in time"
    );

    // Verify total count on all nodes
    println!("  5. Verifying total row count on all nodes...");
    let count_sql = format!("SELECT count(*) FROM {}", full_table);

    for (idx, url) in urls.iter().enumerate() {
        let count = query_count_on_url(url, &count_sql);
        println!("     Node {}: {} rows", idx, count);
        assert_eq!(count, 20, "Expected 20 rows on node {}, got {}", idx, count);
    }

    // Verify batch distribution
    println!("  6. Verifying batch distribution...");
    let batch1_sql = format!("SELECT count(*) FROM {} WHERE batch = 1", full_table);
    let batch2_sql = format!("SELECT count(*) FROM {} WHERE batch = 2", full_table);

    for (idx, url) in urls.iter().enumerate() {
        let batch1_count = query_count_on_url(url, &batch1_sql);
        let batch2_count = query_count_on_url(url, &batch2_sql);
        assert_eq!(batch1_count, 10, "Batch 1 count mismatch on node {}", idx);
        assert_eq!(batch2_count, 10, "Batch 2 count mismatch on node {}", idx);
        println!("     Node {}: batch1={}, batch2={} ✓", idx, batch1_count, batch2_count);
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Cluster multiple flushes test passed\n");
}

/// Test: Flush with concurrent reads doesn't cause issues
///
/// Verifies that flushing doesn't block or corrupt concurrent read operations
#[test]
fn cluster_test_flush_during_reads() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Cluster Flush During Reads ===\n");

    let urls = cluster_urls();
    if urls.len() < 2 {
        println!("  ⏭ Skipping test: need at least 2 nodes");
        return;
    }

    let namespace = generate_unique_namespace("cluster_fread");
    let table_name = "read_during_flush";
    let full_table = format!("{}.{}", namespace, table_name);

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    // Setup
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!("CREATE SHARED TABLE {} (id INT PRIMARY KEY, data TEXT)", full_table),
    )
    .expect("Failed to create table");

    // Wait for table to replicate
    for _ in 0..10 {
        if wait_for_table_on_all_nodes(&namespace, table_name, 500) {
            break;
        }
    }

    // Insert data
    println!("  1. Inserting 50 rows...");
    for i in 0..50 {
        execute_on_node(
            &urls[0],
            &format!("INSERT INTO {} (id, data) VALUES ({}, 'data_{}')", full_table, i, i),
        )
        .expect("Failed to insert");
    }

    // Start flush and immediately query from another node
    println!("  2. Flushing and querying concurrently...");
    let _ = execute_on_node(&urls[0], &format!("STORAGE FLUSH TABLE {}", full_table));

    // Query from another node during flush
    let read_result = execute_on_node(&urls[1], &format!("SELECT count(*) FROM {}", full_table));
    assert!(read_result.is_ok(), "Read during flush should not fail");

    // Wait for flush to complete
    std::thread::sleep(Duration::from_secs(3));

    // Verify final state
    println!("  3. Verifying final state...");
    let count_sql = format!("SELECT count(*) FROM {}", full_table);
    for (idx, url) in urls.iter().enumerate() {
        let count = query_count_on_url(url, &count_sql);
        assert_eq!(count, 50, "Expected 50 rows on node {}, got {}", idx, count);
        println!("     Node {}: {} rows ✓", idx, count);
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Cluster flush during reads test passed\n");
}
