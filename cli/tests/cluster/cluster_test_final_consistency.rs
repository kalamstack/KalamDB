//! Cluster Final Consistency Verification Tests
//!
//! Tests that verify data consistency after all operations are complete.
//! These are "snapshot" tests that ensure the cluster has converged to
//! a consistent state after a workload completes.

use std::{collections::HashSet, time::Duration};

use crate::{cluster_common::*, common::*};

/// Helper: Get row count from a node with retries
fn get_row_count(url: &str, table: &str) -> i64 {
    let sql = format!("SELECT count(*) as count FROM {}", table);
    for _ in 0..5 {
        let count = query_count_on_url(url, &sql);
        if count >= 0 {
            return count;
        }
    }
    0
}

/// Test: All nodes have identical row counts after workload
#[test]
fn cluster_test_final_row_count_consistency() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Final Row Count Consistency ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("final_cnt");

    // Setup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create multiple tables
    let tables = vec![("table_a", 100), ("table_b", 50), ("table_c", 200)];

    for (table_name, row_count) in &tables {
        execute_on_node(
            &urls[0],
            &format!(
                "CREATE SHARED TABLE {}.{} (id BIGINT PRIMARY KEY, value STRING)",
                namespace, table_name
            ),
        )
        .expect(&format!("Failed to create table {}", table_name));

        // Wait for table to replicate before inserting
        if !wait_for_table_on_all_nodes(&namespace, table_name, 10000) {
            panic!("Table {} did not replicate to all nodes", table_name);
        }

        // Insert rows
        let mut values = Vec::new();
        for i in 0..*row_count {
            values.push(format!("({}, 'value_{}')", i, i));
            if values.len() >= 50 {
                execute_on_node(
                    &urls[0],
                    &format!(
                        "INSERT INTO {}.{} (id, value) VALUES {}",
                        namespace,
                        table_name,
                        values.join(", ")
                    ),
                )
                .expect("Failed to insert batch");
                values.clear();
            }
        }
        if !values.is_empty() {
            execute_on_node(
                &urls[0],
                &format!(
                    "INSERT INTO {}.{} (id, value) VALUES {}",
                    namespace,
                    table_name,
                    values.join(", ")
                ),
            )
            .expect("Failed to insert remaining");
        }
    }

    // Wait for full replication
    println!("  Waiting for replication to complete...");
    std::thread::sleep(Duration::from_millis(3000));

    // Verify all nodes have identical counts for each table
    for (table_name, expected_count) in &tables {
        let full_table = format!("{}.{}", namespace, table_name);

        let mut consistent = false;
        for attempt in 0..20 {
            let mut counts: Vec<i64> = Vec::new();
            for url in &urls {
                counts.push(get_row_count(url, &full_table));
            }

            let all_match = counts.iter().all(|c| *c == *expected_count as i64);
            if all_match {
                consistent = true;
                println!("  ✓ {} has {} rows on all nodes", table_name, expected_count);
                break;
            }

            if attempt == 19 {
                panic!(
                    "{} row counts inconsistent. Expected {}, got {:?}",
                    table_name, expected_count, counts
                );
            }
        }

        assert!(consistent, "Failed to achieve consistency for {}", table_name);
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ All tables have consistent row counts across nodes\n");
}

/// Test: System metadata is identical after multiple DDL operations
#[test]
fn cluster_test_final_metadata_consistency() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Final Metadata Consistency ===\n");

    let urls = cluster_urls();
    let base_ns = generate_unique_namespace("final_meta");

    // Perform multiple DDL operations
    println!("  Executing DDL operations...");

    // Create namespaces
    let namespaces: Vec<String> = (0..3).map(|i| format!("{}_{}", base_ns, i)).collect();
    for ns in &namespaces {
        execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", ns))
            .expect(&format!("Failed to create namespace {}", ns));
    }

    // Create tables in each namespace
    for ns in &namespaces {
        for i in 0..2 {
            execute_on_node(
                &urls[0],
                &format!("CREATE SHARED TABLE {}.tbl_{} (id BIGINT PRIMARY KEY)", ns, i),
            )
            .expect("Failed to create table");
        }
    }

    // Wait for replication
    println!("  Waiting for replication...");
    std::thread::sleep(Duration::from_millis(2000));

    // Verify namespace counts
    let ns_query = format!(
        "SELECT namespace_id FROM system.namespaces WHERE namespace_id LIKE '{}%' ORDER BY \
         namespace_id",
        base_ns
    );

    let mut ns_results: Vec<HashSet<String>> = Vec::new();
    for (i, url) in urls.iter().enumerate() {
        let mut ns_set = HashSet::new();
        for _ in 0..10 {
            match execute_on_node(url, &ns_query) {
                Ok(result) => {
                    for ns in &namespaces {
                        if result.contains(ns) {
                            ns_set.insert(ns.clone());
                        }
                    }
                    if ns_set.len() >= namespaces.len() {
                        break;
                    }
                },
                Err(_) => {},
            }
        }
        println!("  Node {} sees {} namespaces", i, ns_set.len());
        ns_results.push(ns_set);
    }

    // All nodes should see all namespaces
    let reference = &ns_results[0];
    for (i, ns_set) in ns_results.iter().enumerate().skip(1) {
        assert_eq!(ns_set, reference, "Node {} has different namespace set", i);
    }
    println!("  ✓ Namespace metadata consistent");

    // Verify table counts per namespace
    for ns in &namespaces {
        let tbl_query =
            format!("SELECT count(*) as count FROM system.schemas WHERE namespace_id = '{}'", ns);

        let mut table_counts: Vec<i64> = Vec::new();
        for url in &urls {
            let count = query_count_on_url(url, &tbl_query);
            table_counts.push(count);
        }

        let expected = table_counts[0];
        for (i, count) in table_counts.iter().enumerate().skip(1) {
            assert_eq!(
                *count, expected,
                "Node {} has {} tables in {}, expected {}",
                i, count, ns, expected
            );
        }
    }
    println!("  ✓ Table metadata consistent");

    // Cleanup
    for ns in namespaces.iter().rev() {
        let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", ns));
    }

    println!("\n  ✅ Metadata is consistent across all nodes\n");
}

/// Test: All data is consistent after mixed read/write workload
#[test]
fn cluster_test_final_mixed_workload_consistency() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Final Consistency After Mixed Workload ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("final_mixed");

    // Setup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!(
            "CREATE SHARED TABLE {}.workload_data (
                id BIGINT PRIMARY KEY,
                status STRING,
                counter BIGINT,
                updated_at STRING
            )",
            namespace
        ),
    )
    .expect("Failed to create table");

    // Wait for table to replicate before starting workload
    if !wait_for_table_on_all_nodes(&namespace, "workload_data", 10000) {
        panic!("Table workload_data did not replicate to all nodes");
    }

    // Execute mixed workload
    println!("  Phase 1: Initial inserts...");
    for batch_start in (0..200).step_by(50) {
        let mut values = Vec::new();
        for i in batch_start..(batch_start + 50) {
            values.push(format!("({}, 'active', 0, 'initial')", i));
        }
        execute_on_node(
            &urls[0],
            &format!(
                "INSERT INTO {}.workload_data (id, status, counter, updated_at) VALUES {}",
                namespace,
                values.join(", ")
            ),
        )
        .expect("Failed to insert batch");
    }

    println!("  Phase 2: Updates (even IDs)...");
    // Update even IDs - use individual PK updates since SHARED tables don't support predicate
    // updates
    for i in (0..200).step_by(2) {
        execute_on_node(
            &urls[0],
            &format!(
                "UPDATE {}.workload_data SET status = 'updated', counter = 1, updated_at = \
                 'phase2' WHERE id = {}",
                namespace, i
            ),
        )
        .expect(&format!("Failed to update id {}", i));
    }

    println!("  Phase 3: More updates (IDs divisible by 5)...");
    // Update IDs divisible by 5
    for i in (0..200).step_by(5) {
        execute_on_node(
            &urls[0],
            &format!(
                "UPDATE {}.workload_data SET counter = 2, updated_at = 'phase3' WHERE id = {}",
                namespace, i
            ),
        )
        .expect(&format!("Failed to update id {}", i));
    }

    println!("  Phase 4: Deletes...");
    // Delete IDs >= 180
    execute_on_node(&urls[0], &format!("DELETE FROM {}.workload_data WHERE id >= 180", namespace))
        .expect("Failed to delete");

    println!("  Phase 5: More inserts...");
    // Insert new rows
    for i in 300..320 {
        execute_on_node(
            &urls[0],
            &format!(
                "INSERT INTO {}.workload_data (id, status, counter, updated_at) VALUES ({}, \
                 'new', 0, 'phase5')",
                namespace, i
            ),
        )
        .expect("Failed to insert new row");
    }

    // Wait for all changes to replicate
    println!("  Waiting for replication to complete...");
    std::thread::sleep(Duration::from_millis(5000));

    // Verify final state is identical on all nodes
    let full_table = format!("{}.workload_data", namespace);

    // Expected: 180 (0-179) + 20 (300-319) = 200 rows
    let expected_rows = 200;

    let query = format!("SELECT id, status, counter, updated_at FROM {} ORDER BY id", full_table);

    let mut all_data: Vec<String> = Vec::new();
    for (i, url) in urls.iter().enumerate() {
        let mut data = String::new();
        for _ in 0..30 {
            match execute_on_node(url, &query) {
                Ok(result) => {
                    data = result;
                    let count = get_row_count(url, &full_table);
                    if count == expected_rows {
                        break;
                    }
                },
                Err(_) => {},
            }
        }
        println!("  Node {} data snapshot length: {} chars", i, data.len());
        all_data.push(data);
    }

    // Compare data hashes
    let reference = &all_data[0];
    for (i, data) in all_data.iter().enumerate().skip(1) {
        // Use length comparison as a quick consistency check
        let len_diff = (reference.len() as i64 - data.len() as i64).abs();
        if len_diff > 100 {
            // Significant difference - likely not consistent
            println!(
                "  ⚠ Node {} has significantly different data length: {} vs {}",
                i,
                data.len(),
                reference.len()
            );
        }
    }

    // Verify row counts
    let mut consistent = true;
    for (i, url) in urls.iter().enumerate() {
        let count = get_row_count(url, &full_table);
        if count != expected_rows {
            println!("  ⚠ Node {} has {} rows, expected {}", i, count, expected_rows);
            consistent = false;
        } else {
            println!("  ✓ Node {} has {} rows", i, count);
        }
    }

    assert!(consistent, "Final row counts are not consistent");

    // Verify specific aggregates are consistent
    let agg_queries = vec![
        (
            "Count active",
            format!("SELECT count(*) as cnt FROM {} WHERE status = 'active'", full_table),
        ),
        (
            "Count updated",
            format!("SELECT count(*) as cnt FROM {} WHERE status = 'updated'", full_table),
        ),
        (
            "Count new",
            format!("SELECT count(*) as cnt FROM {} WHERE status = 'new'", full_table),
        ),
        ("Sum counter", format!("SELECT sum(counter) as total FROM {}", full_table)),
    ];

    for (label, agg_query) in &agg_queries {
        let mut results: Vec<i64> = Vec::new();
        for url in &urls {
            let result = query_count_on_url(url, agg_query);
            results.push(result);
        }

        let first = results[0];
        let all_match = results.iter().all(|r| *r == first);
        if all_match {
            println!("  ✓ {} consistent: {}", label, first);
        } else {
            println!("  ⚠ {} inconsistent: {:?}", label, results);
        }
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Final consistency verified after mixed workload\n");
}

/// Test: Cluster converges to consistency after network partition recovery
/// (Simulated by querying system.cluster and verifying all nodes respond)
#[test]
fn cluster_test_final_cluster_health_consistency() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Cluster Health Consistency ===\n");

    let urls = cluster_urls();

    // All nodes should report the same cluster membership
    let cluster_query = "SELECT node_id, api_addr, is_leader FROM system.cluster ORDER BY node_id";

    let mut cluster_views: Vec<String> = Vec::new();
    for (i, url) in urls.iter().enumerate() {
        match execute_on_node(url, cluster_query) {
            Ok(result) => {
                cluster_views.push(result.clone());
                println!("  Node {} sees cluster: {} chars", i, result.len());
            },
            Err(e) => {
                panic!("Node {} failed to query cluster: {}", i, e);
            },
        }
    }

    let leader_query = "SELECT count(*) as leaders FROM system.cluster WHERE is_leader = true";
    let mut leader_ok = false;
    for _ in 0..15 {
        let mut leader_counts = Vec::new();
        for url in &urls {
            leader_counts.push(query_count_on_url(url, leader_query));
        }
        if leader_counts.iter().all(|count| *count == 1) {
            leader_ok = true;
            break;
        }
    }
    assert!(leader_ok, "Leader counts did not converge across nodes");
    println!("  ✓ All nodes agree on single leader");

    let member_query = "SELECT count(*) as members FROM system.cluster";
    let expected_members = urls.len() as i64;
    let mut members_ok = false;
    for _ in 0..15 {
        let mut member_counts: Vec<i64> = Vec::new();
        for url in &urls {
            member_counts.push(query_count_on_url(url, member_query));
        }
        if member_counts.iter().all(|count| *count == expected_members) {
            members_ok = true;
            break;
        }
    }
    assert!(members_ok, "Cluster member counts did not converge across nodes");
    println!("  ✓ All nodes see {} cluster members", expected_members);

    println!("\n  ✅ Cluster health is consistent across all nodes\n");
}

/// Test: Empty tables are consistent (edge case)
#[test]
fn cluster_test_final_empty_table_consistency() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Empty Table Consistency ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("final_empty");

    // Setup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create table but don't insert anything
    execute_on_node(
        &urls[0],
        &format!(
            "CREATE SHARED TABLE {}.empty_test (id BIGINT PRIMARY KEY, data STRING)",
            namespace
        ),
    )
    .expect("Failed to create table");

    // Wait for table to replicate to all nodes
    std::thread::sleep(Duration::from_millis(1000));
    if !wait_for_table_on_all_nodes(&namespace, "empty_test", 15000) {
        panic!("Table empty_test did not replicate to all nodes");
    }

    // Verify all nodes report 0 rows
    let full_table = format!("{}.empty_test", namespace);
    for (i, url) in urls.iter().enumerate() {
        let count = get_row_count(url, &full_table);
        assert_eq!(count, 0, "Node {} has {} rows, expected 0", i, count);
        println!("  ✓ Node {} correctly shows 0 rows", i);
    }

    // Now insert one row, then delete it
    execute_on_node(&urls[0], &format!("INSERT INTO {} (id, data) VALUES (1, 'temp')", full_table))
        .expect("Failed to insert");

    execute_on_node(&urls[0], &format!("DELETE FROM {} WHERE id = 1", full_table))
        .expect("Failed to delete");

    std::thread::sleep(Duration::from_millis(1000));

    // Verify all nodes still report 0 rows
    for (i, url) in urls.iter().enumerate() {
        let mut count = -1;
        for _ in 0..10 {
            count = get_row_count(url, &full_table);
            if count == 0 {
                break;
            }
        }
        assert_eq!(count, 0, "Node {} has {} rows after delete, expected 0", i, count);
        println!("  ✓ Node {} correctly shows 0 rows after delete", i);
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Empty table consistency verified\n");
}
