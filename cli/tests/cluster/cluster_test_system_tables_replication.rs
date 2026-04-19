//! Cluster System Tables Replication Tests
//!
//! Tests that verify system table data is properly replicated across all cluster nodes.
//! System tables include: system.schemas, system.users, system.namespaces, system.cluster, etc.
//!
//! These tests ensure that:
//! 1. System table metadata is identical across all nodes
//! 2. CRUD operations on system tables propagate to all nodes
//! 3. System table queries return consistent results from any node

use crate::cluster_common::*;
use crate::common::*;
use std::collections::HashSet;
use std::time::Duration;

/// Helper to extract rows from query response as a set of normalized strings
fn extract_row_set(base_url: &str, sql: &str) -> Result<HashSet<String>, String> {
    let response = execute_on_node_response(base_url, sql)?;
    let result = response.results.first().ok_or_else(|| "Missing query result".to_string())?;
    let rows = result.rows.as_ref().ok_or_else(|| "Missing row data".to_string())?;

    let mut set = HashSet::new();
    for row in rows {
        let normalized: Vec<String> =
            row.iter().map(|v| extract_typed_value(v).to_string()).collect();
        set.insert(normalized.join("|"));
    }
    Ok(set)
}

/// Test: system.schemas is identical across all nodes
#[test]
fn cluster_test_system_tables_replication() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: system.schemas Replication ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("sys_tbl_repl");

    // Setup: Create namespace and tables on first node
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create multiple table types
    execute_on_node(
        &urls[0],
        &format!(
            "CREATE USER TABLE {}.users_data (id BIGINT PRIMARY KEY, name STRING)",
            namespace
        ),
    )
    .expect("Failed to create user table");

    execute_on_node(
        &urls[0],
        &format!(
            "CREATE SHARED TABLE {}.config_data (id BIGINT PRIMARY KEY, value STRING)",
            namespace
        ),
    )
    .expect("Failed to create shared table");

    execute_on_node(
        &urls[0],
        &format!("CREATE STREAM TABLE {}.events (id BIGINT PRIMARY KEY, payload STRING) WITH (TTL_SECONDS = 3600)", namespace),
    ).expect("Failed to create stream table");

    // Wait for all tables to replicate
    let tables_to_check = vec!["users_data", "config_data", "events"];
    for table in &tables_to_check {
        if !wait_for_table_on_all_nodes(&namespace, table, 10000) {
            panic!("Table {} did not replicate to all nodes", table);
        }
    }
    println!("  ✓ All tables replicated to all nodes");

    // Verify system.schemas is identical on all nodes
    let query = format!(
        "SELECT table_name, table_type FROM system.schemas WHERE namespace_id = '{}' ORDER BY table_name",
        namespace
    );

    let mut all_sets: Vec<HashSet<String>> = Vec::new();
    for (i, url) in urls.iter().enumerate() {
        let mut rows = HashSet::new();
        for _ in 0..10 {
            match extract_row_set(url, &query) {
                Ok(set) if set.len() >= 3 => {
                    rows = set;
                    break;
                },
                Ok(set) => rows = set,
                Err(_) => {},
            }
        }
        println!("  Node {} has {} tables: {:?}", i, rows.len(), rows);
        all_sets.push(rows);
    }

    // Compare all sets - they should be identical
    let reference = &all_sets[0];
    assert!(!reference.is_empty(), "Reference node has no tables");

    for (i, set) in all_sets.iter().enumerate().skip(1) {
        let missing: HashSet<_> = reference.difference(set).collect();
        let extra: HashSet<_> = set.difference(reference).collect();

        assert!(
            missing.is_empty() && extra.is_empty(),
            "Node {} has different tables. Missing: {:?}, Extra: {:?}",
            i,
            missing,
            extra
        );
        println!("  ✓ Node {} matches reference node", i);
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ system.schemas is identical across all nodes\n");
}

/// Test: system.namespaces is identical across all nodes
#[test]
fn cluster_test_system_namespaces_replication() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: system.namespaces Replication ===\n");

    let urls = cluster_urls();
    let base_ns = generate_unique_namespace("ns_repl");

    // Create multiple namespaces
    let namespaces: Vec<String> = (0..5).map(|i| format!("{}_{}", base_ns, i)).collect();

    for ns in &namespaces {
        let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", ns));
    }

    for ns in &namespaces {
        execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", ns))
            .expect(&format!("Failed to create namespace {}", ns));
    }

    // Wait for replication
    std::thread::sleep(Duration::from_millis(1500));

    // Verify all namespaces exist on all nodes
    let query = format!(
        "SELECT namespace_id FROM system.namespaces WHERE namespace_id LIKE '{}%' ORDER BY namespace_id",
        base_ns
    );

    let mut all_sets: Vec<HashSet<String>> = Vec::new();
    for (i, url) in urls.iter().enumerate() {
        let mut rows = HashSet::new();
        for _ in 0..10 {
            match extract_row_set(url, &query) {
                Ok(set) if set.len() >= 5 => {
                    rows = set;
                    break;
                },
                Ok(set) => rows = set,
                Err(_) => {},
            }
        }
        println!("  Node {} has {} namespaces", i, rows.len());
        all_sets.push(rows);
    }

    let reference = &all_sets[0];
    assert!(reference.len() >= 5, "Reference node has only {} namespaces", reference.len());

    for (i, set) in all_sets.iter().enumerate().skip(1) {
        assert_eq!(set, reference, "Node {} has different namespaces than reference", i);
        println!("  ✓ Node {} matches reference node", i);
    }

    // Cleanup
    for ns in namespaces.iter().rev() {
        let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", ns));
    }

    println!("\n  ✅ system.namespaces is identical across all nodes\n");
}

/// Test: system.users is identical across all nodes after user creation
#[test]
fn cluster_test_system_users_replication() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: system.users Replication ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("usr_repl");

    // Setup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Wait for namespace to replicate
    if !wait_for_namespace_on_all_nodes(&namespace, 10000) {
        panic!("Namespace {} did not replicate to all nodes", namespace);
    }

    // Create test users with correct syntax (no quotes around user, no NAMESPACE clause)
    let users: Vec<String> = (0..3).map(|i| format!("test_user_{}_{}", namespace, i)).collect();

    for user in &users {
        let sql = format!("CREATE USER {} WITH PASSWORD 'test_password_123' ROLE 'user'", user);
        execute_on_node(&urls[0], &sql)
            .unwrap_or_else(|e| panic!("Failed to create user {}: {}", user, e));
    }

    // Wait for replication
    std::thread::sleep(Duration::from_millis(1500));

    // Verify users exist on all nodes
    for (i, url) in urls.iter().enumerate() {
        for user in &users {
            let query = format!("SELECT user_id FROM system.users WHERE user_id = '{}'", user);

            let mut found = false;
            for _ in 0..10 {
                match execute_on_node(url, &query) {
                    Ok(result) if result.contains(user) => {
                        found = true;
                        break;
                    },
                    _ => std::thread::sleep(Duration::from_millis(20)),
                }
            }

            assert!(found, "User {} not found on node {}", user, i);
        }
        println!("  ✓ Node {} has all {} users", i, users.len());
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ system.users is identical across all nodes\n");
}

/// Test: system.cluster shows consistent view from all nodes
#[test]
fn cluster_test_system_cluster_consistency() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: system.cluster Consistency ===\n");

    let urls = cluster_urls();

    // Query cluster info from each node (ignore leader flag to avoid duplicate rows)
    let query = "SELECT node_id, api_addr FROM system.cluster ORDER BY node_id";

    let mut cluster_views: Vec<HashSet<String>> = Vec::new();
    for (i, url) in urls.iter().enumerate() {
        match extract_row_set(url, query) {
            Ok(set) => {
                println!("  Node {} sees {} cluster members", i, set.len());
                cluster_views.push(set);
            },
            Err(e) => {
                panic!("Node {} failed to query system.cluster: {}", i, e);
            },
        }
    }

    // All nodes should see the same cluster configuration
    let reference = &cluster_views[0];
    assert!(
        reference.len() >= 3,
        "Expected at least 3 cluster members, got {}",
        reference.len()
    );

    for (i, view) in cluster_views.iter().enumerate().skip(1) {
        let diff: HashSet<_> = reference.symmetric_difference(view).collect();
        assert!(diff.is_empty(), "Node {} has different cluster view: {:?}", i, diff);
        println!("  ✓ Node {} has consistent cluster view", i);
    }

    // Verify exactly one leader
    for (i, url) in urls.iter().enumerate() {
        let leader_count = query_count_on_url(
            url,
            "SELECT count(*) as count FROM system.cluster WHERE is_leader = true",
        );
        assert_eq!(leader_count, 1, "Node {} sees {} leaders, expected 1", i, leader_count);
    }
    println!("  ✓ All nodes agree on single leader");

    println!("\n  ✅ system.cluster is consistent across all nodes\n");
}

/// Test: ALTER TABLE is replicated to all nodes
#[test]
fn cluster_test_alter_table_replication() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: ALTER TABLE Replication ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("alter_repl");

    // Setup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!(
            "CREATE SHARED TABLE {}.alter_test (id BIGINT PRIMARY KEY, name STRING)",
            namespace
        ),
    )
    .expect("Failed to create table");

    // Wait for table to replicate
    if !wait_for_table_on_all_nodes(&namespace, "alter_test", 10000) {
        panic!("Table alter_test did not replicate to all nodes");
    }
    println!("  ✓ Table replicated to all nodes");

    // Add a column via ALTER TABLE
    execute_on_node(
        &urls[0],
        &format!("ALTER TABLE {}.alter_test ADD COLUMN age BIGINT", namespace),
    )
    .expect("Failed to alter table");

    // Wait for replication
    std::thread::sleep(Duration::from_millis(1500));

    // Verify new column exists on all nodes by attempting to use it
    // Note: information_schema.columns may not reflect dynamically added columns,
    // so we verify by actually querying the column in a SELECT statement
    let verify_query = format!("SELECT id, name, age FROM {}.alter_test LIMIT 1", namespace);

    for (i, url) in urls.iter().enumerate() {
        let mut found_age = false;
        for _ in 0..15 {
            match execute_on_node(url, &verify_query) {
                Ok(result) => {
                    // If the query succeeds, the column exists
                    // (query would fail with "column not found" if age doesn't exist)
                    if !result.contains("column not found") && !result.contains("Unknown column") {
                        found_age = true;
                        break;
                    }
                },
                Err(e) => {
                    let err_str = e.to_string();
                    // If error contains "not found" or "unknown column", column doesn't exist yet
                    if !err_str.contains("not found") && !err_str.contains("nknown column") {
                        // Some other error - might be worth investigating
                        println!("  Query error on node {}: {}", i, err_str);
                    }
                },
            }
        }
        assert!(found_age, "Node {} does not have 'age' column", i);
        println!("  ✓ Node {} has updated schema", i);
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ ALTER TABLE replicated to all nodes\n");
}

/// Test: DROP operations are replicated to all nodes
#[test]
fn cluster_test_drop_replication() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: DROP Operation Replication ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("drop_repl");

    // Setup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!("CREATE SHARED TABLE {}.drop_test (id BIGINT PRIMARY KEY)", namespace),
    )
    .expect("Failed to create table");

    // Wait for table to replicate to all nodes
    if !wait_for_table_on_all_nodes(&namespace, "drop_test", 10000) {
        panic!("Table drop_test did not replicate to all nodes");
    }

    // Verify table exists on all nodes first
    for (i, url) in urls.iter().enumerate() {
        let mut found = false;
        for _ in 0..10 {
            let query = format!(
                "SELECT table_name FROM system.schemas WHERE table_name = 'drop_test' AND namespace_id = '{}'",
                namespace
            );
            match execute_on_node(url, &query) {
                Ok(result) if result.contains("drop_test") => {
                    found = true;
                    break;
                },
                _ => std::thread::sleep(Duration::from_millis(20)),
            }
        }
        assert!(found, "Table not found on node {} before drop", i);
    }
    println!("  ✓ Table exists on all nodes before drop");

    // Drop the table
    execute_on_node(&urls[0], &format!("DROP TABLE {}.drop_test", namespace))
        .expect("Failed to drop table");

    // Wait for replication
    std::thread::sleep(Duration::from_millis(1500));

    // Verify table is gone from all nodes
    for (i, url) in urls.iter().enumerate() {
        let mut gone = false;
        for _ in 0..10 {
            let query = format!(
                "SELECT table_name FROM system.schemas WHERE table_name = 'drop_test' AND namespace_id = '{}'",
                namespace
            );
            match execute_on_node(url, &query) {
                Ok(result) if !result.contains("drop_test") || result.contains("\"rows\":[]") => {
                    gone = true;
                    break;
                },
                _ => std::thread::sleep(Duration::from_millis(20)),
            }
        }
        assert!(gone, "Table still exists on node {} after drop", i);
        println!("  ✓ Table removed from node {}", i);
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ DROP operation replicated to all nodes\n");
}
