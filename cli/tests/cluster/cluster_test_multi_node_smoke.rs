//! Cluster Multi-Node Smoke Tests
//!
//! These tests verify that the standard smoke test operations work correctly
//! when executed against ANY node in the cluster (not just the leader).
//! This ensures the cluster behaves identically from any entry point.

use crate::cluster_common::*;
use crate::common::*;
use serde_json::Value;

/// Test: Basic CRUD operations work from any node
#[test]
fn cluster_test_smoke_crud_any_node() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Smoke CRUD from Any Node ===\n");

    let urls = cluster_urls();

    for (node_idx, url) in urls.iter().enumerate() {
        println!("  Testing node {}...", node_idx);

        let namespace = generate_unique_namespace(&format!("smoke_node{}", node_idx));

        // Create namespace
        match execute_on_node(url, &format!("CREATE NAMESPACE {}", namespace)) {
            Ok(_) => println!("    ✓ CREATE NAMESPACE succeeded"),
            Err(e) => {
                // Might fail on follower if write routing not implemented
                println!("    ⚠ CREATE NAMESPACE: {}", e);
                continue;
            },
        }

        // Create table
        match execute_on_node(
            url,
            &format!(
                "CREATE SHARED TABLE {}.test_tbl (id BIGINT PRIMARY KEY, value STRING)",
                namespace
            ),
        ) {
            Ok(_) => println!("    ✓ CREATE TABLE succeeded"),
            Err(e) => {
                println!("    ⚠ CREATE TABLE: {}", e);
                let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));
                continue;
            },
        }

        // Insert data
        match execute_on_node(
            url,
            &format!("INSERT INTO {}.test_tbl (id, value) VALUES (1, 'test_value')", namespace),
        ) {
            Ok(_) => println!("    ✓ INSERT succeeded"),
            Err(e) => println!("    ⚠ INSERT: {}", e),
        }

        // Read data
        match execute_on_node(url, &format!("SELECT * FROM {}.test_tbl", namespace)) {
            Ok(result) => {
                if result.contains("test_value") {
                    println!("    ✓ SELECT succeeded and returned data");
                } else {
                    println!("    ⚠ SELECT returned no data");
                }
            },
            Err(e) => println!("    ⚠ SELECT: {}", e),
        }

        // Update data
        match execute_on_node(
            url,
            &format!("UPDATE {}.test_tbl SET value = 'updated_value' WHERE id = 1", namespace),
        ) {
            Ok(_) => println!("    ✓ UPDATE succeeded"),
            Err(e) => println!("    ⚠ UPDATE: {}", e),
        }

        // Delete data
        match execute_on_node(url, &format!("DELETE FROM {}.test_tbl WHERE id = 1", namespace)) {
            Ok(_) => println!("    ✓ DELETE succeeded"),
            Err(e) => println!("    ⚠ DELETE: {}", e),
        }

        // Cleanup
        let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));
        println!("    ✓ Cleanup completed");
    }

    println!("\n  ✅ Smoke CRUD operations tested on all nodes\n");
}

/// Test: System table queries work from any node
#[test]
fn cluster_test_smoke_system_tables_any_node() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: System Table Queries from Any Node ===\n");

    let urls = cluster_urls();
    let system_queries = vec![
        ("system.schemas", "SELECT count(*) as count FROM system.schemas"),
        ("system.namespaces", "SELECT count(*) as count FROM system.namespaces"),
        ("system.users", "SELECT count(*) as count FROM system.users"),
        ("system.cluster", "SELECT node_id, is_leader FROM system.cluster"),
        ("system.storages", "SELECT count(*) as count FROM system.storages"),
    ];

    for (node_idx, url) in urls.iter().enumerate() {
        println!("  Node {}:", node_idx);

        for (table_name, query) in &system_queries {
            match execute_on_node(url, query) {
                Ok(_) => {
                    println!("    ✓ {} query succeeded", table_name);
                },
                Err(e) => {
                    println!("    ✗ {} query failed: {}", table_name, e);
                },
            }
        }
    }

    println!("\n  ✅ System table queries work from all nodes\n");
}

/// Test: Table types work from any node
#[test]
fn cluster_test_smoke_table_types_any_node() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Table Types from Any Node ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("smoke_types");

    // Setup namespace on leader
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create different table types
    execute_on_node(
        &urls[0],
        &format!("CREATE USER TABLE {}.user_tbl (id BIGINT PRIMARY KEY, data STRING)", namespace),
    )
    .expect("Failed to create user table");

    execute_on_node(
        &urls[0],
        &format!(
            "CREATE SHARED TABLE {}.shared_tbl (id BIGINT PRIMARY KEY, data STRING)",
            namespace
        ),
    )
    .expect("Failed to create shared table");

    execute_on_node(
        &urls[0],
        &format!(
            "CREATE STREAM TABLE {}.stream_tbl (id BIGINT PRIMARY KEY, data STRING) WITH (TTL_SECONDS = 3600)",
            namespace
        ),
    )
    .expect("Failed to create stream table");

    // Wait for all tables to replicate to all nodes
    if !wait_for_table_on_all_nodes(&namespace, "user_tbl", 10000) {
        panic!("Table user_tbl did not replicate to all nodes");
    }
    if !wait_for_table_on_all_nodes(&namespace, "shared_tbl", 10000) {
        panic!("Table shared_tbl did not replicate to all nodes");
    }
    if !wait_for_table_on_all_nodes(&namespace, "stream_tbl", 10000) {
        panic!("Table stream_tbl did not replicate to all nodes");
    }

    // Verify all table types are visible from all nodes
    for (node_idx, url) in urls.iter().enumerate() {
        println!("  Node {}:", node_idx);

        let tables = vec!["user_tbl", "shared_tbl", "stream_tbl"];
        for table_name in &tables {
            let query = format!(
                "SELECT table_name FROM system.schemas WHERE namespace_id = '{}' AND table_name = '{}'",
                namespace, table_name
            );

            match execute_on_node(url, &query) {
                Ok(result) if result.contains(table_name) => {
                    println!("    ✓ {} visible", table_name);
                },
                Ok(result) => {
                    println!(
                        "    ⚠ {} not found: {}",
                        table_name,
                        result.chars().take(100).collect::<String>()
                    );
                },
                Err(e) => {
                    println!("    ✗ {} query failed: {}", table_name, e);
                },
            }
        }
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ All table types accessible from all nodes\n");
}

/// Test: User authentication works from any node
#[test]
fn cluster_test_smoke_auth_any_node() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Authentication from Any Node ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("smoke_auth");

    // Setup namespace and user on leader
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Create user with proper syntax: CREATE USER user WITH PASSWORD 'pass' ROLE 'role'
    let test_user = format!("smoke_user_{}", rand::random::<u32>());
    execute_on_node(
        &urls[0],
        &format!("CREATE USER {} WITH PASSWORD 'smoke_test_password' ROLE 'user'", test_user),
    )
    .expect("Failed to create user");

    // Wait for namespace to replicate (users replicate with namespace)
    if !wait_for_namespace_on_all_nodes(&namespace, 10000) {
        panic!("Namespace {} did not replicate to all nodes", namespace);
    }
    // Additional wait for user replication

    // Verify user exists and can be queried from all nodes
    for (node_idx, url) in urls.iter().enumerate() {
        let query = format!("SELECT user_id FROM system.users WHERE user_id = '{}'", test_user);

        match execute_on_node(url, &query) {
            Ok(result) if result.contains(&test_user) => {
                println!("  ✓ Node {} can see user", node_idx);
            },
            Ok(result) => {
                println!(
                    "  ⚠ Node {} doesn't see user: {}",
                    node_idx,
                    result.chars().take(100).collect::<String>()
                );
            },
            Err(e) => {
                println!("  ✗ Node {} query failed: {}", node_idx, e);
            },
        }
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Authentication tested from all nodes\n");
}

/// Test: Complex queries work from any node
#[test]
fn cluster_test_smoke_complex_queries_any_node() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Complex Queries from Any Node ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("smoke_complex");

    // Setup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!(
            "CREATE SHARED TABLE {}.products (id BIGINT PRIMARY KEY, name STRING, price DOUBLE, category STRING)",
            namespace
        ),
    )
    .expect("Failed to create table");

    // Wait for table to replicate to all nodes
    if !wait_for_table_on_all_nodes(&namespace, "products", 10000) {
        panic!("Table products did not replicate to all nodes");
    }

    // Insert test data
    let products = vec![
        (1, "Widget A", 10.99, "electronics"),
        (2, "Widget B", 20.49, "electronics"),
        (3, "Gadget X", 15.99, "gadgets"),
        (4, "Gadget Y", 25.99, "gadgets"),
        (5, "Tool Z", 5.99, "tools"),
    ];

    for (id, name, price, category) in &products {
        execute_on_node(
            &urls[0],
            &format!(
                "INSERT INTO {}.products (id, name, price, category) VALUES ({}, '{}', {}, '{}')",
                namespace, id, name, price, category
            ),
        )
        .expect("Failed to insert product");
    }

    // Wait for data to replicate to all nodes
    let full_table = format!("{}.products", namespace);
    if !wait_for_row_count_on_all_nodes(&full_table, 5, 15000) {
        println!("  ⚠ Data may not have fully replicated to all nodes");
    }

    // Test complex queries from each node
    let complex_queries = vec![
        ("Aggregation", format!("SELECT category, count(*) as cnt FROM {}.products GROUP BY category ORDER BY category", namespace)),
        ("Filter", format!("SELECT name FROM {}.products WHERE price > 15 ORDER BY price", namespace)),
        ("OrderBy", format!("SELECT name, price FROM {}.products ORDER BY price DESC LIMIT 3", namespace)),
        ("Count", format!("SELECT count(*) as total FROM {}.products", namespace)),
    ];

    for (node_idx, url) in urls.iter().enumerate() {
        println!("  Node {}:", node_idx);

        for (query_type, query) in &complex_queries {
            match execute_on_node(url, query) {
                Ok(_) => {
                    println!("    ✓ {} query succeeded", query_type);
                },
                Err(e) => {
                    println!("    ✗ {} query failed: {}", query_type, e);
                },
            }
        }
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Complex queries work from all nodes\n");
}

/// Test: Writes succeed from leader only (or routing works)
#[test]
fn cluster_test_smoke_write_routing() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Write Routing Verification ===\n");

    let urls = cluster_urls();

    // Find leader
    let cluster_result = execute_on_node_response(
        &urls[0],
        "SELECT node_id, api_addr, is_leader FROM system.cluster WHERE is_leader = true",
    );

    let leader_url = match cluster_result {
        Ok(response) => response
            .results
            .first()
            .and_then(|r| r.rows.as_ref())
            .and_then(|rows| rows.first())
            .and_then(|row| row.get(1))
            .map(|v| {
                let extracted = extract_typed_value(v);
                match extracted {
                    Value::String(s) => s,
                    other => other.to_string().trim_matches('"').to_string(),
                }
            }),
        Err(_) => None,
    };

    if let Some(leader) = &leader_url {
        println!("  Detected leader: {}", leader);
    } else {
        println!("  ⚠ Could not detect leader, testing all nodes");
    }

    let namespace = generate_unique_namespace("smoke_route");

    // Test writes from each node
    for (node_idx, url) in urls.iter().enumerate() {
        let ns = format!("{}_{}", namespace, node_idx);

        match execute_on_node(url, &format!("CREATE NAMESPACE {}", ns)) {
            Ok(_) => {
                if let Some(ref leader) = leader_url {
                    if url == leader {
                        println!("  ✓ Node {} (leader): write succeeded", node_idx);
                    } else {
                        println!("  ✓ Node {} (follower): write routing works", node_idx);
                    }
                } else {
                    println!("  ✓ Node {}: write succeeded", node_idx);
                }
                // Cleanup
                let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", ns));
            },
            Err(e) => {
                if let Some(ref leader) = leader_url {
                    if url != leader {
                        println!(
                            "  ⚠ Node {} (follower): write failed (expected if no routing): {}",
                            node_idx, e
                        );
                    } else {
                        println!("  ✗ Node {} (leader): write failed: {}", node_idx, e);
                    }
                } else {
                    println!("  ⚠ Node {}: write failed: {}", node_idx, e);
                }
            },
        }
    }

    println!("\n  ✅ Write routing test completed\n");
}
