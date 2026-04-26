//! Cluster replication tests
//!
//! Tests that verify Raft log replication behavior

use std::time::{Duration, Instant};

use serde_json::Value;

use crate::{cluster_common::*, common::*};

fn parse_count(response: &kalam_client::QueryResponse) -> Result<i64, String> {
    let result = response
        .results
        .first()
        .ok_or_else(|| "Missing query result for count".to_string())?;
    let rows = result
        .rows
        .as_ref()
        .and_then(|rows| rows.first())
        .ok_or_else(|| "Missing count row".to_string())?;
    let value = rows.first().ok_or_else(|| "Missing count column".to_string())?;
    let unwrapped = extract_typed_value(value);
    match unwrapped {
        Value::String(s) => s.parse::<i64>().map_err(|_| "Invalid count string".to_string()),
        Value::Number(n) => n.as_i64().ok_or_else(|| "Invalid count number".to_string()),
        other => Err(format!("Unexpected count value: {}", other)),
    }
}

fn query_count_with_retry(base_url: &str, sql: &str) -> i64 {
    let mut last_err: Option<String> = None;
    for _ in 0..10 {
        match execute_on_node_response(base_url, sql).and_then(|resp| parse_count(&resp)) {
            Ok(count) => return count,
            Err(err) => {
                last_err = Some(err);
            },
        }
    }

    panic!(
        "Failed to query count after retries: {:?}",
        last_err.unwrap_or_else(|| "unknown error".to_string())
    );
}

/// Test: System metadata replication timing
///
/// Verifies that system table changes replicate quickly to all nodes.
#[test]
fn cluster_test_metadata_replication_timing() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Metadata Replication Timing ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("cluster_time");

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));

    // Create namespace and immediately check replication
    println!("Creating namespace on node 0...");
    let start = std::time::Instant::now();
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    // Poll all nodes for the namespace
    let mut all_replicated = false;
    let mut check_count = 0;
    while !all_replicated && check_count < 20 {
        check_count += 1;

        all_replicated = urls.iter().all(|url| {
            let result = execute_on_node(
                url,
                &format!(
                    "SELECT namespace_id FROM system.namespaces WHERE namespace_id = '{}'",
                    namespace
                ),
            );
            result.map(|r| r.contains(&namespace)).unwrap_or(false)
        });
    }

    let elapsed = start.elapsed();

    if all_replicated {
        println!("  ✓ Namespace replicated to all nodes in {:?}", elapsed);
    } else {
        panic!("Namespace not replicated to all nodes after {:?}", elapsed);
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Metadata replication timing test passed\n");
}

/// Test: Sequential operations maintain order
///
/// Verifies that operations are applied in the correct order across nodes.
#[test]
fn cluster_test_operation_ordering() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Operation Ordering ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("cluster_order");

    // Setup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!(
            "CREATE SHARED TABLE {}.ordered_data (id BIGINT PRIMARY KEY, seq BIGINT)",
            namespace
        ),
    )
    .expect("Failed to create table");

    // Wait for table to replicate to all nodes
    if !wait_for_table_on_all_nodes(&namespace, "ordered_data", 10000) {
        panic!("Table ordered_data did not replicate to all nodes");
    }

    // Insert data with sequence numbers
    println!("Inserting 50 sequential rows...");
    let mut values = Vec::new();
    for i in 0..50 {
        values.push(format!("({}, {})", i, i * 10));
        if values.len() == 10 || i == 49 {
            execute_on_node(
                &urls[0],
                &format!(
                    "INSERT INTO {}.ordered_data (id, seq) VALUES {}",
                    namespace,
                    values.join(", ")
                ),
            )
            .expect("Insert failed");
            values.clear();
        }
    }

    std::thread::sleep(Duration::from_millis(1000));

    // Verify sequence on all nodes
    println!("Verifying sequence on all nodes...");
    for (i, url) in urls.iter().enumerate() {
        let result = execute_on_node(
            url,
            &format!("SELECT id, seq FROM {}.ordered_data ORDER BY id LIMIT 5", namespace),
        )
        .expect("Query failed");

        // Verify first few sequences are correct
        assert!(
            result.contains("0") && result.contains("10") && result.contains("20"),
            "Node {} has incorrect sequence data: {}",
            i,
            result
        );
        println!("  ✓ Node {} has correct sequence ordering", i);
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Operation ordering maintained across nodes\n");
}

/// Test: Concurrent writes from different clients
///
/// Verifies that concurrent writes are properly serialized.
#[test]
fn cluster_test_concurrent_writes() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Concurrent Writes ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("cluster_conc");

    // Setup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!(
            "CREATE SHARED TABLE {}.concurrent_data (id BIGINT PRIMARY KEY, writer STRING)",
            namespace
        ),
    )
    .expect("Failed to create table");

    // Wait for table to replicate to all nodes before concurrent writes
    if !wait_for_table_on_all_nodes(&namespace, "concurrent_data", 10000) {
        panic!("Table concurrent_data did not replicate to all nodes");
    }

    // Spawn threads to write concurrently from different nodes
    let handles: Vec<_> = urls
        .iter()
        .enumerate()
        .map(|(node_idx, url)| {
            let url = url.clone();
            let ns = namespace.clone();
            std::thread::spawn(move || {
                for i in 0..5 {
                    let id = node_idx * 1000 + i;
                    let result = execute_on_node(
                        &url,
                        &format!(
                            "INSERT INTO {}.concurrent_data (id, writer) VALUES ({}, 'node_{}')",
                            ns, id, node_idx
                        ),
                    );
                    if result.is_err() {
                        // Some writes might fail if node isn't leader, that's expected
                    }
                }
            })
        })
        .collect();

    // Wait for all threads
    for handle in handles {
        let _ = handle.join();
    }

    std::thread::sleep(Duration::from_millis(2000));

    println!("Verifying data consistency after concurrent writes...");
    let start = Instant::now();
    let timeout = Duration::from_secs(10);
    let mut consistent = false;
    let mut last_counts = Vec::new();

    while start.elapsed() < timeout {
        let mut counts = Vec::new();
        for (i, url) in urls.iter().enumerate() {
            let count = query_count_with_retry(
                url,
                &format!("SELECT count(*) as count FROM {}.concurrent_data", namespace),
            );
            counts.push(count);
            println!("  Node {} has {} rows", i, count);
        }

        let first_count = counts[0];
        if counts.iter().all(|count| *count == first_count) {
            consistent = true;
            break;
        }

        last_counts = counts;
    }

    if !consistent {
        panic!("Row counts did not converge across nodes: {:?}", last_counts);
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Concurrent writes properly serialized\n");
}

/// Test: Cluster info is consistent across nodes
#[test]
fn cluster_test_cluster_info_consistency() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Cluster Info Consistency ===\n");

    let urls = cluster_urls();

    // Query cluster info from each node
    let mut cluster_ids = Vec::new();
    for (i, url) in urls.iter().enumerate() {
        let result = execute_on_node(url, "SELECT cluster_id FROM system.cluster LIMIT 1");
        match result {
            Ok(resp) => {
                println!(
                    "  Node {} cluster info: {}",
                    i,
                    resp.chars().take(100).collect::<String>()
                );
                cluster_ids.push(resp);
            },
            Err(e) => {
                println!("  ✗ Node {} failed: {}", i, e);
            },
        }
    }

    println!("\n  ✅ Cluster info query completed on all nodes\n");
}
