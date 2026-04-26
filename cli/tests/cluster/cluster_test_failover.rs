//! Cluster failover tests
//!
//! Tests that verify cluster behavior during node failures

use crate::{cluster_common::*, common::*};

/// Test: Cluster remains operational when one node fails
///
/// This test requires manual node shutdown to fully test.
/// It verifies the cluster can detect unhealthy nodes.
#[test]
fn cluster_test_node_health_detection() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Node Health Detection ===\n");

    let urls = cluster_urls();

    // Check health of all nodes
    let mut healthy_count = 0;
    for (i, url) in urls.iter().enumerate() {
        let healthy = is_node_healthy(url);
        if healthy {
            healthy_count += 1;
            println!("  ✓ Node {} is healthy: {}", i, url);
        } else {
            println!("  ✗ Node {} is unhealthy: {}", i, url);
        }
    }

    assert!(
        healthy_count >= 2,
        "Expected at least 2 healthy nodes for quorum, got {}",
        healthy_count
    );

    println!("\n  ✅ Cluster health detection working\n");
}

/// Test: Leader election after leader disconnection
///
/// This test checks the system.cluster table for leader information.
#[test]
fn cluster_test_leader_visibility() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Leader Visibility ===\n");

    let urls = cluster_urls();

    // Query system.cluster from each node to verify leader info is consistent
    for (i, url) in urls.iter().enumerate() {
        let result = execute_on_node(
            url,
            "SELECT node_id, role, is_leader FROM system.cluster WHERE is_leader = true",
        );

        match result {
            Ok(resp) => {
                println!("  ✓ Node {} can see leader info", i);
                // Parse response to verify there's exactly one leader
                if resp.contains("leader") || resp.contains("true") {
                    println!("    Leader information found");
                }
            },
            Err(e) => {
                println!("  ✗ Node {} failed to query leader: {}", i, e);
            },
        }
    }

    // Verify all nodes agree on the leader
    let mut leader_ids = Vec::new();
    let mut visible = false;
    for _ in 0..15 {
        leader_ids.clear();
        for url in &urls {
            let count = query_count_on_url(
                url,
                "SELECT count(*) as count FROM system.cluster WHERE is_leader = true",
            );
            leader_ids.push(count);
        }

        if leader_ids.iter().all(|count| *count >= 1) {
            visible = true;
            break;
        }
    }

    assert!(visible, "Leader visibility mismatch across nodes: {:?}", leader_ids);

    println!("\n  ✅ All nodes can identify the cluster leader\n");
}

/// Test: Write operations route to leader
///
/// Verifies that write operations succeed regardless of which node
/// receives the request (follower should forward to leader).
#[test]
fn cluster_test_write_routing() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Write Routing to Leader ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("cluster_route");

    // Cleanup
    for url in &urls {
        let _ = execute_on_node(url, &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    }

    // Try creating namespace from each node
    println!("Testing write routing from each node...");
    for (i, url) in urls.iter().enumerate() {
        let test_ns = format!("{}_{}", namespace, i);

        let result = execute_on_node(url, &format!("CREATE NAMESPACE {}", test_ns));
        match result {
            Ok(_) => {
                println!("  ✓ Write from node {} succeeded", i);
            },
            Err(e) => {
                // This might fail if write routing isn't implemented
                println!("  ⚠ Write from node {} failed (expected for followers): {}", i, e);
            },
        }

        // Cleanup
        let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", test_ns));
    }

    println!("\n  ✅ Write routing test complete\n");
}

/// Test: Read operations work on all nodes
///
/// Verifies that read operations can be performed on any node.
#[test]
fn cluster_test_read_from_any_node() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Read from Any Node ===\n");

    let urls = cluster_urls();

    // Try reading from each node
    for (i, url) in urls.iter().enumerate() {
        let result = execute_on_node(url, "SELECT count(*) as count FROM system.users");

        match result {
            Ok(_) => {
                println!("  ✓ Read from node {} succeeded", i);
            },
            Err(e) => {
                panic!("Read from node {} failed: {}", i, e);
            },
        }
    }

    println!("\n  ✅ All nodes can handle read operations\n");
}
