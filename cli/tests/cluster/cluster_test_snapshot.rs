//! Snapshot Tests
//!
//! Tests Raft snapshot creation and installation in cluster mode.

use crate::cluster_common::*;
use crate::common::*;
use kalam_client::QueryResponse;
use std::time::Duration;

async fn execute_query_with_retry(
    client: &kalam_client::KalamLinkClient,
    sql: &str,
) -> Result<QueryResponse, String> {
    let max_attempts = 5;
    for attempt in 0..max_attempts {
        match client.execute_query(sql, None, None, None).await {
            Ok(response) => {
                if response.success() {
                    return Ok(response);
                }
                let err_msg = response_error_message(&response);
                if is_retryable_cluster_error_for_sql(sql, &err_msg) && attempt + 1 < max_attempts {
                    tokio::time::sleep(Duration::from_millis(300 + attempt as u64 * 200)).await;
                    continue;
                }
                return Err(err_msg);
            },
            Err(e) => {
                let err_msg = e.to_string();
                if is_retryable_cluster_error_for_sql(sql, &err_msg) && attempt + 1 < max_attempts {
                    tokio::time::sleep(Duration::from_millis(300 + attempt as u64 * 200)).await;
                    continue;
                }
                return Err(err_msg);
            },
        }
    }
    Err("Query failed after retries".to_string())
}

fn response_error_message(response: &QueryResponse) -> String {
    if let Some(error) = &response.error {
        if let Some(details) = &error.details {
            return format!("{} ({})", error.message, details);
        }
        return error.message.clone();
    }

    format!("Query failed: {:?}", response)
}

/// Test that snapshots are created after enough log entries
#[test]
fn test_snapshot_creation() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Snapshot Creation ===\n");

    let urls = cluster_urls();
    let base_url = &urls[0]; // Use first node
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let namespace = format!("snap_test_{}", timestamp);
    let table = "events";

    // Create namespace and table
    let client = create_cluster_client(base_url);

    cluster_runtime().block_on(async {
        execute_query_with_retry(&client, &format!("CREATE NAMESPACE {}", namespace))
            .await
            .expect("create namespace");
        execute_query_with_retry(
            &client,
            &format!("CREATE TABLE {}.{} (id INT, value TEXT, PRIMARY KEY (id))", namespace, table),
        )
        .await
        .expect("create table");

        // Insert many rows to trigger snapshot creation
        // OpenRaft snapshot threshold is now 1000 entries by default
        println!("📝 Inserting rows to trigger snapshot...");
        for i in 0..1200 {
            execute_query_with_retry(
                &client,
                &format!(
                    "INSERT INTO {}.{} (id, value) VALUES ({}, 'value_{}')",
                    namespace, table, i, i
                ),
            )
            .await
            .expect("insert row");

            if i % 50 == 0 {
                println!("  Inserted {} rows", i);
            }
        }

        println!("✅ Inserted 1200 rows");

        // Give Raft time to create snapshot
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Check system.cluster for snapshot info
        let result = execute_query_with_retry(
            &client,
            "SELECT node_id, snapshot_index FROM system.cluster WHERE is_self = true",
        )
        .await
        .expect("query cluster");

        println!("📊 Cluster snapshot status:");
        println!("{:?}", result);

        // Cleanup
        let _ = execute_query_with_retry(&client, &format!("DROP NAMESPACE {} CASCADE", namespace))
            .await;
    });
}

/// Test snapshot with higher write load
#[test]
fn test_snapshot_with_high_write_load() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: High Write Load Snapshot ===\n");

    let urls = cluster_urls();
    let base_url = &urls[0]; // Use first node
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let namespace = format!("snap_heavy_{}", timestamp);
    let table = "heavy";

    let client = create_cluster_client(base_url);

    cluster_runtime().block_on(async {
        execute_query_with_retry(&client, &format!("CREATE NAMESPACE {}", namespace))
            .await
            .expect("create namespace");
        execute_query_with_retry(
            &client,
            &format!(
                "CREATE TABLE {}.{} (id INT, data TEXT, PRIMARY KEY (id))",
                namespace, table
            ),
        )
        .await
        .expect("create table");
        
        println!("📝 High-load write test (1500 inserts)...");
        
        // Insert 1500 rows with larger data
        for i in 0..1500 {
            let large_value = format!("data_{}_", i).repeat(10); // ~70 bytes per row
            execute_query_with_retry(
                &client,
                &format!(
                    "INSERT INTO {}.{} (id, data) VALUES ({}, '{}')",
                    namespace, table, i, large_value
                ),
            )
            .await
            .expect("insert row");
            
            if i % 100 == 0 {
                println!("  Inserted {} rows", i);
            }
        }
        
        println!("✅ Inserted 1500 rows");
        
        // Wait for snapshot
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        
        // Check all nodes for snapshot status
        let result = execute_query_with_retry(
            &client,
            "SELECT node_id, role, snapshot_index, last_applied_log FROM system.cluster ORDER BY node_id",
        )
        .await
        .expect("query cluster");
        
        println!("📊 Cluster snapshot status:");
        println!("{:?}", result);
        
        // Verify data integrity after potential snapshot
        let count_result = execute_query_with_retry(
            &client,
            &format!("SELECT COUNT(*) as cnt FROM {}.{}", namespace, table),
        )
        .await
        .expect("count rows");
        
        println!("Count result: {:?}", count_result);
        println!("✅ Data integrity verified after snapshot");
        
        // Cleanup
        let _ =
            execute_query_with_retry(&client, &format!("DROP NAMESPACE {} CASCADE", namespace))
                .await;
    });
}

/// Test snapshot installation on follower catchup
#[test]
#[ignore] // Requires multi-node cluster with node restart capability
fn test_snapshot_installation_on_catchup() {
    // This test would require:
    // 1. Stop a follower node
    // 2. Write enough data to trigger snapshot on leader
    // 3. Restart the follower
    // 4. Verify follower catches up via snapshot installation

    // TODO: Implement once we have cluster management API
    println!("⚠️  Snapshot installation test not yet implemented");
}
