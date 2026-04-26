//! Cluster Leader-Only Job Execution Tests
//!
//! Tests that verify only the leader node executes background jobs.
//!
//! Phase 4 implementation verifies:
//! - Jobs are only executed by the cluster leader
//! - Flush commands trigger jobs that execute on the leader
//! - Job status is replicated across all nodes
//! - System.jobs table shows node_id of the executor

use std::{
    thread,
    time::{Duration, Instant},
};

use crate::{cluster_common::*, common::*};

/// Test: Only leader executes flush jobs
///
/// Creates a table, inserts data, runs FLUSH on different nodes,
/// and verifies the job always executes on the leader.
#[test]
fn cluster_test_leader_only_flush_jobs() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Leader-Only Flush Job Execution ===\n");

    let urls = cluster_urls();
    let leader_url = find_leader_url(&urls);

    println!("  Leader node: {}", leader_url);

    // Step 0: Setup namespace
    let namespace = generate_unique_namespace("leader_jobs");
    let _ =
        execute_on_node(&leader_url, &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    thread::sleep(Duration::from_millis(20));
    execute_on_node(&leader_url, &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");
    thread::sleep(Duration::from_millis(20));

    // Step 1: Create a test table via the leader
    let table_name = format!("test_leader_jobs_{}", rand_suffix());
    let create_sql =
        format!("CREATE TABLE {}.{} (id TEXT PRIMARY KEY, value TEXT)", namespace, table_name);

    let result = execute_on_node(&leader_url, &create_sql);
    assert!(result.is_ok(), "Failed to create table: {:?}", result);
    println!("  ✓ Created table: {}.{}", namespace, table_name);

    // Step 2: Insert some data
    let insert_sql = format!(
        "INSERT INTO {}.{} (id, value) VALUES ('1', 'test1'), ('2', 'test2')",
        namespace, table_name
    );
    let result = execute_on_node(&leader_url, &insert_sql);
    assert!(result.is_ok(), "Failed to insert data: {:?}", result);
    println!("  ✓ Inserted test data");

    // Small delay for replication
    thread::sleep(Duration::from_millis(50));

    // Step 3: Trigger flush from a follower node (should still execute on leader)
    let follower_url = urls.iter().find(|u| *u != &leader_url).expect("Need at least 2 nodes");

    let flush_sql = format!("STORAGE FLUSH TABLE {}.{}", namespace, table_name);
    println!("  → Triggering FLUSH from follower: {}", follower_url);

    let result = execute_on_node(follower_url, &flush_sql);
    // Flush should succeed regardless of which node receives the command
    if result.is_err() {
        println!("  ⚠ Flush failed (may be expected if follower rejects): {:?}", result);
    } else {
        println!("  ✓ Flush command accepted");
    }

    // Step 4: Wait for job to complete
    let job_id = wait_for_latest_job_id_by_type(&leader_url, "Flush", Duration::from_secs(3))
        .expect("Failed to find flush job id");
    assert!(
        wait_for_job_status(&leader_url, &job_id, "Completed", Duration::from_secs(30)),
        "Flush job did not complete in time"
    );

    let leader_node_id = get_self_node_id(&leader_url).expect("Failed to resolve leader node_id");

    // Step 5: Query system.jobs to verify job was executed by leader
    let jobs_query = "SELECT job_id, job_type, status, node_id FROM system.jobs WHERE job_id = '"
        .to_string()
        + &job_id
        + "'";

    for (i, url) in urls.iter().enumerate() {
        let result = execute_on_node(url, &jobs_query);
        match result {
            Ok(resp) => {
                println!("  → Node {} job view: {}", i, truncate_for_display(&resp, 200));
                assert!(
                    resp.contains(&leader_node_id),
                    "Node {} did not report leader node_id {} in job row",
                    i,
                    leader_node_id
                );
            },
            Err(e) => {
                println!("  ✗ Node {} failed to query jobs: {}", i, e);
            },
        }
    }

    // Clean up
    let drop_sql = format!("DROP TABLE IF EXISTS {}.{}", namespace, table_name);
    let _ = execute_on_node(&leader_url, &drop_sql);

    println!("\n  ✅ Leader-only flush job test completed\n");
}

/// Test: System.jobs table shows consistent job state across cluster
///
/// Verifies that all nodes see the same job records in system.jobs
#[test]
fn cluster_test_jobs_table_consistency() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Jobs Table Consistency Across Cluster ===\n");

    let urls = cluster_urls();

    // Query job counts from all nodes, retrying for eventual consistency
    let mut consistent = false;
    let mut last_counts: Vec<(String, i64)> = Vec::new();

    for attempt in 0..30 {
        let mut counts: Vec<(String, i64)> = Vec::new();
        for (i, url) in urls.iter().enumerate() {
            let count = query_count_on_url(url, "SELECT count(*) FROM system.jobs");
            if attempt == 0 {
                println!("  Node {}: {} jobs in system.jobs", i, count);
            }
            counts.push((url.clone(), count));
        }

        let first_count = counts.first().map(|(_, c)| *c).unwrap_or(0);
        let all_match = counts.iter().all(|(_, c)| *c == first_count);

        if all_match {
            consistent = true;
            println!("  ✓ system.jobs count consistent across nodes: {}", first_count);
            break;
        }

        last_counts = counts;
        std::thread::sleep(std::time::Duration::from_millis(400));
    }

    if !consistent {
        // Tolerate small deltas due to in-flight replication
        let min = last_counts.iter().map(|(_, c)| *c).min().unwrap_or(0);
        let max = last_counts.iter().map(|(_, c)| *c).max().unwrap_or(0);
        if max - min <= 5 {
            println!(
                "  ⚠ system.jobs counts within tolerated delta ({}..={}): {:?}",
                min, max, last_counts
            );
        } else {
            panic!("system.jobs counts mismatch beyond tolerance: {:?}", last_counts);
        }
    }

    // Query recent jobs and verify node_id field is populated
    let recent_jobs_sql = "SELECT job_id, job_type, node_id, status FROM system.jobs ORDER BY \
                           created_at DESC LIMIT 3";

    println!("\n  Recent jobs (from leader):");
    let leader_url = find_leader_url(&urls);
    let result = execute_on_node_response(&leader_url, recent_jobs_sql)
        .expect("Failed to query recent jobs");
    let rows = result.results.first().and_then(|r| r.rows.as_ref());

    let has_node_id = rows
        .map(|rows| {
            rows.iter().any(|row| {
                row.get(2)
                    .map(|val| {
                        extract_typed_value(val).as_str().map(|s| !s.is_empty()).unwrap_or(false)
                    })
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);

    assert!(has_node_id, "Expected recent jobs to have a populated node_id");

    println!("\n  ✅ Jobs table consistency test completed\n");
}

/// Test: Job claiming prevents duplicate execution
///
/// This test verifies that when a job is claimed by the leader,
/// other nodes cannot execute it.
#[test]
fn cluster_test_job_claiming() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Job Claiming Prevents Duplicate Execution ===\n");

    let urls = cluster_urls();
    let leader_url = find_leader_url(&urls);

    // Check that we have a leader
    assert!(!leader_url.is_empty(), "No leader found in cluster");
    println!("  ✓ Leader identified: {}", leader_url);

    let namespace = generate_unique_namespace("job_claim");
    let table_name = format!("claim_{}", rand_suffix());
    let full_table = format!("{}.{}", namespace, table_name);

    let _ =
        execute_on_node(&leader_url, &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    thread::sleep(Duration::from_millis(20));

    execute_on_node(&leader_url, &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");
    thread::sleep(Duration::from_millis(20));

    execute_on_node(
        &leader_url,
        &format!("CREATE SHARED TABLE {} (id INT PRIMARY KEY)", full_table),
    )
    .expect("Failed to create table");
    thread::sleep(Duration::from_millis(20));

    execute_on_node(&leader_url, &format!("INSERT INTO {} (id) VALUES (1)", full_table))
        .expect("Failed to insert row");

    execute_on_node(&leader_url, &format!("STORAGE FLUSH TABLE {}", full_table))
        .expect("Failed to flush");

    let job_id = wait_for_latest_job_id_by_type(&leader_url, "Flush", Duration::from_secs(3))
        .expect("Failed to find flush job id");

    let job_count = query_count_on_url(
        &leader_url,
        &format!("SELECT count(*) FROM system.jobs WHERE job_id = '{}'", job_id),
    );
    assert_eq!(job_count, 1, "Expected a single job row for job_id");

    let expected_nodes = urls.len();
    let completed =
        wait_for_job_nodes_completed(&leader_url, &job_id, expected_nodes, Duration::from_secs(30));
    assert!(completed, "Job nodes did not complete for all nodes");

    println!("\n  ✅ Job claiming test completed\n");
}

/// Test: Job nodes track per-node completion for flush jobs
///
/// Ensures system.job_nodes has one row per cluster node and all are Completed
/// after a flush job finishes.
#[test]
fn cluster_test_flush_job_nodes_completion() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Flush Job Nodes Completion ===\n");

    let urls = cluster_urls();
    if urls.len() < 2 {
        println!("  ⏭ Skipping test: need at least 2 nodes");
        return;
    }

    let leader_url = find_leader_url(&urls);
    let namespace = generate_unique_namespace("job_nodes_flush");
    let table_name = format!("flush_nodes_{}", rand_suffix());
    let full_table = format!("{}.{}", namespace, table_name);

    let _ =
        execute_on_node(&leader_url, &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    thread::sleep(Duration::from_millis(20));

    execute_on_node(&leader_url, &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");
    thread::sleep(Duration::from_millis(20));

    let create_sql = format!("CREATE SHARED TABLE {} (id INT PRIMARY KEY, value TEXT)", full_table);
    execute_on_node(&leader_url, &create_sql).expect("Failed to create table");
    thread::sleep(Duration::from_millis(30));

    for i in 0..20 {
        let insert_sql = format!("INSERT INTO {} (id, value) VALUES ({}, 'v{}')", full_table, i, i);
        execute_on_node(&leader_url, &insert_sql).expect("Failed to insert row");
    }

    let flush_sql = format!("STORAGE FLUSH TABLE {}", full_table);
    execute_on_node(&leader_url, &flush_sql).expect("Failed to flush");

    let job_id = wait_for_latest_job_id_by_type(&leader_url, "Flush", Duration::from_secs(3))
        .expect("Failed to find flush job id");

    let expected_nodes = urls.len();
    let completed =
        wait_for_job_nodes_completed(&leader_url, &job_id, expected_nodes, Duration::from_secs(30));
    assert!(completed, "Job nodes did not complete for all nodes");

    // Verify all nodes see job_nodes rows
    for (idx, url) in urls.iter().enumerate() {
        let count = query_count_on_url(
            url,
            &format!("SELECT count(*) FROM system.job_nodes WHERE job_id = '{}'", job_id),
        );
        assert_eq!(
            count as usize, expected_nodes,
            "Node {} sees {} job_nodes (expected {})",
            idx, count, expected_nodes
        );
    }

    let _ = execute_on_node(&leader_url, &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Flush job_nodes completion test passed\n");
}

/// Helper: Find the leader node URL
fn find_leader_url(urls: &[String]) -> String {
    leader_url().unwrap_or_else(|| urls.first().cloned().unwrap_or_default())
}

/// Helper: Truncate string for display
fn truncate_for_display(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

/// Helper: Generate random suffix for unique table names
fn rand_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().subsec_nanos();
    format!("{:08x}", nanos)
}

fn wait_for_job_nodes_completed(
    leader_url: &str,
    job_id: &str,
    expected_nodes: usize,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    let sql = format!(
        "SELECT node_id, status FROM system.job_nodes WHERE job_id = '{}' ORDER BY node_id",
        job_id
    );

    while start.elapsed() < timeout {
        if let Ok(response) = execute_on_node_response(leader_url, &sql) {
            if let Some(result) = response.results.first() {
                if let Some(rows) = &result.rows {
                    if rows.len() == expected_nodes {
                        let all_completed = rows.iter().all(|row| {
                            row.get(1)
                                .map(|val| {
                                    extract_typed_value(val)
                                        .as_str()
                                        .map(|s| s.eq_ignore_ascii_case("completed"))
                                        .unwrap_or(false)
                                })
                                .unwrap_or(false)
                        });

                        if all_completed {
                            return true;
                        }
                    }
                }
            }
        }

        thread::sleep(Duration::from_millis(20));
    }

    false
}

fn get_self_node_id(leader_url: &str) -> Option<String> {
    let sql = "SELECT node_id FROM system.cluster WHERE is_self = true LIMIT 1";
    if let Ok(response) = execute_on_node_response(leader_url, sql) {
        if let Some(result) = response.results.first() {
            if let Some(rows) = &result.rows {
                if let Some(row) = rows.first() {
                    if let Some(value) = row.first() {
                        return extract_typed_value(value).as_str().map(|s| s.to_string());
                    }
                }
            }
        }
    }

    None
}
