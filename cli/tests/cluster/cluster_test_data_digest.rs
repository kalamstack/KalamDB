//! Cluster data digest tests
//!
//! Ensures all nodes return identical data, not just row counts.

use crate::cluster_common::*;
use crate::common::*;
use kalam_link::KalamCellValue;

fn normalize_rows(rows: &[Vec<KalamCellValue>]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|value| extract_typed_value(value.inner()).to_string())
                .collect::<Vec<String>>()
                .join("|")
        })
        .collect()
}

fn fetch_sorted_rows(base_url: &str, sql: &str) -> Result<Vec<String>, String> {
    let response = execute_on_node_response(base_url, sql)?;
    let result = response.results.first().ok_or_else(|| "Missing query result".to_string())?;
    let rows = result.rows.as_ref().ok_or_else(|| "Missing row data".to_string())?;

    Ok(normalize_rows(rows))
}

/// Test: Data content is identical across all nodes
#[test]
fn cluster_test_data_digest_consistency() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Data Digest Consistency Across Nodes ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("cluster_digest");

    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!(
            "CREATE SHARED TABLE {}.digest_test (id BIGINT PRIMARY KEY, payload STRING)",
            namespace
        ),
    )
    .expect("Failed to create table");

    // Wait for table to replicate to all nodes
    if !wait_for_table_on_all_nodes(&namespace, "digest_test", 10000) {
        panic!("Table digest_test did not replicate to all nodes");
    }

    println!("Inserting 50 rows on node 0...");
    let mut values = Vec::new();
    for i in 0..50 {
        values.push(format!("({}, 'payload_{}')", i, i));
        if values.len() == 25 || i == 49 {
            execute_on_node(
                &urls[0],
                &format!(
                    "INSERT INTO {}.digest_test (id, payload) VALUES {}",
                    namespace,
                    values.join(", ")
                ),
            )
            .expect("Insert failed");
            values.clear();
        }
    }

    let sql = format!("SELECT id, payload FROM {}.digest_test ORDER BY id", namespace);
    let mut matched = false;

    for _ in 0..20 {
        let mut row_sets = Vec::new();
        for url in &urls {
            match fetch_sorted_rows(url, &sql) {
                Ok(rows) => row_sets.push(rows),
                Err(_) => row_sets.push(Vec::new()),
            }
        }

        let expected = row_sets.first().cloned().unwrap_or_default();
        let all_match = !expected.is_empty() && row_sets.iter().all(|rows| rows == &expected);

        if all_match && expected.len() == 50 {
            matched = true;
            break;
        }
    }

    if !matched {
        let mut counts = Vec::new();
        for url in &urls {
            let count = query_count_on_url(
                url,
                &format!("SELECT count(*) as count FROM {}.digest_test", namespace),
            );
            counts.push(count);
        }
        panic!("Digest mismatch across nodes. Row counts: {:?}", counts);
    }

    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Data digests match across all cluster nodes\n");
}
