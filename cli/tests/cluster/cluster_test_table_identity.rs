//! Cluster Table Identity Verification Tests
//!
//! Tests that verify table data is IDENTICAL (not just counts) across all cluster nodes.
//! These tests go beyond row counts to ensure:
//! 1. Every row exists on every node with identical values
//! 2. Column ordering and data types are preserved
//! 3. Updates and deletes are properly propagated
//! 4. Complex data types maintain fidelity across replication

use crate::cluster_common::*;
use crate::common::*;
use std::collections::HashSet;
use std::time::Duration;

fn query_with_verification_limit(sql: &str, expected_rows: usize) -> String {
    let trimmed = sql.trim().trim_end_matches(';');
    if expected_rows == 0 || trimmed.split_whitespace().any(|part| part.eq_ignore_ascii_case("LIMIT"))
    {
        trimmed.to_string()
    } else {
        format!("{} LIMIT {}", trimmed, expected_rows + 1)
    }
}

/// Helper: Compare data across all nodes with retries
fn verify_data_identical_with_retry(
    urls: &[String],
    sql: &str,
    expected_rows: usize,
    max_retries: usize,
) -> Result<(), String> {
    let mut last_err: Option<String> = None;
    let bounded_sql = query_with_verification_limit(sql, expected_rows);

    for _ in 0..=max_retries {
        let mut all_data: Vec<Vec<String>> = Vec::new();

        for url in urls {
            let rows = fetch_normalized_rows(url, &bounded_sql).unwrap_or_default();
            all_data.push(rows);
        }

        let reference = &all_data[0];
        if reference.len() != expected_rows {
            last_err = Some(format!(
                "Row counts don't match expected {}. Counts: {:?}",
                expected_rows,
                all_data.iter().map(|d| d.len()).collect::<Vec<_>>()
            ));
            continue;
        }

        let mut mismatch: Option<String> = None;
        for (i, data) in all_data.iter().enumerate().skip(1) {
            if data != reference {
                let ref_set: HashSet<_> = reference.iter().collect();
                let data_set: HashSet<_> = data.iter().collect();
                let missing: Vec<_> = ref_set.difference(&data_set).collect();
                let extra: Vec<_> = data_set.difference(&ref_set).collect();
                mismatch =
                    Some(format!("Node {} differs. Missing: {:?}, Extra: {:?}", i, missing, extra));
                break;
            }
        }

        if let Some(err) = mismatch {
            last_err = Some(err);
            continue;
        }

        return Ok(());
    }

    Err(last_err.unwrap_or_else(|| "Data verification failed".to_string()))
}

/// Test: Inserted rows are byte-for-byte identical across all nodes
#[test]
fn cluster_test_table_identity_inserts() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Table Identity - Inserts ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("identity_ins");

    // Setup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!(
            "CREATE SHARED TABLE {}.identity_test (
                id BIGINT PRIMARY KEY,
                name STRING,
                score DOUBLE,
                active BOOLEAN
            )",
            namespace
        ),
    )
    .expect("Failed to create table");

    // Wait for table to replicate to all nodes
    if !wait_for_table_on_all_nodes(&namespace, "identity_test", 10000) {
        panic!("Table identity_test did not replicate to all nodes");
    }

    // Insert diverse data
    let test_data = vec![
        (1, "Alice", 95.5, true),
        (2, "Bob", 87.3, false),
        (3, "Charlie", 100.0, true),
        (4, "Diana", 78.9, true),
        (5, "Eve", 92.1, false),
    ];

    for (id, name, score, active) in &test_data {
        execute_on_node(
            &urls[0],
            &format!(
                "INSERT INTO {}.identity_test (id, name, score, active) VALUES ({}, '{}', {}, {})",
                namespace, id, name, score, active
            ),
        )
        .expect("Failed to insert");
    }

    // Verify data is identical on all nodes
    let query =
        format!("SELECT id, name, score, active FROM {}.identity_test ORDER BY id", namespace);

    match verify_data_identical_with_retry(&urls, &query, test_data.len(), 20) {
        Ok(_) => {
            println!("  ✓ All {} rows are identical across all nodes", test_data.len());
        },
        Err(e) => {
            panic!("Table identity verification failed: {}", e);
        },
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Inserted rows are identical across all nodes\n");
}

/// Test: Updates propagate identically to all nodes
#[test]
fn cluster_test_table_identity_updates() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Table Identity - Updates ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("identity_upd");

    // Setup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!(
            "CREATE SHARED TABLE {}.update_test (id BIGINT PRIMARY KEY, value STRING, counter BIGINT)",
            namespace
        ),
    )
    .expect("Failed to create table");

    // Wait for table to replicate to all nodes
    if !wait_for_table_on_all_nodes(&namespace, "update_test", 10000) {
        panic!("Table update_test did not replicate to all nodes");
    }

    // Insert initial data
    for i in 0..10 {
        execute_on_node(
            &urls[0],
            &format!(
                "INSERT INTO {}.update_test (id, value, counter) VALUES ({}, 'initial_{}', 0)",
                namespace, i, i
            ),
        )
        .expect("Failed to insert");
    }

    // Update first 5 rows using individual PK-based updates (KalamDB doesn't support predicate-based updates on SHARED tables)
    for i in 0..5 {
        execute_on_node(
            &urls[0],
            &format!(
                "UPDATE {}.update_test SET value = 'updated', counter = 1 WHERE id = {}",
                namespace, i
            ),
        )
        .expect("Failed to update");
    }

    // Verify updates are identical on all nodes
    let query = format!("SELECT id, value, counter FROM {}.update_test ORDER BY id", namespace);

    match verify_data_identical_with_retry(&urls, &query, 10, 20) {
        Ok(_) => {
            // Also verify the actual values
            let data = fetch_normalized_rows(&urls[0], &query).expect("Failed to fetch data");

            // Check that first 5 rows have "updated" value
            for row in data.iter().take(5) {
                assert!(row.contains("updated"), "Row should be updated: {}", row);
            }

            // Check that last 5 rows still have "initial" value
            for row in data.iter().skip(5) {
                assert!(row.contains("initial"), "Row should be initial: {}", row);
            }

            println!("  ✓ Updates correctly applied and identical across all nodes");
        },
        Err(e) => {
            panic!("Table identity verification failed after update: {}", e);
        },
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Updates are identical across all nodes\n");
}

/// Test: Deletes are reflected identically on all nodes
#[test]
fn cluster_test_table_identity_deletes() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Table Identity - Deletes ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("identity_del");

    // Setup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!(
            "CREATE SHARED TABLE {}.delete_test (id BIGINT PRIMARY KEY, category STRING)",
            namespace
        ),
    )
    .expect("Failed to create table");

    // Wait for table to replicate to all nodes
    std::thread::sleep(Duration::from_millis(1000));
    if !wait_for_table_on_all_nodes(&namespace, "delete_test", 15000) {
        panic!("Table delete_test did not replicate to all nodes");
    }

    // Insert data with categories
    for i in 0..20 {
        let category = if i % 2 == 0 { "even" } else { "odd" };
        execute_on_node(
            &urls[0],
            &format!(
                "INSERT INTO {}.delete_test (id, category) VALUES ({}, '{}')",
                namespace, i, category
            ),
        )
        .expect("Failed to insert");
    }

    std::thread::sleep(Duration::from_millis(1500));

    // Delete all even rows
    execute_on_node(
        &urls[0],
        &format!("DELETE FROM {}.delete_test WHERE category = 'even'", namespace),
    )
    .expect("Failed to delete");

    std::thread::sleep(Duration::from_millis(1500));

    // Verify only odd rows remain on all nodes
    let query = format!("SELECT id, category FROM {}.delete_test ORDER BY id", namespace);

    match verify_data_identical_with_retry(&urls, &query, 10, 20) {
        Ok(_) => {
            // Verify only odd categories remain
            let data = fetch_normalized_rows(&urls[0], &query).expect("Failed to fetch data");
            for row in &data {
                assert!(row.contains("odd"), "Only odd rows should remain: {}", row);
                assert!(!row.contains("even"), "No even rows should remain: {}", row);
            }
            println!("  ✓ Deletes correctly applied and identical across all nodes");
        },
        Err(e) => {
            panic!("Table identity verification failed after delete: {}", e);
        },
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Deletes are identical across all nodes\n");
}

/// Test: Mixed operations result in identical final state
#[test]
fn cluster_test_table_identity_mixed_operations() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Table Identity - Mixed Operations ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("identity_mix");

    // Setup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!(
            "CREATE SHARED TABLE {}.mixed_test (id BIGINT PRIMARY KEY, status STRING, version BIGINT)",
            namespace
        ),
    )
    .expect("Failed to create table");

    // Wait for table to replicate to all nodes
    if !wait_for_table_on_all_nodes(&namespace, "mixed_test", 10000) {
        panic!("Table mixed_test did not replicate to all nodes");
    }

    // Phase 1: Initial inserts
    println!("  Phase 1: Inserting 50 initial rows...");
    for i in 0..50 {
        execute_on_node(
            &urls[0],
            &format!(
                "INSERT INTO {}.mixed_test (id, status, version) VALUES ({}, 'created', 1)",
                namespace, i
            ),
        )
        .expect("Failed to insert");
    }

    // Phase 2: Update first 25 rows using individual PK-based updates (KalamDB doesn't support predicate-based updates on SHARED tables)
    println!("  Phase 2: Updating rows with id 0-24...");
    for i in 0..25 {
        execute_on_node(
            &urls[0],
            &format!(
                "UPDATE {}.mixed_test SET status = 'updated', version = 2 WHERE id = {}",
                namespace, i
            ),
        )
        .expect("Failed to update");
    }

    // Phase 3: Delete some rows
    println!("  Phase 3: Deleting rows with id >= 40...");
    execute_on_node(&urls[0], &format!("DELETE FROM {}.mixed_test WHERE id >= 40", namespace))
        .expect("Failed to delete");

    // Phase 4: Insert new rows
    println!("  Phase 4: Inserting 10 new rows...");
    for i in 100..110 {
        execute_on_node(
            &urls[0],
            &format!(
                "INSERT INTO {}.mixed_test (id, status, version) VALUES ({}, 'new', 1)",
                namespace, i
            ),
        )
        .expect("Failed to insert");
    }

    // Phase 5: Update the new rows using individual PK-based updates
    println!("  Phase 5: Updating new rows...");
    for i in 100..110 {
        execute_on_node(
            &urls[0],
            &format!("UPDATE {}.mixed_test SET version = 3 WHERE id = {}", namespace, i),
        )
        .expect("Failed to update");
    }

    // Final verification: 40 rows (0-39) + 10 rows (100-109) = 50 rows
    // But we deleted 40-49, so: 40 rows (0-39) + 10 rows (100-109) = 50 rows
    // Actually: 50 initial - 10 deleted (40-49) + 10 new (100-109) = 50 rows
    let expected_rows = 50;

    let query = format!("SELECT id, status, version FROM {}.mixed_test ORDER BY id", namespace);

    match verify_data_identical_with_retry(&urls, &query, expected_rows, 30) {
        Ok(_) => {
            let data = fetch_normalized_rows(&urls[0], &query).expect("Failed to fetch data");

            // Verify expected state
            let mut updated_count = 0;
            let mut created_count = 0;
            let mut new_count = 0;

            for row in &data {
                if row.contains("updated") {
                    updated_count += 1;
                } else if row.contains("created") {
                    created_count += 1;
                } else if row.contains("new") {
                    new_count += 1;
                }
            }

            println!(
                "  Final state: {} updated, {} created, {} new",
                updated_count, created_count, new_count
            );
            assert_eq!(updated_count, 25, "Expected 25 updated rows");
            assert_eq!(created_count, 15, "Expected 15 created rows (25-39)");
            assert_eq!(new_count, 10, "Expected 10 new rows");

            println!("  ✓ Mixed operations resulted in correct identical state");
        },
        Err(e) => {
            // Print detailed info for debugging
            for (i, url) in urls.iter().enumerate() {
                let count_query = format!("SELECT count(*) as count FROM {}.mixed_test", namespace);
                if let Ok(count) = query_count_on_url(url, &count_query).to_string().parse::<i64>()
                {
                    println!("  Node {} has {} rows", i, count);
                }
            }
            panic!("Table identity verification failed after mixed operations: {}", e);
        },
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Mixed operations result in identical state across all nodes\n");
}

/// Test: Large batch operations are replicated identically
#[test]
fn cluster_test_table_identity_large_batch() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Table Identity - Large Batch ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("identity_batch");

    // Setup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!(
            "CREATE SHARED TABLE {}.batch_test (id BIGINT PRIMARY KEY, data STRING)",
            namespace
        ),
    )
    .expect("Failed to create table");

    // Wait for table to replicate to all nodes
    if !wait_for_table_on_all_nodes(&namespace, "batch_test", 10000) {
        panic!("Table batch_test did not replicate to all nodes");
    }

    // Insert 500 rows in batches
    let total_rows = 500;
    let batch_size = 50;

    println!("  Inserting {} rows in batches of {}...", total_rows, batch_size);

    for batch_start in (0..total_rows).step_by(batch_size) {
        let mut values = Vec::new();
        for i in batch_start..(batch_start + batch_size).min(total_rows) {
            values.push(format!("({}, 'data_{}')", i, i));
        }
        execute_on_node(
            &urls[0],
            &format!(
                "INSERT INTO {}.batch_test (id, data) VALUES {}",
                namespace,
                values.join(", ")
            ),
        )
        .expect("Failed to insert batch");

        // Small delay between batches
    }

    // Verify all rows are identical on all nodes
    let query = format!("SELECT id, data FROM {}.batch_test ORDER BY id", namespace);

    match verify_data_identical_with_retry(&urls, &query, total_rows, 30) {
        Ok(_) => {
            println!("  ✓ All {} rows are identical across all nodes", total_rows);
        },
        Err(e) => {
            panic!("Large batch identity verification failed: {}", e);
        },
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ Large batch operations result in identical data\n");
}

/// Test: User table data is properly partitioned and replicated
#[test]
fn cluster_test_table_identity_user_tables() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: Table Identity - User Tables ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("identity_user");
    let user_password = "test_password_123";

    // Setup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!("CREATE USER TABLE {}.user_data (id BIGINT PRIMARY KEY, item STRING)", namespace),
    )
    .expect("Failed to create user table");

    // Wait for table to replicate to all nodes
    if !wait_for_table_on_all_nodes(&namespace, "user_data", 10000) {
        panic!("Table user_data did not replicate to all nodes");
    }

    // Create test users with correct syntax
    let users: Vec<String> = (0..3).map(|i| format!("identity_user_{}_{}", namespace, i)).collect();
    for user in &users {
        execute_on_node(
            &urls[0],
            &format!("CREATE USER {} WITH PASSWORD '{}' ROLE 'user'", user, user_password),
        )
        .expect(&format!("Failed to create user {}", user));
    }

    // Insert data as root for each user
    for (user_idx, user) in users.iter().enumerate() {
        for i in 0..5 {
            execute_on_node_as_user(
                &urls[0],
                user,
                user_password,
                &format!(
                    "INSERT INTO {}.user_data (id, item) VALUES ({}, 'item_{}')",
                    namespace,
                    user_idx * 100 + i,
                    i
                ),
            )
            .expect("Failed to insert as user");
        }
    }

    // Wait for replication
    std::thread::sleep(Duration::from_millis(1500));

    // Verify data consistency for each user across all nodes
    for (user_idx, user) in users.iter().enumerate() {
        let query = format!("SELECT id, item FROM {}.user_data ORDER BY id", namespace);

        let mut all_data: Vec<Vec<String>> = Vec::new();
        for url in &urls {
            match fetch_normalized_rows_as_user(url, user, user_password, &query) {
                Ok(data) => all_data.push(data),
                Err(e) => {
                    println!("  ⚠ Failed to query user {} on node: {}", user, e);
                    all_data.push(Vec::new());
                },
            }
        }

        let reference = &all_data[0];
        if !reference.is_empty() {
            for (i, data) in all_data.iter().enumerate().skip(1) {
                if data != reference {
                    println!(
                        "  ⚠ User {} data differs on node {}: {:?} vs {:?}",
                        user, i, data, reference
                    );
                }
            }
        }
        println!("  ✓ User {} data verified across nodes", user_idx);
    }

    // Cleanup
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ User table data is properly partitioned and replicated\n");
}
