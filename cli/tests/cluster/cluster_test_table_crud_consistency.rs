//! Cluster CRUD consistency tests for user/shared/stream tables
//!
//! Verifies insert/update/delete results are identical across all nodes.

use std::time::Duration;

use crate::{cluster_common::*, common::*};

fn assert_rows_on_all_nodes(urls: &[String], sql: &str, expected: &[String]) {
    let mut expected_rows = expected.to_vec();
    expected_rows.sort();

    // Retry with backoff for Raft replication lag (writes auto-forwarded to
    // shard leaders via HTTP may take extra time to replicate to all nodes)
    for attempt in 0..20 {
        let mut all_match = true;
        let mut last_mismatch = String::new();

        for (idx, url) in urls.iter().enumerate() {
            match fetch_normalized_rows(url, sql) {
                Ok(rows) => {
                    if rows != expected_rows {
                        all_match = false;
                        last_mismatch = format!(
                            "Node {} data mismatch: got {:?}, expected {:?}",
                            idx, rows, expected_rows
                        );
                    }
                },
                Err(e) => {
                    all_match = false;
                    last_mismatch = format!("Node {} query failed: {}", idx, e);
                },
            }
        }

        if all_match {
            return;
        }

        if attempt < 19 {
            std::thread::sleep(Duration::from_millis(500));
        } else {
            panic!("{} for query: {}", last_mismatch, sql);
        }
    }
}

fn assert_rows_on_all_nodes_as_user(
    urls: &[String],
    user: &str,
    password: &str,
    sql: &str,
    expected: &[String],
) {
    let mut expected_rows = expected.to_vec();
    expected_rows.sort();

    for attempt in 0..20 {
        let mut all_match = true;
        let mut last_mismatch = String::new();

        for (idx, url) in urls.iter().enumerate() {
            match fetch_normalized_rows_as_user(url, user, password, sql) {
                Ok(rows) => {
                    if rows != expected_rows {
                        all_match = false;
                        last_mismatch = format!(
                            "Node {} data mismatch: got {:?}, expected {:?}",
                            idx, rows, expected_rows
                        );
                    }
                },
                Err(e) => {
                    all_match = false;
                    last_mismatch = format!("Node {} query failed: {}", idx, e);
                },
            }
        }

        if all_match {
            return;
        }

        if attempt < 19 {
            std::thread::sleep(Duration::from_millis(500));
        } else {
            panic!("{} for query: {}", last_mismatch, sql);
        }
    }
}

#[test]
fn cluster_test_table_crud_consistency() {
    if !require_cluster_running() {
        return;
    }

    println!("\n=== TEST: CRUD Consistency for User/Shared/Stream Tables ===\n");

    let urls = cluster_urls();
    let namespace = generate_unique_namespace("crud_consistency");
    let test_user = format!("crud_user_{}", namespace);
    let user_password = "test_password_123";

    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE IF EXISTS {} CASCADE", namespace));
    execute_on_node(&urls[0], &format!("CREATE NAMESPACE {}", namespace))
        .expect("Failed to create namespace");

    execute_on_node(
        &urls[0],
        &format!("CREATE USER {} WITH PASSWORD '{}' ROLE 'user'", test_user, user_password),
    )
    .expect("Failed to create test user");

    execute_on_node(
        &urls[0],
        &format!(
            "CREATE USER TABLE {}.user_crud (id BIGINT PRIMARY KEY, value STRING)",
            namespace
        ),
    )
    .expect("Failed to create user table");
    execute_on_node(
        &urls[0],
        &format!(
            "CREATE SHARED TABLE {}.shared_crud (id BIGINT PRIMARY KEY, value STRING)",
            namespace
        ),
    )
    .expect("Failed to create shared table");
    execute_on_node(
        &urls[0],
        &format!(
            "CREATE STREAM TABLE {}.stream_crud (id BIGINT PRIMARY KEY, value STRING) WITH \
             (TTL_SECONDS = 3600)",
            namespace
        ),
    )
    .expect("Failed to create stream table");

    execute_on_node_as_user(
        &urls[0],
        &test_user,
        user_password,
        &format!(
            "INSERT INTO {}.user_crud (id, value) VALUES (1, 'u1'), (2, 'u2'), (3, 'u3')",
            namespace
        ),
    )
    .expect("Failed to insert into user table");
    execute_on_node(
        &urls[0],
        &format!(
            "INSERT INTO {}.shared_crud (id, value) VALUES (1, 's1'), (2, 's2'), (3, 's3')",
            namespace
        ),
    )
    .expect("Failed to insert into shared table");
    execute_on_node(
        &urls[0],
        &format!(
            "INSERT INTO {}.stream_crud (id, value) VALUES (1, 't1'), (2, 't2'), (3, 't3')",
            namespace
        ),
    )
    .expect("Failed to insert into stream table");

    execute_on_node_as_user(
        &urls[0],
        &test_user,
        user_password,
        &format!("UPDATE {}.user_crud SET value = 'u2_updated' WHERE id = 2", namespace),
    )
    .expect("Failed to update user table");
    execute_on_node(
        &urls[0],
        &format!("UPDATE {}.shared_crud SET value = 's2_updated' WHERE id = 2", namespace),
    )
    .expect("Failed to update shared table");
    // STREAM tables don't support UPDATE; validate insert/delete consistency instead.

    execute_on_node_as_user(
        &urls[0],
        &test_user,
        user_password,
        &format!("DELETE FROM {}.user_crud WHERE id = 1", namespace),
    )
    .expect("Failed to delete from user table");
    execute_on_node(&urls[0], &format!("DELETE FROM {}.shared_crud WHERE id = 1", namespace))
        .expect("Failed to delete from shared table");
    execute_on_node(&urls[0], &format!("DELETE FROM {}.stream_crud WHERE id = 1", namespace))
        .expect("Failed to delete from stream table");

    assert_rows_on_all_nodes_as_user(
        &urls,
        &test_user,
        user_password,
        &format!("SELECT id, value FROM {}.user_crud ORDER BY id", namespace),
        &["2|u2_updated".to_string(), "3|u3".to_string()],
    );
    assert_rows_on_all_nodes(
        &urls,
        &format!("SELECT id, value FROM {}.shared_crud ORDER BY id", namespace),
        &["2|s2_updated".to_string(), "3|s3".to_string()],
    );
    assert_rows_on_all_nodes(
        &urls,
        &format!("SELECT id, value FROM {}.stream_crud ORDER BY id", namespace),
        &["2|t2".to_string(), "3|t3".to_string()],
    );

    let _ = execute_on_node(&urls[0], &format!("DROP USER IF EXISTS {}", test_user));
    let _ = execute_on_node(&urls[0], &format!("DROP NAMESPACE {} CASCADE", namespace));

    println!("\n  ✅ CRUD results match across all nodes for all table types\n");
}
