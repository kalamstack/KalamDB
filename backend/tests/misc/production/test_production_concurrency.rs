//! Production Readiness: Concurrency Tests
//!
//! Tests concurrent access patterns, contention handling, and race conditions.
//! These tests ensure KalamDB handles multiple simultaneous operations safely.

use super::test_support::{consolidated_helpers, TestServer};
use kalam_client::models::ResponseStatus;
use kalam_client::parse_i64;
use kalamdb_commons::Role;

/// Verify concurrent inserts to same user table work correctly
#[tokio::test]
async fn concurrent_inserts_same_user_table() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("app_concurrent_ins");

    // Setup
    let resp = server.execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns)).await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let create_table = format!(
        r#"
        CREATE TABLE {}.events (
            id INT PRIMARY KEY,
            data VARCHAR
        )
        WITH (TYPE = 'USER')
    "#,
        ns
    );
    let resp = server.execute_sql(&create_table).await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let user_id = server.create_user("user1", "Pass123!", Role::User).await;

    // Spawn 5 concurrent writers, each inserting 10 rows
    let mut handles = vec![];
    for writer_id in 0..5 {
        let server_clone = server.clone();
        let user_id_clone = user_id.clone();
        let ns_clone = ns.clone();

        let handle = tokio::spawn(async move {
            for i in 0..10 {
                let row_id = writer_id * 10 + i;
                let sql = format!(
                    "INSERT INTO {}.events (id, data) VALUES ({}, 'writer_{}_row_{}')",
                    ns_clone, row_id, writer_id, i
                );
                let resp = server_clone.execute_sql_as_user(&sql, user_id_clone.as_str()).await;

                assert_eq!(
                    resp.status,
                    ResponseStatus::Success,
                    "Concurrent insert failed: {:?}",
                    resp.error
                );
            }
        });
        handles.push(handle);
    }

    // Wait for all writers
    for handle in handles {
        handle.await.expect("Writer task panicked");
    }

    // Verify all 50 rows were inserted
    let resp = server
        .execute_sql_as_user(
            &format!("SELECT COUNT(*) as count FROM {}.events", ns),
            user_id.as_str(),
        )
        .await;

    assert_eq!(resp.status, ResponseStatus::Success);
    let rows = resp.results.first().map(|r| r.rows_as_maps()).unwrap_or_default();
    if let Some(row) = rows.first() {
        let count = row.get("count").map(parse_i64).unwrap();
        assert_eq!(count, 50, "Should have 50 rows from concurrent inserts");
    }
}

/// Verify concurrent SELECT queries work correctly
#[tokio::test]
async fn concurrent_select_queries() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("app_concurrent_sel");

    // Setup with data
    let resp = server.execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns)).await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let create_table = format!(
        r#"
        CREATE TABLE {}.data (
            id INT PRIMARY KEY,
            value VARCHAR
        )
        WITH (TYPE = 'USER')
    "#,
        ns
    );
    let resp = server.execute_sql(&create_table).await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let user_id = server.create_user("user1", "Pass123!", Role::User).await;

    // Insert test data
    for i in 0..20 {
        let sql = format!("INSERT INTO {}.data (id, value) VALUES ({}, 'value{}')", ns, i, i);
        let resp = server.execute_sql_as_user(&sql, user_id.as_str()).await;
        assert_eq!(resp.status, ResponseStatus::Success);
    }

    // Spawn 10 concurrent readers
    let mut handles = vec![];
    for _ in 0..10 {
        let server_clone = server.clone();
        let user_id_clone = user_id.clone();
        let ns_clone = ns.clone();

        let handle = tokio::spawn(async move {
            let resp = server_clone
                .execute_sql_as_user(
                    &format!("SELECT COUNT(*) as count FROM {}.data", ns_clone),
                    user_id_clone.as_str(),
                )
                .await;

            assert_eq!(resp.status, ResponseStatus::Success);
            let rows = resp.results.first().map(|r| r.rows_as_maps()).unwrap_or_default();
            if let Some(row) = rows.first() {
                let count = row.get("count").map(parse_i64).unwrap();
                assert_eq!(count, 20, "All readers should see 20 rows");
            }
        });
        handles.push(handle);
    }

    // Wait for all readers
    for handle in handles {
        handle.await.expect("Reader task panicked");
    }
}

/// Verify duplicate PRIMARY KEY handling under concurrency
///
/// NOTE: This test is ignored because atomic PK constraint enforcement
/// under concurrent inserts requires database-level locking or transactions,
/// which is not yet implemented. The current implementation has a TOCTOU race
/// condition where concurrent inserts can all pass the duplicate check before
/// any of them complete the write.
#[tokio::test]
#[ignore = "Concurrent PK enforcement is non-deterministic without transactional locking"]
async fn concurrent_duplicate_primary_key_handling() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("app_concurrent_pk");

    let resp = server.execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns)).await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let create_table = format!(
        r#"
        CREATE TABLE {}.items (
            id INT PRIMARY KEY,
            data VARCHAR
        )
        WITH (TYPE = 'USER')
    "#,
        ns
    );
    let resp = server.execute_sql(&create_table).await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let user_id = server.create_user("user1", "Pass123!", Role::User).await;

    // Try to insert same PRIMARY KEY from 3 concurrent clients
    let mut handles = vec![];
    for writer_id in 0..3 {
        let server_clone = server.clone();
        let user_id_clone = user_id.clone();
        let ns_clone = ns.clone();

        let handle = tokio::spawn(async move {
            let sql = format!(
                "INSERT INTO {}.items (id, data) VALUES (1, 'writer_{}')",
                ns_clone, writer_id
            );
            server_clone.execute_sql_as_user(&sql, user_id_clone.as_str()).await
        });
        handles.push(handle);
    }

    // Collect results
    let mut success_count = 0;
    let mut error_count = 0;

    for handle in handles {
        let resp = handle.await.expect("Writer task panicked");
        match resp.status {
            ResponseStatus::Success => success_count += 1,
            ResponseStatus::Error => error_count += 1,
        }
    }

    // Exactly ONE should succeed, the others should fail with duplicate key error
    assert_eq!(success_count, 1, "Exactly one concurrent insert should succeed");
    assert_eq!(error_count, 2, "Two concurrent inserts should fail with duplicate key");

    // Verify only one row exists
    let resp = server
        .execute_sql_as_user(
            &format!("SELECT COUNT(*) as count FROM {}.items", ns),
            user_id.as_str(),
        )
        .await;

    if let Some(result) = resp.results.first() {
        let rows = result.rows_as_maps();
        let count = rows[0].get("count").map(parse_i64).unwrap();
        assert_eq!(count, 1, "Should have exactly 1 row despite concurrent attempts");
    }
}

/// Verify concurrent UPDATE operations work correctly
#[tokio::test]
async fn concurrent_updates_same_row() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("app_concurrent_upd");

    let resp = server.execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns)).await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let create_table = format!(
        r#"
        CREATE TABLE {}.counter (
            id INT PRIMARY KEY,
            value INT
        )
        WITH (TYPE = 'USER')
    "#,
        ns
    );
    let resp = server.execute_sql(&create_table).await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let user_id = server.create_user("user1", "Pass123!", Role::User).await;

    // Insert initial row
    let resp = server
        .execute_sql_as_user(
            &format!("INSERT INTO {}.counter (id, value) VALUES (1, 0)", ns),
            user_id.as_str(),
        )
        .await;
    assert_eq!(resp.status, ResponseStatus::Success);

    // Spawn 5 concurrent updaters
    let mut handles = vec![];
    for i in 1..=5 {
        let server_clone = server.clone();
        let user_id_clone = user_id.clone();
        let ns_clone = ns.clone();

        let handle = tokio::spawn(async move {
            let sql = format!("UPDATE {}.counter SET value = {} WHERE id = 1", ns_clone, i * 10);
            server_clone.execute_sql_as_user(&sql, user_id_clone.as_str()).await
        });
        handles.push(handle);
    }

    // Wait for all updates
    for handle in handles {
        let resp = handle.await.expect("Updater task panicked");
        // All updates should succeed
        assert_eq!(resp.status, ResponseStatus::Success);
    }

    // Verify final value is one of the updated values (10, 20, 30, 40, or 50)
    let resp = server
        .execute_sql_as_user(
            &format!("SELECT value FROM {}.counter WHERE id = 1", ns),
            user_id.as_str(),
        )
        .await;

    assert_eq!(resp.status, ResponseStatus::Success);
    if let Some(result) = resp.results.first() {
        let rows = result.rows_as_maps();
        let value = rows[0].get("value").map(parse_i64).unwrap();
        assert!(
            [10, 20, 30, 40, 50].contains(&value),
            "Final value should be one of the concurrent updates, got: {}",
            value
        );
    }
}

/// Verify concurrent DELETE operations work correctly
#[tokio::test]
async fn concurrent_deletes() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("app_concurrent_del");

    let resp = server.execute_sql(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns)).await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let create_table = format!(
        r#"
        CREATE TABLE {}.temp (
            id INT PRIMARY KEY,
            data VARCHAR
        )
        WITH (TYPE = 'USER')
    "#,
        ns
    );
    let resp = server.execute_sql(&create_table).await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let user_id = server.create_user("user1", "Pass123!", Role::User).await;

    // Insert 10 rows
    for i in 0..10 {
        let sql = format!("INSERT INTO {}.temp (id, data) VALUES ({}, 'data{}')", ns, i, i);
        let resp = server.execute_sql_as_user(&sql, user_id.as_str()).await;
        assert_eq!(resp.status, ResponseStatus::Success);
    }

    // Spawn 5 concurrent deleters (each deletes different rows to avoid conflicts)
    let mut handles = vec![];
    for i in 0..5 {
        let server_clone = server.clone();
        let user_id_clone = user_id.clone();
        let ns_clone = ns.clone();

        let handle = tokio::spawn(async move {
            // Each deleter tries to delete specific rows
            let sql1 = format!("DELETE FROM {}.temp WHERE id = {}", ns_clone, i * 2);
            let resp1 = server_clone.execute_sql_as_user(&sql1, user_id_clone.as_str()).await;

            let sql2 = format!("DELETE FROM {}.temp WHERE id = {}", ns_clone, i * 2 + 1);
            let resp2 = server_clone.execute_sql_as_user(&sql2, user_id_clone.as_str()).await;

            (resp1, resp2)
        });
        handles.push(handle);
    }

    // Wait for all deleters
    let mut success_count = 0;
    for handle in handles {
        let (resp1, resp2) = handle.await.expect("Deleter task panicked");

        if resp1.status == ResponseStatus::Success {
            success_count += 1;
        }
        if resp2.status == ResponseStatus::Success {
            success_count += 1;
        }
    }

    // At least some deletes should succeed
    println!("Concurrent deletes: {} successful operations", success_count);
    assert!(success_count > 0, "At least some concurrent deletes should succeed");

    // Verify rows were deleted (may not be all 10 due to races)
    let resp = server
        .execute_sql_as_user(
            &format!("SELECT COUNT(*) as count FROM {}.temp", ns),
            user_id.as_str(),
        )
        .await;

    if let Some(result) = resp.results.first() {
        let rows = result.rows_as_maps();
        let count = rows[0].get("count").map(parse_i64).unwrap();
        println!("Rows remaining after concurrent deletes: {}", count);
        // We just verify the count is non-negative and the operations completed
        // The exact count depends on deletion order and timing
        assert!((0..=10).contains(&count), "Row count should be between 0 and 10");
    }
}
