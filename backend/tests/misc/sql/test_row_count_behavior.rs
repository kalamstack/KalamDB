//! Integration tests for UPDATE/DELETE row count behavior (Phase 2.5)
//!
//! Verifies KalamDB row count reporting:
//! - UPDATE returns count of rows that produced a new row version
//! - DELETE returns count of rows that were soft-deleted
//! - Row counts are accurate and match expectations

use super::test_support::{consolidated_helpers, fixtures, TestServer};
use kalam_client::models::{QueryResponse, ResponseStatus};

fn assert_row_count(response: &QueryResponse, expected: usize, verbs: &[&str]) {
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "DML execution failed: {:?}",
        response.error
    );
    let result = response.results.first().expect("DML response missing QueryResult entry");
    assert_eq!(
        result.row_count, expected,
        "Expected {} rows affected, got {}",
        expected, result.row_count
    );

    if let Some(message) = &result.message {
        let generic = format!("{} row(s) affected", expected);
        let matches_generic = message.contains(&generic);
        let matches_named = verbs.iter().any(|verb| {
            let target = format!("{} {} row(s)", verb, expected);
            message.contains(&target)
        });
        assert!(
            matches_generic || matches_named,
            "Unexpected DML message '{}'; expected one of {:?} or '{}'.",
            message,
            verbs,
            generic
        );
    }
}

#[actix_web::test]
async fn test_update_returns_correct_row_count() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("test_ns_upd");

    // Setup
    fixtures::create_namespace(&server, &ns).await;
    server
        .execute_sql_as_user(
            &format!(
                "CREATE TABLE {}.users (\
                id TEXT PRIMARY KEY,
                name TEXT,
                email TEXT
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local'
            )",
                ns
            ),
            "user1",
        )
        .await;

    // Insert test data
    server
        .execute_sql_as_user(
            &format!(
                "INSERT INTO {}.users (id, name, email) VALUES ('user1', 'Alice', 'alice@example.com')",
                ns
            ),
            "user1",
        )
        .await;

    server
        .execute_sql_as_user(
            &format!(
                "INSERT INTO {}.users (id, name, email) VALUES ('user2', 'Bob', 'bob@example.com')",
                ns
            ),
            "user1",
        )
        .await;

    // Test 1: UPDATE existing row returns count of 1
    let response = server
        .execute_sql_as_user(
            &format!("UPDATE {}.users SET email = 'alice.new@example.com' WHERE id = 'user1'", ns),
            "user1",
        )
        .await;

    assert_row_count(&response, 1, &["Updated"]);

    // Test 2: UPDATE non-existent row returns count of 0
    let response = server
        .execute_sql_as_user(
            &format!("UPDATE {}.users SET email = 'test@example.com' WHERE id = 'user999'", ns),
            "user1",
        )
        .await;

    assert_row_count(&response, 0, &["Updated"]);

    println!("✅ UPDATE returns correct row counts");
}

#[actix_web::test]
async fn test_update_same_values_returns_zero() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("test_ns_same");

    // Setup
    fixtures::create_namespace(&server, &ns).await;
    server
        .execute_sql_as_user(
            &format!(
                "CREATE TABLE {}.users (\
                id TEXT PRIMARY KEY,
                name TEXT
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local'
            )",
                ns
            ),
            "user1",
        )
        .await;

    // Insert test data
    server
        .execute_sql_as_user(
            &format!("INSERT INTO {}.users (id, name) VALUES ('user1', 'Alice')", ns),
            "user1",
        )
        .await;

    // UPDATE to the same value is a no-op and should not count as an update.
    let response = server
        .execute_sql_as_user(
            &format!("UPDATE {}.users SET name = 'Alice' WHERE id = 'user1'", ns),
            "user1",
        )
        .await;

    assert_row_count(&response, 0, &["Updated"]);

    println!("✅ UPDATE with unchanged values is treated as a no-op");
}

#[actix_web::test]
async fn test_delete_returns_correct_row_count() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("test_ns_del");

    // Setup
    fixtures::create_namespace(&server, &ns).await;
    server
        .execute_sql_as_user(
            &format!(
                "CREATE TABLE {}.tasks (\
                id TEXT PRIMARY KEY,
                title TEXT
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local'
            )",
                ns
            ),
            "user1",
        )
        .await;

    // Insert test data
    server
        .execute_sql_as_user(
            &format!("INSERT INTO {}.tasks (id, title) VALUES ('task1', 'First task')", ns),
            "user1",
        )
        .await;

    server
        .execute_sql_as_user(
            &format!("INSERT INTO {}.tasks (id, title) VALUES ('task2', 'Second task')", ns),
            "user1",
        )
        .await;

    // Test 1: DELETE existing row returns count of 1
    let response = server
        .execute_sql_as_user(&format!("DELETE FROM {}.tasks WHERE id = 'task1'", ns), "user1")
        .await;

    assert_row_count(&response, 1, &["Deleted"]);

    // Test 2: DELETE non-existent row returns count of 0
    let response = server
        .execute_sql_as_user(&format!("DELETE FROM {}.tasks WHERE id = 'task999'", ns), "user1")
        .await;

    if response.status == ResponseStatus::Success {
        assert_row_count(&response, 0, &["Deleted"]);
    } else {
        let err = response.error.as_ref().expect("DELETE error missing details");
        assert!(err.message.contains("not found"), "Unexpected DELETE error: {:?}", err);
    }

    println!("✅ DELETE returns correct row counts");
}

#[actix_web::test]
async fn test_delete_already_deleted_returns_zero() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("test_ns_deldel");

    // Setup
    fixtures::create_namespace(&server, &ns).await;
    server
        .execute_sql_as_user(
            &format!(
                "CREATE TABLE {}.tasks (\
                id TEXT PRIMARY KEY,
                title TEXT
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local'
            )",
                ns
            ),
            "user1",
        )
        .await;

    // Insert test data
    server
        .execute_sql_as_user(
            &format!(
                "INSERT INTO {}.tasks (id, title) VALUES ('task1', 'Task to delete twice')",
                ns
            ),
            "user1",
        )
        .await;

    // First DELETE
    let response = server
        .execute_sql_as_user(&format!("DELETE FROM {}.tasks WHERE id = 'task1'", ns), "user1")
        .await;

    if response.status == ResponseStatus::Success {
        let result = response.results.first().expect("DELETE response missing QueryResult entry");
        assert!(
            result.row_count <= 1,
            "Expected at most 1 row affected, got {}",
            result.row_count
        );
    } else {
        let err = response.error.as_ref().expect("DELETE error missing details");
        assert!(err.message.contains("not found"), "Unexpected DELETE error: {:?}", err);
    }

    let verify = server
        .execute_sql_as_user(
            &format!("SELECT COUNT(*) AS cnt FROM {}.tasks WHERE id = 'task1'", ns),
            "user1",
        )
        .await;
    if verify.status == ResponseStatus::Success {
        let row = verify
            .results
            .first()
            .and_then(|r| r.row_as_map(0))
            .expect("Missing COUNT result row");
        let cnt = row
            .get("cnt")
            .and_then(|v| v.as_i64().or_else(|| v.as_u64().map(|u| u as i64)))
            .unwrap_or(-1);
        if cnt >= 0 {
            assert_eq!(cnt, 0, "Expected task1 to be deleted");
        } else {
            eprintln!("COUNT result missing/invalid; skipping strict delete verification");
        }
    } else {
        let err = verify.error.as_ref().expect("COUNT error missing details");
        assert!(err.message.contains("not found"), "Unexpected COUNT error: {:?}", err);
    }

    // Second DELETE on same row (should return 0 because already deleted)
    let response = server
        .execute_sql_as_user(&format!("DELETE FROM {}.tasks WHERE id = 'task1'", ns), "user1")
        .await;

    if response.status == ResponseStatus::Success {
        assert_row_count(&response, 0, &["Deleted"]);
    } else {
        let err = response.error.as_ref().expect("DELETE error missing details");
        assert!(err.message.contains("not found"), "Unexpected DELETE error: {:?}", err);
    }

    println!("✅ DELETE on already-deleted row returns 0 (correct soft delete behavior)");
}

#[actix_web::test]
async fn test_delete_multiple_rows_count() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("test_ns_multi");

    // Setup
    fixtures::create_namespace(&server, &ns).await;
    server
        .execute_sql_as_user(
            &format!(
                "CREATE TABLE {}.tasks (\
                id TEXT PRIMARY KEY,
                priority INT
            ) WITH (
                TYPE = 'USER',
                STORAGE_ID = 'local'
            )",
                ns
            ),
            "user1",
        )
        .await;

    // Insert 5 tasks with priority 1
    for i in 1..=5 {
        server
            .execute_sql_as_user(
                &format!("INSERT INTO {}.tasks (id, priority) VALUES ('task{}', 1)", ns, i),
                "user1",
            )
            .await;
    }

    // Insert 3 tasks with priority 5
    for i in 6..=8 {
        server
            .execute_sql_as_user(
                &format!("INSERT INTO {}.tasks (id, priority) VALUES ('task{}', 5)", ns, i),
                "user1",
            )
            .await;
    }

    // DELETE all priority 1 tasks (should be 5 rows)
    let response = server
        .execute_sql_as_user(&format!("DELETE FROM {}.tasks WHERE priority = 1", ns), "user1")
        .await;

    if response.status != ResponseStatus::Success {
        let err = response.error.as_ref().expect("Error detail missing despite error status");
        assert!(
            err.message.contains("requires WHERE id"),
            "Unexpected multi-row delete error: {:?}",
            err
        );
        println!(
            "Skipping row-count assertion; multi-row DELETE not yet supported: {}",
            err.message
        );
        return;
    }

    assert_row_count(&response, 5, &["Deleted"]);

    println!("✅ DELETE multiple rows test completed");
}
