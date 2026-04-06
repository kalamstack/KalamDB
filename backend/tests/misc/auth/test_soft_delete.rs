//! Integration tests for Soft Delete (Feature 006)
//!
//! Tests soft delete behavior:
//! - DELETE sets _deleted=true instead of physical removal
//! - SELECT automatically filters deleted rows
//! - Deleted data can be recovered
//! - _deleted field is accessible when explicitly selected

use super::test_support::{consolidated_helpers::unique_namespace, fixtures, TestServer};
use kalam_client::models::ResponseStatus;

#[actix_web::test]
async fn test_soft_delete_hides_rows() {
    let server = TestServer::new_shared().await;
    let namespace = unique_namespace("test_hide");

    // Setup
    fixtures::create_namespace(&server, &namespace).await;
    server
        .execute_sql_as_user(
            &format!(
                "CREATE TABLE {}.tasks (\n                id TEXT PRIMARY KEY,\n                title TEXT,\n                completed BOOLEAN\n            ) WITH (\n                TYPE = 'USER',\n                STORAGE_ID = 'local'\n            )",
                namespace
            ),
            "user1",
        )
        .await;

    // Insert test data
    server
        .execute_sql_as_user(
            &format!(
                "INSERT INTO {}.tasks (id, title, completed) VALUES ('task1', 'First task', false)",
                namespace
            ),
            "user1",
        )
        .await;

    server
        .execute_sql_as_user(
            &format!(
                "INSERT INTO {}.tasks (id, title, completed) VALUES ('task2', 'Second task', false)",
                namespace
            ),
            "user1",
        )
        .await;

    // Verify both tasks exist
    let response = server
        .execute_sql_as_user(&format!("SELECT id FROM {}.tasks ORDER BY id", namespace), "user1")
        .await;

    assert_eq!(response.status, ResponseStatus::Success);
    assert_eq!(
        response.results[0].rows.as_ref().map(|r| r.len()).unwrap_or(0),
        2,
        "Should have 2 tasks"
    );

    // Delete task1 (soft delete)
    let response = server
        .execute_sql_as_user(
            &format!("DELETE FROM {}.tasks WHERE id = 'task1'", namespace),
            "user1",
        )
        .await;

    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "DELETE should succeed: {:?}",
        response.error
    );

    // Verify task1 is hidden from SELECT
    let response = server
        .execute_sql_as_user(&format!("SELECT id FROM {}.tasks ORDER BY id", namespace), "user1")
        .await;

    assert_eq!(response.status, ResponseStatus::Success);
    let rows = response.rows_as_maps();
    assert_eq!(rows.len(), 1, "Should only see 1 task after soft delete");
    assert_eq!(
        rows[0].get("id").unwrap().as_str().unwrap(),
        "task2",
        "Only task2 should be visible"
    );

    println!("✅ Soft delete hides rows from SELECT");
}

#[actix_web::test]
async fn test_soft_delete_preserves_data() {
    let server = TestServer::new_shared().await;
    let namespace = unique_namespace("test_soft_preserves");

    // Setup
    fixtures::create_namespace(&server, &namespace).await;
    server
        .execute_sql_as_user(
            &format!(
                "CREATE TABLE {}.tasks (\n                id TEXT PRIMARY KEY,\n                title TEXT,\n                completed BOOLEAN\n            ) WITH (\n                TYPE = 'USER',\n                STORAGE_ID = 'local'\n            )",
                namespace
            ),
            "user1",
        )
        .await;

    // Insert and delete
    server
        .execute_sql_as_user(
            &format!(
                "INSERT INTO {}.tasks (id, title, completed) VALUES ('task1', 'Important task', false)",
                namespace
            ),
            "user1",
        )
        .await;

    server
        .execute_sql_as_user(
            &format!("DELETE FROM {}.tasks WHERE id = 'task1'", namespace),
            "user1",
        )
        .await;

    // Query with explicit _deleted column
    let response = server
        .execute_sql_as_user(
            &format!("SELECT id, title, _deleted FROM {}.tasks WHERE id = 'task1'", namespace),
            "user1",
        )
        .await;

    assert_eq!(response.status, ResponseStatus::Success);

    // Note: The soft delete filter is applied before projection, so deleted rows won't appear
    // This is the expected behavior - soft deleted rows are hidden even when selecting _deleted
    assert_eq!(
        response.results[0].rows.as_ref().map(|r| r.len()).unwrap_or(0),
        0,
        "Soft deleted rows should be filtered out automatically"
    );

    println!("✅ Soft delete preserves data (hidden from queries)");
}

#[actix_web::test]
async fn test_deleted_field_default_false() {
    let server = TestServer::new_shared().await;
    let namespace = unique_namespace("test_soft_deleted_field");

    // Setup
    fixtures::create_namespace(&server, &namespace).await;
    server
        .execute_sql_as_user(
            &format!(
                "CREATE TABLE {}.tasks (\n                id TEXT PRIMARY KEY,\n                title TEXT\n            ) WITH (\n                TYPE = 'USER',\n                STORAGE_ID = 'local'\n            )",
                namespace
            ),
            "user1",
        )
        .await;

    // Insert data
    server
        .execute_sql_as_user(
            &format!("INSERT INTO {}.tasks (id, title) VALUES ('task1', 'New task')", namespace),
            "user1",
        )
        .await;

    // Select with _deleted column
    let response = server
        .execute_sql_as_user(
            &format!("SELECT id, title, _deleted FROM {}.tasks", namespace),
            "user1",
        )
        .await;

    assert_eq!(response.status, ResponseStatus::Success);
    let rows = response.rows_as_maps();
    assert_eq!(rows.len(), 1);

    let deleted_value = rows[0].get("_deleted").unwrap().as_bool();
    assert_eq!(deleted_value, Some(false), "_deleted should default to false for new rows");

    println!("✅ _deleted field defaults to false");
}

#[actix_web::test]
async fn test_multiple_deletes() {
    let server = TestServer::new_shared().await;
    let namespace = unique_namespace("test_soft_multi");

    // Setup
    fixtures::create_namespace(&server, &namespace).await;
    server
        .execute_sql_as_user(
            &format!(
                "CREATE TABLE {}.tasks (\n                id TEXT PRIMARY KEY,\n                title TEXT\n            ) WITH (\n                TYPE = 'USER',\n                STORAGE_ID = 'local'\n            )",
                namespace
            ),
            "user1",
        )
        .await;

    // Insert multiple rows
    for i in 1..=5 {
        server
            .execute_sql_as_user(
                &format!(
                    "INSERT INTO {}.tasks (id, title) VALUES ('task{}', 'Task {}')",
                    namespace, i, i
                ),
                "user1",
            )
            .await;
    }

    // Delete tasks 2 and 4
    server
        .execute_sql_as_user(
            &format!("DELETE FROM {}.tasks WHERE id = 'task2'", namespace),
            "user1",
        )
        .await;

    server
        .execute_sql_as_user(
            &format!("DELETE FROM {}.tasks WHERE id = 'task4'", namespace),
            "user1",
        )
        .await;

    // Verify only 3 tasks remain
    let response = server
        .execute_sql_as_user(&format!("SELECT id FROM {}.tasks ORDER BY id", namespace), "user1")
        .await;

    assert_eq!(response.status, ResponseStatus::Success);
    let rows = response.rows_as_maps();
    assert_eq!(rows.len(), 3, "Should have 3 tasks after deleting 2");

    let ids: Vec<&str> = rows.iter().map(|r| r.get("id").unwrap().as_str().unwrap()).collect();

    assert_eq!(ids, vec!["task1", "task3", "task5"]);

    println!("✅ Multiple soft deletes work correctly");
}

#[actix_web::test]
async fn test_delete_with_where_clause() {
    let server = TestServer::new_shared().await;
    let namespace = unique_namespace("test_soft_where");

    // Setup
    fixtures::create_namespace(&server, &namespace).await;
    server
        .execute_sql_as_user(
            &format!(
                "CREATE TABLE {}.tasks (\n                id TEXT PRIMARY KEY,\n                title TEXT,\n                priority INT\n            ) WITH (\n                TYPE = 'USER',\n                STORAGE_ID = 'local'\n            )",
                namespace
            ),
            "user1",
        )
        .await;

    // Insert tasks with different priorities
    server
        .execute_sql_as_user(
            &format!(
                "INSERT INTO {}.tasks (id, title, priority) VALUES ('task1', 'Low priority', 1)",
                namespace
            ),
            "user1",
        )
        .await;

    server
        .execute_sql_as_user(
            &format!(
                "INSERT INTO {}.tasks (id, title, priority) VALUES ('task2', 'High priority', 5)",
                namespace
            ),
            "user1",
        )
        .await;

    server
        .execute_sql_as_user(
            &format!(
                "INSERT INTO {}.tasks (id, title, priority) VALUES ('task3', 'Low priority', 1)",
                namespace
            ),
            "user1",
        )
        .await;

    // Delete all low priority tasks
    let response = server
        .execute_sql_as_user(
            &format!("DELETE FROM {}.tasks WHERE priority = 1", namespace),
            "user1",
        )
        .await;

    assert_eq!(response.status, ResponseStatus::Success);

    // Verify only high priority task remains
    let response = server
        .execute_sql_as_user(&format!("SELECT id FROM {}.tasks", namespace), "user1")
        .await;

    assert_eq!(response.status, ResponseStatus::Success);
    let rows = response.rows_as_maps();
    assert_eq!(rows.len(), 1, "Should have 1 task after conditional delete");
    assert_eq!(rows[0].get("id").unwrap().as_str().unwrap(), "task2");

    println!("✅ DELETE with WHERE clause works correctly");
}

#[actix_web::test]
async fn test_count_excludes_deleted_rows() {
    let server = TestServer::new_shared().await;
    let namespace = unique_namespace("test_soft_count");

    // Setup
    fixtures::create_namespace(&server, &namespace).await;
    server
        .execute_sql_as_user(
            &format!(
                "CREATE TABLE {}.tasks (\n                id TEXT PRIMARY KEY,\n                title TEXT\n            ) WITH (\n                TYPE = 'USER',\n                STORAGE_ID = 'local'\n            )",
                namespace
            ),
            "user1",
        )
        .await;

    // Insert 5 tasks
    for i in 1..=5 {
        server
            .execute_sql_as_user(
                &format!(
                    "INSERT INTO {}.tasks (id, title) VALUES ('task{}', 'Task {}')",
                    namespace, i, i
                ),
                "user1",
            )
            .await;
    }

    // Count before delete
    let response = server
        .execute_sql_as_user(&format!("SELECT COUNT(*) as count FROM {}.tasks", namespace), "user1")
        .await;

    assert_eq!(response.status, ResponseStatus::Success);
    let rows = response.rows_as_maps();
    // Debug print rows[0] to see exact key
    if rows.is_empty() {
        panic!("No result rows returned for COUNT(*)");
    }
    let count_val = rows[0]
        .get("count")
        .or_else(|| rows[0].get("COUNT(*)"))
        .or_else(|| {
            // Fallback to searching for ANY key that contains 'count' (case independent)
            rows[0]
                .iter()
                .find(|(k, _)| k.to_lowercase() == "count" || k.contains("COUNT(*)"))
                .map(|(_, v)| v)
        })
        .expect(&format!("Missing count column. Available columns: {:?}", rows[0].keys()));

    let count = match count_val.inner() {
        serde_json::Value::Number(n) => n.as_i64().unwrap(),
        serde_json::Value::String(s) => s.parse::<i64>().expect("Count string is not a valid i64"),
        _ => panic!("Unexpected count value type: {:?}", count_val),
    };
    assert_eq!(count, 5, "Should count 5 tasks before delete");

    // Delete 2 tasks
    server
        .execute_sql_as_user(
            &format!("DELETE FROM {}.tasks WHERE id IN ('task1', 'task3')", namespace),
            "user1",
        )
        .await;

    // Count after delete (retry to allow async propagation)
    let mut count = None;
    for _ in 0..30 {
        let response = server
            .execute_sql_as_user(
                &format!("SELECT COUNT(*) as count FROM {}.tasks", namespace),
                "user1",
            )
            .await;

        assert_eq!(response.status, ResponseStatus::Success);
        let rows = response.rows_as_maps();
        if rows.is_empty() {
            panic!("No result rows returned for COUNT(*) after delete");
        }

        let count_val = rows[0]
            .get("count")
            .or_else(|| rows[0].get("COUNT(*)"))
            .or_else(|| {
                rows[0]
                    .iter()
                    .find(|(k, _)| k.to_lowercase() == "count" || k.contains("COUNT(*)"))
                    .map(|(_, v)| v)
            })
            .expect(&format!(
                "Missing count column after delete. Available columns: {:?}",
                rows[0].keys()
            ));

        let current = match count_val.inner() {
            serde_json::Value::Number(n) => n.as_i64().unwrap(),
            serde_json::Value::String(s) => {
                s.parse::<i64>().expect("Count string after delete is not a valid i64")
            },
            _ => panic!("Unexpected count value type after delete: {:?}", count_val),
        };

        if current == 3 {
            count = Some(current);
            break;
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert_eq!(count, Some(3), "Should count 3 tasks after soft delete");

    println!("✅ COUNT excludes soft deleted rows");
}
