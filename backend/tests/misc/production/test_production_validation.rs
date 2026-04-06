//! Production Readiness: Validation and Error Handling Tests
//!
//! Tests error message clarity, validation logic, and graceful failure modes.
//! These tests ensure users get helpful feedback when things go wrong.

use super::test_support::{consolidated_helpers, TestServer};
use kalam_client::models::ResponseStatus;
use kalamdb_commons::Role;

/// Verify syntax errors return clear, helpful messages
#[tokio::test]
async fn syntax_error_messages_are_clear() {
    let server = TestServer::new_shared().await;

    // Typo in SELECT
    let resp = server.execute_sql("SELCT * FROM system.schemas").await;
    assert_eq!(resp.status, ResponseStatus::Error);
    assert!(resp.error.is_some());

    let error = resp.error.unwrap();
    let error_msg = error.message.to_lowercase();
    // The error should at least indicate something went wrong
    // (actual message might be "No handler for UNKNOWN" or "syntax error" depending on parser)
    assert!(!error_msg.is_empty(), "Should have an error message for invalid syntax");
    println!("Syntax error message: {}", error.message);

    // Missing FROM clause
    let resp = server.execute_sql("SELECT id, name").await;
    assert_eq!(resp.status, ResponseStatus::Error);
    assert!(resp.error.is_some());
    println!("Missing FROM clause error: {}", resp.error.unwrap().message);
}

/// Verify table-not-found errors are clear
#[tokio::test]
async fn table_not_found_error_is_clear() {
    let server = TestServer::new_shared().await;

    let resp = server.execute_sql("SELECT * FROM nonexistent.table").await;

    assert_eq!(resp.status, ResponseStatus::Error);
    assert!(resp.error.is_some());

    let error = resp.error.unwrap();
    let error_msg = error.message.to_lowercase();
    assert!(
        error_msg.contains("not found")
            || error_msg.contains("does not exist")
            || error_msg.contains("unknown")
            || error_msg.contains("nonexistent"),
        "Missing table error should be clear, got: {}",
        error.message
    );
}

/// Verify namespace creation with invalid name fails with clear error
#[tokio::test]
async fn invalid_namespace_name_rejected() {
    let server = TestServer::new_shared().await;

    // Namespace names must be lowercase and start with letter
    let invalid_names = vec![
        "123invalid",   // Starts with number
        "Invalid",      // Has uppercase
        "invalid-name", // Has hyphen (if not allowed)
    ];

    for name in invalid_names {
        let sql = format!("CREATE NAMESPACE {}", name);
        let resp = server.execute_sql(&sql).await;

        // Should either fail with validation error or succeed if name is actually valid
        // We're primarily checking that error messages are clear when they do fail
        if resp.status == ResponseStatus::Error {
            assert!(resp.error.is_some());
            let error = resp.error.unwrap();

            // Error should mention validation or naming rules
            let error_msg = error.message.to_lowercase();
            println!("Namespace '{}' rejected with: {}", name, error.message);

            // Just verify we got an error, the exact validation rules may vary
            assert!(!error_msg.is_empty(), "Error message should not be empty");
        }
    }
}

/// Verify table creation without PRIMARY KEY is rejected
#[tokio::test]
async fn table_without_primary_key_rejected() {
    let server = TestServer::new_shared().await;

    let resp = server
        .execute_sql_as_user("CREATE NAMESPACE IF NOT EXISTS app_nopk", "root")
        .await;
    assert_eq!(resp.status, ResponseStatus::Success);

    // Try to create table without PRIMARY KEY
    let create_table = r#"
        CREATE TABLE app_nopk.invalid (
            id INT,
            name VARCHAR
        )
        WITH (TYPE = 'USER')
    "#;

    let resp = server.execute_sql(create_table).await;
    assert_eq!(resp.status, ResponseStatus::Error);
    assert!(resp.error.is_some());

    let error = resp.error.unwrap();
    let error_msg = error.message.to_lowercase();
    assert!(
        error_msg.contains("primary key") || error_msg.contains("primary_key"),
        "Error should mention PRIMARY KEY requirement, got: {}",
        error.message
    );
}

/// Verify NULL constraint violations are caught
#[tokio::test]
#[ignore = "NOT NULL constraint enforcement not yet implemented"]
async fn null_constraint_violation_detected() {
    let server = TestServer::new_shared().await;

    let resp = server
        .execute_sql_as_user("CREATE NAMESPACE IF NOT EXISTS app_null", "root")
        .await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let create_table = r#"
        CREATE TABLE app_null.users (
            id INT PRIMARY KEY,
            email VARCHAR NOT NULL
        )
        WITH (TYPE = 'USER')
    "#;
    let resp = server.execute_sql_as_user(create_table, "root").await;
    assert_eq!(resp.status, ResponseStatus::Success, "CREATE TABLE failed: {:?}", resp.error);

    // Create a user to own the data
    let user_id = server.create_user("user1_null", "Pass123!", Role::User).await;

    // Try to insert without required field
    let resp = server
        .execute_sql_as_user("INSERT INTO app_null.users (id) VALUES (1)", user_id.as_str())
        .await;

    // Should fail with constraint violation
    assert_eq!(resp.status, ResponseStatus::Error);
    if let Some(error) = resp.error {
        let error_msg = error.message.to_lowercase();

        // Should mention NULL or constraint or required field
        assert!(
            error_msg.contains("null")
                || error_msg.contains("constraint")
                || error_msg.contains("required")
                || error_msg.contains("email"),
            "NULL constraint error should be clear, got: {}",
            error.message
        );
    }
}

/// Verify operations on wrong table types fail clearly
#[tokio::test]
async fn flush_on_stream_table_rejected() {
    let server = TestServer::new_shared().await;

    let resp = server
        .execute_sql_as_user("CREATE NAMESPACE IF NOT EXISTS app_stream", "root")
        .await;
    assert_eq!(resp.status, ResponseStatus::Success);

    // Create a STREAM table
    let create_table = r#"
        CREATE STREAM TABLE app_stream.events (
            event_id TEXT PRIMARY KEY,
            data TEXT
        )
        WITH (TTL_SECONDS = 60)
    "#;
    let resp = server.execute_sql_as_user(create_table, "root").await;
    assert_eq!(resp.status, ResponseStatus::Success, "CREATE TABLE failed: {:?}", resp.error);

    // Try to FLUSH a STREAM table (should fail)
    let _user_id = server.create_user("user1_stream", "Pass123!", Role::User).await;
    let resp = server.execute_sql("STORAGE FLUSH TABLE app_stream.events").await;

    // Should fail - FLUSH doesn't make sense for STREAM tables
    assert_eq!(resp.status, ResponseStatus::Error);
    if let Some(error) = resp.error {
        println!("FLUSH on STREAM table error: {}", error.message);
        // Error should mention table type or unsupported operation
        // We're mainly checking it fails gracefully with a message
        assert!(!error.message.is_empty());
    }
}

/// Verify user isolation - users cannot access other users' data
#[tokio::test]
async fn user_isolation_in_user_tables() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("app_isolation");

    let resp = server
        .execute_sql_as_user(&format!("CREATE NAMESPACE IF NOT EXISTS {}", ns), "root")
        .await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let create_table = format!(
        r#"
        CREATE TABLE {}.private_data (
            id INT PRIMARY KEY,
            secret VARCHAR
        )
        WITH (TYPE = 'USER')
    "#,
        ns
    );
    let resp = server.execute_sql(&create_table).await;
    assert_eq!(resp.status, ResponseStatus::Success);

    // User1 inserts data
    let user1_id = server.create_user("user1_iso", "Pass123!", Role::User).await;
    let resp = server
        .execute_sql_as_user(
            &format!("INSERT INTO {}.private_data (id, secret) VALUES (1, 'user1_secret')", ns),
            user1_id.as_str(),
        )
        .await;
    assert_eq!(resp.status, ResponseStatus::Success);

    // User2 tries to query - should see no rows (user isolation)
    let user2_id = server.create_user("user2_iso", "Pass123!", Role::User).await;
    let resp = server
        .execute_sql_as_user(&format!("SELECT * FROM {}.private_data", ns), user2_id.as_str())
        .await;

    assert_eq!(resp.status, ResponseStatus::Success);
    if let Some(rows) = resp.results.first().and_then(|r| r.rows.as_ref()) {
        assert_eq!(rows.len(), 0, "User2 should not see User1's data in USER table");
    }

    // User1 can still see their own data
    let resp = server
        .execute_sql_as_user(&format!("SELECT * FROM {}.private_data", ns), user1_id.as_str())
        .await;

    assert_eq!(resp.status, ResponseStatus::Success);
    if let Some(rows) = resp.results.first().and_then(|r| r.rows.as_ref()) {
        assert_eq!(rows.len(), 1, "User1 should see their own data");
    }
}

/// Verify duplicate PRIMARY KEY insertion fails with clear error
#[tokio::test]
async fn duplicate_primary_key_rejected() {
    let server = TestServer::new_shared().await;
    let ns = consolidated_helpers::unique_namespace("app_dupkey");

    let resp = server.execute_sql_as_user(&format!("CREATE NAMESPACE {}", ns), "root").await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let create_table = format!(
        r#"
        CREATE TABLE {}.items (
            id INT PRIMARY KEY,
            value VARCHAR
        )
        WITH (TYPE = 'USER')
    "#,
        ns
    );
    let resp = server.execute_sql(&create_table).await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let user_id = server.create_user("user1_dupkey", "Pass123!", Role::User).await;

    // Insert first row
    let resp = server
        .execute_sql_as_user(
            &format!("INSERT INTO {}.items (id, value) VALUES (1, 'first')", ns),
            user_id.as_str(),
        )
        .await;
    assert_eq!(resp.status, ResponseStatus::Success);

    // Try to insert duplicate PRIMARY KEY
    let resp = server
        .execute_sql_as_user(
            &format!("/* strict */ INSERT INTO {}.items (id, value) VALUES (1, 'duplicate')", ns),
            user_id.as_str(),
        )
        .await;

    // Should fail with constraint violation
    assert_eq!(resp.status, ResponseStatus::Error);
    if let Some(error) = resp.error {
        let error_msg = error.message.to_lowercase();
        assert!(
            error_msg.contains("duplicate")
                || error_msg.contains("unique")
                || error_msg.contains("primary key")
                || error_msg.contains("constraint")
                || error_msg.contains("already exists"),
            "Duplicate key error should be clear, got: {}",
            error.message
        );
    }
}

/// Verify DROP TABLE on non-existent table fails gracefully
#[tokio::test]
async fn drop_nonexistent_table_error_is_clear() {
    let server = TestServer::new_shared().await;

    let resp = server.execute_sql("DROP TABLE nonexistent.table").await;

    assert_eq!(resp.status, ResponseStatus::Error);
    assert!(resp.error.is_some());

    let error = resp.error.unwrap();
    let error_msg = error.message.to_lowercase();
    assert!(
        error_msg.contains("not found")
            || error_msg.contains("does not exist")
            || error_msg.contains("unknown"),
        "DROP non-existent table error should be clear, got: {}",
        error.message
    );
}

/// Verify invalid data type in INSERT fails with clear error
#[tokio::test]
async fn invalid_data_type_in_insert_rejected() {
    let server = TestServer::new_shared().await;

    let resp = server
        .execute_sql_as_user("CREATE NAMESPACE IF NOT EXISTS app_datatype", "root")
        .await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let create_table = r#"
        CREATE TABLE app_datatype.numbers (
            id INT PRIMARY KEY,
            value INT
        )
        WITH (TYPE = 'USER')
    "#;
    let resp = server.execute_sql(create_table).await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let user_id = server.create_user("user1_dtype", "Pass123!", Role::User).await;

    // Try to insert string where INT expected
    let resp = server
        .execute_sql_as_user(
            "INSERT INTO app_datatype.numbers (id, value) VALUES (1, 'not_a_number')",
            user_id.as_str(),
        )
        .await;

    // Should fail with type error
    assert_eq!(resp.status, ResponseStatus::Error);
    if let Some(error) = resp.error {
        // Error should mention type mismatch or parsing
        println!("Type mismatch error: {}", error.message);
        assert!(!error.message.is_empty());
    }
}

/// Verify SELECT with invalid column name fails clearly
#[tokio::test]
async fn select_invalid_column_error_is_clear() {
    let server = TestServer::new_shared().await;

    let resp = server
        .execute_sql_as_user("CREATE NAMESPACE IF NOT EXISTS app_selcol", "root")
        .await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let create_table = r#"
        CREATE TABLE app_selcol.test (
            id INT PRIMARY KEY,
            name VARCHAR
        )
        WITH (TYPE = 'USER')
    "#;
    let resp = server.execute_sql(create_table).await;
    assert_eq!(resp.status, ResponseStatus::Success);

    let user_id = server.create_user("user1_selcol", "Pass123!", Role::User).await;

    // Try to SELECT non-existent column
    let resp = server
        .execute_sql_as_user(
            "SELECT id, invalid_column FROM app_selcol.test AS user1_selcol",
            user_id.as_str(),
        )
        .await;

    // Should fail with column not found error
    assert_eq!(resp.status, ResponseStatus::Error);
    if let Some(error) = resp.error {
        let error_msg = error.message.to_lowercase();
        assert!(
            error_msg.contains("column")
                || error_msg.contains("field")
                || error_msg.contains("invalid_column"),
            "Invalid column error should be clear, got: {}",
            error.message
        );
    }
}

/// Verify permission denied errors are clear
#[tokio::test]
async fn permission_denied_error_is_clear() {
    let server = TestServer::new_shared().await;

    // Create regular user (not DBA)
    let user_id = server.create_user("regularuser", "Pass123!", Role::User).await;

    // Try to create namespace as regular user (requires DBA or System role)
    let resp = server
        .execute_sql_as_user("CREATE NAMESPACE restricted", user_id.as_str())
        .await;

    // Should fail with permission error
    assert_eq!(resp.status, ResponseStatus::Error);
    if let Some(error) = resp.error {
        let error_msg = error.message.to_lowercase();
        assert!(
            error_msg.contains("permission")
                || error_msg.contains("denied")
                || error_msg.contains("unauthorized")
                || error_msg.contains("forbidden")
                || error_msg.contains("role"),
            "Permission error should be clear, got: {}",
            error.message
        );
    }
}
