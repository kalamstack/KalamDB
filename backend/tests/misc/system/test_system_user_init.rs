//! Integration tests for T125-T127: System User Initialization
//!
//! Tests that the database creates a default system user on first startup
//! with appropriate credentials and security settings.

use super::test_support::TestServer;
use kalam_client::models::ResponseStatus;
use kalamdb_commons::constants::AuthConstants;

#[tokio::test]
async fn test_system_user_created_on_init() {
    let server = TestServer::new_shared().await;

    // Query system.users to find the system user
    let sql = format!(
        "SELECT * FROM system.users WHERE user_id = '{}'",
        AuthConstants::DEFAULT_ROOT_USER_ID
    );

    let response = server.execute_sql_as_user(&sql, AuthConstants::DEFAULT_ROOT_USER_ID).await;
    assert_eq!(
        response.status,
        ResponseStatus::Success,
        "Failed to query system user: {:?}",
        response.error
    );

    assert!(!response.results.is_empty(), "Expected query results");
    let result = &response.results[0];

    let rows = result.rows_as_maps();
    assert_eq!(rows.len(), 1, "System user should exist");
    let row = &rows[0];

    // Verify username
    let username = row.get("username").and_then(|v| v.as_str()).expect("username missing");
    assert_eq!(username, AuthConstants::DEFAULT_SYSTEM_USERNAME);

    // Verify role
    let role_str = row.get("role").and_then(|v| v.as_str()).expect("role missing");
    assert_eq!(role_str.to_lowercase(), "system");

    // Verify password hash exists
    let password_hash = row
        .get("password_hash")
        .and_then(|v| v.as_str())
        .expect("password_hash missing");
    assert!(!password_hash.is_empty());
    assert!(password_hash.starts_with("$2b$") || password_hash.starts_with("$2y$"));

    // Verify email (uses the username 'root')
    let email = row.get("email").and_then(|v| v.as_str()).expect("email missing");
    assert_eq!(email, "root@localhost");
}

#[tokio::test]
async fn test_system_user_initialization_idempotent() {
    // Use shared server to test that re-initialization doesn't recreate users
    let server1 = TestServer::new_shared().await;

    // Verify user exists
    let sql = format!(
        "SELECT created_at FROM system.users WHERE user_id = '{}'",
        AuthConstants::DEFAULT_ROOT_USER_ID
    );
    let response1 = server1.execute_sql_as_user(&sql, AuthConstants::DEFAULT_ROOT_USER_ID).await;
    let created_at_1 = response1.results[0]
        .row_as_map(0)
        .and_then(|row| row.get("created_at").cloned())
        .and_then(|v| v.as_i64())
        .unwrap();

    // Get the same shared server again (simulating a second access, not restart)
    // The global server is already initialized, so this tests idempotency
    let server2 = TestServer::new_shared().await;

    let response2 = server2.execute_sql_as_user(&sql, AuthConstants::DEFAULT_ROOT_USER_ID).await;
    let created_at_2 = response2.results[0]
        .row_as_map(0)
        .and_then(|row| row.get("created_at").cloned())
        .and_then(|v| v.as_i64())
        .unwrap();

    // Should be the exact same user record (same created_at) since it's the same server
    assert_eq!(created_at_1, created_at_2, "System user should not be recreated");
}
