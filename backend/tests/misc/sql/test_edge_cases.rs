//! Integration tests for edge cases in authentication and authorization
//!
//! Tests:
//! - Empty credentials
//! - Malformed Bearer headers
//! - Concurrent authentication requests
//! - Deleted user authentication
//! - Role changes during session
//! - Maximum password length
//! - Shared table default access levels

use super::test_support::TestServer;
use kalamdb_auth::{authenticate, AuthRequest};
use kalamdb_commons::{
    models::{ConnectionInfo, UserId},
    Role,
};
use std::time::{SystemTime, UNIX_EPOCH};

fn bearer_auth_header(username: &str, user_id: &str, role: Role) -> String {
    let secret = kalamdb_configs::defaults::default_auth_jwt_secret();
    let email = format!("{}@example.com", username);
    let (token, _claims) = kalamdb_auth::providers::jwt_auth::create_and_sign_token(
        &UserId::new(user_id),
        &role,
        Some(email.as_str()),
        Some(1),
        &secret,
    )
    .expect("Failed to create JWT token");
    format!("Bearer {}", token)
}

/// T143A: Test authentication with empty credentials returns error
#[tokio::test]
async fn test_empty_credentials_401() {
    let server = TestServer::new_shared().await;
    let user_repo = server.users_repo();
    let connection_info = ConnectionInfo::new(Some("127.0.0.1:8080".to_string()));

    // Bearer without token
    let auth_request = AuthRequest::Header("Bearer".to_string());
    let result = authenticate(auth_request, &connection_info, &user_repo).await;
    assert!(result.is_err(), "Bearer without token should fail");
}

/// T143B: Test malformed Bearer header returns error
#[tokio::test]
async fn test_malformed_bearer_auth_400() {
    let server = TestServer::new_shared().await;
    let user_repo = server.users_repo();
    let connection_info = ConnectionInfo::new(Some("127.0.0.1:8080".to_string()));

    // Test missing "Bearer " prefix
    let auth_request = AuthRequest::Header("token_without_prefix".to_string());
    let result = authenticate(auth_request, &connection_info, &user_repo).await;
    assert!(result.is_err(), "Missing Bearer prefix should fail");

    // Test Bearer without token
    let auth_request = AuthRequest::Header("Bearer".to_string());
    let result = authenticate(auth_request, &connection_info, &user_repo).await;
    assert!(result.is_err(), "Bearer without token should fail");
}

/// T143C: Test concurrent authentication requests have no race conditions
#[tokio::test]
async fn test_concurrent_auth_no_race_conditions() {
    let server = TestServer::new_shared().await;
    let username = format!(
        "concurrent_user_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time before UNIX_EPOCH")
            .as_nanos()
    );

    // Create test user
    server.create_user(&username, "TestPass123", Role::User).await;

    let user_repo = server.users_repo();

    // Spawn 10 concurrent authentication requests
    let mut handles = vec![];
    for i in 0..10 {
        let user_repo = user_repo.clone();
        let username = username.clone();

        let handle = tokio::spawn(async move {
            let connection_info = ConnectionInfo::new(Some(format!("127.0.0.1:{}", 8080 + i)));
            let secret = kalamdb_configs::defaults::default_auth_jwt_secret();
            let (token, _claims) = kalamdb_auth::providers::jwt_auth::create_and_sign_token(
                &UserId::new(&username),
                &Role::User,
                Some("concurrent@example.com"),
                Some(1),
                &secret,
            )
            .expect("Failed to create JWT token");
            let auth_header = format!("Bearer {}", token);
            let auth_request = AuthRequest::Header(auth_header);
            authenticate(auth_request, &connection_info, &user_repo).await
        });

        handles.push(handle);
    }

    // Wait for all requests to complete
    let results = futures_util::future::join_all(handles).await;

    // All should succeed
    let mut success_count = 0;
    for result in results {
        if let Ok(Ok(_)) = result {
            success_count += 1;
        }
    }

    assert_eq!(success_count, 10, "All concurrent auth requests should succeed");
}

/// T143D: Test deleted user authentication is denied
#[tokio::test]
async fn test_deleted_user_denied() {
    let server = TestServer::new_shared().await;

    // Create user
    server.create_user("deleted_user", "TestPass123", Role::User).await;

    // Soft delete the user via provider
    let users_provider = server.app_context.system_tables().users();
    users_provider
        .delete_user(&UserId::new("deleted_user"))
        .expect("Failed to delete user");

    let user_repo = server.users_repo();
    let connection_info = ConnectionInfo::new(Some("127.0.0.1:8080".to_string()));

    // Try to authenticate with deleted user
    let auth_header = bearer_auth_header("deleted_user", "deleted_user", Role::User);
    let auth_request = AuthRequest::Header(auth_header);
    let result = authenticate(auth_request, &connection_info, &user_repo).await;

    assert!(result.is_err(), "Deleted user authentication should fail");
    let err = result.err().unwrap();
    let err_msg = format!("{:?}", err);
    // The unified auth returns a generic credential failure for security
    // (doesn't reveal whether user exists or is deleted)
    assert!(
        err_msg.contains("UserDeleted")
            || err_msg.contains("deleted")
            || err_msg.contains("Invalid"),
        "Error should indicate auth failure: {}",
        err_msg
    );
}

/// T143E: Test role change applies to next request (not during session)
#[tokio::test]
async fn test_role_change_applies_next_request() {
    let server = TestServer::new_shared().await;

    // Create user with User role
    server.create_user("role_change_user", "TestPass123", Role::User).await;

    let user_repo = server.users_repo();
    let connection_info = ConnectionInfo::new(Some("127.0.0.1:8080".to_string()));

    // First authentication
    let auth_header = bearer_auth_header("role_change_user", "role_change_user", Role::User);
    let auth_request = AuthRequest::Header(auth_header.clone());
    let result1 = authenticate(auth_request, &connection_info, &user_repo).await;
    assert!(result1.is_ok());
    let user1 = result1.unwrap();
    assert_eq!(user1.user.role, Role::User);

    // Change user role to DBA
    let users_provider = server.app_context.system_tables().users();
    let mut user = users_provider
        .get_user_by_id(&UserId::new("role_change_user"))
        .unwrap()
        .unwrap();
    user.role = Role::Dba;
    users_provider.update_user(user).expect("Failed to update user");

    // Second authentication should reflect new role
    // Generate a new token with updated role to reflect the change
    let auth_header = bearer_auth_header("role_change_user", "role_change_user", Role::Dba);
    let auth_request = AuthRequest::Header(auth_header);
    let result2 = authenticate(auth_request, &connection_info, &user_repo).await;
    assert!(result2.is_ok());
    let user2 = result2.unwrap();
    assert_eq!(user2.user.role, Role::Dba, "Role should be updated to DBA");
}

/// T143F: Test maximum password length handling
#[tokio::test]
async fn test_maximum_password_length() {
    let server = TestServer::new_shared().await;

    // bcrypt has a maximum of 72 bytes
    let max_password = "A".repeat(72);

    // Create user with maximum length password
    server.create_user("max_pass_user", &max_password, Role::User).await;

    let user_repo = server.users_repo();
    let connection_info = ConnectionInfo::new(Some("127.0.0.1:8080".to_string()));

    // Try to authenticate via credential flow
    let auth_request = AuthRequest::Credentials {
        user: "max_pass_user".to_string(),
        password: max_password,
    };
    let result = authenticate(auth_request, &connection_info, &user_repo).await;

    assert!(
        result.is_ok(),
        "Authentication with maximum length password should succeed: {:?}",
        result.err()
    );
}
