//! Integration tests for Bearer token authentication
//!
//! These tests verify the unified authentication module works correctly:
//! - Successful authentication with valid token
//! - Rejection of Basic auth headers
//! - Rejection of missing Authorization header
//! - Rejection of malformed Authorization header
//!
//! **Test Philosophy**: Follow TDD - these tests verify the unified authentication
//! flow that is used by both HTTP and WebSocket handlers.

use super::test_support::{auth_helper, TestServer};
use base64::Engine as _;
use kalamdb_auth::AuthError;
use kalamdb_commons::{models::ConnectionInfo, Role};
use std::sync::Arc;

/// Test successful Bearer auth with valid token
#[tokio::test]
async fn test_bearer_auth_success() {
    let server = TestServer::new_shared().await;

    // Create test user with password
    let username = "alice";
    let password = "SecurePassword123!";
    auth_helper::create_test_user(&server, username, password, Role::User).await;

    use kalamdb_auth::{authenticate, AuthRequest, CoreUsersRepo, UserRepository};

    let connection_info = ConnectionInfo::new(Some("127.0.0.1".to_string()));

    // Create Bearer token header
    let secret = kalamdb_configs::defaults::default_auth_jwt_secret();
    let (token, _claims) = kalamdb_auth::providers::jwt_auth::create_and_sign_token(
        &kalamdb_commons::models::UserId::new(username),
        &Role::User,
        Some("alice@example.com"),
        Some(1),
        &secret,
    )
    .expect("Failed to create JWT token");
    let auth_header = format!("Bearer {}", token);
    let auth_request = AuthRequest::Header(auth_header);

    // Create user repository adapter
    let user_repo: Arc<dyn UserRepository> =
        Arc::new(CoreUsersRepo::new(server.app_context.system_tables().users()));

    // Authenticate using unified auth
    let result = authenticate(auth_request, &connection_info, &user_repo).await;

    // Verify success
    if let Err(e) = &result {
        eprintln!("Authentication error: {:?}", e);
    }
    assert!(
        result.is_ok(),
        "Authentication should succeed with valid credentials: {:?}",
        result.as_ref().err()
    );
    let auth_result = result.unwrap();
    assert_eq!(auth_result.user.user_id.as_str(), username);
    assert_eq!(auth_result.user.role, Role::User);

    println!("✓ Bearer auth test passed - User authenticated successfully");
}

/// Test authentication failure with Basic auth header
#[tokio::test]
async fn test_basic_auth_rejected() {
    let server = TestServer::new_shared().await;

    // Create test user
    let username = "bob";
    let correct_password = "CorrectPassword123!";
    let wrong_password = "WrongPassword456!";
    auth_helper::create_test_user(&server, username, correct_password, Role::User).await;

    use kalamdb_auth::{authenticate, AuthRequest, CoreUsersRepo, UserRepository};

    let connection_info = ConnectionInfo::new(Some("127.0.0.1".to_string()));

    // Create Basic Auth header (should be rejected)
    let credentials = base64::engine::general_purpose::STANDARD
        .encode(format!("{}:{}", username, wrong_password));
    let auth_header = format!("Basic {}", credentials);
    let auth_request = AuthRequest::Header(auth_header);

    // Create user repository adapter
    let user_repo: Arc<dyn UserRepository> =
        Arc::new(CoreUsersRepo::new(server.app_context.system_tables().users()));

    // Authenticate using unified auth
    let result = authenticate(auth_request, &connection_info, &user_repo).await;

    // Verify failure
    assert!(result.is_err(), "Basic auth should be rejected");

    println!("✓ Basic auth header correctly rejected");
}

/// Test authentication failure with missing Authorization header
#[tokio::test]
async fn test_auth_missing_header() {
    use kalamdb_auth::{authenticate, AuthRequest, CoreUsersRepo, UserRepository};

    let server = TestServer::new_shared().await;
    let connection_info = ConnectionInfo::new(Some("127.0.0.1".to_string()));

    // Create user repository adapter
    let user_repo: Arc<dyn UserRepository> =
        Arc::new(CoreUsersRepo::new(server.app_context.system_tables().users()));

    // Empty authorization header
    let auth_request = AuthRequest::Header("".to_string());
    let result = authenticate(auth_request, &connection_info, &user_repo).await;

    // Verify failure
    assert!(result.is_err(), "Authentication should fail with missing header");

    println!("✓ Missing Authorization header correctly rejected");
}

/// Test authentication failure with malformed Authorization header
#[tokio::test]
async fn test_basic_auth_malformed_header() {
    use kalamdb_auth::{authenticate, AuthRequest, CoreUsersRepo, UserRepository};

    let server = TestServer::new_shared().await;
    let connection_info = ConnectionInfo::new(Some("127.0.0.1".to_string()));

    // Create user repository adapter
    let user_repo: Arc<dyn UserRepository> =
        Arc::new(CoreUsersRepo::new(server.app_context.system_tables().users()));

    // Test various malformed headers
    let malformed_headers = vec![
        "Basic",                 // Missing credentials
        "Basic notbase64!@#",    // Invalid base64
        "Bearer token123",       // Bearer without valid JWT
        "Basic YWxpY2U=",        // Valid base64 but missing colon
        "BasicYWxpY2U6cGFzcw==", // Missing space after "Basic"
    ];

    for malformed_header in malformed_headers {
        let auth_request = AuthRequest::Header(malformed_header.to_string());
        let result = authenticate(auth_request, &connection_info, &user_repo).await;

        assert!(
            result.is_err(),
            "Authentication should fail for malformed header: {}",
            malformed_header
        );

        println!("✓ Malformed header rejected: {}", malformed_header);
    }
}

/// Test authentication with non-existent user
#[tokio::test]
async fn test_basic_auth_nonexistent_user() {
    use kalamdb_auth::{authenticate, AuthRequest, CoreUsersRepo, UserRepository};

    let server = TestServer::new_shared().await;
    let connection_info = ConnectionInfo::new(Some("127.0.0.1".to_string()));

    // Create user repository adapter
    let user_repo: Arc<dyn UserRepository> =
        Arc::new(CoreUsersRepo::new(server.app_context.system_tables().users()));

    // Create bearer token for user that doesn't exist
    let secret = kalamdb_configs::defaults::default_auth_jwt_secret();
    let (token, _claims) = kalamdb_auth::providers::jwt_auth::create_and_sign_token(
        &kalamdb_commons::models::UserId::new("nonexistent"),
        &Role::User,
        Some("nonexistent@example.com"),
        Some(1),
        &secret,
    )
    .expect("Failed to create JWT token");
    let auth_header = format!("Bearer {}", token);
    let auth_request = AuthRequest::Header(auth_header);

    let result = authenticate(auth_request, &connection_info, &user_repo).await;

    // Verify failure
    assert!(result.is_err(), "Authentication should fail for non-existent user");

    println!("✓ Nonexistent user correctly rejected");
}

/// Test authentication failure when a bearer token carries a stale elevated role.
#[tokio::test]
async fn test_bearer_auth_rejects_role_claim_mismatch() {
    let server = TestServer::new_shared().await;

    let username = "elevated_user";
    let password = "SecurePassword123!";
    auth_helper::create_test_user(&server, username, password, Role::Dba).await;

    let user_id = kalamdb_commons::models::UserId::new(username);
    let users_provider = server.app_context.system_tables().users();
    let mut stored_user = users_provider
        .get_user_by_id(&user_id)
        .expect("Failed to load test user")
        .expect("Test user should exist");
    stored_user.role = Role::User;
    stored_user.updated_at = chrono::Utc::now().timestamp_millis();
    users_provider
        .update_user(stored_user)
        .expect("Failed to downgrade test user role");

    use kalamdb_auth::{authenticate, AuthRequest, CoreUsersRepo, UserRepository};

    let connection_info = ConnectionInfo::new(Some("127.0.0.1".to_string()));
    let secret = kalamdb_configs::defaults::default_auth_jwt_secret();
    let (token, _claims) = kalamdb_auth::providers::jwt_auth::create_and_sign_token(
        &user_id,
        &Role::Dba,
        Some("elevated_user@example.com"),
        Some(1),
        &secret,
    )
    .expect("Failed to create JWT token");
    let auth_request = AuthRequest::Header(format!("Bearer {}", token));

    let user_repo: Arc<dyn UserRepository> =
        Arc::new(CoreUsersRepo::new(server.app_context.system_tables().users()));

    let result = authenticate(auth_request, &connection_info, &user_repo).await;

    assert!(matches!(result, Err(AuthError::InvalidCredentials(_))));

    println!("✓ Stale elevated bearer token correctly rejected");
}
