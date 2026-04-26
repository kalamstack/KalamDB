//! Tests for last_seen tracking.
//!
//! NOTE: last_seen tracking is not part of the unified authentication module.
//! The stateless authenticate() function returns authentication results without
//! side effects. last_seen updates would need to be handled at the handler level.
//!
//! These tests verify token authentication behavior and are placeholders for
//! future last_seen implementation at the HTTP/WebSocket handler level.

use kalamdb_auth::{authenticate, AuthRequest};
use kalamdb_commons::{
    models::{ConnectionInfo, UserId},
    Role,
};

use super::test_support::TestServer;

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

#[tokio::test]
async fn test_authentication_returns_user() {
    let server = TestServer::new_shared().await;
    let user_repo = server.users_repo();

    let username = "auth_user";
    let password = "StrongPass1!";

    // Create user
    server.create_user(username, password, Role::User).await;

    let auth_header = bearer_auth_header(username, username, Role::User);
    let connection_info = ConnectionInfo::new(Some("127.0.0.1".to_string()));
    let auth_request = AuthRequest::Header(auth_header);

    let result = authenticate(auth_request, &connection_info, &user_repo).await;
    assert!(result.is_ok(), "Authentication should succeed");

    let auth_result = result.unwrap();
    assert_eq!(auth_result.user.user_id.as_str(), username);
    assert_eq!(auth_result.user.role, Role::User);
}

#[tokio::test]
async fn test_multiple_authentications_succeed() {
    let server = TestServer::new_shared().await;
    let user_repo = server.users_repo();

    let username = "multi_auth_user";
    let password = "StrongPass1!";

    // Create user
    server.create_user(username, password, Role::User).await;

    let auth_header = bearer_auth_header(username, username, Role::User);
    let connection_info = ConnectionInfo::new(Some("127.0.0.1".to_string()));

    // First authentication
    let auth_request = AuthRequest::Header(auth_header.clone());
    let result1 = authenticate(auth_request, &connection_info, &user_repo).await;
    assert!(result1.is_ok(), "First authentication should succeed");

    // Second authentication (same user, same credentials)
    let auth_request = AuthRequest::Header(auth_header);
    let result2 = authenticate(auth_request, &connection_info, &user_repo).await;
    assert!(result2.is_ok(), "Second authentication should succeed");

    // Both should return the same user
    let user1 = result1.unwrap();
    let user2 = result2.unwrap();
    assert_eq!(user1.user.user_id, user2.user.user_id);
}
