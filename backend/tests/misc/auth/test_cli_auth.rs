//! Integration tests for CLI authentication and system user initialization
//!
//! **Implements T109**: Test database initialization creates system user
//!
//! These tests verify that:
//! - Database bootstrap creates default "root" system user
//! - System user has correct auth_type (internal) and role (system)
//! - System user can authenticate from localhost
//! - System user credentials are generated securely

use super::test_support::TestServer;
use kalamdb_commons::constants::AuthConstants;
use kalamdb_commons::{AuthType, Role, UserId};

#[tokio::test]
async fn test_init_creates_system_user() {
    // **T109**: Test that database initialization creates system user
    let server = TestServer::new_shared().await;

    // Verify system user exists
    let system_user_id = UserId::new(AuthConstants::DEFAULT_ROOT_USER_ID);
    let user = server
        .app_context
        .system_tables()
        .users()
        .get_user_by_id(&system_user_id)
        .expect("Failed to get user")
        .expect("System user should exist");

    // Verify user properties
    assert_eq!(user.user_id.as_str(), AuthConstants::DEFAULT_ROOT_USER_ID);
    assert_eq!(
        user.auth_type,
        AuthType::Internal,
        "System user should have 'internal' auth type"
    );
    assert_eq!(user.role, Role::System, "System user should have 'system' role");

    // Verify password hash is set (not empty)
    assert!(!user.password_hash.is_empty(), "System user should have password hash");

    // Verify user is not deleted
    assert!(user.deleted_at.is_none(), "System user should not be deleted");

    println!("✓ System user created successfully on bootstrap");
    println!("  User ID: {}", user.user_id);
    println!("  Auth type: {:?}", user.auth_type);
    println!("  Role: {:?}", user.role);
}

#[tokio::test]
async fn test_system_user_password_is_hashed() {
    // Verify that system user password is bcrypt hashed, not plaintext
    let server = TestServer::new_shared().await;

    let system_user_id = UserId::new(AuthConstants::DEFAULT_ROOT_USER_ID);
    let user = server
        .app_context
        .system_tables()
        .users()
        .get_user_by_id(&system_user_id)
        .expect("Failed to get user")
        .expect("System user should exist");

    // Bcrypt hashes start with "" (e.g., "b$...")
    assert!(
        user.password_hash.starts_with(""),
        "Password should be bcrypt hashed, got: {}",
        &user.password_hash[..10]
    );

    // Bcrypt hashes are typically 60 characters
    assert!(
        user.password_hash.len() >= 59 && user.password_hash.len() <= 61,
        "Bcrypt hash should be ~60 characters, got: {}",
        user.password_hash.len()
    );

    println!("✓ System user password is properly bcrypt hashed");
}

#[tokio::test]
async fn test_system_user_has_metadata() {
    // Verify system user has proper metadata (created_at, updated_at)
    let server = TestServer::new_shared().await;

    let system_user_id = UserId::new(AuthConstants::DEFAULT_ROOT_USER_ID);
    let user = server
        .app_context
        .system_tables()
        .users()
        .get_user_by_id(&system_user_id)
        .expect("Failed to get user")
        .expect("System user should exist");

    // Verify timestamps are set
    assert!(user.created_at > 0, "System user should have created_at timestamp");
    assert!(user.updated_at > 0, "System user should have updated_at timestamp");

    // Verify email is set (can be empty or a default)
    // Email is optional for system users
    println!("✓ System user has proper metadata");
    println!("  Created at: {}", user.created_at);
    println!("  Updated at: {}", user.updated_at);
    println!("  Email: {:?}", user.email);
}
