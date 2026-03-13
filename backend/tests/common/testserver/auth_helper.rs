#![allow(dead_code)]
//! Authentication test helpers for integration tests.
//!
//! This module provides utilities for testing authentication flows:
//! - Creating test users with passwords
//! - Authenticating with Bearer tokens
//! - Generating test JWT tokens
//! - Validating authentication responses

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use kalamdb_commons::{AuthType, Role, StorageId, UserId, UserName};
use kalamdb_core::error::KalamDbError;
use kalamdb_core::sql::context::ExecutionContext;
use kalamdb_system::providers::storages::models::StorageMode;
use kalamdb_system::User;
use serde::{Deserialize, Serialize};

use super::consolidated_helpers::ensure_user_exists;
use super::http_server::HttpTestServer;

/// Create a test user with password authentication
///
/// # Arguments
///
/// * `server` - Test server instance
/// * `username` - Username for the test user
/// * `password` - Plain-text password (will be hashed with bcrypt)
/// * `role` - User role (User, Service, Dba, or System)
///
/// # Returns
///
/// The created User object
///
/// # Example
///
/// ```no_run
/// let user = create_test_user(&server, "alice", "SecurePass123!", Role::User).await;
/// assert_eq!(user.username, "alice");
/// ```
pub async fn create_test_user(
    server: &super::TestServer,
    username: &str,
    password: &str,
    role: Role,
) -> User {
    let now = chrono::Utc::now().timestamp_millis();

    // Use username as the user ID (not "test_{username}")
    let user_id = UserId::new(username);

    // Create user via SQL executor (bypassing HTTP layer)
    let role_str = match role {
        Role::Anonymous => "anonymous",
        Role::User => "user",
        Role::Service => "service",
        Role::Dba => "dba",
        Role::System => "system",
    };

    let create_user_sql = format!(
        "CREATE USER '{}' WITH PASSWORD '{}' ROLE {} EMAIL '{}@example.com'",
        username, password, role_str, username
    );

    // Use system user to create the user
    let system_user_id = UserId::system();
    let session = server.app_context.base_session_context();
    let exec_ctx = ExecutionContext::new(system_user_id, Role::System, session);

    let users_provider = server.app_context.system_tables().users();
    if let Ok(Some(mut existing)) = users_provider.get_user_by_username(username) {
        existing.password_hash =
            bcrypt::hash(password, bcrypt::DEFAULT_COST).expect("Failed to hash password");
        existing.role = role;
        existing.email = Some(format!("{}@example.com", username));
        existing.auth_type = AuthType::Password;
        existing.auth_data = None;
        existing.failed_login_attempts = 0;
        existing.locked_until = None;
        existing.deleted_at = None;
        existing.updated_at = chrono::Utc::now().timestamp_millis();
        users_provider.update_user(existing).expect("Failed to update test user");
    } else {
        let result = server
            .sql_executor
            .execute(create_user_sql.as_str(), &exec_ctx, Vec::new())
            .await;

        if let Err(e) = &result {
            if matches!(e, KalamDbError::AlreadyExists(_)) {
                if let Ok(Some(mut existing)) = users_provider.get_user_by_username(username) {
                    existing.password_hash = bcrypt::hash(password, bcrypt::DEFAULT_COST)
                        .expect("Failed to hash password");
                    existing.role = role;
                    existing.email = Some(format!("{}@example.com", username));
                    existing.auth_type = AuthType::Password;
                    existing.auth_data = None;
                    existing.failed_login_attempts = 0;
                    existing.locked_until = None;
                    existing.deleted_at = None;
                    existing.updated_at = chrono::Utc::now().timestamp_millis();
                    users_provider.update_user(existing).expect("Failed to update test user");
                } else {
                    panic!("Failed to load existing test user after create conflict");
                }
            } else {
                panic!("Failed to create test user: {:?}", e);
            }
        }
    }

    eprintln!("✓ Created test user: {}", username);

    // Return user object for test verification
    User {
        user_id,
        username: username.into(),
        password_hash: String::new(), // Not needed for tests
        role,
        email: Some(format!("{}@example.com", username)),
        auth_type: AuthType::Password,
        auth_data: None,
        storage_mode: StorageMode::Table,
        storage_id: Some(StorageId::local()),
        failed_login_attempts: 0,
        locked_until: None,
        last_login_at: None,
        created_at: now,
        updated_at: now,
        last_seen: None,
        deleted_at: None,
    }
}

/// Create Bearer auth header value
///
/// # Arguments
///
/// * `username` - Username for authentication
/// * `password` - Password for authentication
///
/// # Returns
///
/// Authorization header value in format "Bearer <jwt>"
///
/// # Example
///
/// ```no_run
/// let auth_header = create_bearer_auth_header("alice", "user_id", Role::User);
/// assert!(auth_header.starts_with("Bearer "));
/// ```
pub fn create_bearer_auth_header(username: &str, user_id: &str, role: Role) -> String {
    let secret = kalamdb_configs::defaults::default_auth_jwt_secret();
    let email = format!("{}@example.com", username);
    let (token, _claims) = kalamdb_auth::providers::jwt_auth::create_and_sign_token(
        &UserId::new(user_id),
        &UserName::new(username),
        &role,
        Some(email.as_str()),
        Some(1),
        &secret,
    )
    .expect("Failed to create JWT token for test user");
    format!("Bearer {}", token)
}

/// Authenticate with Bearer token and verify response
///
/// Helper function for integration tests that need to authenticate requests.
///
/// # Arguments
///
/// * `username` - Username for authentication
/// * `password` - Password for authentication
///
/// # Returns
///
/// Tuple of (Authorization header string, expected success status)
///
/// # Example
///
/// ```no_run
/// let (auth_header, should_succeed) = authenticate_bearer("alice", "user_id", Role::User);
/// // Use auth_header in HTTP request
/// ```
pub fn authenticate_bearer(username: &str, user_id: &str, role: Role) -> (String, bool) {
    let auth_header = create_bearer_auth_header(username, user_id, role);
    // For now, assume authentication should succeed
    // Tests will verify actual success/failure based on user existence and credentials
    (auth_header, true)
}

/// Create JWT token for testing
///
/// # Arguments
///
/// * `username` - Username to include in JWT claims
/// * `secret` - JWT secret key for signing
/// * `exp_seconds` - Token expiration time in seconds from now
///
/// # Returns
///
/// JWT token string
///
/// # Example
///
/// ```no_run
/// let token = create_jwt_token("alice", "my-secret-key", 3600);
/// let auth_header = format!("Bearer {}", token);
/// ```
pub fn create_jwt_token(username: &str, secret: &str, exp_seconds: i64) -> String {
    #[derive(Debug, Serialize, Deserialize)]
    struct Claims {
        sub: String,
        iss: String,
        exp: usize,
        iat: usize,
        username: String,
        email: Option<String>,
        role: String,
    }

    let now = chrono::Utc::now().timestamp() as usize;
    let claims = Claims {
        sub: format!("user_{}", username),
        iss: "kalamdb".to_string(),
        exp: (now as i64 + exp_seconds) as usize,
        iat: now,
        username: username.to_string(),
        email: Some(format!("{}@example.com", username)),
        role: "user".to_string(),
    };

    let header = Header::new(Algorithm::HS256);
    let encoding_key = EncodingKey::from_secret(secret.as_bytes());
    encode(&header, &claims, &encoding_key).expect("Failed to create JWT token")
}

/// Create test system user for internal operations
///
/// # Example
///
/// ```no_run
/// let system_user = create_system_user(&server, "system").await;
/// assert_eq!(system_user.role, Role::System);
/// ```
pub async fn create_system_user(server: &super::TestServer, username: &str) -> User {
    let now = chrono::Utc::now().timestamp_millis();

    let user = User {
        user_id: UserId::new(format!("sys_{}", username)),
        username: username.into(),
        password_hash: String::new(), // No password for system users (localhost-only)
        role: Role::System,
        email: None,
        auth_type: AuthType::Internal,
        auth_data: None,
        storage_mode: StorageMode::Table,
        storage_id: Some(StorageId::local()),
        failed_login_attempts: 0,
        locked_until: None,
        last_login_at: None,
        created_at: now,
        updated_at: now,
        last_seen: None,
        deleted_at: None,
    };

    server
        .app_context
        .system_tables()
        .users()
        .create_user(user.clone())
        .expect("Failed to insert system user");

    user
}

/// Create a user (if needed) and return a cached Bearer auth header.
///
/// This is the shared helper for tests that need per-user auth headers.
pub async fn create_user_auth_header(
    server: &HttpTestServer,
    username: &str,
    password: &str,
    role: &Role,
) -> anyhow::Result<String> {
    let _ = ensure_user_exists(server, username, password, role).await?;
    server.bearer_auth_header(&UserName::new(username))
}

/// Create a user (if needed) and return both the Bearer auth header and user_id.
pub async fn create_user_auth_header_with_id(
    server: &HttpTestServer,
    username: &str,
    password: &str,
    role: &Role,
) -> anyhow::Result<(String, String)> {
    let user_id = ensure_user_exists(server, username, password, role).await?;
    let auth = server.bearer_auth_header(&UserName::new(username))?;
    Ok((auth, user_id))
}

/// Create a user with a default password and return a cached Bearer auth header.
pub async fn create_user_auth_header_default(
    server: &HttpTestServer,
    username: &str,
) -> anyhow::Result<String> {
    create_user_auth_header(server, username, "UserPass123!", &Role::User).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_bearer_auth_header() {
        let header = create_bearer_auth_header("alice", "user_alice", Role::User);
        assert!(header.starts_with("Bearer "));
    }
}
