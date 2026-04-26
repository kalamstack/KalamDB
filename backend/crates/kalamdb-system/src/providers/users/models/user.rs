//! User entity for system.users table.
//!
//! Represents a database user with authentication and authorization information.

use kalamdb_commons::{
    datatypes::KalamDataType,
    models::{ids::UserId, AuthType, Role, StorageId},
};
use kalamdb_macros::table;
use serde::{Deserialize, Serialize};

use crate::providers::{storages::models::StorageMode, users::models::auth_data::AuthData};

/// Default maximum failed login attempts before lockout
pub const DEFAULT_MAX_FAILED_ATTEMPTS: i32 = 5;

/// Default lockout duration in minutes
pub const DEFAULT_LOCKOUT_DURATION_MINUTES: i64 = 15;

/// User entity for system.users table.
///
/// Represents a database user with authentication and authorization information.
///
/// ## Fields
/// - `user_id`: Unique user identifier (e.g., "u_123456")
/// - `password_hash`: bcrypt hash of password (cost factor 12)
/// - `role`: User role (User, Service, DBA, System)
/// - `email`: Optional email address
/// - `auth_type`: Authentication method (Password, OAuth, Internal)
/// - `auth_data`: Linked OIDC/OAuth provider connections (typed [`AuthData`])
/// - `storage_mode`: Preferred storage partitioning mode (Table, Region)
/// - `storage_id`: Optional preferred storage configuration ID
/// - `failed_login_attempts`: Number of consecutive failed login attempts
/// - `locked_until`: Unix timestamp in milliseconds when lockout expires
/// - `last_login_at`: Unix timestamp in milliseconds of last successful login
/// - `created_at`: Unix timestamp in milliseconds when user was created
/// - `updated_at`: Unix timestamp in milliseconds when user was last modified
/// - `last_seen`: Optional Unix timestamp in milliseconds of last activity
/// - `deleted_at`: Optional Unix timestamp in milliseconds for soft delete
///
/// ## Serialization
/// - **RocksDB**: FlatBuffers envelope + FlexBuffers payload
/// - **API**: JSON via Serde
///
/// ## Example
///
/// ```rust
/// use kalamdb_commons::{AuthType, Role, StorageId, StorageMode, UserId};
/// use kalamdb_system::User;
///
/// let user = User {
///     user_id:               UserId::new("u_123456"),
///     password_hash:         "$2b$12$...".to_string(),
///     role:                  Role::User,
///     email:                 Some("alice@example.com".to_string()),
///     auth_type:             AuthType::Password,
///     auth_data:             None,
///     storage_mode:          StorageMode::Table,
///     storage_id:            Some(StorageId::new("storage_1")),
///     failed_login_attempts: 0,
///     locked_until:          None,
///     last_login_at:         None,
///     created_at:            1730000000000,
///     updated_at:            1730000000000,
///     last_seen:             None,
///     deleted_at:            None,
/// };
/// ```
/// User struct with fields ordered for optimal memory alignment.
/// 8-byte aligned fields first, then 4-byte, then 1-byte types.
/// This minimizes struct padding and improves cache efficiency.
#[table(
    name = "users",
    comment = "System users for authentication and authorization"
)]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct User {
    // 8-byte aligned fields first (i64, Option<i64>, String/pointer types)
    #[column(
        id = 10,
        ordinal = 12,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Account creation timestamp"
    )]
    pub created_at: i64,
    #[column(
        id = 11,
        ordinal = 13,
        data_type(KalamDataType::Timestamp),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Last account update timestamp"
    )]
    pub updated_at: i64,
    /// Unix timestamp in milliseconds when account lockout expires (None = not locked)
    #[column(
        id = 15,
        ordinal = 10,
        data_type(KalamDataType::Timestamp),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Account lockout expiry timestamp"
    )]
    pub locked_until: Option<i64>,
    /// Unix timestamp in milliseconds of last successful login
    #[column(
        id = 16,
        ordinal = 11,
        data_type(KalamDataType::Timestamp),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Last successful login timestamp"
    )]
    pub last_login_at: Option<i64>,
    #[column(
        id = 12,
        ordinal = 14,
        data_type(KalamDataType::Timestamp),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Last authentication timestamp"
    )]
    pub last_seen: Option<i64>,
    #[column(
        id = 13,
        ordinal = 15,
        data_type(KalamDataType::Timestamp),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Soft delete timestamp"
    )]
    pub deleted_at: Option<i64>,
    #[column(
        id = 1,
        ordinal = 1,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = true,
        default = "None",
        comment = "User identifier (UUID)"
    )]
    pub user_id: UserId,
    #[column(
        id = 3,
        ordinal = 2,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "bcrypt password hash"
    )]
    pub password_hash: String,
    #[column(
        id = 5,
        ordinal = 4,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "User email address"
    )]
    pub email: Option<String>,
    #[column(
        id = 7,
        ordinal = 6,
        data_type(KalamDataType::Json),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Authentication data (JSON for OAuth provider/subject)"
    )]
    pub auth_data: Option<AuthData>,
    #[column(
        id = 9,
        ordinal = 8,
        data_type(KalamDataType::Text),
        nullable = true,
        primary_key = false,
        default = "None",
        comment = "Optional preferred storage configuration ID"
    )]
    pub storage_id: Option<StorageId>,
    // 4-byte aligned fields (enums, i32)
    /// Number of consecutive failed login attempts (reset on successful login)
    #[column(
        id = 14,
        ordinal = 9,
        data_type(KalamDataType::Int),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Number of consecutive failed login attempts"
    )]
    pub failed_login_attempts: i32,
    #[column(
        id = 4,
        ordinal = 3,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "User role: user, service, dba, system"
    )]
    pub role: Role,
    #[column(
        id = 6,
        ordinal = 5,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Authentication type: Password, OAuth, ApiKey"
    )]
    pub auth_type: AuthType,
    #[column(
        id = 8,
        ordinal = 7,
        data_type(KalamDataType::Text),
        nullable = false,
        primary_key = false,
        default = "None",
        comment = "Preferred storage partitioning mode"
    )]
    pub storage_mode: StorageMode,
}

impl User {
    /// Check if the user account is currently locked
    #[inline]
    pub fn is_locked(&self) -> bool {
        if let Some(locked_until) = self.locked_until {
            let now = chrono::Utc::now().timestamp_millis();
            locked_until > now
        } else {
            false
        }
    }

    /// Get remaining lockout time in seconds (0 if not locked)
    #[inline]
    pub fn lockout_remaining_seconds(&self) -> i64 {
        if let Some(locked_until) = self.locked_until {
            let now = chrono::Utc::now().timestamp_millis();
            if locked_until > now {
                return (locked_until - now) / 1000;
            }
        }
        0
    }

    /// Record a failed login attempt, potentially locking the account
    pub fn record_failed_login(&mut self, max_attempts: i32, lockout_duration_minutes: i64) {
        self.failed_login_attempts += 1;
        self.updated_at = chrono::Utc::now().timestamp_millis();

        if self.failed_login_attempts >= max_attempts {
            let now = chrono::Utc::now();
            let lockout_until = now + chrono::Duration::minutes(lockout_duration_minutes);
            self.locked_until = Some(lockout_until.timestamp_millis());
        }
    }

    /// Record a successful login, resetting failed attempts and lockout
    pub fn record_successful_login(&mut self) {
        let now = chrono::Utc::now().timestamp_millis();
        self.failed_login_attempts = 0;
        self.locked_until = None;
        self.last_login_at = Some(now);
        self.updated_at = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_user() -> User {
        User {
            user_id: UserId::new("u_123"),
            password_hash: "$2b$12$hash".to_string(),
            role: Role::User,
            email: Some("test@example.com".to_string()),
            auth_type: AuthType::Password,
            auth_data: None,
            storage_mode: StorageMode::Table,
            storage_id: Some(StorageId::new("storage_1")),
            failed_login_attempts: 0,
            locked_until: None,
            last_login_at: None,
            created_at: 1730000000000,
            updated_at: 1730000000000,
            last_seen: None,
            deleted_at: None,
        }
    }

    #[test]
    fn test_user_serialization() {
        let user = create_test_user();

        let bytes = serde_json::to_vec(&user).unwrap();
        let deserialized: User = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(user, deserialized);
    }

    #[test]
    fn test_user_not_locked_by_default() {
        let user = create_test_user();
        assert!(!user.is_locked());
        assert_eq!(user.lockout_remaining_seconds(), 0);
    }

    #[test]
    fn test_record_failed_login_increments_count() {
        let mut user = create_test_user();
        assert_eq!(user.failed_login_attempts, 0);

        user.record_failed_login(5, 15);
        assert_eq!(user.failed_login_attempts, 1);
        assert!(!user.is_locked());

        user.record_failed_login(5, 15);
        assert_eq!(user.failed_login_attempts, 2);
        assert!(!user.is_locked());
    }

    #[test]
    fn test_account_locks_after_max_attempts() {
        let mut user = create_test_user();

        for _ in 0..5 {
            user.record_failed_login(5, 15);
        }

        assert_eq!(user.failed_login_attempts, 5);
        assert!(user.is_locked());
        assert!(user.lockout_remaining_seconds() > 0);
    }

    #[test]
    fn test_successful_login_resets_failed_attempts() {
        let mut user = create_test_user();
        user.failed_login_attempts = 3;

        user.record_successful_login();

        assert_eq!(user.failed_login_attempts, 0);
        assert!(user.locked_until.is_none());
        assert!(user.last_login_at.is_some());
    }

    #[test]
    fn test_successful_login_clears_lockout() {
        let mut user = create_test_user();
        // Simulate locked account
        user.failed_login_attempts = 5;
        user.locked_until = Some(chrono::Utc::now().timestamp_millis() + 900_000); // 15 min

        user.record_successful_login();

        assert!(!user.is_locked());
        assert_eq!(user.failed_login_attempts, 0);
        assert!(user.locked_until.is_none());
    }
}
