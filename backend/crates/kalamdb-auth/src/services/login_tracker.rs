//! Login tracking and account lockout management
//!
//! Provides functionality for tracking failed login attempts and
//! implementing account lockout to prevent brute-force attacks.

use std::sync::Arc;

use kalamdb_system::{User, DEFAULT_LOCKOUT_DURATION_MINUTES, DEFAULT_MAX_FAILED_ATTEMPTS};
use log::{info, warn};

use crate::{
    errors::error::{AuthError, AuthResult},
    repository::user_repo::UserRepository,
};

/// Configuration for login tracking behavior
#[derive(Debug, Clone)]
pub struct LoginTrackingConfig {
    /// Maximum failed attempts before lockout
    pub max_failed_attempts: i32,
    /// Lockout duration in minutes
    pub lockout_duration_minutes: i64,
    /// Whether to enable login tracking
    pub enabled: bool,
}

impl Default for LoginTrackingConfig {
    fn default() -> Self {
        Self {
            max_failed_attempts: DEFAULT_MAX_FAILED_ATTEMPTS,
            lockout_duration_minutes: DEFAULT_LOCKOUT_DURATION_MINUTES,
            enabled: true,
        }
    }
}

/// Login tracker for managing failed attempts and lockouts
pub struct LoginTracker {
    config: LoginTrackingConfig,
}

impl LoginTracker {
    /// Create a new login tracker with default configuration
    pub fn new() -> Self {
        Self {
            config: LoginTrackingConfig::default(),
        }
    }

    /// Create a new login tracker with custom configuration
    pub fn with_config(config: LoginTrackingConfig) -> Self {
        Self { config }
    }

    /// Check if a user account is locked and return error if so
    pub fn check_lockout(&self, user: &User) -> AuthResult<()> {
        if !self.config.enabled {
            return Ok(());
        }

        if user.is_locked() {
            let remaining_seconds = user.lockout_remaining_seconds();
            let remaining_minutes = (remaining_seconds + 59) / 60; // Round up

            warn!(
                "Login attempt for locked account: user_id={}, remaining_minutes={}",
                user.user_id, remaining_minutes
            );

            return Err(AuthError::AccountLocked(format!("{} minute(s)", remaining_minutes)));
        }

        Ok(())
    }

    /// Record a failed login attempt and update the user
    pub async fn record_failed_login(
        &self,
        user: &mut User,
        repo: &Arc<dyn UserRepository>,
    ) -> AuthResult<()> {
        if !self.config.enabled {
            return Ok(());
        }

        user.record_failed_login(
            self.config.max_failed_attempts,
            self.config.lockout_duration_minutes,
        );

        if user.is_locked() {
            warn!(
                "Account locked after {} failed attempts: user_id={}, locked_for_minutes={}",
                user.failed_login_attempts, user.user_id, self.config.lockout_duration_minutes
            );
        } else {
            info!(
                "Failed login attempt {}/{} for user_id={}",
                user.failed_login_attempts, self.config.max_failed_attempts, user.user_id
            );
        }

        repo.update_user(user).await
    }

    /// Record a successful login and reset tracking
    ///
    /// **Performance**: Only writes to database if there were failed login attempts
    /// to clear. This avoids a database write on every authenticated request.
    pub async fn record_successful_login(
        &self,
        user: &mut User,
        repo: &Arc<dyn UserRepository>,
    ) -> AuthResult<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let had_failed_attempts = user.failed_login_attempts > 0;

        // Only update the database if there were failed attempts to clear
        if had_failed_attempts {
            user.record_successful_login();
            info!("Successful login, reset failed attempts: user_id={}", user.user_id);
            repo.update_user(user).await
        } else {
            Ok(())
        }
    }
}

impl Default for LoginTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_login_tracking_config_default() {
        let config = LoginTrackingConfig::default();
        assert_eq!(config.max_failed_attempts, 5);
        assert_eq!(config.lockout_duration_minutes, 15);
        assert!(config.enabled);
    }

    #[test]
    fn test_check_lockout_not_locked() {
        use kalamdb_commons::models::{AuthType, Role, UserId};

        let tracker = LoginTracker::new();
        let user = User {
            user_id: UserId::new("u_123"),
            password_hash: "$2b$12$hash".to_string(),
            role: Role::User,
            email: None,
            auth_type: AuthType::Password,
            auth_data: None,
            storage_mode: kalamdb_system::providers::storages::models::StorageMode::Table,
            storage_id: None,
            failed_login_attempts: 0,
            locked_until: None,
            last_login_at: None,
            created_at: 0,
            updated_at: 0,
            last_seen: None,
            deleted_at: None,
        };

        assert!(tracker.check_lockout(&user).is_ok());
    }

    #[test]
    fn test_check_lockout_when_locked() {
        use kalamdb_commons::models::{AuthType, Role, UserId};

        let tracker = LoginTracker::new();
        let user = User {
            user_id: UserId::new("u_123"),
            password_hash: "$2b$12$hash".to_string(),
            role: Role::User,
            email: None,
            auth_type: AuthType::Password,
            auth_data: None,
            storage_mode: kalamdb_system::providers::storages::models::StorageMode::Table,
            storage_id: None,
            failed_login_attempts: 5,
            locked_until: Some(chrono::Utc::now().timestamp_millis() + 900_000),
            last_login_at: None,
            created_at: 0,
            updated_at: 0,
            last_seen: None,
            deleted_at: None,
        };

        let result = tracker.check_lockout(&user);
        assert!(result.is_err());
        assert!(matches!(result, Err(AuthError::AccountLocked(_))));
    }

    #[test]
    fn test_disabled_tracking_skips_lockout_check() {
        use kalamdb_commons::models::{AuthType, Role, UserId};

        let config = LoginTrackingConfig {
            enabled: false,
            ..Default::default()
        };
        let tracker = LoginTracker::with_config(config);

        let user = User {
            user_id: UserId::new("u_123"),
            password_hash: "$2b$12$hash".to_string(),
            role: Role::User,
            email: None,
            auth_type: AuthType::Password,
            auth_data: None,
            storage_mode: kalamdb_system::providers::storages::models::StorageMode::Table,
            storage_id: None,
            failed_login_attempts: 5,
            locked_until: Some(chrono::Utc::now().timestamp_millis() + 900_000),
            last_login_at: None,
            created_at: 0,
            updated_at: 0,
            last_seen: None,
            deleted_at: None,
        };

        // Should pass even though user is "locked" because tracking is disabled
        assert!(tracker.check_lockout(&user).is_ok());
    }
}
