//! AS USER Impersonation Context (Phase 7)
//!
//! Provides secure impersonation capabilities for service and admin accounts
//! to execute DML operations on behalf of other users.
//!
//! ## Security Model
//! - Only Service, Dba, and System roles can use AS USER
//! - Authorization checked in DML handler check_authorization methods
//! - All impersonation operations are audited with both actor and subject
//! - RLS policies applied as if subject_user_id executed the operation
//!
//! ## Usage
//! ```ignore
//! let context = ImpersonationContext::new(
//!     actor_user_id,
//!     actor_role,
//!     subject_user_id,
//!     session_id,
//!     ImpersonationOrigin::SQL
//! );
//! // Authorization is validated in DML handler check_authorization
//! // Context used for audit logging
//! ```

use kalamdb_commons::{models::UserId, Role};

/// Origin of the impersonation request
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImpersonationOrigin {
    /// AS USER clause in SQL statement
    SQL,
    /// API endpoint with impersonation header
    API,
}

/// Context for AS USER impersonation operations
///
/// Captures both the actor (who is making the request) and the subject
/// (who the operation should be executed as) for audit logging.
#[derive(Debug, Clone)]
pub struct ImpersonationContext {
    /// User ID of the actor making the impersonation request
    pub actor_user_id: UserId,
    /// Role of the actor (must be Service, Dba, or System)
    pub actor_role: Role,
    /// User ID of the subject being impersonated
    pub subject_user_id: UserId,
    /// Session ID for audit trail
    pub session_id: String,
    /// Origin of the impersonation request
    pub origin: ImpersonationOrigin,
}

impl ImpersonationContext {
    /// Create a new ImpersonationContext
    ///
    /// # Arguments
    /// * `actor_user_id` - User ID of the actor making the request
    /// * `actor_role` - Role of the actor
    /// * `subject_user_id` - User ID to impersonate
    /// * `session_id` - Session ID for audit trail
    /// * `origin` - Origin of the impersonation request
    ///
    /// # Returns
    /// A new ImpersonationContext instance
    ///
    /// # Note
    /// Authorization validation happens in DML handler check_authorization methods.
    /// This struct is primarily used for audit logging.
    pub fn new(
        actor_user_id: UserId,
        actor_role: Role,
        subject_user_id: UserId,
        session_id: String,
        origin: ImpersonationOrigin,
    ) -> Self {
        Self {
            actor_user_id,
            actor_role,
            subject_user_id,
            session_id,
            origin,
        }
    }

    /// Check if actor role is authorized to use AS USER
    ///
    /// Only Service, Dba, and System roles are permitted.
    /// This is a lightweight check for authorization.
    pub fn is_authorized(&self) -> bool {
        matches!(self.actor_role, Role::Service | Role::Dba | Role::System)
    }

    /// Get the effective user ID for operation execution
    ///
    /// Returns the subject_user_id since operations should be executed
    /// as if the subject performed them.
    pub fn effective_user_id(&self) -> &UserId {
        &self.subject_user_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_impersonation_context_new() {
        let ctx = ImpersonationContext::new(
            UserId::from("actor123"),
            Role::Service,
            UserId::from("subject456"),
            "session-xyz".to_string(),
            ImpersonationOrigin::SQL,
        );

        assert_eq!(ctx.actor_user_id.as_str(), "actor123");
        assert_eq!(ctx.actor_role, Role::Service);
        assert_eq!(ctx.subject_user_id.as_str(), "subject456");
        assert_eq!(ctx.session_id, "session-xyz");
        assert_eq!(ctx.origin, ImpersonationOrigin::SQL);
    }

    #[test]
    fn test_is_authorized_service() {
        let ctx = ImpersonationContext::new(
            UserId::from("actor"),
            Role::Service,
            UserId::from("subject"),
            "session".to_string(),
            ImpersonationOrigin::SQL,
        );
        assert!(ctx.is_authorized());
    }

    #[test]
    fn test_is_authorized_dba() {
        let ctx = ImpersonationContext::new(
            UserId::from("actor"),
            Role::Dba,
            UserId::from("subject"),
            "session".to_string(),
            ImpersonationOrigin::SQL,
        );
        assert!(ctx.is_authorized());
    }

    #[test]
    fn test_is_authorized_system() {
        let ctx = ImpersonationContext::new(
            UserId::from("actor"),
            Role::System,
            UserId::from("subject"),
            "session".to_string(),
            ImpersonationOrigin::SQL,
        );
        assert!(ctx.is_authorized());
    }

    #[test]
    fn test_is_not_authorized_user() {
        let ctx = ImpersonationContext::new(
            UserId::from("actor"),
            Role::User,
            UserId::from("subject"),
            "session".to_string(),
            ImpersonationOrigin::SQL,
        );
        assert!(!ctx.is_authorized());
    }

    #[test]
    fn test_effective_user_id() {
        let ctx = ImpersonationContext::new(
            UserId::from("actor123"),
            Role::Service,
            UserId::from("subject456"),
            "session".to_string(),
            ImpersonationOrigin::SQL,
        );
        assert_eq!(ctx.effective_user_id().as_str(), "subject456");
    }

    #[test]
    fn test_origin_sql() {
        let ctx = ImpersonationContext::new(
            UserId::from("actor"),
            Role::Service,
            UserId::from("subject"),
            "session".to_string(),
            ImpersonationOrigin::SQL,
        );
        assert_eq!(ctx.origin, ImpersonationOrigin::SQL);
    }

    #[test]
    fn test_origin_api() {
        let ctx = ImpersonationContext::new(
            UserId::from("actor"),
            Role::Service,
            UserId::from("subject"),
            "session".to_string(),
            ImpersonationOrigin::API,
        );
        assert_eq!(ctx.origin, ImpersonationOrigin::API);
    }
}
