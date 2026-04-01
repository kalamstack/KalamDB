//! User Context
//!
//! This module provides `UserContext` - the lightweight shared user identity
//! and read-routing metadata used across transport, auth, and execution code.
//!
//! ## Usage
//!
//! DataFusion-specific session extension support lives in
//! `kalamdb-session-datafusion`. This base type stays free of query-engine
//! dependencies so it remains cheap to clone and widely reusable.
//!
//! The context is injected into DataFusion's SessionState extensions by the
//! adapter crate and read by TableProviders during `scan()` for:
//! - Per-user data filtering (USER tables)
//! - Role-based access control (SYSTEM tables)
//! - Read routing in Raft clusters
//!
//! ## Architecture
//!
//! ```text
//! HTTP Handler → ExecutionContext → SessionState.extensions → TableProvider.scan()
//! ```
use kalamdb_commons::models::{ReadContext, Role, UserId};

/// Session-level user context passed via DataFusion's extension system
///
/// **Purpose**: Pass (user_id, role, read_context) from HTTP handler → ExecutionContext → TableProvider.scan()
/// via SessionState.config.options.extensions (ConfigExtension trait)
///
/// **Architecture**: Stateless TableProviders read this from SessionState during scan(),
/// eliminating the need for per-request provider instances or SessionState clones.
///
/// **Performance**: Storing metadata in extensions allows zero-copy table registration
/// (tables registered once in base_session_context, no clone overhead per request).
///
/// **Read Context** (Spec 021): Determines whether reads must go to the Raft leader.
/// - `Client` (default): External SQL queries - must read from leader for consistency
/// - `Internal`: Background jobs, notifications - can read from any node
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserContext {
    pub user_id: UserId,
    /// Username (if available, for CURRENT_USER() function)
    pub username: Option<kalamdb_commons::UserName>,
    pub role: Role,
    /// Read routing context (default: Client = requires leader)
    pub read_context: ReadContext,
}

impl Default for UserContext {
    fn default() -> Self {
        UserContext {
            user_id: UserId::anonymous(),
            username: None,
            role: Role::Anonymous,
            read_context: ReadContext::Client, // Default to client reads (require leader)
        }
    }
}

impl UserContext {
    /// Create a new session context
    pub fn new(user_id: UserId, role: Role, read_context: ReadContext) -> Self {
        Self {
            user_id,
            username: None,
            role,
            read_context,
        }
    }

    /// Create a new session context with username
    pub fn with_username(
        user_id: UserId,
        username: kalamdb_commons::UserName,
        role: Role,
        read_context: ReadContext,
    ) -> Self {
        Self {
            user_id,
            username: Some(username),
            role,
            read_context,
        }
    }

    /// Create a session context for a client request (requires leader)
    pub fn client(user_id: UserId, role: Role) -> Self {
        Self::new(user_id, role, ReadContext::Client)
    }

    /// Create a session context for a client request with username (requires leader)
    pub fn client_with_username(
        user_id: UserId,
        username: kalamdb_commons::UserName,
        role: Role,
    ) -> Self {
        Self::with_username(user_id, username, role, ReadContext::Client)
    }

    /// Create a session context for internal operations (can read from any node)
    pub fn internal(user_id: UserId, role: Role) -> Self {
        Self::new(user_id, role, ReadContext::Internal)
    }

    /// Check if this is an admin user (System or Dba role)
    #[inline]
    pub fn is_admin(&self) -> bool {
        matches!(self.role, Role::System | Role::Dba)
    }

    /// Check if this is the system role
    #[inline]
    pub fn is_system(&self) -> bool {
        matches!(self.role, Role::System)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_user_context() {
        let ctx = UserContext::default();
        assert_eq!(ctx.user_id.as_str(), "anonymous");
        assert_eq!(ctx.role, Role::Anonymous);
        assert_eq!(ctx.read_context, ReadContext::Client);
        assert!(!ctx.is_admin());
    }

    #[test]
    fn test_user_context_admin() {
        let ctx = UserContext::client(UserId::new("admin"), Role::Dba);
        assert!(ctx.is_admin());
        assert!(!ctx.is_system());

        let ctx = UserContext::client(UserId::system(), Role::System);
        assert!(ctx.is_admin());
        assert!(ctx.is_system());
    }

    #[test]
    fn test_user_context_internal() {
        let ctx = UserContext::internal(UserId::new("job_worker"), Role::Service);
        assert_eq!(ctx.read_context, ReadContext::Internal);
        assert!(!ctx.is_admin());
    }
}
