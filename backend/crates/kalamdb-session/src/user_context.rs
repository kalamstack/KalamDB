//! User Context
//!
//! Lightweight shared user identity and read-routing metadata used across
//! transport, auth, and execution code.
use kalamdb_commons::models::{ReadContext, Role, UserId};

/// Session-level user context passed via DataFusion's extension system
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserContext {
    pub user_id: UserId,
    pub role: Role,
    /// Read routing context (default: Client = requires leader)
    pub read_context: ReadContext,
}

impl Default for UserContext {
    fn default() -> Self {
        UserContext {
            user_id: UserId::anonymous(),
            role: Role::Anonymous,
            read_context: ReadContext::Client,
        }
    }
}

impl UserContext {
    pub fn new(user_id: UserId, role: Role, read_context: ReadContext) -> Self {
        Self {
            user_id,
            role,
            read_context,
        }
    }

    /// Create a session context for a client request (requires leader)
    pub fn client(user_id: UserId, role: Role) -> Self {
        Self::new(user_id, role, ReadContext::Client)
    }

    /// Create a session context for internal operations (can read from any node)
    pub fn internal(user_id: UserId, role: Role) -> Self {
        Self::new(user_id, role, ReadContext::Internal)
    }

    #[inline]
    pub fn is_admin(&self) -> bool {
        matches!(self.role, Role::System | Role::Dba)
    }

    #[inline]
    pub fn is_system(&self) -> bool {
        matches!(self.role, Role::System)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_context() {
        let ctx = UserContext::default();
        assert_eq!(ctx.user_id, UserId::anonymous());
        assert_eq!(ctx.role, Role::Anonymous);
        assert_eq!(ctx.read_context, ReadContext::Client);
    }

    #[test]
    fn test_client_context() {
        let ctx = UserContext::client(UserId::new("u_test"), Role::User);
        assert_eq!(ctx.read_context, ReadContext::Client);
        assert!(!ctx.is_admin());
    }

    #[test]
    fn test_internal_context() {
        let ctx = UserContext::internal(UserId::new("u_sys"), Role::System);
        assert_eq!(ctx.read_context, ReadContext::Internal);
        assert!(ctx.is_system());
        assert!(ctx.is_admin());
    }
}
