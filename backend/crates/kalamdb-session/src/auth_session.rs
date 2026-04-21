//! Authenticated Session Context

use crate::UserContext;
use kalamdb_commons::models::{ConnectionInfo, ReadContext, Role, UserId};
use std::sync::Arc;

/// Authentication method used for the session
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthMethod {
    Basic,
    Bearer,
    Direct,
}

/// Authenticated session with user identity and optional metadata
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AuthSession {
    pub user_context: UserContext,
    pub request_id: Option<Arc<str>>,
    pub connection_info: ConnectionInfo,
    pub auth_method: AuthMethod,
}

impl AuthSession {
    pub fn new(user_id: UserId, role: Role) -> Self {
        Self {
            user_context: UserContext::client(user_id, role),
            request_id: None,
            connection_info: ConnectionInfo::new(None),
            auth_method: AuthMethod::Bearer,
        }
    }

    pub fn with_auth_details(
        user_id: UserId,
        role: Role,
        connection_info: ConnectionInfo,
        auth_method: AuthMethod,
    ) -> Self {
        Self {
            user_context: UserContext::client(user_id, role),
            request_id: None,
            connection_info,
            auth_method,
        }
    }

    pub fn with_read_context(user_id: UserId, role: Role, read_context: ReadContext) -> Self {
        Self {
            user_context: UserContext::new(user_id, role, read_context),
            request_id: None,
            connection_info: ConnectionInfo::new(None),
            auth_method: AuthMethod::Bearer,
        }
    }

    pub fn internal(user_id: UserId, role: Role) -> Self {
        Self::with_read_context(user_id, role, ReadContext::Internal)
    }

    pub fn anonymous() -> Self {
        Self {
            user_context: UserContext::client(UserId::anonymous(), Role::Anonymous),
            request_id: None,
            connection_info: ConnectionInfo::new(None),
            auth_method: AuthMethod::Bearer,
        }
    }

    pub fn with_audit_info(
        user_id: UserId,
        role: Role,
        request_id: Option<String>,
        ip_address: Option<String>,
    ) -> Self {
        let connection_info = ConnectionInfo::new(ip_address.clone());
        Self {
            user_context: UserContext::client(user_id, role),
            request_id: request_id.map(Arc::<str>::from),
            connection_info,
            auth_method: AuthMethod::Bearer,
        }
    }

    // Builder methods

    pub fn with_request_id(mut self, request_id: impl Into<Arc<str>>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn with_ip(mut self, ip_address: String) -> Self {
        self.connection_info.remote_addr = Some(Arc::<str>::from(ip_address));
        self
    }

    pub fn with_read_context_mode(mut self, read_context: ReadContext) -> Self {
        self.user_context.read_context = read_context;
        self
    }

    // Accessor methods

    #[inline]
    pub fn user_id(&self) -> &UserId {
        &self.user_context.user_id
    }

    #[inline]
    pub fn role(&self) -> Role {
        self.user_context.role
    }

    #[inline]
    pub fn read_context(&self) -> ReadContext {
        self.user_context.read_context
    }

    #[inline]
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }

    #[inline]
    pub fn ip_address(&self) -> Option<&str> {
        self.connection_info.remote_addr.as_deref()
    }

    #[inline]
    pub fn user_context(&self) -> &UserContext {
        &self.user_context
    }

    #[inline]
    pub fn is_admin(&self) -> bool {
        self.user_context.is_admin()
    }

    #[inline]
    pub fn is_system(&self) -> bool {
        self.user_context.is_system()
    }

    #[inline]
    pub fn is_anonymous(&self) -> bool {
        self.user_context.user_id.is_anonymous()
    }

    #[inline]
    pub fn connection_info(&self) -> &ConnectionInfo {
        &self.connection_info
    }

    #[inline]
    pub fn auth_method(&self) -> AuthMethod {
        self.auth_method
    }

    #[inline]
    pub fn is_localhost(&self) -> bool {
        self.connection_info.is_localhost()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_session_new() {
        let session = AuthSession::new(UserId::new("alice"), Role::User);
        assert_eq!(session.user_id().as_str(), "alice");
        assert_eq!(session.role(), Role::User);
        assert_eq!(session.read_context(), ReadContext::Client);
        assert!(!session.is_admin());
    }

    #[test]
    fn test_auth_session_builder() {
        let session = AuthSession::new(UserId::new("bob"), Role::User)
            .with_request_id("req-123".to_string())
            .with_ip("127.0.0.1".to_string());

        assert_eq!(session.request_id(), Some("req-123"));
        assert_eq!(session.ip_address(), Some("127.0.0.1"));
    }

    #[test]
    fn test_auth_session_internal() {
        let session = AuthSession::internal(UserId::new("worker"), Role::Service);
        assert_eq!(session.read_context(), ReadContext::Internal);
    }

    #[test]
    fn test_auth_session_anonymous() {
        let session = AuthSession::anonymous();
        assert!(session.is_anonymous());
        assert_eq!(session.role(), Role::Anonymous);
    }

    #[test]
    fn test_auth_session_admin_checks() {
        let dba = AuthSession::new(UserId::new("admin"), Role::Dba);
        assert!(dba.is_admin());
        assert!(!dba.is_system());

        let system = AuthSession::new(UserId::system(), Role::System);
        assert!(system.is_admin());
        assert!(system.is_system());
    }
}
